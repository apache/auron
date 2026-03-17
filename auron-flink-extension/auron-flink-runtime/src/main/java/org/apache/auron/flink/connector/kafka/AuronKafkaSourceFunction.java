/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.auron.flink.connector.kafka;

import static org.apache.auron.flink.connector.kafka.KafkaConstants.*;

import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;
import org.apache.auron.flink.arrow.FlinkArrowReader;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.auron.flink.runtime.operator.FlinkAuronFunction;
import org.apache.auron.flink.table.data.AuronColumnarRowData;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.jni.AuronCallNativeWrapper;
import org.apache.auron.jni.JniBridge;
import org.apache.auron.metric.MetricNode;
import org.apache.auron.protobuf.KafkaFormat;
import org.apache.auron.protobuf.KafkaScanExecNode;
import org.apache.auron.protobuf.KafkaStartupMode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGeneratorSupplier;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.SerializableObject;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auron Kafka source function.
 * Only support AT-LEAST ONCE semantics.
 * If checkpoints are enabled, Kafka offsets are committed via Auron after a successful checkpoint.
 * If checkpoints are disabled, Kafka offsets are committed periodically via Auron.
 *
 * <p>Watermark support uses the table-runtime {@link WatermarkGenerator} directly
 * (from {@code WatermarkPushDownSpec}) with per-partition watermark tracking.
 * The combined watermark emitted downstream is the minimum across all assigned partitions.
 */
public class AuronKafkaSourceFunction extends RichParallelSourceFunction<RowData>
        implements FlinkAuronFunction, CheckpointListener, CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(AuronKafkaSourceFunction.class);
    private final LogicalType outputType;
    private final String auronOperatorId;
    private final String topic;
    private final Properties kafkaProperties;
    private final String format;
    private final Map<String, String> formatConfig;
    private final int bufferSize;
    private final String startupMode;
    private transient PhysicalPlanNode physicalPlanNode;

    // Flink Checkpoint-related, compatible with Flink Kafka Legacy source
    /** State name of the consumer's partition offset states. */
    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";

    private transient ListState<Tuple2<KafkaTopicPartition, Long>> unionOffsetStates;
    /** Data for pending but uncommitted offsets. */
    private transient LinkedMap pendingOffsetsToCommit;

    private transient Map<Integer, Long> restoredOffsets;
    private transient Map<Integer, Long> currentOffsets;
    private final SerializableObject lock = new SerializableObject();
    private volatile boolean isRunning;
    private transient String auronOperatorIdWithSubtaskIndex;
    private transient MetricNode nativeMetric;
    private transient ObjectMapper mapper;

    // Kafka Consumer for partition metadata discovery only (does NOT consume data)
    private transient KafkaConsumer<byte[], byte[]> kafkaConsumer;
    private transient List<Integer> assignedPartitions;

    // Watermark related: uses table-runtime WatermarkGenerator directly
    private WatermarkStrategy<RowData> watermarkStrategy;
    private transient WatermarkGenerator tableWatermarkGenerator;
    private transient Map<Integer, Long> partitionWatermarks;
    private transient long currentCombinedWatermark;

    public AuronKafkaSourceFunction(
            LogicalType outputType,
            String auronOperatorId,
            String topic,
            Properties kafkaProperties,
            String format,
            Map<String, String> formatConfig,
            int bufferSize,
            String startupMode) {
        this.outputType = outputType;
        this.auronOperatorId = auronOperatorId;
        this.topic = topic;
        this.kafkaProperties = kafkaProperties;
        this.format = format;
        this.formatConfig = formatConfig;
        this.bufferSize = bufferSize;
        this.startupMode = startupMode;
    }

    @Override
    public void open(Configuration config) throws Exception {
        // init auron plan
        mapper = new ObjectMapper();
        PhysicalPlanNode.Builder sourcePlan = PhysicalPlanNode.newBuilder();
        KafkaScanExecNode.Builder scanExecNode = KafkaScanExecNode.newBuilder();
        scanExecNode.setKafkaTopic(this.topic);
        scanExecNode.setKafkaPropertiesJson(mapper.writeValueAsString(kafkaProperties));
        scanExecNode.setDataFormat(KafkaFormat.valueOf(this.format.toUpperCase(Locale.ROOT)));
        scanExecNode.setFormatConfigJson(mapper.writeValueAsString(formatConfig));
        scanExecNode.setBatchSize(this.bufferSize);
        if (this.format.equalsIgnoreCase(KafkaConstants.KAFKA_FORMAT_PROTOBUF)) {
            // copy pb desc file
            ClassLoader userClassloader = Thread.currentThread().getContextClassLoader();
            String pbDescFileName = formatConfig.get(KafkaConstants.KAFKA_PB_FORMAT_PB_DESC_FILE_FIELD);
            InputStream in = userClassloader.getResourceAsStream(pbDescFileName);
            String pwd = System.getenv("PWD");
            if (new File(pwd).exists()) {
                File descFile = new File(pwd + "/" + pbDescFileName);
                if (!descFile.exists()) {
                    LOG.info("Auron kafka source writer pb desc file: {}", pbDescFileName);
                    FileUtils.copyInputStreamToFile(in, descFile);
                } else {
                    LOG.warn("Auron kafka source pb desc file already exist, skip copy {}", pbDescFileName);
                }
            } else {
                throw new RuntimeException("PWD is not exist");
            }
        }
        // add kafka meta fields
        scanExecNode.setSchema(SchemaConverters.convertToAuronSchema((RowType) outputType, true));
        auronOperatorIdWithSubtaskIndex =
                this.auronOperatorId + "-" + getRuntimeContext().getIndexOfThisSubtask();
        scanExecNode.setAuronOperatorId(auronOperatorIdWithSubtaskIndex);
        scanExecNode.setStartupMode(KafkaStartupMode.valueOf(startupMode));
        sourcePlan.setKafkaScan(scanExecNode.build());
        this.physicalPlanNode = sourcePlan.build();

        // 1. Initialize Kafka Consumer for partition metadata discovery only (not for data consumption)
        Properties kafkaProps = new Properties();
        kafkaProps.putAll(kafkaProperties);
        // Override to ensure this consumer does not interfere with actual data consumption
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-auron-fetch-meta");
        kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        kafkaProps.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        kafkaProps.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        this.kafkaConsumer = new KafkaConsumer<>(kafkaProps);

        StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
        // 2. Discover and assign partitions for this subtask
        List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(topic);
        int subtaskIndex = runtimeContext.getIndexOfThisSubtask();
        int numSubtasks = runtimeContext.getNumberOfParallelSubtasks();

        this.assignedPartitions = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            int partitionId = partitionInfo.partition();
            if (KafkaTopicPartitionAssigner.assign(topic, partitionId, numSubtasks) == subtaskIndex) {
                assignedPartitions.add(partitionId);
            }
        }
        boolean enableCheckpoint = runtimeContext.isCheckpointingEnabled();
        Map<String, Object> auronRuntimeInfo = new HashMap<>();
        auronRuntimeInfo.put("subtask_index", subtaskIndex);
        auronRuntimeInfo.put("num_readers", numSubtasks);
        auronRuntimeInfo.put("enable_checkpoint", enableCheckpoint);
        auronRuntimeInfo.put("restored_offsets", restoredOffsets);
        auronRuntimeInfo.put("assigned_partitions", assignedPartitions);
        JniBridge.putResource(auronOperatorIdWithSubtaskIndex, mapper.writeValueAsString(auronRuntimeInfo));
        currentOffsets = new HashMap<>();
        pendingOffsetsToCommit = new LinkedMap();
        LOG.info(
                "Auron kafka source init successful, Auron operator id: {}, enableCheckpoint is {}, "
                        + "subtask {} assigned partitions: {}",
                auronOperatorIdWithSubtaskIndex,
                enableCheckpoint,
                subtaskIndex,
                assignedPartitions);

        // 3. Initialize table-runtime WatermarkGenerator if watermarkStrategy is set
        if (watermarkStrategy != null) {
            MetricGroup metricGroup = runtimeContext.getMetricGroup();
            // Create DataStream API WatermarkGenerator via the strategy
            org.apache.flink.api.common.eventtime.WatermarkGenerator<RowData> dsGenerator =
                    watermarkStrategy.createWatermarkGenerator(() -> metricGroup);
            // Extract inner table-runtime WatermarkGenerator from DefaultWatermarkGenerator
            if (dsGenerator instanceof GeneratedWatermarkGeneratorSupplier.DefaultWatermarkGenerator) {
                Field field = GeneratedWatermarkGeneratorSupplier.DefaultWatermarkGenerator.class.getDeclaredField(
                        "innerWatermarkGenerator");
                field.setAccessible(true);
                this.tableWatermarkGenerator = (WatermarkGenerator) field.get(dsGenerator);
            } else {
                throw new IllegalStateException("Expected DefaultWatermarkGenerator from WatermarkPushDownSpec, got: "
                        + dsGenerator.getClass().getName());
            }
            this.partitionWatermarks = new HashMap<>();
            this.currentCombinedWatermark = Long.MIN_VALUE;
        }
        this.isRunning = true;
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        nativeMetric = new MetricNode(new ArrayList<>()) {
            @Override
            public void add(String name, long value) {
                // TODO Integration with Flink metrics
                LOG.info("Metric Auron Source: {} = {}", name, value);
            }
        };
        List<RowType.RowField> fieldList = new LinkedList<>();
        fieldList.add(new RowType.RowField(KAFKA_AURON_META_PARTITION_ID, new IntType(false)));
        fieldList.add(new RowType.RowField(KAFKA_AURON_META_OFFSET, new BigIntType(false)));
        fieldList.add(new RowType.RowField(KAFKA_AURON_META_TIMESTAMP, new BigIntType(false)));
        fieldList.addAll(((RowType) outputType).getFields());
        RowType auronOutputRowType = new RowType(fieldList);

        // Pre-check watermark flag to avoid per-record null checks in the hot path
        final boolean enableWatermark = tableWatermarkGenerator != null;

        AuronCallNativeWrapper wrapper = new AuronCallNativeWrapper(
                FlinkArrowUtils.getRootAllocator(),
                physicalPlanNode,
                nativeMetric,
                0,
                0,
                0,
                AuronAdaptor.getInstance().getAuronConfiguration().getLong(FlinkAuronConfiguration.NATIVE_MEMORY_SIZE));

        if (enableWatermark) {
            // Watermark-enabled path: use table-runtime WatermarkGenerator directly
            while (wrapper.loadNextBatch(batch -> {
                Map<Integer, Long> tmpOffsets = new HashMap<>(currentOffsets);
                FlinkArrowReader arrowReader = FlinkArrowReader.create(batch, auronOutputRowType, 3);
                for (int i = 0; i < batch.getRowCount(); i++) {
                    AuronColumnarRowData tmpRowData = (AuronColumnarRowData) arrowReader.read(i);
                    // Extract kafka meta fields
                    int partitionId = tmpRowData.getInt(-3);
                    long offset = tmpRowData.getLong(-2);
                    long kafkaTimestamp = tmpRowData.getLong(-1);
                    tmpOffsets.put(partitionId, offset);

                    try {
                        // Compute watermark using table-runtime WatermarkGenerator (stateless pure function)
                        // with local Timezone
                        Long watermark = tableWatermarkGenerator.currentWatermark(tmpRowData);
                        // Update per-partition watermark tracking
                        if (watermark != null) {
                            partitionWatermarks.merge(partitionId, watermark, Math::max);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Generated WatermarkGenerator fails to generate:", e);
                    }
                    // Emit record with kafka timestamp
                    sourceContext.collectWithTimestamp(tmpRowData, kafkaTimestamp);
                }

                // After each batch, compute combined watermark (min across all partitions) and emit
                if (!partitionWatermarks.isEmpty()) {
                    long minWatermark = Collections.min(partitionWatermarks.values());
                    if (minWatermark > currentCombinedWatermark) {
                        currentCombinedWatermark = minWatermark;
                        sourceContext.emitWatermark(new Watermark(minWatermark));
                    }
                }

                synchronized (lock) {
                    currentOffsets = tmpOffsets;
                }
            })) {}
        } else {
            // No-watermark path: still use collectWithTimestamp with kafka timestamp
            while (wrapper.loadNextBatch(batch -> {
                Map<Integer, Long> tmpOffsets = new HashMap<>(currentOffsets);
                FlinkArrowReader arrowReader = FlinkArrowReader.create(batch, auronOutputRowType, 3);
                for (int i = 0; i < batch.getRowCount(); i++) {
                    AuronColumnarRowData tmpRowData = (AuronColumnarRowData) arrowReader.read(i);
                    int partitionId = tmpRowData.getInt(-3);
                    long offset = tmpRowData.getLong(-2);
                    long kafkaTimestamp = tmpRowData.getLong(-1);
                    tmpOffsets.put(partitionId, offset);
                    sourceContext.collectWithTimestamp(tmpRowData, kafkaTimestamp);
                }
                synchronized (lock) {
                    currentOffsets = tmpOffsets;
                }
            })) {}
        }
        LOG.info("Auron kafka source run end");
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }

    @Override
    public void close() throws Exception {
        this.isRunning = false;

        // Close the metadata-only Kafka Consumer
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }

        // Close table-runtime WatermarkGenerator
        if (tableWatermarkGenerator != null) {
            tableWatermarkGenerator.close();
        }

        super.close();
    }

    @Override
    public List<PhysicalPlanNode> getPhysicalPlanNodes() {
        return Collections.singletonList(physicalPlanNode);
    }

    @Override
    public RowType getOutputType() {
        return (RowType) outputType;
    }

    @Override
    public String getAuronOperatorId() {
        return auronOperatorId;
    }

    @Override
    public MetricNode getMetricNode() {
        return nativeMetric;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        try {
            final int posInMap = pendingOffsetsToCommit.indexOf(checkpointId);
            if (posInMap == -1) {
                LOG.debug(
                        "Consumer subtask {} received confirmation for unknown checkpoint id {}",
                        getRuntimeContext().getIndexOfThisSubtask(),
                        checkpointId);
                return;
            }

            @SuppressWarnings("unchecked")
            Map<Integer, Long> offsets = (Map<Integer, Long>) pendingOffsetsToCommit.remove(posInMap);

            // remove older checkpoints in map
            for (int i = 0; i < posInMap; i++) {
                pendingOffsetsToCommit.remove(0);
            }

            int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            if (offsets == null || offsets.size() == 0) {
                LOG.info("Consumer subtask {} has empty checkpoint state.", subTaskIndex);
                return;
            }
            String commitOffsetsKey = auronOperatorIdWithSubtaskIndex + "-offsets2commit";
            LOG.info(
                    "Subtask {} commit [{}] offsets for checkpoint: {}, offsets: {}",
                    subTaskIndex,
                    commitOffsetsKey,
                    checkpointId,
                    offsets);
            JniBridge.putResource(commitOffsetsKey, mapper.writeValueAsString(offsets));
        } catch (Exception e) {
            LOG.error("NotifyCheckpointComplete error: ", e);
            if (isRunning) {
                throw e;
            }
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (!isRunning) {
            LOG.warn("Auron kafka source is not running, skip snapshot state");
        } else {
            Map<Integer, Long> copyCurrentOffsets;
            synchronized (lock) {
                // copy offsets, ensure that the corresponding offset has been dispatched to downstream.
                copyCurrentOffsets = new HashMap<>(currentOffsets);
            }
            pendingOffsetsToCommit.put(context.getCheckpointId(), copyCurrentOffsets);
            for (Map.Entry<Integer, Long> offset : copyCurrentOffsets.entrySet()) {
                unionOffsetStates.add(Tuple2.of(new KafkaTopicPartition(topic, offset.getKey()), offset.getValue()));
            }
            LOG.info(
                    "snapshotState for checkpointId: {}, currentOffsets: {}",
                    context.getCheckpointId(),
                    copyCurrentOffsets);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        OperatorStateStore stateStore = context.getOperatorStateStore();
        this.unionOffsetStates = stateStore.getUnionListState(new ListStateDescriptor<>(
                OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<KafkaTopicPartition, Long>>() {})));
        this.restoredOffsets = new HashMap<>();
        if (context.isRestored()) {
            for (Tuple2<KafkaTopicPartition, Long> kafkaTopicPartitionOffsetEntry : unionOffsetStates.get()) {
                restoredOffsets.put(
                        kafkaTopicPartitionOffsetEntry.f0.getPartition(), kafkaTopicPartitionOffsetEntry.f1);
            }
            LOG.info("Restore from state, restoredOffsets: {}", restoredOffsets);
        } else {
            LOG.info("Not restore from state.");
        }
    }

    public void setWatermarkStrategy(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }
}
