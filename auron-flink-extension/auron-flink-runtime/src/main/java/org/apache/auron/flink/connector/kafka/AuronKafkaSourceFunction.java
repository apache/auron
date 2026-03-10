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

import org.apache.auron.flink.arrow.FlinkArrowReader;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.auron.flink.runtime.operator.FlinkAuronFunction;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.jni.AuronCallNativeWrapper;
import org.apache.auron.metric.MetricNode;
import org.apache.auron.protobuf.KafkaFormat;
import org.apache.auron.protobuf.KafkaScanExecNode;
import org.apache.auron.protobuf.KafkaStartupMode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.*;

import static org.apache.auron.flink.connector.kafka.KafkaConstants.*;

/**
 * Auron Kafka source function.
 */
public class AuronKafkaSourceFunction extends RichParallelSourceFunction<RowData> implements FlinkAuronFunction {
    private static final Logger LOG = LoggerFactory.getLogger(AuronKafkaSourceFunction.class);

    private final LogicalType outputType;
    private final String auronOperatorId;
    private final String topic;
    private final String kafkaPropertiesJson;
    private final String format;
    private final Map<String, String> formatConfig;
    private final int bufferSize;
    private final String startupMode;
    private transient PhysicalPlanNode physicalPlanNode;
    private volatile boolean isRunning;
    private transient String auronOperatorIdWithSubtaskIndex;
    private transient MetricNode nativeMetric;
    public AuronKafkaSourceFunction(
            LogicalType outputType,
            String auronOperatorId,
            String topic,
            String kafkaPropertiesJson,
            String format,
            Map<String, String> formatConfig,
            int bufferSize,
            String startupMode
    ) {
        this.outputType = outputType;
        this.auronOperatorId = auronOperatorId;
        this.topic = topic;
        this.kafkaPropertiesJson = kafkaPropertiesJson;
        this.format = format;
        this.formatConfig = formatConfig;
        this.bufferSize = bufferSize;
        this.startupMode = startupMode;
    }

    @Override
    public void open(Configuration config) throws Exception {
        // init auron plan
        PhysicalPlanNode.Builder sourcePlan = PhysicalPlanNode.newBuilder();
        KafkaScanExecNode.Builder scanExecNode = KafkaScanExecNode.newBuilder();
        scanExecNode.setKafkaTopic(this.topic);
        scanExecNode.setKafkaPropertiesJson(this.kafkaPropertiesJson);
        scanExecNode.setDataFormat(KafkaFormat.valueOf(this.format.toUpperCase(Locale.ROOT)));
        ObjectMapper mapper = new ObjectMapper();
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
                    LOG.info("Auron kafka source writer pb desc file: " + pbDescFileName);
                    FileUtils.copyInputStreamToFile(in, descFile);
                } else {
                    LOG.warn("Auron kafka source pb desc file already exist, skip copy " + pbDescFileName);
                }
            } else {
                throw new RuntimeException("PWD is not exist");
            }
        }
        // add kafka meta fields
        scanExecNode.setSchema(SchemaConverters.convertToAuronSchema((RowType) outputType, true));
        auronOperatorIdWithSubtaskIndex = this.auronOperatorId + "-" + getRuntimeContext().getIndexOfThisSubtask();
        scanExecNode.setAuronOperatorId(auronOperatorIdWithSubtaskIndex);
        scanExecNode.setStartupMode(KafkaStartupMode.valueOf(startupMode));
        sourcePlan.setKafkaScan(scanExecNode.build());
        this.physicalPlanNode = sourcePlan.build();
        this.isRunning = true;
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        nativeMetric = new MetricNode(new ArrayList<>()) {
            @Override
            public void add(String name, long value) {
                // TODO Integration with Flink metrics
                LOG.info(String.format("Metric Auron Source: %s = %s", name, value));
            }
        };
        List<RowType.RowField> fieldList = new LinkedList<>();
        fieldList.add(new RowType.RowField(KAFKA_AURON_META_PARTITION_ID, new IntType(false)));
        fieldList.add(new RowType.RowField(KAFKA_AURON_META_OFFSET, new BigIntType(false)));
        fieldList.add(new RowType.RowField(KAFKA_AURON_META_TIMESTAMP, new BigIntType(false)));
        fieldList.addAll(((RowType) outputType).getFields());
        RowType auronOutputRowType = new RowType(fieldList);
        while (this.isRunning) {
            AuronCallNativeWrapper wrapper = new AuronCallNativeWrapper(
                    FlinkArrowUtils.getRootAllocator(), physicalPlanNode, nativeMetric, 0, 0, 0,
                    AuronAdaptor.getInstance().getAuronConfiguration().getLong(FlinkAuronConfiguration.NATIVE_MEMORY_SIZE));
            while (wrapper.loadNextBatch(batch -> {
                FlinkArrowReader arrowReader = FlinkArrowReader.create(batch, auronOutputRowType, 3);
                for (int i = 0; i < batch.getRowCount(); i++) {
                    sourceContext.collect(arrowReader.read(i));
                }
            }));
        }
        LOG.info("Auron kafka source run end");
    }

    @Override
    public void cancel() {
        this.isRunning = false;
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


}
