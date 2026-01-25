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
package org.apache.auron.flink.planner.execution;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.auron.jni.AuronCallNativeWrapper;
import org.apache.auron.jni.FlinkAuronAdaptor;
import org.apache.auron.metric.MetricNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink SourceFunction that wraps Auron native execution.
 * Executes a PhysicalPlanNode using the Auron native engine and emits results as Flink RowData.
 */
public class AuronBatchExecutionWrapperOperator extends RichSourceFunction<RowData>
        implements ResultTypeQueryable<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(AuronBatchExecutionWrapperOperator.class);

    private final PhysicalPlanNode nativePlan;
    private final RowType outputSchema;
    private final int partitionId;
    private final int stageId;

    private transient BufferAllocator allocator;
    private transient AuronCallNativeWrapper nativeWrapper;
    private transient volatile boolean isRunning;

    /**
     * Creates a new Auron batch execution wrapper.
     *
     * @param nativePlan The Auron PhysicalPlanNode to execute.
     * @param outputSchema The expected output schema (Flink RowType).
     * @param partitionId The partition ID for this task.
     * @param stageId The stage ID for this task.
     */
    public AuronBatchExecutionWrapperOperator(
            PhysicalPlanNode nativePlan, RowType outputSchema, int partitionId, int stageId) {
        this.nativePlan = nativePlan;
        this.outputSchema = outputSchema;
        this.partitionId = partitionId;
        this.stageId = stageId;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        isRunning = true;

        // Get Flink configuration and set in thread context
        // Check if GlobalJobParameters is actually a Configuration
        Configuration config = parameters;
        if (getRuntimeContext().getExecutionConfig().getGlobalJobParameters() instanceof Configuration) {
            config = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        }
        FlinkAuronAdaptor.setThreadConfiguration(config);

        // Create Arrow allocator
        allocator = new RootAllocator(Long.MAX_VALUE);

        // Create native wrapper with null metrics (metrics support can be added later)
        MetricNode emptyMetrics = null;
        int taskId = getRuntimeContext().getIndexOfThisSubtask();
        long nativeMemory = Runtime.getRuntime().maxMemory(); // Use JVM max memory

        LOG.info("Initializing Auron native execution for partition {} stage {} task {}", partitionId, stageId, taskId);

        nativeWrapper = new AuronCallNativeWrapper(
                allocator, nativePlan, emptyMetrics, partitionId, stageId, taskId, nativeMemory);

        LOG.info("Auron native execution initialized successfully");
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {
        // Load and process batches from native engine
        while (isRunning
                && nativeWrapper.loadNextBatch(root -> {
                    try {
                        processBatch(root, ctx);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to process Arrow batch", e);
                    }
                })) {
            // Continue loading batches
        }

        LOG.info("Finished processing all batches from Auron native engine");
    }

    /**
     * Processes an Arrow VectorSchemaRoot batch and emits Flink RowData.
     */
    private void processBatch(VectorSchemaRoot root, SourceContext<RowData> ctx) {
        int rowCount = root.getRowCount();
        int fieldCount = root.getFieldVectors().size();

        LOG.debug("Processing Arrow batch with {} rows and {} columns", rowCount, fieldCount);

        // Convert each row from Arrow to Flink RowData
        for (int rowIdx = 0; rowIdx < rowCount; rowIdx++) {
            GenericRowData rowData = new GenericRowData(fieldCount);

            for (int colIdx = 0; colIdx < fieldCount; colIdx++) {
                FieldVector vector = root.getVector(colIdx);
                LogicalType fieldType = outputSchema.getTypeAt(colIdx);
                Object value = extractValue(vector, rowIdx, fieldType);
                rowData.setField(colIdx, value);
            }

            ctx.collect(rowData);
        }
    }

    /**
     * Extracts a value from an Arrow FieldVector at the given row index.
     */
    private Object extractValue(FieldVector vector, int rowIdx, LogicalType fieldType) {
        // Check for null
        if (vector.isNull(rowIdx)) {
            return null;
        }

        // Extract value based on type
        switch (fieldType.getTypeRoot()) {
            case BOOLEAN:
                return ((org.apache.arrow.vector.BitVector) vector).get(rowIdx) != 0;
            case TINYINT:
                return ((org.apache.arrow.vector.TinyIntVector) vector).get(rowIdx);
            case SMALLINT:
                return ((org.apache.arrow.vector.SmallIntVector) vector).get(rowIdx);
            case INTEGER:
                return ((org.apache.arrow.vector.IntVector) vector).get(rowIdx);
            case BIGINT:
                return ((org.apache.arrow.vector.BigIntVector) vector).get(rowIdx);
            case FLOAT:
                return ((org.apache.arrow.vector.Float4Vector) vector).get(rowIdx);
            case DOUBLE:
                return ((org.apache.arrow.vector.Float8Vector) vector).get(rowIdx);
            case CHAR:
            case VARCHAR:
                byte[] bytes = ((org.apache.arrow.vector.VarCharVector) vector).get(rowIdx);
                return StringData.fromBytes(bytes);
            case DATE:
                int daysSinceEpoch = ((org.apache.arrow.vector.DateDayVector) vector).get(rowIdx);
                return daysSinceEpoch;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Arrow stores microseconds, Flink expects milliseconds
                long micros = ((org.apache.arrow.vector.TimeStampMicroVector) vector).get(rowIdx);
                return org.apache.flink.table.data.TimestampData.fromEpochMillis(micros / 1000);
            case DECIMAL:
                // Handle decimal conversion (requires more complex logic)
                // For MVP, we can throw an exception or return null
                throw new UnsupportedOperationException("Decimal conversion not yet implemented in MVP");
            default:
                throw new UnsupportedOperationException("Type not supported: " + fieldType.getTypeRoot());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
        LOG.info("Cancelling Auron native execution");
    }

    @Override
    public void close() throws Exception {
        try {
            if (nativeWrapper != null) {
                nativeWrapper.close();
                nativeWrapper = null;
            }
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
            FlinkAuronAdaptor.clearThreadConfiguration();
        } finally {
            super.close();
        }
        LOG.info("Closed Auron native execution wrapper");
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return InternalTypeInfo.of(outputSchema);
    }
}
