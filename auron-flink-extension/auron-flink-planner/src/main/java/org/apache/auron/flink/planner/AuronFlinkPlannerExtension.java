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
package org.apache.auron.flink.planner;

import java.util.ArrayList;
import java.util.List;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.auron.flink.planner.execution.AuronBatchExecutionWrapperOperator;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for enabling Auron native execution in Flink batch queries.
 *
 * <p>This class provides utilities to convert Flink table operations to use
 * Auron's native vectorized execution engine for improved performance.
 *
 * <p><strong>MVP Usage Example:</strong>
 * <pre>{@code
 * // Setup environment and configuration
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 * StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
 * Configuration config = tEnv.getConfig().getConfiguration();
 *
 * // Enable Auron
 * config.setBoolean("table.exec.auron.enable", true);
 * config.setString("execution.runtime-mode", "BATCH");
 *
 * // Create Parquet table
 * tEnv.executeSql("CREATE TABLE my_table (...) WITH ('connector' = 'filesystem', 'format' = 'parquet')");
 *
 * // Execute query (future: automatic conversion)
 * Table result = tEnv.sqlQuery("SELECT col1, col2 FROM my_table WHERE col3 > 100");
 * result.execute().print();
 * }</pre>
 *
 * <p><strong>Future Enhancement:</strong> In a future release, this extension will integrate
 * with Flink's optimizer via custom Calcite rules to automatically convert plans. For the MVP,
 * conversion must be explicitly requested.
 */
public class AuronFlinkPlannerExtension {

    private static final Logger LOG = LoggerFactory.getLogger(AuronFlinkPlannerExtension.class);

    /**
     * Checks if Auron native execution is enabled in the configuration.
     *
     * @param config The Flink Configuration to check.
     * @return true if Auron is enabled, false otherwise.
     */
    public static boolean isAuronEnabled(Configuration config) {
        return config.getBoolean(FlinkAuronConfiguration.AURON_ENABLE);
    }

    /**
     * Creates a DataStream that executes a Parquet scan query using Auron native execution.
     * This is a convenience method for MVP testing and development.
     *
     * @param env The Flink StreamExecutionEnvironment.
     * @param filePaths List of Parquet file paths to scan.
     * @param outputSchema The output schema (RowType) of the scan.
     * @param fullSchema The full schema of the Parquet files (before projection).
     * @param projectedFields Indices of fields to project (null for all fields).
     * @param filterPredicates Optional filter predicates for pushdown.
     * @param parallelism Parallelism for the operation.
     * @return A DataStream containing the query results as RowData.
     */
    public static DataStream<RowData> createAuronParquetScan(
            StreamExecutionEnvironment env,
            List<String> filePaths,
            RowType outputSchema,
            RowType fullSchema,
            int[] projectedFields,
            List<RexNode> filterPredicates,
            int parallelism) {

        LOG.info("Creating Auron Parquet scan for {} files with parallelism {}",
                filePaths.size(), parallelism);

        // Build the native plan
        PhysicalPlanNode nativePlan = AuronFlinkConverters.convertParquetScan(
                filePaths,
                outputSchema,
                fullSchema,
                projectedFields,
                filterPredicates,
                parallelism,
                0 // partitionIndex will be set per parallel instance
        );

        // Create source function
        AuronBatchExecutionWrapperOperator sourceFunction =
                new AuronBatchExecutionWrapperOperator(
                        nativePlan,
                        outputSchema,
                        0, // partitionId
                        1  // stageId
                );

        // Add source to environment
        return env.addSource(sourceFunction)
                .name("AuronParquetScan")
                .setParallelism(parallelism);
    }

    /**
     * Creates a DataStream that applies a projection using Auron native execution.
     *
     * @param inputStream The input DataStream.
     * @param inputPlan The PhysicalPlanNode representing the input.
     * @param projections List of projection expressions.
     * @param outputFieldNames Names of output fields.
     * @param outputSchema Output schema (RowType).
     * @param inputFieldNames Names of input fields for expression conversion.
     * @return A DataStream with the projection applied.
     */
    public static DataStream<RowData> createAuronProjection(
            DataStream<RowData> inputStream,
            PhysicalPlanNode inputPlan,
            List<RexNode> projections,
            List<String> outputFieldNames,
            RowType outputSchema,
            List<String> inputFieldNames) {

        LOG.info("Creating Auron projection with {} output fields", outputFieldNames.size());

        // Build projection plan
        PhysicalPlanNode projectionPlan = AuronFlinkConverters.convertProjection(
                inputPlan,
                projections,
                outputFieldNames,
                new ArrayList<>(outputSchema.getChildren()),
                inputFieldNames
        );

        // For MVP, we'll need to create a new source wrapping this plan
        // In a full implementation, this would be a transformation operator
        AuronBatchExecutionWrapperOperator operator =
                new AuronBatchExecutionWrapperOperator(
                        projectionPlan,
                        outputSchema,
                        0, // partitionId
                        2  // stageId for projection
                );

        return inputStream.getExecutionEnvironment()
                .addSource(operator)
                .name("AuronProjection")
                .setParallelism(inputStream.getParallelism());
    }

    /**
     * Creates a DataStream that applies a filter using Auron native execution.
     *
     * @param inputStream The input DataStream.
     * @param inputPlan The PhysicalPlanNode representing the input.
     * @param filterConditions List of filter conditions.
     * @param inputSchema Input schema (RowType).
     * @param inputFieldNames Names of input fields for expression conversion.
     * @return A DataStream with the filter applied.
     */
    public static DataStream<RowData> createAuronFilter(
            DataStream<RowData> inputStream,
            PhysicalPlanNode inputPlan,
            List<RexNode> filterConditions,
            RowType inputSchema,
            List<String> inputFieldNames) {

        LOG.info("Creating Auron filter with {} conditions", filterConditions.size());

        // Build filter plan
        PhysicalPlanNode filterPlan = AuronFlinkConverters.convertFilter(
                inputPlan,
                filterConditions,
                inputFieldNames
        );

        // For MVP, create source wrapping the filter plan
        AuronBatchExecutionWrapperOperator operator =
                new AuronBatchExecutionWrapperOperator(
                        filterPlan,
                        inputSchema,
                        0, // partitionId
                        3  // stageId for filter
                );

        return inputStream.getExecutionEnvironment()
                .addSource(operator)
                .name("AuronFilter")
                .setParallelism(inputStream.getParallelism());
    }

    /**
     * Logs information about the Auron configuration.
     *
     * @param config The Flink Configuration.
     */
    public static void logAuronConfiguration(Configuration config) {
        FlinkAuronConfiguration auronConfig = new FlinkAuronConfiguration(config);

        LOG.info("Auron Configuration:");
        LOG.info("  Enabled: {}", config.getBoolean(FlinkAuronConfiguration.AURON_ENABLE));
        LOG.info("  Scan Enabled: {}", config.getBoolean(FlinkAuronConfiguration.AURON_ENABLE_SCAN));
        LOG.info("  Project Enabled: {}", config.getBoolean(FlinkAuronConfiguration.AURON_ENABLE_PROJECT));
        LOG.info("  Filter Enabled: {}", config.getBoolean(FlinkAuronConfiguration.AURON_ENABLE_FILTER));
        LOG.info("  Batch Size: {}", config.getInteger(FlinkAuronConfiguration.AURON_BATCH_SIZE));
        LOG.info("  Memory Fraction: {}", config.getDouble(FlinkAuronConfiguration.AURON_MEMORY_FRACTION));
        LOG.info("  Log Level: {}", config.getString(FlinkAuronConfiguration.AURON_LOG_LEVEL));
    }

    /**
     * Validates that the runtime mode is BATCH (Auron only supports batch mode in MVP).
     *
     * @param config The Flink Configuration.
     * @throws IllegalStateException if runtime mode is not BATCH.
     */
    public static void validateBatchMode(Configuration config) {
        String runtimeMode = config.getString(
                org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE);

        if (!"BATCH".equalsIgnoreCase(runtimeMode)) {
            throw new IllegalStateException(
                    "Auron MVP only supports BATCH execution mode. Current mode: " + runtimeMode +
                    ". Set 'execution.runtime-mode' to 'BATCH'.");
        }
    }
}
