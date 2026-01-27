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
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Flink batch ExecNodes to Auron PhysicalPlanNode protobuf representations.
 *
 * <p>This converter extracts information from Flink's execution plan nodes and translates them
 * into Auron's native plan format. It handles:
 *
 * <ul>
 *   <li>Table source scans (Parquet files)
 *   <li>Filter predicates
 *   <li>Projection expressions
 *   <li>Combined Calc operations (filter + projection)
 * </ul>
 *
 * <p>The conversion process:
 *
 * <ol>
 *   <li>Identifies the pattern (scan, scan+calc, etc.)
 *   <li>Extracts table metadata (file paths, schema)
 *   <li>Extracts filter predicates and projection expressions
 *   <li>Converts to Auron protobuf using {@link AuronFlinkConverters}
 * </ol>
 */
public class AuronExecNodeConverter {

    private static final Logger LOG = LoggerFactory.getLogger(AuronExecNodeConverter.class);

    /**
     * Converts a Flink ExecNode (and its inputs) to an Auron PhysicalPlanNode.
     *
     * @param node The Flink ExecNode to convert
     * @param inputs The input nodes (already converted/processed)
     * @return The Auron PhysicalPlanNode protobuf
     * @throws UnsupportedOperationException if the node pattern is not supported
     */
    public static PhysicalPlanNode convert(ExecNode<?> node, List<ExecNode<?>> inputs) {
        LOG.info("Converting Flink ExecNode to Auron plan: {}", node.getDescription());

        // Pattern 1: BatchExecCalc on top of TableSourceScan
        if (node instanceof BatchExecCalc && inputs.size() == 1) {
            ExecNode<?> input = inputs.get(0);
            if (input instanceof CommonExecTableSourceScan) {
                return convertCalcWithScan((BatchExecCalc) node, (CommonExecTableSourceScan) input);
            }
        }

        // Pattern 2: Just TableSourceScan (no calc)
        if (node instanceof CommonExecTableSourceScan) {
            return convertScanOnly((CommonExecTableSourceScan) node);
        }

        throw new UnsupportedOperationException("Unsupported ExecNode pattern for Auron conversion: "
                + node.getClass().getSimpleName()
                + " with "
                + inputs.size()
                + " inputs");
    }

    /**
     * Converts a Calc node with a table source scan underneath.
     *
     * <p>This handles the common pattern where Flink's optimizer combines filter and projection
     * operations into a single Calc node on top of a table scan.
     *
     * @param calc The Calc node containing filter/projection
     * @param scan The table source scan node
     * @return The Auron PhysicalPlanNode
     */
    private static PhysicalPlanNode convertCalcWithScan(BatchExecCalc calc, CommonExecTableSourceScan scan) {
        LOG.debug("Converting Calc + Scan pattern to Auron");

        // Extract scan information
        DynamicTableSourceSpec sourceSpec = scan.getTableSourceSpec();
        ContextResolvedTable resolvedTable = sourceSpec.getContextResolvedTable();

        // Extract file paths from the resolved table
        List<String> filePaths = extractFilePaths(resolvedTable);
        LOG.debug("Extracted {} file paths from table source", filePaths.size());

        // Get the full schema from the scan
        RowType scanOutputSchema = (RowType) scan.getOutputType();
        LOG.debug("Scan output schema: {}", scanOutputSchema);

        // Extract calc information - access protected fields via reflection
        List<RexNode> projections = null;
        RexNode condition = null;
        try {
            java.lang.reflect.Field projectionField =
                    calc.getClass().getSuperclass().getDeclaredField("projection");
            projectionField.setAccessible(true);
            projections = (List<RexNode>) projectionField.get(calc);

            java.lang.reflect.Field conditionField =
                    calc.getClass().getSuperclass().getDeclaredField("condition");
            conditionField.setAccessible(true);
            condition = (RexNode) conditionField.get(calc);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract projection/condition from calc node", e);
        }

        LOG.debug(
                "Calc has {} projections and {} filter",
                projections != null ? projections.size() : 0,
                condition != null ? "a" : "no");

        // Get calc output schema
        RowType calcOutputSchema = (RowType) calc.getOutputType();

        // Split filter conditions if present
        List<RexNode> filterConditions = null;
        if (condition != null) {
            filterConditions = AuronFlinkConverters.splitAndConditions(condition);
            LOG.debug("Split filter into {} conditions", filterConditions.size());
        }

        // Build base Parquet scan plan
        PhysicalPlanNode scanPlan = AuronFlinkConverters.convertParquetScan(
                filePaths,
                scanOutputSchema, // Schema after scan (full schema)
                scanOutputSchema, // Full schema
                null, // No projection at scan level
                filterConditions, // Push filters down to scan
                1, // numPartitions - will be set by runtime
                0 // partitionIndex - will be set by runtime
                );

        // Apply projection if present
        if (projections != null && !projections.isEmpty()) {
            List<String> outputFieldNames = calcOutputSchema.getFieldNames();
            List<LogicalType> outputTypes = new ArrayList<>(calcOutputSchema.getChildren());
            List<String> inputFieldNames = scanOutputSchema.getFieldNames();

            return AuronFlinkConverters.convertProjection(
                    scanPlan, projections, outputFieldNames, outputTypes, inputFieldNames);
        }

        return scanPlan;
    }

    /**
     * Converts a table source scan without any calc operations.
     *
     * @param scan The table source scan node
     * @return The Auron PhysicalPlanNode
     */
    private static PhysicalPlanNode convertScanOnly(CommonExecTableSourceScan scan) {
        LOG.debug("Converting Scan-only pattern to Auron");

        // Extract scan information
        DynamicTableSourceSpec sourceSpec = scan.getTableSourceSpec();
        ContextResolvedTable resolvedTable = sourceSpec.getContextResolvedTable();

        // Extract file paths
        List<String> filePaths = extractFilePaths(resolvedTable);
        LOG.debug("Extracted {} file paths from table source", filePaths.size());

        // Get schema
        RowType outputSchema = (RowType) scan.getOutputType();
        LOG.debug("Scan output schema: {}", outputSchema);

        // Build Parquet scan plan (no filters, no projection)
        return AuronFlinkConverters.convertParquetScan(
                filePaths,
                outputSchema, // Output schema
                outputSchema, // Full schema
                null, // No projection
                null, // No filters
                1, // numPartitions - will be set by runtime
                0 // partitionIndex - will be set by runtime
                );
    }

    /**
     * Extracts file paths from a table source.
     *
     * <p>This method extracts file paths from the table metadata.
     *
     * @param resolvedTable The resolved table metadata
     * @return List of file paths
     */
    private static List<String> extractFilePaths(ContextResolvedTable resolvedTable) {
        List<String> filePaths = new ArrayList<>();

        try {
            // Get the path from table options (standard way for filesystem connector tables)
            if (resolvedTable.getResolvedTable().getOptions().containsKey("path")) {
                String path = resolvedTable.getResolvedTable().getOptions().get("path");
                filePaths.add(path);
                LOG.debug("Extracted path from table options: {}", path);
            }

            if (filePaths.isEmpty()) {
                throw new IllegalStateException("Could not extract file paths from table source. "
                        + "Make sure the table is a filesystem table with 'path' option.");
            }

        } catch (Exception e) {
            LOG.error("Failed to extract file paths from table source", e);
            throw new RuntimeException("Failed to extract file paths for Auron execution", e);
        }

        return filePaths;
    }
}
