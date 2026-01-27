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

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.auron.protobuf.ArrowType;
import org.apache.auron.protobuf.Field;
import org.apache.auron.protobuf.FileGroup;
import org.apache.auron.protobuf.FileScanExecConf;
import org.apache.auron.protobuf.FilterExecNode;
import org.apache.auron.protobuf.ParquetScanExecNode;
import org.apache.auron.protobuf.PartitionedFile;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.apache.auron.protobuf.Schema;
import org.apache.auron.protobuf.Statistics;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

/**
 * Converter for Flink execution plan nodes to Auron PhysicalPlanNode protobuf.
 * Provides methods to convert Parquet scans, projections, and filters.
 */
public class AuronFlinkConverters {

    /**
     * Converts a Parquet scan operation to ParquetScanExecNode protobuf.
     *
     * @param filePaths List of Parquet file paths to scan.
     * @param schema The output schema of the scan (after projection).
     * @param fullSchema The full schema of the Parquet files.
     * @param projectedFieldIndices Indices of fields to project (null for all fields).
     * @param predicates Optional filter predicates for predicate pushdown.
     * @param numPartitions Total number of partitions.
     * @param partitionIndex Current partition index.
     * @return The PhysicalPlanNode containing the ParquetScanExecNode.
     */
    public static PhysicalPlanNode convertParquetScan(
            List<String> filePaths,
            RowType schema,
            RowType fullSchema,
            int[] projectedFieldIndices,
            List<RexNode> predicates,
            int numPartitions,
            int partitionIndex) {

        // Build the file group
        FileGroup.Builder fileGroupBuilder = FileGroup.newBuilder();
        for (String path : filePaths) {
            // Get actual file size from filesystem
            long fileSize = getFileSize(path);

            PartitionedFile partitionedFile = PartitionedFile.newBuilder()
                    .setPath(path)
                    .setSize(fileSize) // Actual file size from filesystem
                    .setLastModifiedNs(0)
                    .build();
            fileGroupBuilder.addFiles(partitionedFile);
        }

        // Convert schema to protobuf
        Schema nativeSchema = convertSchema(fullSchema);
        Schema outputSchema = convertSchema(schema);

        // Build projection indices
        List<Integer> projection = new ArrayList<>();
        if (projectedFieldIndices != null) {
            for (int idx : projectedFieldIndices) {
                projection.add(idx);
            }
        } else {
            // No projection, use all fields
            for (int i = 0; i < fullSchema.getFieldCount(); i++) {
                projection.add(i);
            }
        }

        // Build FileScanExecConf
        FileScanExecConf.Builder confBuilder = FileScanExecConf.newBuilder()
                .setNumPartitions(numPartitions)
                .setPartitionIndex(partitionIndex)
                .setFileGroup(fileGroupBuilder.build())
                .setSchema(nativeSchema)
                .addAllProjection(projection)
                .setStatistics(Statistics.getDefaultInstance())
                .setPartitionSchema(Schema.newBuilder().build()); // Empty partition schema for MVP

        // Convert predicates for pushdown
        List<PhysicalExprNode> pruningPredicates = new ArrayList<>();
        if (predicates != null && !predicates.isEmpty()) {
            List<String> fieldNames = fullSchema.getFieldNames();
            for (RexNode predicate : predicates) {
                try {
                    PhysicalExprNode nativePredicate = FlinkExpressionConverter.convertRexNode(predicate, fieldNames);
                    pruningPredicates.add(nativePredicate);
                } catch (UnsupportedOperationException e) {
                    // Skip unsupported predicates
                }
            }
        }

        // Build ParquetScanExecNode
        ParquetScanExecNode.Builder scanBuilder = ParquetScanExecNode.newBuilder()
                .setBaseConf(confBuilder.build())
                .setFsResourceId("flink-parquet-scan-" + partitionIndex)
                .addAllPruningPredicates(pruningPredicates);

        return PhysicalPlanNode.newBuilder().setParquetScan(scanBuilder.build()).build();
    }

    /**
     * Converts a projection operation to ProjectionExecNode protobuf.
     *
     * @param input The input PhysicalPlanNode (from child operator).
     * @param projections List of projection expressions.
     * @param outputFieldNames Names of output fields.
     * @param outputTypes Output field types.
     * @param inputFieldNames Names of input fields for expression conversion.
     * @return The PhysicalPlanNode containing the ProjectionExecNode.
     */
    public static PhysicalPlanNode convertProjection(
            PhysicalPlanNode input,
            List<RexNode> projections,
            List<String> outputFieldNames,
            List<LogicalType> outputTypes,
            List<String> inputFieldNames) {

        // Convert projection expressions
        List<PhysicalExprNode> nativeExprs = new ArrayList<>();
        for (RexNode projection : projections) {
            PhysicalExprNode nativeExpr = FlinkExpressionConverter.convertRexNode(projection, inputFieldNames);
            nativeExprs.add(nativeExpr);
        }

        // Convert output types
        List<ArrowType> nativeTypes = new ArrayList<>();
        for (LogicalType type : outputTypes) {
            ArrowType arrowType = FlinkTypeConverter.toArrowType(type);
            nativeTypes.add(arrowType);
        }

        // Build ProjectionExecNode
        ProjectionExecNode.Builder projectionBuilder = ProjectionExecNode.newBuilder()
                .setInput(input)
                .addAllExpr(nativeExprs)
                .addAllExprName(outputFieldNames)
                .addAllDataType(nativeTypes);

        return PhysicalPlanNode.newBuilder()
                .setProjection(projectionBuilder.build())
                .build();
    }

    /**
     * Converts a filter operation to FilterExecNode protobuf.
     *
     * @param input The input PhysicalPlanNode (from child operator).
     * @param filterConditions List of filter conditions (will be ANDed together).
     * @param inputFieldNames Names of input fields for expression conversion.
     * @return The PhysicalPlanNode containing the FilterExecNode.
     */
    public static PhysicalPlanNode convertFilter(
            PhysicalPlanNode input, List<RexNode> filterConditions, List<String> inputFieldNames) {

        // Convert filter expressions
        List<PhysicalExprNode> nativeFilters = new ArrayList<>();
        for (RexNode condition : filterConditions) {
            PhysicalExprNode nativeFilter = FlinkExpressionConverter.convertRexNode(condition, inputFieldNames);
            nativeFilters.add(nativeFilter);
        }

        // Build FilterExecNode
        FilterExecNode.Builder filterBuilder =
                FilterExecNode.newBuilder().setInput(input).addAllExpr(nativeFilters);

        return PhysicalPlanNode.newBuilder().setFilter(filterBuilder.build()).build();
    }

    /**
     * Converts a combined calc operation (filter + projection) to chained nodes.
     * This is common in Flink where a single Calc node does both filtering and projection.
     *
     * @param input The input PhysicalPlanNode.
     * @param filterConditions Optional filter conditions.
     * @param projections Projection expressions.
     * @param outputFieldNames Names of output fields.
     * @param outputTypes Output field types.
     * @param inputFieldNames Names of input fields.
     * @return The PhysicalPlanNode (Filter wrapping Projection, or just Projection).
     */
    public static PhysicalPlanNode convertCalc(
            PhysicalPlanNode input,
            List<RexNode> filterConditions,
            List<RexNode> projections,
            List<String> outputFieldNames,
            List<LogicalType> outputTypes,
            List<String> inputFieldNames) {

        PhysicalPlanNode result = input;

        // Apply filter first if present
        if (filterConditions != null && !filterConditions.isEmpty()) {
            result = convertFilter(result, filterConditions, inputFieldNames);
        }

        // Apply projection if present
        if (projections != null && !projections.isEmpty()) {
            // For projection after filter, input field names stay the same
            result = convertProjection(result, projections, outputFieldNames, outputTypes, inputFieldNames);
        }

        return result;
    }

    /**
     * Converts a Flink RowType schema to Auron Schema protobuf.
     *
     * @param rowType The Flink RowType to convert.
     * @return The Schema protobuf message.
     */
    public static Schema convertSchema(RowType rowType) {
        Schema.Builder schemaBuilder = Schema.newBuilder();

        List<String> fieldNames = rowType.getFieldNames();
        List<LogicalType> fieldTypes = rowType.getChildren();

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            LogicalType fieldType = fieldTypes.get(i);
            ArrowType arrowType = FlinkTypeConverter.toArrowType(fieldType);

            Field field = Field.newBuilder()
                    .setName(fieldName)
                    .setArrowType(arrowType)
                    .setNullable(fieldType.isNullable())
                    .build();

            schemaBuilder.addColumns(field);
        }

        return schemaBuilder.build();
    }

    /**
     * Helper to split AND-connected filter conditions.
     * Flink often represents multiple filters as a single AND expression.
     *
     * @param condition The root condition (may be AND).
     * @return List of individual conditions.
     */
    public static List<RexNode> splitAndConditions(RexNode condition) {
        List<RexNode> conditions = new ArrayList<>();
        splitAndConditionsRecursive(condition, conditions);
        return conditions;
    }

    private static void splitAndConditionsRecursive(RexNode condition, List<RexNode> result) {
        if (condition instanceof org.apache.calcite.rex.RexCall) {
            org.apache.calcite.rex.RexCall call = (org.apache.calcite.rex.RexCall) condition;
            if (call.getKind() == org.apache.calcite.sql.SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    splitAndConditionsRecursive(operand, result);
                }
                return;
            }
        }
        result.add(condition);
    }


    /**
     * Gets the file size for a given file path (supports file:// URLs).
     *
     * @param path File path (can be file:// URL or absolute path)
     * @return File size in bytes, or 0 if file cannot be accessed
     */
    private static long getFileSize(String path) {
        try {
            File file;
            if (path.startsWith("file://")) {
                // Handle file:// URL
                URI uri = new URI(path);
                file = new File(uri);
            } else {
                // Handle absolute path
                file = new File(path);
            }

            if (file.exists() && file.isFile()) {
                return file.length();
            }
        } catch (Exception e) {
            // Log warning but don't fail - return 0 and let native code handle it
            System.err.println("Warning: Could not get file size for " + path + ": " + e.getMessage());
        }
        return 0;
    }
}
