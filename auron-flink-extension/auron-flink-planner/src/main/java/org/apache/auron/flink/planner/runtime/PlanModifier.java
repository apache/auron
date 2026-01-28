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
package org.apache.auron.flink.planner.runtime;

import java.util.List;
import org.apache.auron.protobuf.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Modifies Auron execution plans for distributed parallel execution.
 *
 * This class recursively traverses the plan tree and modifies operators that need
 * task-specific information (e.g., file splits for scans, output paths for sinks).
 *
 * Key operations:
 * - ParquetScan: Splits input files across tasks
 * - Projection/Filter: Recursively processes inputs
 * - Other operators: Preserved unchanged
 */
public class PlanModifier {

    private static final Logger LOG = LoggerFactory.getLogger(PlanModifier.class);

    /**
     * Entry point: Modifies a plan for a specific task in distributed execution.
     *
     * @param plan Original plan from planner (contains all files)
     * @param taskIndex This task's index (0 to N-1)
     * @param totalParallelism Total number of parallel tasks (N)
     * @return Modified plan with task-specific file assignments
     */
    public static PhysicalPlanNode modifyForTask(PhysicalPlanNode plan, int taskIndex, int totalParallelism) {

        LOG.info("Modifying plan for task {}/{}", taskIndex, totalParallelism);

        if (totalParallelism == 1) {
            // Single task gets everything - no modification needed
            LOG.debug("Single task execution, returning original plan");
            return plan;
        }

        return traverseAndModify(plan, taskIndex, totalParallelism);
    }

    /**
     * Recursively traverses and modifies the plan tree.
     */
    private static PhysicalPlanNode traverseAndModify(PhysicalPlanNode node, int taskIndex, int totalParallelism) {

        switch (node.getPhysicalPlanTypeCase()) {

                // ========== SOURCE OPERATORS ==========
            case PARQUET_SCAN:
                return modifyParquetScan(node, taskIndex, totalParallelism);

                // ========== UNARY OPERATORS ==========
            case PROJECTION:
                return modifyProjection(node, taskIndex, totalParallelism);

            case FILTER:
                return modifyFilter(node, taskIndex, totalParallelism);

            case LIMIT:
                return modifyLimit(node, taskIndex, totalParallelism);

                // ========== OTHER OPERATORS ==========
                // TODO: Add support for joins, unions, sinks in later phases

            default:
                // Leaf node or unsupported operator - return as-is
                LOG.debug("Operator type {} not modified", node.getPhysicalPlanTypeCase());
                return node;
        }
    }

    /**
     * Modifies ParquetScan: splits input files across tasks.
     */
    private static PhysicalPlanNode modifyParquetScan(PhysicalPlanNode node, int taskIndex, int totalParallelism) {

        ParquetScanExecNode scan = node.getParquetScan();
        FileScanExecConf conf = scan.getBaseConf();

        // Extract all files from the original plan
        List<PartitionedFile> allFiles = conf.getFileGroup().getFilesList();

        LOG.info("ParquetScan: Splitting {} files across {} tasks", allFiles.size(), totalParallelism);

        // Split files for this task
        List<PartitionedFile> taskFiles = FileSplitter.splitFiles(allFiles, taskIndex, totalParallelism);

        LOG.info("Task {}/{}: Assigned {} files", taskIndex, totalParallelism, taskFiles.size());

        // Rebuild FileGroup with task's files only
        FileGroup taskFileGroup = FileGroup.newBuilder().addAllFiles(taskFiles).build();

        // Update configuration with task-specific info
        FileScanExecConf taskConf = conf.toBuilder()
                .setFileGroup(taskFileGroup)
                .setNumPartitions(totalParallelism)
                .setPartitionIndex(taskIndex)
                .build();

        // Create unique resource ID for this task's FileSystem
        String originalResourceId = scan.getFsResourceId();
        String taskResourceId = String.format("%s-task-%d", originalResourceId, taskIndex);

        LOG.debug("Updated FileSystem resource ID: {} -> {}", originalResourceId, taskResourceId);

        // Rebuild scan node with modifications
        ParquetScanExecNode taskScan = scan.toBuilder()
                .setBaseConf(taskConf)
                .setFsResourceId(taskResourceId)
                .build();

        return PhysicalPlanNode.newBuilder().setParquetScan(taskScan).build();
    }

    /**
     * Modifies Projection: recursively processes input.
     */
    private static PhysicalPlanNode modifyProjection(PhysicalPlanNode node, int taskIndex, int totalParallelism) {

        ProjectionExecNode projection = node.getProjection();

        // Recursively modify input
        PhysicalPlanNode modifiedInput = traverseAndModify(projection.getInput(), taskIndex, totalParallelism);

        // Rebuild with modified input (expressions unchanged)
        ProjectionExecNode modifiedProjection =
                projection.toBuilder().setInput(modifiedInput).build();

        return PhysicalPlanNode.newBuilder().setProjection(modifiedProjection).build();
    }

    /**
     * Modifies Filter: recursively processes input.
     */
    private static PhysicalPlanNode modifyFilter(PhysicalPlanNode node, int taskIndex, int totalParallelism) {

        FilterExecNode filter = node.getFilter();

        // Recursively modify input
        PhysicalPlanNode modifiedInput = traverseAndModify(filter.getInput(), taskIndex, totalParallelism);

        // Rebuild with modified input (filter expressions unchanged)
        FilterExecNode modifiedFilter =
                filter.toBuilder().setInput(modifiedInput).build();

        return PhysicalPlanNode.newBuilder().setFilter(modifiedFilter).build();
    }

    /**
     * Modifies Limit: recursively processes input.
     */
    private static PhysicalPlanNode modifyLimit(PhysicalPlanNode node, int taskIndex, int totalParallelism) {

        LimitExecNode limit = node.getLimit();

        // Recursively modify input
        PhysicalPlanNode modifiedInput = traverseAndModify(limit.getInput(), taskIndex, totalParallelism);

        // Rebuild with modified input (limit value unchanged)
        LimitExecNode modifiedLimit = limit.toBuilder().setInput(modifiedInput).build();

        return PhysicalPlanNode.newBuilder().setLimit(modifiedLimit).build();
    }
}
