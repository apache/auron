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

import org.apache.auron.flink.planner.execution.AuronBatchExecutionWrapperOperator;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Flink transformations that execute Auron native plans.
 *
 * <p>This factory bridges between Auron's protobuf plan representation and Flink's transformation
 * API. It creates appropriate Flink transformations that wrap Auron's native execution operators.
 *
 * <p>The factory handles:
 *
 * <ul>
 *   <li>Creating source transformations for Auron scans
 *   <li>Configuring parallelism and resource requirements
 *   <li>Setting up proper data types and schemas
 *   <li>Integrating with Flink's execution environment
 * </ul>
 */
public class AuronTransformationFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AuronTransformationFactory.class);

    /**
     * Creates a Flink transformation that executes an Auron native plan.
     *
     * <p>This method is called by {@link
     * org.apache.flink.table.planner.plan.nodes.exec.batch.AuronBatchExecNode} during plan
     * translation.
     *
     * @param auronPlan The Auron PhysicalPlanNode (protobuf) to execute
     * @param outputSchema The output schema of the transformation
     * @param planner The Flink planner base for accessing environment and configuration
     * @return A Flink transformation that executes the Auron plan
     */
    public static Transformation<RowData> createTransformation(
            Object auronPlan, RowType outputSchema, PlannerBase planner) {

        if (!(auronPlan instanceof PhysicalPlanNode)) {
            throw new IllegalArgumentException("Expected PhysicalPlanNode but got: "
                    + (auronPlan != null ? auronPlan.getClass().getName() : "null"));
        }

        PhysicalPlanNode nativePlan = (PhysicalPlanNode) auronPlan;

        LOG.info("Creating Auron transformation for plan type: {}", nativePlan.getPhysicalPlanTypeCase());

        // Get the execution environment
        StreamExecutionEnvironment env = planner.getExecEnv();
        Configuration config = planner.getTableConfig().getConfiguration();

        // Create the Auron batch execution wrapper operator
        // This operator will execute the native plan and produce RowData results
        AuronBatchExecutionWrapperOperator sourceFunction = new AuronBatchExecutionWrapperOperator(
                nativePlan,
                outputSchema,
                0, // partitionId - will be set by Flink runtime
                1 // stageId - incrementing stage ID for Auron metrics
                );

        // Create a source transformation
        // Note: For MVP, we use the old SourceFunction API
        // Future versions should migrate to the new Source API (FLIP-27)
        DataStream<RowData> dataStream = env.addSource(sourceFunction).name("AuronNativeScan");

        // Get the transformation from the data stream
        Transformation<RowData> transformation = dataStream.getTransformation();

        // Set parallelism - default to the environment's parallelism
        int parallelism = env.getParallelism();
        transformation.setParallelism(parallelism);

        // Set additional metadata
        transformation.setName("Auron Native Execution");
        transformation.setDescription("Auron native vectorized execution: " + nativePlan.getPhysicalPlanTypeCase());

        LOG.info(
                "Created Auron transformation with parallelism {} for plan: {}",
                parallelism,
                nativePlan.getPhysicalPlanTypeCase());

        return transformation;
    }

    /**
     * Alternative method for creating transformations with explicit parallelism.
     *
     * @param auronPlan The Auron PhysicalPlanNode (protobuf) to execute
     * @param outputSchema The output schema of the transformation
     * @param planner The Flink planner base
     * @param parallelism Explicit parallelism setting
     * @return A Flink transformation that executes the Auron plan
     */
    public static Transformation<RowData> createTransformation(
            Object auronPlan, RowType outputSchema, PlannerBase planner, int parallelism) {

        Transformation<RowData> transformation = createTransformation(auronPlan, outputSchema, planner);
        transformation.setParallelism(parallelism);

        LOG.info("Set explicit parallelism {} for Auron transformation", parallelism);

        return transformation;
    }
}
