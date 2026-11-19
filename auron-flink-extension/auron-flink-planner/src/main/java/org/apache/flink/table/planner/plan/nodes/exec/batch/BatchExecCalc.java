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
package org.apache.flink.table.planner.plan.nodes.exec.batch;

import java.security.CodeSource;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.auron.flink.runtime.operator.FlinkAuronCalcOperator;
import org.apache.auron.flink.table.planner.FlinkAuronCalcNode;
import org.apache.auron.flink.table.planner.FlinkAuronExecNode;
import org.apache.auron.flink.table.planner.converter.NativePlanFusionBuilder;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shadows Flink's stock {@code BatchExecCalc} (via FQCN resolution, same mechanism as {@link
 * org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc}) to convert Calc into a
 * native {@link PhysicalPlanNode} via {@link NativePlanFusionBuilder}, falling back to Flink's
 * codegen Calc when conversion fails and fallback is enabled.
 *
 * <p>No graph-level source fusion or operator fusion codegen yet (see {@link
 * #supportFusionCodegen()}); no {@code @ExecNodeMetadata}/{@code @JsonCreator}, matching stock
 * batch {@code BatchExecCalc} at this Flink version (compiled-plan restoration is streaming-only).
 */
public class BatchExecCalc extends CommonExecCalc
        implements BatchExecNode<RowData>, FlinkAuronExecNode, FlinkAuronCalcNode {

    private static final Logger LOG = LoggerFactory.getLogger(BatchExecCalc.class);

    /** One-shot guard so the activation log is emitted only once per JVM. */
    private static final AtomicBoolean ACTIVATION_LOGGED = new AtomicBoolean(false);

    /** Matches Flink stock {@code BatchExecCalc}'s constructor signature for reflective instantiation. */
    public BatchExecCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecCalc.class),
                ExecNodeContext.newPersistedConfig(BatchExecCalc.class, tableConfig),
                projection,
                condition,
                TableStreamOperator.class,
                false,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
        logActivationOnce();
        final Transformation<RowData> upstream =
                (Transformation<RowData>) getInputEdges().get(0).translateToPlan(planner);
        final RowType inputRowType = (RowType) getInputEdges().get(0).getOutputType();
        final RowType outputRowType = (RowType) getOutputType();

        final Optional<PhysicalPlanNode> plan =
                NativePlanFusionBuilder.buildNativeCalcPlan(config, projection, condition, inputRowType, outputRowType);

        if (!plan.isPresent()) {
            final boolean fallbackEnabled = AuronAdaptor.getInstance()
                    .getAuronConfiguration()
                    .get(FlinkAuronConfiguration.FAIL_BACK_FLINK_ENGINE_ENABLED);
            if (fallbackEnabled) {
                LOG.debug("Falling back to Flink's CodeGen Calc for node {}", getId());
                return translateToFlinkCalc(planner, config);
            }
            throw new IllegalStateException(
                    "Auron Calc conversion failed for node " + getId() + " and fallback is disabled");
        }

        final FlinkAuronCalcOperator operator =
                new FlinkAuronCalcOperator(plan.get(), inputRowType, outputRowType, "FlinkAuronCalc-" + getId());

        return ExecNodeUtil.createOneInputTransformation(
                upstream,
                createTransformationMeta(CALC_TRANSFORMATION, config),
                SimpleOperatorFactory.of(operator),
                InternalTypeInfo.of(outputRowType),
                upstream.getParallelism(),
                0L,
                false);
    }

    /**
     * Indirection over {@code super.translateToPlanInternal} so tests can stub the Flink fallback
     * path without running full code generation.
     */
    protected Transformation<RowData> translateToFlinkCalc(PlannerBase planner, ExecNodeConfig config) {
        return super.translateToPlanInternal(planner, config);
    }

    /**
     * No fusion codegen for this shadow yet: native execution already replaces per-record codegen,
     * so this returns {@code false} rather than reaching the throwing fusion-codegen path below.
     */
    @Override
    public boolean supportFusionCodegen() {
        return false;
    }

    @Override
    protected OpFusionCodegenSpecGenerator translateToFusionCodegenSpecInternal(
            PlannerBase planner, ExecNodeConfig config) {
        throw new UnsupportedOperationException(
                "BatchExecCalc (Auron shadow) does not support operator fusion codegen; "
                        + "supportFusionCodegen() returns false so this method must not be reached.");
    }

    /** Projection expressions, exposed for {@link FlinkAuronCalcNode} conformance. */
    @Override
    public List<RexNode> getProjection() {
        return projection;
    }

    /** Filter expression, or {@code null} if none, exposed for {@link FlinkAuronCalcNode} conformance. */
    @Override
    @Nullable
    public RexNode getCondition() {
        return condition;
    }

    /** Logs shadow activation once per JVM, with code source, to help diagnose classpath ordering. */
    private static void logActivationOnce() {
        if (ACTIVATION_LOGGED.compareAndSet(false, true)) {
            final CodeSource cs = BatchExecCalc.class.getProtectionDomain().getCodeSource();
            LOG.info(
                    "Auron BatchExecCalc shadow active (loaded from {}).",
                    cs != null ? cs.getLocation() : "<unknown source>");
        }
    }
}
