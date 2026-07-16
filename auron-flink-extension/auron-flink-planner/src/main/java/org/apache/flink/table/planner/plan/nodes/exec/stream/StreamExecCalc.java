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
package org.apache.flink.table.planner.plan.nodes.exec.stream;

import java.security.CodeSource;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.auron.flink.runtime.operator.FlinkAuronCalcOperator;
import org.apache.auron.flink.runtime.operator.FlinkAuronDynamicTableSource;
import org.apache.auron.flink.table.planner.FlinkAuronCalcNode;
import org.apache.auron.flink.table.planner.FlinkAuronExecNode;
import org.apache.auron.flink.table.planner.converter.NativePlanFusionBuilder;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Shadows Flink's stock {@code StreamExecCalc} via fully-qualified-class-name resolution. Java's
 * classloader resolves one class per FQCN; with {@code auron-flink-planner} ahead of {@code
 * flink-table-planner} on the classpath, Flink's planner constructs this class whenever it builds
 * a Calc {@code ExecNode}.
 *
 * <p>{@link #translateToPlanInternal(PlannerBase, ExecNodeConfig)} runs native when the Calc is
 * convertible — either fused into its source or as a standalone native operator — and falls back to
 * Flink's codegen Calc only when the Calc is not convertible. When the graph-level fusion pass has
 * already staged a merged plan on this Calc's native source, this node emits no standalone operator:
 * it re-types the upstream {@link Transformation} to the projected output and returns it (the source
 * runs the fused {@code Project[Filter?[KafkaScan]]} plan). Otherwise it attempts to translate the
 * projection + condition into a native {@link PhysicalPlanNode} using the converter framework; on
 * success it returns a {@link Transformation} wrapping a {@link FlinkAuronCalcOperator}; on failure
 * it either delegates to {@code super.translateToPlanInternal} (when
 * {@link FlinkAuronConfiguration#FAIL_BACK_FLINK_ENGINE_ENABLED} is {@code true}, the default) or
 * throws {@link IllegalStateException}.
 *
 * <p>Activation observability: the first time {@link #translateToPlanInternal} is invoked per JVM,
 * an INFO log line {@code "Auron StreamExecCalc shadow active"} is emitted with the class's code
 * source. Absence of this log under SQL load indicates that {@code auron-flink-planner} is not
 * classpath-ordered ahead of {@code flink-table-planner} and Flink's stock {@code StreamExecCalc}
 * is being resolved instead — Auron's Calc rewriter is then silently inactive. Setting
 * {@link FlinkAuronConfiguration#FAIL_BACK_FLINK_ENGINE_ENABLED} to {@code false} provides a
 * stricter complementary signal: per-Calc conversion failures throw rather than fall back.
 *
 * <p>Per-fallback observability: the first time a given unsupported {@link RexNode} class is seen
 * (per JVM), a WARN log line is emitted naming the unsupported {@link RexNode} class so users can
 * grep for missing converter coverage. Subsequent fallbacks on the same {@link RexNode}
 * class are silent to avoid log spam.
 */
@ExecNodeMetadata(
        name = "stream-exec-calc",
        version = 1,
        minPlanVersion = FlinkVersion.v1_15,
        minStateVersion = FlinkVersion.v1_15)
public class StreamExecCalc extends CommonExecCalc
        implements StreamExecNode<RowData>, FlinkAuronExecNode, FlinkAuronCalcNode {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExecCalc.class);

    /**
     * One-shot guard for the activation INFO log so a busy planner doesn't repeat the line per
     * Calc submission.
     */
    private static final AtomicBoolean ACTIVATION_LOGGED = new AtomicBoolean(false);

    /**
     * Constructs a stream Calc node. Matches Flink's stock {@code StreamExecCalc} primary
     * constructor signature so the planner's reflective instantiation finds this shadowed class
     * without changes.
     *
     * @param tableConfig table-level configuration to persist into the node context
     * @param projection projection expressions evaluated per row
     * @param condition filter expression evaluated per row, or {@code null} for no filter
     * @param inputProperty input edge property for the single upstream operator
     * @param outputType output {@link RowType} of this node
     * @param description human-readable description used for logging and metrics
     */
    public StreamExecCalc(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecCalc.class),
                ExecNodeContext.newPersistedConfig(StreamExecCalc.class, tableConfig),
                projection,
                condition,
                TableStreamOperator.class,
                true,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    /**
     * JSON deserialization constructor used for compiled plan restoration. Matches Flink's stock
     * {@code StreamExecCalc} {@code @JsonCreator} constructor signature.
     *
     * @param id pre-assigned node ID
     * @param context restored node context
     * @param persistedConfig configuration persisted with the compiled plan
     * @param projection projection expressions evaluated per row
     * @param condition filter expression evaluated per row, or {@code null} for no filter
     * @param inputProperties input edge properties for upstream operators
     * @param outputType output {@link RowType} of this node
     * @param description human-readable description used for logging and metrics
     */
    @JsonCreator
    public StreamExecCalc(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_PROJECTION) List<RexNode> projection,
            @Nullable @JsonProperty(FIELD_NAME_CONDITION) RexNode condition,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                projection,
                condition,
                TableStreamOperator.class,
                true,
                inputProperties,
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

        if (fusedIntoSource(planner)) {
            // The graph-level fusion pass already staged this Calc's Project[Filter?] plan on the
            // native source, which now runs the fused Project[Filter?[KafkaScan]] and emits the
            // projected rows. Emit no standalone Calc operator: re-type the upstream Transformation
            // to the projected output and return it. Re-typing the shared upstream is safe only
            // because the processor fuses exclusively into a sole-consumer source (no other consumer
            // locked its output type); that single-consumer invariant is enforced graph-level by the
            // fusion processor before it stages the plan.
            upstream.setOutputType(InternalTypeInfo.of(outputRowType));
            return upstream;
        }

        final Optional<PhysicalPlanNode> plan = tryBuildAuronPlan(inputRowType, outputRowType);

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
     * Indirection over {@code super.translateToPlanInternal} so tests can stub the Flink
     * codegen-Calc fallback path without running Flink's full code-generation machinery. Production
     * delegates straight through.
     *
     * @param planner the planner forwarded from {@link #translateToPlanInternal}
     * @param config the exec-node config forwarded from {@link #translateToPlanInternal}
     * @return Flink's stock {@link Transformation} produced by {@code CommonExecCalc}
     */
    protected Transformation<RowData> translateToFlinkCalc(PlannerBase planner, ExecNodeConfig config) {
        return super.translateToPlanInternal(planner, config);
    }

    /**
     * Reports whether this Calc's native source already carries a merged Calc plan staged by the
     * graph-level fusion pass. When it does, the source runs the fused {@code
     * Project[Filter?[KafkaScan]]} plan and this Calc must emit no standalone operator.
     *
     * <p>Resolves the single input edge's source {@link ExecNode}; if it is a {@link
     * StreamExecTableSourceScan} whose {@link DynamicTableSource} is a {@link
     * FlinkAuronDynamicTableSource} with a staged plan, fusion has been decided graph-level. The
     * source instance is the lazily-cached one the scan's own translation reuses, so reading its
     * staged-plan flag here reflects the processor's decision.
     *
     * @param planner the planner forwarded from {@link #translateToPlanInternal}, used to resolve
     *     the scan's {@link DynamicTableSource}
     * @return {@code true} if the source carries a staged merged plan
     */
    private boolean fusedIntoSource(PlannerBase planner) {
        final ExecNode<?> source = getInputEdges().get(0).getSource();
        if (!(source instanceof StreamExecTableSourceScan)) {
            return false;
        }
        final DynamicTableSource tableSource = ((StreamExecTableSourceScan) source)
                .getTableSourceSpec()
                .getScanTableSource(planner.getFlinkContext(), planner.getTypeFactory());
        return tableSource instanceof FlinkAuronDynamicTableSource
                && ((FlinkAuronDynamicTableSource) tableSource).isMergedCalcPlanSet();
    }

    /**
     * Attempts to compose a native {@code Project[Filter?[FFIReader-placeholder]]} plan from this
     * node's projection and condition, delegating to {@link NativePlanFusionBuilder}. Returns {@link
     * Optional#empty()} if any {@link RexNode} is unsupported by the converter framework, or if plan
     * composition throws — both signals are the same for the caller: fall back to Flink's codegen
     * Calc.
     *
     * @param inputRowType the upstream row type used by the converter context
     * @param outputRowType the row type of this Calc's output
     * @return a composed plan, or empty if conversion failed
     */
    private Optional<PhysicalPlanNode> tryBuildAuronPlan(RowType inputRowType, RowType outputRowType) {
        return NativePlanFusionBuilder.buildNativeCalcPlan(
                getPersistedConfig(), projection, condition, inputRowType, outputRowType);
    }

    /**
     * The projection expressions this Calc evaluates per row. Exposed so the graph-level fusion pass
     * (in a different package) can build the merged native plan from the same projection this node
     * would otherwise convert standalone.
     *
     * @return the projection {@link RexNode}s
     */
    @Override
    public List<RexNode> getProjection() {
        return projection;
    }

    /**
     * The filter expression this Calc evaluates per row, or {@code null} when there is no filter.
     * Exposed so the graph-level fusion pass (in a different package) can build the merged native
     * plan from the same condition this node would otherwise convert standalone.
     *
     * @return the filter {@link RexNode}, or {@code null}
     */
    @Override
    @Nullable
    public RexNode getCondition() {
        return condition;
    }

    /**
     * Emits the activation INFO log on the first call per JVM. Subsequent calls are no-ops. The
     * code-source location is included to help diagnose classpath-ordering mistakes — if this log
     * appears but points at an unexpected JAR, that JAR is what Flink resolved as
     * {@code StreamExecCalc}.
     */
    private static void logActivationOnce() {
        if (ACTIVATION_LOGGED.compareAndSet(false, true)) {
            final CodeSource cs = StreamExecCalc.class.getProtectionDomain().getCodeSource();
            LOG.info(
                    "Auron StreamExecCalc shadow active (loaded from {}).",
                    cs != null ? cs.getLocation() : "<unknown source>");
        }
    }
}
