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
package org.apache.auron.flink.table.planner.processor;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.auron.flink.connector.kafka.KafkaConstants;
import org.apache.auron.flink.runtime.operator.FlinkAuronDynamicTableSource;
import org.apache.auron.flink.table.planner.FlinkAuronCalcNode;
import org.apache.auron.flink.table.planner.converter.NativePlanFusionBuilder;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ProcessorContext;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Graph-level fusion pass that merges a native Auron source and the {@code StreamExecCalc} that
 * directly consumes it into a single native {@code Project[Filter?[KafkaScan]]} plan, so the chain
 * stays columnar end-to-end and pays a single columnar&rarr;row conversion at the tail.
 *
 * <p>The pass runs over the whole {@link ExecNodeGraph} before Transformation translation. For each
 * {@code source &rarr; Calc} chain it decides whether to fuse and, when it does, stages the merged
 * Calc sub-plan on the source's {@link FlinkAuronDynamicTableSource} instance (annotate-and-skip): it
 * does not remove or re-type any node. The shadowed {@code StreamExecCalc} later observes the staged
 * plan and emits no standalone operator for that Calc. This decision is graph-level because
 * sole-consumership ("does this source feed exactly one consumer?") is a whole-graph property a single
 * node cannot see in Flink's one-directional exec graph.
 *
 * <p>Fusion is additive on top of the standalone native Calc path: a convertible Calc the pass does
 * not fuse still runs as a standalone native operator; only a non-convertible Calc falls back to
 * Flink codegen.
 *
 * <p>A Calc is fused into its source only when all hold:
 *
 * <ul>
 *   <li>the Calc's single input is a {@link StreamExecTableSourceScan} whose {@link
 *       DynamicTableSource} is a {@link FlinkAuronDynamicTableSource};
 *   <li>that scan is the sole consumer of the source (consumer count {@code == 1} &mdash;
 *       multi-consumer fusion is tracked separately);
 *   <li>the source has no event-time watermark (fusing below a watermark generator would strip
 *       per-record event-time);
 *   <li>the Calc fully converts to a native plan;
 *   <li>no projection-output or source-input column name collides with a reserved Kafka metadata
 *       column (the native engine resolves columns by name, so a collision would silently bind to the
 *       metadata column).
 * </ul>
 */
public class AuronOperatorFusionProcessor implements ExecNodeGraphProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AuronOperatorFusionProcessor.class);

    @Override
    public ExecNodeGraph process(ExecNodeGraph graph, ProcessorContext context) {
        final PlannerBase planner = context.getPlanner();
        return process(
                graph,
                scan -> scan.getTableSourceSpec()
                        .getScanTableSource(planner.getFlinkContext(), planner.getTypeFactory()),
                planner.getTableConfig());
    }

    /**
     * Runs the fusion decision over the whole graph: builds the reverse fan-out consumer count,
     * collects every {@code scan &rarr; Calc} chain, and applies {@link #tryFuse} to each. Split from
     * the {@link ProcessorContext} entry point so the full count&rarr;gate&rarr;stage path is drivable
     * with the scan's source and the table config supplied directly, without a live {@link
     * PlannerBase}.
     *
     * @param graph the exec-node graph to process
     * @param sourceResolver resolves a scan node to its {@link DynamicTableSource}
     * @param tableConfig the table config used to seed the native plan converter
     * @return the same graph instance (annotate-and-skip: only source instances are mutated)
     */
    ExecNodeGraph process(
            ExecNodeGraph graph,
            Function<StreamExecTableSourceScan, DynamicTableSource> sourceResolver,
            ReadableConfig tableConfig) {
        final Map<Integer, Integer> consumerCount = buildConsumerCount(graph);

        for (Map.Entry<ExecNode<?>, ExecNode<?>> chain :
                findSourceCalcChains(graph).entrySet()) {
            final StreamExecTableSourceScan scan = (StreamExecTableSourceScan) chain.getKey();
            final StreamExecCalc calc = (StreamExecCalc) chain.getValue();
            tryFuse(scan, calc, consumerCount, sourceResolver, tableConfig);
        }
        // Annotate-and-skip: the graph is structurally unchanged; only source instances are mutated.
        return graph;
    }

    /**
     * Builds a reverse fan-out map keyed by stable {@link ExecNode#getId()}, counting consumers per
     * distinct consuming edge with visited-node dedup across all roots. A node reachable from
     * multiple roots is counted once per distinct consumer edge, not once per traversal path, so a
     * genuinely shared source can never be miscounted as {@code count == 1}.
     *
     * @param graph the exec-node graph to traverse
     * @return a map from node id to the number of distinct consumers of that node
     */
    static Map<Integer, Integer> buildConsumerCount(ExecNodeGraph graph) {
        final Map<Integer, Integer> consumerCount = new HashMap<>();
        final Set<Integer> visited = new HashSet<>();
        // Each visited edge contributes exactly one consumer to its source. Dedup is on the visited
        // SET of nodes (so each node's input edges are walked once across all roots); the consumer
        // increment happens per input edge of every visited node, which is the per-consuming-edge
        // count the design requires.
        final Deque<ExecNode<?>> stack = new ArrayDeque<>(graph.getRootNodes());
        while (!stack.isEmpty()) {
            final ExecNode<?> node = stack.pop();
            if (!visited.add(node.getId())) {
                continue;
            }
            for (ExecEdge edge : node.getInputEdges()) {
                final ExecNode<?> src = edge.getSource();
                consumerCount.merge(src.getId(), 1, Integer::sum);
                stack.push(src);
            }
        }
        return consumerCount;
    }

    /**
     * Collects every {@code scan &rarr; Calc} chain where a {@link StreamExecTableSourceScan} is the
     * single input source of a {@link StreamExecCalc}. Keyed by the scan node so each scan maps to the
     * Calc that directly consumes it.
     *
     * @param graph the exec-node graph to traverse
     * @return a map from each consuming scan to its {@link StreamExecCalc}
     */
    private static Map<ExecNode<?>, ExecNode<?>> findSourceCalcChains(ExecNodeGraph graph) {
        final Map<ExecNode<?>, ExecNode<?>> chains = new HashMap<>();
        final Set<Integer> visited = new HashSet<>();
        final Deque<ExecNode<?>> stack = new ArrayDeque<>(graph.getRootNodes());
        while (!stack.isEmpty()) {
            final ExecNode<?> node = stack.pop();
            if (!visited.add(node.getId())) {
                continue;
            }
            if (node instanceof StreamExecCalc && node.getInputEdges().size() == 1) {
                final ExecNode<?> input = node.getInputEdges().get(0).getSource();
                if (input instanceof StreamExecTableSourceScan) {
                    chains.put(input, node);
                }
            }
            for (ExecEdge edge : node.getInputEdges()) {
                stack.push(edge.getSource());
            }
        }
        return chains;
    }

    /**
     * Applies the full fusion gate to one {@code scan &rarr; Calc} chain and, when every gate passes,
     * stages the merged Calc plan on the source instance. Any failed gate is a silent no-fusion: the
     * Calc then runs standalone native (if convertible) or Flink codegen.
     *
     * @param scan the source scan node
     * @param calc the Calc node directly consuming the scan
     * @param consumerCount the reverse fan-out map from {@link #buildConsumerCount}
     * @param sourceResolver resolves the scan to its {@link DynamicTableSource}
     * @param tableConfig the table config used to seed the native plan converter
     */
    void tryFuse(
            StreamExecTableSourceScan scan,
            StreamExecCalc calc,
            Map<Integer, Integer> consumerCount,
            Function<StreamExecTableSourceScan, DynamicTableSource> sourceResolver,
            ReadableConfig tableConfig) {
        // Sole-consumer gate: multi-consumer fusion is out of scope (tracked separately).
        if (consumerCount.getOrDefault(scan.getId(), 0) != 1) {
            return;
        }

        // Fusion needs the shadowed StreamExecCalc, which implements FlinkAuronCalcNode. If the
        // classpath resolves to Flink's stock StreamExecCalc (a shadow/exclusion misconfiguration),
        // skip fusion rather than failing the job, so fusion stays a best-effort optimization.
        if (!(calc instanceof FlinkAuronCalcNode)) {
            LOG.warn(
                    "StreamExecCalc {} is not a FlinkAuronCalcNode (Auron's shadowed StreamExecCalc is "
                            + "not on the classpath); skipping source-Calc fusion for scan {}.",
                    calc.getId(),
                    scan.getId());
            return;
        }

        final DynamicTableSource tableSource = sourceResolver.apply(scan);

        final FlinkAuronCalcNode calcNode = (FlinkAuronCalcNode) calc;
        stagePlanIfFusible(
                tableSource,
                scan.getId(),
                (RowType) calc.getInputEdges().get(0).getOutputType(),
                (RowType) calc.getOutputType(),
                calcNode.getProjection(),
                calcNode.getCondition(),
                tableConfig);
    }

    /**
     * Applies the source-dependent fusion gates (marker recognition, already-staged, watermark,
     * reserved-meta-name collision, native convertibility) and stages the merged plan on the source
     * when every gate passes. Split out from the graph-walk so the decision is unit-testable without a
     * {@link PlannerBase}: the caller resolves the {@link DynamicTableSource} and the Calc's row types
     * up front. The sole-consumer gate is applied by the caller before resolving the source.
     *
     * @param tableSource the resolved scan source (any {@link DynamicTableSource}; only an unstaged,
     *     unwatermarked {@link FlinkAuronDynamicTableSource} is a fusion target)
     * @param scanId the scan node id, used only for logging
     * @param calcInputRowType the Calc's logical input row type
     * @param calcOutputRowType the Calc's projected output row type
     * @param projection the Calc's projection expressions
     * @param condition the Calc's filter expression, or {@code null}
     * @param tableConfig the table config used to seed the native plan converter
     * @return {@code true} if a merged plan was staged on the source
     */
    static boolean stagePlanIfFusible(
            DynamicTableSource tableSource,
            int scanId,
            RowType calcInputRowType,
            RowType calcOutputRowType,
            List<RexNode> projection,
            RexNode condition,
            ReadableConfig tableConfig) {
        if (!(tableSource instanceof FlinkAuronDynamicTableSource)) {
            return false;
        }
        final FlinkAuronDynamicTableSource auronSource = (FlinkAuronDynamicTableSource) tableSource;

        // Already-staged (re-entry / safety) and watermark gates.
        if (auronSource.isMergedCalcPlanSet() || auronSource.hasWatermark()) {
            return false;
        }

        // Reserved-meta-name gate: the native engine resolves the fused plan's columns by name, so a
        // projection-output or source-input column whose name equals a reserved Kafka metadata column
        // would silently bind to that metadata column. Fall back rather than fuse on a collision.
        if (collidesWithReservedMeta(calcOutputRowType) || collidesWithReservedMeta(calcInputRowType)) {
            LOG.warn(
                    "Skipping source-Calc fusion for scan {}: a column name collides with a reserved "
                            + "Kafka metadata column; the Calc runs unfused.",
                    scanId);
            return false;
        }

        final Optional<PhysicalPlanNode> plan = NativePlanFusionBuilder.buildNativeCalcPlan(
                tableConfig, projection, condition, calcInputRowType, calcOutputRowType);
        if (!plan.isPresent()) {
            // Not convertible to native: the standalone native Calc path handles convertibility and
            // its own Flink-codegen fallback. Nothing to stage here.
            return false;
        }

        auronSource.setMergedCalcPlan(plan.get(), calcOutputRowType);
        LOG.debug("Staged merged Calc plan onto Auron source for scan {}.", scanId);
        return true;
    }

    /**
     * Reports whether any field name in {@code rowType} equals a reserved Kafka metadata column name.
     *
     * @param rowType the row type whose field names to check
     * @return {@code true} if any field name collides with a reserved metadata column
     */
    static boolean collidesWithReservedMeta(RowType rowType) {
        final List<String> fieldNames = rowType.getFieldNames();
        for (RowType.RowField metaField : KafkaConstants.KAFKA_AURON_META_FIELDS) {
            if (fieldNames.contains(metaField.getName())) {
                return true;
            }
        }
        return false;
    }
}
