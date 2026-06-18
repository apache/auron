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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.auron.flink.runtime.operator.FlinkAuronDynamicTableSource;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecTableSourceScan;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link AuronSourceCalcFusionProcessor}.
 *
 * <p>Two seams are covered directly: the reverse fan-out consumer count (a pure graph walk) and the
 * source-dependent staging decision {@code stagePlanIfFusible} (resolved-source + row-type inputs, no
 * {@link PlannerBase} needed). Each staging test asserts the OUTCOME — whether the source's {@link
 * FlinkAuronDynamicTableSource#isMergedCalcPlanSet()} flips — rather than mocking the decision, so a
 * regression in any gate is caught. The full graph-walk + planner resolution path is exercised
 * end-to-end by the Kafka source IT.
 */
class AuronSourceCalcFusionProcessorTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private static final RowType TWO_INT_INPUT =
            RowType.of(new LogicalType[] {new IntType(), new IntType()}, new String[] {"f0", "f1"});
    private static final RowType TWO_INT_OUTPUT =
            RowType.of(new LogicalType[] {new IntType(), new IntType()}, new String[] {"a", "b"});

    private TableConfig tableConfig;

    @BeforeEach
    void setUp() {
        ExecNodeContext.resetIdCounter();
        tableConfig = TableConfig.getDefault();
        EDGES.clear();
    }

    // =====================================================================
    // Consumer counting (reverse fan-out)
    // =====================================================================

    /** Contract: a source feeding exactly one Calc has consumer count 1 (fusible). */
    @Test
    void testSoleConsumerCountedOnce() {
        FakeExecNode source = new FakeExecNode(TWO_INT_INPUT, "src", Collections.emptyList());
        StreamExecCalc calc = newCalc(identityProjection(), null, TWO_INT_OUTPUT);
        edge(source, calc);
        ExecNodeGraph graph = new ExecNodeGraph(Collections.singletonList(calc));

        Map<Integer, Integer> counts = AuronSourceCalcFusionProcessor.buildConsumerCount(graph);

        assertEquals(1, counts.get(source.getId()));
    }

    /** Contract: a source shared by two Calcs (two distinct consumer edges) has count 2, so the
     * sole-consumer gate excludes it from fusion. Dedup must not collapse this to 1. */
    @Test
    void testSharedSourceCountedPerConsumer() {
        FakeExecNode source = new FakeExecNode(TWO_INT_INPUT, "src", Collections.emptyList());
        StreamExecCalc calcA = newCalc(identityProjection(), null, TWO_INT_OUTPUT);
        StreamExecCalc calcB = newCalc(identityProjection(), null, TWO_INT_OUTPUT);
        edge(source, calcA);
        edge(source, calcB);
        ExecNodeGraph graph = new ExecNodeGraph(Arrays.asList(calcA, calcB));

        Map<Integer, Integer> counts = AuronSourceCalcFusionProcessor.buildConsumerCount(graph);

        assertEquals(2, counts.get(source.getId()), "A source feeding two Calcs must count 2 consumers");
    }

    /** Contract: a linear chain source -> calc1 -> calc2 counts each node once even when reachable
     * via a single root, and the source still has exactly one consumer. */
    @Test
    void testChainCountsEachNodeOnce() {
        FakeExecNode source = new FakeExecNode(TWO_INT_INPUT, "src", Collections.emptyList());
        StreamExecCalc calc1 = newCalc(identityProjection(), null, TWO_INT_OUTPUT);
        StreamExecCalc calc2 = newCalc(identityProjection(), null, TWO_INT_OUTPUT);
        edge(source, calc1);
        edge(calc1, calc2);
        ExecNodeGraph graph = new ExecNodeGraph(Collections.singletonList(calc2));

        Map<Integer, Integer> counts = AuronSourceCalcFusionProcessor.buildConsumerCount(graph);

        assertEquals(1, counts.get(source.getId()));
        assertEquals(1, counts.get(calc1.getId()));
    }

    // =====================================================================
    // Full decision path through process(...)
    //
    // These drive the real graph walk (consumer count -> sole-consumer gate -> tryFuse ->
    // stagePlanIfFusible) via the package-private process(graph, sourceResolver, tableConfig)
    // overload. The in-module StreamExecCalc shadow is the only copy on the test classpath (no
    // assembly JAR in front), so these construct the nodes directly and drive process() rather than
    // routing through the planner SPI. The resolver stands in for the planner's scan->source
    // resolution; everything from the consumer count down is the production code path.
    // =====================================================================

    /** Contract: a native source feeding exactly one Calc (count == 1) fuses through the real
     * process() entry point — the source carries the staged merged plan afterward. */
    @Test
    void testProcessFusesSoleConsumerSource() {
        FakeAuronSource source = new FakeAuronSource(false);
        StreamExecTableSourceScan scan = newScan();
        StreamExecCalc calc = newCalc(identityProjection(), null, TWO_INT_OUTPUT);
        edge(scan, calc);
        ExecNodeGraph graph = new ExecNodeGraph(Collections.singletonList(calc));

        new AuronSourceCalcFusionProcessor().process(graph, s -> source, tableConfig);

        assertTrue(source.isMergedCalcPlanSet(), "A sole-consumer native source must fuse through process()");
    }

    /** Contract: a native source SHARED by two Calcs (count == 2) is never staged through the real
     * process() entry point — neither Calc fuses. This is the corruption-prevention guard: fusing a
     * shared source would let one Calc's projection silently rewrite rows the other Calc reads. */
    @Test
    void testProcessDoesNotFuseSharedSource() {
        FakeAuronSource source = new FakeAuronSource(false);
        StreamExecTableSourceScan scan = newScan();
        StreamExecCalc calcA = newCalc(identityProjection(), null, TWO_INT_OUTPUT);
        StreamExecCalc calcB = newCalc(identityProjection(), null, TWO_INT_OUTPUT);
        edge(scan, calcA);
        edge(scan, calcB);
        ExecNodeGraph graph = new ExecNodeGraph(Arrays.asList(calcA, calcB));

        new AuronSourceCalcFusionProcessor().process(graph, s -> source, tableConfig);

        assertFalse(
                source.isMergedCalcPlanSet(),
                "A source shared by two Calcs (count == 2) must never be staged through process()");
    }

    // =====================================================================
    // Staging decision (stagePlanIfFusible)
    // =====================================================================

    /** Contract: a native source, no watermark, unstaged, convertible Calc, no reserved-name
     * collision → the merged plan is staged on the source. */
    @Test
    void testFusesWhenAllGatesPass() {
        FakeAuronSource source = new FakeAuronSource(false);

        boolean staged = stage(source, TWO_INT_INPUT, TWO_INT_OUTPUT, identityProjection(), null);

        assertTrue(staged, "All gates pass → plan must be staged");
        assertTrue(source.isMergedCalcPlanSet());
    }

    /** Contract: a non-Auron source is not recognized as a fusion target. */
    @Test
    void testNonAuronSourceNotStaged() {
        FakeNonAuronSource source = new FakeNonAuronSource();

        boolean staged = AuronSourceCalcFusionProcessor.stagePlanIfFusible(
                source, 1, TWO_INT_INPUT, TWO_INT_OUTPUT, identityProjection(), null, tableConfig);

        assertFalse(staged, "A non-Auron source must never be a fusion target");
    }

    /** Contract: a watermarked source is gated out at plan time (event-time correctness). */
    @Test
    void testWatermarkedSourceNotStaged() {
        FakeAuronSource source = new FakeAuronSource(true);

        boolean staged = stage(source, TWO_INT_INPUT, TWO_INT_OUTPUT, identityProjection(), null);

        assertFalse(staged, "A watermarked source must not fuse");
        assertFalse(source.isMergedCalcPlanSet());
    }

    /** Contract: a source that already carries a staged plan is skipped (no overwrite). */
    @Test
    void testAlreadyStagedSourceNotRestaged() {
        FakeAuronSource source = new FakeAuronSource(false);
        source.setMergedCalcPlan(PhysicalPlanNode.getDefaultInstance(), TWO_INT_OUTPUT);

        boolean staged = stage(source, TWO_INT_INPUT, TWO_INT_OUTPUT, identityProjection(), null);

        assertFalse(staged, "An already-staged source must not be re-staged");
    }

    /** Contract: a Calc with an unsupported RexNode does not convert, so nothing is staged (the
     * standalone native Calc path handles convertibility). */
    @Test
    void testNonConvertibleCalcNotStaged() {
        FakeAuronSource source = new FakeAuronSource(false);
        // SIMILAR_TO produces a RexCall that RexCallConverter.isSupported rejects.
        RexNode unsupported = REX_BUILDER.makeCall(
                TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN),
                SqlStdOperatorTable.SIMILAR_TO,
                Arrays.asList(intRef(0), intRef(1)));

        boolean staged = stage(
                source, TWO_INT_INPUT, RowType.of(new BigIntType()), Collections.singletonList(intRef(0)), unsupported);

        assertFalse(staged, "A non-convertible Calc must not fuse");
        assertFalse(source.isMergedCalcPlanSet());
    }

    /** Contract: a projection OUTPUT field named like a reserved Kafka metadata column blocks fusion
     * (the native engine resolves by name and would silently bind to the metadata column). */
    @Test
    void testReservedOutputNameCollisionNotStaged() {
        FakeAuronSource source = new FakeAuronSource(false);
        RowType collidingOutput =
                RowType.of(new LogicalType[] {new IntType()}, new String[] {"serialized_kafka_records_offset"});

        boolean staged = stage(source, TWO_INT_INPUT, collidingOutput, Collections.singletonList(intRef(0)), null);

        assertFalse(staged, "An output name colliding with a metadata column must block fusion");
    }

    /** Contract: a source INPUT field named like a reserved Kafka metadata column also blocks
     * fusion. */
    @Test
    void testReservedInputNameCollisionNotStaged() {
        FakeAuronSource source = new FakeAuronSource(false);
        RowType collidingInput = RowType.of(
                new LogicalType[] {new IntType(), new IntType()},
                new String[] {"serialized_kafka_records_partition", "f1"});

        boolean staged = stage(source, collidingInput, TWO_INT_OUTPUT, identityProjection(), null);

        assertFalse(staged, "An input name colliding with a metadata column must block fusion");
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private boolean stage(
            FlinkAuronDynamicTableSource source,
            RowType input,
            RowType output,
            List<RexNode> projection,
            RexNode condition) {
        return AuronSourceCalcFusionProcessor.stagePlanIfFusible(
                (DynamicTableSource) source, 1, input, output, projection, condition, tableConfig);
    }

    private static List<RexNode> identityProjection() {
        return Arrays.asList(intRef(0), intRef(1));
    }

    private static RexNode intRef(int idx) {
        return REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), idx);
    }

    private StreamExecCalc newCalc(List<RexNode> projection, RexNode condition, RowType outputType) {
        return new StreamExecCalc(tableConfig, projection, condition, InputProperty.DEFAULT, outputType, "calc");
    }

    /**
     * A real {@link StreamExecTableSourceScan} graph node so {@code findSourceCalcChains} recognizes
     * the chain and {@code tryFuse}'s scan cast holds. The source spec is null because the test's
     * source resolver returns the fusion target directly; the constructor only stores the spec and
     * the resolver never reads it back from the scan.
     */
    private StreamExecTableSourceScan newScan() {
        StreamExecTableSourceScan scan = new StreamExecTableSourceScan(
                ExecNodeContext.newNodeId(), new ExecNodeContext("scan_1"), tableConfig, null, TWO_INT_INPUT, "scan");
        // A leaf scan has no input edges; the graph walk reads getInputEdges() on every node.
        scan.setInputEdges(Collections.emptyList());
        return scan;
    }

    private static void edge(ExecNode<?> source, ExecNode<?> target) {
        ExecEdge e = ExecEdge.builder().source(source).target(target).build();
        List<ExecEdge> merged = new java.util.ArrayList<>(EDGES.getOrDefault(target, Collections.emptyList()));
        merged.add(e);
        EDGES.put(target, merged);
        ((ExecNodeBase<?>) target).setInputEdges(merged);
    }

    private static final Map<ExecNode<?>, List<ExecEdge>> EDGES = new java.util.IdentityHashMap<>();

    /** Minimal {@link ExecNodeBase} used as a graph leaf / intermediate node in fan-out tests. */
    static class FakeExecNode extends ExecNodeBase<RowData> implements StreamExecNode<RowData> {
        FakeExecNode(LogicalType outputType, String description, List<InputProperty> inputProperties) {
            super(
                    ExecNodeContext.newNodeId(),
                    new ExecNodeContext("fake_1"),
                    new Configuration(),
                    inputProperties,
                    outputType,
                    description);
            // A leaf source has no input edges; the graph walk reads getInputEdges() on every node.
            setInputEdges(Collections.emptyList());
        }

        @Override
        protected Transformation<RowData> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
            throw new UnsupportedOperationException();
        }
    }

    /** A native Auron source stub that also satisfies {@link ScanTableSource}. */
    static class FakeAuronSource implements ScanTableSource, FlinkAuronDynamicTableSource {
        private final boolean hasWatermark;
        private PhysicalPlanNode mergedPlan;

        FakeAuronSource(boolean hasWatermark) {
            this.hasWatermark = hasWatermark;
        }

        @Override
        public void setMergedCalcPlan(PhysicalPlanNode logicalCalcSubPlan, RowType projectedOutputType) {
            this.mergedPlan = logicalCalcSubPlan;
        }

        @Override
        public boolean isMergedCalcPlanSet() {
            return mergedPlan != null;
        }

        @Override
        public boolean hasWatermark() {
            return hasWatermark;
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DynamicTableSource copy() {
            return this;
        }

        @Override
        public String asSummaryString() {
            return "fake-auron-source";
        }
    }

    /** A non-Auron source: a plain {@link ScanTableSource} with no fusion marker. */
    static class FakeNonAuronSource implements ScanTableSource {
        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DynamicTableSource copy() {
            return this;
        }

        @Override
        public String asSummaryString() {
            return "fake-non-auron-source";
        }
    }
}
