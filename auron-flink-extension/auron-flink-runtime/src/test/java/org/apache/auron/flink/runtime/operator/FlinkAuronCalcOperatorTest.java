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
package org.apache.auron.flink.runtime.operator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.auron.flink.arrow.FlinkArrowFFIExporter;
import org.apache.auron.jni.JniBridge;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.FilterExecNode;
import org.apache.auron.protobuf.LimitExecNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.apache.auron.protobuf.Schema;
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FlinkAuronCalcOperator}.
 *
 * <p>All tests use the {@code NativeRuntime} mock seam — no real native engine invocation, no
 * {@code libauron} load, no real {@code JniBridge.callNative}. The Flink runtime context is
 * stubbed via a test subclass that overrides {@link FlinkAuronCalcOperator#getRuntimeContext()}
 * and {@link FlinkAuronCalcOperator#getMetricGroup()}; the inherited protected {@code output}
 * field is set through reflection.
 *
 * <p>Contracts verified by this class:
 *
 * <ol>
 *   <li>{@code open()} must register the exporter into {@link JniBridge} resources under a
 *       runtime-bound resource ID.
 *   <li>The resource ID must encode the Flink operator unique ID and subtask index so
 *       registrations from concurrent subtasks of the same operator never collide.
 *   <li>{@code processElement} must buffer rows into the exporter without prematurely draining.
 *   <li>When the exporter signals batch-full, the operator must drain the native runtime in the
 *       same {@code processElement} invocation.
 *   <li>{@code processWatermark} must drain BEFORE forwarding the watermark downstream.
 *   <li>{@code prepareSnapshotPreBarrier} must drain in-flight rows (Calc is stateless, but
 *       buffered rows must reach downstream before the checkpoint barrier).
 *   <li>{@code close()} must flush residue rows, then close native runtime, exporter, and
 *       allocator in order; idempotent.
 *   <li>{@link SupportsAuronNative#getPhysicalPlanNodes()} must return the constructor-passed
 *       plan as the sole element.
 *   <li>{@link SupportsAuronNative#getAuronOperatorId()} must return the unsuffixed value
 *       passed in the constructor.
 * </ol>
 */
public class FlinkAuronCalcOperatorTest {

    private static final String FLINK_OP_UNIQUE_ID = "calc-op-7";
    private static final int SUBTASK_INDEX = 3;

    /**
     * Repo-wide surefire sets {@code java.io.tmpdir=target/tmp} which may not exist on a clean
     * build. The Arrow C-Data JNI loader uses {@link File#createTempFile} when extracting its
     * native library; ensure the dir exists before any test runs.
     */
    @BeforeAll
    public static void ensureTmpDirExists() {
        String tmp = System.getProperty("java.io.tmpdir");
        if (tmp != null) {
            new File(tmp).mkdirs();
        }
    }

    private List<String> registeredKeysBefore;

    @BeforeEach
    public void snapshotRegisteredKeys() throws Exception {
        registeredKeysBefore = new ArrayList<>(peekJniResourceKeys());
    }

    @AfterEach
    public void cleanupResources() throws Exception {
        // Remove any new keys the test created so subsequent tests start clean.
        for (String key : peekJniResourceKeys()) {
            if (!registeredKeysBefore.contains(key)) {
                JniBridge.getResource(key); // removes
            }
        }
    }

    // ---------------------------------------------------------------------
    // 1) JniBridge.putResource integration
    // ---------------------------------------------------------------------

    /**
     * Contract: after {@code open()}, the operator must have registered its exporter via
     * {@link JniBridge#putResource(String, Object)} under a key starting with
     * {@code "FlinkAuronCalc-"}; the registered value must be a {@link FlinkArrowFFIExporter}.
     */
    @Test
    public void testOpenRegistersExporterViaPutResource() throws Exception {
        TestFlinkAuronCalcOperator op = newOperator();
        op.open();

        String key = findOnlyNewResourceKey();
        assertTrue(key.startsWith("FlinkAuronCalc-"), "Resource key should start with FlinkAuronCalc- but was: " + key);

        Object value = peekResource(key);
        assertNotNull(value, "putResource should have registered a non-null value");
        assertTrue(
                value instanceof FlinkArrowFFIExporter,
                "Registered resource should be a FlinkArrowFFIExporter but was: "
                        + value.getClass().getName());

        op.close();
    }

    // ---------------------------------------------------------------------
    // 2) Resource ID encodes Flink operator unique ID + subtask
    // ---------------------------------------------------------------------

    /**
     * Contract: the resource ID injected into the FFI Reader leaf must encode the Flink operator
     * unique ID and subtask index. Non-FFI fields ({@code num_partitions}, {@code schema}) must
     * be preserved unchanged. The {@link FFIReaderExecNode} proto has no
     * {@code auron_operator_id} field (only {@code KafkaScanExecNode} does); the resource ID is
     * the only place where the Flink operator identity is propagated to native code.
     */
    @Test
    public void testResourceIdEmbedsFlinkOperatorIdAndSubtask() throws Exception {
        PhysicalPlanNode plan = buildProjectFilterFfiReaderPlan(4);
        TestFlinkAuronCalcOperator op = newOperator(plan);
        op.open();

        PhysicalPlanNode runtime = readRuntimePlan(op);
        FFIReaderExecNode leaf = descendToFfiReader(runtime);

        String resourceId = leaf.getExportIterProviderResourceId();
        Pattern expected = Pattern.compile("^FlinkAuronCalc-" + FLINK_OP_UNIQUE_ID + "-" + SUBTASK_INDEX + ":(.+)$");
        Matcher m = expected.matcher(resourceId);
        assertTrue(
                m.matches(), "Resource ID should match FlinkAuronCalc-<opId>-<subtask>:<uuid> but was: " + resourceId);
        UUID.fromString(m.group(1));

        // Constructor-supplied non-FFI fields are preserved.
        FFIReaderExecNode originalLeaf = descendToFfiReader(plan);
        assertEquals(originalLeaf.getNumPartitions(), leaf.getNumPartitions());
        assertEquals(originalLeaf.getSchema(), leaf.getSchema());

        // FFIReaderExecNode proto has only 3 fields; no auron_operator_id.
        assertEquals(3, FFIReaderExecNode.getDescriptor().getFields().size());

        op.close();
    }

    // ---------------------------------------------------------------------
    // 3) processElement buffers into exporter
    // ---------------------------------------------------------------------

    /**
     * Contract: a single {@code processElement} that does not fill the batch must invoke
     * {@code exporter.offer(row)} exactly once and must NOT trigger a drain.
     */
    @Test
    public void testProcessElementBuffersIntoExporter() throws Exception {
        FakeExporterTrackingOperator op = newTrackingOperator(/* fullAfterRows */ 999);
        op.open();
        try {
            RowData row = GenericRowData.of(42);
            op.processElement(new StreamRecord<>(row));

            assertEquals(1, op.tracker.offerCount);
            assertSame(row, op.tracker.lastOffered);
            assertEquals(
                    0, op.fakeRuntime.loadNextBatchInvocations, "Drain must not be triggered when batch is not full");
        } finally {
            op.close();
        }
    }

    // ---------------------------------------------------------------------
    // 4) processElement drains when batch is full
    // ---------------------------------------------------------------------

    /**
     * Contract: when {@code exporter.isBatchFull()} returns true after the offer, the operator
     * must drain the native runtime in the same {@code processElement} invocation.
     */
    @Test
    public void testProcessElementDrainsWhenBatchFull() throws Exception {
        FakeExporterTrackingOperator op = newTrackingOperator(/* fullAfterRows */ 1);
        op.open();
        try {
            op.processElement(new StreamRecord<>(GenericRowData.of(1)));
            assertTrue(
                    op.fakeRuntime.loadNextBatchInvocations >= 1,
                    "Drain should be triggered when exporter.isBatchFull() returns true");
        } finally {
            op.close();
        }
    }

    // ---------------------------------------------------------------------
    // 4b) Multi-cycle drain — operator must reinit after each drain cycle
    // ---------------------------------------------------------------------

    /**
     * Contract: the real {@link org.apache.auron.jni.AuronCallNativeWrapper#loadNextBatch}
     * auto-closes its native runtime when {@code JniBridge.nextBatch} returns false, and the
     * native FFI Reader's drop calls Java {@code close()} on the exporter. The operator
     * therefore must reinitialize both {@link FlinkArrowFFIExporter} and the
     * {@link FlinkAuronCalcOperator.NativeRuntime} after every drain cycle so subsequent
     * {@code processElement} calls work correctly. Three consecutive batch-full triggers must
     * produce no exception; the factory must be invoked once for {@code open()} plus once per
     * drain cycle (= 4 total); the exporter must be re-registered under the same
     * {@code resourceId} after each cycle; and the operator must still accept offers after the
     * reinit cycles.
     */
    @Test
    public void testProcessElementMultiCycleDrainReinitializesWrapper() throws Exception {
        java.util.concurrent.atomic.AtomicInteger factoryCalls = new java.util.concurrent.atomic.AtomicInteger();
        FlinkAuronCalcOperator.NativeRuntimeFactory factory = (a, p, m, mem) -> {
            factoryCalls.incrementAndGet();
            return new OneShotNativeRuntime();
        };
        PhysicalPlanNode plan = buildProjectFilterFfiReaderPlan(1);
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        FakeExporterTrackingOperator op =
                new FakeExporterTrackingOperator(plan, rowType, rowType, "auron-op-id", factory, /* fullAfterRows */ 1);
        op.open();
        String resourceId = findOnlyNewResourceKey(); // captured after open()'s putResource
        try {
            for (int cycle = 1; cycle <= 3; cycle++) {
                op.processElement(new StreamRecord<>(GenericRowData.of(cycle)));

                // After each drain cycle the operator must have re-registered the exporter
                // under the same resourceId; otherwise the next native runtime would fail to
                // look up the FFI exporter.
                assertNotNull(
                        peekResource(resourceId),
                        "Cycle " + cycle + ": exporter must be re-registered under resourceId "
                                + "after drainNative reinit");
                assertSame(
                        op.tracker,
                        peekResource(resourceId),
                        "Cycle " + cycle + ": the re-registered exporter must be the same "
                                + "tracker instance (per-cycle reinit reuses the operator-owned exporter)");
            }

            assertEquals(
                    4,
                    factoryCalls.get(),
                    "Native runtime must be reinitialized after each batch-full drain cycle "
                            + "(1 for open() + 3 reinit)");

            // After three reinit cycles, the operator must still accept input. Offer one more
            // row and verify the tracker (re-registered each cycle, never closed by our fake)
            // still records it.
            int offerCountBefore = op.tracker.offerCount;
            op.processElement(new StreamRecord<>(GenericRowData.of(99)));
            assertEquals(
                    offerCountBefore + 1,
                    op.tracker.offerCount,
                    "After three drain reinit cycles the operator must still accept offers "
                            + "(verifies the exporter is in a usable state post-reinit)");
        } finally {
            op.close();
        }
    }

    // ---------------------------------------------------------------------
    // 5) processWatermark: drain happens BEFORE watermark forwarding
    // ---------------------------------------------------------------------

    /**
     * Contract: drain must run BEFORE the watermark is forwarded downstream. Otherwise a
     * downstream operator may receive a watermark that crosses rows still buffered upstream.
     */
    @Test
    public void testProcessWatermarkDrainsBeforeForward() throws Exception {
        FakeExporterTrackingOperator op = newTrackingOperator(/* fullAfterRows */ 999);
        op.open();
        try {
            op.processElement(new StreamRecord<>(GenericRowData.of(7)));
            op.events.clear();

            // Wire both the runtime and the output to record ordered events.
            op.fakeRuntime.eventsListener = op.events;
            op.fakeOutput.eventsListener = op.events;

            op.processWatermark(new Watermark(123L));

            assertTrue(op.events.size() >= 2, "Expected at least drain+watermark events but got: " + op.events);
            assertEquals("drain", op.events.get(0), "First event must be drain");
            assertEquals(
                    "watermark:123", op.events.get(op.events.size() - 1), "Last event must be the watermark forward");
        } finally {
            op.close();
        }
    }

    // ---------------------------------------------------------------------
    // 6) prepareSnapshotPreBarrier: drain in-flight rows
    // ---------------------------------------------------------------------

    /**
     * Contract: {@code prepareSnapshotPreBarrier} must drain the native pipeline so buffered
     * rows reach downstream before the checkpoint barrier crosses them. Calc itself is
     * stateless; no operator state writes are required.
     */
    @Test
    public void testPrepareSnapshotPreBarrierDrainsInFlight() throws Exception {
        FakeExporterTrackingOperator op = newTrackingOperator(/* fullAfterRows */ 999);
        op.open();
        try {
            op.processElement(new StreamRecord<>(GenericRowData.of(11)));
            int loadCountBefore = op.fakeRuntime.loadNextBatchInvocations;

            op.prepareSnapshotPreBarrier(7L);

            assertTrue(
                    op.fakeRuntime.loadNextBatchInvocations > loadCountBefore,
                    "prepareSnapshotPreBarrier should trigger a drain");
        } finally {
            op.close();
        }
    }

    // ---------------------------------------------------------------------
    // 7) close: drains residue, closes resources, idempotent
    // ---------------------------------------------------------------------

    /**
     * Contract: {@code close()} must flush in-flight rows by draining, then release the native
     * runtime, exporter, and child allocator in order. Calling {@code close()} twice must not
     * throw and must not double-close.
     */
    @Test
    public void testCloseDrainsResidueAndIsIdempotent() throws Exception {
        FakeExporterTrackingOperator op = newTrackingOperator(/* fullAfterRows */ 999);
        op.open();
        for (int i = 0; i < 3; i++) {
            op.processElement(new StreamRecord<>(GenericRowData.of(i)));
        }

        op.close();

        assertTrue(op.tracker.closeCount >= 1, "close() must close the exporter");
        assertEquals(1, op.fakeRuntime.closeInvocations, "close() must close native runtime");
        assertEquals(0L, op.openedChildAllocator.getAllocatedMemory(), "Child allocator must be drained after close");

        // Second close: must not throw or double-close.
        op.close();
        assertEquals(1, op.fakeRuntime.closeInvocations, "Second close() must not re-close native runtime");
    }

    // ---------------------------------------------------------------------
    // 8) SupportsAuronNative.getPhysicalPlanNodes
    // ---------------------------------------------------------------------

    /**
     * Contract: {@code getPhysicalPlanNodes()} returns a singleton list containing the
     * constructor-passed plan (NOT the runtime-rewritten one). Pure getter, no {@code open()}
     * required.
     */
    @Test
    public void testGetPhysicalPlanNodesReturnsConstructorPlan() {
        PhysicalPlanNode plan = buildProjectFilterFfiReaderPlan(2);
        TestFlinkAuronCalcOperator op = newOperator(plan);
        List<PhysicalPlanNode> nodes = op.getPhysicalPlanNodes();
        assertEquals(1, nodes.size(), "Should return exactly one plan node");
        assertSame(plan, nodes.get(0), "Should return the constructor-passed instance");
    }

    // ---------------------------------------------------------------------
    // 9) SupportsAuronNative.getAuronOperatorId returns unsuffixed value
    // ---------------------------------------------------------------------

    /**
     * Contract: {@code getAuronOperatorId()} returns the unsuffixed value passed in the
     * constructor — never the subtask-suffixed runtime variant. Mirrors
     * {@code AuronKafkaSourceFunction#getAuronOperatorId()}.
     */
    @Test
    public void testGetAuronOperatorIdReturnsUnsuffixed() throws Exception {
        TestFlinkAuronCalcOperator op = newOperator("my-calc-op");
        op.open();
        try {
            assertEquals("my-calc-op", op.getAuronOperatorId());
        } finally {
            op.close();
        }
    }

    // ---------------------------------------------------------------------
    // 10) injectFfiReaderLeaf shape coverage: bare FFIReader
    // ---------------------------------------------------------------------

    /**
     * Contract: when the input plan is a bare {@link FFIReaderExecNode}, {@code
     * injectFfiReaderLeaf} must return a plan whose FFI Reader has the supplied resource ID;
     * non-FFI fields ({@code num_partitions}, {@code schema}) are preserved.
     */
    @Test
    public void testInjectFfiReaderLeaf_BareFFIReader() {
        Schema schema = Schema.newBuilder().build();
        PhysicalPlanNode in = PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setNumPartitions(5)
                        .setSchema(schema)
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();

        PhysicalPlanNode out = FlinkAuronCalcOperator.injectFfiReaderLeaf(in, "rid-bare");

        assertTrue(out.hasFfiReader(), "Output root must remain an FFIReader");
        FFIReaderExecNode leaf = out.getFfiReader();
        assertEquals("rid-bare", leaf.getExportIterProviderResourceId());
        assertEquals(5, leaf.getNumPartitions());
        assertEquals(schema, leaf.getSchema());
    }

    // ---------------------------------------------------------------------
    // 11) injectFfiReaderLeaf shape coverage: Project[FFIReader]
    // ---------------------------------------------------------------------

    /**
     * Contract: when the input plan is {@code Project[FFIReader]}, {@code injectFfiReaderLeaf}
     * must preserve the projection wrapper and inject the resource ID into the FFI Reader leaf.
     */
    @Test
    public void testInjectFfiReaderLeaf_ProjectThenFFIReader() {
        PhysicalPlanNode leaf = PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setNumPartitions(2)
                        .setSchema(Schema.newBuilder().build())
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();
        PhysicalPlanNode in = PhysicalPlanNode.newBuilder()
                .setProjection(ProjectionExecNode.newBuilder().setInput(leaf).build())
                .build();

        PhysicalPlanNode out = FlinkAuronCalcOperator.injectFfiReaderLeaf(in, "rid-proj");

        assertTrue(out.hasProjection(), "Projection wrapper must be preserved");
        PhysicalPlanNode inner = out.getProjection().getInput();
        assertTrue(inner.hasFfiReader(), "Project's child must remain an FFIReader");
        assertEquals("rid-proj", inner.getFfiReader().getExportIterProviderResourceId());
    }

    // ---------------------------------------------------------------------
    // 12) injectFfiReaderLeaf shape coverage: Filter[FFIReader]
    // ---------------------------------------------------------------------

    /**
     * Contract: when the input plan is {@code Filter[FFIReader]}, {@code injectFfiReaderLeaf}
     * must preserve the filter wrapper and inject the resource ID into the FFI Reader leaf.
     */
    @Test
    public void testInjectFfiReaderLeaf_FilterThenFFIReader() {
        PhysicalPlanNode leaf = PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setNumPartitions(3)
                        .setSchema(Schema.newBuilder().build())
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();
        PhysicalPlanNode in = PhysicalPlanNode.newBuilder()
                .setFilter(FilterExecNode.newBuilder().setInput(leaf).build())
                .build();

        PhysicalPlanNode out = FlinkAuronCalcOperator.injectFfiReaderLeaf(in, "rid-filter");

        assertTrue(out.hasFilter(), "Filter wrapper must be preserved");
        PhysicalPlanNode inner = out.getFilter().getInput();
        assertTrue(inner.hasFfiReader(), "Filter's child must remain an FFIReader");
        assertEquals("rid-filter", inner.getFfiReader().getExportIterProviderResourceId());
    }

    // ---------------------------------------------------------------------
    // 13) injectFfiReaderLeaf: throws on unsupported plan shape
    // ---------------------------------------------------------------------

    /**
     * Contract: {@code injectFfiReaderLeaf} must fail fast with an {@link
     * IllegalArgumentException} on any node type outside the accepted {@code Project} /
     * {@code Filter} / {@code FFIReader} set, so misconfigurations surface at operator startup
     * rather than as silent fallthrough. The exception message must mention the unexpected node
     * type to aid diagnostics.
     */
    @Test
    public void testInjectFfiReaderLeaf_ThrowsOnUnknownShape() {
        // LimitExecNode is a valid oneof in PhysicalPlanNode but unsupported by the Calc
        // operator's shape contract (Project / Filter above FFIReader only).
        PhysicalPlanNode unsupported = PhysicalPlanNode.newBuilder()
                .setLimit(LimitExecNode.newBuilder().setLimit(10).build())
                .build();

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class, () -> FlinkAuronCalcOperator.injectFfiReaderLeaf(unsupported, "rid-x"));
        assertTrue(
                ex.getMessage().contains("LIMIT"),
                "Exception message must identify the unexpected node type for diagnostics, got: " + ex.getMessage());
    }

    // =====================================================================
    // Construction helpers
    // =====================================================================

    private TestFlinkAuronCalcOperator newOperator() {
        return newOperator(buildProjectFilterFfiReaderPlan(2), "auron-op-id");
    }

    private TestFlinkAuronCalcOperator newOperator(String auronOperatorId) {
        return newOperator(buildProjectFilterFfiReaderPlan(2), auronOperatorId);
    }

    private TestFlinkAuronCalcOperator newOperator(PhysicalPlanNode plan) {
        return newOperator(plan, "auron-op-id");
    }

    private TestFlinkAuronCalcOperator newOperator(PhysicalPlanNode plan, String auronOperatorId) {
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        FakeNativeRuntime runtime = new FakeNativeRuntime();
        TestFlinkAuronCalcOperator op =
                new TestFlinkAuronCalcOperator(plan, rowType, rowType, auronOperatorId, (a, p, m, mem) -> runtime);
        op.fakeRuntime = runtime;
        return op;
    }

    private FakeExporterTrackingOperator newTrackingOperator(int fullAfterRows) {
        PhysicalPlanNode plan = buildProjectFilterFfiReaderPlan(2);
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        FakeNativeRuntime runtime = new FakeNativeRuntime();
        FakeExporterTrackingOperator op = new FakeExporterTrackingOperator(
                plan, rowType, rowType, "auron-op-id", (a, p, m, mem) -> runtime, fullAfterRows);
        op.fakeRuntime = runtime;
        return op;
    }

    // =====================================================================
    // Plan construction helpers
    // =====================================================================

    /** Builds Project[Filter[FFIReader-placeholder]]. */
    private static PhysicalPlanNode buildProjectFilterFfiReaderPlan(int numPartitions) {
        Schema emptySchema = Schema.newBuilder().build();
        PhysicalPlanNode leaf = PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setNumPartitions(numPartitions)
                        .setSchema(emptySchema)
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();
        PhysicalPlanNode filter = PhysicalPlanNode.newBuilder()
                .setFilter(FilterExecNode.newBuilder().setInput(leaf).build())
                .build();
        return PhysicalPlanNode.newBuilder()
                .setProjection(ProjectionExecNode.newBuilder().setInput(filter).build())
                .build();
    }

    private static FFIReaderExecNode descendToFfiReader(PhysicalPlanNode node) {
        if (node.hasFfiReader()) {
            return node.getFfiReader();
        }
        if (node.hasProjection()) {
            return descendToFfiReader(node.getProjection().getInput());
        }
        if (node.hasFilter()) {
            return descendToFfiReader(node.getFilter().getInput());
        }
        return fail("No FFI Reader found in plan");
    }

    // =====================================================================
    // Reflection helpers
    // =====================================================================

    private static PhysicalPlanNode readRuntimePlan(FlinkAuronCalcOperator op) throws Exception {
        Field f = FlinkAuronCalcOperator.class.getDeclaredField("runtimePlan");
        f.setAccessible(true);
        return (PhysicalPlanNode) f.get(op);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> peekJniResourceMap() throws Exception {
        Field f = JniBridge.class.getDeclaredField("resourcesMap");
        f.setAccessible(true);
        return (Map<String, Object>) f.get(null);
    }

    private static List<String> peekJniResourceKeys() throws Exception {
        return new ArrayList<>(peekJniResourceMap().keySet());
    }

    private static Object peekResource(String key) throws Exception {
        return peekJniResourceMap().get(key);
    }

    private String findOnlyNewResourceKey() throws Exception {
        List<String> after = peekJniResourceKeys();
        after.removeAll(registeredKeysBefore);
        assertEquals(1, after.size(), "Exactly one new resource key expected, got: " + after);
        return after.get(0);
    }

    // =====================================================================
    // Test subclasses of the operator
    // =====================================================================

    /**
     * Test seam: a subclass of {@link FlinkAuronCalcOperator} that stubs out the Flink runtime
     * context dependencies the operator under test consumes (operator unique ID, subtask index,
     * metric group, processing-time service, and the inherited {@code output} field).
     * {@link StreamingRuntimeContext} is built via {@link sun.misc.Unsafe#allocateInstance} to
     * skip its heavy real constructor; only the methods the operator actually calls are
     * overridden.
     */
    static class TestFlinkAuronCalcOperator extends FlinkAuronCalcOperator {
        FakeNativeRuntime fakeRuntime; // set by factory closure after construction
        final StubMetricGroup metricGroup = new StubMetricGroup();
        final FakeOutput fakeOutput = new FakeOutput();
        BufferAllocator openedChildAllocator;

        TestFlinkAuronCalcOperator(
                PhysicalPlanNode plan,
                RowType inputRowType,
                RowType outputRowType,
                String auronOperatorId,
                NativeRuntimeFactory factory) {
            super(plan, inputRowType, outputRowType, auronOperatorId, factory);
            setProtectedOutput(this, fakeOutput);
        }

        @Override
        public StreamingRuntimeContext getRuntimeContext() {
            return makeStubRuntimeContext();
        }

        @Override
        public OperatorMetricGroup getMetricGroup() {
            return metricGroup;
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return new StubProcessingTimeService();
        }

        @Override
        public void open() throws Exception {
            super.open();
            this.openedChildAllocator = readChildAllocator();
        }

        BufferAllocator readChildAllocator() throws Exception {
            Field f = FlinkAuronCalcOperator.class.getDeclaredField("childAllocator");
            f.setAccessible(true);
            return (BufferAllocator) f.get(this);
        }
    }

    /**
     * Subclass that swaps the real exporter for a {@link TrackingExporter} so tests can observe
     * offer/isBatchFull/close calls deterministically.
     */
    static class FakeExporterTrackingOperator extends TestFlinkAuronCalcOperator {
        final int fullAfterRows;
        TrackingExporter tracker;
        final List<String> events = new ArrayList<>();

        FakeExporterTrackingOperator(
                PhysicalPlanNode plan,
                RowType inputRowType,
                RowType outputRowType,
                String auronOperatorId,
                NativeRuntimeFactory factory,
                int fullAfterRows) {
            super(plan, inputRowType, outputRowType, auronOperatorId, factory);
            this.fullAfterRows = fullAfterRows;
        }

        @Override
        public void open() throws Exception {
            super.open();
            tracker = new TrackingExporter(openedChildAllocator, super.getOutputType(), fullAfterRows);
            // Swap exporter in the parent.
            Field f = FlinkAuronCalcOperator.class.getDeclaredField("exporter");
            f.setAccessible(true);
            FlinkArrowFFIExporter original = (FlinkArrowFFIExporter) f.get(this);
            original.close();
            f.set(this, tracker);
        }
    }

    // =====================================================================
    // Fakes
    // =====================================================================

    /** Fake {@link FlinkAuronCalcOperator.NativeRuntime} — programmable + invocation tracking. */
    static final class FakeNativeRuntime implements FlinkAuronCalcOperator.NativeRuntime {
        int loadNextBatchInvocations;
        int closeInvocations;
        final Deque<VectorSchemaRoot> rootsToEmit = new ArrayDeque<>();
        List<String> eventsListener; // optional: appends "drain" on each call

        @Override
        public boolean loadNextBatch(Consumer<VectorSchemaRoot> consumer) {
            loadNextBatchInvocations++;
            if (eventsListener != null) {
                eventsListener.add("drain");
            }
            VectorSchemaRoot next = rootsToEmit.poll();
            if (next == null) {
                return false;
            }
            consumer.accept(next);
            return true;
        }

        @Override
        public void close() {
            closeInvocations++;
        }
    }

    /**
     * Fake {@link FlinkAuronCalcOperator.NativeRuntime} mirroring the production close-on-false
     * semantic of {@link org.apache.auron.jni.AuronCallNativeWrapper#loadNextBatch}
     * (auron-core/.../AuronCallNativeWrapper.java:127): the first {@code loadNextBatch} call
     * returns false and closes itself; subsequent calls would indicate the operator failed to
     * reinitialize per drain cycle (asserted as a test failure).
     */
    static final class OneShotNativeRuntime implements FlinkAuronCalcOperator.NativeRuntime {
        int loadNextBatchInvocations;
        int closeInvocations;
        boolean closedByDrain;

        @Override
        public boolean loadNextBatch(Consumer<VectorSchemaRoot> consumer) {
            if (closedByDrain) {
                throw new IllegalStateException(
                        "loadNextBatch called on a closed OneShotNativeRuntime — operator did not reinit");
            }
            loadNextBatchInvocations++;
            closedByDrain = true; // production wrapper auto-closes on false return
            return false;
        }

        @Override
        public void close() {
            closeInvocations++;
        }
    }

    /**
     * Tracking subclass of {@link FlinkArrowFFIExporter} that records call counts. The Arrow
     * allocations are still set up by the parent constructor; {@code offer} is intercepted so
     * tests can observe rows without committing them to the writer (avoids row-type/schema
     * enforcement issues for hand-built rows).
     */
    static final class TrackingExporter extends FlinkArrowFFIExporter {
        int offerCount;
        int closeCount;
        RowData lastOffered;
        final int fullAfterRows;

        TrackingExporter(BufferAllocator allocator, RowType inputRowType, int fullAfterRows) {
            super(allocator, inputRowType, /* batchRowsLimit */ Math.max(1, fullAfterRows));
            this.fullAfterRows = fullAfterRows;
        }

        @Override
        public void offer(RowData row) {
            offerCount++;
            lastOffered = row;
        }

        @Override
        public boolean isBatchFull() {
            return offerCount >= fullAfterRows;
        }

        /**
         * Bypasses the parent's writer in {@link #offer(RowData)}, so the parent's
         * {@code rowCount} stays at zero. Without this override, the operator's empty-drain
         * short-circuit would skip every drain in tests that rely on {@code offerCount}-based
         * fullness.
         */
        @Override
        public boolean isEmpty() {
            return offerCount == 0;
        }

        @Override
        public void close() {
            closeCount++;
            super.close();
        }
    }

    /** Fake {@link Output} recording every invocation in order. */
    static final class FakeOutput implements Output<StreamRecord<RowData>> {
        final List<RowData> collected = new ArrayList<>();
        final List<Watermark> emittedWatermarks = new ArrayList<>();
        List<String> eventsListener;

        @Override
        public void collect(StreamRecord<RowData> record) {
            collected.add(record.getValue());
            if (eventsListener != null) {
                eventsListener.add("collect");
            }
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitWatermark(Watermark mark) {
            emittedWatermarks.add(mark);
            if (eventsListener != null) {
                eventsListener.add("watermark:" + mark.getTimestamp());
            }
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {}

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {}

        @Override
        public void close() {}
    }

    // =====================================================================
    // Stub OperatorMetricGroup
    // =====================================================================

    static final class StubMetricGroup implements OperatorMetricGroup {
        @Override
        public Counter counter(String name) {
            return new StubCounter();
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            return counter;
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            return gauge;
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            return histogram;
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            return meter;
        }

        @Override
        public MetricGroup addGroup(String name) {
            return this;
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            return this;
        }

        @Override
        public String[] getScopeComponents() {
            return new String[0];
        }

        @Override
        public Map<String, String> getAllVariables() {
            return new HashMap<>();
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            return metricName;
        }

        @Override
        public String getMetricIdentifier(String metricName, CharacterFilter filter) {
            return metricName;
        }

        @Override
        public OperatorIOMetricGroup getIOMetricGroup() {
            return null;
        }
    }

    /** Stub {@link ProcessingTimeService}: never schedules anything; only used to satisfy
     * {@code TableStreamOperator.open()}'s non-null requirement on the timer service. */
    static final class StubProcessingTimeService implements ProcessingTimeService {
        @Override
        public long getCurrentProcessingTime() {
            return 0L;
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> registerTimer(long timestamp, ProcessingTimeCallback callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleAtFixedRate(
                ProcessingTimeCallback callback, long initialDelay, long period) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.ScheduledFuture<?> scheduleWithFixedDelay(
                ProcessingTimeCallback callback, long initialDelay, long delay) {
            throw new UnsupportedOperationException();
        }

        @Override
        public java.util.concurrent.CompletableFuture<Void> quiesce() {
            return java.util.concurrent.CompletableFuture.completedFuture(null);
        }
    }

    static final class StubCounter implements Counter {
        long n;

        @Override
        public void inc() {
            n++;
        }

        @Override
        public void inc(long n) {
            this.n += n;
        }

        @Override
        public void dec() {
            n--;
        }

        @Override
        public void dec(long n) {
            this.n -= n;
        }

        @Override
        public long getCount() {
            return n;
        }
    }

    // =====================================================================
    // StreamingRuntimeContext stub via Unsafe.allocateInstance
    //
    // StreamingRuntimeContext's real constructors require many non-null arguments (Environment,
    // OperatorMetricGroup, OperatorID, ProcessingTimeService, etc.) that are heavy to mock and
    // not needed by the operator under test. The operator only calls
    // getOperatorUniqueID() and getIndexOfThisSubtask(); we instantiate the stub without
    // invoking any constructor (via Unsafe) and override exactly those two methods.
    // =====================================================================

    private static StreamingRuntimeContext makeStubRuntimeContext() {
        try {
            return (StreamingRuntimeContext) unsafe().allocateInstance(StubStreamingRuntimeContext.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** StreamingRuntimeContext that overrides only the methods the operator under test calls. */
    static class StubStreamingRuntimeContext extends StreamingRuntimeContext {
        // Constructor is intentionally never invoked; instances are allocated via Unsafe.
        // The compiler requires a parent constructor call; supply a never-executed one.
        @SuppressWarnings("unused")
        private StubStreamingRuntimeContext() {
            super(null, null, null);
        }

        @Override
        public String getOperatorUniqueID() {
            return FLINK_OP_UNIQUE_ID;
        }

        @Override
        public int getIndexOfThisSubtask() {
            return SUBTASK_INDEX;
        }
    }

    private static sun.misc.Unsafe unsafe() throws Exception {
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (sun.misc.Unsafe) f.get(null);
    }

    private static void setProtectedOutput(FlinkAuronCalcOperator op, Output<?> out) {
        try {
            Field f = findField(op.getClass(), "output");
            f.setAccessible(true);
            f.set(op, out);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Field findField(Class<?> klass, String name) throws NoSuchFieldException {
        Class<?> c = klass;
        while (c != null) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        throw new NoSuchFieldException(name);
    }
}
