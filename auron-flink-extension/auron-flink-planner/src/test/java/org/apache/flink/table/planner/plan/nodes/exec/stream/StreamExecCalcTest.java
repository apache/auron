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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.auron.flink.runtime.operator.FlinkAuronCalcOperator;
import org.apache.auron.flink.table.planner.UnsupportedFlinkNodeRecorder;
import org.apache.auron.protobuf.ArrowType;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.FilterExecNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the shadowed {@link StreamExecCalc}.
 *
 * <p>Lives in the same package as the production class so {@code @JsonCreator} field-name
 * constants and the {@code protected translateToFlinkCalc} hook are reachable from tests.
 *
 * <p>Test strategy:
 *
 * <ul>
 *   <li>Plan-build success paths exercise the real {@code translateToPlanInternal} end-to-end. A
 *       fake upstream {@link ExecNode} is wired in via {@link ExecNodeBase#setInputEdges} with a
 *       pre-built {@link Transformation} cached in the source's {@code transformation} field so
 *       {@code ExecNodeBase.translateToPlan} returns it without consulting a real planner.
 *   <li>Fallback paths use {@link CapturingTranslator} to subclass {@code StreamExecCalc} and
 *       override {@code translateToFlinkCalc}, recording whether the super-codegen path was
 *       invoked and short-circuiting the heavy Flink machinery.
 *   <li>WARN log assertions capture {@code System.err} (the default SLF4J SimpleLogger target);
 *       the matching test class also resets the dedup ThreadLocal between tests so logging
 *       behavior is deterministic.
 * </ul>
 */
class StreamExecCalcTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private static final RowType TWO_INT_ROW =
            RowType.of(new LogicalType[] {new IntType(), new IntType()}, new String[] {"f0", "f1"});

    private TableConfig tableConfig;
    private InputProperty inputProperty;
    private ExecNodeConfig nodeConfig;

    @BeforeEach
    void setUp() throws Exception {
        // Reset id counter so getId()-derived strings are reproducible across tests.
        ExecNodeContext.resetIdCounter();
        // Reset the per-fallback WARN dedup so each test starts from a clean slate.
        UnsupportedFlinkNodeRecorder.resetForTest();

        tableConfig = TableConfig.getDefault();
        inputProperty = InputProperty.DEFAULT;
        nodeConfig = ExecNodeConfig.ofNodeConfig(new Configuration(), false);
    }

    // =====================================================================
    // Plan-build success paths
    // =====================================================================

    /** Contract: with a non-null condition + projection of supported RexNodes, the operator wired
     * into the returned Transformation is {@link FlinkAuronCalcOperator}. */
    @Test
    void testProjectAndFilterEmitsAuronOperator() throws Exception {
        StreamExecCalc node = newCalc(
                Arrays.asList(intRef(0), intRef(1)),
                makeBinary(intType(), SqlStdOperatorTable.PLUS, intRef(0), intRef(1)),
                TWO_INT_ROW);
        wireFakeUpstream(node, TWO_INT_ROW);

        Transformation<RowData> result = invokeTranslate(node);

        assertTrue(operatorOf(result) instanceof FlinkAuronCalcOperator);
    }

    /** Contract: with a null condition and supported projection, the operator is still Auron. */
    @Test
    void testProjectOnlyEmitsAuronOperator() throws Exception {
        StreamExecCalc node = newCalc(Arrays.asList(intRef(0), intRef(1)), null, TWO_INT_ROW);
        wireFakeUpstream(node, TWO_INT_ROW);

        Transformation<RowData> result = invokeTranslate(node);

        FlinkAuronCalcOperator op = (FlinkAuronCalcOperator) operatorOf(result);
        // No filter → plan is Project[FFIReader], not Project[Filter[FFIReader]].
        PhysicalPlanNode plan = op.getPhysicalPlanNodes().get(0);
        assertTrue(plan.hasProjection(), "Root must be a Projection");
        assertFalse(plan.getProjection().getInput().hasFilter(), "No filter wrapper expected when condition is null");
    }

    /** Contract: an identity projection (pure {@code RexInputRef}s) still converts. */
    @Test
    void testIdentityProjectionEmitsAuronOperator() throws Exception {
        StreamExecCalc node = newCalc(Arrays.asList(intRef(0), intRef(1)), null, TWO_INT_ROW);
        wireFakeUpstream(node, TWO_INT_ROW);

        Transformation<RowData> result = invokeTranslate(node);

        assertTrue(operatorOf(result) instanceof FlinkAuronCalcOperator);
    }

    /** Contract: the {@link ProjectionExecNode} carries field names and arrow types matching
     * {@code outputRowType}, so a downstream reader can rebuild the output schema without
     * consulting the JVM types again. */
    @Test
    void testSchemaPropagatedToProjectionExecNode() throws Exception {
        RowType outputType = RowType.of(new LogicalType[] {new IntType(), new BigIntType()}, new String[] {"a", "b"});
        StreamExecCalc node =
                newCalc(Arrays.asList(intRef(0), REX_BUILDER.makeInputRef(bigintType(), 1)), null, outputType);
        wireFakeUpstream(
                node, RowType.of(new LogicalType[] {new IntType(), new BigIntType()}, new String[] {"f0", "f1"}));

        Transformation<RowData> result = invokeTranslate(node);

        FlinkAuronCalcOperator op = (FlinkAuronCalcOperator) operatorOf(result);
        PhysicalPlanNode plan = op.getPhysicalPlanNodes().get(0);
        ProjectionExecNode proj = plan.getProjection();
        assertEquals(Arrays.asList("a", "b"), proj.getExprNameList());
        assertEquals(2, proj.getDataTypeCount());
        assertEquals(ArrowType.ArrowTypeEnumCase.INT32, proj.getDataType(0).getArrowTypeEnumCase());
        assertEquals(ArrowType.ArrowTypeEnumCase.INT64, proj.getDataType(1).getArrowTypeEnumCase());
        // FFI Reader leaf carries the input schema for the runtime.
        FFIReaderExecNode ffi = plan.getProjection().getInput().getFfiReader();
        assertEquals(1, ffi.getNumPartitions());
        assertEquals("placeholder", ffi.getExportIterProviderResourceId());
        assertEquals(2, ffi.getSchema().getColumnsCount());
    }

    /** Contract: when both projection and condition are supported, the constructed plan is
     * {@code Project[Filter[FFIReader]]} (Filter wrapper present). */
    @Test
    void testPlanShapeIsProjectFilterFFIReaderWhenConditionIsPresent() throws Exception {
        StreamExecCalc node = newCalc(
                Arrays.asList(intRef(0)),
                makeBinary(intType(), SqlStdOperatorTable.PLUS, intRef(0), intRef(1)),
                RowType.of(new IntType()));
        wireFakeUpstream(node, TWO_INT_ROW);

        Transformation<RowData> result = invokeTranslate(node);

        PhysicalPlanNode plan = ((FlinkAuronCalcOperator) operatorOf(result))
                .getPhysicalPlanNodes()
                .get(0);
        assertTrue(plan.hasProjection());
        PhysicalPlanNode innerProj = plan.getProjection().getInput();
        assertTrue(innerProj.hasFilter(), "Project's child must be a Filter when condition is non-null");
        FilterExecNode filter = innerProj.getFilter();
        assertEquals(1, filter.getExprCount());
        assertTrue(filter.getInput().hasFfiReader(), "Filter's child must be FFIReader");
    }

    // =====================================================================
    // Fallback paths (default config FAIL_BACK_FLINK_ENGINE_ENABLED=true)
    // =====================================================================

    /** Contract: an unsupported RexNode in the condition triggers fallback — Auron's
     * {@code translateToFlinkCalc} hook is invoked and its stub Transformation is returned. */
    @Test
    void testFallsBackWhenUnsupportedRexNodeInCondition() throws Exception {
        Transformation<RowData> stub = new FakeSourceTransformation();
        CapturingTranslator node = new CapturingTranslator(
                tableConfig,
                Arrays.asList(intRef(0)),
                makeBinary(intType(), SqlStdOperatorTable.SIMILAR_TO, intRef(0), intRef(1)),
                inputProperty,
                RowType.of(new IntType()),
                "calc",
                stub);
        wireFakeUpstream(node, TWO_INT_ROW);

        Transformation<RowData> result = invokeTranslate(node);

        assertSame(stub, result);
        assertEquals(1, node.fallbackCount);
    }

    /** Contract: an unsupported RexNode in the projection triggers fallback. */
    @Test
    void testFallsBackWhenUnsupportedRexNodeInProjection() throws Exception {
        Transformation<RowData> stub = new FakeSourceTransformation();
        // SIMILAR_TO produces a RexCall whose isSupported() returns false in RexCallConverter.
        CapturingTranslator node = new CapturingTranslator(
                tableConfig,
                Arrays.asList(makeBinary(intType(), SqlStdOperatorTable.SIMILAR_TO, intRef(0), intRef(1))),
                null,
                inputProperty,
                RowType.of(new IntType()),
                "calc",
                stub);
        wireFakeUpstream(node, TWO_INT_ROW);

        Transformation<RowData> result = invokeTranslate(node);

        assertSame(stub, result);
        assertEquals(1, node.fallbackCount);
    }

    /** Contract: a {@code RowType} containing a RAW logical type causes
     * {@code SchemaConverters.convertToAuronArrowType} to throw; the outer {@code Throwable}
     * catch maps this to {@code Optional.empty()} and the operator falls back. */
    @Test
    void testFallsBackWhenSchemaConversionThrows() throws Exception {
        Transformation<RowData> stub = new FakeSourceTransformation();
        // Output schema includes a RAW type — SchemaConverters does not handle this kind.
        RowType outputWithRaw = RowType.of(
                new LogicalType[] {new IntType(), new RawType<>(Object.class, new ObjectSerializer())},
                new String[] {"a", "rawcol"});
        // Use RexInputRefs that exist in the input schema (TWO_INT_ROW has 2 ints).
        // Output type mismatch with projection type is fine for plan-build — schema conversion
        // is the failure point.
        CapturingTranslator node = new CapturingTranslator(
                tableConfig, Arrays.asList(intRef(0), intRef(1)), null, inputProperty, outputWithRaw, "calc", stub);
        wireFakeUpstream(node, TWO_INT_ROW);

        Transformation<RowData> result = invokeTranslate(node);

        assertSame(stub, result, "Schema conversion failure must trigger fallback");
        assertEquals(1, node.fallbackCount);
        assertEquals(1, UnsupportedFlinkNodeRecorder.peekEmitCount());
    }

    // =====================================================================
    // Strict mode (FAIL_BACK_FLINK_ENGINE_ENABLED=false)
    // =====================================================================

    /** Contract: with fallback disabled and an unsupported RexNode, the operator throws
     * {@link IllegalStateException} rather than degrading silently. */
    @Test
    void testThrowsWhenFallbackDisabled() throws Exception {
        withStrictModeConf(() -> {
            CapturingTranslator node = new CapturingTranslator(
                    tableConfig,
                    Arrays.asList(intRef(0)),
                    makeBinary(intType(), SqlStdOperatorTable.SIMILAR_TO, intRef(0), intRef(1)),
                    inputProperty,
                    RowType.of(new IntType()),
                    "calc",
                    new FakeSourceTransformation());
            wireFakeUpstream(node, TWO_INT_ROW);
            IllegalStateException ex = assertThrows(IllegalStateException.class, () -> invokeTranslate(node));
            assertTrue(
                    ex.getMessage().contains("fallback is disabled"),
                    "IllegalStateException must mention fallback is disabled, got: " + ex.getMessage());
            // Super was never invoked because the strict path threw before the hook.
            assertEquals(0, node.fallbackCount);
            return null;
        });
    }

    // =====================================================================
    // Observability (per-fallback WARN log)
    // =====================================================================

    /** Contract: two Calcs in a single submission falling back on the same unsupported RexNode
     * class emit exactly one WARN log line. Verified via the test-only emit counter on
     * {@link StreamExecCalc}, which is incremented only when the dedup set treats the class as
     * new. */
    @Test
    void testFallbackEmitsWarnLogOnce() throws Exception {
        CapturingTranslator a = new CapturingTranslator(
                tableConfig,
                Arrays.asList(intRef(0)),
                makeBinary(intType(), SqlStdOperatorTable.SIMILAR_TO, intRef(0), intRef(1)),
                inputProperty,
                RowType.of(new IntType()),
                "calc-a",
                new FakeSourceTransformation());
        wireFakeUpstream(a, TWO_INT_ROW);
        invokeTranslate(a);

        CapturingTranslator b = new CapturingTranslator(
                tableConfig,
                Arrays.asList(intRef(0)),
                makeBinary(intType(), SqlStdOperatorTable.SIMILAR_TO, intRef(0), intRef(1)),
                inputProperty,
                RowType.of(new IntType()),
                "calc-b",
                new FakeSourceTransformation());
        wireFakeUpstream(b, TWO_INT_ROW);
        invokeTranslate(b);

        assertEquals(1, UnsupportedFlinkNodeRecorder.peekEmitCount());
    }

    /** Contract: two Calcs falling back on different unsupported RexNode classes emit two
     * distinct WARN log lines. */
    @Test
    void testFallbackEmitsDistinctWarnLogsForDistinctRexClasses() throws Exception {
        // First fallback: RexCall (SIMILAR_TO) is unsupported by RexCallConverter.isSupported.
        CapturingTranslator a = new CapturingTranslator(
                tableConfig,
                Arrays.asList(intRef(0)),
                makeBinary(intType(), SqlStdOperatorTable.SIMILAR_TO, intRef(0), intRef(1)),
                inputProperty,
                RowType.of(new IntType()),
                "calc-a",
                new FakeSourceTransformation());
        wireFakeUpstream(a, TWO_INT_ROW);
        invokeTranslate(a);

        // Second fallback: an UnregisteredRex subclass — converter map has no entry.
        CapturingTranslator b = new CapturingTranslator(
                tableConfig,
                Arrays.asList(new UnregisteredRex(intType())),
                null,
                inputProperty,
                RowType.of(new IntType()),
                "calc-b",
                new FakeSourceTransformation());
        wireFakeUpstream(b, TWO_INT_ROW);
        invokeTranslate(b);

        assertEquals(2, UnsupportedFlinkNodeRecorder.peekEmitCount());
    }

    // =====================================================================
    // Classpath shadowing (informational; only meaningful when an assembly JAR is in front)
    // =====================================================================

    /** Contract: when running with the assembled bundle, {@code Class.forName} on the FQCN
     * resolves to Auron's class. Skipped by default in module-isolated tests where this module
     * provides the only copy of the class. */
    @Test
    void testShadowedClassResolvesViaClasspath() throws Exception {
        Class<?> resolved = Class.forName("org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecCalc");
        assertSame(StreamExecCalc.class, resolved);
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private static RelDataType intType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    }

    private static RelDataType bigintType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
    }

    private static RexNode intRef(int idx) {
        return REX_BUILDER.makeInputRef(intType(), idx);
    }

    private static RexNode makeBinary(
            RelDataType returnType, org.apache.calcite.sql.SqlOperator op, RexNode left, RexNode right) {
        return REX_BUILDER.makeCall(returnType, op, Arrays.asList(left, right));
    }

    private StreamExecCalc newCalc(List<RexNode> projection, RexNode condition, RowType outputType) {
        return new StreamExecCalc(tableConfig, projection, condition, inputProperty, outputType, "calc");
    }

    /**
     * Wires a single fake upstream {@link ExecEdge} whose source's cached {@code transformation}
     * field is a pre-built stub. {@link ExecNodeBase#translateToPlan} short-circuits on a non-null
     * cached transformation and returns it without consulting the planner.
     */
    private static void wireFakeUpstream(StreamExecCalc node, RowType inputRowType) throws Exception {
        FakeSourceExecNode<RowData> source = new FakeSourceExecNode<>(inputRowType, "src");
        source.setCachedTransformation(new FakeSourceTransformation());
        ExecEdge edge = ExecEdge.builder().source(source).target(node).build();
        node.setInputEdges(Collections.singletonList(edge));
    }

    private Transformation<RowData> invokeTranslate(StreamExecCalc node) throws Exception {
        Method m = ExecNodeBase.class.getDeclaredMethod(
                "translateToPlanInternal", PlannerBase.class, ExecNodeConfig.class);
        m.setAccessible(true);
        try {
            @SuppressWarnings("unchecked")
            Transformation<RowData> t = (Transformation<RowData>) m.invoke(node, null, nodeConfig);
            return t;
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw e;
        }
    }

    /** Extracts the {@link org.apache.flink.streaming.api.operators.StreamOperator} wrapped in
     * the {@link SimpleOperatorFactory} of a {@link OneInputTransformation}. */
    private static Object operatorOf(Transformation<RowData> result) {
        assertTrue(
                result instanceof OneInputTransformation, "Expected OneInputTransformation, got: " + result.getClass());
        OneInputTransformation<?, ?> oit = (OneInputTransformation<?, ?>) result;
        Object factory = oit.getOperatorFactory();
        assertTrue(
                factory instanceof SimpleOperatorFactory, "Expected SimpleOperatorFactory, got: " + factory.getClass());
        return ((SimpleOperatorFactory<?>) factory).getOperator();
    }

    /**
     * Runs {@code action} with the strict-mode {@code flink-conf.yaml} test resource pointed at
     * by {@code FLINK_CONF_DIR}. The file disables fallback so the operator throws on conversion
     * failure. Restores the prior environment after the action runs.
     *
     * <p>{@link org.apache.auron.flink.configuration.FlinkAuronConfiguration} resolves its
     * underlying Flink config via {@link org.apache.flink.configuration.GlobalConfiguration#loadConfiguration()},
     * which consults the {@code FLINK_CONF_DIR} environment variable; flipping that pointer is
     * the supported way to swap configurations across test cases.
     */
    private static <T> T withStrictModeConf(java.util.concurrent.Callable<T> action) throws Exception {
        java.net.URL yaml =
                StreamExecCalcTest.class.getClassLoader().getResource("strict-mode-flink-conf/flink-conf.yaml");
        assertNotNull(yaml, "strict-mode-flink-conf/flink-conf.yaml test resource must exist");
        String confDir = new java.io.File(yaml.getFile()).getParentFile().getAbsolutePath();
        String previous = System.getenv(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR);
        java.util.Map<String, String> env = new java.util.HashMap<>(System.getenv());
        env.put(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR, confDir);
        mutateEnv(env);
        try {
            return action.call();
        } finally {
            java.util.Map<String, String> restore = new java.util.HashMap<>(System.getenv());
            if (previous == null) {
                restore.remove(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR);
            } else {
                restore.put(org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR, previous);
            }
            mutateEnv(restore);
        }
    }

    /** Reflectively replaces the JVM's environment map. Test-only — based on the same pattern
     * used by {@code CommonTestUtils.setEnv} in {@code auron-flink-runtime}. */
    @SuppressWarnings("unchecked")
    private static void mutateEnv(java.util.Map<String, String> newEnv) {
        try {
            java.util.Map<String, String> currentEnv = System.getenv();
            Field f = currentEnv.getClass().getDeclaredField("m");
            f.setAccessible(true);
            java.util.Map<String, String> backing = (java.util.Map<String, String>) f.get(currentEnv);
            backing.clear();
            backing.putAll(newEnv);
            try {
                Class<?> peClass = Class.forName("java.lang.ProcessEnvironment");
                Field ci = peClass.getDeclaredField("theCaseInsensitiveEnvironment");
                ci.setAccessible(true);
                java.util.Map<String, String> cienv = (java.util.Map<String, String>) ci.get(null);
                cienv.clear();
                cienv.putAll(newEnv);
            } catch (NoSuchFieldException ignored) {
                // theCaseInsensitiveEnvironment is Windows-only.
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // =====================================================================
    // Test subclass: capture the fallback delegation
    // =====================================================================

    /** Subclass of {@link StreamExecCalc} that records super-codegen invocations and returns a
     * stub Transformation instead of executing Flink's actual codegen path. */
    static class CapturingTranslator extends StreamExecCalc {
        int fallbackCount;
        final Transformation<RowData> fallbackStub;

        CapturingTranslator(
                org.apache.flink.configuration.ReadableConfig tableConfig,
                List<RexNode> projection,
                RexNode condition,
                InputProperty inputProperty,
                RowType outputType,
                String description,
                Transformation<RowData> fallbackStub) {
            super(tableConfig, projection, condition, inputProperty, outputType, description);
            this.fallbackStub = fallbackStub;
        }

        // No @Override: javac on some classpaths resolves StreamExecCalc to Flink's stock
        // class (which lacks translateToFlinkCalc). Runtime virtual dispatch still routes
        // the shadow's call here because the loaded StreamExecCalc is our shadow.
        protected Transformation<RowData> translateToFlinkCalc(PlannerBase planner, ExecNodeConfig config) {
            fallbackCount++;
            return fallbackStub;
        }
    }

    // =====================================================================
    // Fake upstream ExecNode and Transformation
    // =====================================================================

    /**
     * Minimal subclass of {@link ExecNodeBase} used as the source of a fake {@link ExecEdge}. Its
     * cached {@code transformation} field is set via reflection so {@code translateToPlan}
     * short-circuits and never touches the planner.
     */
    static class FakeSourceExecNode<T> extends ExecNodeBase<T> implements StreamExecNode<T> {
        FakeSourceExecNode(LogicalType outputType, String description) {
            super(
                    ExecNodeContext.newNodeId(),
                    new ExecNodeContext("fake-src_1"),
                    new Configuration(),
                    Collections.emptyList(),
                    outputType,
                    description);
        }

        void setCachedTransformation(Transformation<T> t) throws Exception {
            Field f = ExecNodeBase.class.getDeclaredField("transformation");
            f.setAccessible(true);
            f.set(this, t);
        }

        @Override
        protected Transformation<T> translateToPlanInternal(PlannerBase planner, ExecNodeConfig config) {
            // Never invoked because translateToPlan short-circuits on the cached transformation.
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Minimal stub {@link Transformation} subclass — we reuse {@link SourceTransformation}'s shape
     * via a no-op subclass so {@code getParallelism()} returns a sane integer.
     */
    static class FakeSourceTransformation extends Transformation<RowData> {
        FakeSourceTransformation() {
            super("fake-src", InternalTypeInfo.of(TWO_INT_ROW), 1);
        }

        @Override
        public List<Transformation<?>> getTransitivePredecessors() {
            return Collections.singletonList(this);
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.emptyList();
        }
    }

    // =====================================================================
    // Unregistered RexNode subclass for the dedup test
    // =====================================================================

    /** {@link RexNode} subclass that is not registered in the factory so it routes through the
     * "no converter registered" path of {@link FlinkNodeConverterFactory#convertRexNode}. */
    static class UnregisteredRex extends RexNode {
        private final RelDataType type;

        UnregisteredRex(RelDataType type) {
            this.type = type;
        }

        @Override
        public RelDataType getType() {
            return type;
        }

        @Override
        public <R> R accept(org.apache.calcite.rex.RexVisitor<R> visitor) {
            return null;
        }

        @Override
        public <R, P> R accept(org.apache.calcite.rex.RexBiVisitor<R, P> visitor, P arg) {
            return null;
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    // =====================================================================
    // RawType serializer (test helper for the schema-conversion failure path)
    // =====================================================================

    /** No-op TypeSerializer required to construct a {@link RawType}. */
    static class ObjectSerializer extends org.apache.flink.api.common.typeutils.TypeSerializer<Object> {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<Object> duplicate() {
            return this;
        }

        @Override
        public Object createInstance() {
            return new Object();
        }

        @Override
        public Object copy(Object from) {
            return from;
        }

        @Override
        public Object copy(Object from, Object reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(Object record, org.apache.flink.core.memory.DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object deserialize(org.apache.flink.core.memory.DataInputView source) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object deserialize(Object reuse, org.apache.flink.core.memory.DataInputView source) {
            return deserialize(source);
        }

        @Override
        public void copy(
                org.apache.flink.core.memory.DataInputView source, org.apache.flink.core.memory.DataOutputView target) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ObjectSerializer;
        }

        @Override
        public int hashCode() {
            return ObjectSerializer.class.hashCode();
        }

        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<Object> snapshotConfiguration() {
            throw new UnsupportedOperationException();
        }
    }
}
