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
package org.apache.auron.flink.table.planner.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.auron.flink.arrow.FlinkArrowReader;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.arrow.FlinkArrowWriter;
import org.apache.auron.flink.functions.AuronGeneratedUDF;
import org.apache.auron.flink.functions.FlinkAuronUDFWrapperContext;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FlinkUDFCodeGenerator} and for the artifact it produces.
 *
 * <p>Generating source proves nothing on its own, so every case here either compiles and runs the
 * generated class or asserts that a source which cannot be compiled is declined.
 */
public class FlinkUDFCodeGeneratorTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private static final FlinkTypeFactory FLINK_TYPE_FACTORY =
            new FlinkTypeFactory(FlinkUDFCodeGeneratorTest.class.getClassLoader(), RelDataTypeSystem.DEFAULT);

    /**
     * Repo-wide surefire sets {@code java.io.tmpdir=target/tmp} which may not exist on a clean
     * build. The Arrow C-Data JNI loader extracts its native library via
     * {@link File#createTempFile}, which fails if the directory is missing. Ensure it exists
     * before any test runs.
     */
    @BeforeAll
    public static void ensureTmpDirExists() {
        String tmp = System.getProperty("java.io.tmpdir");
        if (tmp != null) {
            new File(tmp).mkdirs();
        }
    }

    /**
     * Contract: where two {@code eval} overloads could both take the argument, the generated call
     * reaches the most specific one — the overload javac would pick for the same literal call.
     *
     * <p>This is what lets the plan-time overload gate go. Selecting the first invokable candidate
     * instead is a wrong answer rather than a failure, so the assertion is on the value the
     * function produced, which differs per overload.
     */
    @Test
    public void testMostSpecificEvalOverloadWins() throws Exception {
        RexCall call = udfCall(varcharType(), new AmbiguousFunction(), REX_BUILDER.makeInputRef(varcharType(), 0));

        AuronGeneratedUDF invoker = compile(call, RowType.of(new LogicalType[] {new VarCharType(100)}));

        Object result = invoker.eval(GenericRowData.of(StringData.fromString("hi")));
        assertEquals("STRING:hi", String.valueOf(result));
        invoker.close();
    }

    /**
     * Contract: a call whose generated source does not compile is declined, not thrown out of.
     *
     * <p>The generator's compile check is the whole reason a decline is possible here: the source
     * is emitted successfully for this function and only fails to compile, because an anonymous
     * class is not accessible from the generated class's package. Without the check the failure
     * would reach the task instead and fail the running job.
     */
    @Test
    public void testUncompilableSourceDeclines() {
        RexCall call = udfCall(varcharType(), anonymousFunction(), REX_BUILDER.makeInputRef(varcharType(), 0));

        assertFalse(FlinkUDFCodeGenerator.generate(
                        call,
                        RowType.of(new LogicalType[] {new VarCharType(100)}),
                        new Configuration(),
                        getClass().getClassLoader())
                .isPresent());
    }

    /**
     * Contract: the blob the planner attaches to the wrapper node deserializes into a working
     * invoker and produces the right answer across the Arrow boundary.
     *
     * <p>This is the case that runs the whole chain the planner and the runtime share — the
     * generated source, the reference array it can only be instantiated against, the class name it
     * must be compiled under, and the argument types the Arrow schemas are built from. A mismatch
     * in any one of them is invisible to a test of either side alone.
     */
    @Test
    public void testPlannerBlobEvaluatesAcrossTheArrowBoundary() throws Exception {
        ConcatFunction udf = new ConcatFunction();
        RexCall call = udfCall(
                varcharType(), udf, REX_BUILDER.makeInputRef(varcharType(), 0), REX_BUILDER.makeInputRef(intType(), 1));

        List<Object> results = runThroughPlanner(
                call,
                udf,
                RowType.of(new LogicalType[] {new VarCharType(100), new IntType()}),
                RowType.of(new VarCharType(100)),
                Arrays.asList(
                        GenericRowData.of(StringData.fromString("ab"), 5),
                        GenericRowData.of(StringData.fromString("x"), -1)),
                row -> row.getString(0).toString());

        assertEquals(Arrays.<Object>asList("ab5", "x-1"), results);
    }

    /**
     * Contract: argument {@code i} of the wrapper's input row reaches {@code eval} parameter
     * {@code i}.
     *
     * <p>Both arguments are {@code VARCHAR} with distinct values on purpose. Where two parameters
     * have incompatible types a transposition dies on a cast and any test catches it; where they
     * share a type it produces a wrong answer instead, and nothing else here would notice.
     */
    @Test
    public void testSameTypedArgumentsBindPositionally() throws Exception {
        PairFunction udf = new PairFunction();
        RexCall call = udfCall(
                varcharType(),
                udf,
                REX_BUILDER.makeInputRef(varcharType(), 0),
                REX_BUILDER.makeInputRef(varcharType(), 1));

        List<Object> results = runThroughPlanner(
                call,
                udf,
                RowType.of(new LogicalType[] {new VarCharType(100), new VarCharType(100)}),
                RowType.of(new VarCharType(100)),
                Arrays.asList(GenericRowData.of(StringData.fromString("alpha"), StringData.fromString("beta"))),
                row -> row.getString(0).toString());

        assertEquals(Arrays.<Object>asList("alpha|beta"), results);
    }

    /**
     * Contract: a {@code DECIMAL} argument reaches {@code eval} as the {@link BigDecimal} the
     * function declares, and a returned {@code BigDecimal} travels back out.
     *
     * <p>{@code DECIMAL} is one of the few supported roots whose internal representation is not the
     * external one, so both directions of the generated conversion are exercised here and nowhere
     * else. The result is read back through {@code toPlainString}, which renders the scale, so a
     * conversion that carried the wrong scale renders {@code 12.4400} rather than {@code 12.44}
     * even though the two are numerically equal. The negative operand is there because its result
     * crosses zero.
     */
    @Test
    public void testDecimalArgumentAndResultConvertBothWays() throws Exception {
        DecimalFunction udf = new DecimalFunction();
        RexCall call = udfCall(decimalType(), udf, REX_BUILDER.makeInputRef(decimalType(), 0));

        List<Object> results = runThroughPlanner(
                call,
                udf,
                RowType.of(new DecimalType(10, 2)),
                RowType.of(new DecimalType(10, 2)),
                Arrays.asList(
                        GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2)),
                        GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("-0.05"), 10, 2))),
                row -> row.getDecimal(0, 10, 2).toBigDecimal().toPlainString());

        // Rendered at scale 2 because that is the declared scale; a result at any other scale
        // renders with a different number of digits, such as 12.4400.
        assertEquals(Arrays.<Object>asList("12.44", "0.05"), results);
    }

    /**
     * Contract: a {@code DATE} argument reaches {@code eval} as the {@link LocalDate} the function
     * declares, and a returned {@code LocalDate} travels back out.
     *
     * <p>The internal representation is an epoch day count, so the conversion is arithmetic rather
     * than a cast. One date before the epoch is included because a signed/unsigned or absolute/
     * relative slip only shows up on that side of it.
     */
    @Test
    public void testDateArgumentAndResultConvertBothWays() throws Exception {
        DateFunction udf = new DateFunction();
        RexCall call = udfCall(dateType(), udf, REX_BUILDER.makeInputRef(dateType(), 0));

        List<Object> results = runThroughPlanner(
                call,
                udf,
                RowType.of(new DateType()),
                RowType.of(new DateType()),
                Arrays.asList(GenericRowData.of((int) LocalDate.of(2024, 2, 28).toEpochDay()), GenericRowData.of((int)
                        LocalDate.of(1969, 12, 31).toEpochDay())),
                row -> LocalDate.ofEpochDay(row.getInt(0)).toString());

        // The function adds a day, so 2024 lands on the leap day and 1969 crosses the epoch.
        assertEquals(Arrays.<Object>asList("2024-02-29", "1970-01-01"), results);
    }

    /**
     * Contract: a null argument reaches {@code eval} as a null and a null returned by {@code eval}
     * is written out as a null.
     *
     * <p>The generated body ends in an explicit {@code if (<nullTerm>) return null;}, and that
     * branch is dead for every other case here: Calcite's {@code createSqlType} produces NOT NULL
     * types, and the logical type the wrapper builds its Arrow schema from copies that nullability,
     * so a fixture has to ask for a nullable type to reach it. Both rows matter — the non-null
     * one proves the branch is conditional rather than always taken.
     */
    @Test
    public void testNullArgumentAndNullResultCrossTheBoundary() throws Exception {
        NullReturningFunction udf = new NullReturningFunction();
        RexCall call = udfCall(nullable(varcharType()), udf, REX_BUILDER.makeInputRef(nullable(varcharType()), 0));

        List<Object> results = runThroughPlanner(
                call,
                udf,
                RowType.of(new VarCharType(true, 100)),
                RowType.of(new VarCharType(true, 100)),
                Arrays.asList(GenericRowData.of(StringData.fromString("hi")), GenericRowData.of((Object) null)),
                row -> row.getString(0).toString());

        assertEquals(Arrays.<Object>asList("X:hi", null), results);
    }

    // ---- Helpers ----

    /** Pulls the single result column out of one non-null output row. */
    @FunctionalInterface
    private interface ResultExtractor {
        Object extract(RowData row);
    }

    /**
     * Runs a call the whole way: the planner builds the wrapper node, the runtime rebuilds the
     * invoker from the blob it carries, and the rows cross the Arrow boundary in both directions.
     */
    private List<Object> runThroughPlanner(
            RexCall call,
            ScalarFunction udf,
            RowType paramsRowType,
            RowType resultRowType,
            List<RowData> rows,
            ResultExtractor extractor)
            throws Exception {
        FlinkNodeConverterFactory factory = new FlinkNodeConverterFactory();
        factory.registerRexConverter(new RexInputRefConverter());
        factory.registerRexConverter(new RexLiteralConverter());
        factory.registerRexConverter(new RexCallConverter(factory));
        ConverterContext context =
                new ConverterContext(new Configuration(), null, getClass().getClassLoader(), paramsRowType);

        PhysicalExprNode node =
                FlinkUDFFallbackBuilder.build(call, udf, context, factory).orElseThrow(AssertionError::new);

        FlinkAuronUDFWrapperContext wrapper = new FlinkAuronUDFWrapperContext(
                node.getUdfWrapperExpr().getSerialized().toByteArray(),
                getClass().getClassLoader(),
                runtimeContext());
        try {
            return evalOverArrow(wrapper, paramsRowType, resultRowType, rows, extractor);
        } finally {
            wrapper.close();
        }
    }

    /** Generates, compiles and opens the invoker for one call. */
    private AuronGeneratedUDF compile(RexCall call, RowType paramsRowType) throws Exception {
        FlinkUDFCodeGenerator.GeneratedCode generated = FlinkUDFCodeGenerator.generate(
                        call, paramsRowType, new Configuration(), getClass().getClassLoader())
                .orElseThrow(AssertionError::new);
        Class<?> compiled =
                CompileUtils.compile(getClass().getClassLoader(), generated.getClassName(), generated.getCode());
        AuronGeneratedUDF invoker = (AuronGeneratedUDF)
                compiled.getConstructor(Object[].class).newInstance((Object) generated.getReferences());
        invoker.open(runtimeContext());
        return invoker;
    }

    /**
     * Drives one {@code eval} the way the native side does: the argument columns are exported as a
     * struct array and the single result column is imported back out of a second one.
     */
    private static List<Object> evalOverArrow(
            FlinkAuronUDFWrapperContext wrapper,
            RowType paramsRowType,
            RowType resultRowType,
            List<RowData> rows,
            ResultExtractor extractor)
            throws Exception {
        List<Object> results = new ArrayList<>();
        try (BufferAllocator allocator = FlinkArrowUtils.createChildAllocator("udfCodeGenTest")) {
            // Declared before the roots so they are closed last: an imported root must be released
            // while the ArrowArray struct it was imported from is still alive.
            try (ArrowArray inputArray = ArrowArray.allocateNew(allocator);
                    ArrowArray outputArray = ArrowArray.allocateNew(allocator)) {
                try (VectorSchemaRoot paramsRoot =
                        VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(paramsRowType), allocator)) {
                    FlinkArrowWriter writer = FlinkArrowWriter.create(paramsRoot, paramsRowType);
                    for (RowData row : rows) {
                        writer.write(row);
                    }
                    writer.finish();
                    Data.exportVectorSchemaRoot(allocator, paramsRoot, null, inputArray);
                }

                wrapper.eval(inputArray.memoryAddress(), outputArray.memoryAddress());

                try (VectorSchemaRoot resultRoot =
                                VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(resultRowType), allocator);
                        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
                    Data.importIntoVectorSchemaRoot(allocator, outputArray, resultRoot, provider);
                    try (FlinkArrowReader reader = FlinkArrowReader.create(resultRoot, resultRowType)) {
                        for (int i = 0; i < resultRoot.getRowCount(); i++) {
                            RowData row = reader.read(i);
                            results.add(row.isNullAt(0) ? null : extractor.extract(row));
                        }
                    }
                }
            }
        }
        return results;
    }

    /**
     * Generated open statements reach the runtime context for the user function's own
     * {@code FunctionContext} and for the classloader every reusable converter is opened with, so a
     * real one is required rather than a null.
     */
    private static StreamingRuntimeContext runtimeContext() {
        return new MockStreamingRuntimeContext(false, 1, 0);
    }

    private static RexCall udfCall(RelDataType returnType, ScalarFunction udf, RexNode... operands) {
        ScalarSqlFunction operator = new ScalarSqlFunction(
                FunctionIdentifier.of("test_udf"),
                "test_udf",
                udf,
                FLINK_TYPE_FACTORY,
                scala.Option.apply((SqlReturnTypeInference) null));
        return (RexCall) REX_BUILDER.makeCall(returnType, operator, Arrays.asList(operands));
    }

    private static RelDataType intType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    }

    /**
     * Calcite's {@code createSqlType} produces NOT NULL types, and that nullability is copied all
     * the way into the logical type the Arrow schema is built from, so a nullable fixture has to be
     * asked for explicitly.
     */
    private static RelDataType nullable(RelDataType type) {
        return TYPE_FACTORY.createTypeWithNullability(type, true);
    }

    private static RelDataType decimalType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 2);
    }

    private static RelDataType dateType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    }

    /**
     * A bounded VARCHAR, because the length is load-bearing here. Calcite's unbounded VARCHAR
     * carries precision {@code -1}, which no Flink logical type accepts.
     */
    private static RelDataType varcharType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 100);
    }

    /**
     * Declared in a static method so it captures no enclosing instance and stays serializable: the
     * generator serializes the function before it emits anything, and a capture would make this
     * decline for the wrong reason.
     */
    private static ScalarFunction anonymousFunction() {
        return new ScalarFunction() {
            private static final long serialVersionUID = 1L;

            /**
             * Marks its argument.
             *
             * @param s the argument
             * @return the marked argument
             */
            public String eval(String s) {
                return "ANON:" + s;
            }
        };
    }

    // ---- UDF fixtures ----

    /** Two overloads a string argument can bind to, producing different values. */
    public static class AmbiguousFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Renders any argument.
         *
         * @param o the argument
         * @return its string form, marked
         */
        public String eval(Object o) {
            return "OBJECT:" + o;
        }

        /**
         * Renders a string argument.
         *
         * @param s the argument
         * @return the argument, marked
         */
        public String eval(String s) {
            return "STRING:" + s;
        }
    }

    /** Two same-typed arguments rendered in declaration order, so a transposition changes the value. */
    public static class PairFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Joins its arguments in order.
         *
         * @param first the first argument
         * @param second the second argument
         * @return the two arguments joined by a bar
         */
        public String eval(String first, String second) {
            return first + "|" + second;
        }
    }

    /** Declares the external representation of {@code DECIMAL} on both sides of the call. */
    public static class DecimalFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Adds a tenth.
         *
         * @param value the argument
         * @return the argument plus {@code 0.10}
         */
        public BigDecimal eval(BigDecimal value) {
            return value.add(new BigDecimal("0.10"));
        }
    }

    /** Declares the external representation of {@code DATE} on both sides of the call. */
    public static class DateFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Advances by one day.
         *
         * @param value the argument
         * @return the day after the argument
         */
        public LocalDate eval(LocalDate value) {
            return value.plusDays(1);
        }
    }

    /** Returns null for a null argument, so a null travels in both directions through one call. */
    public static class NullReturningFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Marks its argument, or returns null when there is none.
         *
         * @param value the argument
         * @return the marked argument, or {@code null}
         */
        public String eval(String value) {
            return value == null ? null : "X:" + value;
        }
    }

    /** Mixed argument types, so a transposed argument changes the value. */
    public static class ConcatFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Appends the number to the string.
         *
         * @param s the string argument
         * @param i the number argument
         * @return the concatenation
         */
        public String eval(String s, int i) {
            return s + i;
        }
    }
}
