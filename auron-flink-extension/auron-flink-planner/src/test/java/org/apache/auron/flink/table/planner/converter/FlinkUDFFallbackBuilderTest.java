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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.apache.auron.flink.functions.FlinkUDFPayload;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalUDFWrapperExprNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link FlinkUDFFallbackBuilder}.
 *
 * <p>Every call under test is built with {@link ScalarSqlFunction}, the only user-function operator
 * with a constructor reachable outside a running planner. {@code BridgingSqlFunction} detection has
 * no test-constructible factory and is covered end to end by the integration cases instead.
 */
class FlinkUDFFallbackBuilderTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);
    private static final FlinkTypeFactory FLINK_TYPE_FACTORY =
            new FlinkTypeFactory(FlinkUDFFallbackBuilderTest.class.getClassLoader(), RelDataTypeSystem.DEFAULT);

    private FlinkNodeConverterFactory factory;
    private ConverterContext context;

    @BeforeEach
    void setUp() {
        factory = new FlinkNodeConverterFactory();
        factory.registerRexConverter(new RexInputRefConverter());
        factory.registerRexConverter(new RexLiteralConverter());
        factory.registerRexConverter(new RexCallConverter(factory));

        RowType inputType = RowType.of(
                new LogicalType[] {
                    new IntType(),
                    new IntType(),
                    new IntType(),
                    new VarCharType(),
                    new BooleanType(),
                    new TimeType(),
                    new TimestampType(3)
                },
                new String[] {"f0", "f1", "f2", "f3", "f4", "f5", "f6"});
        context = new ConverterContext(new Configuration(), null, getClass().getClassLoader(), inputType);
    }

    /** An admitted call emits the wrapper oneof, and param {@code i} is the converted operand {@code i}. */
    @Test
    void testEmitsWrapperNodeWithPositionalParams() {
        // Three same-typed operands with distinct column indices: a positional swap then shows up as
        // a wrong column index rather than dying on a type mismatch, which is what makes it visible.
        RexNode arg0 = intRef(0);
        RexNode arg1 = intRef(1);
        RexNode arg2 = intRef(2);
        RexCall call = udfCall(intType(), new ThreeIntFunction(), arg0, arg1, arg2);

        PhysicalExprNode node = buildFor(call).orElseThrow(AssertionError::new);

        assertTrue(node.hasUdfWrapperExpr(), "an admitted user function must emit the wrapper oneof");
        PhysicalUDFWrapperExprNode wrapper = node.getUdfWrapperExpr();
        assertEquals(3, wrapper.getParamsCount());
        assertEquals(convertAlone(arg0), wrapper.getParams(0));
        assertEquals(convertAlone(arg1), wrapper.getParams(1));
        assertEquals(convertAlone(arg2), wrapper.getParams(2));
        assertEquals(0, wrapper.getParams(0).getColumn().getIndex());
        assertEquals(1, wrapper.getParams(1).getColumn().getIndex());
        assertEquals(2, wrapper.getParams(2).getColumn().getIndex());
        assertEquals(call.toString(), wrapper.getExprString());
    }

    /**
     * Two wrapper nodes in one plan carry different payload bytes even when everything the payload
     * describes is identical.
     *
     * <p>The runtime keys its wrapper registry on those bytes, because the native callback carries
     * nothing else, so byte-equal payloads would collapse two call sites onto one user function
     * instance. Building the same call twice is the strongest form of the case: every input to the
     * payload matches, and only the per-node ordinal and the name the invoker was generated under
     * separate them.
     */
    @Test
    void testEachWrapperNodeGetsDistinctPayloadBytes() {
        RexCall call = udfCall(intType(), new OneIntFunction(), intRef(0));

        PhysicalExprNode first = buildFor(call).orElseThrow(AssertionError::new);
        PhysicalExprNode second = buildFor(call).orElseThrow(AssertionError::new);

        assertNotEquals(
                first.getUdfWrapperExpr().getSerialized(),
                second.getUdfWrapperExpr().getSerialized(),
                "two call sites sharing payload bytes would share one wrapper, and one function instance");
    }

    /** The wrapper's return type and nullability are read off the call, not off the function class. */
    @Test
    void testReturnTypeAndNullabilityMatchTheCall() {
        RelDataType nullableInt = TYPE_FACTORY.createTypeWithNullability(intType(), true);
        RelDataType notNullInt = TYPE_FACTORY.createTypeWithNullability(intType(), false);

        PhysicalUDFWrapperExprNode nullableWrapper = buildFor(udfCall(nullableInt, new OneIntFunction(), intRef(0)))
                .orElseThrow(AssertionError::new)
                .getUdfWrapperExpr();
        PhysicalUDFWrapperExprNode notNullWrapper = buildFor(udfCall(notNullInt, new OneIntFunction(), intRef(0)))
                .orElseThrow(AssertionError::new)
                .getUdfWrapperExpr();

        assertEquals(SchemaConverters.convertToAuronArrowType(new IntType()), nullableWrapper.getReturnType());
        assertTrue(nullableWrapper.getReturnNullable());
        assertFalse(notNullWrapper.getReturnNullable());
    }

    /**
     * The serialized blob round-trips to the argument and return types the call carried, in
     * argument order, and names the function class.
     *
     * <p>Those types are what the runtime builds its Arrow schemas from, so a transposed or dropped
     * entry surfaces there as a decode against the wrong column type rather than as a wrong value.
     */
    @Test
    void testBlobDeserializesToTheCallsTypes() throws Exception {
        RexCall call = udfCall(varcharType(), new ConcatFunction(), strRef(3), intRef(0));

        PhysicalUDFWrapperExprNode wrapper =
                buildFor(call).orElseThrow(AssertionError::new).getUdfWrapperExpr();
        FlinkUDFPayload payload = InstantiationUtil.deserializeObject(
                wrapper.getSerialized().toByteArray(), getClass().getClassLoader());

        DataType[] argTypes = payload.getArgTypes();
        assertEquals(2, argTypes.length);
        assertEquals(LogicalTypeRoot.VARCHAR, argTypes[0].getLogicalType().getTypeRoot());
        assertEquals(LogicalTypeRoot.INTEGER, argTypes[1].getLogicalType().getTypeRoot());
        assertEquals(
                LogicalTypeRoot.VARCHAR,
                payload.getReturnType().getLogicalType().getTypeRoot());
        assertEquals(ConcatFunction.class.getName(), payload.getUdfClassName());
    }

    /** A {@code TIME} argument is declined: the Auron Arrow type table cannot represent it at all. */
    @Test
    void testDeclinesTimeArgument() {
        RexCall call = udfCall(intType(), new TimeFunction(), REX_BUILDER.makeInputRef(timeType(), 5));

        assertFalse(buildFor(call).isPresent());
    }

    /** A {@code NULL} argument is declined; downstream only the Arrow reader would reject it. */
    @Test
    void testDeclinesNullArgument() {
        RexCall call = udfCall(intType(), new OneStringFunction(), REX_BUILDER.constantNull());

        assertFalse(buildFor(call).isPresent());
    }

    /** A {@code TIMESTAMP} argument is declined while the precision divergence is unresolved. */
    @Test
    void testDeclinesTimestampArgument() {
        RexCall call = udfCall(intType(), new TimestampFunction(), REX_BUILDER.makeInputRef(timestampType(), 6));

        assertFalse(buildFor(call).isPresent());
    }

    /**
     * Competing {@code eval} overloads are admitted. Which one runs is settled while the invocation
     * is generated, by the same resolution the query would get without Auron, so there is nothing
     * left here to guess at.
     */
    @Test
    void testAdmitsCompetingEvalOverloads() {
        RexCall call = udfCall(varcharType(), new AmbiguousFunction(), strRef(3));

        assertTrue(buildFor(call).isPresent());
    }

    /**
     * A Flink built-in reaches the detector as a {@code BuiltInFunctionDefinition} and must not be
     * read as a user function.
     *
     * <p>This is the branch every built-in fronted by {@code BridgingSqlFunction} takes, and nothing
     * else exercises it: the other cases all build calls on {@code ScalarSqlFunction}, because
     * {@code BridgingSqlFunction} has no factory reachable outside a running planner. A defect here
     * is not confined to user functions. Detection runs ahead of every kind check, so it would take
     * the whole Calc off the native path for ordinary built-ins.
     */
    @Test
    void testBuiltInDefinitionIsNotAUserScalarFunction() {
        assertFalse(FlinkUDFFallbackBuilder.userScalarFunctionOf(BuiltInFunctionDefinitions.IF_NULL)
                .isPresent());
    }

    /**
     * A UDF whose class is not public is declined. Flink's own validation is what rejects it, and
     * the generated source could not name such a class either.
     */
    @Test
    void testDeclinesUdfWithInaccessibleClass() {
        RexCall call = udfCall(intType(), new PackagePrivateFunction(), intRef(0));

        assertFalse(buildFor(call).isPresent());
    }

    /** A UDF that cannot be serialized is declined at plan time rather than failing the running job. */
    @Test
    void testDeclinesNonSerializableUdf() {
        RexCall call = udfCall(intType(), new NonSerializableFunction(), intRef(0));

        assertFalse(buildFor(call).isPresent());
    }

    /** A varargs eval is declined rather than guessed at. */
    @Test
    void testDeclinesVarargsEval() {
        RexCall call = udfCall(intType(), new VarargsFunction(), intRef(0));

        assertFalse(buildFor(call).isPresent());
    }

    /** A zero-argument eval is declined rather than sent through an untested zero-column batch. */
    @Test
    void testDeclinesZeroArgumentEval() {
        RexCall call = (RexCall)
                REX_BUILDER.makeCall(intType(), scalarSqlFunction(new NoArgFunction()), Collections.emptyList());

        assertFalse(buildFor(call).isPresent());
    }

    /** One unconvertible operand declines the whole call instead of emitting a wrapper missing a param. */
    @Test
    void testDeclinesWhenAnOperandDoesNotConvert() {
        // SIMILAR_TO is boolean-typed, so it clears the type gate, and is unsupported by
        // RexCallConverter, so the operand conversion itself is what fails.
        RexNode unconvertible = REX_BUILDER.makeCall(
                booleanType(), SqlStdOperatorTable.SIMILAR_TO, Arrays.asList(strRef(3), strRef(3)));
        RexCall call = udfCall(booleanType(), new OneBooleanFunction(), unconvertible);

        assertFalse(buildFor(call).isPresent());
    }

    // ---- Helpers ----

    /** Resolves the call's user function through the builder itself, then runs the admission gates. */
    private Optional<PhysicalExprNode> buildFor(RexCall call) {
        ScalarFunction udf = FlinkUDFFallbackBuilder.userScalarFunctionOf(call)
                .orElseThrow(() -> new AssertionError("fixture call must carry a user ScalarFunction"));
        return FlinkUDFFallbackBuilder.build(call, udf, context, factory);
    }

    /** Converts one operand on its own, as the reference value the wrapper param must equal. */
    private PhysicalExprNode convertAlone(RexNode operand) {
        return factory.convertRexNode(operand, context).orElseThrow(AssertionError::new);
    }

    private static ScalarSqlFunction scalarSqlFunction(ScalarFunction udf) {
        return new ScalarSqlFunction(
                FunctionIdentifier.of("test_udf"),
                "test_udf",
                udf,
                FLINK_TYPE_FACTORY,
                scala.Option.apply((SqlReturnTypeInference) null));
    }

    private static RexCall udfCall(RelDataType returnType, ScalarFunction udf, RexNode... operands) {
        return (RexCall) REX_BUILDER.makeCall(returnType, scalarSqlFunction(udf), Arrays.asList(operands));
    }

    private static RelDataType intType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    }

    /**
     * A bounded VARCHAR, because the length is load-bearing here. Calcite's unbounded VARCHAR
     * carries precision {@code -1}, which no Flink logical type accepts, so a fixture built from it
     * would decline for a reason unrelated to the check under test.
     */
    private static RelDataType varcharType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 100);
    }

    private static RelDataType booleanType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
    }

    private static RelDataType timeType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIME);
    }

    private static RelDataType timestampType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    }

    private static RexNode intRef(int index) {
        return REX_BUILDER.makeInputRef(intType(), index);
    }

    private static RexNode strRef(int index) {
        return REX_BUILDER.makeInputRef(varcharType(), index);
    }

    // ---- UDF fixtures ----

    /** Three same-typed arguments, so a positional swap shows up as a wrong column index. */
    public static class ThreeIntFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Packs the arguments by position.
         *
         * @param a first argument
         * @param b second argument
         * @param c third argument
         * @return the arguments packed by position
         */
        public int eval(int a, int b, int c) {
            return a * 100 + b * 10 + c;
        }
    }

    /** A minimal admissible function. */
    public static class OneIntFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Increments its argument.
         *
         * @param a the argument
         * @return {@code a + 1}
         */
        public int eval(int a) {
            return a + 1;
        }
    }

    /** Mixed argument types, so the payload's argument order is observable. */
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

    /** Declares a {@code TIME} argument, which the type gate must decline. */
    public static class TimeFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Returns the hour of the given time.
         *
         * @param t the time argument
         * @return the hour
         */
        public int eval(LocalTime t) {
            return t.getHour();
        }
    }

    /** Declares a {@code TIMESTAMP} argument, which the type gate must decline. */
    public static class TimestampFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Returns the year of the given timestamp.
         *
         * @param ts the timestamp argument
         * @return the year
         */
        public int eval(LocalDateTime ts) {
            return ts.getYear();
        }
    }

    /** A single string-argument function, used where the argument type is what is under test. */
    public static class OneStringFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Returns the length of its argument.
         *
         * @param s the argument
         * @return the length, or {@code -1} for null
         */
        public int eval(String s) {
            return s == null ? -1 : s.length();
        }
    }

    /** A single boolean-argument function, used where the operand is what is under test. */
    public static class OneBooleanFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Negates its argument.
         *
         * @param b the argument
         * @return the negation
         */
        public boolean eval(Boolean b) {
            return !Boolean.TRUE.equals(b);
        }
    }

    /** Two overloads a string argument can bind to. */
    public static class AmbiguousFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Renders any argument.
         *
         * @param o the argument
         * @return its string form
         */
        public String eval(Object o) {
            return String.valueOf(o);
        }

        /**
         * Renders a string argument.
         *
         * @param s the argument
         * @return the argument unchanged
         */
        public String eval(String s) {
            return s;
        }
    }

    /** Not public, which both instance preparation and a public method-handle lookup reject. */
    static class PackagePrivateFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Returns its argument.
         *
         * @param a the argument
         * @return the argument unchanged
         */
        public int eval(int a) {
            return a;
        }
    }

    /** Holds a non-serializable field, so instance preparation fails. */
    public static class NonSerializableFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        private final Object lock = new Object();

        /**
         * Returns its argument.
         *
         * @param a the argument
         * @return the argument unchanged
         */
        public int eval(int a) {
            synchronized (lock) {
                return a;
            }
        }
    }

    /** Declares a varargs eval. */
    public static class VarargsFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Sums its arguments.
         *
         * @param values the arguments
         * @return the sum
         */
        public int eval(int... values) {
            int sum = 0;
            for (int value : values) {
                sum += value;
            }
            return sum;
        }
    }

    /** Declares a zero-argument eval. */
    public static class NoArgFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Returns a constant.
         *
         * @return the constant
         */
        public int eval() {
            return 42;
        }
    }
}
