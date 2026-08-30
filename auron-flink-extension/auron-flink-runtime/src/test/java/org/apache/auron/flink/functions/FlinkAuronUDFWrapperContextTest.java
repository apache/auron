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
package org.apache.auron.flink.functions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.Method;
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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FlinkAuronUDFWrapperContext}.
 *
 * <p>Each test drives {@link FlinkAuronUDFWrapperContext#eval(long, long)} across a real Arrow
 * C-Data FFI boundary with no native engine involved: the test plays the role the native side
 * plays in production, exporting the parameter columns into an {@link ArrowArray} and importing
 * the single result column back out of a second one.
 */
public class FlinkAuronUDFWrapperContextTest {

    /**
     * The repo-wide surefire config sets {@code java.io.tmpdir} to {@code target/tmp}, which does
     * not yet exist on a clean build. The Arrow C-Data JNI loader extracts its native library via
     * {@link java.io.File#createTempFile}, which fails if the directory is missing. Ensure it
     * exists before any test runs.
     */
    @BeforeAll
    public static void ensureTmpDirExists() {
        String tmp = System.getProperty("java.io.tmpdir");
        if (tmp != null) {
            new File(tmp).mkdirs();
        }
    }

    /**
     * Contract: types whose Flink-internal representation is already the UDF's external
     * representation ({@code BOOLEAN}, {@code INTEGER}, {@code BIGINT}, {@code DOUBLE},
     * {@code VARBINARY}) reach {@code eval} unchanged and the result travels back out.
     *
     * <p>The weights are distinct per argument so a dropped, duplicated or mis-read column changes
     * the sum.
     */
    @Test
    public void testIdentityConversionTypesRoundTrip() throws Exception {
        FlinkUDFPayload payload = new FlinkUDFPayload(
                new WeightedSumFunction(),
                new DataType[] {
                    DataTypes.BOOLEAN(), DataTypes.INT(), DataTypes.BIGINT(), DataTypes.DOUBLE(), DataTypes.BYTES()
                },
                DataTypes.BIGINT(),
                evalParameterTypeNames(WeightedSumFunction.class));

        List<RowData> rows = Arrays.asList(
                row(true, 7, 1_000_000_000_000L, 2.5d, new byte[] {1, 2, 3}),
                row(false, -3, 5L, -0.5d, new byte[] {7, 7}));

        List<Object> results = evalUdf(payload, rows, r -> r.isNullAt(0) ? null : r.getLong(0));

        // 1 + 2*7 + 3*1_000_000_000_000 + (long) (4 * 2.5) + 5*3
        // 0 + 2*-3 + 3*5 + (long) (4 * -0.5) + 5*2
        assertEquals(Arrays.<Object>asList(3_000_000_000_040L, 17L), results);
    }

    /**
     * Contract: types whose Flink-internal representation differs from the UDF's external
     * representation are converted on the way in and back on the way out — {@code StringData} to
     * {@link String}, {@code DecimalData} to {@link BigDecimal}, and an epoch-day {@code int} to
     * {@link LocalDate}.
     */
    @Test
    public void testConvertedTypesRoundTrip() throws Exception {
        FlinkUDFPayload payload = new FlinkUDFPayload(
                new ExternalTypesFunction(),
                new DataType[] {DataTypes.STRING(), DataTypes.DECIMAL(10, 2), DataTypes.DATE()},
                DataTypes.STRING(),
                evalParameterTypeNames(ExternalTypesFunction.class));

        List<RowData> rows = Arrays.asList(
                row(StringData.fromString("ab"), DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2), (int)
                        LocalDate.of(2024, 2, 29).toEpochDay()),
                row(StringData.fromString("x"), DecimalData.fromBigDecimal(new BigDecimal("-0.05"), 10, 2), (int)
                        LocalDate.of(1969, 12, 31).toEpochDay()));

        List<Object> results = evalUdf(
                payload, rows, r -> r.isNullAt(0) ? null : r.getString(0).toString());

        assertEquals(Arrays.<Object>asList("ab|12.34|2024-02-29", "x|-0.05|1969-12-31"), results);
    }

    /**
     * Contract: a null in any argument position reaches {@code eval} as a null argument — not only
     * in the first position — and a null returned by {@code eval} is written out as a null result.
     */
    @Test
    public void testNullsInEveryArgumentPositionAndNullResult() throws Exception {
        FlinkUDFPayload payload = new FlinkUDFPayload(
                new NullProbeFunction(),
                new DataType[] {DataTypes.STRING(), DataTypes.INT(), DataTypes.BIGINT()},
                DataTypes.STRING(),
                evalParameterTypeNames(NullProbeFunction.class));

        List<RowData> rows = Arrays.asList(
                row(null, 2, 3L),
                row(StringData.fromString("a"), null, 3L),
                row(StringData.fromString("a"), 2, null),
                row(null, null, null));

        List<Object> results = evalUdf(
                payload, rows, r -> r.isNullAt(0) ? null : r.getString(0).toString());

        assertEquals(Arrays.<Object>asList("-/2/3", "a/-/3", "a/2/-", null), results);
    }

    /**
     * Contract: an {@code eval} overload declared with primitive parameters is invoked
     * successfully even though the argument {@link DataType}'s conversion class is the boxed type.
     *
     * <p>Binding through {@code MethodHandles.Lookup#findVirtual} with a descriptor built from
     * {@link DataType#getConversionClass()} cannot find {@code eval(int)}, because the conversion
     * class of {@code DataTypes.INT()} is {@link Integer}. The payload here deliberately reproduces
     * that shape: boxed conversion classes on the {@link DataType}s, primitive names in
     * {@code evalParameterTypeNames}, primitives on the declared method.
     */
    @Test
    public void testPrimitiveParameterEvalIsBound() throws Exception {
        FlinkUDFPayload payload = new FlinkUDFPayload(
                new PrimitiveTripleFunction(), new DataType[] {DataTypes.INT()}, DataTypes.INT(), new String[] {"int"});

        List<Object> results =
                evalUdf(payload, Arrays.asList(row(5), row(-2)), r -> r.isNullAt(0) ? null : r.getInt(0));

        assertEquals(Arrays.<Object>asList(15, -6), results);
    }

    /**
     * Contract: parameter {@code i} of the input struct is passed as {@code eval} argument
     * {@code i}.
     *
     * <p>The first two arguments are both {@code STRING} with distinct values, so transposing them
     * changes the produced value instead of raising a {@link ClassCastException}. A silent
     * positional swap between two same-typed arguments is invisible to every other test in this
     * class, which is what this one exists to catch.
     */
    @Test
    public void testParametersBindPositionally() throws Exception {
        FlinkUDFPayload payload = FlinkUDFPayload.of(
                new PositionalFunction(),
                new DataType[] {DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT()},
                DataTypes.STRING(),
                evalMethod(PositionalFunction.class),
                0);

        List<RowData> rows = Arrays.asList(row(StringData.fromString("alpha"), StringData.fromString("beta"), 7));

        List<Object> results = evalUdf(
                payload, rows, r -> r.isNullAt(0) ? null : r.getString(0).toString());

        assertEquals(Arrays.<Object>asList("alpha|beta|7"), results);
    }

    /**
     * Contract: a failure raised inside {@code eval} surfaces as an exception whose message or
     * cause chain names the UDF class, so an error crossing back to the native side is
     * diagnosable.
     */
    @Test
    public void testEvalFailureNamesTheUdfClass() {
        FlinkUDFPayload payload = new FlinkUDFPayload(
                new ThrowingFunction(),
                new DataType[] {DataTypes.INT()},
                DataTypes.INT(),
                evalParameterTypeNames(ThrowingFunction.class));

        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> evalUdf(payload, Arrays.asList(row(1)), r -> r.isNullAt(0) ? null : r.getInt(0)));

        String chain = describeChain(thrown);
        assertTrue(
                chain.contains(ThrowingFunction.class.getSimpleName()),
                "eval failure must name the UDF class, but the exception chain was: " + chain);
    }

    /**
     * Contract: one context evaluates many batches. The {@code args} array, the output row and the
     * converters are reused across calls, so state left behind by one call must not change what the
     * next one produces.
     *
     * <p>The second batch is deliberately shorter than the first and repeats none of its values, so
     * a stale argument or a stale output field surfaces as a wrong value rather than being masked
     * by an identical replay.
     */
    @Test
    public void testRepeatedEvalOnOneContextStaysCorrect() throws Exception {
        FlinkUDFPayload payload = FlinkUDFPayload.of(
                new PositionalFunction(),
                new DataType[] {DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT()},
                DataTypes.STRING(),
                evalMethod(PositionalFunction.class),
                0);

        FlinkAuronUDFWrapperContext context = new FlinkAuronUDFWrapperContext(
                serialize(payload), FlinkAuronUDFWrapperContextTest.class.getClassLoader(), new FunctionContext(null));
        try {
            List<Object> first = evalOn(
                    context,
                    payload,
                    Arrays.asList(
                            row(StringData.fromString("a"), StringData.fromString("b"), 1),
                            row(StringData.fromString("c"), StringData.fromString("d"), 2)),
                    r -> r.isNullAt(0) ? null : r.getString(0).toString());
            assertEquals(Arrays.<Object>asList("a|b|1", "c|d|2"), first);

            List<Object> second = evalOn(
                    context,
                    payload,
                    Arrays.asList(row(StringData.fromString("e"), StringData.fromString("f"), 3)),
                    r -> r.isNullAt(0) ? null : r.getString(0).toString());
            assertEquals(Arrays.<Object>asList("e|f|3"), second);
        } finally {
            context.close();
        }
    }

    /**
     * Contract: the wrapper leaves nothing allocated in the shared root allocator once the native
     * side has taken the exported result, on the failing path as well as the succeeding one.
     *
     * <p>The wrapper allocates its two roots from {@link FlinkArrowUtils#getRootAllocator()} rather
     * than from a child, so a leak there is invisible to the per-test child allocators every other
     * test closes. A throwing UDF aborts {@code eval} between allocating the roots and exporting the
     * result, which is the path where a missed release would go unnoticed.
     */
    @Test
    public void testRootAllocatorIsReleasedOnBothSuccessAndFailure() throws Exception {
        FlinkUDFPayload ok = FlinkUDFPayload.of(
                new PrimitiveTripleFunction(),
                new DataType[] {DataTypes.INT()},
                DataTypes.INT(),
                evalMethod(PrimitiveTripleFunction.class),
                0);
        FlinkUDFPayload boom = FlinkUDFPayload.of(
                new ThrowingFunction(),
                new DataType[] {DataTypes.INT()},
                DataTypes.INT(),
                evalMethod(ThrowingFunction.class),
                0);

        long baseline = FlinkArrowUtils.getRootAllocator().getAllocatedMemory();

        for (int i = 0; i < 25; i++) {
            assertEquals(
                    Arrays.<Object>asList(3 * i),
                    evalUdf(ok, Arrays.asList(row(i)), r -> r.isNullAt(0) ? null : r.getInt(0)));
            assertThrows(
                    IllegalStateException.class,
                    () -> evalUdf(boom, Arrays.asList(row(1)), r -> r.isNullAt(0) ? null : r.getInt(0)));
        }

        assertEquals(
                baseline,
                FlinkArrowUtils.getRootAllocator().getAllocatedMemory(),
                "the wrapper must not retain root-allocator memory after eval returns");
    }

    // ------------------------------------------------------------------------------------------
    // UDF fixtures. Static nested so they capture no reference to the enclosing test instance and
    // stay serializable.
    // ------------------------------------------------------------------------------------------

    /** Sums its arguments under distinct weights so a lost or mis-ordered column changes the sum. */
    public static class WeightedSumFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Long eval(Boolean flag, Integer i, Long l, Double d, byte[] bytes) {
            return (flag ? 1L : 0L) + 2L * i + 3L * l + (long) (4L * d) + 5L * bytes.length;
        }
    }

    /** Accepts only external representations, proving conversion happened on the way in. */
    public static class ExternalTypesFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String s, BigDecimal decimal, LocalDate date) {
            return s + "|" + decimal.toPlainString() + "|" + date;
        }
    }

    /** Renders which argument positions arrived null; returns null when every argument is null. */
    public static class NullProbeFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String a, Integer b, Long c) {
            if (a == null && b == null && c == null) {
                return null;
            }
            return (a == null ? "-" : a) + "/" + (b == null ? "-" : b.toString()) + "/"
                    + (c == null ? "-" : c.toString());
        }
    }

    /** Declares primitive parameter and return types. */
    public static class PrimitiveTripleFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public int eval(int value) {
            return value * 3;
        }
    }

    /**
     * Renders its arguments in declaration order. The first two parameters share a type on purpose:
     * where parameter types are mutually incompatible a positional swap dies on a
     * {@link ClassCastException}, which any test would catch, rather than producing a wrong value.
     */
    public static class PositionalFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public String eval(String first, String second, Integer third) {
            return first + "|" + second + "|" + third;
        }
    }

    /** Always fails. */
    public static class ThrowingFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Integer eval(Integer value) {
            throw new IllegalStateException("deliberate UDF failure");
        }
    }

    // ------------------------------------------------------------------------------------------
    // Harness
    // ------------------------------------------------------------------------------------------

    /** Pulls the single result column out of one output row. */
    @FunctionalInterface
    private interface ResultExtractor {
        Object extract(RowData row);
    }

    /**
     * Runs one {@link FlinkAuronUDFWrapperContext#eval(long, long)} over the supplied rows and
     * returns the single result column, one entry per input row.
     *
     * <p>Mirrors what the native side does: the parameter columns are exported as a struct array
     * whose Arrow schema is {@code toArrowSchema(RowType.of(argLogicalTypes))}, and the result is
     * imported from an initially empty {@link ArrowArray} against
     * {@code toArrowSchema(RowType.of(returnLogicalType))}.
     */
    private static List<Object> evalUdf(FlinkUDFPayload payload, List<RowData> inputRows, ResultExtractor extractor)
            throws Exception {
        FlinkAuronUDFWrapperContext context = new FlinkAuronUDFWrapperContext(
                serialize(payload), FlinkAuronUDFWrapperContextTest.class.getClassLoader(), new FunctionContext(null));
        try {
            return evalOn(context, payload, inputRows, extractor);
        } finally {
            context.close();
        }
    }

    /**
     * Runs one {@code eval} on a caller-supplied context, so a test can drive the same instance more
     * than once. Otherwise identical to {@link #evalUdf}.
     */
    private static List<Object> evalOn(
            FlinkAuronUDFWrapperContext context,
            FlinkUDFPayload payload,
            List<RowData> inputRows,
            ResultExtractor extractor)
            throws Exception {
        DataType[] argTypes = payload.getArgTypes();
        LogicalType[] argLogicalTypes = new LogicalType[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            argLogicalTypes[i] = argTypes[i].getLogicalType();
        }
        RowType paramsRowType = RowType.of(argLogicalTypes);
        RowType resultRowType = RowType.of(payload.getReturnType().getLogicalType());

        List<Object> results = new ArrayList<>();
        try (BufferAllocator allocator = FlinkArrowUtils.createChildAllocator("udfWrapperTest")) {
            // Declared before the roots so they are closed last: an imported root must be released
            // while the ArrowArray struct it was imported from is still alive.
            try (ArrowArray inputArray = ArrowArray.allocateNew(allocator);
                    ArrowArray outputArray = ArrowArray.allocateNew(allocator)) {

                try (VectorSchemaRoot paramsRoot =
                        VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(paramsRowType), allocator)) {
                    FlinkArrowWriter writer = FlinkArrowWriter.create(paramsRoot, paramsRowType);
                    for (RowData row : inputRows) {
                        writer.write(row);
                    }
                    writer.finish();
                    Data.exportVectorSchemaRoot(allocator, paramsRoot, null, inputArray);
                }

                context.eval(inputArray.memoryAddress(), outputArray.memoryAddress());

                try (VectorSchemaRoot resultRoot =
                                VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(resultRowType), allocator);
                        CDataDictionaryProvider provider = new CDataDictionaryProvider()) {
                    Data.importIntoVectorSchemaRoot(allocator, outputArray, resultRoot, provider);
                    try (FlinkArrowReader reader = FlinkArrowReader.create(resultRoot, resultRowType)) {
                        for (int i = 0; i < resultRoot.getRowCount(); i++) {
                            results.add(extractor.extract(reader.read(i)));
                        }
                    }
                }
            }
        }
        return results;
    }

    /** Serializes the payload the way the planner does. */
    private static byte[] serialize(FlinkUDFPayload payload) throws Exception {
        return InstantiationUtil.serializeObject(payload);
    }

    /**
     * Reads the declared parameter type names off the fixture's single {@code eval} overload, in
     * the {@link Class#getName()} form the payload carries ({@code "int"}, {@code "[B"},
     * {@code "java.lang.String"}).
     */
    private static Method evalMethod(Class<? extends ScalarFunction> udfClass) {
        for (Method method : udfClass.getDeclaredMethods()) {
            if ("eval".equals(method.getName())) {
                return method;
            }
        }
        throw new IllegalStateException("no eval method declared on " + udfClass.getName());
    }

    private static String[] evalParameterTypeNames(Class<? extends ScalarFunction> udfClass) {
        for (Method method : udfClass.getDeclaredMethods()) {
            if ("eval".equals(method.getName())) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                String[] names = new String[parameterTypes.length];
                for (int i = 0; i < parameterTypes.length; i++) {
                    names[i] = parameterTypes[i].getName();
                }
                return names;
            }
        }
        throw new IllegalStateException("no eval method declared on " + udfClass.getName());
    }

    private static RowData row(Object... fields) {
        GenericRowData row = new GenericRowData(fields.length);
        for (int i = 0; i < fields.length; i++) {
            row.setField(i, fields[i]);
        }
        return row;
    }

    private static String describeChain(Throwable thrown) {
        StringBuilder sb = new StringBuilder();
        Throwable current = thrown;
        while (current != null) {
            sb.append(current).append(" | ");
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return sb.toString();
    }
}
