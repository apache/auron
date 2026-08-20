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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.auron.protobuf.ArrowType;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalScalarFunctionNode;
import org.apache.auron.protobuf.PhysicalWhenThen;
import org.apache.auron.protobuf.ScalarFunction;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link RexCallConverter}. */
class RexCallConverterTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private FlinkNodeConverterFactory factory;
    private RexCallConverter converter;
    private ConverterContext context;

    @BeforeEach
    void setUp() {
        factory = new FlinkNodeConverterFactory();
        converter = new RexCallConverter(factory);
        factory.registerRexConverter(new RexInputRefConverter());
        factory.registerRexConverter(new RexLiteralConverter());
        factory.registerRexConverter(converter);

        RowType inputType = RowType.of(
                new LogicalType[] {
                    new IntType(),
                    new BigIntType(),
                    new BooleanType(),
                    new BooleanType(),
                    new BooleanType(),
                    new VarCharType(),
                    new VarCharType()
                },
                new String[] {"f0", "f1", "f2", "f3", "f4", "f5", "f6"});
        // The session time zone is pinned rather than left unset, because a converter that reads it
        // rejects ids the native side cannot resolve. An unset zone resolves to the machine's
        // default, which would make those cases depend on where the suite runs.
        Configuration conf = new Configuration();
        conf.setString("table.local-time-zone", "UTC");
        context = new ConverterContext(conf, null, getClass().getClassLoader(), inputType);
    }

    @Test
    void testGetNodeClass() {
        assertEquals(RexCall.class, converter.getNodeClass());
    }

    @Test
    void testConvertPlus() {
        RexNode plus = makeCall(intType(), SqlStdOperatorTable.PLUS, makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(plus, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("Plus", result.getBinaryExpr().getOp());
    }

    @Test
    void testConvertMinus() {
        RexNode minus = makeCall(intType(), SqlStdOperatorTable.MINUS, makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(minus, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("Minus", result.getBinaryExpr().getOp());
    }

    @Test
    void testConvertTimes() {
        RexNode times = makeCall(intType(), SqlStdOperatorTable.MULTIPLY, makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(times, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("Multiply", result.getBinaryExpr().getOp());
    }

    @Test
    void testConvertDivide() {
        RexNode divide = makeCall(intType(), SqlStdOperatorTable.DIVIDE, makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(divide, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("Divide", result.getBinaryExpr().getOp());
    }

    @Test
    void testConvertMod() {
        RexNode mod = makeCall(intType(), SqlStdOperatorTable.MOD, makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(mod, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("Modulo", result.getBinaryExpr().getOp());
    }

    @Test
    void testConvertUnaryMinus() {
        RexNode neg = REX_BUILDER.makeCall(SqlStdOperatorTable.UNARY_MINUS, makeIntRef(0));

        PhysicalExprNode result = converter.convert(neg, context);

        assertTrue(result.hasNegative());
        assertTrue(result.getNegative().getExpr().hasColumn());
    }

    @Test
    void testConvertUnaryPlus() {
        RexNode pos = REX_BUILDER.makeCall(SqlStdOperatorTable.UNARY_PLUS, makeIntRef(0));

        PhysicalExprNode result = converter.convert(pos, context);

        // Unary plus is identity — passthrough to operand
        assertTrue(result.hasColumn());
        assertEquals("f0", result.getColumn().getName());
    }

    @Test
    void testConvertCast() {
        // Explicit CAST routes to the strict cast node (throws on bad conversion).
        RexNode cast = makeCall(bigintType(), SqlStdOperatorTable.CAST, makeIntRef(0));

        PhysicalExprNode result = converter.convert(cast, context);

        assertTrue(result.hasCast());
        assertTrue(result.getCast().getExpr().hasColumn());
        assertTrue(result.getCast().hasArrowType());
    }

    @Test
    void testConvertTryCast() {
        // TRY_CAST has SqlKind OTHER_FUNCTION; it is matched by operator identity
        // and converts to a try-cast node (null-on-failure semantics).
        RexNode tryCast = makeCall(bigintType(), FlinkSqlOperatorTable.TRY_CAST, makeIntRef(0));

        PhysicalExprNode result = converter.convert(tryCast, context);

        assertTrue(result.hasTryCast());
        assertTrue(result.getTryCast().getExpr().hasColumn());
        assertTrue(result.getTryCast().hasArrowType());
    }

    @Test
    void testTryCastIsSupported() {
        RexNode tryCast = makeCall(bigintType(), FlinkSqlOperatorTable.TRY_CAST, makeIntRef(0));

        assertTrue(converter.isSupported(tryCast, context));
    }

    @Test
    void testCastToUnsupportedTypeFallsBack() {
        // INT -> DATE is outside the conservative supported set → not supported.
        RexNode cast = makeCall(dateType(), SqlStdOperatorTable.CAST, makeIntRef(0));

        assertFalse(converter.isSupported(cast, context));
    }

    @Test
    void testTryCastToUnsupportedTypeFallsBack() {
        // INT -> DATE is outside the conservative supported set → not supported.
        RexNode tryCast = makeCall(dateType(), FlinkSqlOperatorTable.TRY_CAST, makeIntRef(0));

        assertFalse(converter.isSupported(tryCast, context));
    }

    @Test
    void testCastNumericToNumeric() {
        RexNode cast = makeCall(bigintType(), SqlStdOperatorTable.CAST, makeIntRef(0));

        assertTrue(converter.isSupported(cast, context));
    }

    @Test
    void testCastNumericToString() {
        RexNode cast = makeCall(varcharType(), SqlStdOperatorTable.CAST, makeIntRef(0));

        assertTrue(converter.isSupported(cast, context));
    }

    @Test
    void testCastStringToNumeric() {
        RexNode cast = makeCall(intType(), SqlStdOperatorTable.CAST, strRef(5));

        assertTrue(converter.isSupported(cast, context));
    }

    @Test
    void testCastBooleanToString() {
        RexNode cast = makeCall(varcharType(), SqlStdOperatorTable.CAST, makeBoolRef(2));

        assertTrue(converter.isSupported(cast, context));
    }

    @Test
    void testCastStringToDecimal() {
        RexNode cast = makeCall(decimalType(), SqlStdOperatorTable.CAST, strRef(5));

        assertTrue(converter.isSupported(cast, context));
    }

    @Test
    void testConvertMixedTypePromotion() {
        // INT (f0) + BIGINT (f1) — left operand should be promoted
        RexNode intRef = makeIntRef(0);
        RexNode bigintRef = REX_BUILDER.makeInputRef(bigintType(), 1);
        RexNode mixedPlus = makeCall(bigintType(), SqlStdOperatorTable.PLUS, intRef, bigintRef);

        PhysicalExprNode result = converter.convert(mixedPlus, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("Plus", result.getBinaryExpr().getOp());
        // Left operand (INT) should be wrapped in TryCast to BIGINT
        PhysicalExprNode left = result.getBinaryExpr().getL();
        assertTrue(left.hasTryCast(), "Left operand should be cast from INT to BIGINT");
        // Right operand (BIGINT) should be plain column
        PhysicalExprNode right = result.getBinaryExpr().getR();
        assertTrue(right.hasColumn(), "Right operand should be plain column (already BIGINT)");
    }

    @Test
    void testConvertOutputTypeCast() {
        // INT + INT where output type is BIGINT → result wrapped in TryCast
        RexNode plus = makeCall(bigintType(), SqlStdOperatorTable.PLUS, makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(plus, context);

        // Both operands are INT, compatible type is INT,
        // but output type is BIGINT → outer TryCast
        assertTrue(result.hasTryCast(), "Result should be wrapped in TryCast when output " + "!= compatible type");
        assertTrue(result.getTryCast().getExpr().hasBinaryExpr());
    }

    @Test
    void testConvertNestedExpr() {
        // (f0 + 1) * f0
        RexNode f0 = makeIntRef(0);
        RexNode one = REX_BUILDER.makeExactLiteral(BigDecimal.ONE, intType());
        RexNode innerPlus = makeCall(intType(), SqlStdOperatorTable.PLUS, f0, one);
        RexNode outer = makeCall(intType(), SqlStdOperatorTable.MULTIPLY, innerPlus, makeIntRef(0));

        PhysicalExprNode result = converter.convert(outer, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("Multiply", result.getBinaryExpr().getOp());
        // Left child is the inner (f0 + 1)
        PhysicalExprNode leftChild = result.getBinaryExpr().getL();
        assertTrue(leftChild.hasBinaryExpr());
        assertEquals("Plus", leftChild.getBinaryExpr().getOp());
    }

    @Test
    void testIsSupportedNumericArithmetic() {
        RexNode plus = makeCall(intType(), SqlStdOperatorTable.PLUS, makeIntRef(0), makeIntRef(0));

        assertTrue(converter.isSupported(plus, context));
    }

    @Test
    void testIsNotSupportedNonNumericKind() {
        // SIMILAR_TO is not in the supported set
        RexNode similar = REX_BUILDER.makeCall(SqlStdOperatorTable.SIMILAR_TO, makeIntRef(0), makeIntRef(0));

        assertFalse(converter.isSupported(similar, context));
    }

    @Test
    void testConvertEquals() {
        assertComparison(SqlStdOperatorTable.EQUALS, "Eq");
    }

    @Test
    void testConvertNotEquals() {
        assertComparison(SqlStdOperatorTable.NOT_EQUALS, "NotEq");
    }

    @Test
    void testConvertGreaterThan() {
        assertComparison(SqlStdOperatorTable.GREATER_THAN, "Gt");
    }

    @Test
    void testConvertLessThan() {
        assertComparison(SqlStdOperatorTable.LESS_THAN, "Lt");
    }

    @Test
    void testConvertGreaterThanOrEqual() {
        assertComparison(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "GtEq");
    }

    @Test
    void testConvertLessThanOrEqual() {
        assertComparison(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "LtEq");
    }

    @Test
    void testConvertComparisonPromotesOperands() {
        // INT (f0) = BIGINT (f1): the INT operand is promoted to BIGINT,
        // and the comparison result is a plain BINARY expr (no outer result cast).
        RexNode intRef = makeIntRef(0);
        RexNode bigintRef = REX_BUILDER.makeInputRef(bigintType(), 1);
        RexNode eq = makeCall(boolType(), SqlStdOperatorTable.EQUALS, intRef, bigintRef);

        PhysicalExprNode result = converter.convert(eq, context);

        assertTrue(result.hasBinaryExpr(), "Top-level node must be a plain binary expr (no outer TryCast)");
        assertEquals("Eq", result.getBinaryExpr().getOp());
        PhysicalExprNode left = result.getBinaryExpr().getL();
        assertTrue(left.hasTryCast(), "Left operand (INT) should be cast to BIGINT");
        PhysicalExprNode right = result.getBinaryExpr().getR();
        assertTrue(right.hasColumn(), "Right operand (BIGINT) should be a plain column");
    }

    @Test
    void testConvertAndTwoOperands() {
        RexNode and = makeCall(booleanType(), SqlStdOperatorTable.AND, makeBoolRef(2), makeBoolRef(3));

        PhysicalExprNode result = converter.convert(and, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("And", result.getBinaryExpr().getOp());
        assertTrue(result.getBinaryExpr().getL().hasColumn());
        assertTrue(result.getBinaryExpr().getR().hasColumn());
    }

    @Test
    void testConvertAndThreeOperands() {
        // AND(f2, f3, f4) folds left-deep to ((f2 AND f3) AND f4)
        RexNode and = makeCall(booleanType(), SqlStdOperatorTable.AND, makeBoolRef(2), makeBoolRef(3), makeBoolRef(4));

        PhysicalExprNode result = converter.convert(and, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("And", result.getBinaryExpr().getOp());
        // Left child is the inner (f2 AND f3); right child is f4
        PhysicalExprNode left = result.getBinaryExpr().getL();
        assertTrue(left.hasBinaryExpr());
        assertEquals("And", left.getBinaryExpr().getOp());
        assertTrue(result.getBinaryExpr().getR().hasColumn());
    }

    @Test
    void testConvertOr() {
        RexNode or = makeCall(booleanType(), SqlStdOperatorTable.OR, makeBoolRef(2), makeBoolRef(3));

        PhysicalExprNode result = converter.convert(or, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals("Or", result.getBinaryExpr().getOp());
    }

    @Test
    void testConvertNot() {
        RexNode not = makeCall(booleanType(), SqlStdOperatorTable.NOT, makeBoolRef(2));

        PhysicalExprNode result = converter.convert(not, context);

        assertTrue(result.hasNotExpr());
        assertTrue(result.getNotExpr().getExpr().hasColumn());
    }

    @Test
    void testConvertIsNull() {
        RexNode isNull = makeCall(booleanType(), SqlStdOperatorTable.IS_NULL, makeIntRef(0));

        PhysicalExprNode result = converter.convert(isNull, context);

        assertTrue(result.hasIsNullExpr());
        assertTrue(result.getIsNullExpr().getExpr().hasColumn());
    }

    @Test
    void testConvertIsNotNull() {
        RexNode isNotNull = makeCall(booleanType(), SqlStdOperatorTable.IS_NOT_NULL, makeIntRef(0));

        PhysicalExprNode result = converter.convert(isNotNull, context);

        assertTrue(result.hasIsNotNullExpr());
        assertTrue(result.getIsNotNullExpr().getExpr().hasColumn());
    }

    @Test
    void testConvertCaseNoCast() {
        // CASE WHEN f2 THEN f0 ELSE f0 END — all branches INT, result INT → no cast
        RexNode caseExpr = makeCall(intType(), SqlStdOperatorTable.CASE, makeBoolRef(2), makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(caseExpr, context);

        assertTrue(result.hasCase());
        assertEquals(1, result.getCase().getWhenThenExprCount());
        // Searched CASE leaves the simple-CASE expr unset.
        assertFalse(result.getCase().hasExpr());
        PhysicalWhenThen whenThen = result.getCase().getWhenThenExpr(0);
        assertTrue(whenThen.getWhenExpr().hasColumn());
        // then is plain column (INT == result INT), not cast-wrapped.
        assertTrue(whenThen.getThenExpr().hasColumn());
        assertFalse(whenThen.getThenExpr().hasTryCast());
        assertTrue(result.getCase().hasElseExpr());
        assertTrue(result.getCase().getElseExpr().hasColumn());
        assertFalse(result.getCase().getElseExpr().hasTryCast());
    }

    @Test
    void testConvertCaseWithBranchCast() {
        // CASE WHEN f2 THEN f0(INT) ELSE f0(INT) END with result BIGINT → branches cast to BIGINT
        RexNode caseExpr =
                makeCall(bigintType(), SqlStdOperatorTable.CASE, makeBoolRef(2), makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(caseExpr, context);

        assertTrue(result.hasCase());
        PhysicalWhenThen whenThen = result.getCase().getWhenThenExpr(0);
        assertTrue(whenThen.getThenExpr().hasTryCast(), "then INT should be cast to result BIGINT");
        assertTrue(result.getCase().getElseExpr().hasTryCast(), "else INT should be cast to result BIGINT");
    }

    @Test
    void testConvertCaseMultipleBranches() {
        // CASE WHEN f2 THEN f0 WHEN f3 THEN f0 ELSE f0 END → two when/then branches
        RexNode caseExpr = makeCall(
                intType(),
                SqlStdOperatorTable.CASE,
                makeBoolRef(2),
                makeIntRef(0),
                makeBoolRef(3),
                makeIntRef(0),
                makeIntRef(0));

        PhysicalExprNode result = converter.convert(caseExpr, context);

        assertTrue(result.hasCase());
        assertEquals(2, result.getCase().getWhenThenExprCount());
        assertTrue(result.getCase().hasElseExpr());
    }

    @Test
    void testConvertLike() {
        RexNode like = makeCall(boolType(), SqlStdOperatorTable.LIKE, strRef(5), strRef(6));

        PhysicalExprNode result = converter.convert(like, context);

        assertTrue(result.hasLikeExpr());
        assertFalse(result.getLikeExpr().getNegated(), "Plain LIKE must not be negated");
        assertFalse(result.getLikeExpr().getCaseInsensitive(), "LIKE is case-sensitive");
        assertTrue(result.getLikeExpr().hasExpr(), "Expr operand must be present");
        assertTrue(result.getLikeExpr().hasPattern(), "Pattern operand must be present");
    }

    @Test
    void testNotLikeConvertsAsNotOfLike() {
        // Calcite never builds a negated LIKE RexCall (RexCall.<init> rejects a negated
        // SqlLikeOperator via validRexOperands). At the Rex layer x NOT LIKE y is
        // NOT(x LIKE y), which converts to a NOT wrapping the un-negated like node.
        RexNode like = makeCall(boolType(), SqlStdOperatorTable.LIKE, strRef(5), strRef(6));
        RexNode notLike = REX_BUILDER.makeCall(SqlStdOperatorTable.NOT, like);

        assertTrue(converter.isSupported(notLike, context));
        PhysicalExprNode result = converter.convert(notLike, context);
        assertTrue(result.hasNotExpr());
        assertTrue(result.getNotExpr().getExpr().hasLikeExpr(), "NOT wraps the like node");
        assertFalse(result.getNotExpr().getExpr().getLikeExpr().getNegated(), "Inner like node stays un-negated");
    }

    @Test
    void testLikeWithExplicitEscapeIsUnsupported() {
        // 3-operand LIKE (expr, pattern, ESCAPE) has no native escape field → falls back.
        RexNode escapeLike = makeCall(boolType(), SqlStdOperatorTable.LIKE, strRef(5), strRef(6), strRef(5));

        assertFalse(converter.isSupported(escapeLike, context));
    }

    // ---- UNIX_TIMESTAMP ----

    @Test
    void testUnixTimestampNodeShape() throws IOException {
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5));

        PhysicalExprNode result = converter.convert(call, context);

        assertTrue(result.hasScalarFunction());
        PhysicalScalarFunctionNode fn = result.getScalarFunction();
        assertEquals("Flink_UnixTimestamp", fn.getName());
        assertEquals(ScalarFunction.AuronExtFunctions, fn.getFun());
        assertEquals(3, fn.getArgsCount(), "Args are always [value, chronoFormat, zoneId]");
        assertEquals(ArrowType.ArrowTypeEnumCase.INT64, fn.getReturnType().getArrowTypeEnumCase());
        assertTrue(fn.getArgs(0).hasColumn(), "First arg is the converted value operand");
        assertTrue(fn.getArgs(1).hasLiteral(), "Second arg is the format literal");
        assertTrue(fn.getArgs(2).hasLiteral(), "Third arg is the zone literal");
        // The default single-arg format translates to the native specifier string.
        assertEquals("%Y-%m-%d %H:%M:%S", decodeStringLiteral(fn.getArgs(1)));
    }

    @Test
    void testUnixTimestampLiteralFormatIsSupportedAndTranslated() throws IOException {
        RexNode fmt = REX_BUILDER.makeLiteral("yyyy/MM/dd");
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5), fmt);

        assertTrue(converter.isSupported(call, context));
        PhysicalExprNode result = converter.convert(call, context);
        assertEquals("%Y/%m/%d", decodeStringLiteral(result.getScalarFunction().getArgs(1)));
    }

    /** The session timezone read from {@code TableConfig} reaches the node's third argument. This is
     * the converter-level half of the config-propagation contract the shadowed {@code StreamExecCalc}
     * enables. */
    @Test
    void testUnixTimestampTimezonePropagatesToNode() throws IOException {
        ConverterContext tzContext = contextWithZone("Asia/Shanghai");
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5));

        PhysicalExprNode result = converter.convert(call, tzContext);

        assertEquals(
                "Asia/Shanghai", decodeStringLiteral(result.getScalarFunction().getArgs(2)));
    }

    /** A fixed-offset session zone names an offset rather than a region: the native function parses
     * the offset instead of looking it up, so the gate admits it. The id reaches the node verbatim,
     * because the two sides have to agree on the spelling and not merely on the offset it denotes. */
    @Test
    void testUnixTimestampFixedOffsetZoneIsSupported() throws IOException {
        ConverterContext tzContext = contextWithZone("GMT-08:00");
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5));

        assertTrue(converter.isSupported(call, tzContext));
        PhysicalExprNode result = converter.convert(call, tzContext);
        assertEquals("GMT-08:00", decodeStringLiteral(result.getScalarFunction().getArgs(2)));
    }

    /** A legacy {@code SystemV/*} session zone still resolves in the JDK, so it reaches the gate
     * looking like an ordinary region id, but the native lookup does not carry it. */
    @Test
    void testUnixTimestampSystemVZoneFallsBack() {
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5));

        assertFalse(converter.isSupported(call, contextWithZone("SystemV/PST8")));
    }

    /** The zero-argument form parses no operand, so it converts to a distinct native function that
     * takes no arguments at all rather than to the string-parsing one. */
    @Test
    void testUnixTimestampZeroArgNodeShape() {
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP);

        PhysicalExprNode result = converter.convert(call, context);

        assertTrue(result.hasScalarFunction());
        PhysicalScalarFunctionNode fn = result.getScalarFunction();
        assertEquals("Flink_UnixTimestampNow", fn.getName());
        assertEquals(ScalarFunction.AuronExtFunctions, fn.getFun());
        assertEquals(0, fn.getArgsCount(), "The zero-argument form carries no native argument");
        assertEquals(ArrowType.ArrowTypeEnumCase.INT64, fn.getReturnType().getArrowTypeEnumCase());
    }

    /** The zero-argument result is epoch seconds, which no session time zone bears on, so the gate
     * admits it even under a zone the native lookup cannot resolve. The zone has to be one the gate
     * actually rejects, or the test passes without exercising the ordering it pins. */
    @Test
    void testUnixTimestampZeroArgSupportedWithUnresolvableZone() {
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP);

        assertTrue(converter.isSupported(call, contextWithZone("SystemV/PST8")));
    }

    /**
     * The gate must admit every {@code GMT±HH:MM} id, because the native function resolves every one
     * of them. Admitting a zone it cannot resolve is worse than rejecting one it can: the native
     * call returns an error, the unwind handler turns that into a default-valued batch, and the
     * query yields no rows while recording no fallback.
     *
     * <p>The family is swept in full rather than sampled, because where the boundary falls is the
     * whole content of the predicate. The ids are built from the loop indices under
     * {@link Locale#ROOT}, so a locale with non-ASCII digits cannot generate ids that fail the
     * ASCII-only pattern for a reason unrelated to the gate.
     */
    @Test
    void testUnixTimestampZoneGateAdmitsEveryFixedOffsetZone() {
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5));

        int covered = 0;
        for (int sign = -1; sign <= 1; sign += 2) {
            for (int hh = 0; hh <= 18; hh++) {
                for (int mm = 0; mm <= 59; mm++) {
                    int magnitude = hh * 3600 + mm * 60;
                    // ±00:00 normalizes to plain GMT and never reaches the gate spelled this way;
                    // anything past ±18:00 is not a zone id at all.
                    if (magnitude == 0 || magnitude > 18 * 3600) {
                        continue;
                    }
                    String zoneId = String.format(Locale.ROOT, "GMT%s%02d:%02d", sign > 0 ? "+" : "-", hh, mm);
                    assertTrue(converter.isSupported(call, contextWithZone(zoneId)), zoneId);
                    covered++;
                }
            }
        }
        assertEquals(2160, covered, "a skipped sweep would pass vacuously");
    }

    /**
     * The gate narrows rather than disappearing: the {@code SystemV/*} family is absent from the
     * time zone database the native side consults, so every one of its ids must still fall back.
     *
     * <p>The family is swept from the JDK's own id set rather than a fixed list, so it covers
     * whatever the runtime carries. How many ids that is comes from the bundled tzdb and can change
     * without the gate's behavior changing, so the sweep asserts only that it was not vacuous.
     */
    @Test
    void testUnixTimestampZoneGateRejectsEverySystemVZone() {
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5));

        int covered = 0;
        for (String zoneId : ZoneId.getAvailableZoneIds()) {
            if (zoneId.startsWith("SystemV/")) {
                assertFalse(converter.isSupported(call, contextWithZone(zoneId)), zoneId);
                covered++;
            }
        }
        assertTrue(covered > 0, "a skipped sweep would pass vacuously");
    }

    /**
     * The bare {@code ±HH:MM} spelling cannot be configured — Flink's validator rejects it — but an
     * unset session zone resolves to {@link ZoneId#systemDefault()} without passing through that
     * validator, and a process running under {@code TZ=EST}, {@code MST} or {@code HST} lands on
     * exactly these three ids. The native parser treats the {@code GMT} prefix as optional for this
     * reason, so the gate has to as well.
     *
     * <p>Each iteration asserts the id it actually reached before asserting the verdict, so the
     * sweep cannot silently degrade into testing the machine's own time zone three times.
     */
    @Test
    void testUnixTimestampZoneGateAdmitsTheBareOffsetsReachableFromTheSystemDefault() {
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5));

        Map<String, String> shortIdZones = new LinkedHashMap<>();
        shortIdZones.put("EST", "-05:00");
        shortIdZones.put("MST", "-07:00");
        shortIdZones.put("HST", "-10:00");
        TimeZone savedDefault = TimeZone.getDefault();
        try {
            for (Map.Entry<String, String> entry : shortIdZones.entrySet()) {
                TimeZone.setDefault(TimeZone.getTimeZone(entry.getKey()));
                assertEquals(entry.getValue(), ZoneId.systemDefault().getId());
                assertTrue(converter.isSupported(call, contextWithUnsetZone()), entry.getValue());
            }
        } finally {
            TimeZone.setDefault(savedDefault);
        }
    }

    @Test
    void testUnixTimestampNonLiteralFormatFallsBack() {
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5), strRef(6));

        assertFalse(converter.isSupported(call, context));
    }

    @Test
    void testUnixTimestampUnsupportedFormatTokenFallsBack() {
        RexNode fmt = REX_BUILDER.makeLiteral("yyyy-MM-dd z");
        RexNode call = makeCall(bigintType(), FlinkSqlOperatorTable.UNIX_TIMESTAMP, strRef(5), fmt);

        assertFalse(converter.isSupported(call, context));
    }

    // ---- Helpers ----

    /** Returns a copy of the shared context whose session time zone is {@code zoneId}. */
    private ConverterContext contextWithZone(String zoneId) {
        Configuration conf = new Configuration();
        conf.setString("table.local-time-zone", zoneId);
        return new ConverterContext(conf, null, getClass().getClassLoader(), context.getInputType());
    }

    /**
     * Returns a copy of the shared context with no session time zone configured, which resolves to
     * {@link ZoneId#systemDefault()} without passing through Flink's zone-id validator. This is the
     * only path by which an id the validator rejects can reach the converter.
     */
    private ConverterContext contextWithUnsetZone() {
        return new ConverterContext(new Configuration(), null, getClass().getClassLoader(), context.getInputType());
    }

    private static String decodeStringLiteral(PhysicalExprNode node) throws IOException {
        byte[] bytes = node.getLiteral().getIpcBytes().toByteArray();
        try (BufferAllocator alloc = new RootAllocator(Long.MAX_VALUE);
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), alloc)) {
            reader.loadNextBatch();
            VarCharVector vec = (VarCharVector) reader.getVectorSchemaRoot().getVector(0);
            return vec.getObject(0).toString();
        }
    }

    private void assertComparison(org.apache.calcite.sql.SqlOperator op, String expectedOp) {
        RexNode call = makeCall(boolType(), op, makeIntRef(0), makeIntRef(0));

        PhysicalExprNode result = converter.convert(call, context);

        assertTrue(result.hasBinaryExpr());
        assertEquals(expectedOp, result.getBinaryExpr().getOp());
        assertTrue(result.getBinaryExpr().hasL(), "Left operand must be present");
        assertTrue(result.getBinaryExpr().hasR(), "Right operand must be present");
    }

    private static RelDataType boolType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
    }

    private static RexNode makeIntRef(int index) {
        return REX_BUILDER.makeInputRef(intType(), index);
    }

    private static RexNode makeBoolRef(int index) {
        return REX_BUILDER.makeInputRef(booleanType(), index);
    }

    private static RelDataType intType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    }

    private static RelDataType bigintType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
    }

    private static RelDataType booleanType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
    }

    private static RexNode strRef(int index) {
        return REX_BUILDER.makeInputRef(varcharType(), index);
    }

    private static RelDataType varcharType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);
    }

    private static RelDataType decimalType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 2);
    }

    private static RelDataType dateType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.DATE);
    }

    /**
     * Creates a {@link org.apache.calcite.rex.RexCall} with an explicit
     * return type using the List-based {@code makeCall} overload.
     */
    private static RexNode makeCall(
            RelDataType returnType, org.apache.calcite.sql.SqlOperator op, RexNode... operands) {
        return REX_BUILDER.makeCall(returnType, op, Arrays.asList(operands));
    }
}
