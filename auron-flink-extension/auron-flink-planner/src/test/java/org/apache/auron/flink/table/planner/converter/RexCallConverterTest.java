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

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalWhenThen;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
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
                    new IntType(), new BigIntType(), new BooleanType(), new BooleanType(), new BooleanType()
                },
                new String[] {"f0", "f1", "f2", "f3", "f4"});
        context = new ConverterContext(new Configuration(), null, getClass().getClassLoader(), inputType);
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
        RexNode cast = makeCall(bigintType(), SqlStdOperatorTable.CAST, makeIntRef(0));

        PhysicalExprNode result = converter.convert(cast, context);

        assertTrue(result.hasTryCast());
        assertTrue(result.getTryCast().getExpr().hasColumn());
        assertTrue(result.getTryCast().hasArrowType());
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

    // ---- Helpers ----

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

    /**
     * Creates a {@link org.apache.calcite.rex.RexCall} with an explicit
     * return type using the List-based {@code makeCall} overload.
     */
    private static RexNode makeCall(
            RelDataType returnType, org.apache.calcite.sql.SqlOperator op, RexNode... operands) {
        return REX_BUILDER.makeCall(returnType, op, Arrays.asList(operands));
    }
}
