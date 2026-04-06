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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.auron.protobuf.PhysicalExprNode;
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

        RowType inputType = RowType.of(new LogicalType[] {new IntType(), new BigIntType()}, new String[] {"f0", "f1"});
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
        // EQUALS is not in the supported set
        RexNode eq = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS, makeIntRef(0), makeIntRef(0));

        assertFalse(converter.isSupported(eq, context));
    }

    // ---- getCommonTypeForComparison direct tests ----

    @Test
    void testCommonTypeDecimalWins() {
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType decType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 2);
        RelDataType result = RexCallConverter.getCommonTypeForComparison(intType, decType, TYPE_FACTORY);

        assertEquals(SqlTypeName.DECIMAL, result.getSqlTypeName());
    }

    @Test
    void testCommonTypeExactIntegerPromotesToBigint() {
        // TINYINT + INTEGER → BIGINT (both exact, promoted to widest exact type)
        RelDataType tinyintType = TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT);
        RelDataType integerType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType result = RexCallConverter.getCommonTypeForComparison(tinyintType, integerType, TYPE_FACTORY);

        assertEquals(SqlTypeName.BIGINT, result.getSqlTypeName());
    }

    @Test
    void testCommonTypeApproxFallbackToDouble() {
        // INT + FLOAT → DOUBLE (FLOAT is approx, so exact integer rule skipped)
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType floatType = TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT);
        RelDataType result = RexCallConverter.getCommonTypeForComparison(intType, floatType, TYPE_FACTORY);

        assertEquals(SqlTypeName.DOUBLE, result.getSqlTypeName());
    }

    @Test
    void testCommonTypeIncompatible() {
        // BOOLEAN + INTEGER → null (incompatible)
        RelDataType boolType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType result = RexCallConverter.getCommonTypeForComparison(boolType, intType, TYPE_FACTORY);

        assertNull(result, "Incompatible types should return null");
    }

    // ---- Helpers ----

    private static RexNode makeIntRef(int index) {
        return REX_BUILDER.makeInputRef(intType(), index);
    }

    private static RelDataType intType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    }

    private static RelDataType bigintType() {
        return TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);
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
