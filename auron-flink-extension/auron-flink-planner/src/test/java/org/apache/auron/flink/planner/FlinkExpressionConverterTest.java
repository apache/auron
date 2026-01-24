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
package org.apache.auron.flink.planner;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.List;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for FlinkExpressionConverter.
 * Tests conversion of Calcite RexNode expressions to Auron PhysicalExprNode protobuf.
 */
public class FlinkExpressionConverterTest {

    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory;
    private List<String> fieldNames;

    @BeforeEach
    public void setup() {
        typeFactory = new SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
        rexBuilder = new RexBuilder(typeFactory);
        fieldNames = Arrays.asList("col1", "col2", "col3", "col4");
    }

    @Test
    public void testInputRefConversion() {
        // Test column reference: col1 (index 0)
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef inputRef = rexBuilder.makeInputRef(intType, 0);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(inputRef, fieldNames);

        assertTrue(exprNode.hasColumn());
        assertEquals("col1", exprNode.getColumn().getName());
        assertEquals(0, exprNode.getColumn().getIndex());
    }

    @Test
    public void testMultipleInputRefs() {
        // Test multiple column references
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        for (int i = 0; i < fieldNames.size(); i++) {
            RexInputRef inputRef = rexBuilder.makeInputRef(intType, i);
            PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(inputRef, fieldNames);

            assertTrue(exprNode.hasColumn());
            assertEquals(fieldNames.get(i), exprNode.getColumn().getName());
            assertEquals(i, exprNode.getColumn().getIndex());
        }
    }

    @Test
    public void testIntegerLiteralConversion() {
        // Test integer literal: 42
        RexNode literal = rexBuilder.makeLiteral(42, typeFactory.createSqlType(SqlTypeName.INTEGER), true);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(literal, fieldNames);

        assertTrue(exprNode.hasLiteral());
        // IPC bytes should be non-empty
        assertFalse(exprNode.getLiteral().getIpcBytes().isEmpty());
    }

    @Test
    public void testStringLiteralConversion() {
        // Test string literal: "test"
        RexLiteral literal = rexBuilder.makeLiteral("test");

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(literal, fieldNames);

        assertTrue(exprNode.hasLiteral());
        assertFalse(exprNode.getLiteral().getIpcBytes().isEmpty());
    }

    @Test
    public void testBooleanLiteralConversion() {
        // Test boolean literals: true and false
        RexLiteral truelit = rexBuilder.makeLiteral(true);
        RexLiteral falseLit = rexBuilder.makeLiteral(false);

        PhysicalExprNode trueExpr = FlinkExpressionConverter.convertRexNode(truelit, fieldNames);
        PhysicalExprNode falseExpr = FlinkExpressionConverter.convertRexNode(falseLit, fieldNames);

        assertTrue(trueExpr.hasLiteral());
        assertTrue(falseExpr.hasLiteral());
    }

    @Test
    public void testNullLiteralConversion() {
        // Test NULL literal
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexLiteral nullLiteral = rexBuilder.makeNullLiteral(intType);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(nullLiteral, fieldNames);

        assertTrue(exprNode.hasLiteral());
    }

    @Test
    public void testEqualsOperator() {
        // Test: col1 = 10
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode ten = rexBuilder.makeLiteral(10, intType, true);
        RexNode equals = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS, col1, ten);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(equals, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("Eq", exprNode.getBinaryExpr().getOp());
        assertTrue(exprNode.getBinaryExpr().hasL());
        assertTrue(exprNode.getBinaryExpr().hasR());
    }

    @Test
    public void testNotEqualsOperator() {
        // Test: col1 != 20
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode twenty = rexBuilder.makeLiteral(20, intType, true);
        RexNode notEquals =
                rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT_EQUALS, col1, twenty);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(notEquals, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("NotEq", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testLessThanOperator() {
        // Test: col1 < 100
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode hundred = rexBuilder.makeLiteral(100, intType, true);
        RexNode lessThan = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN, col1, hundred);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(lessThan, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("Lt", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testGreaterThanOperator() {
        // Test: col1 > 50
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode fifty = rexBuilder.makeLiteral(50, intType, true);
        RexNode greaterThan =
                rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, col1, fifty);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(greaterThan, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("Gt", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testLessThanOrEqualOperator() {
        // Test: col1 <= 100
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode hundred = rexBuilder.makeLiteral(100, intType, true);
        RexNode lessOrEqual =
                rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN_OR_EQUAL, col1, hundred);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(lessOrEqual, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("LtEq", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testGreaterThanOrEqualOperator() {
        // Test: col1 >= 50
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode fifty = rexBuilder.makeLiteral(50, intType, true);
        RexNode greaterOrEqual =
                rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, col1, fifty);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(greaterOrEqual, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("GtEq", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testPlusOperator() {
        // Test: col1 + 10
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode ten = rexBuilder.makeLiteral(10, intType, true);
        RexNode plus = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS, col1, ten);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(plus, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("Plus", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testMinusOperator() {
        // Test: col1 - 5
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode five = rexBuilder.makeLiteral(5, intType, true);
        RexNode minus = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.MINUS, col1, five);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(minus, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("Minus", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testMultiplyOperator() {
        // Test: col1 * 2
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode two = rexBuilder.makeLiteral(2, intType, true);
        RexNode multiply = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.MULTIPLY, col1, two);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(multiply, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("Multiply", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testDivideOperator() {
        // Test: col1 / 2
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode two = rexBuilder.makeLiteral(2, intType, true);
        RexNode divide = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.DIVIDE, col1, two);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(divide, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("Divide", exprNode.getBinaryExpr().getOp());
    }

    @Test
    public void testAndOperator() {
        // Test: col1 > 10 AND col2 < 100
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexInputRef col2 = rexBuilder.makeInputRef(intType, 1);
        RexNode ten = rexBuilder.makeLiteral(10, intType, true);
        RexNode hundred = rexBuilder.makeLiteral(100, intType, true);

        RexNode gt = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, col1, ten);
        RexNode lt = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN, col2, hundred);
        RexNode and = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.AND, gt, lt);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(and, fieldNames);

        // Should use short-circuit AND
        assertTrue(exprNode.hasScAndExpr());
        assertTrue(exprNode.getScAndExpr().hasLeft());
        assertTrue(exprNode.getScAndExpr().hasRight());
    }

    @Test
    public void testOrOperator() {
        // Test: col1 < 10 OR col2 > 100
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexInputRef col2 = rexBuilder.makeInputRef(intType, 1);
        RexNode ten = rexBuilder.makeLiteral(10, intType, true);
        RexNode hundred = rexBuilder.makeLiteral(100, intType, true);

        RexNode lt = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN, col1, ten);
        RexNode gt = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, col2, hundred);
        RexNode or = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.OR, lt, gt);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(or, fieldNames);

        // Should use short-circuit OR
        assertTrue(exprNode.hasScOrExpr());
        assertTrue(exprNode.getScOrExpr().hasLeft());
        assertTrue(exprNode.getScOrExpr().hasRight());
    }

    @Test
    public void testNotOperator() {
        // Test: NOT (col1 > 10)
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexNode ten = rexBuilder.makeLiteral(10, intType, true);

        RexNode gt = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, col1, ten);
        RexNode not = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.NOT, gt);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(not, fieldNames);

        assertTrue(exprNode.hasNotExpr());
        assertTrue(exprNode.getNotExpr().hasExpr());
    }

    @Test
    public void testComplexExpression() {
        // Test: (col1 > 10 AND col2 < 100) OR col3 = 50
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexInputRef col1 = rexBuilder.makeInputRef(intType, 0);
        RexInputRef col2 = rexBuilder.makeInputRef(intType, 1);
        RexInputRef col3 = rexBuilder.makeInputRef(intType, 2);
        RexNode ten = rexBuilder.makeLiteral(10, intType, true);
        RexNode hundred = rexBuilder.makeLiteral(100, intType, true);
        RexNode fifty = rexBuilder.makeLiteral(50, intType, true);

        RexNode gt = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.GREATER_THAN, col1, ten);
        RexNode lt = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.LESS_THAN, col2, hundred);
        RexNode and = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.AND, gt, lt);
        RexNode eq = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS, col3, fifty);
        RexNode or = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.OR, and, eq);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(or, fieldNames);

        // Should have nested structure
        assertTrue(exprNode.hasScOrExpr());
        assertTrue(exprNode.getScOrExpr().getLeft().hasScAndExpr());
        assertTrue(exprNode.getScOrExpr().getRight().hasBinaryExpr());
    }

    @Test
    public void testUnsupportedOperatorThrowsException() {
        // Test an unsupported operator (e.g., CASE)
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode one = rexBuilder.makeLiteral(1, intType, true);

        // Create a CASE expression (not supported in MVP)
        RexNode caseExpr = rexBuilder.makeCall(
                org.apache.calcite.sql.fun.SqlStdOperatorTable.CASE, rexBuilder.makeLiteral(true), one, one);

        assertThrows(UnsupportedOperationException.class, () -> {
            FlinkExpressionConverter.convertRexNode(caseExpr, fieldNames);
        });
    }

    @Test
    public void testArithmeticWithDifferentTypes() {
        // Test: DOUBLE + INT
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RexInputRef doubleCol = rexBuilder.makeInputRef(doubleType, 0);
        RexNode intLiteral = rexBuilder.makeLiteral(10, intType, true);

        RexNode plus = rexBuilder.makeCall(org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS, doubleCol, intLiteral);

        PhysicalExprNode exprNode = FlinkExpressionConverter.convertRexNode(plus, fieldNames);

        assertTrue(exprNode.hasBinaryExpr());
        assertEquals("Plus", exprNode.getBinaryExpr().getOp());
    }
}
