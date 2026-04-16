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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

/** Tests for {@link TypeCastUtils}. */
class TypeCastUtilsTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();

    @Test
    void testCommonTypeDecimalWins() {
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType decType = TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 2);
        RelDataType result = TypeCastUtils.getCommonTypeForComparison(intType, decType, TYPE_FACTORY);

        assertEquals(SqlTypeName.DECIMAL, result.getSqlTypeName());
    }

    @Test
    void testCommonTypeExactIntegerPromotesToBigint() {
        // TINYINT + INTEGER → BIGINT (both exact, promoted to widest exact type)
        RelDataType tinyintType = TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT);
        RelDataType integerType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType result = TypeCastUtils.getCommonTypeForComparison(tinyintType, integerType, TYPE_FACTORY);

        assertEquals(SqlTypeName.BIGINT, result.getSqlTypeName());
    }

    @Test
    void testCommonTypeApproxFallbackToDouble() {
        // INT + FLOAT → DOUBLE (FLOAT is approx, so exact integer rule skipped)
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType floatType = TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT);
        RelDataType result = TypeCastUtils.getCommonTypeForComparison(intType, floatType, TYPE_FACTORY);

        assertEquals(SqlTypeName.DOUBLE, result.getSqlTypeName());
    }

    @Test
    void testCommonTypeIncompatible() {
        // BOOLEAN + INTEGER → null (incompatible)
        RelDataType boolType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType result = TypeCastUtils.getCommonTypeForComparison(boolType, intType, TYPE_FACTORY);

        assertNull(result, "Incompatible types should return null");
    }

    @Test
    void testCommonTypeSameTypeReturnsAsIs() {
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType result = TypeCastUtils.getCommonTypeForComparison(intType, intType, TYPE_FACTORY);

        assertSame(intType, result, "Same type should return the first operand's type");
    }

    @Test
    void testCommonTypeCharFamilyProducesVarchar() {
        RelDataType charType = TYPE_FACTORY.createSqlType(SqlTypeName.CHAR, 10);
        RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 20);
        RelDataType result = TypeCastUtils.getCommonTypeForComparison(charType, varcharType, TYPE_FACTORY);

        assertEquals(SqlTypeName.VARCHAR, result.getSqlTypeName());
    }

    @Test
    void testCastIfNecessarySameTypePassthrough() {
        PhysicalExprNode expr = PhysicalExprNode.getDefaultInstance();
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

        PhysicalExprNode result = TypeCastUtils.castIfNecessary(expr, intType, intType);

        assertSame(expr, result, "Same type should return the original expression unchanged");
    }

    @Test
    void testCastIfNecessaryDifferentTypeWrapsTryCast() {
        PhysicalExprNode expr = PhysicalExprNode.getDefaultInstance();
        RelDataType intType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
        RelDataType bigintType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);

        PhysicalExprNode result = TypeCastUtils.castIfNecessary(expr, intType, bigintType);

        assertTrue(result.hasTryCast(), "Different types should wrap in TryCast");
    }

    @Test
    void testWrapInTryCast() {
        PhysicalExprNode expr = PhysicalExprNode.getDefaultInstance();
        RelDataType bigintType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);

        PhysicalExprNode result = TypeCastUtils.wrapInTryCast(expr, bigintType);

        assertTrue(result.hasTryCast(), "Should produce a TryCast node");
        assertTrue(result.getTryCast().hasArrowType(), "TryCast should have an ArrowType");
    }
}
