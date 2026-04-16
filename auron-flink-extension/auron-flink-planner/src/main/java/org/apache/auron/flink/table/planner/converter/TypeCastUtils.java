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

import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalTryCastNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Utility class for type promotion and casting operations used during
 * {@link RexCallConverter} expression conversion.
 *
 * <p>Provides helpers to compute a common numeric type for binary arithmetic
 * operands and to wrap native expressions in {@code TryCast} nodes.
 */
public final class TypeCastUtils {

    /** Shared type factory used for creating common SQL types. */
    public static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private TypeCastUtils() {
        // utility class
    }

    /**
     * Computes the common type for two operand types during arithmetic
     * promotion.
     *
     * <p>Rules:
     * <ul>
     *   <li>Same type → return as-is
     *   <li>Both numeric: DECIMAL wins; BIGINT wins over smaller integers
     *       (when neither is approximate); otherwise DOUBLE
     *   <li>Both character → VARCHAR
     *   <li>Otherwise → {@code null} (incompatible)
     * </ul>
     *
     * @param type1 left operand type
     * @param type2 right operand type
     * @param typeFactory factory for creating result types
     * @return the common type, or {@code null} if incompatible
     */
    public static RelDataType getCommonTypeForComparison(
            RelDataType type1, RelDataType type2, RelDataTypeFactory typeFactory) {
        if (type1.getSqlTypeName().equals(type2.getSqlTypeName())) {
            return type1;
        }
        if (SqlTypeUtil.isNumeric(type1) && SqlTypeUtil.isNumeric(type2)) {
            SqlTypeName t1 = type1.getSqlTypeName();
            SqlTypeName t2 = type2.getSqlTypeName();
            if (t1 == SqlTypeName.DECIMAL || t2 == SqlTypeName.DECIMAL) {
                return typeFactory.createSqlType(SqlTypeName.DECIMAL);
            }
            if (notApproxType(t1) && notApproxType(t2)) {
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            }
            return typeFactory.createSqlType(SqlTypeName.DOUBLE);
        }
        if (SqlTypeUtil.inCharFamily(type1) && SqlTypeUtil.inCharFamily(type2)) {
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);
        }
        return null;
    }

    /**
     * Wraps the given expression in a {@link PhysicalTryCastNode} targeting
     * the specified type if the source type differs from the target type.
     *
     * <p>If {@code sourceType} and {@code targetType} share the same
     * {@link SqlTypeName}, the original expression is returned unchanged.
     *
     * @param converted the already-converted native expression
     * @param sourceType the current SQL type of the expression
     * @param targetType the desired SQL type
     * @return {@code converted} unchanged, or wrapped in a TryCast
     */
    public static PhysicalExprNode castIfNecessary(
            PhysicalExprNode converted, RelDataType sourceType, RelDataType targetType) {
        if (sourceType.getSqlTypeName().equals(targetType.getSqlTypeName())) {
            return converted;
        }
        return wrapInTryCast(converted, targetType);
    }

    /**
     * Wraps the given native expression in a {@link PhysicalTryCastNode}
     * targeting the specified Calcite type.
     *
     * @param expr the native expression to wrap
     * @param targetType the desired output type
     * @return a new {@link PhysicalExprNode} containing a {@code TryCast}
     */
    public static PhysicalExprNode wrapInTryCast(PhysicalExprNode expr, RelDataType targetType) {
        LogicalType logicalType = FlinkTypeFactory.toLogicalType(targetType);
        org.apache.auron.protobuf.ArrowType arrowType = SchemaConverters.convertToAuronArrowType(logicalType);
        return PhysicalExprNode.newBuilder()
                .setTryCast(PhysicalTryCastNode.newBuilder().setExpr(expr).setArrowType(arrowType))
                .build();
    }

    private static boolean notApproxType(SqlTypeName typeName) {
        return typeName != SqlTypeName.FLOAT && typeName != SqlTypeName.DOUBLE;
    }
}
