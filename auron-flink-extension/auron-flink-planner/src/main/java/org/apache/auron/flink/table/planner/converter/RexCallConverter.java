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

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.protobuf.PhysicalBinaryExprNode;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalNegativeNode;
import org.apache.auron.protobuf.PhysicalTryCastNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Converts a Calcite {@link RexCall} (operator expression) to an Auron native
 * {@link PhysicalExprNode}.
 *
 * <p>Handles arithmetic operators ({@code +}, {@code -}, {@code *}, {@code /},
 * {@code %}), unary minus/plus, and {@code CAST}. Binary arithmetic operands
 * are promoted to a common type before conversion, and the result is cast to
 * the output type if it differs from the common type.
 */
public class RexCallConverter implements FlinkRexNodeConverter {

    private static final RelDataTypeFactory TYPE_FACTORY = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    /** Binary arithmetic kinds that require numeric result type. */
    private static final Set<SqlKind> BINARY_ARITHMETIC_KINDS =
            EnumSet.of(SqlKind.PLUS, SqlKind.MINUS, SqlKind.TIMES, SqlKind.DIVIDE, SqlKind.MOD);

    /** All supported SqlKinds including unary and cast. */
    private static final Set<SqlKind> SUPPORTED_KINDS = EnumSet.of(
            SqlKind.PLUS,
            SqlKind.MINUS,
            SqlKind.TIMES,
            SqlKind.DIVIDE,
            SqlKind.MOD,
            SqlKind.MINUS_PREFIX,
            SqlKind.PLUS_PREFIX,
            SqlKind.CAST);

    private final FlinkNodeConverterFactory factory;

    /**
     * Creates a new converter that delegates operand conversion to the given
     * factory.
     *
     * @param factory the factory used for recursive operand conversion
     */
    public RexCallConverter(FlinkNodeConverterFactory factory) {
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override
    public Class<? extends RexNode> getNodeClass() {
        return RexCall.class;
    }

    /**
     * Returns {@code true} if the call's {@link SqlKind} is supported.
     *
     * <p>For binary arithmetic kinds, the call's result type must also be
     * numeric to reject non-arithmetic uses (e.g., TIMESTAMP + INTERVAL).
     */
    @Override
    public boolean isSupported(RexNode node, ConverterContext context) {
        RexCall call = (RexCall) node;
        SqlKind kind = call.getKind();
        if (!SUPPORTED_KINDS.contains(kind)) {
            return false;
        }
        if (BINARY_ARITHMETIC_KINDS.contains(kind)) {
            return SqlTypeUtil.isNumeric(call.getType());
        }
        return true;
    }

    /**
     * Converts the given {@link RexCall} to a native {@link PhysicalExprNode}.
     *
     * <p>Dispatches by {@link SqlKind}:
     * <ul>
     *   <li>Binary arithmetic → {@link PhysicalBinaryExprNode} with type
     *       promotion
     *   <li>{@code MINUS_PREFIX} → {@link PhysicalNegativeNode}
     *   <li>{@code PLUS_PREFIX} → identity (passthrough to operand)
     *   <li>{@code CAST} → {@link PhysicalTryCastNode}
     * </ul>
     *
     * @throws IllegalArgumentException if the SqlKind is not supported
     */
    @Override
    public PhysicalExprNode convert(RexNode node, ConverterContext context) {
        RexCall call = (RexCall) node;
        SqlKind kind = call.getKind();
        switch (kind) {
            case PLUS:
                return buildBinaryExpr(call, "Plus", context);
            case MINUS:
                return buildBinaryExpr(call, "Minus", context);
            case TIMES:
                return buildBinaryExpr(call, "Multiply", context);
            case DIVIDE:
                return buildBinaryExpr(call, "Divide", context);
            case MOD:
                return buildBinaryExpr(call, "Modulo", context);
            case MINUS_PREFIX:
                return buildNegative(call, context);
            case PLUS_PREFIX:
                return convertOperand(call.getOperands().get(0), context);
            case CAST:
                return buildTryCast(call, context);
            default:
                throw new IllegalArgumentException("Unsupported SqlKind: " + kind);
        }
    }

    /**
     * Builds a binary expression with type promotion between operands.
     *
     * <p>Operands are promoted to a common type. If the call's output type
     * differs from the common type, the result is wrapped in a TryCast.
     */
    private PhysicalExprNode buildBinaryExpr(RexCall call, String op, ConverterContext context) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);
        RelDataType outputType = call.getType();

        RelDataType compatibleType = getCommonTypeForComparison(left.getType(), right.getType(), TYPE_FACTORY);
        if (compatibleType == null) {
            throw new IllegalStateException("Incompatible types: "
                    + left.getType().getSqlTypeName()
                    + " and "
                    + right.getType().getSqlTypeName());
        }

        PhysicalExprNode leftExpr = castIfNecessary(left, compatibleType, context);
        PhysicalExprNode rightExpr = castIfNecessary(right, compatibleType, context);

        PhysicalExprNode binaryExpr = PhysicalExprNode.newBuilder()
                .setBinaryExpr(PhysicalBinaryExprNode.newBuilder()
                        .setL(leftExpr)
                        .setR(rightExpr)
                        .setOp(op))
                .build();

        if (!outputType.getSqlTypeName().equals(compatibleType.getSqlTypeName())) {
            return wrapInTryCast(binaryExpr, outputType);
        }
        return binaryExpr;
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
    static RelDataType getCommonTypeForComparison(
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

    private static boolean notApproxType(SqlTypeName typeName) {
        return typeName != SqlTypeName.FLOAT && typeName != SqlTypeName.DOUBLE;
    }

    /**
     * Wraps the converted operand in a TryCast if its type differs from the
     * target type.
     */
    private PhysicalExprNode castIfNecessary(RexNode expr, RelDataType targetType, ConverterContext context) {
        PhysicalExprNode converted = convertOperand(expr, context);
        if (expr.getType().getSqlTypeName().equals(targetType.getSqlTypeName())) {
            return converted;
        }
        return wrapInTryCast(converted, targetType);
    }

    /**
     * Delegates operand conversion to the factory.
     *
     * @throws IllegalStateException if no converter is registered for
     *     the operand
     */
    private PhysicalExprNode convertOperand(RexNode operand, ConverterContext context) {
        Optional<PhysicalExprNode> result = factory.convertRexNode(operand, context);
        if (!result.isPresent()) {
            throw new IllegalStateException("Failed to convert operand: " + operand + " (no converter registered)");
        }
        return result.get();
    }

    private static PhysicalExprNode wrapInTryCast(PhysicalExprNode expr, RelDataType targetType) {
        LogicalType logicalType = FlinkTypeFactory.toLogicalType(targetType);
        org.apache.auron.protobuf.ArrowType arrowType = SchemaConverters.convertToAuronArrowType(logicalType);
        return PhysicalExprNode.newBuilder()
                .setTryCast(PhysicalTryCastNode.newBuilder().setExpr(expr).setArrowType(arrowType))
                .build();
    }

    private PhysicalExprNode buildNegative(RexCall call, ConverterContext context) {
        PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
        return PhysicalExprNode.newBuilder()
                .setNegative(PhysicalNegativeNode.newBuilder().setExpr(operand))
                .build();
    }

    private PhysicalExprNode buildTryCast(RexCall call, ConverterContext context) {
        PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
        return wrapInTryCast(operand, call.getType());
    }
}
