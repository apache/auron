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
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.auron.protobuf.PhysicalBinaryExprNode;
import org.apache.auron.protobuf.PhysicalCaseNode;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalIsNotNull;
import org.apache.auron.protobuf.PhysicalIsNull;
import org.apache.auron.protobuf.PhysicalLikeExprNode;
import org.apache.auron.protobuf.PhysicalNegativeNode;
import org.apache.auron.protobuf.PhysicalNot;
import org.apache.auron.protobuf.PhysicalWhenThen;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

/**
 * Converts a Calcite {@link RexCall} (operator expression) to an Auron native
 * {@link PhysicalExprNode}.
 *
 * <p>Handles arithmetic operators ({@code +}, {@code -}, {@code *}, {@code /},
 * {@code %}), comparison operators ({@code =}, {@code <>}, {@code >}, {@code <},
 * {@code >=}, {@code <=}), {@code LIKE} / {@code NOT LIKE}, unary minus/plus,
 * {@code CAST}, and {@code TRY_CAST}. Binary arithmetic and comparison operands
 * are promoted to a common type before conversion; arithmetic additionally casts
 * the result to the output type if it differs from the common type, while a
 * comparison result is already BOOLEAN and needs no cast. {@code LIKE} maps to a
 * dedicated like node; an explicit {@code ESCAPE} clause is unsupported and falls
 * back.
 *
 * <p>An explicit {@code CAST} maps to a strict cast node that errors on a bad
 * conversion, whereas {@code TRY_CAST} maps to a try-cast node that yields
 * {@code NULL} on failure; the internal operand/result promotions above use the
 * try-cast node. Only the source&rarr;target pairs in {@link #isCastTypeSupported}
 * convert; every other pair falls back to Flink's engine.
 *
 * <p>Also handles logical operators: {@code AND} and {@code OR} (folded
 * left-deep over Calcite's n-ary operands into binary nodes), {@code NOT},
 * {@code IS NULL}, and {@code IS NOT NULL}. Logical operands are already
 * boolean and are not cast.
 *
 * <p>{@code CASE WHEN} (searched form) becomes a {@link PhysicalCaseNode} with
 * one {@link PhysicalWhenThen} per branch and a trailing else; each then/else
 * result is cast to the call's result type so all branches share one type.
 */
public class RexCallConverter implements FlinkRexNodeConverter {

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
            SqlKind.CAST,
            SqlKind.AND,
            SqlKind.OR,
            SqlKind.NOT,
            SqlKind.IS_NULL,
            SqlKind.IS_NOT_NULL,
            SqlKind.CASE,
            SqlKind.EQUALS,
            SqlKind.NOT_EQUALS,
            SqlKind.GREATER_THAN,
            SqlKind.LESS_THAN,
            SqlKind.GREATER_THAN_OR_EQUAL,
            SqlKind.LESS_THAN_OR_EQUAL,
            SqlKind.LIKE);

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
     *
     * <p>{@code LIKE} is supported only in its two-operand form ({@code expr
     * LIKE pattern}). A three-operand {@code LIKE} with an explicit
     * {@code ESCAPE} clause has no native equivalent and falls back.
     */
    @Override
    public boolean isSupported(RexNode node, ConverterContext context) {
        RexCall call = (RexCall) node;
        // TRY_CAST has SqlKind OTHER_FUNCTION (not in SUPPORTED_KINDS), so it is
        // matched by operator identity before the kind checks.
        if (call.getOperator() == FlinkSqlOperatorTable.TRY_CAST) {
            return isCastTypeSupported(call.getOperands().get(0).getType(), call.getType());
        }
        SqlKind kind = call.getKind();
        if (!SUPPORTED_KINDS.contains(kind)) {
            return false;
        }
        if (kind == SqlKind.LIKE) {
            return call.getOperator() instanceof SqlLikeOperator
                    && call.getOperands().size() == 2;
        }
        if (BINARY_ARITHMETIC_KINDS.contains(kind)) {
            return SqlTypeUtil.isNumeric(call.getType());
        }
        if (kind == SqlKind.CAST) {
            return isCastTypeSupported(call.getOperands().get(0).getType(), call.getType());
        }
        return true;
    }

    /**
     * Returns {@code true} if a cast from {@code source} to {@code target} is in
     * the conservatively supported set that the native cast kernel performs
     * faithfully: numeric&harr;numeric (including decimal), numeric&rarr;string,
     * string&rarr;numeric (including string&rarr;decimal), boolean&rarr;string, and
     * string&rarr;boolean. Every other pair (temporal, binary, complex, etc.)
     * returns {@code false} so the whole Calc falls back to Flink's engine.
     */
    private static boolean isCastTypeSupported(RelDataType source, RelDataType target) {
        boolean srcNum = SqlTypeUtil.isNumeric(source);
        boolean tgtNum = SqlTypeUtil.isNumeric(target);
        boolean srcStr = SqlTypeUtil.isString(source);
        boolean tgtStr = SqlTypeUtil.isString(target);
        boolean srcBool = SqlTypeUtil.isBoolean(source);
        boolean tgtBool = SqlTypeUtil.isBoolean(target);
        if (srcNum && tgtNum) {
            return true;
        }
        if (srcNum && tgtStr) {
            return true;
        }
        if (srcStr && tgtNum) {
            return true;
        }
        if (srcBool && tgtStr) {
            return true;
        }
        if (srcStr && tgtBool) {
            return true;
        }
        return false;
    }

    /**
     * Converts the given {@link RexCall} to a native {@link PhysicalExprNode}.
     *
     * <p>Dispatches by {@link SqlKind}:
     * <ul>
     *   <li>Binary arithmetic → {@link PhysicalBinaryExprNode} with type
     *       promotion and an output cast when the result type differs
     *   <li>Comparison ({@code =}, {@code <>}, {@code >}, {@code <}, {@code >=},
     *       {@code <=}) → {@link PhysicalBinaryExprNode} with type promotion and
     *       no output cast (the result is already BOOLEAN)
     *   <li>{@code LIKE} → {@link PhysicalLikeExprNode} (case-sensitive, never
     *       negated); {@code NOT LIKE} is {@code NOT(LIKE(...))} and converts via
     *       the {@code NOT} case
     *   <li>{@code MINUS_PREFIX} → {@link PhysicalNegativeNode}
     *   <li>{@code PLUS_PREFIX} → identity (passthrough to operand)
     *   <li>{@code CAST} → {@link org.apache.auron.protobuf.PhysicalCastNode}
     *       (strict; errors on a bad conversion)
     *   <li>{@code TRY_CAST} → {@link org.apache.auron.protobuf.PhysicalTryCastNode}
     *       (yields {@code NULL} on a bad conversion); matched by operator
     *       identity before the {@link SqlKind} switch
     *   <li>{@code AND}/{@code OR} → {@link PhysicalBinaryExprNode} folded
     *       left-deep over the n-ary operands
     *   <li>{@code NOT} → {@link PhysicalNot}
     *   <li>{@code IS NULL} → {@link PhysicalIsNull}
     *   <li>{@code IS NOT NULL} → {@link PhysicalIsNotNull}
     *   <li>{@code CASE} → {@link PhysicalCaseNode}
     * </ul>
     *
     * @throws IllegalArgumentException if the SqlKind is not supported
     */
    @Override
    public PhysicalExprNode convert(RexNode node, ConverterContext context) {
        RexCall call = (RexCall) node;
        // TRY_CAST has SqlKind OTHER_FUNCTION, which the switch below would route
        // to the throwing default; match it by operator identity beforehand.
        if (call.getOperator() == FlinkSqlOperatorTable.TRY_CAST) {
            return buildTryCast(call, context);
        }
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
            case EQUALS:
                return buildComparison(call, "Eq", context);
            case NOT_EQUALS:
                return buildComparison(call, "NotEq", context);
            case GREATER_THAN:
                return buildComparison(call, "Gt", context);
            case LESS_THAN:
                return buildComparison(call, "Lt", context);
            case GREATER_THAN_OR_EQUAL:
                return buildComparison(call, "GtEq", context);
            case LESS_THAN_OR_EQUAL:
                return buildComparison(call, "LtEq", context);
            case LIKE:
                return buildLike(call, context);
            case MINUS_PREFIX:
                return buildNegative(call, context);
            case PLUS_PREFIX:
                return convertOperand(call.getOperands().get(0), context);
            case CAST:
                return buildCast(call, context);
            case AND:
                return buildBinaryFold(call, "And", context);
            case OR:
                return buildBinaryFold(call, "Or", context);
            case NOT:
                return buildNot(call, context);
            case IS_NULL:
                return buildIsNull(call, context);
            case IS_NOT_NULL:
                return buildIsNotNull(call, context);
            case CASE:
                return buildCase(call, context);
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

        RelDataType compatibleType = FlinkNodeConverterUtils.getCommonTypeForComparison(
                left.getType(), right.getType(), FlinkNodeConverterUtils.TYPE_FACTORY);
        if (compatibleType == null) {
            throw new IllegalStateException("Incompatible types: "
                    + left.getType().getSqlTypeName()
                    + " and "
                    + right.getType().getSqlTypeName());
        }

        PhysicalExprNode leftExpr =
                FlinkNodeConverterUtils.castIfNecessary(convertOperand(left, context), left.getType(), compatibleType);
        PhysicalExprNode rightExpr = FlinkNodeConverterUtils.castIfNecessary(
                convertOperand(right, context), right.getType(), compatibleType);

        PhysicalExprNode binaryExpr = PhysicalExprNode.newBuilder()
                .setBinaryExpr(PhysicalBinaryExprNode.newBuilder()
                        .setL(leftExpr)
                        .setR(rightExpr)
                        .setOp(op))
                .build();

        if (!outputType.getSqlTypeName().equals(compatibleType.getSqlTypeName())) {
            return FlinkNodeConverterUtils.wrapInTryCast(binaryExpr, outputType);
        }
        return binaryExpr;
    }

    /**
     * Builds a comparison expression with type promotion between operands.
     *
     * <p>Operands are promoted to a common type so the native comparison kernel
     * sees matching operand types. The result is already BOOLEAN, so it is
     * returned without an output cast.
     */
    private PhysicalExprNode buildComparison(RexCall call, String op, ConverterContext context) {
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        RelDataType compatibleType = FlinkNodeConverterUtils.getCommonTypeForComparison(
                left.getType(), right.getType(), FlinkNodeConverterUtils.TYPE_FACTORY);
        if (compatibleType == null) {
            throw new IllegalStateException("Incompatible types: "
                    + left.getType().getSqlTypeName()
                    + " and "
                    + right.getType().getSqlTypeName());
        }

        PhysicalExprNode leftExpr =
                FlinkNodeConverterUtils.castIfNecessary(convertOperand(left, context), left.getType(), compatibleType);
        PhysicalExprNode rightExpr = FlinkNodeConverterUtils.castIfNecessary(
                convertOperand(right, context), right.getType(), compatibleType);

        return PhysicalExprNode.newBuilder()
                .setBinaryExpr(PhysicalBinaryExprNode.newBuilder()
                        .setL(leftExpr)
                        .setR(rightExpr)
                        .setOp(op))
                .build();
    }

    /**
     * Builds a {@code LIKE} expression as a {@link PhysicalLikeExprNode}. Matching
     * is case-sensitive (Flink SQL {@code LIKE} semantics). The node is never
     * negated here: Calcite represents {@code NOT LIKE} as {@code NOT(LIKE(...))}
     * (a negated like operator cannot form a {@link RexCall}), so a {@code LIKE}
     * call always reaches this method un-negated.
     */
    private PhysicalExprNode buildLike(RexCall call, ConverterContext context) {
        PhysicalExprNode expr = convertOperand(call.getOperands().get(0), context);
        PhysicalExprNode pattern = convertOperand(call.getOperands().get(1), context);
        return PhysicalExprNode.newBuilder()
                .setLikeExpr(PhysicalLikeExprNode.newBuilder()
                        .setNegated(false)
                        .setCaseInsensitive(false)
                        .setExpr(expr)
                        .setPattern(pattern))
                .build();
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

    private PhysicalExprNode buildNegative(RexCall call, ConverterContext context) {
        PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
        return PhysicalExprNode.newBuilder()
                .setNegative(PhysicalNegativeNode.newBuilder().setExpr(operand))
                .build();
    }

    private PhysicalExprNode buildCast(RexCall call, ConverterContext context) {
        PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
        return FlinkNodeConverterUtils.wrapInCast(operand, call.getType());
    }

    private PhysicalExprNode buildTryCast(RexCall call, ConverterContext context) {
        PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
        return FlinkNodeConverterUtils.wrapInTryCast(operand, call.getType());
    }

    /**
     * Folds a Calcite n-ary {@code AND}/{@code OR} (operand count &ge; 2) into a
     * left-deep chain of binary nodes: {@code ((o0 op o1) op o2) ...}. Operands
     * are already boolean and are not cast.
     */
    private PhysicalExprNode buildBinaryFold(RexCall call, String op, ConverterContext context) {
        PhysicalExprNode acc = convertOperand(call.getOperands().get(0), context);
        for (int i = 1; i < call.getOperands().size(); i++) {
            PhysicalExprNode right = convertOperand(call.getOperands().get(i), context);
            acc = PhysicalExprNode.newBuilder()
                    .setBinaryExpr(PhysicalBinaryExprNode.newBuilder()
                            .setL(acc)
                            .setR(right)
                            .setOp(op))
                    .build();
        }
        return acc;
    }

    private PhysicalExprNode buildNot(RexCall call, ConverterContext context) {
        PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
        return PhysicalExprNode.newBuilder()
                .setNotExpr(PhysicalNot.newBuilder().setExpr(operand))
                .build();
    }

    private PhysicalExprNode buildIsNull(RexCall call, ConverterContext context) {
        PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
        return PhysicalExprNode.newBuilder()
                .setIsNullExpr(PhysicalIsNull.newBuilder().setExpr(operand))
                .build();
    }

    private PhysicalExprNode buildIsNotNull(RexCall call, ConverterContext context) {
        PhysicalExprNode operand = convertOperand(call.getOperands().get(0), context);
        return PhysicalExprNode.newBuilder()
                .setIsNotNullExpr(PhysicalIsNotNull.newBuilder().setExpr(operand))
                .build();
    }

    /**
     * Builds a searched {@code CASE} from Calcite's interleaved operands
     * {@code [when1, then1, ..., whenN, thenN, else]} (odd count, trailing
     * else). Each then and the else are cast to the call's result type so the
     * native {@code CaseExpr} receives uniformly-typed branches. The
     * simple-CASE {@code expr} field is left unset.
     */
    private PhysicalExprNode buildCase(RexCall call, ConverterContext context) {
        RelDataType resultType = call.getType();
        List<RexNode> operands = call.getOperands();
        PhysicalCaseNode.Builder caseNode = PhysicalCaseNode.newBuilder();
        int i = 0;
        for (; i + 1 < operands.size(); i += 2) {
            RexNode when = operands.get(i);
            RexNode then = operands.get(i + 1);
            PhysicalExprNode whenExpr = convertOperand(when, context);
            PhysicalExprNode thenExpr =
                    FlinkNodeConverterUtils.castIfNecessary(convertOperand(then, context), then.getType(), resultType);
            caseNode.addWhenThenExpr(
                    PhysicalWhenThen.newBuilder().setWhenExpr(whenExpr).setThenExpr(thenExpr));
        }
        RexNode elseOperand = operands.get(i);
        caseNode.setElseExpr(FlinkNodeConverterUtils.castIfNecessary(
                convertOperand(elseOperand, context), elseOperand.getType(), resultType));
        return PhysicalExprNode.newBuilder().setCase(caseNode).build();
    }
}
