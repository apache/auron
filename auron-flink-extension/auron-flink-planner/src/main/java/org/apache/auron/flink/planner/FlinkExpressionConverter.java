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

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.protobuf.PhysicalBinaryExprNode;
import org.apache.auron.protobuf.PhysicalColumn;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalNot;
import org.apache.auron.protobuf.PhysicalSCAndExprNode;
import org.apache.auron.protobuf.PhysicalSCOrExprNode;
import org.apache.auron.protobuf.ScalarValue;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.NlsString;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * Converter for Calcite RexNode expressions to Auron PhysicalExprNode protobuf.
 * Handles column references, literals, and binary/logical operators.
 */
public class FlinkExpressionConverter {

    private static final BufferAllocator ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    /**
     * Converts a Calcite RexNode to an Auron PhysicalExprNode protobuf.
     *
     * @param rexNode The RexNode to convert.
     * @param inputFieldNames Names of input fields for column references.
     * @return The corresponding PhysicalExprNode protobuf.
     * @throws UnsupportedOperationException If the expression type is not supported.
     */
    public static PhysicalExprNode convertRexNode(RexNode rexNode, List<String> inputFieldNames) {
        if (rexNode instanceof RexInputRef) {
            return convertInputRef((RexInputRef) rexNode, inputFieldNames);
        } else if (rexNode instanceof RexLiteral) {
            return convertLiteral((RexLiteral) rexNode);
        } else if (rexNode instanceof RexCall) {
            return convertCall((RexCall) rexNode, inputFieldNames);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported RexNode type: " + rexNode.getClass().getSimpleName());
        }
    }

    /**
     * Converts a RexInputRef (column reference) to a PhysicalColumn.
     */
    private static PhysicalExprNode convertInputRef(RexInputRef inputRef, List<String> inputFieldNames) {
        int index = inputRef.getIndex();
        String columnName = (index < inputFieldNames.size()) ? inputFieldNames.get(index) : "col_" + index;

        PhysicalColumn column =
                PhysicalColumn.newBuilder().setName(columnName).setIndex(index).build();

        return PhysicalExprNode.newBuilder().setColumn(column).build();
    }

    /**
     * Converts a RexLiteral to a ScalarValue using Arrow IPC format.
     */
    private static PhysicalExprNode convertLiteral(RexLiteral literal) {
        try {
            // Create Arrow schema for the literal value
            // Convert from Calcite type to Arrow type via SqlTypeName
            ArrowType arrowType = convertCalciteToArrowType(literal.getType());
            Field field = new Field("literal", new FieldType(true, arrowType, null), null);
            Schema schema = new Schema(java.util.Collections.singletonList(field));

            // Create VectorSchemaRoot and write the value
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, ALLOCATOR)) {
                root.allocateNew();

                if (literal.isNull()) {
                    root.getVector(0).setNull(0);
                } else {
                    writeValueToVector(root.getVector(0), literal.getValue(), literal.getType());
                }

                root.setRowCount(1);

                // Serialize to Arrow IPC format
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out))) {
                    writer.start();
                    writer.writeBatch();
                    writer.end();
                }

                byte[] ipcBytes = out.toByteArray();
                ScalarValue scalarValue = ScalarValue.newBuilder()
                        .setIpcBytes(ByteString.copyFrom(ipcBytes))
                        .build();

                return PhysicalExprNode.newBuilder().setLiteral(scalarValue).build();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert literal: " + literal, e);
        }
    }

    /**
     * Converts a RexCall (function/operator call) to the appropriate PhysicalExprNode.
     */
    private static PhysicalExprNode convertCall(RexCall call, List<String> inputFieldNames) {
        SqlKind kind = call.getKind();
        List<RexNode> operands = call.getOperands();

        switch (kind) {
                // Comparison operators
            case EQUALS:
                return buildBinaryExpr(operands.get(0), operands.get(1), "Eq", inputFieldNames);
            case NOT_EQUALS:
                return buildBinaryExpr(operands.get(0), operands.get(1), "NotEq", inputFieldNames);
            case LESS_THAN:
                return buildBinaryExpr(operands.get(0), operands.get(1), "Lt", inputFieldNames);
            case LESS_THAN_OR_EQUAL:
                return buildBinaryExpr(operands.get(0), operands.get(1), "LtEq", inputFieldNames);
            case GREATER_THAN:
                return buildBinaryExpr(operands.get(0), operands.get(1), "Gt", inputFieldNames);
            case GREATER_THAN_OR_EQUAL:
                return buildBinaryExpr(operands.get(0), operands.get(1), "GtEq", inputFieldNames);

                // Arithmetic operators
            case PLUS:
                return buildBinaryExpr(operands.get(0), operands.get(1), "Plus", inputFieldNames);
            case MINUS:
                return buildBinaryExpr(operands.get(0), operands.get(1), "Minus", inputFieldNames);
            case TIMES:
                return buildBinaryExpr(operands.get(0), operands.get(1), "Multiply", inputFieldNames);
            case DIVIDE:
                return buildBinaryExpr(operands.get(0), operands.get(1), "Divide", inputFieldNames);

                // Logical operators (use short-circuit versions for safety)
            case AND:
                return buildShortCircuitAnd(operands.get(0), operands.get(1), inputFieldNames);
            case OR:
                return buildShortCircuitOr(operands.get(0), operands.get(1), inputFieldNames);
            case NOT:
                return buildNot(operands.get(0), inputFieldNames);

            default:
                throw new UnsupportedOperationException("Unsupported operator: " + kind + " in call: " + call);
        }
    }

    /**
     * Builds a binary expression node.
     */
    private static PhysicalExprNode buildBinaryExpr(
            RexNode left, RexNode right, String op, List<String> inputFieldNames) {
        PhysicalBinaryExprNode binaryExpr = PhysicalBinaryExprNode.newBuilder()
                .setL(convertRexNode(left, inputFieldNames))
                .setR(convertRexNode(right, inputFieldNames))
                .setOp(op)
                .build();

        return PhysicalExprNode.newBuilder().setBinaryExpr(binaryExpr).build();
    }

    /**
     * Builds a short-circuit AND expression.
     */
    private static PhysicalExprNode buildShortCircuitAnd(RexNode left, RexNode right, List<String> inputFieldNames) {
        PhysicalSCAndExprNode andExpr = PhysicalSCAndExprNode.newBuilder()
                .setLeft(convertRexNode(left, inputFieldNames))
                .setRight(convertRexNode(right, inputFieldNames))
                .build();

        return PhysicalExprNode.newBuilder().setScAndExpr(andExpr).build();
    }

    /**
     * Builds a short-circuit OR expression.
     */
    private static PhysicalExprNode buildShortCircuitOr(RexNode left, RexNode right, List<String> inputFieldNames) {
        PhysicalSCOrExprNode orExpr = PhysicalSCOrExprNode.newBuilder()
                .setLeft(convertRexNode(left, inputFieldNames))
                .setRight(convertRexNode(right, inputFieldNames))
                .build();

        return PhysicalExprNode.newBuilder().setScOrExpr(orExpr).build();
    }

    /**
     * Builds a NOT expression.
     */
    private static PhysicalExprNode buildNot(RexNode operand, List<String> inputFieldNames) {
        PhysicalNot notExpr = PhysicalNot.newBuilder()
                .setExpr(convertRexNode(operand, inputFieldNames))
                .build();

        return PhysicalExprNode.newBuilder().setNotExpr(notExpr).build();
    }

    /**
     * Converts Calcite RelDataType to Arrow ArrowType for literal serialization.
     */
    private static ArrowType convertCalciteToArrowType(org.apache.calcite.rel.type.RelDataType calciteType) {
        switch (calciteType.getSqlTypeName()) {
            case BOOLEAN:
                return new ArrowType.Bool();
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
            case REAL:
                return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            case CHAR:
            case VARCHAR:
                return new ArrowType.Utf8();
            case DATE:
                return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
            case DECIMAL:
                int precision = calciteType.getPrecision();
                int scale = calciteType.getScale();
                return new ArrowType.Decimal(precision, scale, 128);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Calcite type for literal: " + calciteType.getSqlTypeName());
        }
    }

    /**
     * Converts Flink LogicalType to Arrow ArrowType for literal serialization.
     */
    private static ArrowType convertFlinkToArrowType(LogicalType flinkType) {
        switch (flinkType.getTypeRoot()) {
            case BOOLEAN:
                return new ArrowType.Bool();
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
                return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE);
            case CHAR:
            case VARCHAR:
                return new ArrowType.Utf8();
            case DATE:
                return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);
            default:
                throw new UnsupportedOperationException("Unsupported type for literal: " + flinkType);
        }
    }

    /**
     * Writes a value to an Arrow vector based on the Calcite type.
     */
    private static void writeValueToVector(
            org.apache.arrow.vector.FieldVector vector,
            Object value,
            org.apache.calcite.rel.type.RelDataType calciteType) {
        switch (calciteType.getSqlTypeName()) {
            case BOOLEAN:
                ((BitVector) vector).setSafe(0, (Boolean) value ? 1 : 0);
                break;
            case TINYINT:
                ((TinyIntVector) vector).setSafe(0, ((Number) value).byteValue());
                break;
            case SMALLINT:
                ((SmallIntVector) vector).setSafe(0, ((Number) value).shortValue());
                break;
            case INTEGER:
                ((IntVector) vector).setSafe(0, ((Number) value).intValue());
                break;
            case BIGINT:
                ((BigIntVector) vector).setSafe(0, ((Number) value).longValue());
                break;
            case FLOAT:
            case REAL:
                ((Float4Vector) vector).setSafe(0, ((Number) value).floatValue());
                break;
            case DOUBLE:
                ((Float8Vector) vector).setSafe(0, ((Number) value).doubleValue());
                break;
            case CHAR:
            case VARCHAR:
                String strValue = (value instanceof NlsString) ? ((NlsString) value).getValue() : value.toString();
                ((VarCharVector) vector).setSafe(0, strValue.getBytes());
                break;
            case DATE:
                // Calcite stores dates as days since epoch
                ((DateDayVector) vector).setSafe(0, ((Number) value).intValue());
                break;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Calcite stores timestamps as milliseconds, convert to microseconds
                long millis = ((Number) value).longValue();
                ((TimeStampMicroVector) vector).setSafe(0, millis * 1000);
                break;
            case DECIMAL:
                // Calcite stores decimals as BigDecimal
                // DecimalVector requires scale which is available from calciteType
                ((org.apache.arrow.vector.DecimalVector) vector).setSafe(0, (java.math.BigDecimal) value);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Calcite type for literal write: " + calciteType.getSqlTypeName());
        }
    }
}
