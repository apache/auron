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

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.EnumSet;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.arrow.FlinkArrowWriter;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.ScalarValue;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Converts a Calcite {@link RexLiteral} to an Auron native {@link PhysicalExprNode}
 * containing a {@link ScalarValue} with Arrow IPC bytes.
 *
 * <p>The literal value is serialized as a single-element Arrow record batch in IPC stream
 * format, using Flink's {@link GenericRowData} and {@link FlinkArrowWriter} to perform
 * type-aware conversion via the project's shared {@link FlinkArrowUtils} infrastructure.
 *
 * <p>Supported types: {@code TINYINT}, {@code SMALLINT}, {@code INTEGER}, {@code BIGINT},
 * {@code FLOAT}, {@code DOUBLE}, {@code DECIMAL}, {@code BOOLEAN}, {@code CHAR},
 * {@code VARCHAR}, and {@code NULL} (of a supported type).
 */
public class RexLiteralConverter implements FlinkRexNodeConverter {

    private static final Set<SqlTypeName> SUPPORTED_TYPES = EnumSet.of(
            SqlTypeName.TINYINT,
            SqlTypeName.SMALLINT,
            SqlTypeName.INTEGER,
            SqlTypeName.BIGINT,
            SqlTypeName.FLOAT,
            SqlTypeName.DOUBLE,
            SqlTypeName.DECIMAL,
            SqlTypeName.BOOLEAN,
            SqlTypeName.CHAR,
            SqlTypeName.VARCHAR);

    /** {@inheritDoc} */
    @Override
    public Class<? extends RexNode> getNodeClass() {
        return RexLiteral.class;
    }

    /**
     * Returns {@code true} if the literal's SQL type is supported for native conversion.
     *
     * <p>For null literals, the underlying type is still checked — a null of an unsupported
     * type (e.g., TIMESTAMP) returns {@code false}.
     */
    @Override
    public boolean isSupported(RexNode node, ConverterContext context) {
        RexLiteral literal = (RexLiteral) node;
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        return isSupportedType(typeName);
    }

    /**
     * Converts the given {@link RexLiteral} to a {@link PhysicalExprNode} with Arrow IPC bytes.
     *
     * @throws IllegalArgumentException if the literal type is not supported
     */
    @Override
    public PhysicalExprNode convert(RexNode node, ConverterContext context) {
        RexLiteral literal = (RexLiteral) node;
        byte[] ipcBytes = serializeToIpc(literal);
        return PhysicalExprNode.newBuilder()
                .setLiteral(ScalarValue.newBuilder().setIpcBytes(ByteString.copyFrom(ipcBytes)))
                .build();
    }

    private static boolean isSupportedType(SqlTypeName typeName) {
        return SUPPORTED_TYPES.contains(typeName);
    }

    /**
     * Serializes the literal value as a single-element Arrow record batch in IPC stream format.
     *
     * <p>Uses Flink's {@link GenericRowData} and {@link FlinkArrowWriter} for type-aware
     * value conversion, delegating all type-to-Arrow mapping to {@link FlinkArrowUtils}.
     */
    private static byte[] serializeToIpc(RexLiteral literal) {
        LogicalType logicalType = FlinkTypeFactory.toLogicalType(literal.getType());
        RowType rowType = RowType.of(logicalType);

        try (BufferAllocator allocator =
                        FlinkArrowUtils.getRootAllocator().newChildAllocator("literal", 0, Long.MAX_VALUE);
                VectorSchemaRoot root = VectorSchemaRoot.create(FlinkArrowUtils.toArrowSchema(rowType), allocator)) {

            GenericRowData rowData = new GenericRowData(1);
            rowData.setField(0, toFlinkInternalValue(literal, logicalType));

            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);
            writer.write(rowData);
            writer.finish();

            return writeIpcBytes(root);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize literal to Arrow IPC", e);
        }
    }

    /**
     * Converts a {@link RexLiteral} value to its Flink internal RowData representation.
     *
     * <p>Returns {@code null} for null literals. For DECIMAL, returns {@link DecimalData};
     * for CHAR/VARCHAR, returns {@link StringData}. For numeric and boolean types, returns
     * the correctly-typed boxed Java value that {@link FlinkArrowWriter} expects.
     *
     * @param literal the Calcite literal to convert
     * @param logicalType the Flink logical type of the literal
     * @return the Flink internal value, or {@code null} if the literal is null
     * @throws IllegalArgumentException if the logical type is not supported
     */
    private static Object toFlinkInternalValue(RexLiteral literal, LogicalType logicalType) {
        if (literal.isNull()) {
            return null;
        }
        if (logicalType instanceof TinyIntType) {
            return literal.getValueAs(Byte.class);
        }
        if (logicalType instanceof SmallIntType) {
            return literal.getValueAs(Short.class);
        }
        if (logicalType instanceof IntType) {
            return literal.getValueAs(Integer.class);
        }
        if (logicalType instanceof BigIntType) {
            return literal.getValueAs(Long.class);
        }
        if (logicalType instanceof FloatType) {
            return literal.getValueAs(Float.class);
        }
        if (logicalType instanceof DoubleType) {
            return literal.getValueAs(Double.class);
        }
        if (logicalType instanceof BooleanType) {
            return literal.getValueAs(Boolean.class);
        }
        if (logicalType instanceof DecimalType) {
            DecimalType dt = (DecimalType) logicalType;
            return DecimalData.fromBigDecimal(literal.getValueAs(BigDecimal.class), dt.getPrecision(), dt.getScale());
        }
        if (logicalType instanceof CharType || logicalType instanceof VarCharType) {
            return StringData.fromString(literal.getValueAs(String.class));
        }
        throw new IllegalArgumentException("Unsupported logical type: " + logicalType.asSummaryString());
    }

    /**
     * Writes the given {@link VectorSchemaRoot} to Arrow IPC stream format.
     */
    private static byte[] writeIpcBytes(VectorSchemaRoot root) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return out.toByteArray();
    }
}
