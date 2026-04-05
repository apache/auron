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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.ScalarValue;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Converts a Calcite {@link RexLiteral} to an Auron native {@link PhysicalExprNode}
 * containing a {@link ScalarValue} with Arrow IPC bytes.
 *
 * <p>The literal value is serialized as a single-element Arrow vector in IPC stream format,
 * following the same pattern as the Spark implementation in {@code NativeConverters}.
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
     * Serializes the literal value as a single-element Arrow vector in IPC stream format.
     */
    private static byte[] serializeToIpc(RexLiteral literal) {
        Field field = arrowFieldForType(literal);
        Schema schema = new Schema(Collections.singletonList(field));

        try (BufferAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {

            root.allocateNew();
            FieldVector vector = root.getVector(0);

            if (literal.isNull()) {
                vector.setNull(0);
            } else {
                setVectorValue(literal, vector);
            }

            vector.setValueCount(1);
            root.setRowCount(1);

            return writeIpcBytes(root);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize literal to Arrow IPC", e);
        }
    }

    /**
     * Returns the Arrow {@link Field} corresponding to the literal's Calcite type.
     */
    private static Field arrowFieldForType(RexLiteral literal) {
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        switch (typeName) {
            case TINYINT:
                return Field.nullable("v", new ArrowType.Int(8, true));
            case SMALLINT:
                return Field.nullable("v", new ArrowType.Int(16, true));
            case INTEGER:
                return Field.nullable("v", new ArrowType.Int(32, true));
            case BIGINT:
                return Field.nullable("v", new ArrowType.Int(64, true));
            case FLOAT:
                return Field.nullable("v", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
            case DOUBLE:
                return Field.nullable("v", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            case DECIMAL:
                int precision = literal.getType().getPrecision();
                int scale = literal.getType().getScale();
                return Field.nullable("v", new ArrowType.Decimal(precision, scale, 128));
            case BOOLEAN:
                return Field.nullable("v", ArrowType.Bool.INSTANCE);
            case CHAR:
            case VARCHAR:
                return Field.nullable("v", ArrowType.Utf8.INSTANCE);
            default:
                throw new IllegalArgumentException("Unsupported type: " + typeName);
        }
    }

    /**
     * Sets the value at index 0 of the given vector based on the literal's type.
     */
    private static void setVectorValue(RexLiteral literal, FieldVector vector) {
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        switch (typeName) {
            case TINYINT:
                ((TinyIntVector) vector).set(0, literal.getValueAs(Byte.class));
                break;
            case SMALLINT:
                ((SmallIntVector) vector).set(0, literal.getValueAs(Short.class));
                break;
            case INTEGER:
                ((IntVector) vector).set(0, literal.getValueAs(Integer.class));
                break;
            case BIGINT:
                ((BigIntVector) vector).set(0, literal.getValueAs(Long.class));
                break;
            case FLOAT:
                ((Float4Vector) vector).set(0, literal.getValueAs(Float.class));
                break;
            case DOUBLE:
                ((Float8Vector) vector).set(0, literal.getValueAs(Double.class));
                break;
            case DECIMAL:
                ((DecimalVector) vector).set(0, literal.getValueAs(BigDecimal.class));
                break;
            case BOOLEAN:
                ((BitVector) vector).set(0, literal.getValueAs(Boolean.class) ? 1 : 0);
                break;
            case CHAR:
            case VARCHAR:
                ((VarCharVector) vector).set(0, literal.getValueAs(String.class).getBytes(StandardCharsets.UTF_8));
                break;
            default:
                throw new IllegalArgumentException("Unsupported type: " + typeName);
        }
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
