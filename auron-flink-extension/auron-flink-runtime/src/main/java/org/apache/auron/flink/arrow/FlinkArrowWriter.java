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
package org.apache.auron.flink.arrow;

import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

/** Writer that converts Flink RowData to Arrow VectorSchemaRoot. */
public class FlinkArrowWriter {

    private final VectorSchemaRoot root;
    private final FlinkArrowFieldWriter[] fieldWriters;

    private FlinkArrowWriter(VectorSchemaRoot root, FlinkArrowFieldWriter[] fieldWriters) {
        this.root = root;
        this.fieldWriters = fieldWriters;
    }

    /**
     * Creates a FlinkArrowWriter from a Flink RowType.
     *
     * @param rowType The Flink row type
     * @return A new FlinkArrowWriter instance
     */
    public static FlinkArrowWriter create(RowType rowType) {
        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, FlinkArrowUtils.ROOT_ALLOCATOR);
        return create(root, rowType);
    }

    /**
     * Creates a FlinkArrowWriter from a Flink RowType with a custom allocator.
     *
     * @param rowType   The Flink row type
     * @param allocator The buffer allocator to use
     * @return A new FlinkArrowWriter instance
     */
    public static FlinkArrowWriter create(RowType rowType, BufferAllocator allocator) {
        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
        return create(root, rowType);
    }

    /**
     * Creates a FlinkArrowWriter from an existing VectorSchemaRoot and RowType.
     *
     * @param root    The Arrow VectorSchemaRoot
     * @param rowType The Flink row type
     * @return A new FlinkArrowWriter instance
     */
    public static FlinkArrowWriter create(VectorSchemaRoot root, RowType rowType) {
        List<RowType.RowField> fields = rowType.getFields();
        List<FieldVector> vectors = root.getFieldVectors();

        if (fields.size() != vectors.size()) {
            throw new IllegalArgumentException("Field count mismatch: RowType has "
                    + fields.size()
                    + " fields but VectorSchemaRoot has "
                    + vectors.size()
                    + " vectors");
        }

        // Initialize vectors with small initial capacity
        for (FieldVector vector : vectors) {
            vector.setInitialCapacity(16);
            vector.allocateNew();
        }

        // Create field writers
        FlinkArrowFieldWriter[] writers = new FlinkArrowFieldWriter[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            writers[i] = createFieldWriter(vectors.get(i), fields.get(i).getType());
        }

        return new FlinkArrowWriter(root, writers);
    }

    /**
     * Creates a field writer for the given vector and logical type.
     *
     * @param vector      The Arrow vector
     * @param logicalType The Flink logical type
     * @return A FlinkArrowFieldWriter instance
     */
    private static FlinkArrowFieldWriter createFieldWriter(ValueVector vector, LogicalType logicalType) {
        if (logicalType instanceof NullType) {
            return new FlinkArrowFieldWriter.NullWriter((org.apache.arrow.vector.NullVector) vector);
        } else if (logicalType instanceof BooleanType) {
            return new FlinkArrowFieldWriter.BooleanWriter((org.apache.arrow.vector.BitVector) vector);
        } else if (logicalType instanceof TinyIntType) {
            return new FlinkArrowFieldWriter.TinyIntWriter((org.apache.arrow.vector.TinyIntVector) vector);
        } else if (logicalType instanceof SmallIntType) {
            return new FlinkArrowFieldWriter.SmallIntWriter((org.apache.arrow.vector.SmallIntVector) vector);
        } else if (logicalType instanceof IntType) {
            return new FlinkArrowFieldWriter.IntWriter((org.apache.arrow.vector.IntVector) vector);
        } else if (logicalType instanceof BigIntType) {
            return new FlinkArrowFieldWriter.BigIntWriter((org.apache.arrow.vector.BigIntVector) vector);
        } else if (logicalType instanceof FloatType) {
            return new FlinkArrowFieldWriter.FloatWriter((org.apache.arrow.vector.Float4Vector) vector);
        } else if (logicalType instanceof DoubleType) {
            return new FlinkArrowFieldWriter.DoubleWriter((org.apache.arrow.vector.Float8Vector) vector);
        } else if (logicalType instanceof VarCharType || logicalType instanceof CharType) {
            return new FlinkArrowFieldWriter.StringWriter((org.apache.arrow.vector.VarCharVector) vector);
        } else if (logicalType instanceof VarBinaryType || logicalType instanceof BinaryType) {
            return new FlinkArrowFieldWriter.BinaryWriter((org.apache.arrow.vector.VarBinaryVector) vector);
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            return new FlinkArrowFieldWriter.DecimalWriter(
                    (org.apache.arrow.vector.DecimalVector) vector, decimalType.getPrecision(), decimalType.getScale());
        } else if (logicalType instanceof DateType) {
            return new FlinkArrowFieldWriter.DateWriter((org.apache.arrow.vector.DateDayVector) vector);
        } else if (logicalType instanceof TimeType) {
            return new FlinkArrowFieldWriter.TimeWriter((org.apache.arrow.vector.TimeMicroVector) vector);
        } else if (logicalType instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) logicalType;
            return new FlinkArrowFieldWriter.TimestampWriter(
                    (org.apache.arrow.vector.TimeStampMicroVector) vector, timestampType.getPrecision());
        } else if (logicalType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType lzType = (LocalZonedTimestampType) logicalType;
            return new FlinkArrowFieldWriter.LocalZonedTimestampWriter(
                    (org.apache.arrow.vector.TimeStampMicroTZVector) vector, lzType.getPrecision());
        } else if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            ListVector listVector = (ListVector) vector;
            // Use recursive createFieldWriter for nested types support
            FlinkArrowFieldWriter elementWriter =
                    createFieldWriter(listVector.getDataVector(), arrayType.getElementType());
            return new FlinkArrowFieldWriter.ArrayWriter(listVector, elementWriter);
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            MapVector mapVector = (MapVector) vector;
            StructVector structVector = (StructVector) mapVector.getDataVector();
            // Use recursive createFieldWriter for nested types support
            FlinkArrowFieldWriter keyWriter = createFieldWriter(
                    structVector.getChild(org.apache.arrow.vector.complex.MapVector.KEY_NAME), mapType.getKeyType());
            FlinkArrowFieldWriter valueWriter = createFieldWriter(
                    structVector.getChild(org.apache.arrow.vector.complex.MapVector.VALUE_NAME),
                    mapType.getValueType());
            return new FlinkArrowFieldWriter.MapWriter(mapVector, structVector, keyWriter, valueWriter);
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            StructVector structVector = (StructVector) vector;
            List<RowType.RowField> fields = rowType.getFields();
            FlinkArrowFieldWriter[] fieldWriters = new FlinkArrowFieldWriter[fields.size()];
            for (int i = 0; i < fields.size(); i++) {
                fieldWriters[i] = createFieldWriter(
                        structVector.getChildByOrdinal(i), fields.get(i).getType());
            }
            return new FlinkArrowFieldWriter.RowWriter(structVector, fieldWriters);
        } else {
            throw new UnsupportedOperationException("Unsupported Flink type: " + logicalType.asSummaryString());
        }
    }

    /**
     * Writes a single RowData to the Arrow vectors.
     *
     * @param row The RowData to write
     */
    public void write(RowData row) {
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(row, i);
        }
    }

    /**
     * Returns the current number of rows written.
     *
     * @return The row count
     */
    public int currentCount() {
        return fieldWriters.length > 0 ? fieldWriters[0].getCount() : 0;
    }

    /**
     * Finalizes the writing process for the current batch.
     */
    public void finish() {
        for (FlinkArrowFieldWriter writer : fieldWriters) {
            writer.finish();
        }
        root.setRowCount(currentCount());
    }

    /**
     * Resets the writer for a new batch.
     */
    public void reset() {
        for (FlinkArrowFieldWriter writer : fieldWriters) {
            writer.reset();
        }
    }

    /**
     * Gets the underlying VectorSchemaRoot.
     *
     * @return The VectorSchemaRoot
     */
    public VectorSchemaRoot getRoot() {
        return root;
    }
}
