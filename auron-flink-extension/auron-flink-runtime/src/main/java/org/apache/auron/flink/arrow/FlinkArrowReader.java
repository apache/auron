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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.auron.flink.arrow.vectors.ArrowArrayColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowBigIntColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowBooleanColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowDateColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowDecimalColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowDoubleColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowFloatColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowIntColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowMapColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowRowColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowSmallIntColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowTimeColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowTimestampColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowTinyIntColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowVarBinaryColumnVector;
import org.apache.auron.flink.arrow.vectors.ArrowVarCharColumnVector;
import org.apache.auron.flink.table.data.AuronColumnarRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

/**
 * Reads Arrow {@link VectorSchemaRoot} data as Flink {@link RowData}.
 *
 * <p>Uses Flink's columnar data structures ({@link ColumnarRowData} backed by {@link
 * VectorizedColumnBatch}) for zero-copy access to Arrow vectors. Each Arrow {@link FieldVector} is
 * wrapped in a Flink {@link ColumnVector} implementation that delegates reads directly to the
 * underlying Arrow vector.
 *
 * <p>Object reuse: {@link #read(int)} returns the same {@link ColumnarRowData} instance with a
 * different row ID. Callers must consume or copy the returned row before the next call. This is
 * standard Flink practice for columnar readers.
 *
 * <p>Implements {@link AutoCloseable} for use in resource management blocks. Note: this does NOT
 * close the underlying {@link VectorSchemaRoot}. The caller that created the root is responsible
 * for its lifecycle.
 */
public class FlinkArrowReader implements AutoCloseable {

    private final RowType rowType;
    private ColumnVector[] columnVectors;
    private VectorizedColumnBatch batch;
    private AuronColumnarRowData reusableRow;
    private VectorSchemaRoot root;

    private FlinkArrowReader(ColumnVector[] columnVectors, VectorSchemaRoot root, RowType rowType) {
        this(columnVectors, root, rowType, 0);
    }

    private FlinkArrowReader(
            ColumnVector[] columnVectors, VectorSchemaRoot root, RowType rowType, int dataColStartIndex) {
        this.columnVectors = columnVectors;
        this.batch = new VectorizedColumnBatch(columnVectors);
        this.reusableRow = new AuronColumnarRowData(batch);
        if (dataColStartIndex > 0) {
            this.reusableRow.setDataColStartIndex(dataColStartIndex);
        }
        this.root = root;
        this.rowType = rowType;
    }

    /**
     * Creates a {@link FlinkArrowReader} from a {@link VectorSchemaRoot} and {@link RowType}.
     *
     * <p>The RowType must match the schema of the VectorSchemaRoot (same number of fields, matching
     * types). Each Arrow field vector is wrapped in the appropriate Flink {@link ColumnVector}
     * implementation based on the corresponding Flink {@link LogicalType}.
     *
     * @param root the Arrow VectorSchemaRoot containing the data
     * @param rowType the Flink RowType describing the schema
     * @return a new FlinkArrowReader
     * @throws IllegalArgumentException if field counts do not match
     * @throws UnsupportedOperationException if a LogicalType is not supported
     */
    public static FlinkArrowReader create(VectorSchemaRoot root, RowType rowType) {
        return create(root, rowType, 0);
    }

    /**
     * Creates a {@link FlinkArrowReader} from a {@link VectorSchemaRoot} and {@link RowType}.
     *
     * <p>The RowType must match the schema of the VectorSchemaRoot (same number of fields, matching
     * types). Each Arrow field vector is wrapped in the appropriate Flink {@link ColumnVector}
     * implementation based on the corresponding Flink {@link LogicalType}.
     *
     * @param root the Arrow VectorSchemaRoot containing the data
     * @param rowType the Flink RowType describing the schema
     * @param dataColStartIndex the index of the first user data column
     * @return a new FlinkArrowReader
     * @throws IllegalArgumentException if field counts do not match
     * @throws UnsupportedOperationException if a LogicalType is not supported
     */
    public static FlinkArrowReader create(VectorSchemaRoot root, RowType rowType, int dataColStartIndex) {
        Preconditions.checkNotNull(root, "root must not be null");
        Preconditions.checkNotNull(rowType, "rowType must not be null");
        List<FieldVector> fieldVectors = root.getFieldVectors();
        List<RowType.RowField> fields = rowType.getFields();
        if (fieldVectors.size() != fields.size()) {
            throw new IllegalArgumentException(
                    "VectorSchemaRoot has " + fieldVectors.size() + " fields but RowType has " + fields.size());
        }
        ColumnVector[] columns = new ColumnVector[fieldVectors.size()];
        for (int i = 0; i < fieldVectors.size(); i++) {
            columns[i] = createColumnVector(fieldVectors.get(i), fields.get(i).getType());
        }
        return new FlinkArrowReader(columns, root, rowType, dataColStartIndex);
    }

    /**
     * Reads a row at the given position. Returns a reused {@link RowData} object — callers must not
     * hold references across calls.
     *
     * @param rowId the row index within the current batch
     * @return the row data at the given position
     */
    public RowData read(int rowId) {
        reusableRow.setRowId(rowId);
        return reusableRow;
    }

    /**
     * Returns the number of rows in the current batch.
     *
     * @return the row count
     */
    public int getRowCount() {
        return root.getRowCount();
    }

    /**
     * Resets the reader to use a new {@link VectorSchemaRoot} with the same schema. Recreates
     * column vector wrappers for the new root's field vectors.
     *
     * <p>The new root must have the same schema (same number and types of fields) as the original.
     *
     * @param newRoot the new VectorSchemaRoot, must not be null
     */
    public void reset(VectorSchemaRoot newRoot) {
        reset(newRoot, 0);
    }

    /**
     * Resets the reader to use a new {@link VectorSchemaRoot} with the same schema. Recreates
     * column vector wrappers for the new root's field vectors.
     *
     * <p>The new root must have the same schema (same number and types of fields) as the original.
     *
     * @param newRoot the new VectorSchemaRoot, must not be null
     * @param dataColStartIndex the index of the first user data column
     */
    public void reset(VectorSchemaRoot newRoot, int dataColStartIndex) {
        Preconditions.checkNotNull(newRoot, "newRoot must not be null");
        this.root = newRoot;
        List<FieldVector> newVectors = newRoot.getFieldVectors();
        Preconditions.checkArgument(
                newVectors.size() == columnVectors.length,
                "New root has %s fields but reader expects %s",
                newVectors.size(),
                columnVectors.length);
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < columnVectors.length; i++) {
            columnVectors[i] =
                    createColumnVector(newVectors.get(i), fields.get(i).getType());
        }
        this.batch = new VectorizedColumnBatch(columnVectors);
        this.reusableRow = new AuronColumnarRowData(batch);
        if (dataColStartIndex > 0) {
            this.reusableRow.setDataColStartIndex(dataColStartIndex);
        }
    }

    /**
     * Implements {@link AutoCloseable} for use in resource management blocks. Note: this does NOT
     * close the underlying {@link VectorSchemaRoot}. The caller that created the root is responsible
     * for its lifecycle.
     */
    @Override
    public void close() {
        // Reader is a view; root lifecycle managed by caller.
    }

    /**
     * Creates the appropriate Flink {@link ColumnVector} wrapper for the given Arrow {@link
     * FieldVector} and Flink {@link LogicalType}.
     */
    static ColumnVector createColumnVector(FieldVector vector, LogicalType type) {
        switch (type.getTypeRoot()) {
            case BOOLEAN:
                return new ArrowBooleanColumnVector((BitVector) vector);
            case TINYINT:
                return new ArrowTinyIntColumnVector((TinyIntVector) vector);
            case SMALLINT:
                return new ArrowSmallIntColumnVector((SmallIntVector) vector);
            case INTEGER:
                return new ArrowIntColumnVector((IntVector) vector);
            case BIGINT:
                return new ArrowBigIntColumnVector((BigIntVector) vector);
            case FLOAT:
                return new ArrowFloatColumnVector((Float4Vector) vector);
            case DOUBLE:
                return new ArrowDoubleColumnVector((Float8Vector) vector);
            case VARCHAR:
            case CHAR:
                return new ArrowVarCharColumnVector((VarCharVector) vector);
            case VARBINARY:
            case BINARY:
                return new ArrowVarBinaryColumnVector((VarBinaryVector) vector);
            case DECIMAL:
                return new ArrowDecimalColumnVector((DecimalVector) vector);
            case DATE:
                return new ArrowDateColumnVector((DateDayVector) vector);
            case TIME_WITHOUT_TIME_ZONE:
                // The native engine (DataFusion) uses microsecond precision for all temporal
                // types, producing TimeMicroVector regardless of declared Flink TIME precision.
                return new ArrowTimeColumnVector((TimeMicroVector) vector);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // The native engine (DataFusion) uses microsecond precision for all temporal
                // types. TimeStampVector is the common parent of TimeStampMicroVector and
                // TimeStampMicroTZVector.
                return new ArrowTimestampColumnVector((TimeStampVector) vector);
            case ARRAY:
                return createArrayColumnVector((ListVector) vector, (ArrayType) type);
            case MAP:
                return createMapColumnVector((MapVector) vector, (MapType) type);
            case ROW:
                return createRowColumnVector((StructVector) vector, (RowType) type);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Flink type for Arrow reader: " + type.asSummaryString());
        }
    }

    private static ColumnVector createArrayColumnVector(ListVector vector, ArrayType arrayType) {
        ColumnVector elementVector = createColumnVector(vector.getDataVector(), arrayType.getElementType());
        return new ArrowArrayColumnVector(vector, elementVector);
    }

    private static ColumnVector createMapColumnVector(MapVector vector, MapType mapType) {
        StructVector entriesVector = (StructVector) vector.getDataVector();
        ColumnVector keyVector = createColumnVector(entriesVector.getChild(MapVector.KEY_NAME), mapType.getKeyType());
        ColumnVector valueVector =
                createColumnVector(entriesVector.getChild(MapVector.VALUE_NAME), mapType.getValueType());
        return new ArrowMapColumnVector(vector, keyVector, valueVector);
    }

    private static ColumnVector createRowColumnVector(StructVector vector, RowType rowType) {
        List<FieldVector> childVectors = vector.getChildrenFromFields();
        List<RowType.RowField> fields = rowType.getFields();
        ColumnVector[] childColumns = new ColumnVector[childVectors.size()];
        for (int i = 0; i < childVectors.size(); i++) {
            childColumns[i] =
                    createColumnVector(childVectors.get(i), fields.get(i).getType());
        }
        return new ArrowRowColumnVector(vector, childColumns);
    }
}
