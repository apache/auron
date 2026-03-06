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
package org.apache.auron.flink.arrow.vectors;

import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.RowColumnVector;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link RowColumnVector} backed by an Arrow {@link StructVector}.
 *
 * <p>This wrapper delegates all reads to the underlying Arrow vector, providing zero-copy access
 * to Arrow struct data from Flink's columnar batch execution engine. Each struct field is
 * represented as a Flink {@link ColumnVector} wrapping the corresponding Arrow child vector,
 * enabling recursive nesting of complex types. The child vectors are bundled into a {@link
 * VectorizedColumnBatch} and accessed through a reusable {@link ColumnarRowData} instance.
 */
public final class ArrowRowColumnVector implements RowColumnVector {

    private StructVector vector;
    private VectorizedColumnBatch childBatch;
    private ColumnarRowData reusableRow;

    /**
     * Creates a new wrapper around the given Arrow {@link StructVector}.
     *
     * @param vector the Arrow struct vector to wrap, must not be null
     * @param childColumnVectors the Flink column vectors wrapping each Arrow child field vector,
     *     must not be null
     */
    public ArrowRowColumnVector(StructVector vector, ColumnVector[] childColumnVectors) {
        this.vector = Preconditions.checkNotNull(vector);
        this.childBatch = new VectorizedColumnBatch(Preconditions.checkNotNull(childColumnVectors));
        this.reusableRow = new ColumnarRowData(childBatch);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /** {@inheritDoc} */
    @Override
    public ColumnarRowData getRow(int i) {
        reusableRow.setRowId(i);
        return reusableRow;
    }

    /**
     * Replaces the underlying Arrow vector and child field vectors. Used during reader reset to
     * point at a new batch without allocating a new wrapper.
     *
     * @param vector the new Arrow struct vector, must not be null
     * @param childColumnVectors the new Flink column vectors for child fields, must not be null
     */
    void setVector(StructVector vector, ColumnVector[] childColumnVectors) {
        this.vector = Preconditions.checkNotNull(vector);
        this.childBatch = new VectorizedColumnBatch(Preconditions.checkNotNull(childColumnVectors));
        this.reusableRow = new ColumnarRowData(childBatch);
    }
}
