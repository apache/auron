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

import org.apache.arrow.vector.complex.ListVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.columnar.ColumnarArrayData;
import org.apache.flink.table.data.columnar.vector.ArrayColumnVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link ArrayColumnVector} backed by an Arrow {@link ListVector}.
 *
 * <p>This wrapper delegates all reads to the underlying Arrow vector, providing zero-copy access
 * to Arrow list data from Flink's columnar batch execution engine. The child element vector is
 * itself a Flink {@link ColumnVector} wrapping the corresponding Arrow child vector, enabling
 * recursive nesting of complex types.
 */
public final class ArrowArrayColumnVector implements ArrayColumnVector {

    private final ListVector vector;
    private final ColumnVector elementColumnVector;

    /**
     * Creates a new wrapper around the given Arrow {@link ListVector}.
     *
     * @param vector the Arrow list vector to wrap, must not be null
     * @param elementColumnVector the Flink column vector wrapping the Arrow child element vector,
     *     must not be null
     */
    public ArrowArrayColumnVector(ListVector vector, ColumnVector elementColumnVector) {
        this.vector = Preconditions.checkNotNull(vector);
        this.elementColumnVector = Preconditions.checkNotNull(elementColumnVector);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /** {@inheritDoc} */
    @Override
    public ArrayData getArray(int i) {
        int offset = vector.getOffsetBuffer().getInt((long) i * ListVector.OFFSET_WIDTH);
        int length = vector.getOffsetBuffer().getInt((long) (i + 1) * ListVector.OFFSET_WIDTH) - offset;
        return new ColumnarArrayData(elementColumnVector, offset, length);
    }
}
