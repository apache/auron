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

import org.apache.arrow.vector.complex.MapVector;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.columnar.ColumnarMapData;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.MapColumnVector;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link MapColumnVector} backed by an Arrow {@link MapVector}.
 *
 * <p>This wrapper delegates all reads to the underlying Arrow vector, providing zero-copy access
 * to Arrow map data from Flink's columnar batch execution engine. Arrow represents maps as a list
 * of struct entries, where each entry contains a "key" and "value" child vector. The key and value
 * Flink {@link ColumnVector} instances wrap the corresponding Arrow child vectors, enabling
 * recursive nesting of complex types.
 */
public final class ArrowMapColumnVector implements MapColumnVector {

    private final MapVector vector;
    private final ColumnVector keyColumnVector;
    private final ColumnVector valueColumnVector;

    /**
     * Creates a new wrapper around the given Arrow {@link MapVector}.
     *
     * @param vector the Arrow map vector to wrap, must not be null
     * @param keyColumnVector the Flink column vector wrapping the Arrow key child vector, must not
     *     be null
     * @param valueColumnVector the Flink column vector wrapping the Arrow value child vector, must
     *     not be null
     */
    public ArrowMapColumnVector(MapVector vector, ColumnVector keyColumnVector, ColumnVector valueColumnVector) {
        this.vector = Preconditions.checkNotNull(vector);
        this.keyColumnVector = Preconditions.checkNotNull(keyColumnVector);
        this.valueColumnVector = Preconditions.checkNotNull(valueColumnVector);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /** {@inheritDoc} */
    @Override
    public MapData getMap(int i) {
        int offset = vector.getOffsetBuffer().getInt((long) i * MapVector.OFFSET_WIDTH);
        int length = vector.getOffsetBuffer().getInt((long) (i + 1) * MapVector.OFFSET_WIDTH) - offset;
        return new ColumnarMapData(keyColumnVector, valueColumnVector, offset, length);
    }
}
