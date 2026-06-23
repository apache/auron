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

import org.apache.arrow.vector.TimeMicroVector;
import org.apache.flink.table.data.columnar.vector.IntColumnVector;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link IntColumnVector} backed by an Arrow {@link TimeMicroVector}.
 *
 * <p>This wrapper delegates all reads to the underlying Arrow vector, providing zero-copy access
 * to Arrow data from Flink's columnar batch execution engine. {@link TimeMicroVector} stores time
 * values as microseconds since midnight ({@code long}), which are converted to milliseconds
 * ({@code int}) to match Flink's internal TIME representation. The native engine (DataFusion)
 * uses microsecond precision for all temporal types.
 */
public final class ArrowTimeColumnVector implements IntColumnVector {

    private final TimeMicroVector vector;

    /**
     * Creates a new wrapper around the given Arrow {@link TimeMicroVector}.
     *
     * @param vector the Arrow vector to wrap, must not be null
     */
    public ArrowTimeColumnVector(TimeMicroVector vector) {
        this.vector = Preconditions.checkNotNull(vector);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /**
     * Returns the time value at the given index as milliseconds since midnight.
     *
     * <p>The underlying Arrow vector stores microseconds; this method divides by 1000 to produce
     * the millisecond value expected by Flink's internal TIME type.
     *
     * @param i the row index
     * @return time of day in milliseconds since midnight
     */
    @Override
    public int getInt(int i) {
        return (int) (vector.get(i) / 1000);
    }
}
