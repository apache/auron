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

import org.apache.arrow.vector.TimeStampVector;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.TimestampColumnVector;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link TimestampColumnVector} backed by an Arrow {@link TimeStampVector}.
 *
 * <p>This wrapper delegates all reads to the underlying Arrow vector, providing zero-copy access
 * to Arrow data from Flink's columnar batch execution engine. It handles both {@code
 * TimeStampMicroVector} (TIMESTAMP) and {@code TimeStampMicroTZVector} (TIMESTAMP_LTZ) by
 * accepting their common parent type {@link TimeStampVector}. The native engine (DataFusion)
 * uses microsecond precision for all temporal types. Microsecond values are converted to Flink's
 * {@link TimestampData} representation (epoch millis + sub-millisecond nanos).
 */
public final class ArrowTimestampColumnVector implements TimestampColumnVector {

    private final TimeStampVector vector;

    /**
     * Creates a new wrapper around the given Arrow {@link TimeStampVector}.
     *
     * <p>Accepts both {@code TimeStampMicroVector} and {@code TimeStampMicroTZVector} since they
     * share the same storage format and parent type.
     *
     * @param vector the Arrow vector to wrap, must not be null
     */
    public ArrowTimestampColumnVector(TimeStampVector vector) {
        this.vector = Preconditions.checkNotNull(vector);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /**
     * Returns the timestamp at the given index as a {@link TimestampData}.
     *
     * <p>The underlying Arrow vector stores microseconds since epoch. This method splits the value
     * into epoch milliseconds and sub-millisecond nanoseconds to construct a {@link TimestampData}.
     *
     * @param i the row index
     * @param precision the timestamp precision (unused; conversion is always from microseconds)
     * @return the timestamp value
     */
    @Override
    public TimestampData getTimestamp(int i, int precision) {
        long micros = vector.get(i);
        // Use floor-based division so that for negative micros (pre-epoch), the remainder is
        // non-negative and nanoOfMillisecond stays within [0, 999_999], as required by
        // TimestampData.fromEpochMillis.
        long millis = Math.floorDiv(micros, 1000);
        int nanoOfMillisecond = ((int) Math.floorMod(micros, 1000)) * 1000;
        return TimestampData.fromEpochMillis(millis, nanoOfMillisecond);
    }
}
