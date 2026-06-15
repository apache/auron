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
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.TimestampColumnVector;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link TimestampColumnVector} backed by an Arrow {@link TimeStampVector}.
 *
 * <p>This wrapper delegates all reads to the underlying Arrow vector, providing zero-copy access
 * to Arrow data from Flink's columnar batch execution engine. It handles all four Arrow
 * timestamp time units (second, millisecond, microsecond, nanosecond) by reading the unit from
 * the vector's {@link ArrowType.Timestamp} metadata and converting to Flink's
 * {@link TimestampData} representation (epoch millis + sub-millisecond nanos).
 */
public final class ArrowTimestampColumnVector implements TimestampColumnVector {

    private final TimeStampVector vector;
    private final TimeUnit timeUnit;

    /**
     * Creates a new wrapper around the given Arrow {@link TimeStampVector}.
     *
     * <p>Accepts any of {@code TimeStampSecVector}, {@code TimeStampMilliVector},
     * {@code TimeStampMicroVector}, {@code TimeStampNanoVector} (and their TZ variants).
     *
     * @param vector the Arrow vector to wrap, must not be null
     */
    public ArrowTimestampColumnVector(TimeStampVector vector) {
        this.vector = Preconditions.checkNotNull(vector);
        this.timeUnit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /**
     * Returns the timestamp at the given index as a {@link TimestampData}.
     *
     * <p>Reads the underlying Arrow value (whose unit is determined by the vector's
     * {@link ArrowType.Timestamp} metadata, captured at construction) and converts to Flink's
     * {@link TimestampData} representation (epoch millis + sub-millisecond nanos).
     *
     * @param i the row index
     * @param precision the timestamp precision (unused; conversion is driven by the Arrow vector's unit)
     * @return the timestamp value
     */
    @Override
    public TimestampData getTimestamp(int i, int precision) {
        long raw = vector.get(i);
        switch (timeUnit) {
            case SECOND:
                return TimestampData.fromEpochMillis(raw * 1000L, 0);
            case MILLISECOND:
                return TimestampData.fromEpochMillis(raw, 0);
            case MICROSECOND:
                return TimestampData.fromEpochMillis(Math.floorDiv(raw, 1000), ((int) Math.floorMod(raw, 1000)) * 1000);
            case NANOSECOND:
                return TimestampData.fromEpochMillis(
                        Math.floorDiv(raw, 1_000_000), (int) Math.floorMod(raw, 1_000_000));
            default:
                throw new IllegalStateException("Unsupported Arrow timestamp unit: " + timeUnit);
        }
    }
}
