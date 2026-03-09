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

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.DecimalVector;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.columnar.vector.DecimalColumnVector;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link DecimalColumnVector} backed by an Arrow {@link DecimalVector}.
 *
 * <p>This wrapper delegates all reads to the underlying Arrow vector, providing zero-copy access
 * to Arrow data from Flink's columnar batch execution engine.
 *
 * <p>For compact decimals (precision &lt;= 18), the implementation avoids BigDecimal allocation by
 * reading the unscaled value directly from Arrow's little-endian byte representation.
 */
public final class ArrowDecimalColumnVector implements DecimalColumnVector {

    /**
     * Maximum precision that fits in a long (same as Flink's internal compact decimal threshold).
     * DecimalData.MAX_COMPACT_PRECISION is package-private, so we define our own constant.
     */
    private static final int MAX_COMPACT_PRECISION = 18;

    private final DecimalVector vector;

    /**
     * Creates a new wrapper around the given Arrow {@link DecimalVector}.
     *
     * @param vector the Arrow vector to wrap, must not be null
     */
    public ArrowDecimalColumnVector(DecimalVector vector) {
        this.vector = Preconditions.checkNotNull(vector);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /** {@inheritDoc} */
    @Override
    public DecimalData getDecimal(int i, int precision, int scale) {
        if (precision <= MAX_COMPACT_PRECISION) {
            // Compact path: avoid BigDecimal allocation for precision <= 18.
            // Arrow stores decimals as 128-bit little-endian two's complement.
            // For precision <= 18, the value fits in a long.
            long unscaledLong = readLittleEndianLong(vector.getDataBuffer(), (long) i * 16);
            return DecimalData.fromUnscaledLong(unscaledLong, precision, scale);
        }
        return DecimalData.fromBigDecimal(vector.getObject(i), precision, scale);
    }

    /**
     * Reads a little-endian long (8 bytes) from an Arrow buffer at the given byte offset. Arrow
     * stores decimals as 128-bit little-endian two's complement. For values that fit in a long
     * (precision &lt;= 18), the lower 8 bytes are sufficient.
     *
     * @param buffer the Arrow data buffer
     * @param offset byte offset into the buffer
     * @return the unscaled value as a long
     */
    private static long readLittleEndianLong(ArrowBuf buffer, long offset) {
        return buffer.getLong(offset);
    }
}
