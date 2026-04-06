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
package org.apache.auron.flink.arrow.writers;

import org.apache.arrow.vector.BigIntVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

/**
 * {@link ArrowFieldWriter} for 64-bit integers ({@link BigIntVector}).
 *
 * <p>Use {@link #forRow(BigIntVector)} when writing from {@link RowData} and {@link
 * #forArray(BigIntVector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class BigIntWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a BigIntWriter that reads from {@link RowData}. */
    public static BigIntWriter<RowData> forRow(BigIntVector bigIntVector) {
        return new BigIntWriterForRow(bigIntVector);
    }

    /** Creates a BigIntWriter that reads from {@link ArrayData}. */
    public static BigIntWriter<ArrayData> forArray(BigIntVector bigIntVector) {
        return new BigIntWriterForArray(bigIntVector);
    }

    private BigIntWriter(BigIntVector bigIntVector) {
        super(bigIntVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract long readLong(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        BigIntVector vector = (BigIntVector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            vector.setSafe(getCount(), readLong(in, ordinal));
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class BigIntWriterForRow extends BigIntWriter<RowData> {
        private BigIntWriterForRow(BigIntVector bigIntVector) {
            super(bigIntVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        long readLong(RowData in, int ordinal) {
            return in.getLong(ordinal);
        }
    }

    public static final class BigIntWriterForArray extends BigIntWriter<ArrayData> {
        private BigIntWriterForArray(BigIntVector bigIntVector) {
            super(bigIntVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        long readLong(ArrayData in, int ordinal) {
            return in.getLong(ordinal);
        }
    }
}
