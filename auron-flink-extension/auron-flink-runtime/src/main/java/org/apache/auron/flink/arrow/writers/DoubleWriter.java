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

import org.apache.arrow.vector.Float8Vector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

/**
 * {@link ArrowFieldWriter} for 64-bit doubles ({@link Float8Vector}).
 *
 * <p>Use {@link #forRow(Float8Vector)} when writing from {@link RowData} and {@link
 * #forArray(Float8Vector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class DoubleWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a DoubleWriter that reads from {@link RowData}. */
    public static DoubleWriter<RowData> forRow(Float8Vector float8Vector) {
        return new DoubleWriterForRow(float8Vector);
    }

    /** Creates a DoubleWriter that reads from {@link ArrayData}. */
    public static DoubleWriter<ArrayData> forArray(Float8Vector float8Vector) {
        return new DoubleWriterForArray(float8Vector);
    }

    private DoubleWriter(Float8Vector float8Vector) {
        super(float8Vector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract double readDouble(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        Float8Vector vector = (Float8Vector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            vector.setSafe(getCount(), readDouble(in, ordinal));
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class DoubleWriterForRow extends DoubleWriter<RowData> {
        private DoubleWriterForRow(Float8Vector float8Vector) {
            super(float8Vector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        double readDouble(RowData in, int ordinal) {
            return in.getDouble(ordinal);
        }
    }

    public static final class DoubleWriterForArray extends DoubleWriter<ArrayData> {
        private DoubleWriterForArray(Float8Vector float8Vector) {
            super(float8Vector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        double readDouble(ArrayData in, int ordinal) {
            return in.getDouble(ordinal);
        }
    }
}
