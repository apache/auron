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

import org.apache.arrow.vector.Float4Vector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

/**
 * {@link ArrowFieldWriter} for 32-bit floats ({@link Float4Vector}).
 *
 * <p>Use {@link #forRow(Float4Vector)} when writing from {@link RowData} and {@link
 * #forArray(Float4Vector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class FloatWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a FloatWriter that reads from {@link RowData}. */
    public static FloatWriter<RowData> forRow(Float4Vector float4Vector) {
        return new FloatWriterForRow(float4Vector);
    }

    /** Creates a FloatWriter that reads from {@link ArrayData}. */
    public static FloatWriter<ArrayData> forArray(Float4Vector float4Vector) {
        return new FloatWriterForArray(float4Vector);
    }

    private FloatWriter(Float4Vector float4Vector) {
        super(float4Vector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract float readFloat(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        Float4Vector vector = (Float4Vector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            vector.setSafe(getCount(), readFloat(in, ordinal));
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class FloatWriterForRow extends FloatWriter<RowData> {
        private FloatWriterForRow(Float4Vector float4Vector) {
            super(float4Vector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        float readFloat(RowData in, int ordinal) {
            return in.getFloat(ordinal);
        }
    }

    public static final class FloatWriterForArray extends FloatWriter<ArrayData> {
        private FloatWriterForArray(Float4Vector float4Vector) {
            super(float4Vector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        float readFloat(ArrayData in, int ordinal) {
            return in.getFloat(ordinal);
        }
    }
}
