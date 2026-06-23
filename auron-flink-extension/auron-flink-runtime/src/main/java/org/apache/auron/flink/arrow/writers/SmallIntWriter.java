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

import org.apache.arrow.vector.SmallIntVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

/**
 * {@link ArrowFieldWriter} for 16-bit integers ({@link SmallIntVector}).
 *
 * <p>Use {@link #forRow(SmallIntVector)} when writing from {@link RowData} and {@link
 * #forArray(SmallIntVector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class SmallIntWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a SmallIntWriter that reads from {@link RowData}. */
    public static SmallIntWriter<RowData> forRow(SmallIntVector smallIntVector) {
        return new SmallIntWriterForRow(smallIntVector);
    }

    /** Creates a SmallIntWriter that reads from {@link ArrayData}. */
    public static SmallIntWriter<ArrayData> forArray(SmallIntVector smallIntVector) {
        return new SmallIntWriterForArray(smallIntVector);
    }

    private SmallIntWriter(SmallIntVector smallIntVector) {
        super(smallIntVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract short readShort(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        SmallIntVector vector = (SmallIntVector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            vector.setSafe(getCount(), readShort(in, ordinal));
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class SmallIntWriterForRow extends SmallIntWriter<RowData> {
        private SmallIntWriterForRow(SmallIntVector smallIntVector) {
            super(smallIntVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        short readShort(RowData in, int ordinal) {
            return in.getShort(ordinal);
        }
    }

    public static final class SmallIntWriterForArray extends SmallIntWriter<ArrayData> {
        private SmallIntWriterForArray(SmallIntVector smallIntVector) {
            super(smallIntVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        short readShort(ArrayData in, int ordinal) {
            return in.getShort(ordinal);
        }
    }
}
