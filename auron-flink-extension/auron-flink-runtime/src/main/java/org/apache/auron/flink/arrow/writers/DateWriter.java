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

import org.apache.arrow.vector.DateDayVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

/**
 * {@link ArrowFieldWriter} for dates stored as days since epoch ({@link DateDayVector}).
 *
 * <p>Use {@link #forRow(DateDayVector)} when writing from {@link RowData} and {@link
 * #forArray(DateDayVector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class DateWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a DateWriter that reads from {@link RowData}. */
    public static DateWriter<RowData> forRow(DateDayVector dateDayVector) {
        return new DateWriterForRow(dateDayVector);
    }

    /** Creates a DateWriter that reads from {@link ArrayData}. */
    public static DateWriter<ArrayData> forArray(DateDayVector dateDayVector) {
        return new DateWriterForArray(dateDayVector);
    }

    private DateWriter(DateDayVector dateDayVector) {
        super(dateDayVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract int readDate(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        DateDayVector vector = (DateDayVector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            vector.setSafe(getCount(), readDate(in, ordinal));
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class DateWriterForRow extends DateWriter<RowData> {
        private DateWriterForRow(DateDayVector dateDayVector) {
            super(dateDayVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readDate(RowData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }

    public static final class DateWriterForArray extends DateWriter<ArrayData> {
        private DateWriterForArray(DateDayVector dateDayVector) {
            super(dateDayVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readDate(ArrayData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }
}
