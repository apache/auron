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

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

/**
 * {@link ArrowFieldWriter} for time values stored in Arrow time vectors.
 *
 * <p>Supports all four Arrow time precisions: {@link TimeSecVector}, {@link TimeMilliVector}, {@link
 * TimeMicroVector}, and {@link TimeNanoVector}. Flink internally stores TIME values as milliseconds
 * in an {@code int}; this writer converts to the target precision on write.
 *
 * <p>Use {@link #forRow(ValueVector)} when writing from {@link RowData} and {@link
 * #forArray(ValueVector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class TimeWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a TimeWriter that reads from {@link RowData}. */
    public static TimeWriter<RowData> forRow(ValueVector valueVector) {
        return new TimeWriterForRow(valueVector);
    }

    /** Creates a TimeWriter that reads from {@link ArrayData}. */
    public static TimeWriter<ArrayData> forArray(ValueVector valueVector) {
        return new TimeWriterForArray(valueVector);
    }

    private TimeWriter(ValueVector valueVector) {
        super(valueVector);
        Preconditions.checkState(
                valueVector instanceof TimeSecVector
                        || valueVector instanceof TimeMilliVector
                        || valueVector instanceof TimeMicroVector
                        || valueVector instanceof TimeNanoVector,
                "Expected a time vector (TimeSecVector/TimeMilliVector/TimeMicroVector/TimeNanoVector), got: %s",
                valueVector.getClass().getSimpleName());
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract int readTime(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        ValueVector vector = getValueVector();
        if (isNullAt(in, ordinal)) {
            ((BaseFixedWidthVector) vector).setNull(getCount());
        } else {
            int millis = readTime(in, ordinal);
            if (vector instanceof TimeSecVector) {
                ((TimeSecVector) vector).setSafe(getCount(), millis / 1000);
            } else if (vector instanceof TimeMilliVector) {
                ((TimeMilliVector) vector).setSafe(getCount(), millis);
            } else if (vector instanceof TimeMicroVector) {
                ((TimeMicroVector) vector).setSafe(getCount(), millis * 1000L);
            } else if (vector instanceof TimeNanoVector) {
                ((TimeNanoVector) vector).setSafe(getCount(), millis * 1_000_000L);
            }
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class TimeWriterForRow extends TimeWriter<RowData> {
        private TimeWriterForRow(ValueVector valueVector) {
            super(valueVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readTime(RowData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }

    public static final class TimeWriterForArray extends TimeWriter<ArrayData> {
        private TimeWriterForArray(ValueVector valueVector) {
            super(valueVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        int readTime(ArrayData in, int ordinal) {
            return in.getInt(ordinal);
        }
    }
}
