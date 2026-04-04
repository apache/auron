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

import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.util.Preconditions;

/**
 * {@link ArrowFieldWriter} for timestamps ({@link TimeStampSecVector}, {@link
 * TimeStampMilliVector}, {@link TimeStampMicroVector}, {@link TimeStampNanoVector}).
 *
 * <p>Handles both {@code TimestampType} and {@code LocalZonedTimestampType} — time zone is not
 * handled at the writer layer.
 *
 * <p>Use {@link #forRow(ValueVector, int)} when writing from {@link RowData} and {@link
 * #forArray(ValueVector, int)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class TimestampWriter<T> extends ArrowFieldWriter<T> {

    protected final int precision;

    /** Creates a TimestampWriter that reads from {@link RowData}. */
    public static TimestampWriter<RowData> forRow(ValueVector valueVector, int precision) {
        return new TimestampWriterForRow(valueVector, precision);
    }

    /** Creates a TimestampWriter that reads from {@link ArrayData}. */
    public static TimestampWriter<ArrayData> forArray(ValueVector valueVector, int precision) {
        return new TimestampWriterForArray(valueVector, precision);
    }

    private TimestampWriter(ValueVector valueVector, int precision) {
        super(valueVector);
        Preconditions.checkState(
                valueVector instanceof TimeStampVector
                        && ((ArrowType.Timestamp) valueVector.getField().getType()).getTimezone() == null,
                "Unexpected vector type or timezone for TimestampWriter: %s",
                valueVector.getClass().getSimpleName());
        this.precision = precision;
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract TimestampData readTimestamp(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (isNullAt(in, ordinal)) {
            ((TimeStampVector) getValueVector()).setNull(getCount());
        } else {
            TimestampData ts = readTimestamp(in, ordinal);
            long millis = ts.getMillisecond();
            int nanoOfMilli = ts.getNanoOfMillisecond();
            ValueVector vector = getValueVector();

            if (vector instanceof TimeStampSecVector) {
                ((TimeStampSecVector) vector).setSafe(getCount(), millis / 1000);
            } else if (vector instanceof TimeStampMilliVector) {
                ((TimeStampMilliVector) vector).setSafe(getCount(), millis);
            } else if (vector instanceof TimeStampMicroVector) {
                ((TimeStampMicroVector) vector).setSafe(getCount(), millis * 1000L + nanoOfMilli / 1000);
            } else if (vector instanceof TimeStampNanoVector) {
                ((TimeStampNanoVector) vector).setSafe(getCount(), millis * 1_000_000L + nanoOfMilli);
            }
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class TimestampWriterForRow extends TimestampWriter<RowData> {
        private TimestampWriterForRow(ValueVector valueVector, int precision) {
            super(valueVector, precision);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        TimestampData readTimestamp(RowData in, int ordinal) {
            return in.getTimestamp(ordinal, precision);
        }
    }

    public static final class TimestampWriterForArray extends TimestampWriter<ArrayData> {
        private TimestampWriterForArray(ValueVector valueVector, int precision) {
            super(valueVector, precision);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        TimestampData readTimestamp(ArrayData in, int ordinal) {
            return in.getTimestamp(ordinal, precision);
        }
    }
}
