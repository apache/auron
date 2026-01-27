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
package org.apache.auron.flink.arrow;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;

/**
 * Base class for Arrow field writers that convert Flink RowData fields to Arrow
 * vectors. Supports reading from both RowData and ArrayData for nested
 * structures.
 */
public abstract class FlinkArrowFieldWriter {

    protected final ValueVector valueVector;
    protected int count = 0;

    protected FlinkArrowFieldWriter(ValueVector valueVector) {
        this.valueVector = valueVector;
    }

    /**
     * Writes a field value from RowData at the specified position.
     *
     * @param row     The RowData containing the field
     * @param ordinal The position of the field in the row
     */
    public void write(RowData row, int ordinal) {
        if (row.isNullAt(ordinal)) {
            setNull();
        } else {
            setValue(row, ordinal);
        }
        count++;
    }

    /**
     * Writes an element value from ArrayData at the specified position.
     * Used for array elements and map keys/values.
     *
     * @param array   The ArrayData containing the element
     * @param ordinal The position of the element in the array
     */
    public void writeFromArray(ArrayData array, int ordinal) {
        if (array.isNullAt(ordinal)) {
            setNull();
        } else {
            setValueFromArray(array, ordinal);
        }
        count++;
    }

    /** Sets a null value at the current position. */
    protected abstract void setNull();

    /**
     * Sets a non-null value from RowData at the current position.
     *
     * @param row     The RowData containing the field
     * @param ordinal The position of the field in the row
     */
    protected abstract void setValue(RowData row, int ordinal);

    /**
     * Sets a non-null value from ArrayData at the current position.
     *
     * @param array   The ArrayData containing the element
     * @param ordinal The position of the element in the array
     */
    protected abstract void setValueFromArray(ArrayData array, int ordinal);

    /** Finalizes the writing process for the current batch. */
    public void finish() {
        valueVector.setValueCount(count);
    }

    /** Resets the writer for a new batch. */
    public void reset() {
        valueVector.reset();
        count = 0;
    }

    public int getCount() {
        return count;
    }

    /** NullWriter for writing null values. */
    public static class NullWriter extends FlinkArrowFieldWriter {
        public NullWriter(NullVector vector) {
            super(vector);
        }

        @Override
        protected void setNull() {}

        @Override
        protected void setValue(RowData row, int ordinal) {}

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {}
    }

    /** BooleanWriter for writing boolean values. */
    public static class BooleanWriter extends FlinkArrowFieldWriter {
        private final BitVector vector;

        public BooleanWriter(BitVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            vector.setSafe(count, row.getBoolean(ordinal) ? 1 : 0);
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            vector.setSafe(count, array.getBoolean(ordinal) ? 1 : 0);
        }
    }

    /** TinyIntWriter for writing byte values. */
    public static class TinyIntWriter extends FlinkArrowFieldWriter {
        private final TinyIntVector vector;

        public TinyIntWriter(TinyIntVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            vector.setSafe(count, row.getByte(ordinal));
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            vector.setSafe(count, array.getByte(ordinal));
        }
    }

    /** SmallIntWriter for writing short values. */
    public static class SmallIntWriter extends FlinkArrowFieldWriter {
        private final SmallIntVector vector;

        public SmallIntWriter(SmallIntVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            vector.setSafe(count, row.getShort(ordinal));
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            vector.setSafe(count, array.getShort(ordinal));
        }
    }

    /** IntWriter for writing integer values. */
    public static class IntWriter extends FlinkArrowFieldWriter {
        private final IntVector vector;

        public IntWriter(IntVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            vector.setSafe(count, row.getInt(ordinal));
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            vector.setSafe(count, array.getInt(ordinal));
        }
    }

    /** BigIntWriter for writing long values. */
    public static class BigIntWriter extends FlinkArrowFieldWriter {
        private final BigIntVector vector;

        public BigIntWriter(BigIntVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            vector.setSafe(count, row.getLong(ordinal));
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            vector.setSafe(count, array.getLong(ordinal));
        }
    }

    /** FloatWriter for writing float values. */
    public static class FloatWriter extends FlinkArrowFieldWriter {
        private final Float4Vector vector;

        public FloatWriter(Float4Vector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            vector.setSafe(count, row.getFloat(ordinal));
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            vector.setSafe(count, array.getFloat(ordinal));
        }
    }

    /** DoubleWriter for writing double values. */
    public static class DoubleWriter extends FlinkArrowFieldWriter {
        private final Float8Vector vector;

        public DoubleWriter(Float8Vector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            vector.setSafe(count, row.getDouble(ordinal));
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            vector.setSafe(count, array.getDouble(ordinal));
        }
    }

    /** StringWriter for writing string values. */
    public static class StringWriter extends FlinkArrowFieldWriter {
        private final VarCharVector vector;

        public StringWriter(VarCharVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            byte[] bytes = row.getString(ordinal).toBytes();
            vector.setSafe(count, bytes);
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            byte[] bytes = array.getString(ordinal).toBytes();
            vector.setSafe(count, bytes);
        }
    }

    /** BinaryWriter for writing binary values. */
    public static class BinaryWriter extends FlinkArrowFieldWriter {
        private final VarBinaryVector vector;

        public BinaryWriter(VarBinaryVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            byte[] bytes = row.getBinary(ordinal);
            vector.setSafe(count, bytes);
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            byte[] bytes = array.getBinary(ordinal);
            vector.setSafe(count, bytes);
        }
    }

    /** DecimalWriter for writing decimal values. */
    public static class DecimalWriter extends FlinkArrowFieldWriter {
        private final DecimalVector vector;
        private final int precision;
        private final int scale;

        public DecimalWriter(DecimalVector vector, int precision, int scale) {
            super(vector);
            this.vector = vector;
            this.precision = precision;
            this.scale = scale;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            DecimalData decimal = row.getDecimal(ordinal, precision, scale);
            vector.setSafe(count, decimal.toBigDecimal());
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            DecimalData decimal = array.getDecimal(ordinal, precision, scale);
            vector.setSafe(count, decimal.toBigDecimal());
        }
    }

    /** DateWriter for writing date values. */
    public static class DateWriter extends FlinkArrowFieldWriter {
        private final DateDayVector vector;

        public DateWriter(DateDayVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            vector.setSafe(count, row.getInt(ordinal));
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            vector.setSafe(count, array.getInt(ordinal));
        }
    }

    /** TimestampWriter for writing timestamp values. */
    public static class TimestampWriter extends FlinkArrowFieldWriter {
        private final TimeStampMicroVector vector;
        private final int precision;

        public TimestampWriter(TimeStampMicroVector vector, int precision) {
            super(vector);
            this.vector = vector;
            this.precision = precision;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            TimestampData timestamp = row.getTimestamp(ordinal, precision);
            // Convert to microseconds: milliseconds * 1000 + nanoseconds / 1000
            long micros = timestamp.getMillisecond() * 1000L + timestamp.getNanoOfMillisecond() / 1000;
            vector.setSafe(count, micros);
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            TimestampData timestamp = array.getTimestamp(ordinal, precision);
            long micros = timestamp.getMillisecond() * 1000L + timestamp.getNanoOfMillisecond() / 1000;
            vector.setSafe(count, micros);
        }
    }

    /** TimeWriter for writing time values. */
    public static class TimeWriter extends FlinkArrowFieldWriter {
        private final TimeMicroVector vector;

        public TimeWriter(TimeMicroVector vector) {
            super(vector);
            this.vector = vector;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            // Flink TimeType stores time as milliseconds (int), convert to microseconds
            int millis = row.getInt(ordinal);
            vector.setSafe(count, millis * 1000L);
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            int millis = array.getInt(ordinal);
            vector.setSafe(count, millis * 1000L);
        }
    }

    /** LocalZonedTimestampWriter for writing local-zoned timestamp values with UTC timezone. */
    public static class LocalZonedTimestampWriter extends FlinkArrowFieldWriter {
        private final TimeStampMicroTZVector vector;
        private final int precision;

        public LocalZonedTimestampWriter(TimeStampMicroTZVector vector, int precision) {
            super(vector);
            this.vector = vector;
            this.precision = precision;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            TimestampData timestamp = row.getTimestamp(ordinal, precision);
            // Convert to microseconds: milliseconds * 1000 + nanoseconds / 1000
            long micros = timestamp.getMillisecond() * 1000L + timestamp.getNanoOfMillisecond() / 1000;
            vector.setSafe(count, micros);
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            TimestampData timestamp = array.getTimestamp(ordinal, precision);
            long micros = timestamp.getMillisecond() * 1000L + timestamp.getNanoOfMillisecond() / 1000;
            vector.setSafe(count, micros);
        }
    }

    /** ArrayWriter for writing array values using recursive field writers. */
    public static class ArrayWriter extends FlinkArrowFieldWriter {
        private final ListVector vector;
        private final FlinkArrowFieldWriter elementWriter;

        public ArrayWriter(ListVector vector, FlinkArrowFieldWriter elementWriter) {
            super(vector);
            this.vector = vector;
            this.elementWriter = elementWriter;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            ArrayData array = row.getArray(ordinal);
            vector.startNewValue(count);
            for (int i = 0; i < array.size(); i++) {
                elementWriter.writeFromArray(array, i);
            }
            vector.endValue(count, array.size());
        }

        @Override
        protected void setValueFromArray(ArrayData arrayData, int ordinal) {
            ArrayData array = arrayData.getArray(ordinal);
            vector.startNewValue(count);
            for (int i = 0; i < array.size(); i++) {
                elementWriter.writeFromArray(array, i);
            }
            vector.endValue(count, array.size());
        }

        @Override
        public void finish() {
            super.finish();
            elementWriter.finish();
        }

        @Override
        public void reset() {
            super.reset();
            elementWriter.reset();
        }
    }

    /** MapWriter for writing map values using recursive field writers. */
    public static class MapWriter extends FlinkArrowFieldWriter {
        private final MapVector vector;
        private final StructVector structVector;
        private final FlinkArrowFieldWriter keyWriter;
        private final FlinkArrowFieldWriter valueWriter;

        public MapWriter(
                MapVector vector,
                StructVector structVector,
                FlinkArrowFieldWriter keyWriter,
                FlinkArrowFieldWriter valueWriter) {
            super(vector);
            this.vector = vector;
            this.structVector = structVector;
            this.keyWriter = keyWriter;
            this.valueWriter = valueWriter;
        }

        @Override
        protected void setNull() {
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            MapData map = row.getMap(ordinal);
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();

            vector.startNewValue(count);
            for (int i = 0; i < map.size(); i++) {
                structVector.setIndexDefined(keyWriter.getCount());
                keyWriter.writeFromArray(keys, i);
                valueWriter.writeFromArray(values, i);
            }
            vector.endValue(count, map.size());
        }

        @Override
        protected void setValueFromArray(ArrayData arrayData, int ordinal) {
            MapData map = arrayData.getMap(ordinal);
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();

            vector.startNewValue(count);
            for (int i = 0; i < map.size(); i++) {
                structVector.setIndexDefined(keyWriter.getCount());
                keyWriter.writeFromArray(keys, i);
                valueWriter.writeFromArray(values, i);
            }
            vector.endValue(count, map.size());
        }

        @Override
        public void finish() {
            super.finish();
            keyWriter.finish();
            valueWriter.finish();
        }

        @Override
        public void reset() {
            super.reset();
            keyWriter.reset();
            valueWriter.reset();
        }
    }

    /** RowWriter for writing row/struct values using recursive field writers. */
    public static class RowWriter extends FlinkArrowFieldWriter {
        private final StructVector vector;
        private final FlinkArrowFieldWriter[] fieldWriters;

        public RowWriter(StructVector vector, FlinkArrowFieldWriter[] fieldWriters) {
            super(vector);
            this.vector = vector;
            this.fieldWriters = fieldWriters;
        }

        @Override
        protected void setNull() {
            for (FlinkArrowFieldWriter writer : fieldWriters) {
                writer.setNull();
                writer.count++;
            }
            vector.setNull(count);
        }

        @Override
        protected void setValue(RowData row, int ordinal) {
            RowData nestedRow = row.getRow(ordinal, fieldWriters.length);
            vector.setIndexDefined(count);
            for (int i = 0; i < fieldWriters.length; i++) {
                fieldWriters[i].write(nestedRow, i);
            }
        }

        @Override
        protected void setValueFromArray(ArrayData array, int ordinal) {
            RowData nestedRow = array.getRow(ordinal, fieldWriters.length);
            vector.setIndexDefined(count);
            for (int i = 0; i < fieldWriters.length; i++) {
                fieldWriters[i].write(nestedRow, i);
            }
        }

        @Override
        public void finish() {
            super.finish();
            for (FlinkArrowFieldWriter writer : fieldWriters) {
                writer.finish();
            }
        }

        @Override
        public void reset() {
            super.reset();
            for (FlinkArrowFieldWriter writer : fieldWriters) {
                writer.reset();
            }
        }
    }
}
