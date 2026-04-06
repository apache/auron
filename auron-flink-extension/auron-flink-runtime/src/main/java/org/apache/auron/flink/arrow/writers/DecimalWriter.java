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

import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.arrow.vector.DecimalVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;

/**
 * {@link ArrowFieldWriter} for decimals ({@link DecimalVector}).
 *
 * <p>Use {@link #forRow(DecimalVector, int, int)} when writing from {@link RowData} and {@link
 * #forArray(DecimalVector, int, int)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class DecimalWriter<T> extends ArrowFieldWriter<T> {

    protected final int precision;
    protected final int scale;

    /** Creates a DecimalWriter that reads from {@link RowData}. */
    public static DecimalWriter<RowData> forRow(DecimalVector decimalVector, int precision, int scale) {
        return new DecimalWriterForRow(decimalVector, precision, scale);
    }

    /** Creates a DecimalWriter that reads from {@link ArrayData}. */
    public static DecimalWriter<ArrayData> forArray(DecimalVector decimalVector, int precision, int scale) {
        return new DecimalWriterForArray(decimalVector, precision, scale);
    }

    private DecimalWriter(DecimalVector decimalVector, int precision, int scale) {
        super(decimalVector);
        this.precision = precision;
        this.scale = scale;
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract DecimalData readDecimal(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        DecimalVector vector = (DecimalVector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            BigDecimal bigDecimal = readDecimal(in, ordinal).toBigDecimal();
            bigDecimal = fitBigDecimal(bigDecimal, precision, scale);
            if (bigDecimal == null) {
                vector.setNull(getCount());
            } else {
                vector.setSafe(getCount(), bigDecimal);
            }
        }
    }

    private static BigDecimal fitBigDecimal(BigDecimal value, int precision, int scale) {
        value = value.setScale(scale, RoundingMode.HALF_UP);
        if (value.precision() > precision) {
            return null;
        }
        return value;
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class DecimalWriterForRow extends DecimalWriter<RowData> {
        private DecimalWriterForRow(DecimalVector decimalVector, int precision, int scale) {
            super(decimalVector, precision, scale);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        DecimalData readDecimal(RowData in, int ordinal) {
            return in.getDecimal(ordinal, precision, scale);
        }
    }

    public static final class DecimalWriterForArray extends DecimalWriter<ArrayData> {
        private DecimalWriterForArray(DecimalVector decimalVector, int precision, int scale) {
            super(decimalVector, precision, scale);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        DecimalData readDecimal(ArrayData in, int ordinal) {
            return in.getDecimal(ordinal, precision, scale);
        }
    }
}
