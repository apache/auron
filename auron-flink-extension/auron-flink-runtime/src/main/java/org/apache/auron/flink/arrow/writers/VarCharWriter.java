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

import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

/**
 * {@link ArrowFieldWriter} for variable-length strings ({@link VarCharVector}).
 *
 * <p>Use {@link #forRow(VarCharVector)} when writing from {@link RowData} and {@link
 * #forArray(VarCharVector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class VarCharWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a VarCharWriter that reads from {@link RowData}. */
    public static VarCharWriter<RowData> forRow(VarCharVector varCharVector) {
        return new VarCharWriterForRow(varCharVector);
    }

    /** Creates a VarCharWriter that reads from {@link ArrayData}. */
    public static VarCharWriter<ArrayData> forArray(VarCharVector varCharVector) {
        return new VarCharWriterForArray(varCharVector);
    }

    private VarCharWriter(VarCharVector varCharVector) {
        super(varCharVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract StringData readString(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        VarCharVector vector = (VarCharVector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            byte[] bytes = readString(in, ordinal).toBytes();
            vector.setSafe(getCount(), bytes);
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class VarCharWriterForRow extends VarCharWriter<RowData> {
        private VarCharWriterForRow(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        StringData readString(RowData in, int ordinal) {
            return in.getString(ordinal);
        }
    }

    public static final class VarCharWriterForArray extends VarCharWriter<ArrayData> {
        private VarCharWriterForArray(VarCharVector varCharVector) {
            super(varCharVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        StringData readString(ArrayData in, int ordinal) {
            return in.getString(ordinal);
        }
    }
}
