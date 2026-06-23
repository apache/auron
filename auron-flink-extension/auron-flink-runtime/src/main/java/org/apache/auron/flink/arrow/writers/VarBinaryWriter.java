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

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

/**
 * {@link ArrowFieldWriter} for variable-length binary data ({@link VarBinaryVector}).
 *
 * <p>Use {@link #forRow(VarBinaryVector)} when writing from {@link RowData} and {@link
 * #forArray(VarBinaryVector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public abstract class VarBinaryWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a VarBinaryWriter that reads from {@link RowData}. */
    public static VarBinaryWriter<RowData> forRow(VarBinaryVector varBinaryVector) {
        return new VarBinaryWriterForRow(varBinaryVector);
    }

    /** Creates a VarBinaryWriter that reads from {@link ArrayData}. */
    public static VarBinaryWriter<ArrayData> forArray(VarBinaryVector varBinaryVector) {
        return new VarBinaryWriterForArray(varBinaryVector);
    }

    private VarBinaryWriter(VarBinaryVector varBinaryVector) {
        super(varBinaryVector);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract byte[] readBinary(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        VarBinaryVector vector = (VarBinaryVector) getValueVector();
        if (isNullAt(in, ordinal)) {
            vector.setNull(getCount());
        } else {
            byte[] bytes = readBinary(in, ordinal);
            vector.setSafe(getCount(), bytes);
        }
    }

    // ---- Inner classes for RowData and ArrayData ----

    public static final class VarBinaryWriterForRow extends VarBinaryWriter<RowData> {
        private VarBinaryWriterForRow(VarBinaryVector varBinaryVector) {
            super(varBinaryVector);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        byte[] readBinary(RowData in, int ordinal) {
            return in.getBinary(ordinal);
        }
    }

    public static final class VarBinaryWriterForArray extends VarBinaryWriter<ArrayData> {
        private VarBinaryWriterForArray(VarBinaryVector varBinaryVector) {
            super(varBinaryVector);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        byte[] readBinary(ArrayData in, int ordinal) {
            return in.getBinary(ordinal);
        }
    }
}
