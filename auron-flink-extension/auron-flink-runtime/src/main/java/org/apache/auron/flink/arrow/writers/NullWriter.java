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

import org.apache.arrow.vector.NullVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;

/**
 * {@link ArrowFieldWriter} for null type ({@link NullVector}).
 *
 * <p>Every value written is null. Use {@link #forRow(NullVector)} when writing from {@link RowData}
 * and {@link #forArray(NullVector)} when writing from {@link ArrayData}.
 *
 * @param <T> the input data type
 */
public class NullWriter<T> extends ArrowFieldWriter<T> {

    /** Creates a NullWriter that reads from {@link RowData}. */
    public static NullWriter<RowData> forRow(NullVector nullVector) {
        return new NullWriter<>(nullVector);
    }

    /** Creates a NullWriter that reads from {@link ArrayData}. */
    public static NullWriter<ArrayData> forArray(NullVector nullVector) {
        return new NullWriter<>(nullVector);
    }

    private NullWriter(NullVector nullVector) {
        super(nullVector);
    }

    @Override
    public void doWrite(T in, int ordinal) {
        // NullVector values are inherently null; no explicit write needed.
    }
}
