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
package org.apache.auron.flink.arrow.vectors;

import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.util.Preconditions;

/**
 * A Flink {@link BytesColumnVector} backed by an Arrow {@link VarCharVector}.
 *
 * <p>This wrapper delegates all reads to the underlying Arrow vector, providing zero-copy access
 * to Arrow data from Flink's columnar batch execution engine.
 */
public final class ArrowVarCharColumnVector implements BytesColumnVector {

    private final VarCharVector vector;

    /**
     * Creates a new wrapper around the given Arrow {@link VarCharVector}.
     *
     * @param vector the Arrow vector to wrap, must not be null
     */
    public ArrowVarCharColumnVector(VarCharVector vector) {
        this.vector = Preconditions.checkNotNull(vector);
    }

    /** {@inheritDoc} */
    @Override
    public boolean isNullAt(int i) {
        return vector.isNull(i);
    }

    /** {@inheritDoc} */
    @Override
    public Bytes getBytes(int i) {
        byte[] bytes = vector.get(i);
        return new Bytes(bytes, 0, bytes.length);
    }
}
