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

import java.util.Objects;
import org.apache.arrow.vector.ValueVector;

/**
 * Base class for writing Flink data into Arrow vectors.
 *
 * <p>Uses a template method pattern: {@link #write(Object, int)} increments the row count and
 * delegates to {@link #doWrite(Object, int)} for type-specific logic.
 *
 * @param <IN> the input data type (e.g., RowData or ArrayData)
 */
public abstract class ArrowFieldWriter<IN> {

    private final ValueVector valueVector;
    private int count;

    protected ArrowFieldWriter(ValueVector valueVector) {
        this.valueVector = Objects.requireNonNull(valueVector, "valueVector must not be null");
    }

    /** Returns the underlying Arrow vector. */
    public ValueVector getValueVector() {
        return valueVector;
    }

    /** Returns the number of values written since the last reset. */
    public int getCount() {
        return count;
    }

    /**
     * Writes a value from the input at the given ordinal position.
     *
     * @param row the input data
     * @param ordinal the field index within the input
     */
    public void write(IN row, int ordinal) {
        doWrite(row, ordinal);
        count++;
    }

    /**
     * Type-specific write logic. Implementations read from the input and write into the Arrow
     * vector at position {@link #getCount()}.
     *
     * @param row the input data
     * @param ordinal the field index within the input
     */
    public abstract void doWrite(IN row, int ordinal);

    /** Finalizes the current batch by setting the value count on the vector. */
    public void finish() {
        valueVector.setValueCount(count);
    }

    /** Resets the vector and count for a new batch. */
    public void reset() {
        valueVector.reset();
        count = 0;
    }
}
