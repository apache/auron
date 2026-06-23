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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.auron.flink.arrow.writers.ArrowFieldWriter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Orchestrates per-column {@link ArrowFieldWriter}s to write {@link RowData} into an Arrow {@link
 * VectorSchemaRoot}.
 *
 * <p>Use {@link #create(VectorSchemaRoot, RowType)} to construct, then call {@link #write(RowData)}
 * for each row, {@link #finish()} to finalize the batch, and {@link #reset()} before starting a new
 * batch.
 */
public final class FlinkArrowWriter {

    private final VectorSchemaRoot root;
    private final ArrowFieldWriter<RowData>[] fieldWriters;

    @SuppressWarnings("unchecked")
    private FlinkArrowWriter(VectorSchemaRoot root, ArrowFieldWriter<RowData>[] fieldWriters) {
        this.root = root;
        this.fieldWriters = fieldWriters;
    }

    /**
     * Creates a writer from an existing {@link VectorSchemaRoot} and Flink {@link RowType}.
     *
     * <p>Each vector in the root is allocated and a matching {@link ArrowFieldWriter} is created via
     * {@link FlinkArrowUtils#createArrowFieldWriterForRow}.
     *
     * @param root the Arrow vector schema root to write into
     * @param rowType the Flink row type describing the schema
     * @return a new writer instance
     */
    @SuppressWarnings("unchecked")
    public static FlinkArrowWriter create(VectorSchemaRoot root, RowType rowType) {
        ArrowFieldWriter<RowData>[] fieldWriters =
                new ArrowFieldWriter[root.getFieldVectors().size()];
        for (int i = 0; i < fieldWriters.length; i++) {
            FieldVector vector = root.getFieldVectors().get(i);
            vector.allocateNew();
            fieldWriters[i] = FlinkArrowUtils.createArrowFieldWriterForRow(vector, rowType.getTypeAt(i));
        }
        return new FlinkArrowWriter(root, fieldWriters);
    }

    /**
     * Writes a single {@link RowData} into the underlying Arrow vectors.
     *
     * @param row the row to write
     */
    public void write(RowData row) {
        for (int i = 0; i < fieldWriters.length; i++) {
            fieldWriters[i].write(row, i);
        }
    }

    /** Finalizes the current batch by setting the row count and finishing each field writer. */
    public void finish() {
        root.setRowCount(fieldWriters.length > 0 ? fieldWriters[0].getCount() : 0);
        for (ArrowFieldWriter<RowData> w : fieldWriters) {
            w.finish();
        }
    }

    /** Resets all vectors and field writers for a new batch. */
    public void reset() {
        root.setRowCount(0);
        for (ArrowFieldWriter<RowData> w : fieldWriters) {
            w.reset();
        }
    }

    /** Returns the underlying {@link VectorSchemaRoot}. */
    public VectorSchemaRoot getRoot() {
        return root;
    }
}
