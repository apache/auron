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

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.arrowio.AuronArrowFFIExporter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Synchronous {@link AuronArrowFFIExporter} that buffers Flink {@link RowData} into an Arrow
 * {@link VectorSchemaRoot} and exports it across the Arrow C-Data FFI boundary on demand.
 *
 * <p>Unlike the Spark counterpart, this exporter is deliberately <strong>single-threaded</strong>.
 * The Flink Calc operator pushes rows synchronously through {@link #offer(RowData)} from
 * {@code processElement}; the native engine pulls batches through {@link #exportNextBatch(long)} on
 * its tokio blocking pool. The two never run concurrently for the same exporter instance, so no
 * locking or background thread is required.
 *
 * <p>Lifecycle:
 *
 * <ul>
 *   <li>Construction allocates an initial {@link VectorSchemaRoot} and a bound
 *       {@link FlinkArrowWriter}.
 *   <li>{@link #offer(RowData)} appends one row to the current batch.
 *   <li>{@link #exportNextBatch(long)} finalizes the current root, exports it via Arrow C-Data,
 *       then rotates to a fresh root for the next batch (the previous root is closed before the
 *       new one is allocated). Returns {@code false} whenever the buffered row count is zero,
 *       signaling end-of-current-cycle to the native FFI Reader's read loop.
 *   <li>{@link #close()} releases the current root back to the supplied allocator. Also invoked
 *       by the native FFI Reader's {@code AutoCloseableExporter} drop at the end of each drain
 *       cycle; the owning operator subsequently calls {@link #reset()} to re-prepare the
 *       exporter for the next cycle.
 * </ul>
 *
 * <p>Row count is tracked on the exporter itself rather than queried from {@link FlinkArrowWriter}
 * because the writer does not expose its internal count before {@link FlinkArrowWriter#finish()};
 * tracking here is exact (incremented per {@code offer}, reset per rotation).
 */
public class FlinkArrowFFIExporter extends AuronArrowFFIExporter {

    private final BufferAllocator allocator;
    private final RowType inputRowType;
    private final Schema arrowSchema;
    private final int batchRowsLimit;

    private VectorSchemaRoot currentRoot;
    private FlinkArrowWriter writer;
    private int rowCount;

    /**
     * Creates a new exporter bound to the supplied allocator and Flink row type.
     *
     * @param allocator the Arrow allocator used for the buffered {@link VectorSchemaRoot} and for
     *     any FFI exports; the caller retains ownership and must outlive this exporter
     * @param inputRowType the Flink row type whose schema this exporter exposes to native code
     * @param batchRowsLimit the soft row count after which {@link #isBatchFull()} returns
     *     {@code true}; the operator decides whether to flush early on this signal
     */
    public FlinkArrowFFIExporter(BufferAllocator allocator, RowType inputRowType, int batchRowsLimit) {
        this.allocator = allocator;
        this.inputRowType = inputRowType;
        this.arrowSchema = FlinkArrowUtils.toArrowSchema(inputRowType);
        this.batchRowsLimit = batchRowsLimit;
        rotateRoot();
    }

    /**
     * Appends a single row to the current batch buffer.
     *
     * @param row the row to write; must conform to the {@link RowType} passed to the constructor
     */
    public void offer(RowData row) {
        writer.write(row);
        rowCount++;
    }

    /**
     * Returns {@code true} once the current batch has reached the configured row limit. The caller
     * (typically the operator) uses this as a hint to flush eagerly before the buffer grows
     * further; it is not an upper bound enforced inside the exporter.
     *
     * @return true if the buffered row count has reached the limit
     */
    public boolean isBatchFull() {
        return rowCount >= batchRowsLimit;
    }

    /**
     * Returns {@code true} when this exporter has no buffered rows. Used by the owning
     * operator to short-circuit no-op drains, which would otherwise tear down and recreate
     * the native wrapper for no useful work.
     *
     * @return true if the buffered row count is zero
     */
    public boolean isEmpty() {
        return rowCount == 0;
    }

    /**
     * Exports the Arrow schema describing this exporter's output into the FFI struct addressed by
     * {@code arrowSchemaPtr}. The native side typically calls this once during operator setup.
     *
     * <p>This method is intentionally not {@code @Override}; the base
     * {@link AuronArrowFFIExporter} only declares {@link #exportNextBatch(long)} as abstract, and
     * schema export is invoked directly by the JVM-side operator during {@code open()}.
     *
     * @param arrowSchemaPtr native address of an Arrow {@link ArrowSchema} FFI struct
     */
    public void exportSchema(long arrowSchemaPtr) {
        try (ArrowSchema ffi = ArrowSchema.wrap(arrowSchemaPtr)) {
            Data.exportSchema(allocator, arrowSchema, null, ffi);
        }
    }

    /**
     * Exports the current batch into the FFI struct addressed by {@code arrowArrayPtr} and rotates
     * to a fresh batch. Returns {@code false} whenever the buffered row count is zero, signaling
     * end-of-current-cycle to the native FFI Reader's read loop; the reader exits and its drop
     * invokes {@link #close()} on this exporter, and the owning operator calls {@link #reset()}
     * to re-prepare before the next drain cycle.
     *
     * <p>Without the empty-buffer false return, mid-stream empty pulls (no rows yet but more
     * expected) would cause the native FFI Reader to send 0-row batches downstream and
     * {@link org.apache.auron.jni.AuronCallNativeWrapper#loadNextBatch} would spin indefinitely
     * on empty output.
     *
     * @param arrowArrayPtr native address of an Arrow {@link ArrowArray} FFI struct
     * @return {@code true} if a batch was written into {@code arrowArrayPtr}; {@code false} if the
     *     buffer is empty (end-of-current-cycle)
     */
    @Override
    public boolean exportNextBatch(long arrowArrayPtr) {
        if (rowCount == 0) {
            return false;
        }
        writer.finish();
        try (ArrowArray ffi = ArrowArray.wrap(arrowArrayPtr)) {
            Data.exportVectorSchemaRoot(allocator, currentRoot, null, ffi);
        }
        rotateRoot();
        return true;
    }

    /**
     * Releases the current {@link VectorSchemaRoot} back to the supplied allocator. Safe to call
     * multiple times.
     */
    @Override
    public void close() {
        if (currentRoot != null) {
            currentRoot.close();
            currentRoot = null;
        }
        writer = null;
        rowCount = 0;
    }

    /**
     * Re-prepares this exporter for the next drain cycle after {@link #close()} ran.
     * Re-allocates {@link VectorSchemaRoot} + binds a fresh {@link FlinkArrowWriter} only when
     * {@code currentRoot == null}; no-op when the root is still open. Zeroes {@code rowCount}.
     *
     * <p>Public because the owning operator lives in a different package; not part of the
     * abstract {@link AuronArrowFFIExporter} contract.
     */
    public void reset() {
        if (currentRoot == null) {
            this.currentRoot = VectorSchemaRoot.create(arrowSchema, allocator);
            this.writer = FlinkArrowWriter.create(currentRoot, inputRowType);
        }
        this.rowCount = 0;
    }

    /**
     * Closes any existing root, allocates a fresh {@link VectorSchemaRoot} for the next batch, and
     * binds a new {@link FlinkArrowWriter} to it. The row counter is reset to zero.
     */
    private void rotateRoot() {
        if (currentRoot != null) {
            currentRoot.close();
        }
        this.currentRoot = VectorSchemaRoot.create(arrowSchema, allocator);
        this.writer = FlinkArrowWriter.create(currentRoot, inputRowType);
        this.rowCount = 0;
    }
}
