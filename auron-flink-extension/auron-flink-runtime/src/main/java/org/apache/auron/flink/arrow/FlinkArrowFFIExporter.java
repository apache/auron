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

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.arrowio.AuronArrowFFIExporter;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

/**
 * Exports Flink RowData to Arrow format via FFI (Foreign Function Interface)
 * for consumption by native code.
 *
 * <p>This exporter uses an asynchronous producer-consumer model with double
 * queues to ensure safe resource management. The producer thread creates
 * batches ahead of time while the consumer (native side) processes them.
 * Resources are only cleaned up after the consumer confirms it has finished
 * using the previous batch.
 *
 * <p>Key design points:
 * <ul>
 *   <li>Producer thread pre-creates batches and puts them in outputQueue</li>
 *   <li>Consumer takes batches and signals via processingQueue when done</li>
 *   <li>Previous batch resources are cleaned up only after consumer confirms</li>
 *   <li>No TaskContext in Flink, so cancellation uses closed flag + thread interrupt</li>
 * </ul>
 */
public class FlinkArrowFFIExporter extends AuronArrowFFIExporter {

    /** Queue state representing a data batch ready for export. */
    private static final class NextBatch {
        final VectorSchemaRoot root;
        final BufferAllocator allocator;

        NextBatch(VectorSchemaRoot root, BufferAllocator allocator) {
            this.root = root;
            this.allocator = allocator;
        }
    }

    /** Queue state representing end of data or an error. */
    private static final class Finished {
        final Throwable error; // null means normal completion

        Finished(Throwable error) {
            this.error = error;
        }
    }

    private final Iterator<RowData> rowIterator;
    private final RowType rowType;
    private final Schema arrowSchema;
    private final DictionaryProvider.MapDictionaryProvider emptyDictionaryProvider;
    private final int maxBatchNumRows;
    private final long maxBatchMemorySize;

    // Double queue synchronization (capacity 4, smaller than Spark's 16 for streaming)
    private final BlockingQueue<Object> outputQueue;
    private final BlockingQueue<Object> processingQueue;

    // Previous batch resources (cleaned up after consumer confirms)
    private VectorSchemaRoot previousRoot;
    private BufferAllocator previousAllocator;

    // Producer thread
    private final Thread producerThread;
    private volatile boolean closed = false;

    /**
     * Creates a new FlinkArrowFFIExporter.
     *
     * @param rowIterator Iterator over RowData to export
     * @param rowType     The Flink row type
     */
    public FlinkArrowFFIExporter(Iterator<RowData> rowIterator, RowType rowType) {
        this.rowIterator = rowIterator;
        this.rowType = rowType;
        this.arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        this.emptyDictionaryProvider = new DictionaryProvider.MapDictionaryProvider();

        // Get configuration
        AuronConfiguration config = AuronAdaptor.getInstance().getAuronConfiguration();
        this.maxBatchNumRows = config.getInteger(AuronConfiguration.BATCH_SIZE);
        this.maxBatchMemorySize = 8 * 1024 * 1024; // 8MB default, same as Spark

        this.outputQueue = new ArrayBlockingQueue<>(4);
        this.processingQueue = new ArrayBlockingQueue<>(4);

        this.producerThread = startProducerThread();
    }

    /**
     * Starts the producer thread that creates batches asynchronously.
     */
    private Thread startProducerThread() {
        Thread thread = new Thread(
                () -> {
                    try {
                        while (!closed && !Thread.currentThread().isInterrupted()) {
                            if (!rowIterator.hasNext()) {
                                outputQueue.put(new Finished(null));
                                return;
                            }

                            // Create a new batch
                            BufferAllocator allocator =
                                    FlinkArrowUtils.createChildAllocator("FlinkArrowFFIExporter-producer");
                            VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
                            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

                            // Fill the batch with data
                            while (!closed
                                    && rowIterator.hasNext()
                                    && allocator.getAllocatedMemory() < maxBatchMemorySize
                                    && writer.currentCount() < maxBatchNumRows) {
                                writer.write(rowIterator.next());
                            }
                            writer.finish();

                            // Put batch in output queue for consumer
                            outputQueue.put(new NextBatch(root, allocator));

                            // Wait for consumer to confirm it's done with previous batch
                            // This is critical for safe resource management!
                            processingQueue.take();
                        }
                        outputQueue.put(new Finished(null));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        // Normal interruption, not an error
                    } catch (Throwable e) {
                        outputQueue.clear();
                        try {
                            outputQueue.put(new Finished(e));
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    }
                },
                "FlinkArrowFFIExporter-producer");

        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler((t, e) -> {
            outputQueue.clear();
            try {
                outputQueue.put(new Finished(e));
            } catch (InterruptedException ignored) {
                // Ignore
            }
        });
        thread.start();
        return thread;
    }

    /**
     * Exports the Arrow schema to the native side.
     *
     * @param exportArrowSchemaPtr Pointer to the Arrow C Data Interface schema
     *                             structure
     */
    public void exportSchema(long exportArrowSchemaPtr) {
        try (ArrowSchema exportSchema = ArrowSchema.wrap(exportArrowSchemaPtr)) {
            Data.exportSchema(FlinkArrowUtils.ROOT_ALLOCATOR, arrowSchema, emptyDictionaryProvider, exportSchema);
        }
    }

    /**
     * Exports the next batch of data to the native side.
     *
     * <p>This method takes a batch from the producer thread and exports it
     * via the Arrow C Data Interface. The previous batch's resources are
     * cleaned up before taking the new batch (after consumer confirms).
     *
     * @param exportArrowArrayPtr Pointer to the Arrow C Data Interface array
     *                            structure
     * @return true if a batch was exported, false if no more data is available
     */
    @Override
    public boolean exportNextBatch(long exportArrowArrayPtr) {
        // Clean up previous batch resources (consumer has confirmed it's done)
        cleanupPreviousBatch();

        if (closed) {
            return false;
        }

        // Wait for producer to provide next batch
        Object state;
        try {
            state = outputQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }

        if (state instanceof Finished) {
            Finished finished = (Finished) state;
            if (finished.error != null) {
                throw new RuntimeException("Producer thread error", finished.error);
            }
            return false;
        }

        NextBatch batch = (NextBatch) state;

        // Export data via Arrow FFI
        try (ArrowArray exportArray = ArrowArray.wrap(exportArrowArrayPtr)) {
            Data.exportVectorSchemaRoot(
                    FlinkArrowUtils.ROOT_ALLOCATOR, batch.root, emptyDictionaryProvider, exportArray);
        }

        // Save references for cleanup on next call
        previousRoot = batch.root;
        previousAllocator = batch.allocator;

        // Signal producer that it can continue (we've taken the batch)
        try {
            processingQueue.put(new Object());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return true;
    }

    /**
     * Cleans up resources from the previous batch.
     */
    private void cleanupPreviousBatch() {
        if (previousRoot != null) {
            previousRoot.close();
            previousRoot = null;
        }
        if (previousAllocator != null) {
            previousAllocator.close();
            previousAllocator = null;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        producerThread.interrupt();
        cleanupPreviousBatch();

        // Drain any remaining batches in the queue to prevent resource leaks
        Object state;
        while ((state = outputQueue.poll()) != null) {
            if (state instanceof NextBatch) {
                NextBatch batch = (NextBatch) state;
                batch.root.close();
                batch.allocator.close();
            }
        }
    }
}
