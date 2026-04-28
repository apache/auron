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
package org.apache.auron.jni;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.metric.MetricNode;
import org.apache.auron.protobuf.PartitionId;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.TaskDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class for calling native functions in the Auron project.
 * It handles initialization, loading data batches, and error handling.
 * Provides methods to interact with the native execution runtime and process data batches.
 */
public class AuronCallNativeWrapper {

    private static final Logger LOG = LoggerFactory.getLogger(AuronCallNativeWrapper.class);

    private final PhysicalPlanNode nativePlan;
    private final MetricNode metrics;
    private final BufferAllocator arrowAllocator;
    private final int partitionId;
    private final int stageId;
    private final int taskId;
    private final AtomicReference<Throwable> error = new AtomicReference<>(null);
    private final CDataDictionaryProvider dictionaryProvider = new CDataDictionaryProvider();
    private Schema arrowSchema;
    private long nativeRuntimePtr;
    private Consumer<VectorSchemaRoot> batchConsumer;

    // initialize native environment
    static {
        LOG.info("Initializing native environment (batchSize="
                + AuronAdaptor.getInstance().getAuronConfiguration().get(AuronConfiguration.BATCH_SIZE) + ", "
                + "memoryFraction="
                + AuronAdaptor.getInstance().getAuronConfiguration().get(AuronConfiguration.MEMORY_FRACTION) + ")");

        // arrow configuration
        System.setProperty("arrow.struct.conflict.policy", "CONFLICT_APPEND");

        // preload JNI bridge classes
        try {
            Class.forName("org.apache.auron.jni.JniBridge");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Cannot load JniBridge class", e);
        }

        AuronAdaptor.getInstance().loadAuronLib();
        Runtime.getRuntime().addShutdownHook(new Thread(JniBridge::onExit));
    }

    public AuronCallNativeWrapper(
            BufferAllocator arrowAllocator,
            PhysicalPlanNode nativePlan,
            MetricNode metrics,
            int partitionId,
            int stageId,
            int taskId,
            long nativeMemory) {
        this.arrowAllocator = arrowAllocator;
        this.nativePlan = nativePlan;
        this.metrics = metrics;
        this.partitionId = partitionId;
        this.stageId = stageId;
        this.taskId = taskId;

        LOG.warn("Start executing native plan");
        this.nativeRuntimePtr = JniBridge.callNative(
                nativeMemory,
                AuronAdaptor.getInstance().getAuronConfiguration().get(AuronConfiguration.NATIVE_LOG_LEVEL),
                this);
    }

    /**
     * Loads and processes the next batch of data from the native plan.
     * <p>
     * This method attempts to fetch the next batch of data from the native execution runtime.
     * If successful, it passes the data to the provided consumer callback. If the native runtime
     * pointer is invalid (0), or if there are no more batches, the method returns false.
     * </p>
     *
     * @param batchConsumer The consumer lambda that processes the loaded data batch.
     *            This lambda receives a `VectorSchemaRoot` containing the batch data.
     * @return true If a batch was successfully loaded and processed; false if no more batches are available.
     * @throws RuntimeException If the native runtime encounters an error during batch processing.
     */
    public boolean loadNextBatch(Consumer<VectorSchemaRoot> batchConsumer) {
        checkError();
        // load next batch
        try {
            this.batchConsumer = batchConsumer;
            if (nativeRuntimePtr != 0 && JniBridge.nextBatch(nativeRuntimePtr)) {
                return true;
            }
        } finally {
            // if error has been set, throw set error instead of this caught exception
            checkError();
        }

        // if no more batches, close the native runtime and return false
        close();
        return false;
    }

    protected MetricNode getMetrics() {
        return metrics;
    }

    protected void importSchema(long ffiSchemaPtr) {
        try (ArrowSchema ffiSchema = ArrowSchema.wrap(ffiSchemaPtr)) {
            arrowSchema = Data.importSchema(arrowAllocator, ffiSchema, dictionaryProvider);
        }
    }

    public Schema getArrowSchema() {
        return arrowSchema;
    }

    protected void importBatch(long ffiArrayPtr) {
        if (nativeRuntimePtr == 0) {
            throw new RuntimeException("Native runtime is finalized");
        }

        try (ArrowArray ffiArray = ArrowArray.wrap(ffiArrayPtr)) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, arrowAllocator)) {
                Data.importIntoVectorSchemaRoot(arrowAllocator, ffiArray, root, dictionaryProvider);
                batchConsumer.accept(root);
            }
        }
    }

    protected void setError(Throwable error) {
        this.error.set(error);
    }

    protected void checkError() {
        Throwable throwable = error.getAndSet(null);
        if (throwable != null) {
            close();
            throw new RuntimeException(throwable);
        }
    }

    protected byte[] getRawTaskDefinition() {
        PartitionId partition = PartitionId.newBuilder()
                .setPartitionId(partitionId)
                .setStageId(stageId)
                .setTaskId(taskId)
                .build();

        TaskDefinition taskDefinition = TaskDefinition.newBuilder()
                .setTaskId(partition)
                .setPlan(nativePlan)
                .build();

        return taskDefinition.toByteArray();
    }

    public synchronized void close() {
        if (nativeRuntimePtr != 0) {
            JniBridge.finalizeNative(nativeRuntimePtr);
            nativeRuntimePtr = 0;
            dictionaryProvider.close();
            checkError();
        }
    }
}
