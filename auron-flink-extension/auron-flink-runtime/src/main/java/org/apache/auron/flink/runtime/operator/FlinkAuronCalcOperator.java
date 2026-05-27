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
package org.apache.auron.flink.runtime.operator;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.auron.flink.arrow.FlinkArrowFFIExporter;
import org.apache.auron.flink.arrow.FlinkArrowReader;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.auron.flink.metric.FlinkMetricNode;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.jni.AuronCallNativeWrapper;
import org.apache.auron.jni.JniBridge;
import org.apache.auron.metric.MetricNode;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.logical.RowType;

/**
 * Flink {@link TableStreamOperator} that executes a pre-composed native Calc plan of shape
 * {@code Project[Filter[FFIReader]]} (or any single/combination of {@code Project} / {@code
 * Filter} above an {@code FFIReader} leaf) by streaming {@link RowData} through Auron's native
 * engine.
 *
 * <p>Architecture:
 *
 * <ul>
 *   <li>Input bridge: {@link FlinkArrowFFIExporter} buffers JVM {@link RowData} into an Arrow
 *       {@link VectorSchemaRoot} and exports it across the Arrow C-Data FFI to native code.
 *   <li>Native runtime: an {@link AuronCallNativeWrapper} executes the {@link PhysicalPlanNode}
 *       and pulls input batches from the exporter (registered in {@link JniBridge}'s resource
 *       map under a per-subtask resource ID).
 *   <li>Output bridge: {@link FlinkArrowReader} wraps each native output batch as Flink
 *       {@link RowData}; rows are forwarded via the inherited {@code output} collector.
 * </ul>
 *
 * <p>Resource ID convention: {@code FlinkAuronCalc-<flinkOpUniqueId>-<subtaskIndex>:<UUID>}. The
 * Flink operator unique ID and subtask index keep the key unique per subtask so native-side
 * registrations from concurrent subtasks of the same operator do not collide; the UUID
 * disambiguates re-runs after operator restart. The {@link FFIReaderExecNode} proto has no
 * {@code auron_operator_id} field, so identity lives exclusively in the resource ID.
 *
 * <p>Lifecycle and drain semantics:
 *
 * <ul>
 *   <li>{@link #processElement(StreamRecord)} buffers each row; if the exporter signals
 *       batch-full it eagerly drains the native pipeline.
 *   <li>{@link #processWatermark(Watermark)} drains BEFORE forwarding the watermark — otherwise
 *       a downstream operator could receive a watermark that crosses rows still buffered here.
 *   <li>{@link #prepareSnapshotPreBarrier(long)} drains so buffered rows reach downstream
 *       before the checkpoint barrier crosses them. Calc is stateless; no operator-state writes
 *       are needed.
 *   <li>{@link #close()} signals end-of-input, performs a final drain, then closes the native
 *       runtime, exporter, and child allocator in nested try/finally so partial failure still
 *       releases resources. Idempotent: fields are nulled after close.
 * </ul>
 *
 * <p>Valid construction (well-formed plan tree, matching row types, non-null operator ID) is
 * the caller's responsibility — no defensive null checks are performed.
 */
public class FlinkAuronCalcOperator extends TableStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData>, FlinkAuronOperator {

    private static final long serialVersionUID = 1L;

    /**
     * Soft row-count threshold above which the exporter reports {@link
     * FlinkArrowFFIExporter#isBatchFull()} and the operator eagerly drains the pipeline. 8192 is
     * the conventional batch size for vectorized engines (matches the Spark integration's
     * batch-size default of ~10000 and DataFusion's typical batch size).
     */
    private static final int BATCH_ROW_LIMIT = 8192;

    private final PhysicalPlanNode plan;
    private final RowType inputRowType;
    private final RowType outputRowType;
    private final String auronOperatorId;
    private final NativeRuntimeFactory nativeRuntimeFactory;

    // Per-subtask runtime state (transient — re-initialized in open()).
    private transient String resourceId;
    private transient BufferAllocator childAllocator;
    private transient FlinkArrowFFIExporter exporter;
    private transient FlinkMetricNode metricNode;
    private transient NativeRuntime nativeRuntime;
    private transient PhysicalPlanNode runtimePlan;
    private transient StreamRecord<RowData> outputRecord;
    // Cached so reinitExporterAndRuntime() can reuse the value resolved once in open().
    private transient long nativeMemory;
    // Set in close() before the final drain so drainNative() skips its post-loop reinit.
    private transient boolean closing;

    /**
     * Production constructor: uses the default factory that creates an
     * {@link AuronCallNativeWrapper}.
     *
     * @param plan the pre-composed native plan tree (Project / Filter / FFIReader shape)
     * @param inputRowType the Flink row type of the input stream
     * @param outputRowType the Flink row type of the output stream
     * @param auronOperatorId the Auron operator ID for metric and identity propagation; the
     *     subtask-suffixed variant is held only in the runtime resource ID
     */
    public FlinkAuronCalcOperator(
            PhysicalPlanNode plan, RowType inputRowType, RowType outputRowType, String auronOperatorId) {
        this(plan, inputRowType, outputRowType, auronOperatorId, defaultNativeRuntimeFactory());
    }

    /** Test seam constructor accepting a {@link NativeRuntimeFactory}. */
    @VisibleForTesting
    FlinkAuronCalcOperator(
            PhysicalPlanNode plan,
            RowType inputRowType,
            RowType outputRowType,
            String auronOperatorId,
            NativeRuntimeFactory nativeRuntimeFactory) {
        this.plan = plan;
        this.inputRowType = inputRowType;
        this.outputRowType = outputRowType;
        this.auronOperatorId = auronOperatorId;
        this.nativeRuntimeFactory = nativeRuntimeFactory;
    }

    @Override
    public void open() throws Exception {
        super.open();
        StreamingRuntimeContext rc = (StreamingRuntimeContext) getRuntimeContext();
        String opIdWithSubtask = rc.getOperatorUniqueID() + "-" + rc.getIndexOfThisSubtask();

        this.childAllocator = FlinkArrowUtils.createChildAllocator("FlinkAuronCalc-" + opIdWithSubtask);
        this.metricNode = new FlinkMetricNode(getMetricGroup(), Collections.emptyList());
        this.exporter = new FlinkArrowFFIExporter(childAllocator, inputRowType, BATCH_ROW_LIMIT);
        // UUID disambiguates re-runs after operator restart so a stale registration cannot
        // collide with the new one.
        this.resourceId = "FlinkAuronCalc-" + opIdWithSubtask + ":" + UUID.randomUUID();
        JniBridge.putResource(resourceId, exporter);

        this.runtimePlan = injectFfiReaderLeaf(plan, resourceId);

        this.nativeMemory =
                AuronAdaptor.getInstance().getAuronConfiguration().getLong(FlinkAuronConfiguration.NATIVE_MEMORY_SIZE);
        this.nativeRuntime =
                nativeRuntimeFactory.create(FlinkArrowUtils.getRootAllocator(), runtimePlan, metricNode, nativeMemory);

        this.outputRecord = new StreamRecord<>(null);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        exporter.offer(element.getValue());
        if (exporter.isBatchFull()) {
            drainNative();
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        drainNative();
        super.processWatermark(mark);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        drainNative();
        super.prepareSnapshotPreBarrier(checkpointId);
    }

    @Override
    public void close() throws Exception {
        // Set first so the final drainNative() below skips its post-loop reinit; otherwise
        // we'd allocate a new wrapper + exporter root only to tear them down immediately.
        closing = true;
        try {
            // Both exporter and nativeRuntime must be initialized before draining: if open()
            // failed between exporter creation and nativeRuntime creation, drainNative() would
            // NPE on nativeRuntime.loadNextBatch(...). Each resource is null-guarded
            // individually in the nested finally so partial-init still releases what exists.
            if (exporter != null && nativeRuntime != null) {
                drainNative();
            }
        } finally {
            try {
                if (nativeRuntime != null) {
                    nativeRuntime.close();
                    nativeRuntime = null;
                }
            } finally {
                try {
                    if (exporter != null) {
                        exporter.close();
                        exporter = null;
                    }
                } finally {
                    try {
                        if (childAllocator != null) {
                            childAllocator.close();
                            childAllocator = null;
                        }
                    } finally {
                        super.close();
                    }
                }
            }
        }
    }

    /**
     * Drains all output of the current cycle, then rebuilds the exporter + native runtime for
     * the next cycle. Each batch is emitted via {@link #emitArrowBatch(VectorSchemaRoot)}.
     *
     * <p>Early-returns when the exporter has no buffered rows: a drain on an empty exporter
     * would produce zero output and still tear down the wrapper (because
     * {@link FlinkArrowFFIExporter#exportNextBatch(long)} returns false on empty, and
     * {@link org.apache.auron.jni.AuronCallNativeWrapper#loadNextBatch} auto-closes the
     * native runtime on a false return). Skipping the round trip keeps the wrapper alive
     * across no-op watermark/checkpoint drains.
     *
     * <p>After a non-empty drain, the underlying {@code AuronCallNativeWrapper} has auto-closed
     * itself, and the native FFI Reader's drop has already called
     * {@link FlinkArrowFFIExporter#close()} on the Java exporter. Without per-cycle reinit, the
     * next {@code processElement} would either silently drop rows (the wrapper's invalid native
     * pointer fast-fails {@code loadNextBatch}) or NPE on {@code exporter.offer(row)} (writer
     * nulled by the FFI Reader's drop). Reinit is suppressed during operator {@link #close()}
     * via the {@link #closing} guard so the final drain does not allocate replacement resources
     * only to tear them down.
     */
    private void drainNative() {
        if (exporter.isEmpty()) {
            return;
        }
        while (nativeRuntime.loadNextBatch(this::emitArrowBatch)) {
            // loop
        }
        if (!closing) {
            reinitExporterAndRuntime();
        }
    }

    /**
     * Re-prepares the exporter, re-registers it under {@link #resourceId}, and builds a fresh
     * {@link NativeRuntime} via the factory. {@link #resourceId}, {@link #runtimePlan},
     * {@link #metricNode}, and {@link #nativeMemory} are all set once in {@link #open()} and
     * reused across every reinit, so per-subtask operator identity is preserved.
     */
    private void reinitExporterAndRuntime() {
        exporter.reset();
        JniBridge.putResource(resourceId, exporter);
        this.nativeRuntime =
                nativeRuntimeFactory.create(FlinkArrowUtils.getRootAllocator(), runtimePlan, metricNode, nativeMemory);
    }

    /**
     * Wraps the Arrow batch as a Flink {@link FlinkArrowReader} and emits each row downstream
     * through the inherited {@code output} collector.
     */
    private void emitArrowBatch(VectorSchemaRoot root) {
        FlinkArrowReader reader = FlinkArrowReader.create(root, outputRowType);
        int n = reader.getRowCount();
        for (int i = 0; i < n; i++) {
            output.collect(outputRecord.replace(reader.read(i)));
        }
    }

    /**
     * Recursively walks the plan tree, locates the {@link FFIReaderExecNode} leaf, and rebuilds
     * the path so that the leaf has the runtime-bound resource ID. All other FFI Reader fields
     * ({@code num_partitions}, {@code schema}) are preserved.
     *
     * <p>Accepted shapes: {@code FFIReader}, {@code Filter[FFIReader]}, {@code Project[FFIReader]},
     * {@code Project[Filter[FFIReader]]}. Throws {@link IllegalArgumentException} on anything
     * else so misconfigurations fail fast at operator startup.
     *
     * @param node the plan node to walk
     * @param resourceId the resource ID to inject into the FFI Reader leaf
     * @return a new plan tree with the resource ID bound on the leaf
     */
    @VisibleForTesting
    static PhysicalPlanNode injectFfiReaderLeaf(PhysicalPlanNode node, String resourceId) {
        if (node.hasFfiReader()) {
            FFIReaderExecNode rewritten = node.getFfiReader().toBuilder()
                    .setExportIterProviderResourceId(resourceId)
                    .build();
            return node.toBuilder().setFfiReader(rewritten).build();
        }
        if (node.hasProjection()) {
            PhysicalPlanNode rewrittenInput =
                    injectFfiReaderLeaf(node.getProjection().getInput(), resourceId);
            return node.toBuilder()
                    .setProjection(node.getProjection().toBuilder()
                            .setInput(rewrittenInput)
                            .build())
                    .build();
        }
        if (node.hasFilter()) {
            PhysicalPlanNode rewrittenInput =
                    injectFfiReaderLeaf(node.getFilter().getInput(), resourceId);
            return node.toBuilder()
                    .setFilter(node.getFilter().toBuilder()
                            .setInput(rewrittenInput)
                            .build())
                    .build();
        }
        throw new IllegalArgumentException("FlinkAuronCalcOperator expects Project[Filter[FFIReader]] / "
                + "Project[FFIReader] / Filter[FFIReader] / FFIReader shape; got: "
                + node.getPhysicalPlanTypeCase());
    }

    // ====================================================================
    // SupportsAuronNative
    // ====================================================================

    @Override
    public List<PhysicalPlanNode> getPhysicalPlanNodes() {
        return Collections.singletonList(plan);
    }

    @Override
    public RowType getOutputType() {
        return outputRowType;
    }

    @Override
    public String getAuronOperatorId() {
        return auronOperatorId;
    }

    @Override
    public MetricNode getMetricNode() {
        return metricNode;
    }

    // ====================================================================
    // Mock seam
    // ====================================================================

    /**
     * Test seam abstracting the native runtime so unit tests can drive the operator without
     * loading {@code libauron} or invoking real JNI.
     */
    @VisibleForTesting
    interface NativeRuntime extends AutoCloseable {
        /**
         * Pulls the next batch from the native engine and passes it to {@code consumer}.
         *
         * @param consumer callback that receives the next batch's {@link VectorSchemaRoot}
         * @return {@code true} if a batch was produced (consumer was called), {@code false} if
         *     the stream is exhausted
         */
        boolean loadNextBatch(Consumer<VectorSchemaRoot> consumer);

        @Override
        void close();
    }

    /** Factory for {@link NativeRuntime} instances, swappable in tests. */
    @VisibleForTesting
    interface NativeRuntimeFactory {
        /**
         * Creates a native runtime for the given plan.
         *
         * @param allocator the Arrow root allocator
         * @param plan the runtime-bound plan (with resource IDs injected)
         * @param metric the metric node to forward native engine metrics to
         * @param nativeMemory the native memory budget for the runtime in bytes
         * @return a new {@link NativeRuntime}
         */
        NativeRuntime create(BufferAllocator allocator, PhysicalPlanNode plan, MetricNode metric, long nativeMemory);
    }

    private static NativeRuntimeFactory defaultNativeRuntimeFactory() {
        return (allocator, plan, metric, mem) -> {
            AuronCallNativeWrapper wrapper = new AuronCallNativeWrapper(allocator, plan, metric, 0, 0, 0, mem);
            return new NativeRuntime() {
                @Override
                public boolean loadNextBatch(Consumer<VectorSchemaRoot> consumer) {
                    return wrapper.loadNextBatch(consumer);
                }

                @Override
                public void close() {
                    wrapper.close();
                }
            };
        };
    }
}
