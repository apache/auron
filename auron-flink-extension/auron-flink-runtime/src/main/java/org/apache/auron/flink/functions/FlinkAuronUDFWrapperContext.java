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
package org.apache.auron.flink.functions;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.flink.arrow.FlinkArrowReader;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.auron.flink.arrow.FlinkArrowWriter;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.runtime.generated.CompileUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates a Flink {@link ScalarFunction} on behalf of the native engine.
 *
 * <p>The native side hands over two Arrow C-Data array pointers per call: the first holds a struct
 * array whose field {@code i} carries the values of {@code eval} argument {@code i}, and the second
 * is an empty struct the single result column is exported into. Values arrive and leave in Flink's
 * internal representation; the conversions to and from the representation the user function
 * declares live inside the generated class this context drives, alongside the {@code eval} call
 * itself.
 *
 * <p>An instance is retained for the lifetime of one subtask by {@link FlinkAuronTaskContext}, so
 * the generated class is compiled and instantiated once per subtask, the user function behind it is
 * opened once, and it is closed when that context is closed.
 */
public final class FlinkAuronUDFWrapperContext implements AuronUDFWrapperContext {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkAuronUDFWrapperContext.class);

    private final AuronGeneratedUDF generated;

    private final String udfClassName;

    private final RowType paramsRowType;

    private final RowType outputRowType;

    private final Schema paramsArrowSchema;

    private final Schema outputArrowSchema;

    /**
     * The exported result array is released only once the native side drops it, which happens after
     * {@link #eval} has returned. Allocating from the root rather than from a shorter-lived child
     * therefore keeps the exported buffers alive for as long as the native side holds them.
     */
    private final BufferAllocator allocator = FlinkArrowUtils.getRootAllocator();

    private final DictionaryProvider dictionaries = new MapDictionaryProvider();

    private final GenericRowData outputRow = new GenericRowData(1);

    /**
     * Builds the wrapper from the bytes the planner attached to the native expression node.
     *
     * <p>{@link FlinkAuronTaskContext} retains the result for the lifetime of the subtask, so this
     * runs once per subtask rather than once per drain cycle, and the {@code open} call at the end
     * is the user function's single initialization for that subtask.
     *
     * @param serialized the serialized {@link FlinkUDFPayload}
     * @param userCodeClassLoader the classloader that can see the user function class; taken from
     *     the runtime context rather than from the calling thread, matching where Flink itself
     *     loads user code from. It is also the only loader that can see both the user function and
     *     the Auron interface the generated class implements, which is why it is what compiles it.
     * @param runtimeContext the runtime context the generated class builds the user function's
     *     {@code FunctionContext} from
     * @throws Exception if the payload cannot be deserialized, the generated source cannot be
     *     compiled or instantiated, or the function's {@code open} fails. Every failure past
     *     deserialization names the user function class, because a message naming only the
     *     generated class tells a user with a broken function nothing about which one it is.
     */
    public FlinkAuronUDFWrapperContext(
            byte[] serialized, ClassLoader userCodeClassLoader, RuntimeContext runtimeContext) throws Exception {
        FlinkUDFPayload payload = InstantiationUtil.deserializeObject(serialized, userCodeClassLoader);

        this.udfClassName = payload.getUdfClassName();
        DataType[] argTypes = payload.getArgTypes();
        DataType returnType = payload.getReturnType();

        LogicalType[] argLogicalTypes = new LogicalType[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            argLogicalTypes[i] = argTypes[i].getLogicalType();
        }
        this.paramsRowType = RowType.of(argLogicalTypes);
        this.outputRowType = RowType.of(returnType.getLogicalType());
        this.paramsArrowSchema = FlinkArrowUtils.toArrowSchema(paramsRowType);
        this.outputArrowSchema = FlinkArrowUtils.toArrowSchema(outputRowType);

        this.generated = instantiate(payload, userCodeClassLoader);
        // Last, so a function that acquires resources in open() does so only once everything it
        // will be evaluated through has been built successfully.
        generated.open(runtimeContext);
        LOG.debug("Initialized UDF wrapper for {}", udfClassName);
    }

    /**
     * Compiles and instantiates the invoker the payload carries.
     *
     * <p>A compile failure arrives from {@code CompileUtils} as an advice to report a Flink bug,
     * naming only the synthetic class it was compiling. Neither half is true or useful here: the
     * source is Auron's, and what the reader needs is the user function it was generated for. The
     * source itself goes to the log rather than into the message, because it is many lines long and
     * this exception travels back across the native boundary.
     *
     * <p>The catch is on {@link Throwable} rather than {@link Exception} because the failures that
     * most need the function named are errors: loading the generated class links the user function,
     * so a missing dependency of it arrives as {@code NoClassDefFoundError} and a throwing static
     * initializer as {@code ExceptionInInitializerError}. Nothing is swallowed — every catch here
     * rethrows.
     */
    private AuronGeneratedUDF instantiate(FlinkUDFPayload payload, ClassLoader userCodeClassLoader) {
        try {
            Class<?> compiled = CompileUtils.compile(userCodeClassLoader, payload.getClassName(), payload.getCode());
            return (AuronGeneratedUDF)
                    compiled.getConstructor(Object[].class).newInstance((Object) payload.getReferences());
        } catch (Throwable t) {
            LOG.debug(
                    "Generated invoker for Flink UDF {} failed to load. Source:\n{}",
                    udfClassName,
                    payload.getCode(),
                    t);
            throw new IllegalStateException(
                    "Flink UDF " + udfClassName + " failed while loading its generated invoker", t);
        }
    }

    /**
     * Evaluates the function once per row of the imported batch.
     *
     * <p>The output row is reused across rows and across calls, and so is whatever per-instance
     * state the generated class holds. None of it is thread-safe, and nothing in the native
     * expression interface forbids a concurrent call: the expression is shared between threads and
     * its evaluation entry takes a shared reference, so the safety here is not a guarantee the
     * engine makes.
     *
     * <p>What makes the reuse safe is how Auron schedules the plan. A Flink native runtime executes
     * exactly one partition, and no plan Auron builds contains a node that fans evaluation of one
     * expression out across threads, so two evaluations of one wrapper never overlap. Adding a
     * repartitioning or partition-coalescing node to a plan that can carry this expression would
     * break that invariant, and this state would then have to become per-call.
     *
     * <p>Retention across drain cycles does not widen that exposure. The registry holding the
     * instance belongs to a single subtask, and each drain builds a runtime that executes one
     * partition for that same subtask, so successive users of this state are the same task thread's
     * successive runtimes rather than concurrent ones.
     *
     * @param importFFIArrayPtr address of the Arrow C-Data array holding the argument columns
     * @param exportFFIArrayPtr address of the Arrow C-Data array the result column is exported into
     */
    @Override
    public void eval(long importFFIArrayPtr, long exportFFIArrayPtr) {
        try (VectorSchemaRoot paramsRoot = VectorSchemaRoot.create(paramsArrowSchema, allocator);
                VectorSchemaRoot outputRoot = VectorSchemaRoot.create(outputArrowSchema, allocator);
                ArrowArray importArray = ArrowArray.wrap(importFFIArrayPtr);
                ArrowArray exportArray = ArrowArray.wrap(exportFFIArrayPtr)) {

            Data.importIntoVectorSchemaRoot(allocator, importArray, paramsRoot, dictionaries);

            FlinkArrowReader reader = FlinkArrowReader.create(paramsRoot, paramsRowType);
            FlinkArrowWriter writer = FlinkArrowWriter.create(outputRoot, outputRowType);

            int rowCount = reader.getRowCount();
            for (int row = 0; row < rowCount; row++) {
                // The reader hands back one reused row instance, which this loop never retains.
                RowData paramsRow = reader.read(row);
                Object internal = generated.eval(paramsRow);
                outputRow.setField(0, internal);
                writer.write(outputRow);
            }
            writer.finish();

            Data.exportVectorSchemaRoot(allocator, outputRoot, dictionaries, exportArray);
        } catch (Throwable t) {
            throw new IllegalStateException("Flink UDF " + udfClassName + " failed during evaluation", t);
        }
    }

    /**
     * Closes the user function.
     *
     * <p>Reached from {@link FlinkAuronTaskContext#close()} when the owning operator closes. The
     * native side binds no {@code close} on this interface, so nothing else calls it.
     */
    @Override
    public void close() {
        try {
            generated.close();
        } catch (Exception e) {
            throw new IllegalStateException("Flink UDF " + udfClassName + " failed while closing", e);
        }
    }
}
