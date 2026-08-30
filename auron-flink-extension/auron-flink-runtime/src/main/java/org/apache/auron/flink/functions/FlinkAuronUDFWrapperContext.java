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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evaluates a Flink {@link ScalarFunction} on behalf of the native engine.
 *
 * <p>The native side hands over two Arrow C-Data array pointers per call: the first holds a struct
 * array whose field {@code i} carries the values of {@code eval} argument {@code i}, and the second
 * is an empty struct the single result column is exported into. Values arrive in Flink's internal
 * representation, are converted to the external representation the user function declares, and the
 * returned value is converted back before it is written out.
 */
public final class FlinkAuronUDFWrapperContext implements AuronUDFWrapperContext {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkAuronUDFWrapperContext.class);

    private static final Map<String, Class<?>> PRIMITIVE_CLASSES = primitiveClasses();

    private final ScalarFunction function;

    private final MethodHandle evalHandle;

    private final RowData.FieldGetter[] fieldGetters;

    /** One entry per argument; null where the internal and external representations coincide. */
    private final DataStructureConverter<Object, Object>[] argConverters;

    /** Null where the internal and external representations of the return type coincide. */
    private final DataStructureConverter<Object, Object> returnConverter;

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

    private final Object[] args;

    private final GenericRowData outputRow = new GenericRowData(1);

    /**
     * Rebuilds the wrapper from the bytes the planner attached to the native expression node.
     *
     * @param serialized the serialized {@link FlinkUDFPayload}, positioned at its first byte
     * @throws Exception if the payload cannot be deserialized, or the {@code eval} overload it
     *     names cannot be resolved on the user function
     */
    @SuppressWarnings("unchecked")
    public FlinkAuronUDFWrapperContext(ByteBuffer serialized) throws Exception {
        // The tokio worker thread reaching this constructor carries the Flink task thread's context
        // classloader, propagated when the native runtime was created, so the user jar is visible.
        ClassLoader userClassLoader = Thread.currentThread().getContextClassLoader();

        // The buffer the native side hands over is direct, so it has no backing array.
        byte[] bytes = new byte[serialized.remaining()];
        serialized.get(bytes);
        FlinkUDFPayload payload = InstantiationUtil.deserializeObject(bytes, userClassLoader);

        this.function = payload.getFunction();
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

        this.fieldGetters = new RowData.FieldGetter[argTypes.length];
        // Generic arrays cannot be created directly; every element written below is a
        // DataStructureConverter<Object, Object> returned by DataStructureConverters.
        DataStructureConverter<Object, Object>[] converters = new DataStructureConverter[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            fieldGetters[i] = RowData.createFieldGetter(argLogicalTypes[i], i);
            if (!DataTypeUtils.isInternal(argTypes[i])) {
                converters[i] = DataStructureConverters.getConverter(argTypes[i]);
                converters[i].open(userClassLoader);
            }
        }
        this.argConverters = converters;

        if (DataTypeUtils.isInternal(returnType)) {
            this.returnConverter = null;
        } else {
            this.returnConverter = DataStructureConverters.getConverter(returnType);
            this.returnConverter.open(userClassLoader);
        }

        this.args = new Object[argTypes.length];
        this.evalHandle = bindEval(payload, userClassLoader);
        LOG.debug("Initialized UDF wrapper for {}", function.getClass().getName());
    }

    /**
     * Evaluates the function once per row of the imported batch.
     *
     * <p>The {@code args} array, the output row and the argument converters are reused across rows
     * and across calls. None of them is thread-safe, and nothing in the native expression interface
     * forbids a concurrent call: the expression is shared between threads and its evaluation entry
     * takes a shared reference, so the safety here is not a guarantee the engine makes.
     *
     * <p>What makes the reuse safe is how Auron schedules the plan. A Flink native runtime executes
     * exactly one partition, and no plan Auron builds contains a node that fans evaluation of one
     * expression out across threads, so two evaluations of one wrapper never overlap. Adding a
     * repartitioning or partition-coalescing node to a plan that can carry this expression would
     * break that invariant, and this state would then have to become per-call.
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
                for (int i = 0; i < args.length; i++) {
                    Object internal = fieldGetters[i].getFieldOrNull(paramsRow);
                    args[i] = argConverters[i] == null ? internal : argConverters[i].toExternalOrNull(internal);
                }
                Object external = (Object) evalHandle.invokeExact(args);
                outputRow.setField(0, returnConverter == null ? external : returnConverter.toInternalOrNull(external));
                writer.write(outputRow);
            }
            writer.finish();

            Data.exportVectorSchemaRoot(allocator, outputRoot, dictionaries, exportArray);
        } catch (Throwable t) {
            throw new IllegalStateException(
                    "Flink UDF " + function.getClass().getName() + " failed during evaluation", t);
        }
    }

    /**
     * Binds the single {@code eval} overload the planner selected to the function instance.
     *
     * <p>{@code unreflect} takes the whole descriptor from the resolved {@link Method}, so only the
     * parameter types have to be recovered from the payload and the return type never has to be
     * named separately — which {@code findVirtual} would require, and which the payload does not
     * carry. The {@code asType} adaptation then inserts the boxing and widening the erased call
     * site needs.
     *
     * <p>The handle is spread over an {@code Object[]} so the per-row call can be an
     * {@code invokeExact}, the one invocation form the JIT can inline. The looser forms re-derive
     * the adaptation on every call.
     */
    private MethodHandle bindEval(FlinkUDFPayload payload, ClassLoader classLoader) throws Exception {
        String[] parameterTypeNames = payload.getEvalParameterTypeNames();
        Class<?>[] declared = new Class<?>[parameterTypeNames.length];
        for (int i = 0; i < declared.length; i++) {
            declared[i] = resolveClass(parameterTypeNames[i], classLoader);
        }
        Method method = function.getClass().getMethod(UserDefinedFunctionHelper.SCALAR_EVAL, declared);
        Class<?>[] erased = new Class<?>[declared.length];
        Arrays.fill(erased, Object.class);
        return MethodHandles.publicLookup()
                .unreflect(method)
                .bindTo(function)
                .asType(MethodType.methodType(Object.class, erased))
                .asSpreader(Object[].class, erased.length);
    }

    /** Resolves a declared parameter type; {@link Class#forName} alone cannot name a primitive. */
    private static Class<?> resolveClass(String name, ClassLoader classLoader) throws ClassNotFoundException {
        Class<?> primitive = PRIMITIVE_CLASSES.get(name);
        return primitive != null ? primitive : Class.forName(name, true, classLoader);
    }

    private static Map<String, Class<?>> primitiveClasses() {
        Map<String, Class<?>> classes = new HashMap<>();
        classes.put("boolean", boolean.class);
        classes.put("byte", byte.class);
        classes.put("short", short.class);
        classes.put("int", int.class);
        classes.put("long", long.class);
        classes.put("float", float.class);
        classes.put("double", double.class);
        classes.put("char", char.class);
        classes.put("void", void.class);
        return classes;
    }
}
