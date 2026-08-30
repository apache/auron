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

import java.lang.reflect.Method;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.util.InstantiationUtil;

/**
 * Builds payloads carrying a stand-in for the planner-generated invoker, so the runtime module can
 * be tested without a dependency on the planner module that generates the real one.
 *
 * <p>The stand-in is real generated source: it is emitted as text, compiled by the same
 * {@code CompileUtils} the production path uses, instantiated through the same {@code (Object[])}
 * constructor, and driven through {@link AuronGeneratedUDF}. Only its body differs — it delegates
 * to the static hooks below, which resolve the {@code eval} overload and the argument conversions
 * reflectively, where the real generator resolves both at plan time and inlines them.
 *
 * <p>Its reference array holds the function, the argument types and the return type. The layout is
 * private to whatever emits the source, exactly as the real generator's layout is private to it.
 */
public final class GeneratedUdfTestSupport {

    /**
     * Fixed, because two payloads built for the same function must be byte-equal: the runtime keys
     * its wrapper registry on the payload bytes, and several tests depend on that key holding
     * across separate serializations.
     */
    private static final String CLASS_NAME = "StandInGeneratedUdf";

    private GeneratedUdfTestSupport() {}

    /**
     * Serializes a payload the way the planner does, around a stand-in invoker for the function.
     *
     * @param function the user function the invoker calls
     * @param argTypes one type per {@code eval} argument, in argument order
     * @param returnType the type of the value {@code eval} produces
     * @param nodeOrdinal the wrapper node's ordinal within its plan
     * @return the serialized payload
     * @throws Exception if the payload cannot be serialized
     */
    public static byte[] payloadBytes(
            ScalarFunction function, DataType[] argTypes, DataType returnType, int nodeOrdinal) throws Exception {
        return InstantiationUtil.serializeObject(new FlinkUDFPayload(
                CLASS_NAME,
                source(CLASS_NAME),
                new Object[] {function, argTypes, returnType},
                argTypes,
                returnType,
                function.getClass().getName(),
                nodeOrdinal));
    }

    /**
     * Serializes a payload whose invoker source does not compile, so the wrapper's load path can be
     * exercised.
     *
     * @param function the user function the payload names
     * @param argTypes one type per {@code eval} argument, in argument order
     * @param returnType the type of the value {@code eval} produces
     * @return the serialized payload
     * @throws Exception if the payload cannot be serialized
     */
    public static byte[] uncompilablePayloadBytes(ScalarFunction function, DataType[] argTypes, DataType returnType)
            throws Exception {
        return InstantiationUtil.serializeObject(new FlinkUDFPayload(
                "BrokenGeneratedUdf",
                "public final class BrokenGeneratedUdf { this is not Java }\n",
                new Object[] {function, argTypes, returnType},
                argTypes,
                returnType,
                function.getClass().getName(),
                0));
    }

    private static String source(String className) {
        String hooks = GeneratedUdfTestSupport.class.getName();
        return "public final class " + className + " implements " + AuronGeneratedUDF.class.getName() + " {\n"
                + "  private final Object[] references;\n"
                + "  public " + className + "(Object[] references) { this.references = references; }\n"
                + "  public void open(org.apache.flink.api.common.functions.RuntimeContext rc) throws Exception {\n"
                + "    " + hooks + ".open(references, rc);\n"
                + "  }\n"
                + "  public Object eval(org.apache.flink.table.data.RowData in1) throws Exception {\n"
                + "    return " + hooks + ".eval(references, in1);\n"
                + "  }\n"
                + "  public void close() throws Exception {\n"
                + "    " + hooks + ".close(references);\n"
                + "  }\n"
                + "}\n";
    }

    /**
     * Opens the function held in {@code references}.
     *
     * @param references the stand-in's reference array
     * @param runtimeContext the runtime context the invoker was opened with
     * @throws Exception if the function's {@code open} fails
     */
    public static void open(Object[] references, RuntimeContext runtimeContext) throws Exception {
        function(references).open(new FunctionContext(runtimeContext));
    }

    /**
     * Calls the function held in {@code references} for one row of arguments.
     *
     * @param references the stand-in's reference array
     * @param args a row whose field {@code i} holds argument {@code i} internally
     * @return the result, internally represented
     * @throws Exception if the conversion or the function fails
     */
    public static Object eval(Object[] references, RowData args) throws Exception {
        ScalarFunction function = function(references);
        DataType[] argTypes = (DataType[]) references[1];
        DataType returnType = (DataType) references[2];

        Object[] external = new Object[argTypes.length];
        for (int i = 0; i < argTypes.length; i++) {
            LogicalType logicalType = argTypes[i].getLogicalType();
            Object internal = RowData.createFieldGetter(logicalType, i).getFieldOrNull(args);
            external[i] = DataTypeUtils.isInternal(argTypes[i])
                    ? internal
                    : converter(argTypes[i]).toExternalOrNull(internal);
        }

        Object result = evalMethod(function.getClass(), argTypes.length).invoke(function, external);
        return DataTypeUtils.isInternal(returnType)
                ? result
                : converter(returnType).toInternalOrNull(result);
    }

    /**
     * Closes the function held in {@code references}.
     *
     * @param references the stand-in's reference array
     * @throws Exception if the function's {@code close} fails
     */
    public static void close(Object[] references) throws Exception {
        function(references).close();
    }

    private static ScalarFunction function(Object[] references) {
        return (ScalarFunction) references[0];
    }

    private static DataStructureConverter<Object, Object> converter(DataType dataType) {
        DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(dataType);
        converter.open(GeneratedUdfTestSupport.class.getClassLoader());
        return converter;
    }

    private static Method evalMethod(Class<?> functionClass, int argCount) {
        for (Method method : functionClass.getMethods()) {
            if ("eval".equals(method.getName()) && method.getParameterCount() == argCount) {
                return method;
            }
        }
        throw new IllegalStateException("no eval taking " + argCount + " arguments on " + functionClass.getName());
    }
}
