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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;

/**
 * The object graph carried inside the opaque {@code serialized} bytes of a native UDF wrapper node.
 * It is the single schema the planner and the runtime must agree on: the planner writes it, and
 * {@link FlinkAuronUDFWrapperContext} reads it back.
 */
public final class FlinkUDFPayload implements Serializable {

    private static final long serialVersionUID = 1L;

    private final ScalarFunction function;

    private final DataType[] argTypes;

    private final DataType returnType;

    private final String[] evalParameterTypeNames;

    /**
     * Distinguishes this wrapper node from every other one in the same plan.
     *
     * <p>Nothing reads it back; its whole job is to be part of the serialized form. The runtime
     * keys its per-subtask wrapper registry on the payload bytes, because the native callback
     * carries nothing else, so two nodes whose payloads were byte-equal would resolve to one
     * wrapper and therefore to one user function instance. That is observable whenever the
     * function keeps instance state, and it diverges from Flink, which hands each call site its
     * own copy of the function. Two calls of one function on arguments of the same types produce
     * exactly such a pair, since the arguments travel outside the payload.
     */
    private final int nodeOrdinal;

    /**
     * Creates a payload for a plan holding a single wrapper node.
     *
     * <p>Prefer {@link #of}, which takes the node's ordinal: a payload built here always carries
     * ordinal zero, so two of them collide.
     *
     * @param function the resolved user function instance the wrapper invokes
     * @param argTypes one type per {@code eval} argument, in argument order
     * @param returnType the type of the value {@code eval} produces
     * @param evalParameterTypeNames the declared parameter type names of the selected {@code eval}
     *     overload, in {@link Class#getName()} form, so that the runtime re-resolves the identical
     *     {@link Method} instead of repeating the overload selection
     * @throws IllegalArgumentException if the argument-type and parameter-name counts disagree
     */
    public FlinkUDFPayload(
            ScalarFunction function, DataType[] argTypes, DataType returnType, String[] evalParameterTypeNames) {
        this(function, argTypes, returnType, evalParameterTypeNames, 0);
    }

    private FlinkUDFPayload(
            ScalarFunction function,
            DataType[] argTypes,
            DataType returnType,
            String[] evalParameterTypeNames,
            int nodeOrdinal) {
        if (argTypes.length != evalParameterTypeNames.length) {
            throw new IllegalArgumentException("argTypes has " + argTypes.length
                    + " entries but the selected eval overload declares " + evalParameterTypeNames.length
                    + " parameters: " + Arrays.toString(evalParameterTypeNames));
        }
        this.function = function;
        this.argTypes = argTypes;
        this.returnType = returnType;
        this.evalParameterTypeNames = evalParameterTypeNames;
        this.nodeOrdinal = nodeOrdinal;
    }

    /**
     * Creates a payload whose parameter type names are taken from the selected {@code eval} overload
     * itself.
     *
     * <p>Prefer this over the constructor. The names must be in {@link Class#getName()} form, which
     * spells a {@code byte[]} parameter {@code "[B"}, and a caller that spells them any other way
     * produces a payload that only fails once the runtime tries to re-resolve the method — off the
     * planning thread, at query time, with no plan-time signal. Deriving them from the {@link Method}
     * removes the opportunity to disagree.
     *
     * @param function the resolved user function instance the wrapper invokes
     * @param argTypes one type per {@code eval} argument, in argument order
     * @param returnType the type of the value {@code eval} produces
     * @param evalMethod the {@code eval} overload the planner selected
     * @param nodeOrdinal a value unique to this wrapper node within its plan, which is what keeps
     *     two call sites of one function from resolving to a single shared wrapper at runtime
     * @return the payload to serialize into the wrapper node
     */
    public static FlinkUDFPayload of(
            ScalarFunction function, DataType[] argTypes, DataType returnType, Method evalMethod, int nodeOrdinal) {
        Class<?>[] parameterTypes = evalMethod.getParameterTypes();
        String[] names = new String[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            names[i] = parameterTypes[i].getName();
        }
        return new FlinkUDFPayload(function, argTypes, returnType, names, nodeOrdinal);
    }

    /**
     * Returns the user function instance the wrapper invokes.
     *
     * @return the scalar function
     */
    public ScalarFunction getFunction() {
        return function;
    }

    /**
     * Returns one type per {@code eval} argument, in argument order.
     *
     * @return the argument types
     */
    public DataType[] getArgTypes() {
        return argTypes;
    }

    /**
     * Returns the type of the value {@code eval} produces.
     *
     * @return the return type
     */
    public DataType getReturnType() {
        return returnType;
    }

    /**
     * Returns the declared parameter type names of the selected {@code eval} overload.
     *
     * @return the parameter type names, in {@link Class#getName()} form
     */
    public String[] getEvalParameterTypeNames() {
        return evalParameterTypeNames;
    }
}
