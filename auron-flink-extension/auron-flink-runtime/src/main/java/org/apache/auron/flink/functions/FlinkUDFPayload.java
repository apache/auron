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
import org.apache.flink.table.types.DataType;

/**
 * The object graph carried inside the opaque {@code serialized} bytes of a native UDF wrapper node.
 * It is the single schema the planner and the runtime must agree on: the planner writes it, and
 * {@link FlinkAuronUDFWrapperContext} reads it back.
 *
 * <p>The generated source and its reference array are two halves of one artifact. The source names
 * the user function and every converter it needs as {@code references[i]} casts, so it can only be
 * instantiated against the array it was generated with, and neither half means anything alone.
 */
public final class FlinkUDFPayload implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String className;

    private final String code;

    private final Object[] references;

    private final DataType[] argTypes;

    private final DataType returnType;

    private final String udfClassName;

    /**
     * Distinguishes this wrapper node from every other one in the same plan.
     *
     * <p>Nothing reads it back; its whole job is to be part of the serialized form. The runtime
     * keys its per-subtask wrapper registry on the payload bytes, because the native callback
     * carries nothing else, so two nodes whose payloads were byte-equal would resolve to one
     * wrapper and therefore share one user function instance between two call sites.
     *
     * <p>It is the deliberate half of that separation rather than the operative one. The bytes
     * already differ without it: {@link #className} comes from Flink's {@code CodeGenUtils.newName},
     * which appends an incrementing counter, and {@link #code} embeds that name. Carrying an explicit
     * ordinal keeps the guarantee off an incidental property of a code-generation helper this class
     * neither owns nor pins.
     *
     * <p>Separating call sites at all is stricter than Flink's own code generation, which names the
     * generated field after the function's identity rather than after the call site and deduplicates
     * the resulting member and lifecycle statements, so two call sites of one function share a
     * single instance there.
     */
    private final int nodeOrdinal;

    /**
     * Creates the payload to serialize into a wrapper node.
     *
     * @param className the name the generated class was emitted under, which is also the name the
     *     runtime compiles it under
     * @param code the generated Java source implementing {@link AuronGeneratedUDF}
     * @param references the constructor argument the generated source's {@code references[i]} casts
     *     resolve against
     * @param argTypes one type per {@code eval} argument, in argument order
     * @param returnType the type of the value {@code eval} produces
     * @param udfClassName the user function's class name, carried so a failure crossing back to the
     *     native side can name it
     * @param nodeOrdinal a value unique to this wrapper node within its plan, carried so that two
     *     call sites of one function cannot resolve to a single shared wrapper at runtime
     */
    public FlinkUDFPayload(
            String className,
            String code,
            Object[] references,
            DataType[] argTypes,
            DataType returnType,
            String udfClassName,
            int nodeOrdinal) {
        this.className = className;
        this.code = code;
        this.references = references;
        this.argTypes = argTypes;
        this.returnType = returnType;
        this.udfClassName = udfClassName;
        this.nodeOrdinal = nodeOrdinal;
    }

    /**
     * Returns the name the generated class must be compiled under.
     *
     * @return the generated class name
     */
    public String getClassName() {
        return className;
    }

    /**
     * Returns the generated Java source implementing {@link AuronGeneratedUDF}.
     *
     * @return the generated source
     */
    public String getCode() {
        return code;
    }

    /**
     * Returns the array the generated class's single {@code (Object[])} constructor takes.
     *
     * <p>The array itself, not a copy. Its entries are the live user function instance and its
     * converters, so copying the array would protect nothing that matters while suggesting it did.
     * The caller hands it straight to the generated constructor.
     *
     * @return the reference array
     */
    public Object[] getReferences() {
        return references;
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
     * Returns the user function's class name.
     *
     * @return the class name to name in an error message
     */
    public String getUdfClassName() {
        return udfClassName;
    }
}
