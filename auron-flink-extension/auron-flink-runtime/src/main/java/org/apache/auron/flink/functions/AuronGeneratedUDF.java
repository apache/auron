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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.data.RowData;

/**
 * The contract a generated user-function invoker satisfies.
 *
 * <p>The planner emits a Java source file implementing this interface, one per admitted call site,
 * and the source travels to the task inside the expression node's serialized payload. The task
 * compiles it and drives it through this interface, so the argument conversions, the {@code eval}
 * overload selection and the result conversion are all decided at plan time by Flink's own
 * expression generator rather than re-derived reflectively per row.
 *
 * <p>Every parameter is declared at its erasure type and the interface carries no type variable,
 * because generated sources are compiled by Janino, which emits no bridge methods. A generic
 * parameter would leave the implementation's specialized method unreachable through this interface.
 *
 * <p>The interface must resolve to a single class in the task JVM. It does under the supported
 * deployment, where the Auron jar sits in {@code $FLINK_HOME/lib}: the child-first user-code
 * classloader that compiles the generated source delegates to the parent for classes the user jar
 * does not carry, so both sides see one copy. A user who also shades Auron into their own jar
 * introduces a second copy, and the cast the task makes after compiling fails immediately with a
 * {@link ClassCastException} rather than misbehaving later.
 */
public interface AuronGeneratedUDF {

    /**
     * Initializes the user function behind this invoker.
     *
     * <p>Called once per instance. The generated body builds the {@code FunctionContext} the user
     * function's own {@code open} expects from the supplied runtime context, so the function sees
     * the metric group, the cached files and the external resources of the subtask it runs in.
     *
     * @param runtimeContext the runtime context of the subtask this invoker belongs to
     * @throws Exception if the user function's initialization fails
     */
    void open(RuntimeContext runtimeContext) throws Exception;

    /**
     * Evaluates the user function for one row of arguments.
     *
     * @param args a row whose field {@code i} holds argument {@code i} in Flink's internal
     *     representation
     * @return the result in Flink's internal representation, or {@code null}
     * @throws Exception if the user function fails
     */
    Object eval(RowData args) throws Exception;

    /**
     * Releases whatever the user function acquired in {@link #open}.
     *
     * @throws Exception if the user function's teardown fails
     */
    void close() throws Exception;
}
