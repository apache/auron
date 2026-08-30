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

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.table.functions.FunctionContext;

/**
 * Subtask-scoped state the native engine reaches through the thread-context channel.
 *
 * <p>An Auron Flink operator builds one instance in {@code open()} and closes it in {@code
 * close()}. The native runtime reads it from the Flink task thread when it is created and installs
 * it on every worker thread of that runtime's pool, so a callback arriving on a worker can find
 * the state belonging to the subtask it is working for.
 *
 * <p>It carries two things:
 *
 * <ul>
 *   <li>the user-code classloader, taken from the runtime context rather than from the thread's
 *       context classloader, which is what Flink itself uses wherever it loads user code;
 *   <li>a registry of {@link FlinkAuronUDFWrapperContext}s, so a wrapper is built once per subtask
 *       instead of once per drain cycle.
 * </ul>
 *
 * <p>The registry is deliberately owned by this per-subtask object rather than being a static map
 * keyed on the payload. Every subtask of an operator deserializes the same operator bytes, so the
 * payloads two subtasks present are byte-identical and could not tell them apart. A wrapper reuses
 * an argument array, an output row and its converters across evaluations, and each subtask is
 * entitled to its own {@code ScalarFunction} instance, so two subtasks sharing one wrapper would
 * corrupt results. Confining the registry to one subtask limits sharing to identical wrapper nodes
 * within a single subtask's own plan, which is the case the wrapper's own reuse argument covers.
 */
public final class FlinkAuronTaskContext implements AutoCloseable {

    private static final ThreadLocal<FlinkAuronTaskContext> CURRENT = new ThreadLocal<>();

    private final ClassLoader userCodeClassLoader;

    private final FunctionContext functionContext;

    /**
     * Keyed on the payload contents: {@link ByteBuffer#equals} and {@link ByteBuffer#hashCode} are
     * defined over the remaining bytes, so two equal payloads that arrive as separate arrays across
     * drain cycles map to one entry.
     */
    private final ConcurrentHashMap<ByteBuffer, FlinkAuronUDFWrapperContext> wrappers = new ConcurrentHashMap<>();

    /**
     * Creates the context for one subtask.
     *
     * @param runtimeContext the operator's runtime context, which supplies the user-code
     *     classloader and backs the {@link FunctionContext} handed to every user function this
     *     context opens
     */
    public FlinkAuronTaskContext(RuntimeContext runtimeContext) {
        this.userCodeClassLoader = runtimeContext.getUserCodeClassLoader();
        this.functionContext = new FunctionContext(runtimeContext);
    }

    /**
     * Returns the context published on the calling thread.
     *
     * @return the published context, or {@code null} if the caller is not running for an Auron
     *     Flink operator that publishes one
     */
    public static FlinkAuronTaskContext current() {
        return CURRENT.get();
    }

    /**
     * Publishes {@code context} on the calling thread.
     *
     * @param context the context to publish
     */
    public static void setCurrent(FlinkAuronTaskContext context) {
        CURRENT.set(context);
    }

    /**
     * Removes whatever context is published on the calling thread.
     *
     * <p>A Flink task thread outlives the operator running on it, so leaving a context published
     * would keep a user-code classloader reachable from a pooled thread after the job that owns it
     * has ended.
     */
    public static void clearCurrent() {
        CURRENT.remove();
    }

    /**
     * Returns the classloader that loaded the user's job, which is the one that can see the user
     * function classes named inside a serialized payload.
     *
     * @return the user-code classloader
     */
    public ClassLoader getUserCodeClassLoader() {
        return userCodeClassLoader;
    }

    /**
     * Returns the wrapper for {@code payload}, building it on the first request and reusing it on
     * every later one.
     *
     * <p>Reuse is what gives the user function a once-per-subtask lifecycle: the function is
     * deserialized and opened when the wrapper is built, and closed when this context is closed,
     * rather than being rebuilt on each of the roughly five drain cycles a busy subtask runs per
     * second.
     *
     * @param payload the serialized {@link FlinkUDFPayload} the planner attached to the expression
     * @return the wrapper for that payload
     * @throws IllegalStateException if the payload cannot be turned into a wrapper
     */
    public AuronUDFWrapperContext getOrCreateWrapper(byte[] payload) {
        return wrappers.computeIfAbsent(ByteBuffer.wrap(payload), key -> {
            try {
                return new FlinkAuronUDFWrapperContext(payload, userCodeClassLoader, functionContext);
            } catch (Exception e) {
                throw new IllegalStateException("error creating Flink UDF wrapper context", e);
            }
        });
    }

    /**
     * Returns how many wrappers this context currently retains.
     *
     * @return the number of retained wrappers
     */
    @VisibleForTesting
    public int wrapperCount() {
        return wrappers.size();
    }

    /**
     * Closes every retained wrapper and empties the registry.
     *
     * <p>The native side has no binding for the wrapper's {@code close}, so this is the only place
     * a user function's {@code close} is reached. A failure closing one wrapper does not stop the
     * rest from being closed; the first is rethrown with the others attached.
     */
    @Override
    public void close() {
        RuntimeException failure = null;
        try {
            for (FlinkAuronUDFWrapperContext wrapper : wrappers.values()) {
                try {
                    wrapper.close();
                } catch (RuntimeException e) {
                    if (failure == null) {
                        failure = e;
                    } else {
                        failure.addSuppressed(e);
                    }
                }
            }
        } finally {
            wrappers.clear();
        }
        if (failure != null) {
            throw failure;
        }
    }
}
