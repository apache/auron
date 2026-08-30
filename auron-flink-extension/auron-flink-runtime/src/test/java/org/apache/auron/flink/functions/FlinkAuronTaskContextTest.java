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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FlinkAuronTaskContext}.
 *
 * <p>The serialized UDF payload is byte-identical across subtasks as well as across drain cycles,
 * so it can only key a registry that is itself scoped to one subtask. The wrapper it maps to owns
 * reusable per-call state (an argument array, an output row, converters), which is why sharing one
 * instance between two subtasks would corrupt results. These tests pin both halves of that: reuse
 * within one context, isolation between two.
 */
public class FlinkAuronTaskContextTest {

    /**
     * Repo-wide surefire sets {@code java.io.tmpdir=target/tmp} which may not exist on a clean
     * build. The Arrow C-Data JNI loader extracts its native library via
     * {@link File#createTempFile}, which fails if the directory is missing. Ensure it exists before
     * any test runs.
     */
    @BeforeAll
    public static void ensureTmpDirExists() {
        String tmp = System.getProperty("java.io.tmpdir");
        if (tmp != null) {
            new File(tmp).mkdirs();
        }
    }

    @BeforeEach
    public void resetLifecycleCounters() {
        LifecycleFunction.openCount = 0;
        LifecycleFunction.closeCount = 0;
        FlinkAuronTaskContext.clearCurrent();
    }

    @AfterEach
    public void clearPublishedContext() {
        FlinkAuronTaskContext.clearCurrent();
    }

    /**
     * Contract: two byte-equal but distinct payload arrays map to one wrapper instance, so a
     * wrapper survives the drain cycles that hand the same payload over again.
     */
    @Test
    public void testByteEqualPayloadsShareOneWrapper() throws Exception {
        byte[] first = payloadBytes(1);
        byte[] second = payloadBytes(1);
        assertNotSame(first, second, "the fixture must produce two distinct arrays");
        assertTrue(Arrays.equals(first, second), "the fixture must produce byte-equal arrays");

        try (FlinkAuronTaskContext context = newContext()) {
            AuronUDFWrapperContext wrapper = context.getOrCreateWrapper(first);

            assertSame(wrapper, context.getOrCreateWrapper(second));
            assertEquals(1, context.wrapperCount());
        }
    }

    /** Contract: payloads that differ in bytes map to separate wrappers within one context. */
    @Test
    public void testDifferentPayloadsGetSeparateWrappers() throws Exception {
        try (FlinkAuronTaskContext context = newContext()) {
            AuronUDFWrapperContext one = context.getOrCreateWrapper(payloadBytes(1));
            AuronUDFWrapperContext two = context.getOrCreateWrapper(payloadBytes(2));

            assertNotSame(one, two);
            assertEquals(2, context.wrapperCount());
        }
    }

    /**
     * Contract: the registry is scoped to one context, never shared globally. Two contexts stand in
     * for two subtasks of the same operator, whose payload bytes are identical; each must build its
     * own wrapper, because the wrapper's per-call state cannot be used concurrently.
     */
    @Test
    public void testSeparateContextsNeverShareAWrapper() throws Exception {
        byte[] payload = payloadBytes(1);

        try (FlinkAuronTaskContext first = newContext();
                FlinkAuronTaskContext second = newContext()) {
            AuronUDFWrapperContext fromFirst = first.getOrCreateWrapper(payload);
            AuronUDFWrapperContext fromSecond = second.getOrCreateWrapper(payload);

            assertNotSame(fromFirst, fromSecond, "subtasks must not share a wrapper instance");
        }
    }

    /**
     * Contract: the user function's {@code open(FunctionContext)} runs once per retained wrapper,
     * no matter how many times the payload is requested. Rebuilding the wrapper per request would
     * reopen the function on every drain.
     */
    @Test
    public void testFunctionIsOpenedOncePerWrapper() throws Exception {
        byte[] payload = payloadBytes(1);

        try (FlinkAuronTaskContext context = newContext()) {
            context.getOrCreateWrapper(payload);
            context.getOrCreateWrapper(payloadBytes(1));
            context.getOrCreateWrapper(payloadBytes(1));

            assertEquals(1, LifecycleFunction.openCount);
        }
    }

    /**
     * Contract: {@code close()} closes the user function behind every retained wrapper and empties
     * the registry, so a subtask releases whatever its UDFs acquired in {@code open}.
     */
    @Test
    public void testCloseClosesEveryWrapperAndEmptiesTheRegistry() throws Exception {
        FlinkAuronTaskContext context = newContext();
        context.getOrCreateWrapper(payloadBytes(1));
        context.getOrCreateWrapper(payloadBytes(2));

        context.close();

        assertEquals(2, LifecycleFunction.closeCount);
        assertEquals(0, context.wrapperCount());
    }

    /**
     * Contract: a request after {@code close()} builds a fresh wrapper rather than handing back a
     * closed one.
     */
    @Test
    public void testWrapperBuiltAfterCloseIsFresh() throws Exception {
        byte[] payload = payloadBytes(1);
        FlinkAuronTaskContext context = newContext();
        AuronUDFWrapperContext before = context.getOrCreateWrapper(payload);
        context.close();

        try {
            AuronUDFWrapperContext after = context.getOrCreateWrapper(payload);

            assertNotSame(before, after);
            assertEquals(1, context.wrapperCount());
        } finally {
            context.close();
        }
    }

    /**
     * Contract: one user function failing to close does not strand the others. Every wrapper is
     * still closed, the registry is still emptied, and the failure surfaces rather than being
     * swallowed — a silently dropped close would leak whatever that function held.
     */
    @Test
    public void testCloseClosesRemainingWrappersWhenOneFails() throws Exception {
        FlinkAuronTaskContext context = newContext();
        context.getOrCreateWrapper(payloadBytes(1));
        context.getOrCreateWrapper(failingPayloadBytes());
        context.getOrCreateWrapper(payloadBytes(2));

        IllegalStateException failure = assertThrows(IllegalStateException.class, context::close);

        assertTrue(
                failure.getMessage().contains(ThrowsOnCloseFunction.class.getName()),
                "the surfaced failure must name the function that failed: " + failure.getMessage());
        assertEquals(2, LifecycleFunction.closeCount, "the wrappers either side of the failure must still close");
        assertEquals(0, context.wrapperCount(), "the registry must be emptied even when a close fails");
    }

    /**
     * Contract: {@code setCurrent} publishes the context on the calling thread, {@code current}
     * reads it back, and {@code clearCurrent} removes it. This is the channel the native worker
     * threads read the context from, and a context left behind would pin a user-code classloader on
     * a long-lived Flink task thread.
     */
    @Test
    public void testCurrentIsPublishedAndCleared() throws Exception {
        assertNull(FlinkAuronTaskContext.current(), "no context may be published before setCurrent");

        try (FlinkAuronTaskContext context = newContext()) {
            FlinkAuronTaskContext.setCurrent(context);
            assertSame(context, FlinkAuronTaskContext.current());

            FlinkAuronTaskContext.clearCurrent();
            assertNull(FlinkAuronTaskContext.current());
        }
    }

    /**
     * Contract: the context exposes the runtime context's user-code classloader, which is what the
     * adaptor installs on each native worker thread so UDF classes resolve there.
     */
    @Test
    public void testUserCodeClassLoaderComesFromTheRuntimeContext() throws Exception {
        ClassLoader loader = new URLClassLoader(new URL[0], FlinkAuronTaskContextTest.class.getClassLoader());

        try (FlinkAuronTaskContext context = new FlinkAuronTaskContext(stubRuntimeContext(loader))) {
            assertSame(loader, context.getUserCodeClassLoader());
        }
    }

    // ------------------------------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------------------------------

    /**
     * Counts its lifecycle callbacks statically: the function instance inside a payload is
     * Java-deserialized when the wrapper is built, so instance fields never travel back here.
     */
    public static class LifecycleFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        static int openCount;
        static int closeCount;

        private final int addend;

        public LifecycleFunction(int addend) {
            this.addend = addend;
        }

        @Override
        public void open(FunctionContext context) {
            openCount++;
        }

        @Override
        public void close() {
            closeCount++;
        }

        public Integer eval(Integer value) {
            return value == null ? null : value + addend;
        }
    }

    /**
     * Serializes a payload the way the planner does. Two calls with the same {@code addend} produce
     * two distinct arrays that are {@link Arrays#equals} — the shape drain cycles hand over.
     */
    private static byte[] payloadBytes(int addend) throws Exception {
        return InstantiationUtil.serializeObject(FlinkUDFPayload.of(
                new LifecycleFunction(addend),
                new DataType[] {DataTypes.INT()},
                DataTypes.INT(),
                evalMethod(LifecycleFunction.class),
                0));
    }

    /** Serializes a payload whose function fails in {@code close}. */
    private static byte[] failingPayloadBytes() throws Exception {
        return InstantiationUtil.serializeObject(FlinkUDFPayload.of(
                new ThrowsOnCloseFunction(),
                new DataType[] {DataTypes.INT()},
                DataTypes.INT(),
                evalMethod(ThrowsOnCloseFunction.class),
                0));
    }

    private static Method evalMethod(Class<? extends ScalarFunction> udfClass) {
        for (Method method : udfClass.getDeclaredMethods()) {
            if ("eval".equals(method.getName())) {
                return method;
            }
        }
        throw new IllegalStateException("no eval method declared on " + udfClass.getName());
    }

    /** Fails in {@code close}, so the aggregation of a failing close can be exercised. */
    public static class ThrowsOnCloseFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        @Override
        public void close() {
            throw new IllegalStateException("close failed");
        }

        public Integer eval(Integer value) {
            return value;
        }
    }

    private static FlinkAuronTaskContext newContext() {
        return new FlinkAuronTaskContext(stubRuntimeContext(FlinkAuronTaskContextTest.class.getClassLoader()));
    }

    /**
     * {@link StreamingRuntimeContext}'s real constructors demand an {@code Environment}, an
     * {@code OperatorMetricGroup} and more, none of which the context under test reads. The stub is
     * allocated without running a constructor and overrides only {@code getUserCodeClassLoader()}.
     */
    private static StreamingRuntimeContext stubRuntimeContext(ClassLoader loader) {
        try {
            StubRuntimeContext context = (StubRuntimeContext) unsafe().allocateInstance(StubRuntimeContext.class);
            context.userCodeClassLoader = loader;
            return context;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Runtime context stub whose only live method is {@code getUserCodeClassLoader()}. */
    static class StubRuntimeContext extends StreamingRuntimeContext {
        ClassLoader userCodeClassLoader;

        // Never invoked; instances are allocated via Unsafe. The compiler still requires a
        // parent constructor call.
        @SuppressWarnings("unused")
        private StubRuntimeContext() {
            super(null, null, null);
        }

        @Override
        public ClassLoader getUserCodeClassLoader() {
            return userCodeClassLoader;
        }
    }

    private static sun.misc.Unsafe unsafe() throws Exception {
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (sun.misc.Unsafe) f.get(null);
    }
}
