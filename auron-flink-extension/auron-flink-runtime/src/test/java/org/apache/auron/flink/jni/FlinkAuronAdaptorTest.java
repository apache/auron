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
package org.apache.auron.flink.jni;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.auron.flink.functions.FlinkAuronTaskContext;
import org.apache.auron.flink.functions.FlinkAuronUDFWrapperContext;
import org.apache.auron.flink.functions.FlinkUDFPayload;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.jni.FlinkAuronAdaptor;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * This class is used to test the FlinkAuronAdaptor class.
 */
public class FlinkAuronAdaptorTest {

    @AfterEach
    public void clearPublishedContext() {
        FlinkAuronTaskContext.clearCurrent();
    }

    @Test
    public void testCreateAuronAdaptor() {
        AuronAdaptor flinkAuronAdaptor = AuronAdaptor.getInstance();
        assertInstanceOf(
                FlinkAuronAdaptor.class, flinkAuronAdaptor, "SPI should discover and instantiate FlinkAuronAdaptor");
    }

    /**
     * Contract: the Flink adaptor builds a {@link FlinkAuronUDFWrapperContext} from a serialized
     * UDF payload rather than rejecting the call.
     */
    @Test
    public void testGetAuronUDFWrapperContextReturnsWrapper() throws Exception {
        try (FlinkAuronTaskContext taskContext = newTaskContext()) {
            FlinkAuronTaskContext.setCurrent(taskContext);
            try {
                AuronUDFWrapperContext context =
                        AuronAdaptor.getInstance().getAuronUDFWrapperContext(directBuffer(payloadBytes()));

                assertInstanceOf(FlinkAuronUDFWrapperContext.class, context);
            } finally {
                FlinkAuronTaskContext.clearCurrent();
            }
        }
    }

    /**
     * Contract: when a task context is published on the calling thread, that context — not a
     * classloader — is what the native side receives as the thread context.
     */
    @Test
    public void testGetThreadContextReturnsPublishedTaskContext() throws Exception {
        try (FlinkAuronTaskContext taskContext = newTaskContext()) {
            FlinkAuronTaskContext.setCurrent(taskContext);
            try {
                assertSame(taskContext, AuronAdaptor.getInstance().getThreadContext());
            } finally {
                FlinkAuronTaskContext.clearCurrent();
            }
        }
    }

    /**
     * Contract: with no task context published, the thread context classloader is still handed
     * over. Auron's Flink source operator creates native runtimes without publishing a context, and
     * must keep working.
     */
    @Test
    public void testGetThreadContextFallsBackToContextClassLoader() {
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        ClassLoader marker = FlinkAuronAdaptorTest.class.getClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(marker);

            assertSame(marker, AuronAdaptor.getInstance().getThreadContext());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    /**
     * Contract: {@code setThreadContext} carries a task context onto a native worker thread — it
     * both publishes the context there and installs the context's user-code classloader as that
     * thread's context classloader, without which UDF classes would not resolve on the worker.
     * Requests for the same payload on that thread then resolve through the published registry, so
     * a second request returns the wrapper the first one built.
     */
    @Test
    public void testSetThreadContextPublishesContextAndClassLoaderOnWorkerThread() throws Exception {
        byte[] payload = payloadBytes();
        ExecutorService worker = Executors.newSingleThreadExecutor();
        try (FlinkAuronTaskContext taskContext = newTaskContext()) {
            worker.submit(() -> {
                        try {
                            AuronAdaptor adaptor = AuronAdaptor.getInstance();
                            adaptor.setThreadContext(taskContext);

                            assertSame(taskContext, FlinkAuronTaskContext.current());
                            assertSame(
                                    taskContext.getUserCodeClassLoader(),
                                    Thread.currentThread().getContextClassLoader());

                            AuronUDFWrapperContext first = adaptor.getAuronUDFWrapperContext(directBuffer(payload));
                            AuronUDFWrapperContext second = adaptor.getAuronUDFWrapperContext(directBuffer(payload));

                            assertNotNull(first);
                            assertSame(first, second, "an equal payload must resolve to the retained wrapper");
                            assertSame(first, taskContext.getOrCreateWrapper(payload));
                        } finally {
                            FlinkAuronTaskContext.clearCurrent();
                        }
                        return null;
                    })
                    .get();
        } finally {
            worker.shutdownNow();
        }
    }

    /**
     * Contract: a wrapper request with no task context published fails loudly rather than silently
     * building an unregistered wrapper that would be rebuilt on every drain.
     */
    @Test
    public void testGetAuronUDFWrapperContextWithoutContextThrows() throws Exception {
        FlinkAuronTaskContext.clearCurrent();
        ByteBuffer buffer = directBuffer(payloadBytes());

        assertThrows(
                IllegalStateException.class, () -> AuronAdaptor.getInstance().getAuronUDFWrapperContext(buffer));
    }

    // ------------------------------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------------------------------

    /** Minimal serializable UDF fixture; static so it captures no enclosing test instance. */
    public static class PlusOneFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Integer eval(Integer value) {
            return value == null ? null : value + 1;
        }
    }

    private static byte[] payloadBytes() throws Exception {
        FlinkUDFPayload payload = new FlinkUDFPayload(
                new PlusOneFunction(), new DataType[] {DataTypes.INT()}, DataTypes.INT(), new String[] {
                    Integer.class.getName()
                });
        return InstantiationUtil.serializeObject(payload);
    }

    private static ByteBuffer directBuffer(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.flip();
        return buffer;
    }

    private static FlinkAuronTaskContext newTaskContext() {
        return new FlinkAuronTaskContext(stubRuntimeContext(FlinkAuronAdaptorTest.class.getClassLoader()));
    }

    /**
     * {@link StreamingRuntimeContext}'s real constructors demand an {@code Environment}, an
     * {@code OperatorMetricGroup} and more, none of which the task context reads. The stub is
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
