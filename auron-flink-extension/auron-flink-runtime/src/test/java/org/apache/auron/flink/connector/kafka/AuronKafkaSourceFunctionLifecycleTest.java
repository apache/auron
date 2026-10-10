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
package org.apache.auron.flink.connector.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.auron.flink.functions.GeneratedUdfTestSupport;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.PhysicalUDFWrapperExprNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Lifecycle tests for {@link AuronKafkaSourceFunction}'s task context, driving {@code open()} and
 * {@code close()} directly rather than through a cluster.
 *
 * <p>The path that needs this is the one a job never exercises on its happy route: {@code open()}
 * succeeds, {@code run()} is never entered, and {@code close()} is then the only thing that can give
 * a pre-built user function its {@code close()}. Running a query cannot reach that state, because
 * the source thread is always started once the task gets that far.
 *
 * <p>Mock data is what makes the direct drive possible: it takes {@code open()} down the branch that
 * neither contacts a broker nor registers runtime state, leaving the wrapper pre-build as the part
 * under test.
 */
class AuronKafkaSourceFunctionLifecycleTest {

    private static final String MOCK_DATA = "[{\"serialized_kafka_records_partition\": 0, "
            + "\"serialized_kafka_records_offset\": 1, \"serialized_kafka_records_timestamp\": 1, "
            + "\"age\": 20, \"name\": \"a\"}]";

    private static final RowType SOURCE_ROW_TYPE = RowType.of(
            new LogicalType[] {new IntType(), new VarCharType(VarCharType.MAX_LENGTH)}, new String[] {"age", "name"});

    private static final RowType PROJECTED_ROW_TYPE =
            RowType.of(new LogicalType[] {new IntType()}, new String[] {"age"});

    /**
     * Repo-wide surefire sets {@code java.io.tmpdir=target/tmp} which may not exist on a clean
     * build. The Arrow C-Data JNI loader extracts its native library via
     * {@link File#createTempFile}, which fails if the directory is missing. Ensure it exists before
     * any test runs.
     */
    @BeforeAll
    static void ensureTmpDirExists() {
        String tmp = System.getProperty("java.io.tmpdir");
        if (tmp != null) {
            new File(tmp).mkdirs();
        }
    }

    @BeforeEach
    void resetCounters() {
        LifecycleFunction.reset();
    }

    /**
     * Contract: when {@code open()} succeeded but {@code run()} was never entered, {@code close()}
     * closes the task context, so a user function the pre-build opened still gets its {@code
     * close()}. Without that backstop the function is opened and never closed, and anything it
     * acquired leaks for every task attempt.
     *
     * <p>The {@code open()} assertion is a precondition, not the subject: unless the pre-build
     * actually opened the function there is nothing for {@code close()} to have to close, and the
     * closing assertion would hold vacuously.
     */
    @Test
    void testCloseClosesUserFunctionWhenRunNeverEntered() throws Exception {
        AuronKafkaSourceFunction source = newSourceWithUdf();

        source.open(new Configuration());
        assertEquals(1, LifecycleFunction.openCount.get(), "open() must pre-build and open the wrapper");

        source.close();

        assertEquals(1, LifecycleFunction.closeCount.get(), "close() must close a context run() never claimed");
    }

    /**
     * Contract: {@code close()} is safe to call when {@code open()} never built a context, which is
     * the shape of a task that failed early. It must not fault on the null context, and it must
     * still reach the Kafka teardown below it.
     */
    @Test
    void testCloseWithoutOpenDoesNotFail() throws Exception {
        AuronKafkaSourceFunction source = newSourceWithUdf();

        source.close();

        assertEquals(0, LifecycleFunction.closeCount.get(), "no context was built, so no user function was closed");
    }

    /**
     * Contract: when {@code close()} has already taken and closed the task context, {@code run()}
     * must abort instead of starting a native runtime. A runtime started at that point has no
     * published context, so its first UDF callback fails, and it would be driving user functions
     * that are already closed.
     *
     * <p>The closing assertion is the one that pins the abort: reaching the native runtime would
     * either fault on the missing context or, once past it, take the teardown that close() already
     * performed.
     */
    @Test
    void testRunAbortsWhenCloseAlreadyTookTheContext() throws Exception {
        AuronKafkaSourceFunction source = newSourceWithUdf();

        source.open(new Configuration());
        source.close();
        assertEquals(1, LifecycleFunction.closeCount.get(), "close() must have closed the context first");

        CountingSourceContext sourceContext = new CountingSourceContext();
        source.run(sourceContext);

        assertEquals(0, sourceContext.collected.get(), "an aborted run must emit no rows");
        assertEquals(1, LifecycleFunction.closeCount.get(), "an aborted run must not close the user function again");
    }

    /** Source context that only counts what a run would emit. */
    private static class CountingSourceContext implements SourceFunction.SourceContext<RowData> {
        final AtomicInteger collected = new AtomicInteger();
        private final Object checkpointLock = new Object();

        @Override
        public void collect(RowData element) {
            collected.incrementAndGet();
        }

        @Override
        public void collectWithTimestamp(RowData element, long timestamp) {
            collected.incrementAndGet();
        }

        @Override
        public void emitWatermark(Watermark mark) {}

        @Override
        public void markAsTemporarilyIdle() {}

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }

        @Override
        public void close() {}
    }

    private AuronKafkaSourceFunction newSourceWithUdf() throws Exception {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "127.0.0.1:9092");
        AuronKafkaSourceFunction source = new AuronKafkaSourceFunction(
                SOURCE_ROW_TYPE,
                "lifecycle-test-op",
                "mock_topic",
                kafkaProperties,
                "JSON",
                Collections.emptyMap(),
                8192,
                "EARLIEST",
                -1L);
        source.setMockData(MOCK_DATA);
        source.setMergedCalcPlan(planCarryingUdfWrapper(), PROJECTED_ROW_TYPE);
        source.setRuntimeContext(stubRuntimeContext());
        return source;
    }

    /**
     * A {@code Project[FFIReader]} sub-plan whose single expression is a UDF wrapper, which is the
     * shape the fusion pass stages onto the source. {@code open()} splices the Kafka scan into the
     * placeholder leaf, then collects the wrapper payload out of the result.
     */
    private static PhysicalPlanNode planCarryingUdfWrapper() throws Exception {
        byte[] payload = GeneratedUdfTestSupport.payloadBytes(
                new LifecycleFunction(), new DataType[] {DataTypes.INT()}, DataTypes.INT(), 0);
        PhysicalExprNode wrapper = PhysicalExprNode.newBuilder()
                .setUdfWrapperExpr(PhysicalUDFWrapperExprNode.newBuilder()
                        .setSerialized(ByteString.copyFrom(payload))
                        .setReturnType(SchemaConverters.convertToAuronArrowType(new IntType()))
                        .setReturnNullable(true)
                        .setExprString("lifecycle_udf(age)")
                        .build())
                .build();
        PhysicalPlanNode leaf = PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setNumPartitions(1)
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();
        return PhysicalPlanNode.newBuilder()
                .setProjection(ProjectionExecNode.newBuilder()
                        .setInput(leaf)
                        .addExpr(wrapper)
                        .addExprName("age")
                        .addDataType(SchemaConverters.convertToAuronArrowType(new IntType()))
                        .build())
                .build();
    }

    /**
     * {@link StreamingRuntimeContext}'s real constructors demand an {@code Environment}, an
     * {@code OperatorMetricGroup} and more. Taking the mock-data branch narrows what {@code open()}
     * reads to the subtask index and the user-code classloader, so the stub is allocated without
     * running a constructor and overrides only those two.
     */
    private static StreamingRuntimeContext stubRuntimeContext() throws Exception {
        StubRuntimeContext context = (StubRuntimeContext) unsafe().allocateInstance(StubRuntimeContext.class);
        context.userCodeClassLoader = AuronKafkaSourceFunctionLifecycleTest.class.getClassLoader();
        return context;
    }

    /** Runtime context stub whose only live methods are the two {@code open()} reads. */
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

        @Override
        public int getIndexOfThisSubtask() {
            return 0;
        }
    }

    private static sun.misc.Unsafe unsafe() throws Exception {
        Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
        f.setAccessible(true);
        return (sun.misc.Unsafe) f.get(null);
    }

    /** Counts its own {@code open} and {@code close} so the lifecycle is observable. */
    public static class LifecycleFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        static final AtomicInteger openCount = new AtomicInteger();
        static final AtomicInteger closeCount = new AtomicInteger();

        static void reset() {
            openCount.set(0);
            closeCount.set(0);
        }

        @Override
        public void open(FunctionContext context) {
            openCount.incrementAndGet();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        /**
         * Increments its argument.
         *
         * @param a the argument
         * @return {@code a + 1}
         */
        public Integer eval(Integer a) {
            return a + 1;
        }
    }
}
