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
package org.apache.auron.flink.table.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.auron.flink.table.planner.UnsupportedFlinkNodeRecorder;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * IT case for user-defined scalar functions running inside a native Flink Calc on Auron.
 *
 * <p>Every case asserts the fallback-recorder emit count <em>and</em> the row count. A zero emit
 * count only proves the Calc converted: a native library with no arm for the plan converts cleanly
 * and yields an empty result set with the counter still at zero, and assertions over an empty list
 * pass vacuously. The row count is what establishes that the native plan actually executed. The one
 * case whose query is expected to fail asserts the failure instead, which rules out the same thing:
 * a plan with no arm returns rows, empty ones, rather than failing.
 *
 * <p>The cases that compare an expression against the same expression evaluated by Flink obtain the
 * second answer by appending a companion expression the converter declines, since there is no switch
 * that turns the Calc rewriter off. That technique rests on
 * {@code NativePlanFusionBuilder.buildNativeCalcPlan} declining as a whole: one unconvertible
 * expression takes the entire Calc back to Flink's generated code. Were declining ever to become
 * per-expression, the companion would be the only part to fall back, the expression under test would
 * still run natively in both collections, and those cases would compare Auron against Auron — and
 * still pass. Nothing in them can detect that, so a change to that method's decline granularity has
 * to be checked against these cases by hand.
 */
public class AuronFlinkUDFITCase extends AuronFlinkTableTestBase {

    /** A user function registered under the current API reaches the native Calc and produces its values. */
    @Test
    public void testScalarUdfRunsNatively() {
        tableEnvironment.createTemporarySystemFunction("auron_plus_one", PlusOneFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select auron_plus_one(`int`) from T1")
                .collect());

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of(2), Row.of(3), Row.of(3)));
    }

    /** The deprecated registration path yields a different operator class, which must be detected too. */
    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecatedRegisterFunctionUdfRunsNatively() {
        tableEnvironment.registerFunction("auron_legacy_plus_one", new PlusOneFunction());
        UnsupportedFlinkNodeRecorder.resetForTest();

        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select auron_legacy_plus_one(`int`) from T1")
                .collect());

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of(2), Row.of(3), Row.of(3)));
    }

    /** A UDF in a CASE branch must not be evaluated on the rows that branch excludes. */
    @Test
    public void testUdfInsideCaseBranchSkipsExcludedRows() {
        tableEnvironment.createTemporarySystemFunction("auron_throws_on_one", ThrowsOnOneFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select case when `int` > 1 then auron_throws_on_one(`int`) else -1 end from T1")
                .collect());

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of(-1), Row.of(20), Row.of(20)));
    }

    /** A UDF on the right of AND must not be evaluated on the rows the left operand excludes. */
    @Test
    public void testUdfOnAndRightOperandSkipsRowsTheLeftExcludes() {
        tableEnvironment.createTemporarySystemFunction("auron_throws_on_one", ThrowsOnOneFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` > 1 and auron_throws_on_one(`int`) > 0")
                .collect());

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of(2), Row.of(2)));
    }

    /**
     * The negative direction: a UDF outside the admitted type set falls the Calc back to Flink's own
     * codegen, which still produces the correct rows. The argument here converts on its own, so the
     * declined type is the only thing that can have caused the fallback.
     */
    @Test
    public void testUnsupportedTypeFallsBackWithCorrectRows() {
        tableEnvironment.createTemporarySystemFunction("auron_second_of", SecondOfFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select auron_second_of(`int`) from T1")
                .collect());

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a declined type must record a fallback")
                .isNotZero();
        rows.sort(Comparator.comparing(o -> (LocalDateTime) o.getField(0)));
        assertThat(rows)
                .isEqualTo(Arrays.asList(
                        Row.of(LocalDateTime.of(2020, 1, 1, 0, 0, 1)),
                        Row.of(LocalDateTime.of(2020, 1, 1, 0, 0, 2)),
                        Row.of(LocalDateTime.of(2020, 1, 1, 0, 0, 2))));
    }

    /**
     * Two successive jobs in one JVM, each calling a user function the other never touches.
     *
     * <p>The second job is the one under test. A native worker resolves the function class through
     * the context classloader it was given, and a classloader captured during the first job is
     * already closed by the time the second runs. Reusing one function class across both jobs hides
     * that: the class is then already in the JVM's dictionary under the initiating loader, so
     * resolution short-circuits before it ever consults the closed loader. Two distinct classes
     * force the second job to perform a real lookup.
     */
    @Test
    public void testSecondJobResolvesItsOwnUdfClass() {
        tableEnvironment.createTemporarySystemFunction("auron_first_udf", PlusOneFunction.class);
        tableEnvironment.createTemporarySystemFunction("auron_second_udf", TimesTenFunction.class);

        UnsupportedFlinkNodeRecorder.resetForTest();
        List<Row> first = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select auron_first_udf(`int`) from T1")
                .collect());
        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the first Calc did not run natively")
                .isZero();
        first.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(first)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of(2), Row.of(3), Row.of(3)));

        UnsupportedFlinkNodeRecorder.resetForTest();
        List<Row> second = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select auron_second_udf(`int`) from T1")
                .collect());
        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the second Calc did not run natively")
                .isZero();
        second.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(second)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of(10), Row.of(20), Row.of(20)));
    }

    /**
     * A two-argument function over {@code String} rather than {@code Integer}, so the executing
     * evidence is not confined to one argument shape. {@code VARCHAR} is the one admitted type whose
     * values do not cross the boundary as-is: they arrive as Flink's internal string representation
     * and are converted before the function sees them, and the returned value is converted back.
     */
    @Test
    public void testStringArgumentsRunNatively() {
        tableEnvironment.createTemporarySystemFunction("auron_join", JoinFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select auron_join(`name`, `string`) from T1")
                .collect());

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparing(o -> (String) o.getField(0)));
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of("a:Comment#1"), Row.of("a:Comment#1"), Row.of("a:Hi")));
    }

    /**
     * A function overriding {@code open(FunctionContext)} reaches the native Calc and produces its
     * values. {@code eval} reads state that only {@code open} establishes, so a wrapper that never
     * opened the function would fail the job rather than return wrong rows.
     */
    @Test
    public void testUdfOverridingOpenRunsNatively() throws Exception {
        LifecycleFunction.reset();
        environment.setParallelism(1);
        tableEnvironment.createTemporarySystemFunction("auron_lifecycle_plus", LifecycleFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        TableResult result = tableEnvironment.executeSql("select auron_lifecycle_plus(`int`) from T1");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        result.await();

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of(2), Row.of(3), Row.of(3)));
        assertThat(LifecycleFunction.openCount.get())
                .as("open must run for the subtask")
                .isOne();
        assertThat(LifecycleFunction.closeCount.get())
                .as("close must run when the operator closes")
                .isOne();
    }

    /**
     * {@code open} runs once per subtask rather than once per drain cycle.
     *
     * <p>The operator drains when its exporter reports batch-full at 8192 buffered rows, and again
     * from {@code close()} for the remainder, so 10000 rows force at least two drain cycles. Each
     * cycle builds a fresh native runtime whose expression tree asks the JVM for the wrapper again,
     * so without per-subtask retention the wrapper would be rebuilt and {@code open} would run once
     * per cycle. The count is what separates the two.
     */
    @Test
    public void testOpenRunsOncePerSubtaskAcrossDrainCycles() throws Exception {
        LifecycleFunction.reset();
        environment.setParallelism(1);
        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            data.add(row(i));
        }
        tableEnvironment.executeSql("CREATE TABLE TWide ( `int` INT ) WITH ("
                + " 'connector' = 'values',"
                + " 'data-id' = '" + TestValuesTableFactory.registerData(data) + "',"
                + " 'failing-source' = 'false' )");
        tableEnvironment.createTemporarySystemFunction("auron_lifecycle_plus", LifecycleFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        TableResult result = tableEnvironment.executeSql("select auron_lifecycle_plus(`int`) from TWide");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        result.await();

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .hasSize(10000);
        assertThat(LifecycleFunction.openCount.get())
                .as("open ran per drain cycle instead of once for the subtask")
                .isOne();
        assertThat(LifecycleFunction.closeCount.get())
                .as("close must run when the operator closes")
                .isOne();
    }

    /**
     * Overriding {@code close} alone is admitted as well, and the hook still runs at operator close.
     * The two hooks are independent, so admitting one does not establish the other.
     */
    @Test
    public void testUdfOverridingCloseOnlyRunsNatively() throws Exception {
        ClosesFunction.closeCount.set(0);
        environment.setParallelism(1);
        tableEnvironment.createTemporarySystemFunction("auron_closes", ClosesFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        TableResult result = tableEnvironment.executeSql("select auron_closes(`int`) from T1");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        result.await();

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(Row.of(10), Row.of(20), Row.of(20)));
        assertThat(ClosesFunction.closeCount.get())
                .as("close must run when the operator closes")
                .isOne();
    }

    /**
     * Subtask isolation, end to end. The registry is scoped to a subtask precisely so two subtasks
     * of one operator never share a wrapper, and every other test pins parallelism to 1, where the
     * property cannot be observed.
     *
     * <p>Each subtask opens its own function, so the number of {@code open} calls and the number of
     * distinct instances that received one must agree. A shared wrapper would open once and be
     * evaluated by both subtasks, which shows up here as fewer opens than subtasks.
     */
    @Test
    public void testEachSubtaskGetsItsOwnFunctionInstance() throws Exception {
        ParallelLifecycleFunction.reset();
        // A values source is not parallel, and the Calc chains to it, so it would run on a single
        // subtask however high the environment parallelism is. A sequence source is parallel, which
        // is what puts several subtasks of the Calc in flight at once.
        environment.setParallelism(4);
        tableEnvironment.createTemporaryView(
                "TParallel", tableEnvironment.fromDataStream(environment.fromSequence(1, 8)));
        tableEnvironment.createTemporarySystemFunction("auron_parallel_plus", ParallelLifecycleFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        TableResult result = tableEnvironment.executeSql("select auron_parallel_plus(f0) from TParallel");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        result.await();

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingLong(o -> (long) o.getField(0)));
        assertThat(rows)
                .as("an empty result set means the plan converted but never executed natively")
                .isEqualTo(Arrays.asList(
                        Row.of(2L),
                        Row.of(3L),
                        Row.of(4L),
                        Row.of(5L),
                        Row.of(6L),
                        Row.of(7L),
                        Row.of(8L),
                        Row.of(9L)));
        assertThat(ParallelLifecycleFunction.openCount.get())
                .as("the operator must run at a parallelism where sharing could be observed")
                .isGreaterThan(1);
        assertThat(ParallelLifecycleFunction.instances.size())
                .as("subtasks shared a function instance")
                .isEqualTo(ParallelLifecycleFunction.openCount.get());
        assertThat(ParallelLifecycleFunction.closeCount.get())
                .as("every subtask must close the function it opened")
                .isEqualTo(ParallelLifecycleFunction.openCount.get());
    }

    /**
     * Two call sites of one function must not share a wrapper, and therefore must not share a
     * function instance.
     *
     * <p>The two calls carry byte-identical payloads — same function, same argument and return
     * types — because the arguments themselves travel outside the payload, so only the node
     * ordinal separates them. The function counts its own invocations from state {@code open}
     * initialises, which is what makes sharing produce a wrong answer rather than merely a shared
     * object: each call site evaluates the whole batch in turn, so one shared counter yields
     * {@code (1,4) (2,5) (3,6)} where two independent ones yield {@code (1,1) (2,2) (3,3)}.
     */
    @Test
    public void testTwoCallSitesOfOneFunctionDoNotShareInstanceState() throws Exception {
        environment.setParallelism(1);
        tableEnvironment.createTemporarySystemFunction("auron_call_count", CallCountingFunction.class);
        UnsupportedFlinkNodeRecorder.resetForTest();

        TableResult result =
                tableEnvironment.executeSql("select auron_call_count(`int`), auron_call_count(`int` * 2) from T1");
        List<Row> rows = CollectionUtil.iteratorToList(result.collect());
        result.await();

        assertThat(UnsupportedFlinkNodeRecorder.peekEmitCount())
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows)
                .as("the two call sites shared one function instance")
                .isEqualTo(Arrays.asList(Row.of(1, 1), Row.of(2, 2), Row.of(3, 3)));
    }

    /**
     * A function declaring two {@code eval} overloads both invokable for the call's argument runs
     * natively, and produces the overload Flink itself would have chosen.
     *
     * <p>{@code eval(int)} and {@code eval(Integer)} are both reachable for an {@code INT} argument
     * once autoboxing is allowed, so nothing in Auron can pick between them; the choice belongs to
     * the type inference that runs when Auron is not involved. Registering through
     * {@code createTemporarySystemFunction} is what puts that inference in play, and the value
     * itself names the overload that ran, so a divergence shows up as a wrong string rather than as
     * an equal-but-unverified answer.
     */
    @Test
    public void testCompetingEvalOverloadsResolveAsFlinkDoes() {
        tableEnvironment.createTemporarySystemFunction("auron_overloaded", OverloadedFunction.class);
        tableEnvironment.createTemporarySystemFunction("auron_second_of", SecondOfFunction.class);

        UnsupportedFlinkNodeRecorder.resetForTest();
        List<Row> nativeRows = collectSorted("select auron_overloaded(`int`) from T1");
        int nativeFallbacks = UnsupportedFlinkNodeRecorder.peekEmitCount();

        UnsupportedFlinkNodeRecorder.resetForTest();
        List<Row> flinkRows =
                collectFirstColumnSorted("select auron_overloaded(`int`), auron_second_of(`int`) from T1");
        int comparisonFallbacks = UnsupportedFlinkNodeRecorder.peekEmitCount();

        assertThat(nativeFallbacks)
                .as("a non-zero fallback count means the Calc did not run natively")
                .isZero();
        assertThat(comparisonFallbacks)
                .as("the comparison run recorded no fallback, the only signal available that it left the"
                        + " native path")
                .isNotZero();
        assertThat(nativeRows)
                .as("an empty result set means the plan converted but never executed natively")
                .hasSize(3);
        assertThat(nativeRows)
                .as("the native wrapper resolved a different overload than Flink's own Calc")
                .isEqualTo(flinkRows);
    }

    /**
     * A call mixing a column and a literal runs natively and produces the same values Flink's own
     * Calc does.
     *
     * <p>The generated invoker receives its arguments as a row, so every operand is rewritten to a
     * reference into that row and a literal operand stops being one by the time Flink generates the
     * call. Whether that matters is a property of the call, not of the rewrite, and this pins the
     * answer for the ordinary case.
     */
    @Test
    public void testLiteralArgumentRunsNatively() {
        tableEnvironment.createTemporarySystemFunction("auron_join", JoinFunction.class);
        tableEnvironment.createTemporarySystemFunction("auron_second_of", SecondOfFunction.class);

        UnsupportedFlinkNodeRecorder.resetForTest();
        List<Row> nativeRows = collectSorted("select auron_join(`name`, 'lit') from T1");
        int nativeFallbacks = UnsupportedFlinkNodeRecorder.peekEmitCount();

        UnsupportedFlinkNodeRecorder.resetForTest();
        List<Row> flinkRows =
                collectFirstColumnSorted("select auron_join(`name`, 'lit'), auron_second_of(`int`) from T1");
        int comparisonFallbacks = UnsupportedFlinkNodeRecorder.peekEmitCount();

        assertThat(nativeFallbacks)
                .as("a literal argument must not decline the call")
                .isZero();
        assertThat(comparisonFallbacks)
                .as("the comparison run recorded no fallback, the only signal available that it left the"
                        + " native path")
                .isNotZero();
        assertThat(nativeRows)
                .as("an empty result set means the plan converted but never executed natively")
                .hasSize(3);
        assertThat(nativeRows)
                .as("the literal argument reached the function as a different value than Flink passes")
                .isEqualTo(flinkRows);
    }

    /**
     * A NULL arriving at an {@code eval} declared over a primitive parameter fails the job, and
     * fails it inside the generated invoker for that function rather than by declining the call.
     *
     * <p>Flink's generated call unboxes the argument rather than short-circuiting on null, so such a
     * function is not null-safe over a nullable column, and the wrapper inherits that because it
     * runs the same generated call. The failure is the assertion: a wrapper that substituted the
     * primitive default would return rows here where Flink returns none, and a native plan with no
     * arm for this expression would return an empty result set rather than fail at all. The
     * companion run establishes that the query fails without the wrapper too, so the failure is
     * Flink's semantics rather than something the wrapper introduced.
     *
     * <p>The two failures cannot be compared by identity, and the assertions differ accordingly.
     * Crossing back from the native side, the JNI bridge carries the Java exception as the text of
     * {@code Throwable.toString()}, so the cause chain does not survive and only the wrapper's own
     * message — which names the function — is left to assert on. On Flink's own path the unboxing
     * raises before {@code eval} is entered, so the function is not a stack frame there and only the
     * root cause is left to assert on. Neither side can present the other's evidence.
     *
     * <p>This is not the only NULL coverage: a null argument reaching a null-tolerant function, and
     * a null result travelling back, are covered over the Arrow boundary by
     * {@code FlinkUDFCodeGeneratorTest#testNullArgumentAndNullResultCrossTheBoundary}.
     */
    @Test
    public void testPrimitiveParameterFedNullFailsInsideTheGeneratedInvoker() {
        String dataId = TestValuesTableFactory.registerData(Arrays.asList(row(1), row((Integer) null), row(3)));
        tableEnvironment.executeSql("CREATE TABLE TNullable ( `int` INT ) WITH ("
                + " 'connector' = 'values',"
                + " 'data-id' = '" + dataId + "',"
                + " 'failing-source' = 'false' )");
        tableEnvironment.createTemporarySystemFunction("auron_primitive_arg", PrimitiveArgFunction.class);
        tableEnvironment.createTemporarySystemFunction("auron_second_of", SecondOfFunction.class);

        UnsupportedFlinkNodeRecorder.resetForTest();
        Throwable nativeFailure =
                catchThrowable(() -> collectSorted("select auron_primitive_arg(`int`) from TNullable"));
        int nativeFallbacks = UnsupportedFlinkNodeRecorder.peekEmitCount();

        UnsupportedFlinkNodeRecorder.resetForTest();
        Throwable flinkFailure = catchThrowable(
                () -> collectSorted("select auron_primitive_arg(`int`), auron_second_of(`int`) from TNullable"));
        int comparisonFallbacks = UnsupportedFlinkNodeRecorder.peekEmitCount();

        assertThat(nativeFallbacks)
                .as("the call declined at plan time, so the job failed short of the native path")
                .isZero();
        assertThat(comparisonFallbacks)
                .as("the comparison run recorded no fallback, the only signal available that it left the"
                        + " native path")
                .isNotZero();
        assertThat(nativeFailure)
                .as("the wrapper absorbed a null Flink's own call does not survive")
                .isNotNull();
        assertThat(nativeFailure)
                .as("the job failed somewhere other than inside the wrapped function")
                .hasStackTraceContaining(PrimitiveArgFunction.class.getName());
        assertThat(flinkFailure)
                .as("the comparison run returned rows, so Flink's own generated call did not fail on the null")
                .isNotNull();
        assertThat(rootCauseOf(flinkFailure))
                .as("Flink's own generated call must fail on unboxing the null")
                .isInstanceOf(NullPointerException.class);
    }

    // ---- comparison-run helpers ----

    /**
     * Runs a query and returns its rows in a stable order.
     *
     * <p>The expectation of a comparison run is the second collection rather than a literal value,
     * so both sides go through this and neither depends on the order the sources interleaved in.
     */
    private List<Row> collectSorted(String sql) {
        List<Row> rows =
                CollectionUtil.iteratorToList(tableEnvironment.executeSql(sql).collect());
        rows.sort(Comparator.comparing(Row::toString));
        return rows;
    }

    /**
     * Runs a query and returns only its first column, in a stable order.
     *
     * <p>A comparison run appends a companion expression the converter declines, which takes the
     * whole Calc back to Flink's own generated code and leaves the expression under test evaluated
     * the way it would be without Auron. Dropping that companion column here is what makes the two
     * collections comparable.
     *
     * <p>Each row keeps its own kind rather than being rebuilt as an insertion, so that the
     * collection this returns differs from {@link #collectSorted} in its width alone. A comparison
     * that silently normalised the kind on one side could not see a change of kind as a difference.
     */
    private List<Row> collectFirstColumnSorted(String sql) {
        List<Row> rows =
                CollectionUtil.iteratorToList(tableEnvironment.executeSql(sql).collect());
        List<Row> firstColumn = new ArrayList<>(rows.size());
        for (Row row : rows) {
            firstColumn.add(Row.ofKind(row.getKind(), row.getField(0)));
        }
        firstColumn.sort(Comparator.comparing(Row::toString));
        return firstColumn;
    }

    private static Throwable rootCauseOf(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    // ---- UDF fixtures ----

    /** Joins two string arguments, so argument order is visible in the result. */
    public static class JoinFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Joins its arguments with a colon.
         *
         * @param left the first argument
         * @param right the second argument
         * @return {@code left + ":" + right}
         */
        public String eval(String left, String right) {
            return left + ":" + right;
        }
    }

    /**
     * Scales its argument by ten. Distinct from every other fixture, so a job calling it must
     * resolve a class no earlier job has loaded.
     */
    public static class TimesTenFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Scales its argument by ten.
         *
         * @param a the argument
         * @return {@code a * 10}
         */
        public Integer eval(Integer a) {
            return a * 10;
        }
    }

    /** Increments its argument. */
    public static class PlusOneFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

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

    /**
     * Fails on the value {@code 1}, so a query that reaches it on a row the surrounding expression
     * excludes fails the job instead of returning rows.
     */
    public static class ThrowsOnOneFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Scales its argument by ten, rejecting the value {@code 1}.
         *
         * @param a the argument
         * @return {@code a * 10}
         */
        public Integer eval(Integer a) {
            if (a != null && a == 1) {
                throw new IllegalStateException("auron_throws_on_one was evaluated on an excluded row");
            }
            return a * 10;
        }
    }

    /**
     * Returns a {@code TIMESTAMP}, which is outside the admitted type set, so a Calc whose
     * projection contains a call to it converts no further and runs on Flink's own generated code.
     * That is what makes it usable as the companion expression of a comparison run.
     *
     * <p>Null-safe, so that a comparison run over a column containing nulls fails only where the
     * expression under test fails.
     */
    public static class SecondOfFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Builds a timestamp whose second-of-minute is the argument.
         *
         * @param a the argument
         * @return the timestamp
         */
        public LocalDateTime eval(Integer a) {
            return LocalDateTime.of(2020, 1, 1, 0, 0, a == null ? 0 : a);
        }
    }

    /**
     * Overrides both lifecycle hooks and counts them statically, since the instance the job runs is
     * deserialized from the plan and never travels back here. {@code eval} depends on state only
     * {@code open} sets, so a missing {@code open} fails loudly instead of skewing a value.
     */
    public static class LifecycleFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        static final AtomicInteger openCount = new AtomicInteger();
        static final AtomicInteger closeCount = new AtomicInteger();

        private transient int offset;

        static void reset() {
            openCount.set(0);
            closeCount.set(0);
        }

        @Override
        public void open(FunctionContext context) {
            offset = 1;
            openCount.incrementAndGet();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        /**
         * Adds the offset established by {@code open}.
         *
         * @param a the argument
         * @return {@code a + 1}
         */
        public Integer eval(Integer a) {
            if (offset == 0) {
                throw new IllegalStateException("auron_lifecycle_plus was evaluated before open()");
            }
            return a == null ? null : a + offset;
        }
    }

    /** Overrides {@code close} only, so the two hooks are exercised independently. */
    public static class ClosesFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        static final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        /**
         * Scales its argument by ten.
         *
         * @param a the argument
         * @return {@code a * 10}
         */
        public Integer eval(Integer a) {
            return a == null ? null : a * 10;
        }
    }

    /**
     * Records the identity of every instance that is opened, so that two subtasks sharing one
     * wrapper is distinguishable from two subtasks each holding their own.
     */
    public static class ParallelLifecycleFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        static final AtomicInteger openCount = new AtomicInteger();
        static final AtomicInteger closeCount = new AtomicInteger();
        static final Set<Object> instances =
                Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

        static void reset() {
            openCount.set(0);
            closeCount.set(0);
            instances.clear();
        }

        @Override
        public void open(FunctionContext context) {
            instances.add(this);
            openCount.incrementAndGet();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }

        /**
         * Adds one to its argument.
         *
         * @param a the argument
         * @return {@code a + 1}
         */
        public Long eval(Long a) {
            return a == null ? null : a + 1;
        }
    }

    /**
     * Counts its own invocations from state {@code open} establishes, so that two call sites
     * sharing one instance is visible in the values rather than only in object identity.
     */
    public static class CallCountingFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        private transient int calls;

        @Override
        public void open(FunctionContext context) {
            calls = 0;
        }

        /**
         * Returns how many times this instance has been called, ignoring its argument.
         *
         * @param a the argument, which only fixes the call's type signature
         * @return the one-based invocation count
         */
        public Integer eval(Integer a) {
            return ++calls;
        }
    }

    /**
     * Declares two {@code eval} overloads both invokable for an {@code INT} argument, so which one
     * runs is decided by type inference rather than by anything Auron does. The returned value
     * names the overload that ran.
     */
    public static class OverloadedFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * The overload over a primitive parameter.
         *
         * @param a the argument
         * @return the argument, tagged with the overload that produced it
         */
        public String eval(int a) {
            return "primitive:" + a;
        }

        /**
         * The overload over a boxed parameter.
         *
         * @param a the argument
         * @return the argument, tagged with the overload that produced it
         */
        public String eval(Integer a) {
            return "boxed:" + a;
        }
    }

    /**
     * Declares {@code eval} over a primitive parameter while accepting a nullable argument. The
     * hint is what makes the call valid: extraction would otherwise derive a {@code NOT NULL}
     * argument type and a nullable column would be rejected before the query ran.
     */
    @FunctionHint(input = @DataTypeHint("INT"), output = @DataTypeHint("STRING"))
    public static class PrimitiveArgFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Renders its argument.
         *
         * @param a the argument
         * @return the argument's decimal form
         */
        public String eval(int a) {
            return "v=" + a;
        }
    }
}
