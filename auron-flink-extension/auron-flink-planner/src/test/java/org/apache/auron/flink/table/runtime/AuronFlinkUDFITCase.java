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

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.auron.flink.table.planner.UnsupportedFlinkNodeRecorder;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * IT case for user-defined scalar functions running inside a native Flink Calc on Auron.
 *
 * <p>Every case asserts the fallback-recorder emit count <em>and</em> the row count. A zero emit
 * count only proves the Calc converted: a native library with no arm for the plan converts cleanly
 * and yields an empty result set with the counter still at zero, and assertions over an empty list
 * pass vacuously. The row count is what establishes that the native plan actually executed.
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

    /** Returns a {@code TIMESTAMP}, which is outside the admitted type set. */
    public static class SecondOfFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        /**
         * Builds a timestamp whose second-of-minute is the argument.
         *
         * @param a the argument
         * @return the timestamp
         */
        public LocalDateTime eval(Integer a) {
            return LocalDateTime.of(2020, 1, 1, 0, 0, a);
        }
    }
}
