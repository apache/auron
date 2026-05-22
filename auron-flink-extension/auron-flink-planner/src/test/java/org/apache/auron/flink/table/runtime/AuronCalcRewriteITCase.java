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

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * End-to-end IT cases for the shadowed {@code StreamExecCalc}. Each test submits a real SQL job
 * through {@link org.apache.flink.table.api.bridge.java.StreamTableEnvironment} over the {@code T1}
 * table registered in {@link AuronFlinkTableTestBase} and asserts the final row set is correct
 * regardless of whether the Calc executed natively or fell back to Flink's codegen.
 */
public class AuronCalcRewriteITCase extends AuronFlinkTableTestBase {

    /** Multi-column arithmetic projection exercises the projection loop with more than one
     * convertible expression. */
    @Test
    public void testMultiColumnArithmeticProjection() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` + 1, `int` * 2 from T1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2, 2), Row.of(3, 4), Row.of(3, 4)));
    }

    /** A filter-plus-projection Calc whose condition uses a not-yet-supported comparison operator
     * falls back to Flink's codegen path. Asserts the job still produces correct results; the
     * Auron-side {@code Filter[FFIReader]} plan-shape coverage will be added when a
     * predicate-returning converter lands. */
    @Test
    public void testFilterAndProjectEndToEnd() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` * 2 from T1 where `int` > 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(4), Row.of(4)));
    }

    /** Unsupported expression (a string function not in the converter set) triggers silent
     * fallback. The job must still complete and emit the correct rows. */
    @Test
    public void testFallbackOnUnsupportedExprStillExecutes() {
        List<Row> rows = CollectionUtil.iteratorToList(
                tableEnvironment.executeSql("select UPPER(`string`) from T1").collect());
        rows.sort(Comparator.comparing(o -> (String) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of("COMMENT#1"), Row.of("COMMENT#1"), Row.of("HI")));
    }

    /** A job containing two Calcs — one whose expressions are all converter-supported and one
     * that uses an unsupported function — must run end-to-end and emit the correct union of rows.
     * This asserts the job-level correctness contract; observability of which Calc fell back is
     * surfaced through the per-fallback WARN log rather than the test's value assertion. */
    @Test
    public void testMixedSupportedAndUnsupportedCalcs() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` + 1 from T1 union all select CHAR_LENGTH(`string`) from T1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2), Row.of(2), Row.of(3), Row.of(3), Row.of(9), Row.of(9)));
    }
}
