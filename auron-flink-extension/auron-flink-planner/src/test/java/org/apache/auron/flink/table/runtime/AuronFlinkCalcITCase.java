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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * IT case for Flink Calc Operator on Auron.
 */
public class AuronFlinkCalcITCase extends AuronFlinkTableTestBase {

    @Test
    public void testPlus() {
        List<Row> rows = CollectionUtil.iteratorToList(
                tableEnvironment.executeSql("select `int` + 1 from T1").collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2), Row.of(3), Row.of(3)));
    }

    /** An equality filter keeps only rows whose value matches. */
    @Test
    public void testFilterEquals() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` = 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1)));
    }

    /** A not-equals filter drops rows whose value matches. */
    @Test
    public void testFilterNotEquals() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` <> 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2), Row.of(2)));
    }

    /** A greater-than filter keeps only rows strictly above the bound. */
    @Test
    public void testFilterGreaterThan() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` > 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2), Row.of(2)));
    }

    /** A less-than filter keeps only rows strictly below the bound. */
    @Test
    public void testFilterLessThan() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` < 2")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1)));
    }

    /** A greater-or-equal filter keeps rows at or above the bound. */
    @Test
    public void testFilterGreaterEqual() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` >= 1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2), Row.of(2)));
    }

    /** A less-or-equal filter keeps rows at or below the bound. */
    @Test
    public void testFilterLessEqual() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` <= 2")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2), Row.of(2)));
    }

    /** A comparison used directly in the projection yields a Boolean column per row. */
    @Test
    public void testComparisonInBooleanProjection() {
        List<Row> rows = CollectionUtil.iteratorToList(
                tableEnvironment.executeSql("select `int` > 1 from T1").collect());
        rows.sort(Comparator.comparing(o -> (Boolean) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(false), Row.of(true), Row.of(true)));
    }

    /** A filter comparing an INT column against a DOUBLE column exercises INT-to-DOUBLE operand
     * promotion; no row has int greater than double, so the result is empty. */
    @Test
    public void testMixedTypePromotionFilter() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where `int` > `double`")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Collections.emptyList());
    }

    /** A LIKE filter keeps rows whose string matches the pattern. */
    @Test
    public void testFilterLike() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `string` from T1 where `string` LIKE 'Comment%'")
                .collect());
        rows.sort(Comparator.comparing(o -> (String) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of("Comment#1"), Row.of("Comment#1")));
    }

    /**
     * A string that cannot be parsed as INT under TRY_CAST resolves to NULL via the native
     * try-cast path instead of failing the query, confirming the converter routes the
     * TRY_CAST operator to the null-on-failure native node end to end.
     */
    @Test
    public void testTryCastUnparseableStringToInt() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select try_cast(`string` as INT) from T1")
                .collect());
        // Every `string` value ("Hi", "Comment#1") is non-numeric → all NULL.
        assertThat(rows).isEqualTo(Arrays.asList(Row.of((Object) null), Row.of((Object) null), Row.of((Object) null)));
    }

    /** A valid numeric-to-numeric CAST converts to the strict native cast node and yields the
     * widened values. */
    @Test
    public void testCastIntToDouble() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select cast(`int` as DOUBLE) from T1")
                .collect());
        rows.sort(Comparator.comparingDouble(o -> (double) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1d), Row.of(2d), Row.of(2d)));
    }

    /** A string-to-numeric CAST over a parseable per-row string converts to the strict native cast
     * node and yields the parsed values. */
    @Test
    public void testCastStringToInt() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select cast(cast(`int` as STRING) as INT) from T1")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2), Row.of(2)));
    }

    /** A cast to an unsupported target type (TIMESTAMP) is gated to Flink fallback and
     * still produces the correct row set. */
    @Test
    public void testCastToUnsupportedTypeFallsBack() {
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("select `int` from T1 where cast(`ts` as TIMESTAMP) is not null")
                .collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(1), Row.of(2), Row.of(2)));
    }
}
