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
package org.apache.auron.flink.table.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * End-to-end IT cases for the graph-level fusion of a convertible Calc into the native Auron Kafka
 * source. The {@code AuronSourceCalcFusionProcessor} (installed by the shadowed planner factory)
 * counts each source's consumers over the whole exec-node graph and, for a sole-consumer native
 * source with no event-time watermark, stages the Calc's {@code Project[Filter?]} sub-plan onto the
 * source; the shadowed {@code StreamExecCalc} then emits no standalone operator and the source runs
 * the fused {@code Project[Filter?[KafkaScan]]} pipeline. Because the consumer count is computed
 * graph-level, fusion fires under the <em>default</em> optimizer config — no reuse-disable is needed.
 *
 * <p>Each assertion distinguishes fused from not-fused rather than only checking the row set (a row
 * set alone is identical either way). The discriminator is the job graph in the {@code
 * JSON_EXECUTION_PLAN}: a fused Calc collapses into the source node, so the graph contains <em>no</em>
 * {@code Calc} operator; an unfused Calc keeps a separate {@code Calc} operator above the source.
 */
public class AuronKafkaSourceMergeITCase extends AuronKafkaSourceTestBase {

    /** Matches a job-graph operator whose type is a Calc, e.g. {@code "type" : "Calc[4]"}. */
    private static final Pattern CALC_OPERATOR = Pattern.compile("\"type\"\\s*:\\s*\"Calc\\[");

    /**
     * Counts the standalone {@code Calc} operators in the job graph of {@code sql}. A fused Calc is
     * absorbed into the source, so it contributes zero; an unfused Calc contributes one per operator.
     */
    private int calcOperatorCount(String sql) {
        String plan = tableEnvironment.explainSql(sql, ExplainDetail.JSON_EXECUTION_PLAN);
        Matcher m = CALC_OPERATOR.matcher(plan);
        int count = 0;
        while (m.find()) {
            count++;
        }
        return count;
    }

    /**
     * Filter + projection over the no-watermark sole-consumer source {@code T5} fuses into the
     * native source under default config. The row set is correct (only ages 21 and 22 survive {@code
     * age > 20}, projected through {@code age + 1}), and the job graph carries no standalone {@code
     * Calc} operator — the Calc collapsed into the source. If fusion had not fired (e.g. the
     * graph-level consumer count or the default-config path regressed), a {@code Calc} operator would
     * remain and {@code calcOperatorCount} would be 1.
     */
    @Test
    public void testFilterAndProjectionFuseUnderDefaultConfig() {
        environment.setParallelism(1);
        String sql = "SELECT `age` + 1, `name` FROM T5 WHERE `age` > 20";

        assertThat(calcOperatorCount(sql))
                .as("fused Calc must collapse into the source, leaving no standalone Calc operator")
                .isEqualTo(0);

        List<Row> rows =
                CollectionUtil.iteratorToList(tableEnvironment.executeSql(sql).collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(22, "zm2"), Row.of(23, "zm1")));
    }

    /**
     * A directly-watermarked native source ({@code T2} declares {@code WATERMARK FOR ts}) must NOT
     * fuse: fusing a filter below the source's per-record watermark generator would hide
     * filtered-out records from it and stall event-time progress. The fusion processor's
     * watermark gate blocks fusion here, so the Calc stays a separate operator above the native
     * source. The source function's {@code open()}-time guard is the fail-fast backstop for that
     * gate: it throws {@link IllegalStateException} if a merged plan and a watermark ever coexist.
     *
     * <p>This assertion is gate-sensitive: it asserts the job graph keeps exactly one standalone
     * {@code Calc} operator. If the watermark gate were deleted, the watermarked source would fuse,
     * the Calc would collapse into the source, and {@code calcOperatorCount} would drop to 0 —
     * flipping this test. The row set is also verified correct so the unfused path still works.
     */
    @Test
    public void testCalcOverWatermarkedSourceDoesNotFuse() {
        environment.setParallelism(1);
        String sql = "SELECT `age` + 1, `name` FROM T2 WHERE `age` > 20";

        assertThat(calcOperatorCount(sql))
                .as("a watermarked source must not fuse; the Calc must remain a separate operator")
                .isEqualTo(1);

        List<Row> rows =
                CollectionUtil.iteratorToList(tableEnvironment.executeSql(sql).collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(22, "zm2"), Row.of(23, "zm1")));
    }

    /**
     * Under the default optimizer config, source reuse shares one source operator between the two
     * Calcs of a {@code UNION ALL} over the same table, so the source's consumer count is 2. The
     * fusion processor must not fuse a multi-consumer source — staging one Calc's plan onto the
     * shared source would corrupt the other consumer. The processor's sole-consumer gate (count == 1)
     * blocks fusion, so both Calcs stay separate operators and the combined row set is correct.
     *
     * <p>This assertion is gate-sensitive: it asserts the job graph keeps both standalone {@code
     * Calc} operators (count == 2). If the consumer-count gate were broken so a shared source fused,
     * a Calc would collapse and the count would drop below 2. The row set ({@code WHERE age > 20}
     * yields ages 21, 22; {@code WHERE age > 10} yields 20, 21, 22 projected to 21, 22, 23) is also
     * verified, which is the regression the earlier in-Calc approach got wrong (it produced 6 rows
     * instead of 5 by fusing into the shared source).
     */
    @Test
    public void testSharedSourceUnionAllDoesNotFuseUnderDefaultReuse() {
        environment.setParallelism(1);
        String sql = "SELECT `age` FROM T5 WHERE `age` > 20 " + "UNION ALL SELECT `age` + 1 FROM T5 WHERE `age` > 10";

        assertThat(calcOperatorCount(sql))
                .as("a shared (multi-consumer) source must not fuse; both Calcs must remain operators")
                .isEqualTo(2);

        List<Row> rows =
                CollectionUtil.iteratorToList(tableEnvironment.executeSql(sql).collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(21), Row.of(21), Row.of(22), Row.of(22), Row.of(23)));
    }
}
