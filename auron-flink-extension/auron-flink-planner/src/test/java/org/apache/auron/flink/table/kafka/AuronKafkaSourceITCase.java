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

import java.time.LocalDateTime;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * IT case for Auron Flink Kafka Source.
 */
public class AuronKafkaSourceITCase extends AuronKafkaSourceTestBase {

    @Test
    public void testEventTimeTumbleTvfWindow() {
        environment.setParallelism(1);
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql(
                        "SELECT `name`, count(1), window_start FROM TABLE("
                                + "TUMBLE(TABLE T2, DESCRIPTOR(`ts`), INTERVAL '1' MINUTE)) GROUP BY `name`, window_start, window_end")
                .collect());
        assertThat(rows.size()).isEqualTo(3);
        assertRowsContains(
                rows,
                new Object[] {"zm1", 1L, LocalDateTime.parse("2026-03-16T12:03:00")},
                new Object[] {"zm2", 1L, LocalDateTime.parse("2026-03-16T12:03:00")},
                new Object[] {"zm1", 1L, LocalDateTime.parse("2026-03-16T12:05:00")});
    }

    @Test
    public void testEventTimeTumbleGroupWindow() {
        environment.setParallelism(1);
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("SELECT `name`, count(1), TUMBLE_START(`ts`, INTERVAL '1' MINUTE) "
                        + "FROM T2 group by TUMBLE(`ts`, INTERVAL '1' MINUTE), `name`")
                .collect());
        assertThat(rows.size()).isEqualTo(3);
        assertRowsContains(
                rows,
                new Object[] {"zm1", 1L, LocalDateTime.parse("2026-03-16T12:03:00")},
                new Object[] {"zm2", 1L, LocalDateTime.parse("2026-03-16T12:03:00")},
                new Object[] {"zm1", 1L, LocalDateTime.parse("2026-03-16T12:05:00")});
    }

    @Test
    public void testEventTimeTumbleTvfWindowWithIdle() {
        environment.setParallelism(1);
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql(
                        "SELECT `name`, count(1), window_start FROM TABLE("
                                + "TUMBLE(TABLE T3, DESCRIPTOR(`ts`), INTERVAL '1' MINUTE)) GROUP BY `name`, window_start, window_end")
                .collect());
        assertThat(rows.size()).isEqualTo(3);
        assertRowsContains(
                rows,
                new Object[] {"zm1", 1L, LocalDateTime.parse("2026-03-16T12:03:00")},
                new Object[] {"zm2", 1L, LocalDateTime.parse("2026-03-16T12:03:00")},
                new Object[] {"zm1", 1L, LocalDateTime.parse("2026-03-16T12:05:00")});
    }

    /**
     * Test per-partition watermark alignment with multi-partition data.
     * T4 has 2 partitions: partition 0's data appears first in batch (event times 12:03, 12:04, 12:05),
     * then partition 1's data (event times 12:03, 12:04, 12:05).
     * Without per-partition watermark, partition 0 would push the watermark to 12:05,
     * causing partition 1's 12:03 and 12:04 data to be treated as late and dropped.
     * With per-partition watermark (min across partitions), all 6 records should be preserved.
     */
    @Test
    public void testMultiPartitionWatermarkAlignment() {
        environment.setParallelism(1);
        List<Row> rows = CollectionUtil.iteratorToList(tableEnvironment
                .executeSql("SELECT count(1), window_start FROM TABLE("
                        + "TUMBLE(TABLE T4, DESCRIPTOR(`ts`), INTERVAL '1' MINUTE)) GROUP BY window_start, window_end")
                .collect());
        // 3 windows (12:03, 12:04, 12:05), each with 2 records (one from each partition)
        assertThat(rows.size()).isEqualTo(3);
        assertRowsContains(
                rows,
                new Object[] {2L, LocalDateTime.parse("2026-03-16T12:03:00")},
                new Object[] {2L, LocalDateTime.parse("2026-03-16T12:04:00")},
                new Object[] {2L, LocalDateTime.parse("2026-03-16T12:05:00")});
    }
}
