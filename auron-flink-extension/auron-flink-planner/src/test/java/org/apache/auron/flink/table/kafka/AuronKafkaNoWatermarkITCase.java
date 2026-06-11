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

import java.util.List;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * Regression IT for an Auron Kafka source without an event-time watermark strategy.
 * The source must emit all records on the no-watermark path; a source that only marks
 * itself running when a watermark strategy is present would produce an empty result.
 */
public class AuronKafkaNoWatermarkITCase extends AuronKafkaSourceTestBase {

    @Test
    public void testNoWatermarkSourceEmitsAllRows() {
        environment.setParallelism(1);
        List<Row> rows = CollectionUtil.iteratorToList(
                tableEnvironment.executeSql("SELECT * FROM T5").collect());
        assertThat(rows).extracting(row -> row.getField(1)).containsExactlyInAnyOrder(20, 21, 22);
    }
}
