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
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;

/**
 * Base class for Auron Flink Kafka Table Tests.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AuronKafkaSourceTestBase {
    protected StreamExecutionEnvironment environment;
    protected StreamTableEnvironment tableEnvironment;

    @BeforeAll
    public void before() {
        Configuration configuration = new Configuration();
        // TODO Resolving the issue where the Flink classloader is closed and CompileUtils.doCompile fails
        configuration.setString("classloader.check-leaked-classloader", "false");
        // set time zone to UTC
        configuration.setString("table.local-time-zone", "UTC");
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        environment.setRestartStrategy(RestartStrategies.noRestart());
        environment.getConfig().setAutoWatermarkInterval(1);
        tableEnvironment =
                StreamTableEnvironment.create(environment, EnvironmentSettings.fromConfiguration(configuration));
        String jsonArray = "["
                + "{\"serialized_kafka_records_partition\": 1, \"serialized_kafka_records_offset\": 100000, "
                + "\"serialized_kafka_records_timestamp\": 1773662603760, \"event_time\": 1773662603760, \"age\": 20, \"name\":\"zm1\"},"
                + "{\"serialized_kafka_records_partition\": 1, \"serialized_kafka_records_offset\": 100001, "
                + "\"serialized_kafka_records_timestamp\": 1773662603761, \"event_time\": 1773662633760, \"age\": 21, \"name\":\"zm2\"},"
                + "{\"serialized_kafka_records_partition\": 1, \"serialized_kafka_records_offset\": 100002, "
                + "\"serialized_kafka_records_timestamp\": 1773662603762, \"event_time\": 1773662703761, \"age\": 22, \"name\":\"zm1\"}"
                + "]";
        tableEnvironment.executeSql(" CREATE TABLE T2 ( "
                + "\n `event_time` BIGINT, "
                + "\n `age` INT, "
                + "\n `name` STRING,"
                + "\n `ts` AS TO_TIMESTAMP(FROM_UNIXTIME(event_time / 1000)),"
                + "\n WATERMARK FOR `ts` AS `ts` "
                + "\n ) WITH ( "
                + "\n 'connector' = 'auron-kafka',"
                + "\n 'kafka.mock.data' = '" + jsonArray + "',"
                + "\n 'topic' = 'mock_topic',"
                + "\n 'properties.bootstrap.servers' = '127.0.0.1:9092',"
                + "\n 'properties.group.id' = 'flink-test-mock',"
                + "\n 'format' = 'JSON' "
                + "\n )");
    }

    protected void assertRowsContains(List<Row> actualRows, Object[]... expectedRows) {
        for (Object[] expected : expectedRows) {
            boolean found = actualRows.stream().anyMatch(row -> {
                for (int i = 0; i < expected.length; i++) {
                    Object actual = row.getField(i);
                    if (!java.util.Objects.equals(expected[i], actual)) {
                        return false;
                    }
                }
                return true;
            });
            assertThat(found)
                    .as("Expected row %s not found in actual rows: %s", Arrays.toString(expected), actualRows)
                    .isTrue();
        }
    }
}
