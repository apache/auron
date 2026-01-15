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
package org.apache.auron.flink.table;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for Flink Table Tests.
 */
public class AuronFlinkTableTestBase {

    protected StreamExecutionEnvironment environment;
    protected StreamTableEnvironment tableEnvironment;

    @BeforeEach
    public void before() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        tableEnvironment =
                StreamTableEnvironment.create(environment, EnvironmentSettings.fromConfiguration(configuration));
        String timestampDataId = TestValuesTableFactory.registerData(Arrays.asList(
                row("2020-10-10 00:00:01", 1, 1d, 1f, new BigDecimal("1.11"), "Hi", "a"),
                row("2020-10-10 00:00:02", 2, 2d, 2f, new BigDecimal("2.22"), "Comment#1", "a"),
                row("2020-10-10 00:00:03", 2, 2d, 2f, new BigDecimal("2.22"), "Comment#1", "a")));
        tableEnvironment.executeSql(" CREATE TABLE T1 ( "
                + "\n `ts` String, "
                + "\n `int` INT, "
                + "\n `double` DOUBLE, "
                + "\n `float` FLOAT, "
                + "\n `bigdec` DECIMAL(10, 2), "
                + "\n `string` STRING, "
                + "\n `name` STRING "
                + "\n ) WITH ( "
                + "\n 'connector' = 'values',"
                + "\n 'data-id' = '" + timestampDataId + "',"
                + "\n 'failing-source' = 'false' "
                + "\n )");
    }

    protected Row row(Object... values) {
        Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;
    }
}
