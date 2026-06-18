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

import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.HashMap;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AuronKafkaDynamicTableSource}. */
class AuronKafkaDynamicTableSourceTest {

    private static AuronKafkaDynamicTableSource newSource() {
        DataType physicalDataType = DataTypes.ROW(DataTypes.FIELD("int", DataTypes.INT()));
        return new AuronKafkaDynamicTableSource(
                physicalDataType, "topic", new Properties(), "Json", new HashMap<>(), 100, "EARLIEST", null, -1);
    }

    @Test
    void testCopyPreservesAppliedWatermarkStrategy() {
        AuronKafkaDynamicTableSource source = newSource();
        WatermarkStrategy<RowData> strategy = WatermarkStrategy.forMonotonousTimestamps();
        source.applyWatermark(strategy);

        DynamicTableSource copy = source.copy();

        assertSame(strategy, ((AuronKafkaDynamicTableSource) copy).watermarkStrategy);
    }
}
