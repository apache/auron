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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

/**
 * A {@link DynamicTableSource} for Auron Kafka.
 */
public class AuronKafkaDynamicTableSource implements ScanTableSource {

    private final DataType physicalDataType;
    private final String kafkaTopic;
    private final String kafkaPropertiesJson;
    private final String format;
    private final String formatConfigJson;
    private final int bufferSize;
    private final String startupMode;
    private final String nestedColsMappingJson;

    public AuronKafkaDynamicTableSource(
            DataType physicalDataType,
            String kafkaTopic,
            String kafkaPropertiesJson,
            String format,
            String formatConfigJson,
            int bufferSize,
            String startupMode,
            String nestedColsMappingJson) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        this.physicalDataType = physicalDataType;
        this.kafkaTopic = kafkaTopic;
        this.kafkaPropertiesJson = kafkaPropertiesJson;
        this.format = format;
        this.formatConfigJson = formatConfigJson;
        this.bufferSize = bufferSize;
        this.startupMode = startupMode;
        this.nestedColsMappingJson = nestedColsMappingJson;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        return null;
    }

    @Override
    public DynamicTableSource copy() {
        return null;
    }

    @Override
    public String asSummaryString() {
        return "Auron Kafka Dynamic Table Source";
    }
}
