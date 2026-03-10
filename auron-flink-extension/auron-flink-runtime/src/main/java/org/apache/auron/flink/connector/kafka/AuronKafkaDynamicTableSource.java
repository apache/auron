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

import java.util.Map;
import java.util.UUID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
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
    private final Map<String, String> formatConfig;
    private final int bufferSize;
    private final String startupMode;

    public AuronKafkaDynamicTableSource(
            DataType physicalDataType,
            String kafkaTopic,
            String kafkaPropertiesJson,
            String format,
            Map<String, String> formatConfig,
            int bufferSize,
            String startupMode) {
        final LogicalType physicalType = physicalDataType.getLogicalType();
        Preconditions.checkArgument(physicalType.is(LogicalTypeRoot.ROW), "Row data type expected.");
        this.physicalDataType = physicalDataType;
        this.kafkaTopic = kafkaTopic;
        this.kafkaPropertiesJson = kafkaPropertiesJson;
        this.format = format;
        this.formatConfig = formatConfig;
        this.bufferSize = bufferSize;
        this.startupMode = startupMode;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext scanContext) {
        String auronOperatorId = "AuronKafkaSource-" + UUID.randomUUID().toString();
        AuronKafkaSourceFunction sourceFunction = new AuronKafkaSourceFunction(
                physicalDataType.getLogicalType(),
                auronOperatorId,
                kafkaTopic,
                kafkaPropertiesJson,
                format,
                formatConfig,
                bufferSize,
                startupMode);
        return new DataStreamScanProvider() {

            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                return execEnv.addSource(sourceFunction);
            }

            @Override
            public boolean isBounded() {
                return false;
            }
        };
    }

    @Override
    public DynamicTableSource copy() {
        return new AuronKafkaDynamicTableSource(
                physicalDataType, kafkaTopic, kafkaPropertiesJson, format, formatConfig, bufferSize, startupMode);
    }

    @Override
    public String asSummaryString() {
        return "Auron Kafka Dynamic Table Source";
    }
}
