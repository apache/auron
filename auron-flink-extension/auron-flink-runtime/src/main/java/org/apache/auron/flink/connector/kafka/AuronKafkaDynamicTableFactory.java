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

import static org.apache.auron.flink.connector.kafka.KafkaConstants.*;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DynamicTableSourceFactory} for creating configured instances of {@link AuronKafkaDynamicTableSource}.
 */
public class AuronKafkaDynamicTableFactory implements DynamicTableSourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(AuronKafkaDynamicTableFactory.class);
    public static final String IDENTIFIER = "auron-kafka";
    private static final String PROPERTIES_PREFIX = "properties.";

    private static final ObjectMapper mapper = new ObjectMapper();

    public static final ConfigOption<String> PROPS_BOOTSTRAP_SERVERS = ConfigOptions.key("properties.bootstrap.servers")
            .stringType()
            .noDefaultValue()
            .withDescription("Required Kafka server connection string");
    public static final ConfigOption<String> TOPIC = ConfigOptions.key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("The topic to read from or write to.");
    public static final ConfigOption<String> PROPS_GROUP_ID = ConfigOptions.key("properties.group.id")
            .stringType()
            .noDefaultValue()
            .withDescription("Required consumer group in Kafka consumer, no need for Kafka producer");
    public static final ConfigOption<String> PB_DESC_FILE = ConfigOptions.key("pb.desc.filename")
            .stringType()
            .noDefaultValue()
            .withDescription("The filename of the pb descriptor file.");

    public static final ConfigOption<String> PB_ROOT_MESSAGE_NAME = ConfigOptions.key("pb.root.message")
            .stringType()
            .noDefaultValue()
            .withDescription("The root message name of the pb.");

    public static final ConfigOption<Integer> BUFFER_SIZE = ConfigOptions.key("buffer.size")
            .intType()
            .defaultValue(3000)
            .withDescription("kafka message records buffer size.");

    public static final ConfigOption<String> NESTED_COLS_FIELD_MAPPING = ConfigOptions.key("nested.cols.field.mapping")
            .stringType()
            .defaultValue("{}")
            .withDescription(
                    "When certain fields within complex nested structures need to be used at the top level of a Flink Table, this configuration must be specified."
                            + "The JSON key corresponds to the field name in the Flink table, while the value represents nested fields using the a.b.c notation.");

    public static final ConfigOption<String> PB_SKIP_FIELDS = ConfigOptions.key("pb.skip.fields")
            .stringType()
            .defaultValue("")
            .withDescription("Protobuf fields to skip when deserializing. The format is: field1,field2,field3");

    public static final ConfigOption<String> START_UP_MODE = ConfigOptions.key("start-up.mode")
            .stringType()
            .defaultValue("GROUP_OFFSET")
            .withDescription(
                    "offset mode for kafka source, support GROUP_OFFSET, LATEST, EARLIEST, TIMESTAMP will be supported.");

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig tableOptions = helper.getOptions();
        try {
            Map<String, String> formatConfig = new HashMap<>();
            String format = tableOptions.getOptional(FactoryUtil.FORMAT).get();
            formatConfig.put(KAFKA_PB_FORMAT_NESTED_COL_MAPPING_FIELD, tableOptions.get(NESTED_COLS_FIELD_MAPPING));
            if (KAFKA_FORMAT_PROTOBUF.equalsIgnoreCase(format)) {
                formatConfig.put(KAFKA_PB_FORMAT_PB_DESC_FILE_FIELD, tableOptions.get(PB_DESC_FILE));
                formatConfig.put(KAFKA_PB_FORMAT_ROOT_MESSAGE_NAME_FIELD, tableOptions.get(PB_ROOT_MESSAGE_NAME));
                formatConfig.put(KAFKA_PB_FORMAT_SKIP_FIELDS_FIELD, tableOptions.get(PB_SKIP_FIELDS));
            }
            return new AuronKafkaDynamicTableSource(
                    context.getCatalogTable().getSchema().toPhysicalRowDataType(),
                    tableOptions.get(TOPIC),
                    getKafkaProperties(context.getCatalogTable().getOptions()),
                    format,
                    formatConfig,
                    tableOptions.get(BUFFER_SIZE),
                    tableOptions.get(START_UP_MODE));
        } catch (Exception e) {
            throw new FlinkRuntimeException("Could not create Auron Kafka dynamic table source", e);
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PROPS_BOOTSTRAP_SERVERS);
        options.add(PROPS_GROUP_ID);
        options.add(TOPIC);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        options.add(TOPIC);
        options.add(PROPS_GROUP_ID);
        options.add(PROPS_BOOTSTRAP_SERVERS);
        options.add(PB_DESC_FILE);
        options.add(PB_ROOT_MESSAGE_NAME);
        options.add(BUFFER_SIZE);
        options.add(NESTED_COLS_FIELD_MAPPING);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Stream.of(PROPS_BOOTSTRAP_SERVERS, PROPS_GROUP_ID, TOPIC).collect(Collectors.toSet());
    }

    public static Properties getKafkaProperties(Map<String, String> tableOptions) {
        final Properties kafkaProperties = new Properties();

        if (hasKafkaClientProperties(tableOptions)) {
            tableOptions.keySet().stream()
                    .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                    .forEach(key -> {
                        final String value = tableOptions.get(key);
                        final String subKey = key.substring((PROPERTIES_PREFIX).length());
                        if (KAFKA_PROPERTIES_WHITE_LIST.contains(subKey)) {
                            kafkaProperties.put(subKey, value);
                        }
                    });
        }
        return kafkaProperties;
    }

    /**
     * Decides if the table options contains Kafka client properties that start with prefix
     * 'properties'.
     */
    private static boolean hasKafkaClientProperties(Map<String, String> tableOptions) {
        return tableOptions.keySet().stream().anyMatch(k -> k.startsWith(PROPERTIES_PREFIX));
    }
}
