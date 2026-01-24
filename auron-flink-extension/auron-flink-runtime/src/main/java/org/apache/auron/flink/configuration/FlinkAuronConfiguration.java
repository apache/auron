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
package org.apache.auron.flink.configuration;

import java.util.Optional;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;

/**
 * Flink configuration wrapper for Auron.
 * All configuration keys are prefixed with "table.exec." to follow Flink's table config convention.
 */
public class FlinkAuronConfiguration extends AuronConfiguration {

    /**
     * Configuration prefix for all Auron settings in Flink.
     * Following Flink's convention for table execution configuration.
     */
    public static final String FLINK_PREFIX = "table.exec.";

    // Flink-specific Auron configuration options

    /**
     * Master switch to enable/disable Auron native execution.
     */
    public static final org.apache.flink.configuration.ConfigOption<Boolean> AURON_ENABLE =
            org.apache.flink.configuration.ConfigOptions.key("table.exec.auron.enable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable Auron native execution for batch queries.");

    /**
     * Enable Auron for Parquet scan operations.
     */
    public static final org.apache.flink.configuration.ConfigOption<Boolean> AURON_ENABLE_SCAN =
            org.apache.flink.configuration.ConfigOptions.key("table.exec.auron.enable.scan")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable Auron native execution for Parquet scan operations.");

    /**
     * Enable Auron for projection operations.
     */
    public static final org.apache.flink.configuration.ConfigOption<Boolean> AURON_ENABLE_PROJECT =
            org.apache.flink.configuration.ConfigOptions.key("table.exec.auron.enable.project")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable Auron native execution for projection operations.");

    /**
     * Enable Auron for filter operations.
     */
    public static final org.apache.flink.configuration.ConfigOption<Boolean> AURON_ENABLE_FILTER =
            org.apache.flink.configuration.ConfigOptions.key("table.exec.auron.enable.filter")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Enable Auron native execution for filter operations.");

    /**
     * Batch size for Arrow batches in Auron execution.
     */
    public static final org.apache.flink.configuration.ConfigOption<Integer> AURON_BATCH_SIZE =
            org.apache.flink.configuration.ConfigOptions.key("table.exec.auron.batch-size")
                    .intType()
                    .defaultValue(8192)
                    .withDescription("Suggested batch size for Arrow batches in Auron execution.");

    /**
     * Memory fraction for Auron native execution.
     */
    public static final org.apache.flink.configuration.ConfigOption<Double> AURON_MEMORY_FRACTION =
            org.apache.flink.configuration.ConfigOptions.key("table.exec.auron.memory-fraction")
                    .doubleType()
                    .defaultValue(0.7)
                    .withDescription("Fraction of managed memory to use for Auron native execution.");

    /**
     * Log level for Auron native execution.
     */
    public static final org.apache.flink.configuration.ConfigOption<String> AURON_LOG_LEVEL =
            org.apache.flink.configuration.ConfigOptions.key("table.exec.auron.log-level")
                    .stringType()
                    .defaultValue("INFO")
                    .withDescription("Log level for Auron native execution (TRACE, DEBUG, INFO, WARN, ERROR).");

    private final Configuration flinkConfig;

    /**
     * Creates a new FlinkAuronConfiguration wrapping a Flink Configuration.
     *
     * @param config The Flink Configuration to wrap.
     */
    public FlinkAuronConfiguration(Configuration config) {
        if (config == null) {
            throw new IllegalArgumentException("Flink configuration cannot be null");
        }
        this.flinkConfig = config;
    }

    /**
     * Gets a configuration value by ConfigOption.
     * Automatically adds the "table.exec." prefix if not already present.
     *
     * @param option The configuration option to retrieve.
     * @param <T> The type of the configuration value.
     * @return An Optional containing the configured value, or empty if not set.
     */
    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        String key = option.key();
        if (!key.startsWith(FLINK_PREFIX)) {
            key = FLINK_PREFIX + key;
        }
        return getOptionalByKey(key, option.defaultValue());
    }

    /**
     * Gets a configuration value by key string.
     * Automatically adds the "table.exec." prefix if not already present.
     *
     * @param key The configuration key to retrieve.
     * @param <T> The type of the configuration value.
     * @return An Optional containing the configured value, or empty if not set.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getOptional(String key) {
        if (!key.startsWith(FLINK_PREFIX)) {
            key = FLINK_PREFIX + key;
        }
        return getOptionalByKey(key, null);
    }

    /**
     * Internal helper to get configuration value by key with default fallback.
     *
     * @param key The full configuration key.
     * @param defaultValue The default value to use if not configured.
     * @param <T> The type of the configuration value.
     * @return An Optional containing the value, or empty if not set and no default provided.
     */
    @SuppressWarnings("unchecked")
    private <T> Optional<T> getOptionalByKey(String key, T defaultValue) {
        if (!flinkConfig.containsKey(key)) {
            return defaultValue != null ? Optional.of(defaultValue) : Optional.empty();
        }

        // Determine type based on default value and retrieve accordingly
        if (defaultValue == null) {
            // No default, return as string
            String value = flinkConfig.getString(key, null);
            return value != null ? Optional.of((T) value) : Optional.empty();
        }

        // Type-safe retrieval based on default value type
        Object value;
        if (defaultValue instanceof Integer) {
            value = flinkConfig.getInteger(key, (Integer) defaultValue);
        } else if (defaultValue instanceof Long) {
            value = flinkConfig.getLong(key, (Long) defaultValue);
        } else if (defaultValue instanceof Boolean) {
            value = flinkConfig.getBoolean(key, (Boolean) defaultValue);
        } else if (defaultValue instanceof Float) {
            value = flinkConfig.getFloat(key, (Float) defaultValue);
        } else if (defaultValue instanceof Double) {
            value = flinkConfig.getDouble(key, (Double) defaultValue);
        } else if (defaultValue instanceof String) {
            value = flinkConfig.getString(key, (String) defaultValue);
        } else {
            throw new IllegalArgumentException(
                    "Unsupported configuration type: " + defaultValue.getClass().getName());
        }

        return Optional.of((T) value);
    }

    /**
     * Gets the underlying Flink Configuration object.
     *
     * @return The wrapped Flink Configuration.
     */
    public Configuration getFlinkConfiguration() {
        return flinkConfig;
    }
}
