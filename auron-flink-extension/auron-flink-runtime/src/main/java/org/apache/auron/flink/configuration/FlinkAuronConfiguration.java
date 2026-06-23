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

import java.io.File;
import java.util.List;
import java.util.Optional;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;

/**
 * Flink configuration proxy for Auron.
 * All configuration prefixes start with flink.
 */
public class FlinkAuronConfiguration extends AuronConfiguration {

    // When using getOptional, the prefix will be automatically completed. If you only need to print the Option key,
    // please manually add the prefix.
    public static final String FLINK_PREFIX = "flink.";

    public static final ConfigOption<Long> NATIVE_MEMORY_SIZE = new ConfigOption<>(Long.class)
            .withKey("auron.native.memory.size")
            .withDescription("The auron native memory size to use.")
            .withDefaultValue(256 * 1024 * 1024L); // 256 MB

    /**
     * When an Auron operator conversion fails at planning time, controls whether the
     * job falls back to Flink's stock engine for that operator (true) or fails the
     * submission with an {@link IllegalStateException} (false). The default is
     * {@code true}; set to {@code false} in CI or new-converter development to surface
     * gaps in Auron coverage instead of silently degrading.
     */
    public static final ConfigOption<Boolean> FAIL_BACK_FLINK_ENGINE_ENABLED = new ConfigOption<>(Boolean.class)
            .withKey("auron.failback.flink.engine.enabled")
            .withDescription("When an Auron operator conversion fails, does it fall back to "
                    + "the Flink engine for execution?")
            .withDefaultValue(true);

    /**
     * Whether the native execution context records per-batch input statistics for monitoring.
     * Queried by the native engine on every Auron-native operator path; the field must exist
     * on this class so the JniBridge reflection lookup does not NPE.
     */
    public static final ConfigOption<Boolean> INPUT_BATCH_STATISTICS_ENABLE = new ConfigOption<>(Boolean.class)
            .withKey("auron.input.batch.statistics.enable")
            .withDescription("Enable collection of additional metrics for input batch statistics.")
            .withDefaultValue(false);

    private final Configuration flinkConfig;

    public FlinkAuronConfiguration() {
        String pwd = System.getenv("PWD");
        if (new File(pwd + GlobalConfiguration.FLINK_CONF_FILENAME).exists()) {
            // flink on yarn
            flinkConfig = GlobalConfiguration.loadConfiguration(pwd);
        } else {
            // flink on k8s
            flinkConfig = GlobalConfiguration.loadConfiguration();
        }
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> configOption) {
        return Optional.ofNullable(
                getFromFlinkConfig(configOption.key(), configOption.altKeys(), configOption.getValueClass()));
    }

    @SuppressWarnings("unchecked")
    private <T> T getFromFlinkConfig(String key, List<String> altKeys, Class<T> valueClass) {
        String flinkKey = key.startsWith(FLINK_PREFIX) ? key : FLINK_PREFIX + key;
        ConfigOptions.OptionBuilder flinkOptionBuilder = ConfigOptions.key(flinkKey);
        org.apache.flink.configuration.ConfigOption<T> flinkOption;
        if (valueClass == String.class) {
            flinkOption = (org.apache.flink.configuration.ConfigOption<T>)
                    flinkOptionBuilder.stringType().noDefaultValue();
        } else if (valueClass == Integer.class) {
            flinkOption = (org.apache.flink.configuration.ConfigOption<T>)
                    flinkOptionBuilder.intType().noDefaultValue();
        } else if (valueClass == Long.class) {
            flinkOption = (org.apache.flink.configuration.ConfigOption<T>)
                    flinkOptionBuilder.longType().noDefaultValue();
        } else if (valueClass == Boolean.class) {
            flinkOption = (org.apache.flink.configuration.ConfigOption<T>)
                    flinkOptionBuilder.booleanType().noDefaultValue();
        } else if (valueClass == Float.class) {
            flinkOption = (org.apache.flink.configuration.ConfigOption<T>)
                    flinkOptionBuilder.floatType().noDefaultValue();
        } else if (valueClass == Double.class) {
            flinkOption = (org.apache.flink.configuration.ConfigOption<T>)
                    flinkOptionBuilder.doubleType().noDefaultValue();
        } else {
            throw new IllegalArgumentException("Unsupported value class: " + valueClass);
        }
        if (!altKeys.isEmpty()) {
            String[] altKeysArray = new String[altKeys.size()];
            for (int i = 0; i < altKeys.size(); i++) {
                String altKey = altKeys.get(i);
                altKeysArray[i] = altKey.startsWith(FLINK_PREFIX) ? altKey : FLINK_PREFIX + altKey;
            }
            flinkOption = flinkOption.withDeprecatedKeys(altKeysArray);
        }
        return flinkConfig.get(flinkOption);
    }
}
