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
package org.apache.auron.configuration;

import java.util.Optional;

/**
 * Auron configuration base class.
 */
public abstract class AuronConfiguration {

    public static final ConfigOption<Integer> BATCH_SIZE = ConfigOptions.key("auron.batchSize")
            .description("Suggested batch size for arrow batches.")
            .intType()
            .defaultValue(10000);

    public static final ConfigOption<Double> MEMORY_FRACTION = ConfigOptions.key("auron.memoryFraction")
            .description("Suggested fraction of off-heap memory used in native execution. "
                    + "actual off-heap memory usage is expected to be spark.executor.memoryOverhead * fraction.")
            .doubleType()
            .defaultValue(0.6);

    public static final ConfigOption<String> NATIVE_LOG_LEVEL = ConfigOptions.key("auron.native.log.level")
            .description("Log level for native execution.")
            .stringType()
            .defaultValue("info");

    public abstract <T> Optional<T> getOptional(ConfigOption<T> option);

    public abstract <T> Optional<T> getOptional(String key);

    public <T> T get(ConfigOption<T> option) {
        return getOptional(option).orElseGet(() -> getOptionDefaultValue(option));
    }

    /**
     * Returns the value associated with the given config option as a string.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public String getString(ConfigOption<String> configOption) {
        return getOptional(configOption).orElseGet(() -> getOptionDefaultValue(configOption));
    }

    /**
     * Returns the value associated with the given config option as a string. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public String getString(ConfigOption<String> configOption, String overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Returns the value associated with the given config option as an integer.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public int getInteger(ConfigOption<Integer> configOption) {
        return getOptional(configOption).orElseGet(() -> getOptionDefaultValue(configOption));
    }

    /**
     * Returns the value associated with the given config option as an integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapped for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public int getInteger(ConfigOption<Integer> configOption, int overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Returns the value associated with the given config option as a long integer.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public long getLong(ConfigOption<Long> configOption) {
        return getOptional(configOption).orElseGet(() -> getOptionDefaultValue(configOption));
    }

    /**
     * Returns the value associated with the given config option as a long integer. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapped for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public long getLong(ConfigOption<Long> configOption, long overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Returns the value associated with the given config option as a boolean.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public boolean getBoolean(ConfigOption<Boolean> configOption) {
        return getOptional(configOption).orElseGet(() -> getOptionDefaultValue(configOption));
    }

    /**
     * Returns the value associated with the given config option as a boolean. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapped for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public boolean getBoolean(ConfigOption<Boolean> configOption, boolean overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Returns the value associated with the given config option as a float.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public float getFloat(ConfigOption<Float> configOption) {
        return getOptional(configOption).orElseGet(() -> getOptionDefaultValue(configOption));
    }

    /**
     * Returns the value associated with the given config option as a float. If no value is mapped
     * under any key of the option, it returns the specified default instead of the option's default
     * value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapped for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public float getFloat(ConfigOption<Float> configOption, float overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Returns the value associated with the given config option as a {@code double}.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public double getDouble(ConfigOption<Double> configOption) {
        return getOptional(configOption).orElseGet(() -> getOptionDefaultValue(configOption));
    }

    /**
     * Returns the value associated with the given config option as a {@code double}. If no value is
     * mapped under any key of the option, it returns the specified default instead of the option's
     * default value.
     *
     * @param configOption The configuration option
     * @param overrideDefault The value to return if no value was mapped for any key of the option
     * @return the configured value associated with the given config option, or the overrideDefault
     */
    public double getDouble(ConfigOption<Double> configOption, double overrideDefault) {
        return getOptional(configOption).orElse(overrideDefault);
    }

    /**
     * Returns the value associated with the given config option as a {@code double}.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    protected <T> T getOptionDefaultValue(ConfigOption<T> configOption) {
        if (configOption.hasDynamicDefaultValue()) {
            return configOption.dynamicDefaultValueFunction().apply(this);
        } else {
            return configOption.defaultValue();
        }
    }
}
