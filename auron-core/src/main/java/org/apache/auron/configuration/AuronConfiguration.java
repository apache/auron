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

    public static final ConfigOption<Integer> BATCH_SIZE = new ConfigOption<>(Integer.class)
            .withKey("auron.batchSize")
            .withDescription("Suggested batch size for arrow batches.")
            .withDefaultValue(10000);

    public static final ConfigOption<Double> MEMORY_FRACTION = new ConfigOption<>(Double.class)
            .withKey("auron.memoryFraction")
            .withDescription("Suggested fraction of off-heap memory used in native execution. "
                    + "actual off-heap memory usage is expected to be spark.executor.memoryOverhead * fraction.")
            .withDefaultValue(0.6);

    public static final ConfigOption<String> NATIVE_LOG_LEVEL = new ConfigOption<>(String.class)
            .withKey("auron.native.log.level")
            .withDescription("Log level for native execution.")
            .withDefaultValue("info");

    public static final ConfigOption<Integer> TOKIO_WORKER_THREADS_PER_CPU = new ConfigOption<>(Integer.class)
            .withKey("auron.tokio.worker.threads.per.cpu")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Number of Tokio worker threads to create per CPU core (spark.task.cpus). Set to 0 for automatic detection "
                            + "based on available CPU cores. This setting controls the thread pool size for Tokio-based asynchronous operations.")
            .withDefaultValue(0);

    public static final ConfigOption<Integer> SUGGESTED_BATCH_MEM_SIZE = new ConfigOption<>(Integer.class)
            .withKey("auron.suggested.batch.memSize")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Suggested memory size in bytes for record batches. This setting controls the target memory allocation "
                            + "for individual data batches to optimize memory usage and processing efficiency. Default is 8MB (8,388,608 bytes).")
            .withDefaultValue(8388608);

    public static final ConfigOption<Integer> TASK_CPUS = new ConfigOption<>(Integer.class)
            .withKey("task.cpus")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Number of CPU cores allocated per Spark task. This setting determines the parallelism level "
                            + "for individual tasks and affects resource allocation and task scheduling. "
                            + "In Spark, the value is retrieved from SparkEnv via the 'spark.task.cpus' option; "
                            + "if not configured, the default value of 1 is used.")
            .withDefaultValue(1);

    public abstract <T> Optional<T> getOptional(ConfigOption<T> option);

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
     * Returns the value associated with the given config option as an integer.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public int getInteger(ConfigOption<Integer> configOption) {
        return getOptional(configOption).orElseGet(() -> getOptionDefaultValue(configOption));
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
     * Returns the value associated with the given config option as a {@code double}.
     *
     * @param configOption The configuration option
     * @return the (default) value associated with the given config option
     */
    public double getDouble(ConfigOption<Double> configOption) {
        return getOptional(configOption).orElseGet(() -> getOptionDefaultValue(configOption));
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
