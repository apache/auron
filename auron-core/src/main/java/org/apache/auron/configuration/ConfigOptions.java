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

import static org.apache.auron.util.Preconditions.checkNotNull;

import java.util.function.Function;

/**
 * Refer to the design of the Flink engine.
 * {@code ConfigOptions} are used to build a {@link ConfigOption}. The option is typically built in
 * one of the following pattern:
 *
 * <pre>{@code
 * // simple string-valued option with a default value
 * ConfigOption<String> tempDirs = ConfigOptions
 *     .key("tmp.dir")
 *     .stringType()
 *     .defaultValue("/tmp");
 *
 * // simple string-valued option with a default value and with the description
 * ConfigOption<String> tempDirs = ConfigOptions
 *     .key("tmp.dir")
 *     .description("this is a example of string")
 *     .stringType()
 *     .defaultValue("/tmp");
 *
 * // simple integer-valued option with a default value
 * ConfigOption<Integer> batchSize = ConfigOptions
 *     .key("batch.size")
 *     .intType()
 *     .defaultValue(100);
 *
 * // option with no default value
 * ConfigOption<String> userName = ConfigOptions
 *     .key("user.name")
 *     .stringType()
 *     .noDefaultValue();
 * }</pre>
 */
public class ConfigOptions {

    /**
     * Starts building a new {@link ConfigOption}.
     *
     * @param key The key for the config option.
     * @return The builder for the config option with the given key.
     */
    public static OptionBuilder key(String key) {
        checkNotNull(key);
        return new OptionBuilder(key);
    }

    // ------------------------------------------------------------------------

    /**
     * The option builder is used to create a {@link ConfigOption}. It is instantiated via {@link
     * ConfigOptions#key(String)}.
     */
    public static final class OptionBuilder {

        /** The key for the config option. */
        private final String key;

        private String description = ConfigOption.EMPTY_DESCRIPTION;

        /**
         * Creates a new OptionBuilder.
         * @param key The key for the config option
         */
        OptionBuilder(String key) {
            this.key = key;
        }

        OptionBuilder(String key, String description) {
            this.key = key;
            this.description = description;
        }

        public OptionBuilder description(String description) {
            return new OptionBuilder(key, description);
        }

        /** Defines that the value of the option should be of {@link Boolean} type. */
        public TypedConfigOptionBuilder<Boolean> booleanType() {
            return new TypedConfigOptionBuilder<>(key, Boolean.class, description);
        }

        /** Defines that the value of the option should be of {@link Integer} type. */
        public TypedConfigOptionBuilder<Integer> intType() {
            return new TypedConfigOptionBuilder<>(key, Integer.class, description);
        }

        /** Defines that the value of the option should be of {@link Long} type. */
        public TypedConfigOptionBuilder<Long> longType() {
            return new TypedConfigOptionBuilder<>(key, Long.class, description);
        }

        /** Defines that the value of the option should be of {@link Float} type. */
        public TypedConfigOptionBuilder<Float> floatType() {
            return new TypedConfigOptionBuilder<>(key, Float.class, description);
        }

        /** Defines that the value of the option should be of {@link Double} type. */
        public TypedConfigOptionBuilder<Double> doubleType() {
            return new TypedConfigOptionBuilder<>(key, Double.class, description);
        }

        /** Defines that the value of the option should be of {@link String} type. */
        public TypedConfigOptionBuilder<String> stringType() {
            return new TypedConfigOptionBuilder<>(key, String.class, description);
        }
    }

    /**
     * Builder for {@link ConfigOption} with a defined atomic type.
     *
     * @param <T> atomic type of the option
     */
    public static class TypedConfigOptionBuilder<T> {
        private final String key;
        private final Class<T> clazz;

        private final String description;

        TypedConfigOptionBuilder(String key, Class<T> clazz, String description) {
            this.key = key;
            this.clazz = clazz;
            this.description = description;
        }

        /**
         * Creates a ConfigOption with the given default value.
         *
         * @param value The default value for the config option
         * @return The config option with the default value.
         */
        public ConfigOption<T> defaultValue(T value) {
            return new ConfigOption<>(key, clazz, value, description, null);
        }

        /**
         * Creates a ConfigOption without a default value.
         *
         * @return The config option without a default value.
         */
        public ConfigOption<T> noDefaultValue() {
            return new ConfigOption<>(key, clazz, null, description, null);
        }

        public ConfigOption<T> dynamicDefaultValue(Function<AuronConfiguration, T> dynamicDefaultValueFunction) {
            return new ConfigOption<>(key, clazz, null, description, dynamicDefaultValueFunction);
        }
    }

    // ------------------------------------------------------------------------

    /** Not intended to be instantiated. */
    private ConfigOptions() {}
}
