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
 * A {@code ConfigOption} describes a configuration parameter. It encapsulates the configuration
 * key, and an optional default value for the configuration parameter.
 * Refer to the design of the Flink engine.
 *
 * <p>{@code ConfigOptions} are built via the {@link ConfigOptions} class. Once created, a config
 * option is immutable.
 *
 * @param <T> The type of value associated with the configuration option.
 */
public class ConfigOption<T> {

    public static final String EMPTY_DESCRIPTION = "";

    /** The current key for that config option. */
    private final String key;

    /** The default value for this option. */
    private final T defaultValue;

    /** The description for this option. */
    private final String description;

    /** The function to compute the default value. */
    private final Function<AuronConfiguration, T> dynamicDefaultValueFunction;

    /**
     * Type of the value that this ConfigOption describes.
     *
     * <ul>
     *   <li>typeClass == atomic class (e.g. {@code Integer.class}) -> {@code ConfigOption<Integer>}
     *   <li>typeClass == {@code Map.class} -> {@code ConfigOption<Map<String, String>>}
     *   <li>typeClass == atomic class and isList == true for {@code ConfigOption<List<Integer>>}
     * </ul>
     */
    private final Class<?> clazz;

    /**
     * Creates a new config option with fallback keys.
     *
     * @param key The current key for that config option
     * @param clazz describes type of the ConfigOption, see description of the clazz field
     * @param description Description for that option
     * @param defaultValue The default value for this option
     */
    ConfigOption(
            String key,
            Class<?> clazz,
            T defaultValue,
            String description,
            Function<AuronConfiguration, T> dynamicDefaultValueFunction) {
        this.key = checkNotNull(key);
        this.description = description;
        this.defaultValue = defaultValue;
        this.clazz = checkNotNull(clazz);
        this.dynamicDefaultValueFunction = dynamicDefaultValueFunction;
    }

    /**
     * Gets the configuration key.
     *
     * @return The configuration key
     */
    public String key() {
        return key;
    }

    /**
     * Gets the description of configuration key
     *
     * @return
     */
    public String description() {
        return description;
    }

    /**
     * Checks if this option has a default value.
     *
     * @return True if it has a default value, false if not.
     */
    public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    /**
     * Returns the default value, or null, if there is no default value.
     *
     * @return The default value, or null.
     */
    public T defaultValue() {
        return defaultValue;
    }

    /**
     * Checks if this option has a dynamic default value.
     *
     * @return True if it has a dynamic default value, false if not.
     */
    public boolean hasDynamicDefaultValue() {
        return dynamicDefaultValueFunction != null;
    }

    /**
     * Returns the dynamic default value function, or null, if there is no dynamic default value.
     *
     * @return The dynamic default value function, or null.
     */
    public Function<AuronConfiguration, T> dynamicDefaultValueFunction() {
        return dynamicDefaultValueFunction;
    }
}
