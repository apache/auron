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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.auron.jni.AuronAdaptor;

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
    private String key;

    /** The current key for that config option. */
    private List<String> altKeys = new ArrayList<>();

    /** The default value for this option. */
    private T defaultValue;

    /** The current category for that config option. */
    private String category = "Uncategorized";

    /** The description for this option. */
    private String description;

    /** The function to compute the default value. */
    private Function<AuronConfiguration, T> dynamicDefaultValueFunction;

    /**
     * Optional validator. Returns a non-empty error message (wrapped in {@link Optional}) when the
     * value is invalid, or an empty {@link Optional} when it is valid.
     */
    private Function<T, Optional<String>> validator;

    /**
     * Type of the value that this ConfigOption describes.
     *
     * <ul>
     *   <li>typeClass == atomic class (e.g. {@code Integer.class}) -> {@code ConfigOption<Integer>}
     *   <li>typeClass == {@code Map.class} -> {@code ConfigOption<Map<String, String>>}
     *   <li>typeClass == atomic class and isList == true for {@code ConfigOption<List<Integer>>}
     * </ul>
     */
    private Class<T> clazz;

    public ConfigOption(Class<T> clazz) {
        this.clazz = clazz;
    }

    public ConfigOption<T> withKey(String key) {
        this.key = key;
        return this;
    }

    public ConfigOption<T> addAltKey(String altKey) {
        this.altKeys.add(altKey);
        return this;
    }

    public ConfigOption<T> withDefaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public ConfigOption<T> withCategory(String category) {
        this.category = category;
        return this;
    }

    public ConfigOption<T> withDescription(String description) {
        this.description = description;
        return this;
    }

    public ConfigOption<T> withDynamicDefaultValue(Function<AuronConfiguration, T> dynamicDefaultValueFunction) {
        this.dynamicDefaultValueFunction = dynamicDefaultValueFunction;
        return this;
    }

    /**
     * Registers a validator that rejects values for which {@code check} returns {@code false}. When
     * the engine-side configuration backend reads a user-supplied value that fails the check, it
     * fails fast with an {@link IllegalArgumentException} carrying {@code errorMsg}.
     *
     * @param check predicate that returns {@code true} for valid values.
     * @param errorMsg human-readable message used when the check fails.
     * @return this {@code ConfigOption} for chaining.
     */
    public ConfigOption<T> checkValue(Predicate<T> check, String errorMsg) {
        this.validator = v -> check.test(v) ? Optional.empty() : Optional.of(errorMsg);
        return this;
    }

    /**
     * Validates a resolved value against the registered validator, if any. Called by the
     * engine-side configuration backend  when it reads a user-supplied value.
     *
     * @param value the value to validate.
     * @return an empty {@link Optional} if valid or no validator is registered, otherwise an
     *     {@link Optional} carrying the error message.
     */
    public Optional<String> validate(T value) {
        if (validator == null) {
            return Optional.empty();
        }
        return validator.apply(value);
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
     * Gets the alternative configuration keys.
     */
    public List<String> altKeys() {
        return altKeys;
    }

    /**
     * Gets the category of configuration key
     */
    public String category() {
        return category;
    }

    /**
     * Gets the description of configuration key
     */
    public String description() {
        return description;
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

    /**
     * Gets the type class of the value that this ConfigOption describes.
     *
     * @return The type class of the value that this ConfigOption describes.
     */
    public Class<T> getValueClass() {
        return clazz;
    }

    /**
     * Retrieves the current value of this configuration option.
     *
     * @return the current value associated with this configuration option.
     */
    public T get() {
        return AuronAdaptor.getInstance().getAuronConfiguration().get(this);
    }
}
