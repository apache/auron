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

public class MockAuronConfiguration extends AuronConfiguration {

    // Basic configuration options with descriptions
    public static final ConfigOption<String> STRING_CONFIG_OPTION = new ConfigOption<>(String.class)
            .withKey("string")
            .withDescription("A string configuration option for testing.")
            .withDefaultValue("zm");

    public static final ConfigOption<Integer> INT_CONFIG_OPTION = new ConfigOption<>(Integer.class)
            .withKey("int")
            .withDescription("An integer configuration option for testing.")
            .withDefaultValue(1);

    public static final ConfigOption<Long> LONG_CONFIG_OPTION = new ConfigOption<>(Long.class)
            .withKey("long")
            .withDescription("A long configuration option for testing.")
            .withDefaultValue(1L);

    public static final ConfigOption<Boolean> BOOLEAN_CONFIG_OPTION = new ConfigOption<>(Boolean.class)
            .withKey("boolean")
            .withDescription("A boolean configuration option for testing.")
            .withDefaultValue(true);

    public static final ConfigOption<Double> DOUBLE_CONFIG_OPTION = new ConfigOption<>(Double.class)
            .withKey("double")
            .withDescription("A double configuration option for testing.")
            .withDefaultValue(1.0);

    public static final ConfigOption<Float> FLOAT_CONFIG_OPTION = new ConfigOption<>(Float.class)
            .withKey("float")
            .withDescription("A float configuration option for testing.")
            .withDefaultValue(1.0f);

    public static final ConfigOption<Integer> INT_WITH_DYNAMIC_DEFAULT_CONFIG_OPTION = new ConfigOption<>(Integer.class)
            .withKey("int_with_dynamic_default")
            .withDescription("An integer configuration option with dynamic default value.")
            .withDynamicDefaultValue(
                    config -> config.getOptional(INT_CONFIG_OPTION).orElse(1) * 5);

    public MockAuronConfiguration() {}

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        return Optional.empty(); // always use default value
    }
}
