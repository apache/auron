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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MockAuronConfiguration extends AuronConfiguration {

    public static final ConfigOption<String> STRING_CONFIG_OPTION =
            ConfigOptions.key("string").stringType().defaultValue("zm");

    public static final ConfigOption<String> STRING_WITHOUT_DEFAULT_CONFIG_OPTION =
            ConfigOptions.key("string_without_default").stringType().noDefaultValue();

    public static final ConfigOption<Integer> INT_CONFIG_OPTION =
            ConfigOptions.key("int").intType().defaultValue(1);

    public static final ConfigOption<Long> LONG_CONFIG_OPTION =
            ConfigOptions.key("long").longType().defaultValue(1L);

    public static final ConfigOption<Boolean> BOOLEAN_CONFIG_OPTION =
            ConfigOptions.key("boolean").booleanType().defaultValue(true);

    public static final ConfigOption<Double> DOUBLE_CONFIG_OPTION =
            ConfigOptions.key("double").doubleType().defaultValue(1.0);

    public static final ConfigOption<Float> FLOAT_CONFIG_OPTION =
            ConfigOptions.key("float").floatType().defaultValue(1.0f);

    public static final ConfigOption<Integer> INT_WITH_DYNAMIC_DEFAULT_CONFIG_OPTION = ConfigOptions.key(
                    "int_with_dynamic_default")
            .intType()
            .dynamicDefaultValue(config -> config.getInteger(INT_CONFIG_OPTION) * 5);

    private Map<String, Object> configMap = new HashMap<>();

    public MockAuronConfiguration() {}

    public void addConfig(String key, Object value) {
        configMap.put(key, value);
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        return Optional.ofNullable((T) configMap.getOrDefault(option.key(), getOptionDefaultValue(option)));
    }

    @Override
    public <T> Optional<T> getOptional(String key) {
        return Optional.ofNullable((T) configMap.get(key));
    }
}
