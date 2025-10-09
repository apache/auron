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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** Tests for the {@link ConfigOption}. */
public class ConfigOptionTest {

    @Test
    public void testConfigOption() {
        ConfigOption<String> keyOption = ConfigOptions.key("key").stringType().noDefaultValue();
        assertEquals("key", keyOption.key());
        assertEquals(null, keyOption.defaultValue());
        assertEquals(false, keyOption.hasDefaultValue());
        ConfigOption<Boolean> booleanOption =
                ConfigOptions.key("boolean").booleanType().defaultValue(true);
        assertEquals(true, booleanOption.defaultValue());
    }

    @Test
    public void testConfigOptionAddDesc() {
        ConfigOption<String> keyOption = ConfigOptions.key("key")
                .description("this is a description of the key")
                .stringType()
                .noDefaultValue();
        assertEquals("key", keyOption.key());
        assertEquals(null, keyOption.defaultValue());
        assertEquals(false, keyOption.hasDefaultValue());
        ConfigOption<Boolean> booleanOption =
                ConfigOptions.key("boolean").booleanType().defaultValue(true);
        assertEquals(true, booleanOption.defaultValue());
        assertEquals("this is a description of the key", keyOption.description());
    }
}
