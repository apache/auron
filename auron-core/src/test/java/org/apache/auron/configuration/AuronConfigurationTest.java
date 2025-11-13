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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This is a test class for {@link AuronConfiguration}.
 */
public class AuronConfigurationTest {

    private MockAuronConfiguration config;

    @BeforeEach
    public void setUp() {
        config = new MockAuronConfiguration();
        config.addConfig(MockAuronConfiguration.STRING_WITHOUT_DEFAULT_CONFIG_OPTION.key(), "str1");
        config.addConfig(MockAuronConfiguration.INT_CONFIG_OPTION.key(), 100);
        config.addConfig(MockAuronConfiguration.BOOLEAN_CONFIG_OPTION.key(), false);
        config.addConfig(MockAuronConfiguration.DOUBLE_CONFIG_OPTION.key(), 99.9);
        config.addConfig(MockAuronConfiguration.LONG_CONFIG_OPTION.key(), 10000000000L);
        config.addConfig(MockAuronConfiguration.FLOAT_CONFIG_OPTION.key(), 1.2f);
    }

    @Test
    public void testGetConfig() {
        assertEquals("str1", config.get(MockAuronConfiguration.STRING_WITHOUT_DEFAULT_CONFIG_OPTION));
        assertEquals("zm", config.get(MockAuronConfiguration.STRING_CONFIG_OPTION));
        assertEquals(100, config.getInteger(MockAuronConfiguration.INT_CONFIG_OPTION));
        assertEquals(false, config.get(MockAuronConfiguration.BOOLEAN_CONFIG_OPTION));
        assertEquals(99.9, config.get(MockAuronConfiguration.DOUBLE_CONFIG_OPTION), 0.0000000001);
        assertEquals(10000000000L, config.getLong(MockAuronConfiguration.LONG_CONFIG_OPTION));
        assertEquals(1.2f, config.get(MockAuronConfiguration.FLOAT_CONFIG_OPTION), 0.0000000001);
        // test dynamic default value
        assertEquals(500, config.getInteger(MockAuronConfiguration.INT_WITH_DYNAMIC_DEFAULT_CONFIG_OPTION));
    }
}
