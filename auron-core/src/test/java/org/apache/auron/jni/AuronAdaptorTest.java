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
package org.apache.auron.jni;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.auron.configuration.AuronConfiguration;
import org.junit.jupiter.api.Test;

/**
 * This is a test class for {@link AuronAdaptor}.
 */
public class AuronAdaptorTest {

    @Test
    public void testRetrieveConfigWithAuronAdaptor() {
        MockAuronAdaptor auronAdaptor = new MockAuronAdaptor();
        assertNotNull(auronAdaptor.getAuronConfiguration());
        AuronConfiguration auronConfig = auronAdaptor.getAuronConfiguration();
        assertEquals(auronConfig.getInteger(AuronConfiguration.BATCH_SIZE), 10000);
        assertEquals(auronConfig.getDouble(AuronConfiguration.MEMORY_FRACTION), 0.6, 0.0);
        assertEquals(auronConfig.getString(AuronConfiguration.NATIVE_LOG_LEVEL), "info");
    }

    @Test
    public void testLoadAuronAdaptorInstanceViaSPI() {
        AuronAdaptor adaptor = AuronAdaptor.getInstance();
        assertInstanceOf(MockAuronAdaptor.class, adaptor, "SPI should discover and instantiate MockAuronAdaptor");
    }
}
