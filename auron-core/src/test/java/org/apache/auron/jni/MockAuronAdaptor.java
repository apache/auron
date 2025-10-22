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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.configuration.MockAuronConfiguration;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.auron.functions.MockAuronUDFWrapperContext;

/**
 * This is a mock class for testing the AuronAdaptor.
 */
public class MockAuronAdaptor extends AuronAdaptor {
    @Override
    public void loadAuronLib() {
        // Mock implementation, no need to load auron library
    }

    @Override
    public String getDirectWriteSpillToDiskFile() throws IOException {
        File tempFile = File.createTempFile("auron-spill-", ".tmp");
        tempFile.deleteOnExit();
        return tempFile.getAbsolutePath();
    }

    @Override
    public Object getThreadContext() {
        return null;
    }

    @Override
    public void setThreadContext(Object context) {
        // Mock implementation, no need to set thread context
    }

    @Override
    public AuronConfiguration getAuronConfiguration() {
        return new MockAuronConfiguration();
    }

    @Override
    public AuronUDFWrapperContext getAuronUDFWrapperContext(ByteBuffer udfSerialized) {
        return new MockAuronUDFWrapperContext(udfSerialized);
    }
}
