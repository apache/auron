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
package org.apache.auron.flink.table.runtime;

import org.junit.jupiter.api.Test;

/**
 * Test to isolate JNI initialization issues.
 */
public class AuronJniInitTest {

    @Test
    public void testNativeLibraryLoading() {
        System.out.println("🔍 java.library.path = " + System.getProperty("java.library.path"));

        try {
            System.out.println("Attempting to load Auron native library...");
            System.loadLibrary("auron");
            System.out.println("✅ Auron native library loaded successfully");
        } catch (UnsatisfiedLinkError e) {
            System.err.println("❌ Failed to load Auron native library");
            e.printStackTrace();
            throw e;
        }

        System.out.println("🎉 Test passed - library can be loaded");
    }
}
