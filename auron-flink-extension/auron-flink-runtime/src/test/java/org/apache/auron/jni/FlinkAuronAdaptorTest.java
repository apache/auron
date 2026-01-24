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

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for FlinkAuronAdaptor.
 * Tests SPI loading, configuration, and basic adaptor functionality.
 */
public class FlinkAuronAdaptorTest {

    @Test
    public void testAdaptorSPILoading() {
        // Test that the adaptor can be loaded via SPI
        AuronAdaptor adaptor = AuronAdaptor.getInstance();

        assertNotNull(adaptor);
        assertTrue(adaptor instanceof FlinkAuronAdaptor);
    }

    @Test
    public void testGetAuronConfiguration() {
        FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();

        // Set thread configuration first
        Configuration config = new Configuration();
        config.setBoolean("table.exec.auron.enable", true);
        config.setInteger("table.exec.auron.batch-size", 4096);
        FlinkAuronAdaptor.setThreadConfiguration(config);

        try {
            AuronConfiguration auronConfig = adaptor.getAuronConfiguration();
            assertNotNull(auronConfig);
            assertTrue(auronConfig instanceof FlinkAuronConfiguration);

            // Verify configuration can be read
            FlinkAuronConfiguration flinkConfig = (FlinkAuronConfiguration) auronConfig;
            assertNotNull(flinkConfig.getFlinkConfiguration());
        } finally {
            FlinkAuronAdaptor.clearThreadConfiguration();
        }
    }

    @Test
    public void testThreadContextManagement() {
        FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();

        // Initially, context should be null
        Object initialContext = adaptor.getThreadContext();
        assertNull(initialContext);

        // Set a context (use a simple object for testing)
        Object testContext = new Object();
        adaptor.setThreadContext(testContext);

        // Verify it was set
        Object retrievedContext = adaptor.getThreadContext();
        assertSame(testContext, retrievedContext);

        // Clear context
        adaptor.setThreadContext(null);
        Object clearedContext = adaptor.getThreadContext();
        assertNull(clearedContext);
    }

    @Test
    public void testGetJVMTotalMemoryLimited() {
        FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();

        // Without configuration, should return default (Long.MAX_VALUE or runtime max)
        long memoryLimit = adaptor.getJVMTotalMemoryLimited();
        assertTrue(memoryLimit > 0);
    }

    @Test
    public void testGetJVMTotalMemoryWithConfiguration() {
        FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();

        // Set configuration with memory settings
        Configuration config = new Configuration();
        // Flink's memory configuration would be set here
        FlinkAuronAdaptor.setThreadConfiguration(config);

        try {
            long memoryLimit = adaptor.getJVMTotalMemoryLimited();
            assertTrue(memoryLimit > 0);
        } finally {
            FlinkAuronAdaptor.clearThreadConfiguration();
        }
    }

    @Test
    public void testGetDirectWriteSpillToDiskFile() throws IOException {
        FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();

        String spillFilePath = adaptor.getDirectWriteSpillToDiskFile();

        assertNotNull(spillFilePath);
        assertFalse(spillFilePath.isEmpty());

        // Verify it's a valid file path
        File spillFile = new File(spillFilePath);
        assertTrue(spillFile.getPath().contains("auron-flink-spill"));

        // The file should be created with .tmp extension
        assertTrue(spillFilePath.endsWith(".tmp"));

        // Cleanup
        if (spillFile.exists()) {
            spillFile.delete();
        }
    }

    @Test
    public void testGetDirectWriteSpillToDiskFileMultipleCalls() throws IOException {
        FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();

        // Multiple calls should return different file paths
        String path1 = adaptor.getDirectWriteSpillToDiskFile();
        String path2 = adaptor.getDirectWriteSpillToDiskFile();

        assertNotNull(path1);
        assertNotNull(path2);
        assertNotEquals(path1, path2);

        // Cleanup
        new File(path1).delete();
        new File(path2).delete();
    }

    @Test
    public void testGetAuronUDFWrapperContextThrowsException() {
        FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();

        // UDFs are not supported in MVP
        assertThrows(UnsupportedOperationException.class, () -> {
            adaptor.getAuronUDFWrapperContext(java.nio.ByteBuffer.allocate(10));
        });
    }

    @Test
    public void testThreadConfigurationStatic() {
        // Test static thread configuration management
        Configuration config = new Configuration();
        config.setBoolean("table.exec.auron.enable", true);
        config.setInteger("table.exec.auron.batch-size", 8192);

        FlinkAuronAdaptor.setThreadConfiguration(config);

        try {
            FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();
            AuronConfiguration auronConfig = adaptor.getAuronConfiguration();

            assertNotNull(auronConfig);
            // Configuration should be accessible
            FlinkAuronConfiguration flinkConfig = (FlinkAuronConfiguration) auronConfig;
            assertEquals(config, flinkConfig.getFlinkConfiguration());
        } finally {
            FlinkAuronAdaptor.clearThreadConfiguration();
        }
    }

    @Test
    public void testThreadConfigurationIsolation() {
        // Test that thread configuration is isolated per thread
        Configuration config1 = new Configuration();
        config1.setBoolean("table.exec.auron.enable", true);

        FlinkAuronAdaptor.setThreadConfiguration(config1);

        try {
            // In same thread, should get the same config
            FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();
            FlinkAuronConfiguration flinkConfig = (FlinkAuronConfiguration) adaptor.getAuronConfiguration();

            assertEquals(config1, flinkConfig.getFlinkConfiguration());
        } finally {
            FlinkAuronAdaptor.clearThreadConfiguration();
        }

        // After clearing, should get a new default config
        FlinkAuronAdaptor adaptor2 = new FlinkAuronAdaptor();
        FlinkAuronConfiguration flinkConfig2 = (FlinkAuronConfiguration) adaptor2.getAuronConfiguration();

        assertNotNull(flinkConfig2);
        // Should be a fresh configuration, not config1
        assertNotSame(config1, flinkConfig2.getFlinkConfiguration());
    }

    @Test
    public void testLoadAuronLibraryNotCrashing() {
        // Test that loadAuronLib doesn't crash (even if library is missing)
        // In real scenario with library present, this would succeed
        // Without library, it should throw IllegalStateException
        FlinkAuronAdaptor adaptor = new FlinkAuronAdaptor();

        try {
            adaptor.loadAuronLib();
            // If we get here, library was loaded successfully
            // (This would only happen if libauron is in classpath)
        } catch (IllegalStateException e) {
            // Expected if library is not in classpath
            assertTrue(e.getMessage().contains("loading native library")
                    || e.getMessage().contains("not found"));
        }
    }

    @Test
    public void testNativeLibraryNameMapping() {
        // Test that the library name is correctly mapped for the platform
        String libName = System.mapLibraryName("auron");

        assertNotNull(libName);
        assertTrue(libName.startsWith("lib") || libName.endsWith(".dll"));

        // On macOS: libauron.dylib
        // On Linux: libauron.so
        // On Windows: auron.dll
        if (System.getProperty("os.name").toLowerCase().contains("mac")) {
            assertEquals("libauron.dylib", libName);
        } else if (System.getProperty("os.name").toLowerCase().contains("linux")) {
            assertEquals("libauron.so", libName);
        } else if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            assertEquals("auron.dll", libName);
        }
    }

    @Test
    public void testAdaptorSingleton() {
        // Test that getInstance returns the same instance
        AuronAdaptor adaptor1 = AuronAdaptor.getInstance();
        AuronAdaptor adaptor2 = AuronAdaptor.getInstance();

        assertSame(adaptor1, adaptor2);
        assertTrue(adaptor1 instanceof FlinkAuronAdaptor);
    }
}
