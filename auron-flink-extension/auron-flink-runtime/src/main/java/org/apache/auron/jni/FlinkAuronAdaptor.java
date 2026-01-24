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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.flink.configuration.FlinkAuronConfiguration;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;

/**
 * The adaptor for Flink to call Auron native library.
 * Provides Flink-specific context and resource management for native execution.
 */
public class FlinkAuronAdaptor extends AuronAdaptor {

    /**
     * ThreadLocal to store the Flink task context for each execution thread.
     * For MVP, this stores a simple context object. Can be enhanced later.
     */
    private static final ThreadLocal<Object> TASK_CONTEXT = new ThreadLocal<>();

    /**
     * ThreadLocal to store the Flink configuration for each execution thread.
     */
    private static final ThreadLocal<Configuration> CONFIG = new ThreadLocal<>();

    /**
     * Loads the Auron native library from classpath resources.
     * Extracts the library to a temporary file and loads it via System.load().
     */
    @Override
    public void loadAuronLib() {
        String libName = System.mapLibraryName("auron");
        ClassLoader classLoader = AuronAdaptor.class.getClassLoader();
        try {
            InputStream libInputStream = classLoader.getResourceAsStream(libName);
            if (libInputStream == null) {
                throw new IllegalStateException("Native library not found in classpath: " + libName);
            }
            File tempFile = File.createTempFile("libauron-", ".tmp");
            tempFile.deleteOnExit();
            Files.copy(libInputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            libInputStream.close();
            System.load(tempFile.getAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException("Error loading native library: " + e.getMessage(), e);
        }
    }

    /**
     * Retrieves the Flink task context for the current thread.
     *
     * @return The StreamTaskContext for the current thread, or null if not set.
     */
    @Override
    public Object getThreadContext() {
        return TASK_CONTEXT.get();
    }

    /**
     * Sets the Flink task context for the current thread.
     * Also sets the thread name for better debugging.
     *
     * @param context The task context to set (can be null to clear).
     */
    @Override
    public void setThreadContext(Object context) {
        if (context == null) {
            TASK_CONTEXT.remove();
        } else {
            TASK_CONTEXT.set(context);
            // Set thread name for better debugging
            String threadName = "Auron-Flink-Task-" + Thread.currentThread().getId();
            Thread.currentThread().setName(threadName);
        }
    }

    /**
     * Retrieves the JVM total memory limit from Flink configuration.
     * Uses TaskManager's managed memory configuration.
     *
     * @return The maximum allowed memory usage in bytes, or Long.MAX_VALUE if not configured.
     */
    @Override
    public long getJVMTotalMemoryLimited() {
        Configuration config = CONFIG.get();
        if (config == null) {
            // Fallback to default if no configuration is set
            return Long.MAX_VALUE;
        }

        // Get managed memory size from Flink configuration
        MemorySize managedMemory = config.get(TaskManagerOptions.MANAGED_MEMORY_SIZE);
        if (managedMemory != null) {
            return managedMemory.getBytes();
        }

        // If not set, return a reasonable default based on JVM heap
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * Creates a temporary file for direct write spill-to-disk operations.
     * Uses Java's temporary file mechanism as Flink's IOManager is not easily accessible.
     *
     * @return Absolute path of the created temporary file.
     * @throws IOException If the temporary file cannot be created.
     */
    @Override
    public String getDirectWriteSpillToDiskFile() throws IOException {
        // Create temp file with Auron prefix for easy identification
        File tempFile = File.createTempFile("auron-flink-spill-", ".tmp");
        tempFile.deleteOnExit();
        return tempFile.getAbsolutePath();
    }

    /**
     * Retrieves the Auron configuration wrapper for Flink.
     *
     * @return FlinkAuronConfiguration wrapping the current thread's Flink Configuration.
     */
    @Override
    public AuronConfiguration getAuronConfiguration() {
        Configuration config = CONFIG.get();
        if (config == null) {
            // Return a default configuration if none is set
            config = new Configuration();
        }
        return new FlinkAuronConfiguration(config);
    }

    /**
     * Retrieves the UDF wrapper context.
     * UDFs are not supported in the MVP, so this throws UnsupportedOperationException.
     *
     * @param udfSerialized The serialized UDF context (unused).
     * @return Not applicable for MVP.
     * @throws UnsupportedOperationException Always, as UDFs are not supported in MVP.
     */
    @Override
    public AuronUDFWrapperContext getAuronUDFWrapperContext(ByteBuffer udfSerialized) {
        throw new UnsupportedOperationException("UDFs are not supported in Flink MVP");
    }

    /**
     * Sets the Flink configuration for the current thread.
     * This is used by operators to provide configuration context.
     *
     * @param config The Flink Configuration to set for this thread.
     */
    public static void setThreadConfiguration(Configuration config) {
        CONFIG.set(config);
    }

    /**
     * Clears the thread-local configuration.
     * Should be called in cleanup/close methods.
     */
    public static void clearThreadConfiguration() {
        CONFIG.remove();
    }
}
