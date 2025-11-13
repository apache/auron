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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ServiceLoader;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.auron.memory.OnHeapSpillManager;

/**
 * Auron Adaptor abstraction for Auron Native engine.
 * Each engine must implement its own {@link AuronAdaptor} to interact with the Auron Native engine.
 */
public abstract class AuronAdaptor {

    /**
     * The static instance of the AuronAdaptor.
     */
    private static volatile AuronAdaptor INSTANCE = null;

    /**
     * Retrieves the static instance of the AuronAdaptor.
     *
     * @return The current AuronAdaptor instance, or null if none has been set.
     */
    public static AuronAdaptor getInstance() {
        if (INSTANCE == null) {
            synchronized (AuronAdaptor.class) {
                if (INSTANCE == null) {
                    ServiceLoader<AuronAdaptorProvider> loader = ServiceLoader.load(AuronAdaptorProvider.class);
                    for (AuronAdaptorProvider p : loader) {
                        INSTANCE = p.create();
                        break;
                    }
                    if (INSTANCE == null) {
                        throw new IllegalStateException("No AuronAdaptorProvider found");
                    }
                }
            }
        }
        return INSTANCE;
    }

    /**
     * Loads the native Auron library (libauron.dylib or equivalent).
     * <p>This method must be implemented by subclasses to provide the native library loading logic.</p>
     */
    public abstract void loadAuronLib();

    /**
     * Retrieves the limit for JVM total memory usage.
     *
     * @return The maximum allowed memory usage (in bytes), defaulting to Long.MAX_VALUE.
     */
    public long getJVMTotalMemoryLimited() {
        return Long.MAX_VALUE;
    }

    /**
     * Checks if a task is currently running, false to abort native computation.
     *
     * @return true if a task is running, false otherwise. Default implementation returns true.
     */
    public boolean isTaskRunning() {
        return true;
    }

    /**
     * Creates a temporary file for direct write spill-to-disk operations.
     *
     * @return Absolute path of the created temporary file.
     * @throws IOException If the temporary file cannot be created.
     */
    public abstract String getDirectWriteSpillToDiskFile() throws IOException;

    /**
     * Retrieves the context classloader of the current thread.
     *
     * @return For Spark, return TaskContext of the current thread.
     */
    public abstract Object getThreadContext();

    /**
     * Sets the context for the current thread.
     *
     * @param context For spark is TaskContext.
     */
    public abstract void setThreadContext(Object context);

    /**
     * Retrieves the on-heap spill manager implementation.
     *
     * @return An instance of OnHeapSpillManager, defaulting to a disabled manager.
     */
    public OnHeapSpillManager getOnHeapSpillManager() {
        return OnHeapSpillManager.getDisabledOnHeapSpillManager();
    }

    /**
     * Retrieves the AuronConfiguration, It bundles the corresponding engine's Config.
     */
    public abstract AuronConfiguration getAuronConfiguration();

    /**
     * Retrieves the UDF wrapper context. Each engine requires its own implementation.
     *
     * @param udfSerialized The serialized UDF context.
     * @return An instance of AuronUDFWrapperContext.
     * @throws UnsupportedOperationException If the method is not implemented.
     */
    public abstract AuronUDFWrapperContext getAuronUDFWrapperContext(ByteBuffer udfSerialized);
}
