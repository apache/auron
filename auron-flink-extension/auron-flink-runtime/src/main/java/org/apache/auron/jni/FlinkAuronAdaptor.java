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
import org.apache.auron.flink.functions.FlinkAuronTaskContext;
import org.apache.auron.functions.AuronUDFWrapperContext;

/**
 * The adaptor for flink to call auron native library.
 */
public class FlinkAuronAdaptor extends AuronAdaptor {

    private final AuronConfiguration auronFlinkConfig = new FlinkAuronConfiguration();

    @Override
    public void loadAuronLib() {
        String libName = System.mapLibraryName("auron");
        ClassLoader classLoader = AuronAdaptor.class.getClassLoader();
        try (InputStream libInputStream = classLoader.getResourceAsStream(libName)) {
            File tempFile = File.createTempFile("libauron-", ".tmp");
            tempFile.deleteOnExit();
            Files.copy(libInputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            System.load(tempFile.getAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException("error loading native libraries: " + e);
        }
    }

    @Override
    public String getDirectWriteSpillToDiskFile() throws IOException {
        File tempFile = File.createTempFile("auron-spill-", ".tmp", new File(System.getenv("PWD")));
        tempFile.deleteOnExit();
        return tempFile.getAbsolutePath();
    }

    /**
     * Returns the {@link FlinkAuronTaskContext} of the operator whose native runtime is being
     * created on this thread, so that the native engine can install it on every worker thread of
     * that runtime's pool.
     *
     * <p>Falls back to the thread's context classloader for a caller that publishes no such
     * context, which is what an operator that creates a native runtime without needing subtask
     * state does.
     */
    @Override
    public Object getThreadContext() {
        FlinkAuronTaskContext taskContext = FlinkAuronTaskContext.current();
        return taskContext != null ? taskContext : Thread.currentThread().getContextClassLoader();
    }

    /**
     * Installs a context produced by {@link #getThreadContext()} on the calling thread.
     *
     * <p>Both shapes must leave the thread with a usable context classloader: a worker that lost it
     * would fail to resolve JVM classes with no signal pointing back here.
     */
    @Override
    public void setThreadContext(Object context) {
        if (context instanceof FlinkAuronTaskContext) {
            FlinkAuronTaskContext taskContext = (FlinkAuronTaskContext) context;
            FlinkAuronTaskContext.setCurrent(taskContext);
            Thread.currentThread().setContextClassLoader(taskContext.getUserCodeClassLoader());
        } else {
            Thread.currentThread().setContextClassLoader((ClassLoader) context);
        }
    }

    @Override
    public AuronConfiguration getAuronConfiguration() {
        return auronFlinkConfig;
    }

    /**
     * Returns the wrapper for this payload from the registry of the subtask the calling thread is
     * working for, building it on the first request of that subtask.
     *
     * <p>The registry cannot be keyed on the payload alone: every subtask of an operator
     * deserializes the same operator bytes, so parallel subtasks in one TaskManager JVM present
     * identical payloads, and a wrapper keeps reusable per-evaluation buffers they must not share.
     */
    @Override
    public AuronUDFWrapperContext getAuronUDFWrapperContext(ByteBuffer byteBuffer) {
        FlinkAuronTaskContext taskContext = FlinkAuronTaskContext.current();
        if (taskContext == null) {
            throw new IllegalStateException("no Flink task context is published on thread "
                    + Thread.currentThread().getName()
                    + "; a UDF wrapper is only reachable from a native runtime an Auron Flink "
                    + "operator created");
        }
        // The buffer the native side hands over is direct, so it has no backing array.
        byte[] payload = new byte[byteBuffer.remaining()];
        byteBuffer.get(payload);
        return taskContext.getOrCreateWrapper(payload);
    }

    @Override
    public String getEngineName() {
        return "Flink";
    }
}
