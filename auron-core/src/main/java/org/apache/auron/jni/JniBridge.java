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
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.auron.hadoop.fs.FSDataInputWrapper;
import org.apache.auron.hadoop.fs.FSDataOutputWrapper;
import org.apache.auron.memory.OnHeapSpillManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * This class is the entry point for the JNI bridge.
 */
@SuppressWarnings("unused")
public class JniBridge {
    private static final ConcurrentHashMap<String, Object> resourcesMap = new ConcurrentHashMap<>();

    private static final List<BufferPoolMXBean> directMXBeans =
            ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

    public static native long callNative(long initNativeMemory, String logLevel, AuronCallNativeWrapper wrapper);

    public static native boolean nextBatch(long ptr);

    public static native void finalizeNative(long ptr);

    public static native void onExit();

    public static ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    public static void setContextClassLoader(ClassLoader cl) {
        Thread.currentThread().setContextClassLoader(cl);
    }

    public static Object getResource(String key) {
        return resourcesMap.remove(key);
    }

    public static void putResource(String key, Object value) {
        resourcesMap.put(key, value);
    }

    public static FSDataInputWrapper openFileAsDataInputWrapper(FileSystem fs, String path) throws Exception {
        // the path is a URI string, so we need to convert it to a URI object
        return FSDataInputWrapper.wrap(fs.open(new Path(new URI(path))));
    }

    public static FSDataOutputWrapper createFileAsDataOutputWrapper(FileSystem fs, String path) throws Exception {
        return FSDataOutputWrapper.wrap(fs.create(new Path(new URI(path))));
    }

    public static long getDirectMemoryUsed() {
        return directMXBeans.stream()
                .mapToLong(BufferPoolMXBean::getTotalCapacity)
                .sum();
    }

    public static OnHeapSpillManager getTaskOnHeapSpillManager() {
        return AuronAdaptor.getInstance().getOnHeapSpillManager();
    }

    public static long getTotalMemoryLimited() {
        return AuronAdaptor.getInstance().getJVMTotalMemoryLimited();
    }

    public static boolean isTaskRunning() {
        return AuronAdaptor.getInstance().isTaskRunning();
    }

    public static Object getThreadContext() {
        return AuronAdaptor.getInstance().getThreadContext();
    }

    public static void setThreadContext(Object tc) {
        AuronAdaptor.getInstance().setThreadContext(tc);
    }

    public static String getDirectWriteSpillToDiskFile() throws IOException {
        return AuronAdaptor.getInstance().getDirectWriteSpillToDiskFile();
    }

    public static AuronUDFWrapperContext getAuronUDFWrapperContext(ByteBuffer udfSerialized) {
        return AuronAdaptor.getInstance().getAuronUDFWrapperContext(udfSerialized);
    }
}
