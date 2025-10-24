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
package org.apache.spark.sql.auron;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.auron.hadoop.fs.FSDataInputWrapper;
import org.apache.auron.hadoop.fs.FSDataInputWrapper$;
import org.apache.auron.hadoop.fs.FSDataOutputWrapper;
import org.apache.auron.hadoop.fs.FSDataOutputWrapper$;
import org.apache.auron.memory.OnHeapSpillManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.sql.auron.memory.SparkOnHeapSpillManager$;
import org.apache.spark.sql.auron.util.TaskContextHelper$;

/**
 * This class has been deprecated and migrated to {@link org.apache.auron.jni.JniBridge}.
 * Will be removed in the future.
 */
@Deprecated
@SuppressWarnings("unused")
public class JniBridge {

    @Deprecated
    public static final ConcurrentHashMap<String, Object> resourcesMap = new ConcurrentHashMap<>();

    @Deprecated
    public static native long callNative(long initNativeMemory, String logLevel, AuronCallNativeWrapper wrapper);

    @Deprecated
    public static native boolean nextBatch(long ptr);

    @Deprecated
    public static native void finalizeNative(long ptr);

    @Deprecated
    public static native void onExit();

    @Deprecated
    public static ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }

    @Deprecated
    public static void setContextClassLoader(ClassLoader cl) {
        Thread.currentThread().setContextClassLoader(cl);
    }

    @Deprecated
    public static Object getResource(String key) {
        return resourcesMap.remove(key);
    }

    @Deprecated
    public static TaskContext getTaskContext() {
        return TaskContext$.MODULE$.get();
    }

    @Deprecated
    public static OnHeapSpillManager getTaskOnHeapSpillManager() {
        return SparkOnHeapSpillManager$.MODULE$.current();
    }

    @Deprecated
    public static boolean isTaskRunning() {
        TaskContext tc = getTaskContext();
        if (tc == null) { // driver is always running
            return true;
        }
        return !tc.isCompleted() && !tc.isInterrupted();
    }

    @Deprecated
    public static FSDataInputWrapper openFileAsDataInputWrapper(FileSystem fs, String path) throws Exception {
        // the path is a URI string, so we need to convert it to a URI object, ref:
        // org.apache.spark.paths.SparkPath.toPath
        return FSDataInputWrapper$.MODULE$.wrap(fs.open(new Path(new URI(path))));
    }

    @Deprecated
    public static FSDataOutputWrapper createFileAsDataOutputWrapper(FileSystem fs, String path) throws Exception {
        return FSDataOutputWrapper$.MODULE$.wrap(fs.create(new Path(new URI(path))));
    }

    @Deprecated
    private static final List<BufferPoolMXBean> directMXBeans =
            ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);

    @Deprecated
    public static long getTotalMemoryLimited() {
        return NativeHelper$.MODULE$.totalMemory();
    }

    @Deprecated
    public static long getDirectMemoryUsed() {
        return directMXBeans.stream()
                .mapToLong(BufferPoolMXBean::getTotalCapacity)
                .sum();
    }

    @Deprecated
    public static String getDirectWriteSpillToDiskFile() {
        return SparkEnv.get()
                .blockManager()
                .diskBlockManager()
                .createTempLocalBlock()
                ._2
                .getPath();
    }

    @Deprecated
    public static void initNativeThread(ClassLoader cl, TaskContext tc) {
        setContextClassLoader(cl);
        TaskContext$.MODULE$.setTaskContext(tc);
        TaskContextHelper$.MODULE$.setNativeThreadName();
        TaskContextHelper$.MODULE$.setHDFSCallerContext();
    }

    @Deprecated
    public static AuronUDFWrapperContext getAuronUDFWrapperContext(ByteBuffer udfSerialized) {
        throw new UnsupportedOperationException("This API is designed to support next-generation multi-engine.");
    }
}
