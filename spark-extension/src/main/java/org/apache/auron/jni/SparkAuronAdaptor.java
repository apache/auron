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
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.auron.memory.OnHeapSpillManager;
import org.apache.auron.spark.configuration.SparkAuronConfiguration;
import org.apache.auron.spark.sql.SparkAuronUDFWrapperContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.SparkEnv$;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.sql.auron.NativeHelper$;
import org.apache.spark.sql.auron.memory.SparkOnHeapSpillManager$;
import org.apache.spark.sql.auron.util.TaskContextHelper$;

/**
 * The adaptor for spark to call auron native library.
 */
public class SparkAuronAdaptor extends AuronAdaptor {
    @Override
    public void loadAuronLib() {
        String libName = System.mapLibraryName("auron");
        ClassLoader classLoader = AuronAdaptor.class.getClassLoader();
        try {
            InputStream libInputStream = classLoader.getResourceAsStream(libName);
            File tempFile = File.createTempFile("libauron-", ".tmp");
            tempFile.deleteOnExit();
            Files.copy(libInputStream, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            System.load(tempFile.getAbsolutePath());
        } catch (IOException e) {
            throw new IllegalStateException("error loading native libraries: " + e);
        }
    }

    @Override
    public Object getThreadContext() {
        return TaskContext$.MODULE$.get();
    }

    @Override
    public void setThreadContext(Object context) {
        TaskContext$.MODULE$.setTaskContext((TaskContext) context);
        TaskContextHelper$.MODULE$.setNativeThreadName();
        TaskContextHelper$.MODULE$.setHDFSCallerContext();
    }

    @Override
    public long getJVMTotalMemoryLimited() {
        return NativeHelper$.MODULE$.totalMemory();
    }

    @Override
    public String getDirectWriteSpillToDiskFile() {
        return SparkEnv.get()
                .blockManager()
                .diskBlockManager()
                .createTempLocalBlock()
                ._2
                .getPath();
    }

    @Override
    public OnHeapSpillManager getOnHeapSpillManager() {
        return SparkOnHeapSpillManager$.MODULE$.current();
    }

    @Override
    public AuronConfiguration getAuronConfiguration() {
        return new SparkAuronConfiguration(SparkEnv$.MODULE$.get().conf());
    }

    @Override
    public AuronUDFWrapperContext getAuronUDFWrapperContext(ByteBuffer udfSerialized) {
        return new SparkAuronUDFWrapperContext(udfSerialized);
    }
}
