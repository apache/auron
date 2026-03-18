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

/**
 * The adaptor for flink to call auron native library.
 */
public class FlinkAuronAdaptor extends AuronAdaptor {

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
    public String getDirectWriteSpillToDiskFile() throws IOException {
        File tempFile = File.createTempFile("auron-spill-", ".tmp", new File(System.getenv("PWD")));
        tempFile.deleteOnExit();
        return tempFile.getAbsolutePath();
    }

    @Override
    public Object getThreadContext() {
        return Thread.currentThread().getContextClassLoader();
    }

    @Override
    public void setThreadContext(Object context) {
        Thread.currentThread().setContextClassLoader((ClassLoader) context);
    }

    @Override
    public AuronConfiguration getAuronConfiguration() {
        return new FlinkAuronConfiguration();
    }

    @Override
    public AuronUDFWrapperContext getAuronUDFWrapperContext(ByteBuffer byteBuffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getEngineName() {
        return "Flink";
    }
}
