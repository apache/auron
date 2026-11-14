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
package org.apache.spark.sql.execution.auron.plan;

/**
 * JNI entry points called by the native PaimonSinkExec.
 *
 * <p>Paimon specific logic (file naming, statistics, commit messages) lives in the
 * auron-paimon module; this class only forwards calls to the {@link PaimonSinkTaskContext}
 * registered for the current task, so that the native engine can resolve it without depending
 * on Paimon classes.
 */
@SuppressWarnings("unused")
public class NativePaimonSinkUtils {

    /** Called by native code before opening a data file. Returns the absolute output path. */
    public static String getTaskOutputPath() throws Exception {
        return PaimonSinkTaskContext.get().nextOutputPath();
    }

    /** Called by native code after a data file is closed. */
    public static void completeFile(String path, long numRows, long numBytes) throws Exception {
        PaimonSinkTaskContext.get().completeFile(path, numRows, numBytes);
    }
}
