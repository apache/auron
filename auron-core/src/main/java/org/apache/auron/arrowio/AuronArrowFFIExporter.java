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
package org.apache.auron.arrowio;

/**
 * Abstract class for exporting data to Arrow format via FFI (Foreign Function Interface).
 * This class serves as a bridge between SQL's execution engine and Arrow data structures,
 * allowing efficient data transfer between JVM and native code.
 */
public abstract class AuronArrowFFIExporter implements AutoCloseable {

    /**
     * Exports the next batch of data to Arrow format.
     * This method is called by the native code to fetch the next batch of data
     * from the JVM side and convert it to Arrow format.
     *
     * @param contextPtr Native pointer to the Arrow FFI context
     * @return true if there is more data to export, false if all data has been exported
     */
    public abstract boolean exportNextBatch(long contextPtr);
}
