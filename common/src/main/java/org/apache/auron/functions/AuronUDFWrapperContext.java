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
package org.apache.auron.functions;

import java.nio.ByteBuffer;

/**
 * Abstract wrapper context for user-defined functions (UDFs).
 * This class bridges different engines and native UDF implementations.
 * SQL engines such as Spark and Flink should provide their respective implementations based on this.
 */
public abstract class AuronUDFWrapperContext {

    /**
     * Constructor that initializes the UDF wrapper context with a ByteBuffer.
     *
     * @param buffer The ByteBuffer containing serialized UDF configuration or data
     */
    public AuronUDFWrapperContext(ByteBuffer buffer) {}

    /**
     * Evaluates the UDF with the provided input and output pointers.
     * This method is called for each invocation of the UDF during query execution.
     *
     * @param inputPtr Native pointer to the input data
     * @param outputPtr Native pointer to the output location where results should be written
     */
    public abstract void eval(long inputPtr, long outputPtr);
}
