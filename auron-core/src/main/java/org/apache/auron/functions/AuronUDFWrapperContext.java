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

/**
 * Wrapper context for user-defined functions (UDFs).
 * This class bridges different engines and native UDF implementations.
 * SQL engines such as Spark and Flink should provide their respective implementations based on this.
 */
public interface AuronUDFWrapperContext {

    /**
     * Opens the UDF context with the given resource ID.
     * The Flink engine requires the FunctionContext, which can be obtained via the ResourceID, to initialize the Flink ScalarFunction.
     * @param resourceId
     */
    default void open(String resourceId) {}

    /**
     * Evaluates the UDF with the provided input and output pointers.
     * This method is called for each invocation of the UDF during query execution.
     *
     * @param inputPtr Native pointer to the input data
     * @param outputPtr Native pointer to the output location where results should be written
     */
    void eval(long inputPtr, long outputPtr);

    /**
     * Closes the UDF context.
     * Some UDFs may need to perform resource cleanup operations.
     */
    default void close() {}
}
