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
 * Mock class for AuronUDFWrapperContext.
 */
public class MockAuronUDFWrapperContext implements AuronUDFWrapperContext {

    public MockAuronUDFWrapperContext(ByteBuffer udfSerialized) {
        // Mock implementation, We can obtain some information required for initializing the UDF through
        // deserialization.
        byte[] bytes = new byte[udfSerialized.remaining()];
        udfSerialized.get(bytes);
        // Deserialize the UDF information.
        // get the UDF class name and initialize the UDF.
    }

    @Override
    public void eval(long inputPtr, long outputPtr) {
        // Mock implementation, we can use the inputPtr and outputPtr to process the data.
    }
}
