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
package org.apache.auron.flink.jni;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.nio.ByteBuffer;
import org.apache.auron.flink.functions.FlinkAuronUDFWrapperContext;
import org.apache.auron.flink.functions.FlinkUDFPayload;
import org.apache.auron.functions.AuronUDFWrapperContext;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.jni.FlinkAuronAdaptor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

/**
 * This class is used to test the FlinkAuronAdaptor class.
 */
public class FlinkAuronAdaptorTest {

    @Test
    public void testCreateAuronAdaptor() {
        AuronAdaptor flinkAuronAdaptor = AuronAdaptor.getInstance();
        assertInstanceOf(
                FlinkAuronAdaptor.class, flinkAuronAdaptor, "SPI should discover and instantiate FlinkAuronAdaptor");
    }

    /**
     * Contract: the Flink adaptor builds a {@link FlinkAuronUDFWrapperContext} from a serialized
     * UDF payload rather than rejecting the call.
     */
    @Test
    public void testGetAuronUDFWrapperContextReturnsWrapper() throws Exception {
        FlinkUDFPayload payload = new FlinkUDFPayload(
                new PlusOneFunction(), new DataType[] {DataTypes.INT()}, DataTypes.INT(), new String[] {
                    Integer.class.getName()
                });
        byte[] bytes = InstantiationUtil.serializeObject(payload);
        ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
        buffer.put(bytes);
        buffer.flip();

        AuronUDFWrapperContext context = AuronAdaptor.getInstance().getAuronUDFWrapperContext(buffer);

        assertInstanceOf(FlinkAuronUDFWrapperContext.class, context);
    }

    /** Minimal serializable UDF fixture; static so it captures no enclosing test instance. */
    public static class PlusOneFunction extends ScalarFunction {
        private static final long serialVersionUID = 1L;

        public Integer eval(Integer value) {
            return value == null ? null : value + 1;
        }
    }
}
