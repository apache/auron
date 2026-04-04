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
package org.apache.auron.flink.arrow.writers;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TimeWriter}. */
public class TimeWriterTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    public void tearDown() {
        allocator.close();
    }

    @Test
    public void testWriteSecondPrecisionFromRowData() {
        try (TimeSecVector vector = new TimeSecVector("test_time_sec", allocator)) {
            vector.allocateNew();
            TimeWriter<RowData> writer = TimeWriter.forRow(vector);

            // 3661000ms = 1h 1m 1s → 3661s
            writer.write(GenericRowData.of(3661000), 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(3661, vector.get(0));
        }
    }

    @Test
    public void testWriteMillisecondPrecisionFromRowData() {
        try (TimeMilliVector vector = new TimeMilliVector("test_time_milli", allocator)) {
            vector.allocateNew();
            TimeWriter<RowData> writer = TimeWriter.forRow(vector);

            writer.write(GenericRowData.of(3661000), 0);
            writer.write(GenericRowData.of(0), 0);
            writer.write(GenericRowData.of(86399999), 0);
            writer.finish();

            assertEquals(3, vector.getValueCount());
            assertEquals(3661000, vector.get(0));
            assertEquals(0, vector.get(1));
            assertEquals(86399999, vector.get(2));
        }
    }

    @Test
    public void testWriteMicrosecondPrecisionFromRowData() {
        try (TimeMicroVector vector = new TimeMicroVector("test_time_micro", allocator)) {
            vector.allocateNew();
            TimeWriter<RowData> writer = TimeWriter.forRow(vector);

            writer.write(GenericRowData.of(3661000), 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(3661000L * 1000L, vector.get(0));
        }
    }

    @Test
    public void testWriteNanosecondPrecisionFromRowData() {
        try (TimeNanoVector vector = new TimeNanoVector("test_time_nano", allocator)) {
            vector.allocateNew();
            TimeWriter<RowData> writer = TimeWriter.forRow(vector);

            writer.write(GenericRowData.of(3661000), 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(3661000L * 1_000_000L, vector.get(0));
        }
    }

    @Test
    public void testWriteNullFromRowData() {
        try (TimeMilliVector vector = new TimeMilliVector("test_time_null", allocator)) {
            vector.allocateNew();
            TimeWriter<RowData> writer = TimeWriter.forRow(vector);

            writer.write(GenericRowData.of(3661000), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(1000), 0);
            writer.finish();

            assertEquals(3, vector.getValueCount());
            assertFalse(vector.isNull(0));
            assertEquals(3661000, vector.get(0));
            assertTrue(vector.isNull(1));
            assertFalse(vector.isNull(2));
            assertEquals(1000, vector.get(2));
        }
    }

    @Test
    public void testWriteFromArrayData() {
        try (TimeMilliVector vector = new TimeMilliVector("test_time_array", allocator)) {
            vector.allocateNew();
            TimeWriter<ArrayData> writer = TimeWriter.forArray(vector);

            GenericArrayData array = new GenericArrayData(new int[] {3661000, 0, 86399999});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, vector.getValueCount());
            assertEquals(3661000, vector.get(0));
            assertEquals(0, vector.get(1));
            assertEquals(86399999, vector.get(2));
        }
    }

    @Test
    public void testWriteMicroFromArrayData() {
        try (TimeMicroVector vector = new TimeMicroVector("test_time_micro_array", allocator)) {
            vector.allocateNew();
            TimeWriter<ArrayData> writer = TimeWriter.forArray(vector);

            GenericArrayData array = new GenericArrayData(new int[] {5000, 12345});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.finish();

            assertEquals(2, vector.getValueCount());
            assertEquals(5000L * 1000L, vector.get(0));
            assertEquals(12345L * 1000L, vector.get(1));
        }
    }

    @Test
    public void testResetAndWriteNewBatch() {
        try (TimeMilliVector vector = new TimeMilliVector("test_time_reset", allocator)) {
            vector.allocateNew();
            TimeWriter<RowData> writer = TimeWriter.forRow(vector);

            // First batch
            writer.write(GenericRowData.of(1000), 0);
            writer.write(GenericRowData.of(2000), 0);
            writer.finish();

            assertEquals(2, vector.getValueCount());
            assertEquals(1000, vector.get(0));
            assertEquals(2000, vector.get(1));

            // Reset and write second batch
            writer.reset();

            writer.write(GenericRowData.of(9000), 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(9000, vector.get(0));
        }
    }
}
