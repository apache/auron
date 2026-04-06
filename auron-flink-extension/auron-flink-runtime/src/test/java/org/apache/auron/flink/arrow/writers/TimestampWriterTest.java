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
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampNanoVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link TimestampWriter}. */
public class TimestampWriterTest {

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
        try (TimeStampSecVector vector = new TimeStampSecVector("test_ts_sec", allocator)) {
            vector.allocateNew();
            TimestampWriter<RowData> writer = TimestampWriter.forRow(vector, 0);

            // 1704067200000ms = 2024-01-01T00:00:00Z → 1704067200 seconds
            GenericRowData row = GenericRowData.of(TimestampData.fromEpochMillis(1704067200000L));
            writer.write(row, 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(1704067200L, vector.get(0));
        }
    }

    @Test
    public void testWriteMillisecondPrecisionFromRowData() {
        try (TimeStampMilliVector vector = new TimeStampMilliVector("test_ts_milli", allocator)) {
            vector.allocateNew();
            TimestampWriter<RowData> writer = TimestampWriter.forRow(vector, 3);

            GenericRowData row = GenericRowData.of(TimestampData.fromEpochMillis(1704067200123L));
            writer.write(row, 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(1704067200123L, vector.get(0));
        }
    }

    @Test
    public void testWriteMicrosecondPrecisionFromRowData() {
        try (TimeStampMicroVector vector = new TimeStampMicroVector("test_ts_micro", allocator)) {
            vector.allocateNew();
            TimestampWriter<RowData> writer = TimestampWriter.forRow(vector, 6);

            // millis=1704067200123, nanoOfMilli=456000 → micros = 1704067200123*1000 + 456000/1000
            //   = 1704067200123000 + 456 = 1704067200123456
            GenericRowData row = GenericRowData.of(TimestampData.fromEpochMillis(1704067200123L, 456000));
            writer.write(row, 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(1704067200123456L, vector.get(0));
        }
    }

    @Test
    public void testWriteNanosecondPrecisionFromRowData() {
        try (TimeStampNanoVector vector = new TimeStampNanoVector("test_ts_nano", allocator)) {
            vector.allocateNew();
            TimestampWriter<RowData> writer = TimestampWriter.forRow(vector, 9);

            // millis=1704067200123, nanoOfMilli=456789 → nanos = 1704067200123*1_000_000 + 456789
            //   = 1704067200123000000 + 456789 = 1704067200123456789
            GenericRowData row = GenericRowData.of(TimestampData.fromEpochMillis(1704067200123L, 456789));
            writer.write(row, 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(1704067200123456789L, vector.get(0));
        }
    }

    @Test
    public void testWriteNullFromRowData() {
        try (TimeStampMilliVector vector = new TimeStampMilliVector("test_ts_null", allocator)) {
            vector.allocateNew();
            TimestampWriter<RowData> writer = TimestampWriter.forRow(vector, 3);

            GenericRowData row0 = GenericRowData.of(TimestampData.fromEpochMillis(1000L));
            GenericRowData row1 = GenericRowData.of((Object) null);
            GenericRowData row2 = GenericRowData.of(TimestampData.fromEpochMillis(2000L));

            writer.write(row0, 0);
            writer.write(row1, 0);
            writer.write(row2, 0);
            writer.finish();

            assertEquals(3, vector.getValueCount());
            assertFalse(vector.isNull(0));
            assertEquals(1000L, vector.get(0));
            assertTrue(vector.isNull(1));
            assertFalse(vector.isNull(2));
            assertEquals(2000L, vector.get(2));
        }
    }

    @Test
    public void testWritePreEpochMicrosecond() {
        try (TimeStampMicroVector vector = new TimeStampMicroVector("test_ts_pre_epoch_micro", allocator)) {
            vector.allocateNew();
            TimestampWriter<RowData> writer = TimestampWriter.forRow(vector, 6);

            // millis=-1, nanoOfMilli=999000 → micros = -1*1000 + 999000/1000 = -1000 + 999 = -1
            GenericRowData row = GenericRowData.of(TimestampData.fromEpochMillis(-1, 999000));
            writer.write(row, 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(-1L, vector.get(0));
        }
    }

    @Test
    public void testWriteFromArrayData() {
        try (TimeStampMilliVector vector = new TimeStampMilliVector("test_ts_array", allocator)) {
            vector.allocateNew();
            TimestampWriter<ArrayData> writer = TimestampWriter.forArray(vector, 3);

            GenericArrayData array = new GenericArrayData(new TimestampData[] {
                TimestampData.fromEpochMillis(1000L),
                TimestampData.fromEpochMillis(2000L),
                TimestampData.fromEpochMillis(3000L)
            });

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, vector.getValueCount());
            assertEquals(1000L, vector.get(0));
            assertEquals(2000L, vector.get(1));
            assertEquals(3000L, vector.get(2));
        }
    }

    @Test
    public void testWritePreEpochNanosecond() {
        try (TimeStampNanoVector vector = new TimeStampNanoVector("test_ts_pre_epoch_nano", allocator)) {
            vector.allocateNew();
            TimestampWriter<RowData> writer = TimestampWriter.forRow(vector, 9);

            // millis=-1, nanoOfMilli=999000 → nanos = -1*1_000_000 + 999000 = -1000000 + 999000
            //   = -1000
            GenericRowData row = GenericRowData.of(TimestampData.fromEpochMillis(-1, 999000));
            writer.write(row, 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(-1000L, vector.get(0));
        }
    }

    @Test
    public void testResetAndWriteNewBatch() {
        try (TimeStampMilliVector vector = new TimeStampMilliVector("test_ts_reset", allocator)) {
            vector.allocateNew();
            TimestampWriter<RowData> writer = TimestampWriter.forRow(vector, 3);

            // First batch
            writer.write(GenericRowData.of(TimestampData.fromEpochMillis(1000L)), 0);
            writer.write(GenericRowData.of(TimestampData.fromEpochMillis(2000L)), 0);
            writer.finish();

            assertEquals(2, vector.getValueCount());
            assertEquals(1000L, vector.get(0));
            assertEquals(2000L, vector.get(1));

            // Reset
            writer.reset();

            assertEquals(0, vector.getValueCount());

            // Second batch
            writer.write(GenericRowData.of(TimestampData.fromEpochMillis(9999L)), 0);
            writer.finish();

            assertEquals(1, vector.getValueCount());
            assertEquals(9999L, vector.get(0));
        }
    }
}
