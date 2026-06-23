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
import org.apache.arrow.vector.IntVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link IntWriter}. */
public class IntWriterTest {

    private BufferAllocator allocator;
    private IntVector intVector;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
        intVector = new IntVector("test_int", allocator);
        intVector.allocateNew();
    }

    @AfterEach
    public void tearDown() {
        intVector.close();
        allocator.close();
    }

    @Test
    public void testWriteValuesFromRowData() {
        IntWriter<RowData> writer = IntWriter.forRow(intVector);

        GenericRowData row0 = GenericRowData.of(42);
        GenericRowData row1 = GenericRowData.of(100);
        GenericRowData row2 = GenericRowData.of(-7);

        writer.write(row0, 0);
        writer.write(row1, 0);
        writer.write(row2, 0);
        writer.finish();

        assertEquals(3, intVector.getValueCount());
        assertEquals(42, intVector.get(0));
        assertEquals(100, intVector.get(1));
        assertEquals(-7, intVector.get(2));
    }

    @Test
    public void testWriteNullFromRowData() {
        IntWriter<RowData> writer = IntWriter.forRow(intVector);

        GenericRowData row0 = GenericRowData.of(42);
        GenericRowData row1 = GenericRowData.of((Object) null);
        GenericRowData row2 = GenericRowData.of(99);

        writer.write(row0, 0);
        writer.write(row1, 0);
        writer.write(row2, 0);
        writer.finish();

        assertEquals(3, intVector.getValueCount());
        assertFalse(intVector.isNull(0));
        assertEquals(42, intVector.get(0));
        assertTrue(intVector.isNull(1));
        assertFalse(intVector.isNull(2));
        assertEquals(99, intVector.get(2));
    }

    @Test
    public void testWriteValuesFromArrayData() {
        IntWriter<ArrayData> writer = IntWriter.forArray(intVector);

        GenericArrayData array = new GenericArrayData(new int[] {10, 20, 30});

        writer.write(array, 0);
        writer.write(array, 1);
        writer.write(array, 2);
        writer.finish();

        assertEquals(3, intVector.getValueCount());
        assertEquals(10, intVector.get(0));
        assertEquals(20, intVector.get(1));
        assertEquals(30, intVector.get(2));
    }

    @Test
    public void testWriteNullFromArrayData() {
        IntWriter<ArrayData> writer = IntWriter.forArray(intVector);

        GenericArrayData array = new GenericArrayData(new Integer[] {10, null, 30});

        writer.write(array, 0);
        writer.write(array, 1);
        writer.write(array, 2);
        writer.finish();

        assertEquals(3, intVector.getValueCount());
        assertEquals(10, intVector.get(0));
        assertTrue(intVector.isNull(1));
        assertEquals(30, intVector.get(2));
    }

    @Test
    public void testResetAndWriteNewBatch() {
        IntWriter<RowData> writer = IntWriter.forRow(intVector);

        // First batch
        writer.write(GenericRowData.of(1), 0);
        writer.write(GenericRowData.of(2), 0);
        writer.finish();

        assertEquals(2, intVector.getValueCount());
        assertEquals(1, intVector.get(0));
        assertEquals(2, intVector.get(1));

        // Reset
        writer.reset();

        assertEquals(0, intVector.getValueCount());

        // Second batch
        writer.write(GenericRowData.of(100), 0);
        writer.finish();

        assertEquals(1, intVector.getValueCount());
        assertEquals(100, intVector.get(0));
    }
}
