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
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Unit tests for the basic numeric type writers. */
public class BasicWritersTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    public void tearDown() {
        allocator.close();
    }

    // ---- NullWriter ----

    @Nested
    class NullWriterTests {

        private NullVector nullVector;

        @BeforeEach
        public void setUp() {
            nullVector = new NullVector("test_null");
        }

        @AfterEach
        public void tearDown() {
            nullVector.close();
        }

        @Test
        public void testWriteFromRowData() {
            NullWriter<RowData> writer = NullWriter.forRow(nullVector);

            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.finish();

            assertEquals(3, nullVector.getValueCount());
            assertTrue(nullVector.isNull(0));
            assertTrue(nullVector.isNull(1));
            assertTrue(nullVector.isNull(2));
        }

        @Test
        public void testWriteFromArrayData() {
            NullWriter<ArrayData> writer = NullWriter.forArray(nullVector);

            GenericArrayData array = new GenericArrayData(new Object[] {null, null});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.finish();

            assertEquals(2, nullVector.getValueCount());
            assertTrue(nullVector.isNull(0));
            assertTrue(nullVector.isNull(1));
        }
    }

    // ---- BooleanWriter ----

    @Nested
    class BooleanWriterTests {

        private BitVector bitVector;

        @BeforeEach
        public void setUp() {
            bitVector = new BitVector("test_boolean", allocator);
            bitVector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            bitVector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            BooleanWriter<RowData> writer = BooleanWriter.forRow(bitVector);

            writer.write(GenericRowData.of(true), 0);
            writer.write(GenericRowData.of(false), 0);
            writer.write(GenericRowData.of(true), 0);
            writer.finish();

            assertEquals(3, bitVector.getValueCount());
            assertEquals(1, bitVector.get(0));
            assertEquals(0, bitVector.get(1));
            assertEquals(1, bitVector.get(2));
        }

        @Test
        public void testWriteNullFromRowData() {
            BooleanWriter<RowData> writer = BooleanWriter.forRow(bitVector);

            writer.write(GenericRowData.of(true), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(false), 0);
            writer.finish();

            assertEquals(3, bitVector.getValueCount());
            assertFalse(bitVector.isNull(0));
            assertEquals(1, bitVector.get(0));
            assertTrue(bitVector.isNull(1));
            assertFalse(bitVector.isNull(2));
            assertEquals(0, bitVector.get(2));
        }

        @Test
        public void testWriteValuesFromArrayData() {
            BooleanWriter<ArrayData> writer = BooleanWriter.forArray(bitVector);

            GenericArrayData array = new GenericArrayData(new boolean[] {true, false, true});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, bitVector.getValueCount());
            assertEquals(1, bitVector.get(0));
            assertEquals(0, bitVector.get(1));
            assertEquals(1, bitVector.get(2));
        }
    }

    // ---- TinyIntWriter ----

    @Nested
    class TinyIntWriterTests {

        private TinyIntVector tinyIntVector;

        @BeforeEach
        public void setUp() {
            tinyIntVector = new TinyIntVector("test_tinyint", allocator);
            tinyIntVector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            tinyIntVector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            TinyIntWriter<RowData> writer = TinyIntWriter.forRow(tinyIntVector);

            writer.write(GenericRowData.of((byte) 1), 0);
            writer.write(GenericRowData.of((byte) -128), 0);
            writer.write(GenericRowData.of((byte) 127), 0);
            writer.finish();

            assertEquals(3, tinyIntVector.getValueCount());
            assertEquals(1, tinyIntVector.get(0));
            assertEquals(-128, tinyIntVector.get(1));
            assertEquals(127, tinyIntVector.get(2));
        }

        @Test
        public void testWriteNullFromRowData() {
            TinyIntWriter<RowData> writer = TinyIntWriter.forRow(tinyIntVector);

            writer.write(GenericRowData.of((byte) 42), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of((byte) 7), 0);
            writer.finish();

            assertEquals(3, tinyIntVector.getValueCount());
            assertFalse(tinyIntVector.isNull(0));
            assertEquals(42, tinyIntVector.get(0));
            assertTrue(tinyIntVector.isNull(1));
            assertFalse(tinyIntVector.isNull(2));
            assertEquals(7, tinyIntVector.get(2));
        }

        @Test
        public void testWriteValuesFromArrayData() {
            TinyIntWriter<ArrayData> writer = TinyIntWriter.forArray(tinyIntVector);

            GenericArrayData array = new GenericArrayData(new byte[] {10, 20, 30});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, tinyIntVector.getValueCount());
            assertEquals(10, tinyIntVector.get(0));
            assertEquals(20, tinyIntVector.get(1));
            assertEquals(30, tinyIntVector.get(2));
        }
    }

    // ---- SmallIntWriter ----

    @Nested
    class SmallIntWriterTests {

        private SmallIntVector smallIntVector;

        @BeforeEach
        public void setUp() {
            smallIntVector = new SmallIntVector("test_smallint", allocator);
            smallIntVector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            smallIntVector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            SmallIntWriter<RowData> writer = SmallIntWriter.forRow(smallIntVector);

            writer.write(GenericRowData.of((short) 100), 0);
            writer.write(GenericRowData.of((short) -32768), 0);
            writer.write(GenericRowData.of((short) 32767), 0);
            writer.finish();

            assertEquals(3, smallIntVector.getValueCount());
            assertEquals(100, smallIntVector.get(0));
            assertEquals(-32768, smallIntVector.get(1));
            assertEquals(32767, smallIntVector.get(2));
        }

        @Test
        public void testWriteNullFromRowData() {
            SmallIntWriter<RowData> writer = SmallIntWriter.forRow(smallIntVector);

            writer.write(GenericRowData.of((short) 42), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of((short) 99), 0);
            writer.finish();

            assertEquals(3, smallIntVector.getValueCount());
            assertFalse(smallIntVector.isNull(0));
            assertEquals(42, smallIntVector.get(0));
            assertTrue(smallIntVector.isNull(1));
            assertFalse(smallIntVector.isNull(2));
            assertEquals(99, smallIntVector.get(2));
        }

        @Test
        public void testWriteValuesFromArrayData() {
            SmallIntWriter<ArrayData> writer = SmallIntWriter.forArray(smallIntVector);

            GenericArrayData array = new GenericArrayData(new short[] {10, 20, 30});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, smallIntVector.getValueCount());
            assertEquals(10, smallIntVector.get(0));
            assertEquals(20, smallIntVector.get(1));
            assertEquals(30, smallIntVector.get(2));
        }
    }

    // ---- BigIntWriter ----

    @Nested
    class BigIntWriterTests {

        private BigIntVector bigIntVector;

        @BeforeEach
        public void setUp() {
            bigIntVector = new BigIntVector("test_bigint", allocator);
            bigIntVector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            bigIntVector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            BigIntWriter<RowData> writer = BigIntWriter.forRow(bigIntVector);

            writer.write(GenericRowData.of(100L), 0);
            writer.write(GenericRowData.of(Long.MIN_VALUE), 0);
            writer.write(GenericRowData.of(Long.MAX_VALUE), 0);
            writer.finish();

            assertEquals(3, bigIntVector.getValueCount());
            assertEquals(100L, bigIntVector.get(0));
            assertEquals(Long.MIN_VALUE, bigIntVector.get(1));
            assertEquals(Long.MAX_VALUE, bigIntVector.get(2));
        }

        @Test
        public void testWriteNullFromRowData() {
            BigIntWriter<RowData> writer = BigIntWriter.forRow(bigIntVector);

            writer.write(GenericRowData.of(42L), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(99L), 0);
            writer.finish();

            assertEquals(3, bigIntVector.getValueCount());
            assertFalse(bigIntVector.isNull(0));
            assertEquals(42L, bigIntVector.get(0));
            assertTrue(bigIntVector.isNull(1));
            assertFalse(bigIntVector.isNull(2));
            assertEquals(99L, bigIntVector.get(2));
        }

        @Test
        public void testWriteValuesFromArrayData() {
            BigIntWriter<ArrayData> writer = BigIntWriter.forArray(bigIntVector);

            GenericArrayData array = new GenericArrayData(new long[] {10L, 20L, 30L});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, bigIntVector.getValueCount());
            assertEquals(10L, bigIntVector.get(0));
            assertEquals(20L, bigIntVector.get(1));
            assertEquals(30L, bigIntVector.get(2));
        }
    }

    // ---- FloatWriter ----

    @Nested
    class FloatWriterTests {

        private Float4Vector float4Vector;

        @BeforeEach
        public void setUp() {
            float4Vector = new Float4Vector("test_float", allocator);
            float4Vector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            float4Vector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            FloatWriter<RowData> writer = FloatWriter.forRow(float4Vector);

            writer.write(GenericRowData.of(1.5f), 0);
            writer.write(GenericRowData.of(-3.14f), 0);
            writer.write(GenericRowData.of(0.0f), 0);
            writer.finish();

            assertEquals(3, float4Vector.getValueCount());
            assertEquals(1.5f, float4Vector.get(0), 0.0001f);
            assertEquals(-3.14f, float4Vector.get(1), 0.0001f);
            assertEquals(0.0f, float4Vector.get(2), 0.0001f);
        }

        @Test
        public void testWriteNullFromRowData() {
            FloatWriter<RowData> writer = FloatWriter.forRow(float4Vector);

            writer.write(GenericRowData.of(1.5f), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(2.5f), 0);
            writer.finish();

            assertEquals(3, float4Vector.getValueCount());
            assertFalse(float4Vector.isNull(0));
            assertEquals(1.5f, float4Vector.get(0), 0.0001f);
            assertTrue(float4Vector.isNull(1));
            assertFalse(float4Vector.isNull(2));
            assertEquals(2.5f, float4Vector.get(2), 0.0001f);
        }

        @Test
        public void testWriteValuesFromArrayData() {
            FloatWriter<ArrayData> writer = FloatWriter.forArray(float4Vector);

            GenericArrayData array = new GenericArrayData(new float[] {1.0f, 2.0f, 3.0f});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, float4Vector.getValueCount());
            assertEquals(1.0f, float4Vector.get(0), 0.0001f);
            assertEquals(2.0f, float4Vector.get(1), 0.0001f);
            assertEquals(3.0f, float4Vector.get(2), 0.0001f);
        }
    }

    // ---- DoubleWriter ----

    @Nested
    class DoubleWriterTests {

        private Float8Vector float8Vector;

        @BeforeEach
        public void setUp() {
            float8Vector = new Float8Vector("test_double", allocator);
            float8Vector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            float8Vector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            DoubleWriter<RowData> writer = DoubleWriter.forRow(float8Vector);

            writer.write(GenericRowData.of(1.5d), 0);
            writer.write(GenericRowData.of(-3.14d), 0);
            writer.write(GenericRowData.of(0.0d), 0);
            writer.finish();

            assertEquals(3, float8Vector.getValueCount());
            assertEquals(1.5d, float8Vector.get(0), 0.0001d);
            assertEquals(-3.14d, float8Vector.get(1), 0.0001d);
            assertEquals(0.0d, float8Vector.get(2), 0.0001d);
        }

        @Test
        public void testWriteNullFromRowData() {
            DoubleWriter<RowData> writer = DoubleWriter.forRow(float8Vector);

            writer.write(GenericRowData.of(1.5d), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(2.5d), 0);
            writer.finish();

            assertEquals(3, float8Vector.getValueCount());
            assertFalse(float8Vector.isNull(0));
            assertEquals(1.5d, float8Vector.get(0), 0.0001d);
            assertTrue(float8Vector.isNull(1));
            assertFalse(float8Vector.isNull(2));
            assertEquals(2.5d, float8Vector.get(2), 0.0001d);
        }

        @Test
        public void testWriteValuesFromArrayData() {
            DoubleWriter<ArrayData> writer = DoubleWriter.forArray(float8Vector);

            GenericArrayData array = new GenericArrayData(new double[] {1.0d, 2.0d, 3.0d});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, float8Vector.getValueCount());
            assertEquals(1.0d, float8Vector.get(0), 0.0001d);
            assertEquals(2.0d, float8Vector.get(1), 0.0001d);
            assertEquals(3.0d, float8Vector.get(2), 0.0001d);
        }
    }
}
