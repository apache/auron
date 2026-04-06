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

import java.math.BigDecimal;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Unit tests for the non-numeric type writers. */
public class NonNumericWritersTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    public void tearDown() {
        allocator.close();
    }

    // ---- VarCharWriter ----

    @Nested
    class VarCharWriterTests {

        private VarCharVector varCharVector;

        @BeforeEach
        public void setUp() {
            varCharVector = new VarCharVector("test_varchar", allocator);
            varCharVector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            varCharVector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            VarCharWriter<RowData> writer = VarCharWriter.forRow(varCharVector);

            writer.write(GenericRowData.of(StringData.fromString("hello")), 0);
            writer.write(GenericRowData.of(StringData.fromString("world")), 0);
            writer.write(GenericRowData.of(StringData.fromString("")), 0);
            writer.finish();

            assertEquals(3, varCharVector.getValueCount());
            assertArrayEquals("hello".getBytes(), varCharVector.get(0));
            assertArrayEquals("world".getBytes(), varCharVector.get(1));
            assertArrayEquals("".getBytes(), varCharVector.get(2));
        }

        @Test
        public void testWriteNullFromRowData() {
            VarCharWriter<RowData> writer = VarCharWriter.forRow(varCharVector);

            writer.write(GenericRowData.of(StringData.fromString("hello")), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(StringData.fromString("world")), 0);
            writer.finish();

            assertEquals(3, varCharVector.getValueCount());
            assertFalse(varCharVector.isNull(0));
            assertArrayEquals("hello".getBytes(), varCharVector.get(0));
            assertTrue(varCharVector.isNull(1));
            assertFalse(varCharVector.isNull(2));
            assertArrayEquals("world".getBytes(), varCharVector.get(2));
        }

        @Test
        public void testWriteValuesFromArrayData() {
            VarCharWriter<ArrayData> writer = VarCharWriter.forArray(varCharVector);

            GenericArrayData array = new GenericArrayData(new StringData[] {
                StringData.fromString("foo"), StringData.fromString("bar"), StringData.fromString("baz")
            });

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, varCharVector.getValueCount());
            assertArrayEquals("foo".getBytes(), varCharVector.get(0));
            assertArrayEquals("bar".getBytes(), varCharVector.get(1));
            assertArrayEquals("baz".getBytes(), varCharVector.get(2));
        }
    }

    // ---- VarBinaryWriter ----

    @Nested
    class VarBinaryWriterTests {

        private VarBinaryVector varBinaryVector;

        @BeforeEach
        public void setUp() {
            varBinaryVector = new VarBinaryVector("test_varbinary", allocator);
            varBinaryVector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            varBinaryVector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            VarBinaryWriter<RowData> writer = VarBinaryWriter.forRow(varBinaryVector);

            writer.write(GenericRowData.of(new byte[] {1, 2, 3}), 0);
            writer.write(GenericRowData.of(new byte[] {4, 5}), 0);
            writer.write(GenericRowData.of(new byte[] {}), 0);
            writer.finish();

            assertEquals(3, varBinaryVector.getValueCount());
            assertArrayEquals(new byte[] {1, 2, 3}, varBinaryVector.get(0));
            assertArrayEquals(new byte[] {4, 5}, varBinaryVector.get(1));
            assertArrayEquals(new byte[] {}, varBinaryVector.get(2));
        }

        @Test
        public void testWriteNullFromRowData() {
            VarBinaryWriter<RowData> writer = VarBinaryWriter.forRow(varBinaryVector);

            writer.write(GenericRowData.of(new byte[] {1, 2, 3}), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(new byte[] {7, 8}), 0);
            writer.finish();

            assertEquals(3, varBinaryVector.getValueCount());
            assertFalse(varBinaryVector.isNull(0));
            assertArrayEquals(new byte[] {1, 2, 3}, varBinaryVector.get(0));
            assertTrue(varBinaryVector.isNull(1));
            assertFalse(varBinaryVector.isNull(2));
            assertArrayEquals(new byte[] {7, 8}, varBinaryVector.get(2));
        }

        @Test
        public void testWriteValuesFromArrayData() {
            VarBinaryWriter<ArrayData> writer = VarBinaryWriter.forArray(varBinaryVector);

            GenericArrayData array =
                    new GenericArrayData(new byte[][] {new byte[] {10, 20}, new byte[] {30, 40}, new byte[] {50}});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, varBinaryVector.getValueCount());
            assertArrayEquals(new byte[] {10, 20}, varBinaryVector.get(0));
            assertArrayEquals(new byte[] {30, 40}, varBinaryVector.get(1));
            assertArrayEquals(new byte[] {50}, varBinaryVector.get(2));
        }
    }

    // ---- DecimalWriter ----

    @Nested
    class DecimalWriterTests {

        private DecimalVector decimalVector;

        @BeforeEach
        public void setUp() {
            decimalVector = new DecimalVector("test_decimal", allocator, 10, 2);
            decimalVector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            decimalVector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            DecimalWriter<RowData> writer = DecimalWriter.forRow(decimalVector, 10, 2);

            writer.write(GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2)), 0);
            writer.write(GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("-678.90"), 10, 2)), 0);
            writer.write(GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("0.00"), 10, 2)), 0);
            writer.finish();

            assertEquals(3, decimalVector.getValueCount());
            assertEquals(new BigDecimal("123.45"), decimalVector.getObject(0));
            assertEquals(new BigDecimal("-678.90"), decimalVector.getObject(1));
            assertEquals(new BigDecimal("0.00"), decimalVector.getObject(2));
        }

        @Test
        public void testWriteNullFromRowData() {
            DecimalWriter<RowData> writer = DecimalWriter.forRow(decimalVector, 10, 2);

            writer.write(GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2)), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("99.99"), 10, 2)), 0);
            writer.finish();

            assertEquals(3, decimalVector.getValueCount());
            assertFalse(decimalVector.isNull(0));
            assertEquals(new BigDecimal("123.45"), decimalVector.getObject(0));
            assertTrue(decimalVector.isNull(1));
            assertFalse(decimalVector.isNull(2));
            assertEquals(new BigDecimal("99.99"), decimalVector.getObject(2));
        }

        @Test
        public void testWriteValuesFromArrayData() {
            DecimalWriter<ArrayData> writer = DecimalWriter.forArray(decimalVector, 10, 2);

            GenericArrayData array = new GenericArrayData(new DecimalData[] {
                DecimalData.fromBigDecimal(new BigDecimal("1.11"), 10, 2),
                DecimalData.fromBigDecimal(new BigDecimal("2.22"), 10, 2),
                DecimalData.fromBigDecimal(new BigDecimal("3.33"), 10, 2)
            });

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, decimalVector.getValueCount());
            assertEquals(new BigDecimal("1.11"), decimalVector.getObject(0));
            assertEquals(new BigDecimal("2.22"), decimalVector.getObject(1));
            assertEquals(new BigDecimal("3.33"), decimalVector.getObject(2));
        }
    }

    // ---- DateWriter ----

    @Nested
    class DateWriterTests {

        private DateDayVector dateDayVector;

        @BeforeEach
        public void setUp() {
            dateDayVector = new DateDayVector("test_date", allocator);
            dateDayVector.allocateNew();
        }

        @AfterEach
        public void tearDown() {
            dateDayVector.close();
        }

        @Test
        public void testWriteValuesFromRowData() {
            DateWriter<RowData> writer = DateWriter.forRow(dateDayVector);

            // 19000 days since epoch = ~2022-01-06
            writer.write(GenericRowData.of(19000), 0);
            writer.write(GenericRowData.of(0), 0);
            writer.write(GenericRowData.of(18628), 0);
            writer.finish();

            assertEquals(3, dateDayVector.getValueCount());
            assertEquals(19000, dateDayVector.get(0));
            assertEquals(0, dateDayVector.get(1));
            assertEquals(18628, dateDayVector.get(2));
        }

        @Test
        public void testWriteNullFromRowData() {
            DateWriter<RowData> writer = DateWriter.forRow(dateDayVector);

            writer.write(GenericRowData.of(19000), 0);
            writer.write(GenericRowData.of((Object) null), 0);
            writer.write(GenericRowData.of(18628), 0);
            writer.finish();

            assertEquals(3, dateDayVector.getValueCount());
            assertFalse(dateDayVector.isNull(0));
            assertEquals(19000, dateDayVector.get(0));
            assertTrue(dateDayVector.isNull(1));
            assertFalse(dateDayVector.isNull(2));
            assertEquals(18628, dateDayVector.get(2));
        }

        @Test
        public void testWriteValuesFromArrayData() {
            DateWriter<ArrayData> writer = DateWriter.forArray(dateDayVector);

            GenericArrayData array = new GenericArrayData(new int[] {19000, 0, 18628});

            writer.write(array, 0);
            writer.write(array, 1);
            writer.write(array, 2);
            writer.finish();

            assertEquals(3, dateDayVector.getValueCount());
            assertEquals(19000, dateDayVector.get(0));
            assertEquals(0, dateDayVector.get(1));
            assertEquals(18628, dateDayVector.get(2));
        }
    }
}
