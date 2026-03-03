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
package org.apache.auron.flink.arrow;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link FlinkArrowReader}. */
public class FlinkArrowReaderTest {

    @Test
    public void testBooleanVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testBoolean", 0, Long.MAX_VALUE)) {
            BitVector bitVector = new BitVector("col", allocator);
            bitVector.allocateNew(3);
            bitVector.setSafe(0, 1);
            bitVector.setNull(1);
            bitVector.setSafe(2, 0);
            bitVector.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(bitVector));
            RowType rowType = RowType.of(new BooleanType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals(3, reader.getRowCount());
            assertTrue(reader.read(0).getBoolean(0));
            assertTrue(reader.read(1).isNullAt(0));
            assertFalse(reader.read(2).getBoolean(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testTinyIntVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testTinyInt", 0, Long.MAX_VALUE)) {
            TinyIntVector vec = new TinyIntVector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, 1);
            vec.setNull(1);
            vec.setSafe(2, -1);
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new TinyIntType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals((byte) 1, reader.read(0).getByte(0));
            assertTrue(reader.read(1).isNullAt(0));
            assertEquals((byte) -1, reader.read(2).getByte(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testSmallIntVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testSmallInt", 0, Long.MAX_VALUE)) {
            SmallIntVector vec = new SmallIntVector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, 100);
            vec.setNull(1);
            vec.setSafe(2, -100);
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new SmallIntType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals((short) 100, reader.read(0).getShort(0));
            assertTrue(reader.read(1).isNullAt(0));
            assertEquals((short) -100, reader.read(2).getShort(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testIntVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testInt", 0, Long.MAX_VALUE)) {
            IntVector vec = new IntVector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, 42);
            vec.setNull(1);
            vec.setSafe(2, Integer.MAX_VALUE);
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new IntType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals(42, reader.read(0).getInt(0));
            assertTrue(reader.read(1).isNullAt(0));
            assertEquals(Integer.MAX_VALUE, reader.read(2).getInt(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testBigIntVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testBigInt", 0, Long.MAX_VALUE)) {
            BigIntVector vec = new BigIntVector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, Long.MAX_VALUE);
            vec.setNull(1);
            vec.setSafe(2, -1L);
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new BigIntType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals(Long.MAX_VALUE, reader.read(0).getLong(0));
            assertTrue(reader.read(1).isNullAt(0));
            assertEquals(-1L, reader.read(2).getLong(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testFloatVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testFloat", 0, Long.MAX_VALUE)) {
            Float4Vector vec = new Float4Vector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, 3.14f);
            vec.setNull(1);
            vec.setSafe(2, Float.NaN);
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new FloatType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals(3.14f, reader.read(0).getFloat(0), 0.001f);
            assertTrue(reader.read(1).isNullAt(0));
            assertTrue(Float.isNaN(reader.read(2).getFloat(0)));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testDoubleVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testDouble", 0, Long.MAX_VALUE)) {
            Float8Vector vec = new Float8Vector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, 2.718);
            vec.setNull(1);
            vec.setSafe(2, Double.MAX_VALUE);
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new DoubleType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals(2.718, reader.read(0).getDouble(0), 0.001);
            assertTrue(reader.read(1).isNullAt(0));
            assertEquals(Double.MAX_VALUE, reader.read(2).getDouble(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testVarCharVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testVarChar", 0, Long.MAX_VALUE)) {
            VarCharVector vec = new VarCharVector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
            vec.setNull(1);
            vec.setSafe(2, "".getBytes(StandardCharsets.UTF_8));
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new VarCharType(100));
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            RowData row0 = reader.read(0);
            assertArrayEquals(
                    "hello".getBytes(StandardCharsets.UTF_8), row0.getString(0).toBytes());
            assertTrue(reader.read(1).isNullAt(0));
            assertEquals(0, reader.read(2).getString(0).toBytes().length);

            reader.close();
            root.close();
        }
    }

    @Test
    public void testVarBinaryVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testVarBinary", 0, Long.MAX_VALUE)) {
            VarBinaryVector vec = new VarBinaryVector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, new byte[] {0x01, 0x02});
            vec.setNull(1);
            vec.setSafe(2, new byte[] {});
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new VarBinaryType(100));
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertArrayEquals(new byte[] {0x01, 0x02}, reader.read(0).getBinary(0));
            assertTrue(reader.read(1).isNullAt(0));
            assertArrayEquals(new byte[] {}, reader.read(2).getBinary(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testDecimalVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testDecimal", 0, Long.MAX_VALUE)) {
            // Compact path: precision 10 (<= 18)
            DecimalVector compactVec = new DecimalVector("compact", allocator, 10, 2);
            compactVec.allocateNew(2);
            compactVec.setSafe(0, new BigDecimal("123.45"));
            compactVec.setNull(1);
            compactVec.setValueCount(2);

            VectorSchemaRoot compactRoot = new VectorSchemaRoot(Collections.singletonList(compactVec));
            RowType compactType = RowType.of(new DecimalType(10, 2));
            FlinkArrowReader compactReader = FlinkArrowReader.create(compactRoot, compactType);

            DecimalData compactVal = compactReader.read(0).getDecimal(0, 10, 2);
            assertEquals(new BigDecimal("123.45"), compactVal.toBigDecimal());
            assertTrue(compactReader.read(1).isNullAt(0));

            compactReader.close();
            compactRoot.close();

            // Wide path: precision 20 (> 18)
            DecimalVector wideVec = new DecimalVector("wide", allocator, 20, 2);
            wideVec.allocateNew(1);
            wideVec.setSafe(0, new BigDecimal("123456789012345678.90"));
            wideVec.setValueCount(1);

            VectorSchemaRoot wideRoot = new VectorSchemaRoot(Collections.singletonList(wideVec));
            RowType wideType = RowType.of(new DecimalType(20, 2));
            FlinkArrowReader wideReader = FlinkArrowReader.create(wideRoot, wideType);

            DecimalData wideVal = wideReader.read(0).getDecimal(0, 20, 2);
            assertEquals(0, new BigDecimal("123456789012345678.90").compareTo(wideVal.toBigDecimal()));

            wideReader.close();
            wideRoot.close();
        }
    }

    @Test
    public void testDateVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testDate", 0, Long.MAX_VALUE)) {
            DateDayVector vec = new DateDayVector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, 18000);
            vec.setNull(1);
            vec.setSafe(2, 0);
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new DateType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals(18000, reader.read(0).getInt(0));
            assertTrue(reader.read(1).isNullAt(0));
            assertEquals(0, reader.read(2).getInt(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testTimeVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testTime", 0, Long.MAX_VALUE)) {
            TimeMicroVector vec = new TimeMicroVector("col", allocator);
            vec.allocateNew(3);
            vec.setSafe(0, 45_296_000_000L); // 45296000000 micros -> 45296000 millis
            vec.setNull(1);
            vec.setSafe(2, 0L);
            vec.setValueCount(3);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new TimeType(6));
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            // ArrowTimeColumnVector divides micros by 1000 to get millis
            assertEquals(45_296_000, reader.read(0).getInt(0));
            assertTrue(reader.read(1).isNullAt(0));
            assertEquals(0, reader.read(2).getInt(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testTimestampVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testTimestamp", 0, Long.MAX_VALUE)) {
            TimeStampMicroVector vec = new TimeStampMicroVector("col", allocator);
            vec.allocateNew(2);
            vec.setSafe(0, 1_672_531_200_000_123L); // 2023-01-01T00:00:00.000123
            vec.setNull(1);
            vec.setValueCount(2);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new TimestampType(6));
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            TimestampData ts = reader.read(0).getTimestamp(0, 6);
            // millis = 1672531200000123 / 1000 = 1672531200000
            assertEquals(1_672_531_200_000L, ts.getMillisecond());
            // nanoOfMillisecond = (1672531200000123 % 1000) * 1000 = 123 * 1000 = 123000
            assertEquals(123_000, ts.getNanoOfMillisecond());
            assertTrue(reader.read(1).isNullAt(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testTimestampLtzVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testTimestampLtz", 0, Long.MAX_VALUE)) {
            TimeStampMicroTZVector vec = new TimeStampMicroTZVector("col", allocator, "UTC");
            vec.allocateNew(2);
            vec.setSafe(0, 1_672_531_200_000_123L);
            vec.setNull(1);
            vec.setValueCount(2);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new LocalZonedTimestampType(6));
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            TimestampData ts = reader.read(0).getTimestamp(0, 6);
            assertEquals(1_672_531_200_000L, ts.getMillisecond());
            assertEquals(123_000, ts.getNanoOfMillisecond());
            assertTrue(reader.read(1).isNullAt(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testNegativeTimestamp() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testNegTs", 0, Long.MAX_VALUE)) {
            TimeStampMicroVector vec = new TimeStampMicroVector("col", allocator);
            vec.allocateNew(1);
            vec.setSafe(0, -1500L); // 1.5 millis before epoch
            vec.setValueCount(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new TimestampType(6));
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            TimestampData ts = reader.read(0).getTimestamp(0, 6);
            // floorDiv(-1500, 1000) = -2, floorMod(-1500, 1000) = 500
            assertEquals(-2L, ts.getMillisecond());
            assertEquals(500_000, ts.getNanoOfMillisecond());

            reader.close();
            root.close();
        }
    }

    @Test
    public void testArrayVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testArray", 0, Long.MAX_VALUE)) {
            ListVector listVector = ListVector.empty("col", allocator);
            listVector.addOrGetVector(FieldType.nullable(new ArrowType.Int(32, true)));
            listVector.allocateNew();

            UnionListWriter writer = listVector.getWriter();
            // Row 0: [1, 2, 3]
            writer.setPosition(0);
            writer.startList();
            writer.writeInt(1);
            writer.writeInt(2);
            writer.writeInt(3);
            writer.endList();
            // Row 1: null (set after writer completes)
            writer.setPosition(1);
            writer.startList();
            writer.endList();
            // Row 2: []
            writer.setPosition(2);
            writer.startList();
            writer.endList();
            writer.setValueCount(3);
            listVector.setNull(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(listVector));
            RowType rowType = RowType.of(new ArrayType(new IntType()));
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            // Row 0: [1, 2, 3]
            ArrayData arr0 = reader.read(0).getArray(0);
            assertEquals(3, arr0.size());
            assertEquals(1, arr0.getInt(0));
            assertEquals(2, arr0.getInt(1));
            assertEquals(3, arr0.getInt(2));

            // Row 1: null
            assertTrue(reader.read(1).isNullAt(0));

            // Row 2: []
            ArrayData arr2 = reader.read(2).getArray(0);
            assertEquals(0, arr2.size());

            reader.close();
            root.close();
        }
    }

    @Test
    public void testMapVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testMap", 0, Long.MAX_VALUE)) {
            MapVector mapVector = MapVector.empty("col", allocator, false);
            mapVector.allocateNew();

            BaseWriter.MapWriter mapWriter = mapVector.getWriter();
            // Row 0: {"a" -> 1}
            mapWriter.setPosition(0);
            mapWriter.startMap();
            mapWriter.startEntry();
            byte[] keyBytes = "a".getBytes(StandardCharsets.UTF_8);
            ArrowBuf keyBuf = allocator.buffer(keyBytes.length);
            keyBuf.setBytes(0, keyBytes);
            mapWriter.key().varChar().writeVarChar(0, keyBytes.length, keyBuf);
            keyBuf.close();
            mapWriter.value().integer().writeInt(1);
            mapWriter.endEntry();
            mapWriter.endMap();
            // Row 1: null (write empty map, then mark null)
            mapWriter.setPosition(1);
            mapWriter.startMap();
            mapWriter.endMap();
            // Row 2: {} (empty map)
            mapWriter.setPosition(2);
            mapWriter.startMap();
            mapWriter.endMap();
            mapVector.setValueCount(3);
            mapVector.setNull(1);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(mapVector));
            RowType rowType = RowType.of(new MapType(new VarCharType(100), new IntType()));
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            // Row 0: {"a" -> 1}
            MapData map0 = reader.read(0).getMap(0);
            assertEquals(1, map0.size());

            // Row 1: null
            assertTrue(reader.read(1).isNullAt(0));

            // Row 2: empty
            MapData map2 = reader.read(2).getMap(0);
            assertEquals(0, map2.size());

            reader.close();
            root.close();
        }
    }

    @Test
    public void testRowVector() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testRow", 0, Long.MAX_VALUE)) {
            FieldType intFieldType = FieldType.nullable(new ArrowType.Int(32, true));
            FieldType utf8FieldType = FieldType.nullable(ArrowType.Utf8.INSTANCE);
            StructVector structVector = StructVector.empty("col", allocator);
            structVector.addOrGet("f0", intFieldType, IntVector.class);
            structVector.addOrGet("f1", utf8FieldType, VarCharVector.class);

            IntVector intChild = (IntVector) structVector.getChild("f0");
            VarCharVector strChild = (VarCharVector) structVector.getChild("f1");

            structVector.allocateNew();
            structVector.setIndexDefined(0);
            intChild.setSafe(0, 42);
            strChild.setSafe(0, "hello".getBytes(StandardCharsets.UTF_8));
            structVector.setNull(1);
            intChild.setNull(1);
            strChild.setNull(1);
            structVector.setValueCount(2);
            intChild.setValueCount(2);
            strChild.setValueCount(2);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(structVector));
            RowType innerType =
                    RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"f0", "f1"});
            RowType rowType = RowType.of(new LogicalType[] {innerType}, new String[] {"col"});
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            // Row 0: struct(42, "hello")
            RowData nested0 = reader.read(0).getRow(0, 2);
            assertEquals(42, nested0.getInt(0));
            assertArrayEquals(
                    "hello".getBytes(StandardCharsets.UTF_8),
                    nested0.getString(1).toBytes());

            // Row 1: null struct
            assertTrue(reader.read(1).isNullAt(0));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testMultiColumnBatch() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testMultiCol", 0, Long.MAX_VALUE)) {
            IntVector intVec = new IntVector("id", allocator);
            intVec.allocateNew(3);
            intVec.setSafe(0, 1);
            intVec.setSafe(1, 2);
            intVec.setSafe(2, 3);
            intVec.setValueCount(3);

            VarCharVector strVec = new VarCharVector("name", allocator);
            strVec.allocateNew(3);
            strVec.setSafe(0, "alice".getBytes(StandardCharsets.UTF_8));
            strVec.setSafe(1, "bob".getBytes(StandardCharsets.UTF_8));
            strVec.setNull(2);
            strVec.setValueCount(3);

            BitVector boolVec = new BitVector("active", allocator);
            boolVec.allocateNew(3);
            boolVec.setSafe(0, 1);
            boolVec.setSafe(1, 0);
            boolVec.setSafe(2, 1);
            boolVec.setValueCount(3);

            List<FieldVector> vectors = Arrays.asList(intVec, strVec, boolVec);
            VectorSchemaRoot root = new VectorSchemaRoot(vectors);
            RowType rowType = RowType.of(
                    new LogicalType[] {new IntType(), new VarCharType(100), new BooleanType()},
                    new String[] {"id", "name", "active"});
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals(3, reader.getRowCount());

            RowData row0 = reader.read(0);
            assertEquals(1, row0.getInt(0));
            assertArrayEquals(
                    "alice".getBytes(StandardCharsets.UTF_8), row0.getString(1).toBytes());
            assertTrue(row0.getBoolean(2));

            RowData row1 = reader.read(1);
            assertEquals(2, row1.getInt(0));
            assertArrayEquals(
                    "bob".getBytes(StandardCharsets.UTF_8), row1.getString(1).toBytes());
            assertFalse(row1.getBoolean(2));

            RowData row2 = reader.read(2);
            assertEquals(3, row2.getInt(0));
            assertTrue(row2.isNullAt(1));
            assertTrue(row2.getBoolean(2));

            reader.close();
            root.close();
        }
    }

    @Test
    public void testEmptyBatch() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testEmpty", 0, Long.MAX_VALUE)) {
            IntVector vec = new IntVector("col", allocator);
            vec.allocateNew(0);
            vec.setValueCount(0);

            VectorSchemaRoot root = new VectorSchemaRoot(Collections.singletonList(vec));
            RowType rowType = RowType.of(new IntType());
            FlinkArrowReader reader = FlinkArrowReader.create(root, rowType);

            assertEquals(0, reader.getRowCount());

            reader.close();
            root.close();
        }
    }

    @Test
    public void testResetWithNewRoot() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testReset", 0, Long.MAX_VALUE)) {
            // Batch 1
            IntVector vec1 = new IntVector("col", allocator);
            vec1.allocateNew(2);
            vec1.setSafe(0, 10);
            vec1.setSafe(1, 20);
            vec1.setValueCount(2);
            VectorSchemaRoot root1 = new VectorSchemaRoot(Collections.singletonList(vec1));

            RowType rowType = RowType.of(new IntType());
            FlinkArrowReader reader = FlinkArrowReader.create(root1, rowType);

            assertEquals(10, reader.read(0).getInt(0));
            assertEquals(20, reader.read(1).getInt(0));

            // Batch 2 — different values, same schema
            IntVector vec2 = new IntVector("col", allocator);
            vec2.allocateNew(2);
            vec2.setSafe(0, 99);
            vec2.setSafe(1, 100);
            vec2.setValueCount(2);
            VectorSchemaRoot root2 = new VectorSchemaRoot(Collections.singletonList(vec2));

            reader.reset(root2);
            assertEquals(2, reader.getRowCount());
            assertEquals(99, reader.read(0).getInt(0));
            assertEquals(100, reader.read(1).getInt(0));

            reader.close();
            root1.close();
            root2.close();
        }
    }

    @Test
    public void testUnsupportedTypeThrows() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testUnsupported", 0, Long.MAX_VALUE)) {
            IntVector vec = new IntVector("col", allocator);
            vec.allocateNew(1);
            vec.setSafe(0, 1);
            vec.setValueCount(1);

            assertThrows(
                    UnsupportedOperationException.class,
                    () -> FlinkArrowReader.createColumnVector(
                            vec, new RawType<>(String.class, StringSerializer.INSTANCE)));

            vec.close();
        }
    }
}
