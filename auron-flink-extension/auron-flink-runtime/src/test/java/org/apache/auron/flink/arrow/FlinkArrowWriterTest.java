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
import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
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
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link FlinkArrowWriter}. */
public class FlinkArrowWriterTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    public void tearDown() {
        allocator.close();
    }

    /** Writes one row containing all 12 basic types and verifies each vector. */
    @Test
    public void testWriteAllBasicTypes() {
        RowType rowType = RowType.of(
                new LogicalType[] {
                    new NullType(),
                    new BooleanType(),
                    new TinyIntType(),
                    new SmallIntType(),
                    new IntType(),
                    new BigIntType(),
                    new FloatType(),
                    new DoubleType(),
                    new VarCharType(100),
                    new VarBinaryType(100),
                    new DecimalType(10, 2),
                    new DateType()
                },
                new String[] {
                    "f_null", "f_bool", "f_tinyint", "f_smallint", "f_int", "f_bigint",
                    "f_float", "f_double", "f_varchar", "f_varbinary", "f_decimal", "f_date"
                });

        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            GenericRowData row = new GenericRowData(12);
            row.setField(0, null);
            row.setField(1, true);
            row.setField(2, (byte) 42);
            row.setField(3, (short) 1000);
            row.setField(4, 123456);
            row.setField(5, 9876543210L);
            row.setField(6, 3.14f);
            row.setField(7, 2.71828d);
            row.setField(8, StringData.fromString("hello"));
            row.setField(9, new byte[] {1, 2, 3});
            row.setField(10, DecimalData.fromBigDecimal(new BigDecimal("123.45"), 10, 2));
            row.setField(11, 19000); // days since epoch

            writer.write(row);
            writer.finish();

            assertEquals(1, root.getRowCount());

            // Null
            assertTrue(((NullVector) root.getVector("f_null")).isNull(0));
            // Boolean
            assertEquals(1, ((BitVector) root.getVector("f_bool")).get(0));
            // TinyInt
            assertEquals(42, ((TinyIntVector) root.getVector("f_tinyint")).get(0));
            // SmallInt
            assertEquals(1000, ((SmallIntVector) root.getVector("f_smallint")).get(0));
            // Int
            assertEquals(123456, ((IntVector) root.getVector("f_int")).get(0));
            // BigInt
            assertEquals(9876543210L, ((BigIntVector) root.getVector("f_bigint")).get(0));
            // Float
            assertEquals(3.14f, ((Float4Vector) root.getVector("f_float")).get(0), 0.001f);
            // Double
            assertEquals(2.71828d, ((Float8Vector) root.getVector("f_double")).get(0), 0.00001d);
            // VarChar
            assertEquals(
                    "hello", new String(((VarCharVector) root.getVector("f_varchar")).get(0), StandardCharsets.UTF_8));
            // VarBinary
            assertArrayEquals(new byte[] {1, 2, 3}, ((VarBinaryVector) root.getVector("f_varbinary")).get(0));
            // Decimal
            assertEquals(new BigDecimal("123.45"), ((DecimalVector) root.getVector("f_decimal")).getObject(0));
            // Date
            assertEquals(19000, ((DateDayVector) root.getVector("f_date")).get(0));
        }
    }

    /** Writes a row where all nullable fields are null. */
    @Test
    public void testWriteNullValues() {
        RowType rowType = RowType.of(
                new LogicalType[] {new BooleanType(), new IntType(), new VarCharType(100), new DecimalType(10, 2)},
                new String[] {"f_bool", "f_int", "f_varchar", "f_decimal"});

        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            GenericRowData row = new GenericRowData(4);
            row.setField(0, null);
            row.setField(1, null);
            row.setField(2, null);
            row.setField(3, null);

            writer.write(row);
            writer.finish();

            assertEquals(1, root.getRowCount());
            assertTrue(((BitVector) root.getVector("f_bool")).isNull(0));
            assertTrue(((IntVector) root.getVector("f_int")).isNull(0));
            assertTrue(((VarCharVector) root.getVector("f_varchar")).isNull(0));
            assertTrue(((DecimalVector) root.getVector("f_decimal")).isNull(0));
        }
    }

    /** Writes multiple rows, then resets and writes again. */
    @Test
    public void testWriteMultipleRowsAndReset() {
        RowType rowType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"id", "name"});

        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // First batch: 3 rows
            writer.write(GenericRowData.of(1, StringData.fromString("alice")));
            writer.write(GenericRowData.of(2, StringData.fromString("bob")));
            writer.write(GenericRowData.of(3, StringData.fromString("carol")));
            writer.finish();

            assertEquals(3, root.getRowCount());
            IntVector idVector = (IntVector) root.getVector("id");
            VarCharVector nameVector = (VarCharVector) root.getVector("name");
            assertEquals(1, idVector.get(0));
            assertEquals(2, idVector.get(1));
            assertEquals(3, idVector.get(2));
            assertEquals("alice", new String(nameVector.get(0), StandardCharsets.UTF_8));
            assertEquals("bob", new String(nameVector.get(1), StandardCharsets.UTF_8));
            assertEquals("carol", new String(nameVector.get(2), StandardCharsets.UTF_8));

            // Reset and write second batch: 2 rows
            writer.reset();
            writer.write(GenericRowData.of(10, StringData.fromString("dave")));
            writer.write(GenericRowData.of(20, StringData.fromString("eve")));
            writer.finish();

            assertEquals(2, root.getRowCount());
            assertEquals(10, idVector.get(0));
            assertEquals(20, idVector.get(1));
            assertEquals("dave", new String(nameVector.get(0), StandardCharsets.UTF_8));
            assertEquals("eve", new String(nameVector.get(1), StandardCharsets.UTF_8));
        }
    }

    /** Finish without writing any rows produces an empty batch. */
    @Test
    public void testEmptyBatch() {
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            writer.finish();

            assertEquals(0, root.getRowCount());
        }
    }

    /** Finish on a schema with zero columns produces an empty batch. */
    @Test
    public void testEmptyBatchZeroColumns() {
        RowType rowType = RowType.of(new LogicalType[] {}, new String[] {});

        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            writer.finish();

            assertEquals(0, root.getRowCount());
        }
    }

    /** Unsupported types throw UnsupportedOperationException. */
    @Test
    public void testUnsupportedTypeThrows() {
        // MultisetType is not supported by toArrowType, so this should throw
        RowType rowType = RowType.of(new LogicalType[] {new MultisetType(new IntType())}, new String[] {"f_multiset"});

        assertThrows(UnsupportedOperationException.class, () -> FlinkArrowUtils.toArrowSchema(rowType));
    }

    /** Writes one row containing Time, Timestamp, Array, Map, and Row types. */
    @Test
    public void testWriteTemporalAndComplexTypes() {
        RowType innerRowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"nested_id"});

        RowType rowType = RowType.of(
                new LogicalType[] {
                    new TimeType(6), // TIME(6) → TimeMicroVector
                    new TimestampType(6), // TIMESTAMP(6) → TimeStampMicroVector
                    new LocalZonedTimestampType(3), // TIMESTAMP_LTZ(3) → TimeStampMilliVector
                    new ArrayType(new IntType()), // ARRAY<INT>
                    new MapType(new VarCharType(100), new IntType()), // MAP<VARCHAR, INT>
                    innerRowType // ROW<nested_id INT>
                },
                new String[] {"f_time", "f_ts", "f_ts_ltz", "f_array", "f_map", "f_row"});

        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            GenericRowData row = new GenericRowData(6);
            row.setField(0, 3661000); // TIME: 1h 1m 1s in ms
            row.setField(1, TimestampData.fromEpochMillis(1704067200123L, 456000)); // TIMESTAMP(6)
            row.setField(2, TimestampData.fromEpochMillis(1704067200000L)); // TIMESTAMP_LTZ(3)
            row.setField(3, new GenericArrayData(new int[] {10, 20, 30})); // ARRAY
            Map<StringData, Integer> map = new HashMap<>();
            map.put(StringData.fromString("key1"), 100);
            row.setField(4, new GenericMapData(map)); // MAP
            row.setField(5, GenericRowData.of(42)); // ROW

            writer.write(row);
            writer.finish();

            assertEquals(1, root.getRowCount());

            // TIME(6) → micro: 3661000 * 1000 = 3661000000
            assertEquals(3661000L * 1000, ((TimeMicroVector) root.getVector("f_time")).get(0));

            // TIMESTAMP(6) → micro: 1704067200123 * 1000 + 456000 / 1000 = 1704067200123456
            assertEquals(1704067200123L * 1000 + 456, ((TimeStampMicroVector) root.getVector("f_ts")).get(0));

            // TIMESTAMP_LTZ(3) → milli
            assertEquals(1704067200000L, ((TimeStampMilliVector) root.getVector("f_ts_ltz")).get(0));

            // ARRAY
            ListVector arrayVec = (ListVector) root.getVector("f_array");
            assertFalse(arrayVec.isNull(0));

            // MAP
            assertFalse(root.getVector("f_map").isNull(0));

            // ROW
            assertFalse(root.getVector("f_row").isNull(0));
        }
    }

    /** The root returned by getRoot() is the same instance passed to create(). */
    @Test
    public void testGetRoot() {
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);
            assertSame(root, writer.getRoot());
        }
    }
}
