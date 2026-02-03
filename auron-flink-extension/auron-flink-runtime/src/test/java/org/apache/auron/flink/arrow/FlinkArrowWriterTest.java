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

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for FlinkArrowWriter. */
public class FlinkArrowWriterTest {

    private BufferAllocator allocator;

    @BeforeEach
    public void setUp() {
        allocator = FlinkArrowUtils.createChildAllocator("test");
    }

    @AfterEach
    public void tearDown() {
        allocator.close();
    }

    @Test
    public void testWriteBasicTypes() {
        RowType rowType = RowType.of(
                new LogicalType[] {new IntType(), new VarCharType(100), new DoubleType(), new BooleanType()},
                new String[] {"id", "name", "score", "active"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Write first row
            GenericRowData row1 = new GenericRowData(4);
            row1.setField(0, 1);
            row1.setField(1, StringData.fromString("Alice"));
            row1.setField(2, 95.5);
            row1.setField(3, true);
            writer.write(row1);

            // Write second row
            GenericRowData row2 = new GenericRowData(4);
            row2.setField(0, 2);
            row2.setField(1, StringData.fromString("Bob"));
            row2.setField(2, 87.3);
            row2.setField(3, false);
            writer.write(row2);

            writer.finish();

            assertEquals(2, root.getRowCount());
            assertEquals(1, root.getVector("id").getObject(0));
            assertEquals(2, root.getVector("id").getObject(1));
        }
    }

    @Test
    public void testWriteNullValues() {
        RowType rowType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"id", "name"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Write row with null values
            GenericRowData row = new GenericRowData(2);
            row.setField(0, null);
            row.setField(1, null);
            writer.write(row);

            writer.finish();

            assertEquals(1, root.getRowCount());
            assertTrue(root.getVector("id").isNull(0));
            assertTrue(root.getVector("name").isNull(0));
        }
    }

    @Test
    public void testWriteArrayType() {
        RowType rowType = RowType.of(new LogicalType[] {new ArrayType(new IntType())}, new String[] {"numbers"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            GenericRowData row = new GenericRowData(1);
            row.setField(0, new GenericArrayData(new Integer[] {1, 2, 3}));
            writer.write(row);

            writer.finish();

            assertEquals(1, root.getRowCount());
        }
    }

    @Test
    public void testWriteMapType() {
        RowType rowType = RowType.of(
                new LogicalType[] {new MapType(new VarCharType(100), new IntType())}, new String[] {"scores"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            Map<StringData, Integer> mapData = new HashMap<>();
            mapData.put(StringData.fromString("math"), 90);
            mapData.put(StringData.fromString("english"), 85);

            GenericRowData row = new GenericRowData(1);
            row.setField(0, new GenericMapData(mapData));
            writer.write(row);

            writer.finish();

            assertEquals(1, root.getRowCount());
        }
    }

    @Test
    public void testTimestampPrecision() {
        RowType rowType = RowType.of(new LogicalType[] {new TimestampType(6)}, new String[] {"ts"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Create timestamp with microsecond precision
            long millis = 1705622400000L; // 2024-01-19 00:00:00.000
            int nanos = 123456; // 123456 nanoseconds (123.456 microseconds)

            GenericRowData row = new GenericRowData(1);
            row.setField(0, TimestampData.fromEpochMillis(millis, nanos));
            writer.write(row);

            writer.finish();

            assertEquals(1, root.getRowCount());
            // Verify the timestamp is written in microseconds
            Object value = root.getVector("ts").getObject(0);
            assertNotNull(value);
        }
    }

    @Test
    public void testReset() {
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Write first batch
            GenericRowData row1 = new GenericRowData(1);
            row1.setField(0, 1);
            writer.write(row1);
            writer.finish();
            assertEquals(1, root.getRowCount());

            // Reset and write second batch
            writer.reset();
            GenericRowData row2 = new GenericRowData(1);
            row2.setField(0, 2);
            writer.write(row2);
            writer.finish();
            assertEquals(1, root.getRowCount());
            assertEquals(2, root.getVector("id").getObject(0));
        }
    }

    @Test
    public void testNestedArrayType() {
        // Test Array<Array<Int>>
        RowType rowType = RowType.of(
                new LogicalType[] {new ArrayType(new ArrayType(new IntType()))}, new String[] {"nested_array"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Create nested array: [[1, 2], [3, 4, 5]]
            GenericArrayData innerArray1 = new GenericArrayData(new Integer[] {1, 2});
            GenericArrayData innerArray2 = new GenericArrayData(new Integer[] {3, 4, 5});
            GenericArrayData outerArray = new GenericArrayData(new Object[] {innerArray1, innerArray2});

            GenericRowData row = new GenericRowData(1);
            row.setField(0, outerArray);
            writer.write(row);
            writer.finish();

            assertEquals(1, root.getRowCount());
            assertNotNull(root.getVector("nested_array").getObject(0));
        }
    }

    @Test
    public void testArrayOfMapType() {
        // Test Array<Map<String, Int>>
        MapType mapType = new MapType(new VarCharType(100), new IntType());
        RowType rowType = RowType.of(new LogicalType[] {new ArrayType(mapType)}, new String[] {"array_of_map"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Create array of maps: [{a: 1, b: 2}, {c: 3}]
            Map<StringData, Integer> map1 = new HashMap<>();
            map1.put(StringData.fromString("a"), 1);
            map1.put(StringData.fromString("b"), 2);

            Map<StringData, Integer> map2 = new HashMap<>();
            map2.put(StringData.fromString("c"), 3);

            GenericMapData genericMap1 = new GenericMapData(map1);
            GenericMapData genericMap2 = new GenericMapData(map2);
            GenericArrayData arrayOfMaps = new GenericArrayData(new Object[] {genericMap1, genericMap2});

            GenericRowData row = new GenericRowData(1);
            row.setField(0, arrayOfMaps);
            writer.write(row);
            writer.finish();

            assertEquals(1, root.getRowCount());
            assertNotNull(root.getVector("array_of_map").getObject(0));
        }
    }

    @Test
    public void testWriteEmptyBatch() {
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);
            writer.finish();
            assertEquals(0, root.getRowCount());
        }
    }

    @Test
    public void testArrayWithNullElements() {
        RowType rowType = RowType.of(new LogicalType[] {new ArrayType(new IntType())}, new String[] {"numbers"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Array with null element: [1, null, 3]
            GenericRowData row = new GenericRowData(1);
            row.setField(0, new GenericArrayData(new Integer[] {1, null, 3}));
            writer.write(row);
            writer.finish();

            assertEquals(1, root.getRowCount());
        }
    }

    @Test
    public void testNullStruct() {
        RowType innerType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"value"});
        RowType rowType = RowType.of(new LogicalType[] {innerType}, new String[] {"struct_field"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Row with null struct
            GenericRowData row = new GenericRowData(1);
            row.setField(0, null);
            writer.write(row);
            writer.finish();

            assertEquals(1, root.getRowCount());
            assertTrue(root.getVector("struct_field").isNull(0));
        }
    }

    @Test
    public void testWriteTimeType() {
        RowType rowType = RowType.of(new LogicalType[] {new TimeType(3)}, new String[] {"time"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Write time value (milliseconds since midnight)
            GenericRowData row = new GenericRowData(1);
            row.setField(0, 43200000); // 12:00:00.000 = 12 hours * 60 * 60 * 1000 ms
            writer.write(row);

            // Write null time
            GenericRowData nullRow = new GenericRowData(1);
            nullRow.setField(0, null);
            writer.write(nullRow);

            writer.finish();

            assertEquals(2, root.getRowCount());
            // Verify the time is written in microseconds (43200000 ms * 1000 = 43200000000 us)
            Object value = root.getVector("time").getObject(0);
            assertNotNull(value);
            assertEquals(43200000000L, value);
            assertTrue(root.getVector("time").isNull(1));
        }
    }

    @Test
    public void testWriteLocalZonedTimestampType() {
        RowType rowType = RowType.of(new LogicalType[] {new LocalZonedTimestampType(6)}, new String[] {"lz_ts"});

        Schema arrowSchema = FlinkArrowUtils.toArrowSchema(rowType);
        try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator)) {
            FlinkArrowWriter writer = FlinkArrowWriter.create(root, rowType);

            // Create timestamp with microsecond precision
            long millis = 1705622400000L; // 2024-01-19 00:00:00.000
            int nanos = 123456; // 123456 nanoseconds (123.456 microseconds)

            GenericRowData row = new GenericRowData(1);
            row.setField(0, TimestampData.fromEpochMillis(millis, nanos));
            writer.write(row);

            // Write null
            GenericRowData nullRow = new GenericRowData(1);
            nullRow.setField(0, null);
            writer.write(nullRow);

            writer.finish();

            assertEquals(2, root.getRowCount());
            // Verify the timestamp is written in microseconds
            Object value = root.getVector("lz_ts").getObject(0);
            assertNotNull(value);
            assertTrue(root.getVector("lz_ts").isNull(1));
        }
    }
}
