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

import java.util.HashMap;
import java.util.Map;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link MapWriter}. */
public class MapWriterTest {

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
    public void testWriteMapFromRowData() {
        RowType rowType = RowType.of(
                new LogicalType[] {new MapType(new VarCharType(100), new IntType())}, new String[] {"f_map"});
        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            MapVector mapVector = (MapVector) root.getVector("f_map");
            mapVector.allocateNew();

            StructVector entriesVector = (StructVector) mapVector.getDataVector();
            ArrowFieldWriter<ArrayData> keyWriter = VarCharWriter.forArray(
                    (org.apache.arrow.vector.VarCharVector) entriesVector.getChild(MapVector.KEY_NAME));
            ArrowFieldWriter<ArrayData> valueWriter = IntWriter.forArray(
                    (org.apache.arrow.vector.IntVector) entriesVector.getChild(MapVector.VALUE_NAME));

            MapWriter<RowData> writer = MapWriter.forRow(mapVector, keyWriter, valueWriter);

            Map<StringData, Integer> map = new HashMap<>();
            map.put(StringData.fromString("a"), 1);
            map.put(StringData.fromString("b"), 2);

            GenericRowData row = new GenericRowData(1);
            row.setField(0, new GenericMapData(map));
            writer.write(row, 0);

            writer.finish();

            assertEquals(1, mapVector.getValueCount());
            assertFalse(mapVector.isNull(0));
        }
    }

    @Test
    public void testWriteNullMapFromRowData() {
        RowType rowType = RowType.of(
                new LogicalType[] {new MapType(new VarCharType(100), new IntType())}, new String[] {"f_map"});
        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            MapVector mapVector = (MapVector) root.getVector("f_map");
            mapVector.allocateNew();

            StructVector entriesVector = (StructVector) mapVector.getDataVector();
            ArrowFieldWriter<ArrayData> keyWriter = VarCharWriter.forArray(
                    (org.apache.arrow.vector.VarCharVector) entriesVector.getChild(MapVector.KEY_NAME));
            ArrowFieldWriter<ArrayData> valueWriter = IntWriter.forArray(
                    (org.apache.arrow.vector.IntVector) entriesVector.getChild(MapVector.VALUE_NAME));

            MapWriter<RowData> writer = MapWriter.forRow(mapVector, keyWriter, valueWriter);

            GenericRowData row = new GenericRowData(1);
            row.setField(0, null);
            writer.write(row, 0);

            writer.finish();

            assertEquals(1, mapVector.getValueCount());
            assertTrue(mapVector.isNull(0));
        }
    }

    @Test
    public void testWriteEmptyMapFromRowData() {
        RowType rowType = RowType.of(
                new LogicalType[] {new MapType(new VarCharType(100), new IntType())}, new String[] {"f_map"});
        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            MapVector mapVector = (MapVector) root.getVector("f_map");
            mapVector.allocateNew();

            StructVector entriesVector = (StructVector) mapVector.getDataVector();
            ArrowFieldWriter<ArrayData> keyWriter = VarCharWriter.forArray(
                    (org.apache.arrow.vector.VarCharVector) entriesVector.getChild(MapVector.KEY_NAME));
            ArrowFieldWriter<ArrayData> valueWriter = IntWriter.forArray(
                    (org.apache.arrow.vector.IntVector) entriesVector.getChild(MapVector.VALUE_NAME));

            MapWriter<RowData> writer = MapWriter.forRow(mapVector, keyWriter, valueWriter);

            GenericRowData row = new GenericRowData(1);
            row.setField(0, new GenericMapData(new HashMap<>()));
            writer.write(row, 0);

            writer.finish();

            assertEquals(1, mapVector.getValueCount());
            assertFalse(mapVector.isNull(0));
        }
    }
}
