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
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link ArrayWriter}. */
public class ArrayWriterTest {

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
    public void testWriteIntArrayFromRowData() {
        RowType rowType = RowType.of(new LogicalType[] {new ArrayType(new IntType())}, new String[] {"f_array"});
        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ListVector listVector = (ListVector) root.getVector("f_array");
            listVector.allocateNew();
            IntVector dataVector = (IntVector) listVector.getDataVector();
            dataVector.allocateNew();

            ArrowFieldWriter<ArrayData> elementWriter = IntWriter.forArray(dataVector);
            ArrayWriter<RowData> writer = ArrayWriter.forRow(listVector, elementWriter);

            GenericRowData row = new GenericRowData(1);
            row.setField(0, new GenericArrayData(new int[] {10, 20, 30}));
            writer.write(row, 0);

            writer.finish();

            assertEquals(1, listVector.getValueCount());
            assertFalse(listVector.isNull(0));
            // Verify the data vector content
            assertEquals(3, dataVector.getValueCount());
            assertEquals(10, dataVector.get(0));
            assertEquals(20, dataVector.get(1));
            assertEquals(30, dataVector.get(2));
        }
    }

    @Test
    public void testWriteNullArrayFromRowData() {
        RowType rowType = RowType.of(new LogicalType[] {new ArrayType(new IntType())}, new String[] {"f_array"});
        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ListVector listVector = (ListVector) root.getVector("f_array");
            listVector.allocateNew();
            IntVector dataVector = (IntVector) listVector.getDataVector();
            dataVector.allocateNew();

            ArrowFieldWriter<ArrayData> elementWriter = IntWriter.forArray(dataVector);
            ArrayWriter<RowData> writer = ArrayWriter.forRow(listVector, elementWriter);

            GenericRowData row = new GenericRowData(1);
            row.setField(0, null);
            writer.write(row, 0);

            writer.finish();

            assertEquals(1, listVector.getValueCount());
            assertTrue(listVector.isNull(0));
        }
    }

    @Test
    public void testWriteEmptyArrayFromRowData() {
        RowType rowType = RowType.of(new LogicalType[] {new ArrayType(new IntType())}, new String[] {"f_array"});
        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ListVector listVector = (ListVector) root.getVector("f_array");
            listVector.allocateNew();
            IntVector dataVector = (IntVector) listVector.getDataVector();
            dataVector.allocateNew();

            ArrowFieldWriter<ArrayData> elementWriter = IntWriter.forArray(dataVector);
            ArrayWriter<RowData> writer = ArrayWriter.forRow(listVector, elementWriter);

            GenericRowData row = new GenericRowData(1);
            row.setField(0, new GenericArrayData(new int[] {}));
            writer.write(row, 0);

            writer.finish();

            assertEquals(1, listVector.getValueCount());
            assertFalse(listVector.isNull(0));
            assertEquals(0, dataVector.getValueCount());
        }
    }

    @Test
    public void testWriteMultipleArraysFromRowData() {
        RowType rowType = RowType.of(new LogicalType[] {new ArrayType(new IntType())}, new String[] {"f_array"});
        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ListVector listVector = (ListVector) root.getVector("f_array");
            listVector.allocateNew();
            IntVector dataVector = (IntVector) listVector.getDataVector();
            dataVector.allocateNew();

            ArrowFieldWriter<ArrayData> elementWriter = IntWriter.forArray(dataVector);
            ArrayWriter<RowData> writer = ArrayWriter.forRow(listVector, elementWriter);

            writer.write(GenericRowData.of((Object) new GenericArrayData(new int[] {1, 2})), 0);
            writer.write(GenericRowData.of((Object) new GenericArrayData(new int[] {3})), 0);
            writer.write(GenericRowData.of((Object) new GenericArrayData(new int[] {4, 5, 6})), 0);

            writer.finish();

            assertEquals(3, listVector.getValueCount());
            assertEquals(6, dataVector.getValueCount());
        }
    }

    @Test
    public void testResetAndWriteNewBatch() {
        RowType rowType = RowType.of(new LogicalType[] {new ArrayType(new IntType())}, new String[] {"f_array"});
        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ListVector listVector = (ListVector) root.getVector("f_array");
            listVector.allocateNew();
            IntVector dataVector = (IntVector) listVector.getDataVector();
            dataVector.allocateNew();

            ArrowFieldWriter<ArrayData> elementWriter = IntWriter.forArray(dataVector);
            ArrayWriter<RowData> writer = ArrayWriter.forRow(listVector, elementWriter);

            writer.write(GenericRowData.of((Object) new GenericArrayData(new int[] {1, 2})), 0);
            writer.finish();
            assertEquals(1, listVector.getValueCount());

            writer.reset();
            writer.write(GenericRowData.of((Object) new GenericArrayData(new int[] {99})), 0);
            writer.finish();
            assertEquals(1, listVector.getValueCount());
        }
    }
}
