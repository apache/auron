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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.auron.flink.arrow.FlinkArrowUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link RowWriter}. */
public class RowWriterTest {

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
    public void testWriteRowFromRowData() {
        RowType innerType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"id", "name"});
        RowType outerType = RowType.of(new LogicalType[] {innerType}, new String[] {"f_row"});
        Schema schema = FlinkArrowUtils.toArrowSchema(outerType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            StructVector structVector = (StructVector) root.getVector("f_row");
            structVector.allocateNew();

            @SuppressWarnings("unchecked")
            ArrowFieldWriter<RowData>[] fieldsWriters = new ArrowFieldWriter[] {
                IntWriter.forRow((IntVector) structVector.getChild("id")),
                VarCharWriter.forRow((VarCharVector) structVector.getChild("name"))
            };

            RowWriter<RowData> writer = RowWriter.forRow(structVector, fieldsWriters);

            GenericRowData innerRow = GenericRowData.of(42, StringData.fromString("hello"));
            GenericRowData outerRow = new GenericRowData(1);
            outerRow.setField(0, innerRow);
            writer.write(outerRow, 0);

            writer.finish();

            assertEquals(1, structVector.getValueCount());
            assertFalse(structVector.isNull(0));
            IntVector idVector = (IntVector) structVector.getChild("id");
            VarCharVector nameVector = (VarCharVector) structVector.getChild("name");
            assertEquals(42, idVector.get(0));
            assertArrayEquals("hello".getBytes(), nameVector.get(0));
        }
    }

    @Test
    public void testWriteNullRowFromRowData() {
        RowType innerType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        RowType outerType = RowType.of(new LogicalType[] {innerType}, new String[] {"f_row"});
        Schema schema = FlinkArrowUtils.toArrowSchema(outerType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            StructVector structVector = (StructVector) root.getVector("f_row");
            structVector.allocateNew();

            @SuppressWarnings("unchecked")
            ArrowFieldWriter<RowData>[] fieldsWriters =
                    new ArrowFieldWriter[] {IntWriter.forRow((IntVector) structVector.getChild("id"))};

            RowWriter<RowData> writer = RowWriter.forRow(structVector, fieldsWriters);

            GenericRowData outerRow = new GenericRowData(1);
            outerRow.setField(0, null);
            writer.write(outerRow, 0);

            writer.finish();

            assertEquals(1, structVector.getValueCount());
            assertTrue(structVector.isNull(0));
        }
    }

    @Test
    public void testWriteMultipleRowsFromRowData() {
        RowType innerType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"val"});
        RowType outerType = RowType.of(new LogicalType[] {innerType}, new String[] {"f_row"});
        Schema schema = FlinkArrowUtils.toArrowSchema(outerType);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            StructVector structVector = (StructVector) root.getVector("f_row");
            structVector.allocateNew();

            @SuppressWarnings("unchecked")
            ArrowFieldWriter<RowData>[] fieldsWriters =
                    new ArrowFieldWriter[] {IntWriter.forRow((IntVector) structVector.getChild("val"))};

            RowWriter<RowData> writer = RowWriter.forRow(structVector, fieldsWriters);

            writer.write(GenericRowData.of((Object) GenericRowData.of(1)), 0);
            writer.write(GenericRowData.of((Object) GenericRowData.of(2)), 0);
            writer.write(GenericRowData.of((Object) GenericRowData.of(3)), 0);

            writer.finish();

            assertEquals(3, structVector.getValueCount());
            IntVector valVector = (IntVector) structVector.getChild("val");
            assertEquals(1, valVector.get(0));
            assertEquals(2, valVector.get(1));
            assertEquals(3, valVector.get(2));
        }
    }
}
