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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
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

/** Unit tests for FlinkArrowFFIExporter. */
public class FlinkArrowFFIExporterTest {

    private BufferAllocator testAllocator;
    private static Boolean nativeLibrariesAvailable;

    /**
     * Check if Arrow FFI native libraries are available.
     * FFI operations require JNI, which needs native libraries.
     */
    private static boolean isNativeLibraryAvailable() {
        if (nativeLibrariesAvailable == null) {
            try {
                // Try to load the JNI library - JniWrapper.get() triggers ensureLoaded()
                org.apache.arrow.c.jni.JniWrapper.get();
                nativeLibrariesAvailable = true;
            } catch (Throwable e) {
                // Catch all throwables including IllegalStateException, UnsatisfiedLinkError, etc.
                nativeLibrariesAvailable = false;
            }
        }
        return nativeLibrariesAvailable;
    }

    @BeforeEach
    public void setUp() {
        testAllocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    public void tearDown() {
        testAllocator.close();
    }

    @Test
    public void testExportSchemaCreatesExporter() {
        RowType rowType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"id", "name"});

        Iterator<RowData> emptyIter = Collections.emptyIterator();

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(emptyIter, rowType)) {
            assertNotNull(exporter);
        }
    }

    @Test
    public void testExporterWithEmptyIterator() {
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        Iterator<RowData> emptyIter = Collections.emptyIterator();

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(emptyIter, rowType)) {
            assertNotNull(exporter);
        }
    }

    @Test
    public void testExporterWithMultipleRows() {
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        GenericRowData row1 = new GenericRowData(1);
        row1.setField(0, 1);
        GenericRowData row2 = new GenericRowData(1);
        row2.setField(0, 2);

        Iterator<RowData> iter = Arrays.asList((RowData) row1, row2).iterator();

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(iter, rowType)) {
            assertNotNull(exporter);
        }
    }

    @Test
    public void testExporterWithComplexTypes() {
        RowType rowType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"id", "name"});

        GenericRowData row = new GenericRowData(2);
        row.setField(0, 42);
        row.setField(1, StringData.fromString("test"));

        Iterator<RowData> iter = Collections.singletonList((RowData) row).iterator();

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(iter, rowType)) {
            assertNotNull(exporter);
        }
    }

    @Test
    public void testExporterCloseIsIdempotent() {
        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
        Iterator<RowData> emptyIter = Collections.emptyIterator();

        FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(emptyIter, rowType);
        exporter.close();
        exporter.close();
    }

    @Test
    public void testExportSchemaWithArrowFFI() {
        assumeTrue(isNativeLibraryAvailable(), "Skipping test: Arrow FFI native libraries not available");

        RowType rowType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"id", "name"});

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(Collections.emptyIterator(), rowType);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(testAllocator)) {

            exporter.exportSchema(arrowSchema.memoryAddress());

            org.apache.arrow.vector.types.pojo.Schema schema = Data.importSchema(testAllocator, arrowSchema, null);
            assertEquals(2, schema.getFields().size());
            assertEquals("id", schema.getFields().get(0).getName());
            assertEquals("name", schema.getFields().get(1).getName());
        }
    }

    @Test
    public void testExportNextBatchEndToEnd() {
        assumeTrue(isNativeLibraryAvailable(), "Skipping test: Arrow FFI native libraries not available");

        RowType rowType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"id", "name"});

        List<RowData> rows = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            GenericRowData row = new GenericRowData(2);
            row.setField(0, i);
            row.setField(1, StringData.fromString("value" + i));
            rows.add(row);
        }

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(rows.iterator(), rowType);
                ArrowArray arrowArray = ArrowArray.allocateNew(testAllocator);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(testAllocator)) {

            // Export schema
            exporter.exportSchema(arrowSchema.memoryAddress());

            // Export batch
            assertTrue(exporter.exportNextBatch(arrowArray.memoryAddress()));

            // Import and verify - use ArrowSchema directly
            try (VectorSchemaRoot root = Data.importVectorSchemaRoot(testAllocator, arrowArray, arrowSchema, null)) {
                assertTrue(root.getRowCount() > 0);

                // Verify data
                IntVector idVector = (IntVector) root.getVector("id");
                VarCharVector nameVector = (VarCharVector) root.getVector("name");

                assertEquals(0, idVector.get(0));
                assertEquals("value0", new String(nameVector.get(0)));
            }
        }
    }

    @Test
    public void testExportEmptyIteratorReturnsFalse() {
        assumeTrue(isNativeLibraryAvailable(), "Skipping test: Arrow FFI native libraries not available");

        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(Collections.emptyIterator(), rowType);
                ArrowArray arrowArray = ArrowArray.allocateNew(testAllocator)) {

            // Empty iterator should return false on first call
            assertFalse(exporter.exportNextBatch(arrowArray.memoryAddress()));
        }
    }

    @Test
    public void testExportMultipleBatches() {
        assumeTrue(isNativeLibraryAvailable(), "Skipping test: Arrow FFI native libraries not available");

        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        // Create enough rows to potentially span multiple batches
        List<RowData> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            GenericRowData row = new GenericRowData(1);
            row.setField(0, i);
            rows.add(row);
        }

        int totalRowsExported = 0;

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(rows.iterator(), rowType);
                ArrowSchema arrowSchema = ArrowSchema.allocateNew(testAllocator)) {

            exporter.exportSchema(arrowSchema.memoryAddress());

            // Export all batches
            while (true) {
                try (ArrowArray arrowArray = ArrowArray.allocateNew(testAllocator);
                        ArrowSchema batchSchema = ArrowSchema.allocateNew(testAllocator)) {
                    // Re-export schema for each batch since ArrowSchema is consumed
                    exporter.exportSchema(batchSchema.memoryAddress());

                    if (!exporter.exportNextBatch(arrowArray.memoryAddress())) {
                        break;
                    }

                    try (VectorSchemaRoot root =
                            Data.importVectorSchemaRoot(testAllocator, arrowArray, batchSchema, null)) {
                        totalRowsExported += root.getRowCount();
                    }
                }
            }
        }

        assertEquals(100, totalRowsExported);
    }

    @Test
    public void testProducerExceptionPropagation() {
        assumeTrue(isNativeLibraryAvailable(), "Skipping test: Arrow FFI native libraries not available");

        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        // Create an iterator that throws an exception
        Iterator<RowData> failingIterator = new Iterator<RowData>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return count < 5;
            }

            @Override
            public RowData next() {
                if (count >= 3) {
                    throw new RuntimeException("Simulated error");
                }
                GenericRowData row = new GenericRowData(1);
                row.setField(0, count++);
                return row;
            }
        };

        try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(failingIterator, rowType);
                ArrowArray arrowArray = ArrowArray.allocateNew(testAllocator)) {

            // First batch might succeed if it doesn't hit the error
            // Eventually we should get a RuntimeException
            assertThrows(RuntimeException.class, () -> {
                for (int i = 0; i < 10; i++) {
                    if (!exporter.exportNextBatch(arrowArray.memoryAddress())) {
                        break;
                    }
                }
            });
        }
    }

    @Test
    public void testCloseWhileProducerIsRunning() throws InterruptedException {
        assumeTrue(isNativeLibraryAvailable(), "Skipping test: Arrow FFI native libraries not available");

        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        // Create a slow iterator to ensure producer is still running when we close
        Iterator<RowData> slowIterator = new Iterator<RowData>() {
            private int count = 0;

            @Override
            public boolean hasNext() {
                return count < 1000;
            }

            @Override
            public RowData next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                try {
                    Thread.sleep(10); // Slow down the iterator
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                GenericRowData row = new GenericRowData(1);
                row.setField(0, count++);
                return row;
            }
        };

        FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(slowIterator, rowType);

        // Export one batch to start producer
        try (ArrowArray arrowArray = ArrowArray.allocateNew(testAllocator)) {
            exporter.exportNextBatch(arrowArray.memoryAddress());
        }

        // Close while producer might still be running
        exporter.close();

        // Subsequent calls should return false
        try (ArrowArray arrowArray = ArrowArray.allocateNew(testAllocator)) {
            assertFalse(exporter.exportNextBatch(arrowArray.memoryAddress()));
        }
    }

    @Test
    public void testExportAfterClose() {
        assumeTrue(isNativeLibraryAvailable(), "Skipping test: Arrow FFI native libraries not available");

        RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});

        GenericRowData row = new GenericRowData(1);
        row.setField(0, 1);
        Iterator<RowData> iter = Collections.singletonList((RowData) row).iterator();

        FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(iter, rowType);
        exporter.close();

        try (ArrowArray arrowArray = ArrowArray.allocateNew(testAllocator)) {
            assertFalse(exporter.exportNextBatch(arrowArray.memoryAddress()));
        }
    }
}
