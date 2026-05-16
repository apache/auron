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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link FlinkArrowFFIExporter}.
 *
 * <p>All tests are pure Arrow C-Data round-trip — they exercise the FFI export/import boundary
 * without any native engine call. Each test holds its own child allocator inside a
 * try-with-resources block so the allocator itself catches any leaks at test teardown.
 */
public class FlinkArrowFFIExporterTest {

    /**
     * The repo-wide surefire config sets {@code java.io.tmpdir} to {@code target/tmp}, which does
     * not yet exist on a clean build. The Arrow C-Data JNI loader extracts its native library via
     * {@link java.io.File#createTempFile}, which fails if the directory is missing. Ensure it
     * exists before any test runs.
     */
    @BeforeAll
    public static void ensureTmpDirExists() {
        String tmp = System.getProperty("java.io.tmpdir");
        if (tmp != null) {
            new File(tmp).mkdirs();
        }
    }

    /** Schema used by most tests: INT id, BIGINT value, VARCHAR label. */
    private static RowType simpleRowType() {
        return RowType.of(
                new LogicalType[] {new IntType(), new BigIntType(), new VarCharType(100)},
                new String[] {"id", "value", "label"});
    }

    /**
     * Contract: {@code exportSchema(ptr)} populates an Arrow FFI schema struct such that importing
     * it produces a {@link Schema} structurally equal to {@link FlinkArrowUtils#toArrowSchema}.
     */
    @Test
    public void testExportSchemaRoundTrip() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testSchema", 0, Long.MAX_VALUE)) {
            RowType rowType = simpleRowType();
            try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(allocator, rowType, 100);
                    ArrowSchema ffi = ArrowSchema.allocateNew(allocator);
                    CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
                exporter.exportSchema(ffi.memoryAddress());
                Schema imported = Data.importSchema(allocator, ffi, dictProvider);
                assertEquals(FlinkArrowUtils.toArrowSchema(rowType), imported);
            }
        }
    }

    /**
     * Contract: after offering N rows, {@code exportNextBatch} returns {@code true} and the FFI
     * array contains exactly those N rows with field-by-field equality.
     */
    @Test
    public void testOfferThenExportNextBatch_returnsTrueWithRowsAvailable() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testOfferExport", 0, Long.MAX_VALUE)) {
            RowType rowType = simpleRowType();
            int numRows = 100;
            try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(allocator, rowType, 200)) {
                for (int i = 0; i < numRows; i++) {
                    exporter.offer(makeRow(i));
                }

                try (ArrowArray ffiArray = ArrowArray.allocateNew(allocator);
                        ArrowSchema ffiSchema = ArrowSchema.allocateNew(allocator);
                        CDataDictionaryProvider dictProvider = new CDataDictionaryProvider()) {
                    // Export schema once so we can build a matching import root.
                    exporter.exportSchema(ffiSchema.memoryAddress());
                    Schema schema = Data.importSchema(allocator, ffiSchema, dictProvider);

                    assertTrue(exporter.exportNextBatch(ffiArray.memoryAddress()));

                    try (VectorSchemaRoot importRoot = VectorSchemaRoot.create(schema, allocator)) {
                        Data.importIntoVectorSchemaRoot(allocator, ffiArray, importRoot, dictProvider);
                        assertEquals(numRows, importRoot.getRowCount());
                        FlinkArrowReader reader = FlinkArrowReader.create(importRoot, rowType);
                        try {
                            for (int i = 0; i < numRows; i++) {
                                RowData row = reader.read(i);
                                assertEquals(i, row.getInt(0));
                                assertEquals((long) i * 2L, row.getLong(1));
                                assertEquals("row-" + i, row.getString(2).toString());
                            }
                        } finally {
                            reader.close();
                        }
                    }
                }
            }
        }
    }

    /**
     * Contract: {@link FlinkArrowFFIExporter#exportNextBatch(long)} returns {@code false}
     * whenever the buffer is empty. Without this contract, mid-stream empty pulls would
     * return {@code true} with a 0-row batch and the native FFI Reader's loop would spin on
     * empty batches in steady state.
     */
    @Test
    public void testExportNextBatchReturnsFalseOnEmpty() {
        try (BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testEmptyReturnsFalse", 0, Long.MAX_VALUE)) {
            RowType rowType = simpleRowType();
            try (FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(allocator, rowType, 100);
                    ArrowArray ffiArray = ArrowArray.allocateNew(allocator)) {
                assertFalse(
                        exporter.exportNextBatch(ffiArray.memoryAddress()),
                        "exportNextBatch must return false on an empty buffer");
            }
        }
    }

    /**
     * Contract: {@code close()} releases every byte owned by the exporter back to the supplied
     * allocator. After close, {@code allocator.getAllocatedMemory() == 0}.
     */
    @Test
    public void testCloseReleasesAllocator() {
        BufferAllocator allocator =
                FlinkArrowUtils.ROOT_ALLOCATOR.newChildAllocator("testCloseFrees", 0, Long.MAX_VALUE);
        try {
            RowType rowType = simpleRowType();
            FlinkArrowFFIExporter exporter = new FlinkArrowFFIExporter(allocator, rowType, 100);
            for (int i = 0; i < 10; i++) {
                exporter.offer(makeRow(i));
            }
            exporter.close();
            assertEquals(
                    0L,
                    allocator.getAllocatedMemory(),
                    "exporter.close() must release all memory borrowed from its allocator");
        } finally {
            allocator.close();
        }
    }

    private static RowData makeRow(int i) {
        GenericRowData row = new GenericRowData(3);
        row.setField(0, i);
        row.setField(1, (long) i * 2L);
        row.setField(2, StringData.fromBytes(("row-" + i).getBytes(StandardCharsets.UTF_8)));
        return row;
    }
}
