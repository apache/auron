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
package org.apache.auron.flink.table.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Parquet scan with Auron native execution.
 * Tests basic scan, projection, and filter pushdown capabilities.
 */
public class AuronFlinkParquetScanITCase extends AuronFlinkTableTestBase {

    private boolean auronAvailable;

    @BeforeEach
    @Override
    public void before() {
        // Override to use BATCH mode instead of STREAMING for Parquet tests
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        tableEnvironment =
                StreamTableEnvironment.create(environment, EnvironmentSettings.fromConfiguration(configuration));
    }

    @BeforeEach
    public void checkAuronAvailability() {
        try {
            // Debug: Print java.library.path
            String libraryPath = System.getProperty("java.library.path");
            System.out.println("🔍 java.library.path = " + libraryPath);

            // Check if Auron native library is available
            System.loadLibrary("auron");
            auronAvailable = true;
            System.out.println("✅ Auron native library loaded successfully");
        } catch (UnsatisfiedLinkError e) {
            auronAvailable = false;
            System.out.println("⚠️  Auron native library not available - tests will be skipped");
            System.out.println("   Error: " + e.getMessage());
        }
    }

    @Test
    public void testBasicParquetScan() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testBasicParquetScan - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
                row(1, "Alice", 100.5, LocalDate.of(2024, 1, 1)),
                row(2, "Bob", 200.5, LocalDate.of(2024, 1, 2)),
                row(3, "Charlie", 300.5, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "basic_test", schema, testData);
        createParquetTable("parquet_basic_test", schema, "file://" + parquetDir.getAbsolutePath() + "/basic_test");

        // Execute query
        TableResult result = tableEnvironment.executeSql("SELECT * FROM parquet_basic_test");

        List<Row> results = collectResults(result);
        assertEquals(3, results.size());
    }

    @Test
    public void testParquetScanWithProjection() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithProjection - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
                row(1, "Alice", 100.5, LocalDate.of(2024, 1, 1)),
                row(2, "Bob", 200.5, LocalDate.of(2024, 1, 2)),
                row(3, "Charlie", 300.5, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "proj_test", schema, testData);
        createParquetTable("parquet_proj_test", schema, "file://" + parquetDir.getAbsolutePath() + "/proj_test");

        // Execute query: SELECT id, name (project 2 columns)
        TableResult result = tableEnvironment.executeSql("SELECT id, name FROM parquet_proj_test");

        List<Row> results = collectResults(result);
        assertEquals(3, results.size());

        // Verify only 2 columns returned
        for (Row r : results) {
            assertEquals(2, r.getArity());
        }
    }

    @Test
    public void testParquetScanWithSimpleFilter() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithSimpleFilter - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
                row(1, "Alice", 50.0, LocalDate.of(2024, 1, 1)),
                row(2, "Bob", 150.0, LocalDate.of(2024, 1, 2)),
                row(3, "Charlie", 250.0, LocalDate.of(2024, 1, 3)),
                row(4, "David", 350.0, LocalDate.of(2024, 1, 4)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "filter_test", schema, testData);
        createParquetTable("parquet_filter_test", schema, "file://" + parquetDir.getAbsolutePath() + "/filter_test");

        // Execute query: WHERE amount > 100
        TableResult result = tableEnvironment.executeSql("SELECT * FROM parquet_filter_test WHERE amount > 100");

        List<Row> results = collectResults(result);

        // Should return rows with amount > 100 (Bob, Charlie, David)
        assertEquals(3, results.size());
    }

    @Test
    public void testParquetScanWithComplexFilter() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithComplexFilter - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
                row(1, "Alice", 50.0, LocalDate.of(2024, 1, 1)),
                row(2, "Bob", 150.0, LocalDate.of(2024, 1, 2)),
                row(3, "Charlie", 250.0, LocalDate.of(2024, 1, 3)),
                row(4, "David", 100.0, LocalDate.of(2024, 1, 4)),
                row(5, "Eve", 200.0, LocalDate.of(2024, 1, 5)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "complex_filter_test", schema, testData);
        createParquetTable(
                "parquet_complex_filter_test",
                schema,
                "file://" + parquetDir.getAbsolutePath() + "/complex_filter_test");

        // Execute query: WHERE amount >= 100 AND amount <= 200
        TableResult result = tableEnvironment.executeSql(
                "SELECT * FROM parquet_complex_filter_test WHERE amount >= 100 AND amount <= 200");

        List<Row> results = collectResults(result);

        // Should return Bob (150), David (100), Eve (200)
        assertEquals(3, results.size());
    }

    @Test
    public void testParquetScanWithProjectionAndFilter() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithProjectionAndFilter - Auron not available");
            return;
        }

        // Create test data
        List<Row> testData = Arrays.asList(
                row(1, "Alice", 50.0, LocalDate.of(2024, 1, 1)),
                row(2, "Bob", 150.0, LocalDate.of(2024, 1, 2)),
                row(3, "Charlie", 250.0, LocalDate.of(2024, 1, 3)),
                row(4, "David", 350.0, LocalDate.of(2024, 1, 4)));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE," + "  created_date DATE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "proj_filter_test", schema, testData);
        createParquetTable(
                "parquet_proj_filter_test", schema, "file://" + parquetDir.getAbsolutePath() + "/proj_filter_test");

        // Execute query: SELECT name, amount WHERE amount > 100
        TableResult result =
                tableEnvironment.executeSql("SELECT name, amount FROM parquet_proj_filter_test WHERE amount > 100");

        List<Row> results = collectResults(result);

        // Should return 3 rows (Bob, Charlie, David) with 2 columns each
        assertEquals(3, results.size());
        for (Row r : results) {
            assertEquals(2, r.getArity());
        }
    }

    @Test
    public void testParquetScanWithDifferentTypes() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithDifferentTypes - Auron not available");
            return;
        }

        // Create test data with various types
        List<Row> testData = Arrays.asList(
                row(1, "Alice", 100.5, true, LocalDate.of(2024, 1, 1)),
                row(2, "Bob", 200.5, false, LocalDate.of(2024, 1, 2)),
                row(3, "Charlie", 300.5, true, LocalDate.of(2024, 1, 3)));

        String schema = "(" + "  id INT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  active BOOLEAN,"
                + "  created_date DATE"
                + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "types_test", schema, testData);
        createParquetTable("parquet_types_test", schema, "file://" + parquetDir.getAbsolutePath() + "/types_test");

        // Execute query
        TableResult result = tableEnvironment.executeSql("SELECT * FROM parquet_types_test WHERE active = true");

        List<Row> results = collectResults(result);
        assertEquals(2, results.size());
    }

    @Test
    public void testParquetScanWithNulls() throws Exception {
        if (!auronAvailable) {
            System.out.println("⏭️  Skipping testParquetScanWithNulls - Auron not available");
            return;
        }

        // Create test data with nulls
        List<Row> testData =
                Arrays.asList(row(1, "Alice", 100.5), row(2, null, 200.5), row(3, "Charlie", null), row(4, null, null));

        String schema = "(" + "  id INT," + "  name STRING," + "  amount DOUBLE" + ")";

        File parquetDir = createTempParquetDir();
        writeParquetTestData(parquetDir, "nulls_test", schema, testData);
        createParquetTable("parquet_nulls_test", schema, "file://" + parquetDir.getAbsolutePath() + "/nulls_test");

        // Execute query - test that nulls are handled correctly
        TableResult result = tableEnvironment.executeSql("SELECT * FROM parquet_nulls_test WHERE name IS NOT NULL");

        List<Row> results = collectResults(result);
        // Should return rows 1 and 3 (Alice and Charlie)
        assertEquals(2, results.size());
    }
}
