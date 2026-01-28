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
package org.apache.auron.flink.planner;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * E2E test for Auron execution with PARALLELISM > 1.
 * Verifies that file splitting works correctly and data is not duplicated.
 *
 * Expected behavior:
 * - Files are split across parallel tasks
 * - Each task processes only its assigned subset
 * - Total row count is correct (not multiplied by parallelism)
 * - Logs show: "Task X/N: Assigned M files"
 */
public class AuronParallelExecutionTest {

    public static void main(String[] args) throws Exception {
        int parallelism = 4; // Test with 4 parallel tasks

        String separator = repeatString("=", 80);
        System.out.println("\n" + separator);
        System.out.println("Flink-Auron PARALLEL Execution Test (parallelism=" + parallelism + ")");
        System.out.println(separator);
        System.out.println("\nThis test verifies distributed file splitting works correctly.");
        System.out.println("Watch for logs showing file distribution across tasks.");
        System.out.println(separator + "\n");

        // Step 1: Create test data with multiple files
        String testDataPath = createTestParquetDataWithMultipleFiles();
        System.out.println("✅ Test data created at: " + testDataPath);

        // Count files
        java.io.File dir = new java.io.File(testDataPath);
        int fileCount = dir.listFiles((d, name) -> name.endsWith(".parquet")).length;
        System.out.println("   Total Parquet files: " + fileCount + "\n");

        // Step 2: Execute query with parallelism > 1
        System.out.println(separator);
        System.out.println("EXECUTING Query WITH Parallelism = " + parallelism);
        System.out.println(separator);
        System.out.println("IMPORTANT: Watch for these log messages:");
        System.out.println("  - 'Modifying plan for task X/" + parallelism + "'");
        System.out.println("  - 'Task X/" + parallelism + ": Assigned N files'");
        System.out.println("  - Each task should get roughly " + fileCount + "/" + parallelism + " = "
                + (fileCount / parallelism) + " files");
        System.out.println(separator + "\n");

        long rowCount = executeQueryWithParallelism(testDataPath, parallelism);

        // Step 3: Verify results
        System.out.println("\n" + separator);
        System.out.println("VERIFICATION");
        System.out.println(separator);

        // Expected: roughly 50-70 rows match the filter (amount > 100 out of 100 rows)
        // The exact count depends on random data generation
        System.out.println("Total rows returned: " + rowCount);

        if (rowCount > 0 && rowCount < 100) {
            System.out.println("✅ Row count looks correct (filtered subset of 100 rows)");
            System.out.println("✅ Data was NOT duplicated " + parallelism + "x");
        } else if (rowCount >= 100 * parallelism - 20) {
            System.out.println("❌ WARNING: Row count suggests data was processed multiple times!");
            System.out.println("   Expected: ~50-70 rows (filtered)");
            System.out.println("   Got: " + rowCount + " rows");
            System.out.println("   This may indicate file splitting didn't work.");
        } else {
            System.out.println("⚠️  Row count: " + rowCount + " (verify this is expected)");
        }

        // Cleanup
        cleanupTestData(testDataPath);

        System.out.println("\n" + separator);
        System.out.println("Test Completed");
        System.out.println(separator);
        System.out.println("\nCheck the logs above to verify:");
        System.out.println("  1. Each of " + parallelism + " tasks got assigned files");
        System.out.println("  2. Files were split (not all tasks got all files)");
        System.out.println("  3. Row count is correct (not " + parallelism + "x duplicated)");
        System.out.println(separator + "\n");
    }

    private static String createTestParquetDataWithMultipleFiles() throws Exception {
        System.out.println("Creating test Parquet data with multiple files...");
        String testDataPath = "/tmp/auron_parallel_test_" + System.currentTimeMillis();

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create source with more rows to generate multiple files
        tEnv.executeSql("CREATE TABLE test_source ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'number-of-rows' = '100',"
                + "  'fields.id.kind' = 'sequence',"
                + "  'fields.id.start' = '1',"
                + "  'fields.id.end' = '100',"
                + "  'fields.product.length' = '20',"
                + "  'fields.amount.min' = '10.0',"
                + "  'fields.amount.max' = '500.0',"
                + "  'fields.category.length' = '10'"
                + ")");

        // Create Parquet sink with parallelism to generate multiple files
        tEnv.executeSql("CREATE TABLE test_sink ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + testDataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Set parallelism for writing to create multiple files
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", 8);

        // Write data
        tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source").await();

        // Reset parallelism for reading
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", 1);

        System.out.println("Test data written to: " + testDataPath);

        return testDataPath;
    }

    private static long executeQueryWithParallelism(String dataPath, int parallelism) throws Exception {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Enable Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", true);

        // Set parallelism
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", parallelism);

        System.out.println("Configuration:");
        System.out.println("  execution.runtime-mode = BATCH");
        System.out.println("  table.optimizer.auron.enabled = true");
        System.out.println("  table.exec.resource.default-parallelism = " + parallelism);

        // Create table pointing to test data
        tEnv.executeSql("CREATE TABLE sales ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + dataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Execute the query
        String query = "SELECT id, product, amount FROM sales WHERE amount > 100.0";
        System.out.println("Executing query: " + query);
        System.out.println("(Parallelism = " + parallelism + ")\n");

        long startTime = System.currentTimeMillis();

        // Execute and collect results
        TableResult result = tEnv.executeSql(query);

        // Count all results
        long count = 0;
        java.util.Iterator<org.apache.flink.types.Row> iterator = result.collect();
        System.out.println("Sample results (first 5):");
        while (iterator.hasNext()) {
            org.apache.flink.types.Row row = iterator.next();
            if (count < 5) {
                System.out.println("  " + row);
            }
            count++;
        }

        long endTime = System.currentTimeMillis();
        System.out.println("\n✅ Query executed successfully in " + (endTime - startTime) + "ms");

        return count;
    }

    private static void cleanupTestData(String path) {
        java.io.File dir = new java.io.File(path);
        if (dir.exists()) {
            deleteDirectory(dir);
            System.out.println("✅ Test data cleaned up: " + path);
        }
    }

    private static void deleteDirectory(java.io.File directory) {
        if (directory.exists()) {
            java.io.File[] files = directory.listFiles();
            if (files != null) {
                for (java.io.File file : files) {
                    if (file.isDirectory()) {
                        deleteDirectory(file);
                    } else {
                        file.delete();
                    }
                }
            }
            directory.delete();
        }
    }

    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
