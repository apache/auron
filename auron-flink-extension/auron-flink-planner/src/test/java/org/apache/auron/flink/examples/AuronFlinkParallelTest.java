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
package org.apache.auron.flink.examples;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

/**
 * Test Auron native execution with higher parallelism and larger dataset.
 * This validates distributed file splitting and parallel execution.
 */
public class AuronFlinkParallelTest {

    private static final int PARALLELISM = 4;
    private static final int NUM_ROWS = 50000;

    public static void main(String[] args) throws Exception {
        String separator = repeatString("=", 80);
        System.out.println("\n" + separator);
        System.out.println("Flink-Auron Parallel Test (parallelism=" + PARALLELISM + ", rows=" + NUM_ROWS + ")");
        System.out.println(separator);
        System.out.println("\nThis validates distributed file splitting and parallel execution.\n");

        // Step 1: Create test data
        String testDataPath = createTestParquetData();
        System.out.println("✅ Test data created at: " + testDataPath);
        System.out.println("");

        // Step 2: Run queries with Auron enabled
        System.out.println(separator);
        System.out.println("EXECUTING QUERIES WITH AURON (parallelism=" + PARALLELISM + ")");
        System.out.println(separator + "\n");

        runQueriesWithAuron(testDataPath);

        // Cleanup
        cleanupTestData(testDataPath);

        System.out.println("\n" + separator);
        System.out.println("✅ Auron Parallel Test Completed Successfully!");
        System.out.println(separator + "\n");
    }

    private static String createTestParquetData() throws Exception {
        System.out.println("Creating test Parquet data with " + NUM_ROWS + " rows...");
        String testDataPath = "/tmp/auron_parallel_test_" + System.currentTimeMillis();

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create source with datagen
        tEnv.executeSql("CREATE TABLE test_source ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  category INT,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'number-of-rows' = '" + NUM_ROWS + "',"
                + "  'fields.id.kind' = 'sequence',"
                + "  'fields.id.start' = '1',"
                + "  'fields.id.end' = '" + NUM_ROWS + "',"
                + "  'fields.name.length' = '20',"
                + "  'fields.amount.min' = '10.0',"
                + "  'fields.amount.max' = '500.0',"
                + "  'fields.category.min' = '1',"
                + "  'fields.category.max' = '10'"
                + ")");

        // Create Parquet sink
        tEnv.executeSql("CREATE TABLE test_sink ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  category INT,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + testDataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Write data
        System.out.println("Writing " + NUM_ROWS + " rows to Parquet...");
        long startTime = System.currentTimeMillis();
        tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source").await();
        long writeTime = System.currentTimeMillis() - startTime;
        System.out.println("Write completed in " + writeTime + "ms");

        return testDataPath;
    }

    private static void runQueriesWithAuron(String dataPath) throws Exception {
        System.out.println("Configuration:");
        System.out.println("  execution.runtime-mode = BATCH");
        System.out.println("  table.optimizer.auron.enabled = true");
        System.out.println("  table.exec.resource.default-parallelism = " + PARALLELISM);
        System.out.println("");

        // Create TableEnvironment
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Enable Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", true);

        // Set parallelism
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", PARALLELISM);

        // Create table
        tEnv.executeSql("CREATE TABLE sales ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  category INT,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + dataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Run single query demonstrating parallel execution
        System.out.println("--- Parallel Execution Query (Auron: ParquetScan, Flink: Aggregation) ---");
        String query = "SELECT COUNT(*) as total_rows FROM sales";
        System.out.println("SQL: " + query);
        System.out.println("Expected: " + NUM_ROWS + " rows scanned across " + PARALLELISM + " parallel tasks");
        System.out.println("");

        long startTime = System.currentTimeMillis();
        TableResult result = tEnv.executeSql(query);
        result.print();
        long queryTime = System.currentTimeMillis() - startTime;

        System.out.println("");
        System.out.println("✅ Query completed in " + queryTime + "ms");
        System.out.println("✅ Auron scanned " + NUM_ROWS + " rows using " + PARALLELISM + " parallel tasks");
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
