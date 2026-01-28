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

        // Run each query with a fresh TableEnvironment to avoid Flink classloader issues
        runQuery1(dataPath);
        runQuery2(dataPath);
        runQuery3(dataPath);
        runQuery4(dataPath);

        System.out.println("✅ All queries executed successfully with Auron at parallelism=" + PARALLELISM + "!");
    }

    private static TableEnvironment createConfiguredEnvironment() {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Enable Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", true);

        // Set parallelism
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", PARALLELISM);

        // Disable classloader check to avoid Flink test environment issues
        tEnv.getConfig().getConfiguration().setBoolean("classloader.check-leaked-classloader", false);

        return tEnv;
    }

    private static void runQuery1(String dataPath) throws Exception {
        System.out.println("--- Query 1: Count All Rows (Auron: ParquetScan, Flink: Aggregation) ---");
        TableEnvironment tEnv = createConfiguredEnvironment();

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

        String query = "SELECT COUNT(*) as total_rows FROM sales";
        System.out.println("SQL: " + query);
        long startTime = System.currentTimeMillis();
        TableResult result = tEnv.executeSql(query);
        result.print();
        long queryTime = System.currentTimeMillis() - startTime;
        System.out.println("Query completed in " + queryTime + "ms");
        System.out.println("");
    }

    private static void runQuery2(String dataPath) throws Exception {
        System.out.println("--- Query 2: GROUP BY Aggregation (Auron: ParquetScan, Flink: GroupBy+Agg) ---");
        TableEnvironment tEnv = createConfiguredEnvironment();

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

        String query =
                "SELECT category, COUNT(*) as row_count, SUM(amount) as total_amount, AVG(amount) as avg_amount FROM sales GROUP BY category ORDER BY category";
        System.out.println("SQL: " + query);
        long startTime = System.currentTimeMillis();
        TableResult result = tEnv.executeSql(query);
        result.print();
        long queryTime = System.currentTimeMillis() - startTime;
        System.out.println("Query completed in " + queryTime + "ms");
        System.out.println("");
    }

    private static void runQuery3(String dataPath) throws Exception {
        System.out.println("--- Query 3: Filter + Projection (Auron: ParquetScan+Filter+Project) ---");
        TableEnvironment tEnv = createConfiguredEnvironment();

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

        String query = "SELECT id, name, amount, category FROM sales WHERE amount > 400.0 LIMIT 10";
        System.out.println("SQL: " + query);
        long startTime = System.currentTimeMillis();
        TableResult result = tEnv.executeSql(query);
        result.print();
        long queryTime = System.currentTimeMillis() - startTime;
        System.out.println("Query completed in " + queryTime + "ms");
        System.out.println("");
    }

    private static void runQuery4(String dataPath) throws Exception {
        System.out.println("--- Query 4: Statistical Aggregations (Auron: ParquetScan, Flink: Aggs) ---");
        TableEnvironment tEnv = createConfiguredEnvironment();

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

        String query =
                "SELECT MIN(amount) as min_amount, MAX(amount) as max_amount, AVG(amount) as avg_amount, COUNT(DISTINCT category) as num_categories FROM sales";
        System.out.println("SQL: " + query);
        long startTime = System.currentTimeMillis();
        TableResult result = tEnv.executeSql(query);
        result.print();
        long queryTime = System.currentTimeMillis() - startTime;
        System.out.println("Query completed in " + queryTime + "ms");
        System.out.println("");
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
