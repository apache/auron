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
 * Working example of Auron native execution in Flink with parallelism=1.
 * This creates test data, then runs queries with Auron enabled.
 */
public class AuronFlinkMVPWorkingExample {

    public static void main(String[] args) throws Exception {
        String separator = repeatString("=", 80);
        System.out.println("\n" + separator);
        System.out.println("Flink-Auron MVP Working Example (parallelism=1)");
        System.out.println(separator);
        System.out.println("\nThis demonstrates Auron native execution with actual test data.\n");

        // Step 1: Create test data
        String testDataPath = createTestParquetData();
        System.out.println("✅ Test data created at: " + testDataPath);
        System.out.println("");

        // Step 2: Run queries with Auron enabled
        System.out.println(separator);
        System.out.println("EXECUTING QUERIES WITH AURON (parallelism=1)");
        System.out.println(separator + "\n");

        runQueriesWithAuron(testDataPath);

        // Cleanup
        cleanupTestData(testDataPath);

        System.out.println("\n" + separator);
        System.out.println("✅ Auron MVP Example Completed Successfully!");
        System.out.println(separator + "\n");
    }

    private static String createTestParquetData() throws Exception {
        System.out.println("Creating test Parquet data...");
        String testDataPath = "/tmp/auron_mvp_example_" + System.currentTimeMillis();

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create source with datagen
        tEnv.executeSql("CREATE TABLE test_source ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'datagen',"
                + "  'number-of-rows' = '100',"
                + "  'fields.id.kind' = 'sequence',"
                + "  'fields.id.start' = '1',"
                + "  'fields.id.end' = '100',"
                + "  'fields.name.length' = '20',"
                + "  'fields.amount.min' = '10.0',"
                + "  'fields.amount.max' = '500.0'"
                + ")");

        // Create Parquet sink
        tEnv.executeSql("CREATE TABLE test_sink ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + testDataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Write data
        tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source").await();

        return testDataPath;
    }

    private static void runQueriesWithAuron(String dataPath) throws Exception {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Enable Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", true);

        // Set parallelism to 1
        tEnv.getConfig().getConfiguration().setInteger("table.exec.resource.default-parallelism", 1);

        System.out.println("Configuration:");
        System.out.println("  execution.runtime-mode = BATCH");
        System.out.println("  table.optimizer.auron.enabled = true");
        System.out.println("  table.exec.resource.default-parallelism = 1");
        System.out.println("");

        // Create table pointing to test data
        tEnv.executeSql("CREATE TABLE sales ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  amount DOUBLE,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '" + dataPath + "',"
                + "  'format' = 'parquet'"
                + ")");

        // Query 1: Full scan
        System.out.println("--- Query 1: Full Scan ---");
        String query1 = "SELECT * FROM sales LIMIT 5";
        System.out.println("SQL: " + query1);
        TableResult result1 = tEnv.executeSql(query1);
        result1.print();
        System.out.println("");

        // Query 2: Projection
        System.out.println("--- Query 2: Projection (Column Pruning) ---");
        String query2 = "SELECT id, name FROM sales LIMIT 5";
        System.out.println("SQL: " + query2);
        TableResult result2 = tEnv.executeSql(query2);
        result2.print();
        System.out.println("");

        // Query 3: Filter
        System.out.println("--- Query 3: Filter (Predicate Pushdown) ---");
        String query3 = "SELECT id, name, amount FROM sales WHERE amount > 100.0 LIMIT 5";
        System.out.println("SQL: " + query3);
        TableResult result3 = tEnv.executeSql(query3);
        result3.print();
        System.out.println("");

        // Query 4: Count
        System.out.println("--- Query 4: Count ---");
        String query4 = "SELECT COUNT(*) as total_rows FROM sales";
        System.out.println("SQL: " + query4);
        TableResult result4 = tEnv.executeSql(query4);
        result4.print();
        System.out.println("");

        System.out.println("✅ All queries executed successfully with Auron!");
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
