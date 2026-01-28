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
 * Verification test that ACTUALLY EXECUTES queries to test Auron integration.
 *
 * This test creates real Parquet data, then executes queries (not just explain).
 * During execution, the AuronExecNodeGraphProcessor logs will show if conversion happens.
 *
 * To run with logging visible:
 * <pre>
 * export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home
 * cd /Users/vsowrira/git/auron/auron-flink-extension/auron-flink-planner
 *
 * # Create log4j2.properties to see Auron conversion logs
 * cat > /tmp/log4j2.properties << 'EOF'
 * rootLogger.level = INFO
 * rootLogger.appenderRef.console.ref = ConsoleAppender
 *
 * appender.console.type = Console
 * appender.console.name = ConsoleAppender
 * appender.console.layout.type = PatternLayout
 * appender.console.layout.pattern = %d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n
 *
 * # Enable Auron processor logging
 * logger.auron.name = org.apache.flink.table.planner.plan.nodes.exec.processor.AuronExecNodeGraphProcessor
 * logger.auron.level = INFO
 *
 * # Enable Auron converter logging
 * logger.auronconv.name = org.apache.auron.flink.planner
 * logger.auronconv.level = INFO
 * EOF
 *
 * # Run with logging configuration
 * java -Dlog4j.configurationFile=file:///tmp/log4j2.properties \
 *      -cp [classpath] \
 *      org.apache.auron.flink.planner.AuronExecutionVerificationTest
 * </pre>
 *
 * Look for this log message during execution:
 * <pre>
 * INFO  o.a.f.t.p.p.n.e.p.AuronExecNodeGraphProcessor - Converted BatchPhysicalTableSourceScan to Auron native execution
 * </pre>
 */
public class AuronExecutionVerificationTest {

    public static void main(String[] args) throws Exception {
        String separator = repeatString("=", 80);
        System.out.println("\n" + separator);
        System.out.println("Flink-Auron Execution Verification Test");
        System.out.println(separator);
        System.out.println("\nThis test EXECUTES queries to verify Auron integration.");
        System.out.println("Watch for INFO logs from AuronExecNodeGraphProcessor during execution.");
        System.out.println(separator + "\n");

        // Step 1: Create test Parquet data
        String testDataPath = createTestParquetData();
        System.out.println("✅ Test data created at: " + testDataPath + "\n");

        // Step 2: Execute query WITH Auron enabled
        System.out.println(separator);
        System.out.println("EXECUTING Query WITH Auron Enabled");
        System.out.println(separator);
        System.out.println("IMPORTANT: Watch for these log messages during execution:");
        System.out.println("  - 'Auron native execution is available and will be used'");
        System.out.println("  - 'Converted BatchPhysicalTableSourceScan to Auron native execution'");
        System.out.println(separator + "\n");

        executeQueryWithAuron(testDataPath, true);

        // Step 3: Execute query WITHOUT Auron for comparison
        System.out.println("\n" + separator);
        System.out.println("EXECUTING Query WITHOUT Auron (for comparison)");
        System.out.println(separator + "\n");

        executeQueryWithAuron(testDataPath, false);

        // Cleanup
        cleanupTestData(testDataPath);

        System.out.println("\n" + separator);
        System.out.println("Test Completed");
        System.out.println(separator);
        System.out.println("\nDid you see the Auron conversion log messages above?");
        System.out.println("If yes: ✅ Auron integration is working!");
        System.out.println("If no:  ⚠️  Auron conversion may not have occurred");
        System.out.println(separator + "\n");
    }

    private static String createTestParquetData() throws Exception {
        System.out.println("Creating test Parquet data...");
        String testDataPath = "/tmp/auron_exec_test_" + System.currentTimeMillis();

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Create source with generated data
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

        // Create Parquet sink
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

        // Write data and WAIT for completion
        TableResult result = tEnv.executeSql("INSERT INTO test_sink SELECT * FROM test_source");
        result.await(); // This ensures data is written before we proceed

        return testDataPath;
    }

    private static void executeQueryWithAuron(String dataPath, boolean auronEnabled) throws Exception {
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        // Configure Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", auronEnabled);

        System.out.println("Configuration:");
        System.out.println("  execution.runtime-mode = BATCH");
        System.out.println("  table.optimizer.auron.enabled = " + auronEnabled);

        // Create table pointing to test data
        // Note: Using absolute path without file:// protocol
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

        // Execute the query (not explain, actual execution)
        String query = "SELECT id, product, amount FROM sales WHERE amount > 100.0";
        System.out.println("Executing query: " + query);
        System.out.println("(Query will actually execute and process data)\n");

        long startTime = System.currentTimeMillis();

        // Execute and collect results
        TableResult result = tEnv.executeSql(query);

        // Print first 5 results to confirm execution
        int count = 0;
        java.util.Iterator<org.apache.flink.types.Row> iterator = result.collect();
        System.out.println("First 5 results:");
        while (iterator.hasNext() && count < 5) {
            System.out.println("  " + iterator.next());
            count++;
        }

        long endTime = System.currentTimeMillis();
        System.out.println("\n✅ Query executed successfully in " + (endTime - startTime) + "ms");
        System.out.println("   Total rows processed: " + count + "+");
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
