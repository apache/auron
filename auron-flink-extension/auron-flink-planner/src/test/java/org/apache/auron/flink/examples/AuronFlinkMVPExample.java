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

import java.util.Arrays;
import java.util.List;
import org.apache.auron.flink.planner.AuronFlinkPlannerExtension;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Example demonstrating Auron native execution in Flink batch mode (MVP).
 *
 * <p>This example shows how to:
 * <ul>
 *   <li>Enable Auron in Flink configuration</li>
 *   <li>Create a Parquet table source</li>
 *   <li>Execute queries with native acceleration</li>
 * </ul>
 *
 * <p><strong>Prerequisites:</strong>
 * <ul>
 *   <li>Parquet files available at specified paths</li>
 *   <li>Auron native library (libauron) in classpath</li>
 *   <li>Flink runtime mode set to BATCH</li>
 * </ul>
 *
 * <p><strong>Current Limitations (MVP):</strong>
 * <ul>
 *   <li>Manual conversion required (no automatic optimizer integration)</li>
 *   <li>Limited to Parquet scan, project, and filter operations</li>
 *   <li>No join, aggregation, or sort operations</li>
 *   <li>Batch mode only</li>
 * </ul>
 */
public class AuronFlinkMVPExample {

    public static void main(String[] args) throws Exception {
        // Step 1: Setup Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();

        // Step 2: Enable BATCH mode (required for Auron MVP)
        config.set(ExecutionOptions.RUNTIME_MODE, org.apache.flink.api.common.RuntimeExecutionMode.BATCH);

        // Step 3: Enable Auron native execution
        config.setBoolean("table.exec.auron.enable", true);
        config.setBoolean("table.exec.auron.enable.scan", true);
        config.setBoolean("table.exec.auron.enable.project", true);
        config.setBoolean("table.exec.auron.enable.filter", true);

        // Optional: Configure Auron parameters
        config.setInteger("table.exec.auron.batch-size", 8192);
        config.setDouble("table.exec.auron.memory-fraction", 0.7);
        config.setString("table.exec.auron.log-level", "INFO");

        // Validate configuration
        AuronFlinkPlannerExtension.validateBatchMode(config);
        AuronFlinkPlannerExtension.logAuronConfiguration(config);

        // Step 4: Create Table Environment
        EnvironmentSettings settings = EnvironmentSettings.fromConfiguration(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // Step 5: Create Parquet table
        tEnv.executeSql("CREATE TABLE parquet_table ("
                + "  id BIGINT,"
                + "  name STRING,"
                + "  value DOUBLE,"
                + "  created_date DATE"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = 'file:///path/to/parquet/files'," // Update with actual path
                + "  'format' = 'parquet'"
                + ")");

        // Step 6: Execute queries

        // Example 1: Simple scan (all columns)
        Table result1 = tEnv.sqlQuery("SELECT * FROM parquet_table");
        System.out.println("\n=== Example 1: Full Scan ===");
        result1.execute().print();

        // Example 2: Scan with projection
        Table result2 = tEnv.sqlQuery("SELECT id, name FROM parquet_table");
        System.out.println("\n=== Example 2: Scan with Projection ===");
        result2.execute().print();

        // Example 3: Scan with filter
        Table result3 = tEnv.sqlQuery("SELECT * FROM parquet_table WHERE value > 100");
        System.out.println("\n=== Example 3: Scan with Filter ===");
        result3.execute().print();

        // Example 4: Scan with filter and projection
        Table result4 = tEnv.sqlQuery(
                "SELECT id, name, value FROM parquet_table WHERE value > 100 AND created_date > DATE '2024-01-01'");
        System.out.println("\n=== Example 4: Scan with Filter and Projection ===");
        result4.execute().print();

        System.out.println("\n=== Auron Flink MVP Examples Completed ===");
    }

    /**
     * Example of programmatic usage (without SQL).
     * This shows how to use the AuronFlinkPlannerExtension API directly.
     */
    public static void programmaticExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration config = new Configuration();

        // Enable Auron
        config.set(ExecutionOptions.RUNTIME_MODE, org.apache.flink.api.common.RuntimeExecutionMode.BATCH);
        config.setBoolean("table.exec.auron.enable", true);

        // Define Parquet files
        List<String> parquetFiles = Arrays.asList("/path/to/file1.parquet", "/path/to/file2.parquet");

        // Define schema (would typically come from Flink's planner)
        // org.apache.flink.table.types.logical.RowType outputSchema = ...;
        // org.apache.flink.table.types.logical.RowType fullSchema = ...;

        // Create Auron-accelerated Parquet scan
        // DataStream<RowData> dataStream = AuronFlinkPlannerExtension.createAuronParquetScan(
        //         env,
        //         parquetFiles,
        //         outputSchema,
        //         fullSchema,
        //         null, // project all fields
        //         null, // no predicates
        //         4     // parallelism
        // );

        // Process results
        // dataStream.print();
        // env.execute("Auron Parquet Scan Example");
    }
}
