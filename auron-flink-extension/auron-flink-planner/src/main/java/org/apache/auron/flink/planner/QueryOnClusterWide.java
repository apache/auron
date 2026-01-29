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

import java.util.Iterator;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;

/** Query wide Parquet table (50 columns) with column pruning on remote Flink cluster. */
public class QueryOnClusterWide {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: QueryOnClusterWide <data_path> <auron_enabled>");
            System.exit(1);
        }

        String dataPath = args[0];
        boolean auronEnabled = Boolean.parseBoolean(args[1]);

        String separator = repeatString("=", 80);

        System.out.println(separator);
        System.out.println("Query Execution (WIDE TABLE - 50 columns) " + (auronEnabled ? "WITH Auron" : "WITHOUT Auron"));
        System.out.println(separator);
        System.out.println();

        // Create batch table environment
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        org.apache.flink.table.api.TableEnvironment tEnv = org.apache.flink.table.api.TableEnvironment.create(settings);

        // Explicitly set BATCH mode
        tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "BATCH");

        // Configure Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", auronEnabled);

        System.out.println("Configuration:");
        System.out.println("  execution.runtime-mode = BATCH");
        System.out.println("  table.optimizer.auron.enabled = " + auronEnabled);
        System.out.println("  data.path = " + dataPath);
        System.out.println("  table schema = 50 columns (10 BIGINT + 20 DOUBLE + 20 STRING)");
        System.out.println();

        // Create wide table with 50 columns
        StringBuilder schema = new StringBuilder();
        schema.append("CREATE TABLE sales (");

        // 10 BIGINT columns
        for (int i = 0; i < 10; i++) {
            schema.append("  id").append(i).append(" BIGINT,");
        }

        // 20 DOUBLE columns
        for (int i = 0; i < 20; i++) {
            schema.append("  metric").append(i).append(" DOUBLE,");
        }

        // 20 STRING columns
        for (int i = 0; i < 19; i++) {
            schema.append("  dim").append(i).append(" STRING,");
        }
        schema.append("  dim19 STRING");

        schema.append(") WITH (");
        schema.append("  'connector' = 'filesystem',");
        schema.append("  'path' = '").append(dataPath).append("',");
        schema.append("  'format' = 'parquet'");
        schema.append(")");

        tEnv.executeSql(schema.toString());

        System.out.println("Table created successfully");
        System.out.println();

        // Query 1: Column pruning - read 4 out of 50 columns
        System.out.println("Query 1: SELECT id0, metric0, metric5, dim0 FROM sales WHERE metric0 > 5000.0 LIMIT 100");
        System.out.println("  (Reading 4 columns out of 50 total - aggressive column pruning)");
        long start1 = System.currentTimeMillis();
        TableResult result1 = tEnv.executeSql("SELECT id0, metric0, metric5, dim0 FROM sales WHERE metric0 > 5000.0 LIMIT 100");

        // Consume and display first 5 results
        Iterator<org.apache.flink.types.Row> iter1 = result1.collect();
        int count1 = 0;
        System.out.println("  First 5 results:");
        while (iter1.hasNext() && count1 < 5) {
            System.out.println("    " + iter1.next());
            count1++;
        }
        // Consume remaining results
        while (iter1.hasNext() && count1 < 100) {
            iter1.next();
            count1++;
        }
        System.out.println("  Retrieved " + count1 + " rows");

        long duration1 = System.currentTimeMillis() - start1;
        System.out.println("  ⏱  Duration: " + duration1 + "ms");
        System.out.println();

        // Query 2: Read a few more columns
        System.out.println("Query 2: SELECT id0, id1, metric0, metric10, dim0, dim5 FROM sales WHERE metric0 > 7500.0 LIMIT 100");
        System.out.println("  (Reading 6 columns out of 50 total)");
        long start2 = System.currentTimeMillis();
        TableResult result2 = tEnv.executeSql("SELECT id0, id1, metric0, metric10, dim0, dim5 FROM sales WHERE metric0 > 7500.0 LIMIT 100");

        // Consume and display first 5 results
        Iterator<org.apache.flink.types.Row> iter2 = result2.collect();
        int count2 = 0;
        System.out.println("  First 5 results:");
        while (iter2.hasNext() && count2 < 5) {
            System.out.println("    " + iter2.next());
            count2++;
        }
        // Consume remaining results
        while (iter2.hasNext() && count2 < 100) {
            iter2.next();
            count2++;
        }
        System.out.println("  Retrieved " + count2 + " rows");

        long duration2 = System.currentTimeMillis() - start2;
        System.out.println("  ⏱  Duration: " + duration2 + "ms");
        System.out.println();

        // Summary
        System.out.println(separator);
        System.out.println("Summary " + (auronEnabled ? "(WITH Auron)" : "(WITHOUT Auron)"));
        System.out.println(separator);
        System.out.println("  Query 1 (4 cols from 50): " + duration1 + "ms");
        System.out.println("  Query 2 (6 cols from 50): " + duration2 + "ms");
        System.out.println("  Total: " + (duration1 + duration2) + "ms");
        System.out.println(separator);
        System.out.println();
    }

    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
