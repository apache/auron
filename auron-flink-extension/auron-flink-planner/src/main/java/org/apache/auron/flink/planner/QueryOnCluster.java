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

/** Query Parquet data on remote Flink cluster. */
public class QueryOnCluster {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: QueryOnCluster <data_path> <auron_enabled>");
            System.exit(1);
        }

        String dataPath = args[0];
        boolean auronEnabled = Boolean.parseBoolean(args[1]);

        String separator = repeatString("=", 80);

        System.out.println(separator);
        System.out.println("Query Execution " + (auronEnabled ? "WITH Auron" : "WITHOUT Auron"));
        System.out.println(separator);
        System.out.println();

        // Create batch table environment
        // IMPORTANT: Create in batch mode from the start to avoid boundedness detection issues
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        org.apache.flink.table.api.TableEnvironment tEnv = org.apache.flink.table.api.TableEnvironment.create(settings);

        // Explicitly set BATCH mode in configuration
        tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "BATCH");

        // Configure Auron
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", auronEnabled);

        System.out.println("Configuration:");
        System.out.println("  execution.runtime-mode = BATCH");
        System.out.println("  table.optimizer.auron.enabled = " + auronEnabled);
        System.out.println("  data.path = " + dataPath);
        System.out.println();

        // Create table
        tEnv.executeSql("CREATE TABLE sales ("
                + "  id BIGINT,"
                + "  product STRING,"
                + "  amount DOUBLE,"
                + "  category STRING"
                + ") WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '"
                + dataPath
                + "',"
                + "  'format' = 'parquet'"
                + ")");

        System.out.println("Table created successfully");
        System.out.println();

        // Query 1: Project with filter
        System.out.println("Query 1: SELECT id, product, amount FROM sales WHERE amount > 1000.0 LIMIT 100");
        long start1 = System.currentTimeMillis();
        TableResult result1 = tEnv.executeSql("SELECT id, product, amount FROM sales WHERE amount > 1000.0 LIMIT 100");

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

        // Query 2: High value items
        System.out.println("Query 2: SELECT * FROM sales WHERE amount > 4000.0 LIMIT 10");
        long start2 = System.currentTimeMillis();
        TableResult result2 = tEnv.executeSql("SELECT * FROM sales WHERE amount > 4000.0 LIMIT 10");

        // Consume and display first 5 results
        Iterator<org.apache.flink.types.Row> iter2 = result2.collect();
        int count = 0;
        System.out.println("  First 5 results:");
        while (iter2.hasNext() && count < 5) {
            System.out.println("    " + iter2.next());
            count++;
        }
        // Consume remaining results
        while (iter2.hasNext() && count < 10) {
            iter2.next();
            count++;
        }

        long duration2 = System.currentTimeMillis() - start2;
        System.out.println("  ⏱  Duration: " + duration2 + "ms");
        System.out.println();

        // Summary
        System.out.println(separator);
        System.out.println("Summary " + (auronEnabled ? "(WITH Auron)" : "(WITHOUT Auron)"));
        System.out.println(separator);
        System.out.println("  Query 1 (PROJECT+FILTER): " + duration1 + "ms");
        System.out.println("  Query 2 (FILTER+LIMIT): " + duration2 + "ms");
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
