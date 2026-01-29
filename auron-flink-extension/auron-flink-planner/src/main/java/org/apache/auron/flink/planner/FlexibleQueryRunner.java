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

/**
 * Flexible query runner that accepts arbitrary SQL and table schema.
 *
 * Usage:
 *   FlexibleQueryRunner <data_path> <auron_enabled> <table_schema> <sql_query>
 *
 * Example:
 *   FlexibleQueryRunner /tmp/data true \
 *     "id BIGINT, name STRING, amount DOUBLE" \
 *     "SELECT id, amount FROM sales WHERE amount > 1000 LIMIT 100"
 */
public class FlexibleQueryRunner {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: FlexibleQueryRunner <data_path> <auron_enabled> <table_schema> <sql_query>");
            System.err.println();
            System.err.println("Example:");
            System.err.println("  FlexibleQueryRunner /tmp/data true \\");
            System.err.println("    \"id BIGINT, name STRING, amount DOUBLE\" \\");
            System.err.println("    \"SELECT id, amount FROM sales WHERE amount > 1000 LIMIT 100\"");
            System.exit(1);
        }

        String dataPath = args[0];
        boolean auronEnabled = Boolean.parseBoolean(args[1]);
        String tableSchema = args[2];
        String sqlQuery = args[3];

        String separator = repeatString("=", 80);

        System.out.println(separator);
        System.out.println("Flexible Query Runner " + (auronEnabled ? "WITH Auron" : "WITHOUT Auron"));
        System.out.println(separator);
        System.out.println();

        // Create batch table environment
        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        org.apache.flink.table.api.TableEnvironment tEnv =
                org.apache.flink.table.api.TableEnvironment.create(settings);

        // Configure execution
        tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "BATCH");
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", auronEnabled);

        System.out.println("Configuration:");
        System.out.println("  execution.runtime-mode = BATCH");
        System.out.println("  table.optimizer.auron.enabled = " + auronEnabled);
        System.out.println("  data.path = " + dataPath);
        System.out.println();

        // Create table with provided schema
        String createTableSql = "CREATE TABLE sales (" + tableSchema + ") WITH (" +
                "  'connector' = 'filesystem'," +
                "  'path' = '" + dataPath + "'," +
                "  'format' = 'parquet'" +
                ")";

        tEnv.executeSql(createTableSql);
        System.out.println("Table created with schema:");
        System.out.println("  " + tableSchema);
        System.out.println();

        // Execute the query
        System.out.println("Executing query:");
        System.out.println("  " + sqlQuery);
        System.out.println();

        long start = System.currentTimeMillis();
        TableResult result = tEnv.executeSql(sqlQuery);

        // Consume and display first 5 results
        Iterator<org.apache.flink.types.Row> iter = result.collect();
        int count = 0;
        int displayLimit = 5;

        System.out.println("First " + displayLimit + " results:");
        while (iter.hasNext()) {
            if (count < displayLimit) {
                System.out.println("  " + iter.next());
            } else {
                iter.next(); // Consume remaining
            }
            count++;
        }

        long duration = System.currentTimeMillis() - start;

        System.out.println();
        System.out.println("Results: " + count + " rows");
        System.out.println("⏱  Duration: " + duration + "ms");
        System.out.println();

        System.out.println(separator);
        System.out.println("Query Complete");
        System.out.println(separator);
    }

    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }
}
