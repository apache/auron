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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

/**
 * Compare query results between Auron and Flink native execution.
 *
 * Usage:
 *   CompareQueryRunner <data_path> <table_schema> <sql_query>
 */
public class CompareQueryRunner {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: CompareQueryRunner <data_path> <table_schema> <sql_query>");
            System.exit(1);
        }

        String dataPath = args[0];
        String tableSchema = args[1];
        String sqlQuery = args[2];

        String separator = repeatString("=", 80);

        System.out.println(separator);
        System.out.println("Query Comparison: Auron vs Flink Native");
        System.out.println(separator);
        System.out.println();
        System.out.println("Data: " + dataPath);
        System.out.println("Schema: " + tableSchema);
        System.out.println("Query: " + sqlQuery);
        System.out.println();

        // Run with Auron
        System.out.println("Executing WITH Auron...");
        QueryResult auronResult = executeQuery(dataPath, tableSchema, sqlQuery, true);

        System.out.println();

        // Run with Flink native
        System.out.println("Executing WITHOUT Auron (Flink native)...");
        QueryResult flinkResult = executeQuery(dataPath, tableSchema, sqlQuery, false);

        System.out.println();

        // Compare results
        System.out.println(separator);
        System.out.println("Comparison Results");
        System.out.println(separator);
        System.out.println();

        System.out.println("Performance:");
        System.out.println("  Auron:        " + auronResult.duration + "ms");
        System.out.println("  Flink native: " + flinkResult.duration + "ms");
        System.out.println("  Difference:   " + (auronResult.duration - flinkResult.duration) + "ms");
        System.out.println();

        System.out.println("Row Count:");
        System.out.println("  Auron:        " + auronResult.rows.size() + " rows");
        System.out.println("  Flink native: " + flinkResult.rows.size() + " rows");

        if (auronResult.rows.size() != flinkResult.rows.size()) {
            System.out.println("  ❌ ROW COUNT MISMATCH!");
            System.exit(1);
        } else {
            System.out.println("  ✅ Row counts match");
        }

        System.out.println();

        // Compare row contents
        System.out.println("Content Comparison:");
        int differences = 0;
        int samplesToShow = Math.min(5, auronResult.rows.size());

        for (int i = 0; i < auronResult.rows.size(); i++) {
            Row auronRow = auronResult.rows.get(i);
            Row flinkRow = flinkResult.rows.get(i);

            if (!rowsEqual(auronRow, flinkRow)) {
                differences++;
                if (differences <= 5) {
                    System.out.println("  Difference at row " + i + ":");
                    System.out.println("    Auron:  " + auronRow);
                    System.out.println("    Flink:  " + flinkRow);
                }
            }
        }

        if (differences == 0) {
            System.out.println("  ✅ All rows match exactly");
            System.out.println();
            System.out.println("First " + samplesToShow + " rows:");
            for (int i = 0; i < samplesToShow; i++) {
                System.out.println("    " + auronResult.rows.get(i));
            }
        } else {
            System.out.println("  ❌ CONTENT MISMATCH: " + differences + " rows differ");
            System.exit(1);
        }

        System.out.println();
        System.out.println(separator);
        System.out.println("✅ Query Results Match!");
        System.out.println(separator);
    }

    private static QueryResult executeQuery(String dataPath, String tableSchema, String sqlQuery, boolean auronEnabled)
            throws Exception {

        EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inBatchMode().build();
        org.apache.flink.table.api.TableEnvironment tEnv = org.apache.flink.table.api.TableEnvironment.create(settings);

        tEnv.getConfig().getConfiguration().setString("execution.runtime-mode", "BATCH");
        tEnv.getConfig().getConfiguration().setBoolean("table.optimizer.auron.enabled", auronEnabled);

        String createTableSql = "CREATE TABLE sales (" + tableSchema + ") WITH (" + "  'connector' = 'filesystem',"
                + "  'path' = '"
                + dataPath + "'," + "  'format' = 'parquet'"
                + ")";

        tEnv.executeSql(createTableSql);

        long start = System.currentTimeMillis();
        TableResult result = tEnv.executeSql(sqlQuery);

        List<Row> rows = new ArrayList<>();
        Iterator<Row> iter = result.collect();
        while (iter.hasNext()) {
            Row row = iter.next();
            // Create a copy of the row
            Row copy = Row.of(new Object[row.getArity()]);
            for (int i = 0; i < row.getArity(); i++) {
                copy.setField(i, row.getField(i));
            }
            rows.add(copy);
        }

        long duration = System.currentTimeMillis() - start;

        System.out.println("  Retrieved " + rows.size() + " rows in " + duration + "ms");

        return new QueryResult(rows, duration);
    }

    private static boolean rowsEqual(Row a, Row b) {
        if (a.getArity() != b.getArity()) {
            return false;
        }

        for (int i = 0; i < a.getArity(); i++) {
            Object aVal = a.getField(i);
            Object bVal = b.getField(i);

            if (aVal == null && bVal == null) {
                continue;
            }

            if (aVal == null || bVal == null) {
                return false;
            }

            if (!aVal.equals(bVal)) {
                return false;
            }
        }

        return true;
    }

    private static String repeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    static class QueryResult {
        List<Row> rows;
        long duration;

        QueryResult(List<Row> rows, long duration) {
            this.rows = rows;
            this.duration = duration;
        }
    }
}
