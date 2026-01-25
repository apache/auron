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
package org.apache.auron.flink.table;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for Flink Table Tests.
 * Enhanced with Parquet test utilities for Auron integration testing.
 */
public class AuronFlinkTableTestBase {

    protected StreamExecutionEnvironment environment;
    protected StreamTableEnvironment tableEnvironment;
    protected File tempParquetDir;

    @BeforeEach
    public void before() {
        environment = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.STREAMING);
        tableEnvironment =
                StreamTableEnvironment.create(environment, EnvironmentSettings.fromConfiguration(configuration));
        String timestampDataId = TestValuesTableFactory.registerData(Arrays.asList(
                row("2020-10-10 00:00:01", 1, 1d, 1f, new BigDecimal("1.11"), "Hi", "a"),
                row("2020-10-10 00:00:02", 2, 2d, 2f, new BigDecimal("2.22"), "Comment#1", "a"),
                row("2020-10-10 00:00:03", 2, 2d, 2f, new BigDecimal("2.22"), "Comment#1", "a")));
        tableEnvironment.executeSql(" CREATE TABLE T1 ( "
                + "\n `ts` String, "
                + "\n `int` INT, "
                + "\n `double` DOUBLE, "
                + "\n `float` FLOAT, "
                + "\n `bigdec` DECIMAL(10, 2), "
                + "\n `string` STRING, "
                + "\n `name` STRING "
                + "\n ) WITH ( "
                + "\n 'connector' = 'values',"
                + "\n 'data-id' = '" + timestampDataId + "',"
                + "\n 'failing-source' = 'false' "
                + "\n )");
    }

    protected Row row(Object... values) {
        Row row = new Row(values.length);
        for (int i = 0; i < values.length; i++) {
            row.setField(i, values[i]);
        }
        return row;
    }

    /**
     * Creates a temporary directory for Parquet test files.
     */
    protected File createTempParquetDir() throws IOException {
        tempParquetDir = Files.createTempDirectory("auron-flink-test-parquet-").toFile();
        tempParquetDir.deleteOnExit();
        return tempParquetDir;
    }

    /**
     * Writes test data to a Parquet file using Flink's FileSystem connector.
     *
     * @param targetDir Directory to write Parquet files
     * @param tableName Name of the table to create
     * @param schema Table schema as a SQL string
     * @param data Test data rows
     * @throws Exception if write fails
     */
    protected void writeParquetTestData(File targetDir, String tableName, String schema, List<Row> data)
            throws Exception {
        // Register test data
        String dataId = TestValuesTableFactory.registerData(data);

        // Create source table with test data
        tableEnvironment.executeSql("CREATE TABLE " + tableName + "_source " + schema + " WITH ("
                + "  'connector' = 'values',"
                + "  'bounded' = 'true',"
                + "  'data-id' = '"
                + dataId + "'" + ")");

        // Create sink table that writes to Parquet
        tableEnvironment.executeSql("CREATE TABLE " + tableName + "_sink " + schema + " WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = 'file://"
                + targetDir.getAbsolutePath() + "/" + tableName + "'," + "  'format' = 'parquet'"
                + ")");

        // Insert data into Parquet sink
        tableEnvironment
                .executeSql("INSERT INTO " + tableName + "_sink SELECT * FROM " + tableName + "_source")
                .await();
    }

    /**
     * Creates a Parquet table source for testing.
     *
     * @param tableName Name of the table
     * @param schema Table schema as SQL string
     * @param parquetPath Path to Parquet files
     */
    protected void createParquetTable(String tableName, String schema, String parquetPath) {
        tableEnvironment.executeSql("CREATE TABLE " + tableName + " " + schema + " WITH ("
                + "  'connector' = 'filesystem',"
                + "  'path' = '"
                + parquetPath + "'," + "  'format' = 'parquet'"
                + ")");
    }

    /**
     * Cleans up temporary Parquet files.
     */
    protected void cleanupParquetDir() {
        if (tempParquetDir != null && tempParquetDir.exists()) {
            deleteDirectory(tempParquetDir);
        }
    }

    private void deleteDirectory(File directory) {
        File[] files = directory.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        directory.delete();
    }

    /**
     * Enables Auron native execution in the table environment.
     */
    protected void enableAuron() {
        Configuration config = tableEnvironment.getConfig().getConfiguration();
        config.setBoolean("table.exec.auron.enable", true);
        config.setBoolean("table.exec.auron.enable.scan", true);
        config.setBoolean("table.exec.auron.enable.project", true);
        config.setBoolean("table.exec.auron.enable.filter", true);
        config.setInteger("table.exec.auron.batch-size", 8192);
    }

    /**
     * Collects all results from a TableResult into a List of Rows.
     *
     * @param result The TableResult to collect
     * @return List of Row objects
     */
    protected List<Row> collectResults(org.apache.flink.table.api.TableResult result) {
        List<Row> results = new java.util.ArrayList<>();
        try (org.apache.flink.util.CloseableIterator<Row> iterator = result.collect()) {
            while (iterator.hasNext()) {
                results.add(iterator.next());
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to collect results", e);
        }
        return results;
    }

    /**
     * Sets the execution mode to BATCH (required for Auron MVP).
     */
    protected void setBatchMode() {
        Configuration config = tableEnvironment.getConfig().getConfiguration();
        config.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
    }
}
