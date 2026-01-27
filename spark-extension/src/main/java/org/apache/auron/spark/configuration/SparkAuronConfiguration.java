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
package org.apache.auron.spark.configuration;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.configuration.ConfigOption;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.ConfigEntryWithDefaultFunction;
import org.apache.spark.sql.internal.SQLConf;
import scala.Option;
import scala.collection.immutable.List$;

/**
 * Spark configuration proxy for Auron.
 * All configuration prefixes start with spark.
 */
public class SparkAuronConfiguration extends AuronConfiguration {

    // When using getOptional, the prefix will be automatically completed. If you only need to print the Option key,
    // please manually add the prefix.
    public static final String SPARK_PREFIX = "spark.";

    public static final ConfigOption<Boolean> AURON_ENABLED = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enabled")
            .addAltKey("auron.enable")
            .withCategory("Runtime Configuration")
            .withDescription("Enable Spark Auron support to accelerate query execution with native implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> UI_ENABLED = new ConfigOption<>(Boolean.class)
            .withKey("auron.ui.enabled")
            .addAltKey("auron.ui.enable")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Enable Spark Auron UI support to display Auron-specific metrics and statistics in Spark UI.")
            .withDefaultValue(true);

    public static final ConfigOption<Double> PROCESS_MEMORY_FRACTION = new ConfigOption<>(Double.class)
            .withKey("auron.process.vmrss.memoryFraction")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Suggested fraction of process total memory (on-heap and off-heap) to use for resident memory. "
                            + "This controls the memory limit for the process's virtual memory resident set size (VMRSS).")
            .withDefaultValue(0.9);

    public static final ConfigOption<Boolean> CASE_CONVERT_FUNCTIONS_ENABLE = new ConfigOption<>(Boolean.class)
            .withKey("auron.enable.caseconvert.functions")
            .withCategory("Expression/Function Supports")
            .withDescription(
                    "Enable converting UPPER/LOWER string functions to native implementations for better performance. "
                            + "Note: May produce different outputs from Spark in special cases due to different Unicode versions.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> INPUT_BATCH_STATISTICS_ENABLE = new ConfigOption<>(Boolean.class)
            .withKey("auron.enableInputBatchStatistics")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Enable collection of additional metrics for input batch statistics to monitor data processing performance.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> UDAF_FALLBACK_ENABLE = new ConfigOption<>(Boolean.class)
            .withKey("auron.udafFallback.enable")
            .withCategory("UDAF Fallback")
            .withDescription(
                    "Enable fallback support for UDAF and other aggregate functions that are not implemented in Auron, "
                            + "allowing them to be executed using Spark's native implementation.")
            .withDefaultValue(true);

    public static final ConfigOption<Integer> SUGGESTED_UDAF_ROW_MEM_USAGE = new ConfigOption<>(Integer.class)
            .withKey("auron.suggested.udaf.memUsedSize")
            .withCategory("UDAF Fallback")
            .withDescription("Suggested memory usage size per row for TypedImperativeAggregate functions in bytes. "
                    + "This helps in memory allocation planning for UDAF operations.")
            .withDefaultValue(64);

    public static final ConfigOption<Integer> UDAF_FALLBACK_NUM_UDAFS_TRIGGER_SORT_AGG = new ConfigOption<>(
                    Integer.class)
            .withKey("auron.udafFallback.num.udafs.trigger.sortAgg")
            .withCategory("UDAF Fallback")
            .withDescription(
                    "Number of UDAFs to trigger sort-based aggregation, by default, all aggs containing udafs are converted to sort-based.")
            .withDefaultValue(1);

    public static final ConfigOption<Integer> UDAF_FALLBACK_ESTIM_ROW_SIZE = new ConfigOption<>(Integer.class)
            .withKey("auron.udafFallback.typedImperativeEstimatedRowSize")
            .withCategory("UDAF Fallback")
            .withDescription("Estimated memory size per row for TypedImperativeAggregate functions in bytes. "
                    + "This estimation is used for memory planning and allocation during UDAF fallback operations.")
            .withDefaultValue(256);

    public static final ConfigOption<Boolean> CAST_STRING_TRIM_ENABLE = new SQLConfOption<>(Boolean.class)
            .withKey("auron.cast.trimString")
            .withCategory("Expression/Function Supports")
            .withDescription(
                    "Enable automatic trimming of whitespace from string inputs before casting to numeric or boolean types. "
                            + "This helps prevent casting errors due to leading/trailing whitespace.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> IGNORE_CORRUPTED_FILES = new ConfigOption<>(Boolean.class)
            .withKey("auron.files.ignoreCorruptFiles")
            .withCategory("Data Sources")
            .withDescription("Ignore corrupted input files, defaults to spark.sql.files.ignoreCorruptFiles")
            .withDynamicDefaultValue(
                    conf -> SparkEnv.get().conf().getBoolean("spark.sql.files.ignoreCorruptFiles", false));

    public static final ConfigOption<Boolean> PARTIAL_AGG_SKIPPING_ENABLE = new ConfigOption<>(Boolean.class)
            .withKey("auron.partialAggSkipping.enable")
            .withCategory("Partial Aggregate Skipping")
            .withDescription(
                    "Enable partial aggregate skipping optimization to improve performance by skipping unnecessary "
                            + "partial aggregation stages when certain conditions are met. See issue #327 for detailed implementation.")
            .withDefaultValue(true);

    public static final ConfigOption<Double> PARTIAL_AGG_SKIPPING_RATIO = new ConfigOption<>(Double.class)
            .withKey("auron.partialAggSkipping.ratio")
            .withCategory("Partial Aggregate Skipping")
            .withDescription(
                    "Threshold ratio for partial aggregate skipping optimization. When the ratio of unique keys to total rows "
                            + "exceeds this value, partial aggregation may be skipped to improve performance.")
            .withDefaultValue(0.9);

    public static final ConfigOption<Integer> PARTIAL_AGG_SKIPPING_MIN_ROWS = new ConfigOption<>(Integer.class)
            .withKey("auron.partialAggSkipping.minRows")
            .withCategory("Partial Aggregate Skipping")
            .withDescription("Minimum number of rows required to trigger partial aggregate skipping optimization. "
                    + "This prevents the optimization from being applied to very small datasets where it may not be beneficial. "
                    + "Defaults to spark.auron.batchSize * 5")
            .withDynamicDefaultValue(
                    config -> config.getOptional(AuronConfiguration.BATCH_SIZE).get() * 5);

    public static final ConfigOption<Boolean> PARTIAL_AGG_SKIPPING_SKIP_SPILL = new ConfigOption<>(Boolean.class)
            .withKey("auron.partialAggSkipping.skipSpill")
            .withCategory("Partial Aggregate Skipping")
            .withDescription("Always skip partial aggregation when spilling is triggered to prevent memory pressure. "
                    + "When enabled, the system will bypass partial aggregation stages if memory spilling occurs, "
                    + "potentially trading off some optimization for memory stability.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> PARQUET_ENABLE_PAGE_FILTERING = new ConfigOption<>(Boolean.class)
            .withKey("auron.parquet.enable.pageFiltering")
            .withCategory("Data Sources")
            .withDescription(
                    "Enable Parquet page-level filtering to skip reading unnecessary data pages during query execution. "
                            + "This optimization can significantly improve read performance by avoiding I/O for pages that don't match filter predicates.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> PARQUET_ENABLE_BLOOM_FILTER = new ConfigOption<>(Boolean.class)
            .withKey("auron.parquet.enable.bloomFilter")
            .withCategory("Data Sources")
            .withDescription(
                    "Enable Parquet bloom filter support for efficient equality predicate filtering. "
                            + "Bloom filters can quickly determine if a value might exist in a data block, reducing unnecessary I/O operations.")
            .withDefaultValue(false);

    public static final ConfigOption<Integer> PARQUET_MAX_OVER_READ_SIZE = new ConfigOption<>(Integer.class)
            .withKey("auron.parquet.maxOverReadSize")
            .withCategory("Data Sources")
            .withDescription(
                    "Maximum over-read size in bytes for Parquet file operations. This controls how much extra data "
                            + "can be read beyond the required data to optimize I/O operations and improve read performance.")
            .withDefaultValue(16384);

    public static final ConfigOption<Integer> PARQUET_METADATA_CACHE_SIZE = new ConfigOption<>(Integer.class)
            .withKey("auron.parquet.metadataCacheSize")
            .withCategory("Data Sources")
            .withDescription("Size of the Parquet metadata cache in number of entries. This cache stores file metadata "
                    + "to avoid repeated metadata reads and improve query performance for frequently accessed files.")
            .withDefaultValue(5);

    public static final ConfigOption<String> SPARK_IO_COMPRESSION_CODEC = new ConfigOption<>(String.class)
            .withKey("io.compression.codec")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Compression codec used for Spark I/O operations. Common options include lz4, snappy, gzip, and zstd. "
                            + "The choice of codec affects both compression ratio and decompression speed.")
            .withDynamicDefaultValue(_conf -> SparkEnv.get().conf().get("spark.io.compression.codec", "lz4"));

    public static final ConfigOption<Integer> SPARK_IO_COMPRESSION_ZSTD_LEVEL = new ConfigOption<>(Integer.class)
            .withKey("io.compression.zstd.level")
            .withCategory("Runtime Configuration")
            .withDescription("Compression level for Zstandard (zstd) compression codec used in Spark I/O operations. "
                    + "Valid values range from 1 (fastest) to 22 (highest compression). Higher levels provide better compression "
                    + "but require more CPU time and memory.")
            .withDynamicDefaultValue(_conf -> SparkEnv.get().conf().getInt("spark.io.compression.zstd.level", 1));

    public static final ConfigOption<Integer> TOKIO_WORKER_THREADS_PER_CPU = new ConfigOption<>(Integer.class)
            .withKey("auron.tokio.worker.threads.per.cpu")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Number of Tokio worker threads to create per CPU core (spark.task.cpus). Set to 0 for automatic detection "
                            + "based on available CPU cores. This setting controls the thread pool size for Tokio-based asynchronous operations.")
            .withDefaultValue(0);

    public static final ConfigOption<Integer> SPARK_TASK_CPUS = new ConfigOption<>(Integer.class)
            .withKey("task.cpus")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Number of CPU cores allocated per Spark task. This setting determines the parallelism level "
                            + "for individual tasks and affects resource allocation and task scheduling. "
                            + "Defaults to spark.task.cpus.")
            .withDynamicDefaultValue(_conf -> SparkEnv.get().conf().getInt("spark.task.cpus", 1));

    public static final ConfigOption<Boolean> FORCE_SHUFFLED_HASH_JOIN = new ConfigOption<>(Boolean.class)
            .withKey("auron.forceShuffledHashJoin")
            .withCategory("Operator Supports")
            .withDescription(
                    "Force replacement of all sort-merge joins with shuffled-hash joins for performance comparison and benchmarking. "
                            + "This setting is primarily used for testing and performance analysis, as different join strategies may be optimal "
                            + "for different data distributions and query patterns.")
            .withDefaultValue(false);

    public static final ConfigOption<Integer> SHUFFLE_COMPRESSION_TARGET_BUF_SIZE = new ConfigOption<>(Integer.class)
            .withKey("auron.shuffle.compression.targetBufSize")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Target buffer size in bytes for shuffle compression operations. This setting controls the buffer size "
                            + "used during shuffle data compression, affecting both compression efficiency and memory usage. Default is 4MB (4,194,304 bytes).")
            .withDefaultValue(4194304);

    public static final ConfigOption<String> SPILL_COMPRESSION_CODEC = new ConfigOption<>(String.class)
            .withKey("auron.spill.compression.codec")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Compression codec used for Spark spill operations when data is written to disk due to memory pressure. "
                            + "Common options include lz4, snappy, and gzip. The choice affects both spill performance and disk space usage.")
            .withDefaultValue("lz4");

    public static final ConfigOption<Boolean> SMJ_FALLBACK_ENABLE = new ConfigOption<>(Boolean.class)
            .withKey("auron.smjfallback.enable")
            .withCategory("Operator Supports")
            .withDescription(
                    "Enable fallback from hash join to sort-merge join when the hash table becomes too large to fit in memory. "
                            + "This prevents out-of-memory errors by switching to a more memory-efficient join strategy when necessary.")
            .withDefaultValue(false);

    public static final ConfigOption<Integer> SMJ_FALLBACK_ROWS_THRESHOLD = new ConfigOption<>(Integer.class)
            .withKey("auron.smjfallback.rows.threshold")
            .withCategory("Operator Supports")
            .withDescription(
                    "Row count threshold that triggers fallback from hash join to sort-merge join. When the number of rows "
                            + "in the hash table exceeds this threshold, the system will switch to sort-merge join to avoid memory issues.")
            .withDefaultValue(10000000);

    public static final ConfigOption<Integer> SMJ_FALLBACK_MEM_SIZE_THRESHOLD = new ConfigOption<>(Integer.class)
            .withKey("auron.smjfallback.mem.threshold")
            .withCategory("Operator Supports")
            .withDescription("Memory size threshold in bytes that triggers fallback from hash join to sort-merge join. "
                    + "When the hash table memory usage exceeds this threshold (128MB by default), the system switches "
                    + "to sort-merge join to prevent memory overflow.")
            .withDefaultValue(134217728);

    public static final ConfigOption<Double> ON_HEAP_SPILL_MEM_FRACTION = new ConfigOption<>(Double.class)
            .withKey("auron.onHeapSpill.memoryFraction")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Maximum memory fraction allocated for on-heap spilling operations. This controls what portion "
                            + "of the available on-heap memory can be used for spilling data to disk when memory pressure occurs.")
            .withDefaultValue(0.9);

    public static final ConfigOption<Integer> SUGGESTED_BATCH_MEM_SIZE = new ConfigOption<>(Integer.class)
            .withKey("auron.suggested.batch.memSize")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Suggested memory size in bytes for record batches. This setting controls the target memory allocation "
                            + "for individual data batches to optimize memory usage and processing efficiency. Default is 8MB (8,388,608 bytes).")
            .withDefaultValue(8388608);

    public static final ConfigOption<Boolean> PARSE_JSON_ERROR_FALLBACK = new ConfigOption<>(Boolean.class)
            .withKey("auron.parseJsonError.fallback")
            .withCategory("Expression/Function Supports")
            .withDescription(
                    "Enable fallback to UDFJson implementation when native JSON parsing encounters errors. "
                            + "This ensures query execution continues even when the native JSON parser fails, at the cost of potentially slower performance.")
            .withDefaultValue(true);

    public static final ConfigOption<Integer> SUGGESTED_BATCH_MEM_SIZE_KWAY_MERGE = new ConfigOption<>(Integer.class)
            .withKey("auron.suggested.batch.memSize.multiwayMerging")
            .withCategory("Runtime Configuration")
            .withDescription(
                    "Suggested memory size in bytes for k-way merging operations. This uses a smaller batch memory size "
                            + "compared to regular operations since multiple batches are kept in memory simultaneously during k-way merging. "
                            + "Default is 1MB (1,048,576 bytes).")
            .withDefaultValue(1048576);

    public static final ConfigOption<Boolean> ORC_FORCE_POSITIONAL_EVOLUTION = new ConfigOption<>(Boolean.class)
            .withKey("auron.orc.force.positional.evolution")
            .withCategory("Data Sources")
            .withDescription(
                    "Force ORC positional evolution mode for schema evolution operations. When enabled, column mapping "
                            + "will be based on column position rather than column name, which can be useful for certain schema evolution scenarios.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> ORC_TIMESTAMP_USE_MICROSECOND = new ConfigOption<>(Boolean.class)
            .withKey("auron.orc.timestamp.use.microsecond")
            .withCategory("Data Sources")
            .withDescription(
                    "Use microsecond precision when reading ORC timestamp columns instead of the default millisecond precision. "
                            + "This provides higher temporal resolution for timestamp data but may require more storage space.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> ORC_SCHEMA_CASE_SENSITIVE = new ConfigOption<>(Boolean.class)
            .withKey("auron.orc.schema.caseSensitive.enable")
            .withCategory("Data Sources")
            .withDescription(
                    "Enable case-sensitive schema matching for ORC files. When true, column names in the schema must match "
                            + "the case of columns in the ORC file exactly. When false, column name matching is case-insensitive.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> FORCE_SHORT_CIRCUIT_AND_OR = new ConfigOption<>(Boolean.class)
            .withKey("auron.forceShortCircuitAndOr")
            .withCategory("Expression/Function Supports")
            .withDescription(
                    "Force the use of short-circuit evaluation (PhysicalSCAndExprNode/PhysicalSCOrExprNode) for AND/OR expressions, "
                            + "regardless of whether the right-hand side contains Hive UDFs. This can improve performance by avoiding unnecessary "
                            + "evaluation of expressions when the result is already determined.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> ENABLE_SCAN = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.scan")
            .withCategory("Operator Supports")
            .withDescription("Enable ScanExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_PAIMON_SCAN = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.paimon.scan")
            .withCategory("Operator Supports")
            .withDescription("Enable PaimonScanExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_PROJECT = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.project")
            .withCategory("Operator Supports")
            .withDescription("Enable ProjectExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_FILTER = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.filter")
            .withCategory("Operator Supports")
            .withDescription("Enable FilterExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_SORT = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.sort")
            .withCategory("Operator Supports")
            .withDescription("Enable SortExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_UNION = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.union")
            .withCategory("Operator Supports")
            .withDescription("Enable UnionExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_SMJ = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.smj")
            .withCategory("Operator Supports")
            .withDescription("Enable SortMergeJoinExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_SHJ = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.shj")
            .withCategory("Operator Supports")
            .withDescription("Enable ShuffledHashJoinExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_BHJ = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.bhj")
            .withCategory("Operator Supports")
            .withDescription("Enable BroadcastHashJoinExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_BNLJ = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.bnlj")
            .withCategory("Operator Supports")
            .withDescription("Enable BroadcastNestedLoopJoinExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_LOCAL_LIMIT = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.local.limit")
            .withCategory("Operator Supports")
            .withDescription("Enable LocalLimitExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_GLOBAL_LIMIT = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.global.limit")
            .withCategory("Operator Supports")
            .withDescription("Enable GlobalLimitExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_TAKE_ORDERED_AND_PROJECT = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.take.ordered.and.project")
            .withCategory("Operator Supports")
            .withDescription("Enable TakeOrderedAndProjectExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_COLLECT_LIMIT = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.collectLimit")
            .withCategory("Operator Supports")
            .withDescription("Enable CollectLimitExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_AGGR = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.aggr")
            .withCategory("Operator Supports")
            .withDescription("Enable AggregateExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_EXPAND = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.expand")
            .withCategory("Operator Supports")
            .withDescription("Enable ExpandExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_WINDOW = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.window")
            .withCategory("Operator Supports")
            .withDescription("Enable WindowExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_WINDOW_GROUP_LIMIT = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.window.group.limit")
            .withCategory("Operator Supports")
            .withDescription("Enable WindowGroupLimitExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_GENERATE = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.generate")
            .withCategory("Operator Supports")
            .withDescription("Enable GenerateExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_LOCAL_TABLE_SCAN = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.local.table.scan")
            .withCategory("Operator Supports")
            .withDescription("Enable LocalTableScanExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_DATA_WRITING = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.data.writing")
            .withCategory("Operator Supports")
            .withDescription("Enable DataWritingExec operation conversion to native Auron implementations.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> ENABLE_SCAN_PARQUET = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.scan.parquet")
            .withCategory("Data Sources")
            .withDescription("Enable ParquetScanExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_SCAN_PARQUET_TIMESTAMP = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.scan.parquet.timestamp")
            .withCategory("Data Sources")
            .withDescription(
                    "Enable ParquetScanExec operation conversion with timestamp fields to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_SCAN_ORC = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.scan.orc")
            .withCategory("Data Sources")
            .withDescription("Enable OrcScanExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_SCAN_ORC_TIMESTAMP = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.scan.orc.timestamp")
            .withCategory("Data Sources")
            .withDescription(
                    "Enable OrcScanExec operation conversion with timestamp fields to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_BROADCAST_EXCHANGE = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.broadcastExchange")
            .withCategory("Operator Supports")
            .withDescription("Enable BroadcastExchangeExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> ENABLE_SHUFFLE_EXCHANGE = new SQLConfOption<>(Boolean.class)
            .withKey("auron.enable.shuffleExchange")
            .withCategory("Operator Supports")
            .withDescription("Enable ShuffleExchangeExec operation conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> UDF_JSON_ENABLED = new SQLConfOption<>(Boolean.class)
            .withKey("auron.udf.UDFJson.enabled")
            .withCategory("Expression/Function Supports")
            .withDescription("Enable UDFJson function conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> UDF_BRICKHOUSE_ENABLED = new SQLConfOption<>(Boolean.class)
            .withKey("auron.udf.brickhouse.enabled")
            .withCategory("Expression/Function Supports")
            .withDescription("Enable Brickhouse UDF conversion to native Auron implementations.")
            .withDefaultValue(true);

    public static final ConfigOption<Boolean> DECIMAL_ARITH_OP_ENABLED = new SQLConfOption<>(Boolean.class)
            .withKey("auron.decimal.arithOp.enabled")
            .withCategory("Expression/Function Supports")
            .withDescription("Enable decimal arithmetic operations conversion to native Auron implementations.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> DATETIME_EXTRACT_ENABLED = new SQLConfOption<>(Boolean.class)
            .withKey("auron.datetime.extract.enabled")
            .withCategory("Expression/Function Supports")
            .withDescription("Enable datetime extract operations conversion to native Auron implementations.")
            .withDefaultValue(false);

    public static final ConfigOption<Boolean> UDF_SINGLE_CHILD_FALLBACK_ENABLED = new SQLConfOption<>(Boolean.class)
            .withKey("auron.udf.singleChildFallback.enabled")
            .withCategory("Expression/Function Supports")
            .withDescription("Enable falling-back UDF/expression with single child.")
            .withDefaultValue(true);

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        GetFromSparkType getFromSparkType = option instanceof SQLConfOption
                ? GetFromSparkType.FROM_SQL_CONF
                : option instanceof SparkContextOption
                        ? GetFromSparkType.FROM_SPARK_CONTEXT
                        : GetFromSparkType.FROM_SPARK_ENV;
        return Optional.ofNullable(getFromSpark(
                option.key(),
                option.altKeys(),
                option.getValueClass(),
                () -> getOptionDefaultValue(option),
                getFromSparkType));
    }

    enum GetFromSparkType {
        FROM_SPARK_ENV,
        FROM_SPARK_CONTEXT,
        FROM_SQL_CONF;
    }

    @SuppressWarnings("unchecked")
    private <T> T getFromSpark(
            String key,
            List<String> altKeys,
            Class<T> valueClass,
            Supplier<T> defaultValueSupplier,
            GetFromSparkType getFromSparkType) {
        Object configEntry;

        synchronized (SparkAuronConfiguration.class) {
            String sparkConfKey = key.startsWith(SPARK_PREFIX) ? key : SPARK_PREFIX + key;
            configEntry = ConfigEntry.findEntry(sparkConfKey);
            for (String altKey : altKeys) {
                String sparkConfAltKey = altKey.startsWith(SPARK_PREFIX) ? altKey : SPARK_PREFIX + altKey;
                if (configEntry != null) {
                    break;
                }
                configEntry = ConfigEntry.findEntry(sparkConfAltKey);
            }

            if (configEntry == null) {
                configEntry = new ConfigEntryWithDefaultFunction<>(
                        sparkConfKey,
                        Option.empty(),
                        "",
                        List$.MODULE$.empty(),
                        defaultValueSupplier::get,
                        val -> valueConverter(val, valueClass),
                        String::valueOf,
                        null,
                        true,
                        null);
            }
        }

        if (getFromSparkType == GetFromSparkType.FROM_SPARK_ENV) {
            return SparkEnv.get().conf().get((ConfigEntry<T>) configEntry);

        } else if (getFromSparkType == GetFromSparkType.FROM_SPARK_CONTEXT) {
            return SparkContext.getOrCreate().getConf().get((ConfigEntry<T>) configEntry);

        } else if (getFromSparkType == GetFromSparkType.FROM_SQL_CONF) {
            return ((ConfigEntry<T>) configEntry).readFrom(SQLConf.get().reader());

        } else {
            throw new IllegalArgumentException("unknown getFromSparkType: " + getFromSparkType);
        }
    }

    private <T> T valueConverter(String value, Class<T> valueClass) {
        if (valueClass == Integer.class) {
            return (T) Integer.valueOf(value);
        } else if (valueClass == Long.class) {
            return (T) Long.valueOf(value);
        } else if (valueClass == Boolean.class) {
            return (T) Boolean.valueOf(value);
        } else if (valueClass == Float.class) {
            return (T) Float.valueOf(value);
        } else if (valueClass == Double.class) {
            return (T) Double.valueOf(value);
        } else if (valueClass == String.class) {
            return (T) value;
        } else {
            throw new IllegalArgumentException("Unsupported default value type: " + valueClass.getName());
        }
    }
}

class SparkContextOption<T> extends ConfigOption<T> {
    SparkContextOption(Class<T> clazz) {
        super(clazz);
    }
}

class SQLConfOption<T> extends ConfigOption<T> {
    SQLConfOption(Class<T> clazz) {
        super(clazz);
    }
}
