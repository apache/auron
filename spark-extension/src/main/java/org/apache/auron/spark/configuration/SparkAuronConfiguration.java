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

import static org.apache.auron.util.Preconditions.checkNotNull;

import java.util.Optional;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.configuration.ConfigOption;
import org.apache.auron.configuration.ConfigOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.internal.config.ConfigEntry;
import org.apache.spark.internal.config.ConfigEntryWithDefault;
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

    public static final ConfigOption<Boolean> UI_ENABLED = ConfigOptions.key("auron.ui.enabled")
            .description("support spark.auron.ui.enabled.")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Double> PROCESS_MEMORY_FRACTION = ConfigOptions.key(
                    "auron.process.vmrss.memoryFraction")
            .description("suggested fraction of process total memory (on-heap and off-heap). "
                    + "this limit is for process's resident memory usage.")
            .doubleType()
            .defaultValue(0.9);

    public static final ConfigOption<Boolean> CASE_CONVERT_FUNCTIONS_ENABLE = ConfigOptions.key(
                    "auron.enable.caseconvert.functions")
            .description("enable converting upper/lower functions to native, special cases may provide different, "
                    + "outputs from spark due to different unicode versions. ")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Boolean> INPUT_BATCH_STATISTICS_ENABLE = ConfigOptions.key(
                    "auron.enableInputBatchStatistics")
            .description("enable extra metrics of input batch statistics. ")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Boolean> UDAF_FALLBACK_ENABLE = ConfigOptions.key("auron.udafFallback.enable")
            .description("supports UDAF and other aggregate functions not implemented. ")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Integer> SUGGESTED_UDAF_ROW_MEM_USAGE = ConfigOptions.key(
                    "auron.suggested.udaf.memUsedSize")
            .description("TypedImperativeAggregate one row mem use size. ")
            .intType()
            .defaultValue(64);

    public static final ConfigOption<Integer> UDAF_FALLBACK_NUM_UDAFS_TRIGGER_SORT_AGG = ConfigOptions.key(
                    "auron.udafFallback.num.udafs.trigger.sortAgg")
            .description(
                    "number of udafs to trigger sort-based aggregation, by default, all aggs containing udafs are converted to sort-based.")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> UDAF_FALLBACK_ESTIM_ROW_SIZE = ConfigOptions.key(
                    "auron.udafFallback.typedImperativeEstimatedRowSize")
            .description("TypedImperativeAggregate one row mem use size.")
            .intType()
            .defaultValue(256);

    public static final ConfigOption<Boolean> CAST_STRING_TRIM_ENABLE = ConfigOptions.key("auron.cast.trimString")
            .description("enable trimming string inputs before casting to numeric/boolean types. ")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Boolean> IGNORE_CORRUPTED_FILES = ConfigOptions.key("files.ignoreCorruptFiles")
            .description("ignore corrupted input files. ")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Boolean> PARTIAL_AGG_SKIPPING_ENABLE = ConfigOptions.key(
                    "auron.partialAggSkipping.enable")
            .description("enable partial aggregate skipping (see https://github.com/apache/auron/issues/327). ")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Double> PARTIAL_AGG_SKIPPING_RATIO = ConfigOptions.key(
                    "auron.partialAggSkipping.ratio")
            .description("partial aggregate skipping ratio. ")
            .doubleType()
            .defaultValue(0.9);

    public static final ConfigOption<Integer> PARTIAL_AGG_SKIPPING_MIN_ROWS = ConfigOptions.key(
                    "auron.partialAggSkipping.minRows")
            .description("minimum number of rows to trigger partial aggregate skipping.")
            .intType()
            .dynamicDefaultValue(
                    config -> config.getOptional(AuronConfiguration.BATCH_SIZE).get() * 5);

    public static final ConfigOption<Boolean> PARTIAL_AGG_SKIPPING_SKIP_SPILL = ConfigOptions.key(
                    "auron.partialAggSkipping.skipSpill")
            .description("always skip partial aggregate when triggered spilling. ")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Boolean> PARQUET_ENABLE_PAGE_FILTERING = ConfigOptions.key(
                    "auron.parquet.enable.pageFiltering")
            .description("parquet enable page filtering. ")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Boolean> PARQUET_ENABLE_BLOOM_FILTER = ConfigOptions.key(
                    "auron.parquet.enable.bloomFilter")
            .description("parquet enable bloom filter. ")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Integer> PARQUET_MAX_OVER_READ_SIZE = ConfigOptions.key(
                    "auron.parquet.maxOverReadSize")
            .description("parquet max over read size.")
            .intType()
            .defaultValue(16384);

    public static final ConfigOption<Integer> PARQUET_METADATA_CACHE_SIZE = ConfigOptions.key(
                    "auron.parquet.metadataCacheSize")
            .description("parquet metadata cache size.")
            .intType()
            .defaultValue(5);

    public static final ConfigOption<String> SPARK_IO_COMPRESSION_CODEC = ConfigOptions.key("io.compression.codec")
            .description("spark io compression codec.")
            .stringType()
            .defaultValue("lz4");

    public static final ConfigOption<Integer> SPARK_IO_COMPRESSION_ZSTD_LEVEL = ConfigOptions.key(
                    "io.compression.zstd.level")
            .description("spark io compression zstd level.")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> TOKIO_WORKER_THREADS_PER_CPU = ConfigOptions.key(
                    "auron.tokio.worker.threads.per.cpu")
            .description("tokio worker threads per cpu (spark.task.cpus), 0 for auto detection.")
            .intType()
            .defaultValue(0);

    public static final ConfigOption<Integer> SPARK_TASK_CPUS = ConfigOptions.key("task.cpus")
            .description("number of cpus per task.")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Boolean> FORCE_SHUFFLED_HASH_JOIN = ConfigOptions.key(
                    "auron.forceShuffledHashJoin")
            .description("replace all sort-merge join to shuffled-hash join, only used for benchmarking. ")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Integer> SHUFFLE_COMPRESSION_TARGET_BUF_SIZE = ConfigOptions.key(
                    "auron.shuffle.compression.targetBufSize")
            .description("shuffle compression target buffer size, default is 4MB.")
            .intType()
            .defaultValue(4194304);

    public static final ConfigOption<String> SPILL_COMPRESSION_CODEC = ConfigOptions.key(
                    "auron.spill.compression.codec")
            .description("spark spill compression codec.")
            .stringType()
            .defaultValue("lz4");

    public static final ConfigOption<Boolean> SMJ_FALLBACK_ENABLE = ConfigOptions.key("auron.smjfallback.enable")
            .description("enable hash join falling back to sort merge join when hash table is too big. ")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Integer> SMJ_FALLBACK_ROWS_THRESHOLD = ConfigOptions.key(
                    "auron.smjfallback.rows.threshold")
            .description("smj fallback threshold.")
            .intType()
            .defaultValue(10000000);

    public static final ConfigOption<Integer> SMJ_FALLBACK_MEM_SIZE_THRESHOLD = ConfigOptions.key(
                    "auron.smjfallback.mem.threshold")
            .description("smj fallback mem threshold.")
            .intType()
            .defaultValue(134217728);

    public static final ConfigOption<Double> ON_HEAP_SPILL_MEM_FRACTION = ConfigOptions.key(
                    "auron.onHeapSpill.memoryFraction")
            .description("max memory fraction of on-heap spills. ")
            .doubleType()
            .defaultValue(0.9);

    public static final ConfigOption<Integer> SUGGESTED_BATCH_MEM_SIZE = ConfigOptions.key(
                    "auron.suggested.batch.memSize")
            .description("suggested memory size for record batch.")
            .intType()
            .defaultValue(8388608);

    public static final ConfigOption<Boolean> PARSE_JSON_ERROR_FALLBACK = ConfigOptions.key(
                    "auron.parseJsonError.fallback")
            .description("fallback to UDFJson when error parsing json in native implementation. ")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<Integer> SUGGESTED_BATCH_MEM_SIZE_KWAY_MERGE = ConfigOptions.key(
                    "auron.suggested.batch.memSize.multiwayMerging")
            .description("suggested memory size for k-way merging use smaller batch memory size for "
                    + "k-way merging since there will be multiple batches in memory at the same time.")
            .intType()
            .defaultValue(1048576);
    public static final ConfigOption<Boolean> ORC_FORCE_POSITIONAL_EVOLUTION = ConfigOptions.key(
                    "auron.orc.force.positional.evolution")
            .description("orc force positional evolution. ")
            .booleanType()
            .defaultValue(false);
    public static final ConfigOption<Boolean> ORC_TIMESTAMP_USE_MICROSECOND = ConfigOptions.key(
                    "auron.orc.timestamp.use.microsecond")
            .description("use microsecond precision when reading ORC timestamp columns. ")
            .booleanType()
            .defaultValue(false);
    public static final ConfigOption<Boolean> ORC_SCHEMA_CASE_SENSITIVE = ConfigOptions.key(
                    "auron.orc.schema.caseSensitive.enable")
            .description("whether ORC file schema matching distinguishes between uppercase and lowercase. ")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Boolean> FORCE_SHORT_CIRCUIT_AND_OR = ConfigOptions.key(
                    "auron.forceShortCircuitAndOr")
            .description("force using short-circuit evaluation (PhysicalSCAndExprNode/PhysicalSCOrExprNode) "
                    + "for And/Or expressions, regardless of whether rhs contains HiveUDF. ")
            .booleanType()
            .defaultValue(false);

    private final SparkConf sparkConf;

    public SparkAuronConfiguration(SparkConf conf) {
        this.sparkConf = checkNotNull(conf, "spark conf cannot be null");
    }

    @Override
    public <T> Optional<T> getOptional(ConfigOption<T> option) {
        if (option.key().startsWith(SPARK_PREFIX)) {
            return Optional.ofNullable(getSparkConf(option.key(), getOptionDefaultValue(option)));
        } else {
            return Optional.ofNullable(getSparkConf(SPARK_PREFIX + option.key(), getOptionDefaultValue(option)));
        }
    }

    @Override
    public <T> Optional<T> getOptional(String key) {
        if (key.startsWith(SPARK_PREFIX)) {
            return Optional.ofNullable(getSparkConf(key, null));
        } else {
            return Optional.ofNullable(getSparkConf(SPARK_PREFIX + key, null));
        }
    }

    private synchronized <T> T getSparkConf(String key, T defaultValue) {
        // Use synchronized to avoid issues with multiple threads.
        synchronized (ConfigEntry.class) {
            ConfigEntry<T> entry = (ConfigEntry<T>) ConfigEntry.findEntry(key);
            if (entry == null) {
                entry = new ConfigEntryWithDefault<>(
                        key,
                        Option.<String>empty(),
                        "",
                        List$.MODULE$.empty(),
                        defaultValue,
                        (val) -> valueConverter(val, defaultValue, defaultValue == null),
                        String::valueOf,
                        null,
                        true,
                        null);
            }
            return sparkConf.get(entry);
        }
    }

    private <T> T valueConverter(String value, T defaultValue, boolean defaultValueIsNull) {
        if (defaultValueIsNull) {
            return (T) value;
        } else {
            if (defaultValue instanceof Integer) {
                return (T) Integer.valueOf(value);
            } else if (defaultValue instanceof Long) {
                return (T) Long.valueOf(value);
            } else if (defaultValue instanceof Boolean) {
                return (T) Boolean.valueOf(value);
            } else if (defaultValue instanceof Float) {
                return (T) Float.valueOf(value);
            } else if (defaultValue instanceof Double) {
                return (T) Double.valueOf(value);
            } else if (defaultValue instanceof String) {
                return (T) String.valueOf(value);
            } else {
                throw new IllegalArgumentException("Unsupported default value type: "
                        + defaultValue.getClass().getName());
            }
        }
    }
}
