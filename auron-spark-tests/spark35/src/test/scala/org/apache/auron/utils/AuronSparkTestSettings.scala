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
package org.apache.auron.utils

import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.parquet._

class AuronSparkTestSettings extends SparkTestSettings {
  {
    // Use Arrow's unsafe implementation.
    System.setProperty("arrow.allocation.manager.type", "Unsafe")
  }

  enableSuite[AuronDataFrameFunctionsSuite]
    // Native execution wraps SparkRuntimeException from validation failures in SparkException.
    .exclude("map with arrays")
    .exclude("map_concat function")
    .exclude("SPARK-24734: Fix containsNull of Concat for array type")
    .exclude("array_insert functions")
    .exclude("transform keys function - Invalid lambda functions and exceptions")
    // Native reverse only supports string inputs; array inputs fail with unsupported List type.
    .exclude("reverse function - array for primitive type not containing null")
    .exclude("reverse function - array for primitive type containing null")
    .exclude("reverse function - array for non-primitive type")
    // Native flatten can fail when child arrays have different containsNull metadata.
    .exclude("flatten function")

  enableSuite[AuronDateFunctionsSuite]
    // Native execution wraps Spark parsing/format validation exceptions in SparkException.
    .exclude("function to_date")
    .exclude("unix_timestamp")
    .exclude("to_unix_timestamp")
    // Native date_trunc does not support all Spark granularity aliases.
    .exclude("function date_trunc")
    // Native date_trunc throws for unsupported fields instead of returning NULL as Spark does.
    .exclude("unsupported fmt fields for trunc/date_trunc results null")
    // Native date_trunc may produce incorrect results for historical timestamps with
    // non-UTC timezones due to timezone handling differences in the DataFusion engine.
    .exclude("SPARK-30766: date_trunc of old timestamps to hours and days")

  enableSuite[AuronMathFunctionsSuite]
    // Native acosh uses a different floating-point formula than Spark's StrictMath.log,
    // producing results that differ at the last ULP for certain edge-case inputs.
    .exclude("acosh")

  enableSuite[AuronMiscFunctionsSuite]

  enableSuite[AuronStringFunctionsSuite]
    // Native levenshtein has a Spark 3.5+ result or schema comparison mismatch.
    .exclude("string Levenshtein distance")
    // Native substr does not support BinaryType inputs.
    // See https://github.com/apache/auron/issues/1724
    .exclude("string / binary substring function")

  enableSuite[AuronDataFrameAggregateSuite]
    // See https://github.com/apache/auron/issues/1840
    .excludeByPrefix("collect functions")
    // A custom version of the SPARK-19471 test has been added to AuronDataFrameAggregateSuite
    // with modified plan checks for Auron's native aggregates, so we exclude the original here.
    .exclude(
      "SPARK-19471: AggregationIterator does not initialize the generated result projection before using it")
    .exclude(
      "SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail")
    .exclude("SPARK-16484: hll_*_agg + hll_union negative tests")
    .exclude("SPARK-43876: Enable fast hashmap for distinct queries")

  enableSuite[AuronDatasetAggregatorSuite]

  enableSuite[AuronTypedImperativeAggregateSuite]

  enableSuite[AuronDataFrameSuite]
    // Auron-specific implementations of these tests are provided above
    .exclude("repartitionByRange")
    .exclude("distributeBy and localSort")
    .exclude("reuse exchange")
    .exclude("SPARK-22520: support code generation for large CaseWhen")
    .exclude("SPARK-27439: Explain result should match collected result after view change")
    // These tests fail due to Auron native execution differences
    .exclude("SPARK-28067: Aggregate sum should not return wrong results for decimal overflow")
    .exclude("SPARK-35955: Aggregate avg should not return wrong results for decimal overflow")
    .exclude("NaN is greater than all other non-NaN numeric values")
    .exclude("SPARK-20897: cached self-join should not fail")
    .exclude("SPARK-22271: mean overflows and returns null for some decimal variables")
    .exclude("SPARK-32764: -0.0 and 0.0 should be equal")
    .exclude("SPARK-41048: Improve output partitioning and ordering with AQE cache")

  enableSuite[AuronParquetAvroCompatibilitySuite]
  enableSuite[AuronParquetColumnIndexSuite]
  enableSuite[AuronParquetCompatibilityTest]
  enableSuite[AuronParquetCompressionCodecPrecedenceSuite]
  enableSuite[AuronParquetEncodingSuite]
  enableSuite[AuronParquetFieldIdIOSuite]
    .exclude("Parquet reads infer fields using field ids correctly")
    .exclude("absence of field ids")
    .exclude("SPARK-38094: absence of field ids: reading nested schema")
    .exclude("multiple id matches")
    .exclude("read parquet file without ids")
    .exclude("global read/write flag should work correctly")
  enableSuite[AuronParquetFieldIdSchemaSuite]
  enableSuite[AuronParquetFileFormatSuite]
  enableSuite[AuronParquetFileFormatV1Suite]
  enableSuite[AuronParquetFileFormatV2Suite]
  enableSuite[AuronParquetIOSuite]
    .exclude("vectorized reader: missing all struct fields")
    .exclude("SPARK-34817: Read UINT_64 as Decimal from parquet")
    .exclude("SPARK-35640: read binary as timestamp should throw schema incompatible error")
    .exclude("SPARK-35640: int as long should throw schema incompatible error")
    .exclude("read dictionary encoded decimals written as INT32")
    .exclude("explode nested lists crossing a rowgroup boundary")
    .exclude("read dictionary encoded decimals written as INT64")
    .exclude("read dictionary encoded decimals written as FIXED_LEN_BYTE_ARRAY")
    .exclude("read dictionary and plain encoded timestamp_millis written as INT64")
    .exclude("SPARK-40128 read DELTA_LENGTH_BYTE_ARRAY encoded strings")
    .exclude("SPARK-36726: test incorrect Parquet row group file offset")
    .exclude("SPARK-34167: read LongDecimals with precision < 10, VectorizedReader true")
    .exclude("SPARK-34167: read LongDecimals with precision < 10, VectorizedReader false")
  enableSuite[AuronParquetInteroperabilitySuite]
    .exclude("parquet timestamp conversion")
  enableSuite[AuronParquetPartitionDiscoverySuite]
  enableSuite[AuronParquetProtobufCompatibilitySuite]
    .exclude("unannotated array of primitive type")
    .exclude("unannotated array of struct")
    .exclude("struct with unannotated array")
    .exclude("unannotated array of struct with unannotated array")
    .exclude("unannotated array of string")
  enableSuite[AuronParquetQuerySuite]
    .exclude("SPARK-10634 timestamp written and read as INT64 - truncation")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
    .exclude("Migration from INT96 to TIMESTAMP_MICROS timestamp type")
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    .exclude("row group skipping doesn't overflow when reading into larger type")
  enableSuite[AuronParquetRebaseDatetimeSuite]
    .exclude(
      "SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps")
    .exclude("SPARK-31159, SPARK-37705: rebasing timestamps in write")
    .exclude("SPARK-31159: rebasing dates in write")
    .exclude("SPARK-35427: datetime rebasing in the EXCEPTION mode")
  enableSuite[AuronParquetRebaseDatetimeV1Suite]
    .exclude(
      "SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps")
    .exclude("SPARK-31159, SPARK-37705: rebasing timestamps in write")
    .exclude("SPARK-31159: rebasing dates in write")
    .exclude("SPARK-35427: datetime rebasing in the EXCEPTION mode")
  enableSuite[AuronParquetRebaseDatetimeV2Suite]
    .exclude(
      "SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps")
    .exclude("SPARK-35427: datetime rebasing in the EXCEPTION mode")
  enableSuite[AuronParquetSchemaInferenceSuite]
  enableSuite[AuronParquetSchemaPruningSuite]
  enableSuite[AuronParquetSchemaSuite]
    .exclude("schema mismatch failure error message for parquet reader")
    .exclude("schema mismatch failure error message for parquet vectorized reader")
    .exclude("SPARK-40819: parquet file with TIMESTAMP(NANOS, true) (with nanosAsLong=true)")
    .exclude(
      "SPARK-40819: parquet file with TIMESTAMP(NANOS, true) (with default nanosAsLong=false)")
  enableSuite[AuronParquetTest]
  enableSuite[AuronParquetThriftCompatibilitySuite]
    .exclude("Read Parquet file generated by parquet-thrift")
  enableSuite[AuronParquetV1FilterSuite]
    .excludeByPrefix("SPARK-40280: filter pushdown -")
    .excludeByPrefix("filter pushdown -")
    .exclude("Filters should be pushed down for vectorized Parquet reader at row group level")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    .exclude("Support Parquet column index")
    .exclude("SPARK-34562: Bloom filter push down")
  enableSuite[AuronParquetV1PartitionDiscoverySuite]
    .exclude("read partitioned table - partition key included in Parquet file")
    .exclude(
      "read partitioned table - with nulls and partition keys are included in Parquet file")
    .exclude(
      "SPARK-18108 Parquet reader fails when data column types conflict with partition ones")
    .exclude(
      "SPARK-21463: MetadataLogFileIndex should respect userSpecifiedSchema for partition cols")
  enableSuite[AuronParquetV1QuerySuite]
    .exclude("SPARK-10634 timestamp written and read as INT64 - truncation")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
    .exclude("Migration from INT96 to TIMESTAMP_MICROS timestamp type")
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    .exclude("row group skipping doesn't overflow when reading into larger type")
    .exclude("returning batch for wide table")
    .exclude("SPARK-39833: pushed filters with count()")
    .exclude("SPARK-39833: pushed filters with project without filter columns")
  enableSuite[AuronParquetV1SchemaPruningSuite]
  enableSuite[AuronParquetV2FilterSuite]
    .excludeByPrefix("SPARK-40280: filter pushdown -")
    .exclude("SPARK-31026: Parquet predicate pushdown for fields having dots in the names")
    .exclude("Filters should be pushed down for Parquet readers at row group level")
    .exclude("SPARK-23852: Broken Parquet push-down for partially-written stats")
    .exclude("SPARK-17091: Convert IN predicate to Parquet filter push-down")
    .exclude("SPARK-25207: exception when duplicate fields in case-insensitive mode")
    .exclude("Support Parquet column index")
  enableSuite[AuronParquetV2PartitionDiscoverySuite]
  enableSuite[AuronParquetV2QuerySuite]
    .exclude("SPARK-10634 timestamp written and read as INT64 - truncation")
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
    .exclude("Migration from INT96 to TIMESTAMP_MICROS timestamp type")
    .exclude("returning batch for wide table")
  enableSuite[AuronParquetV2SchemaPruningSuite]
  enableSuite[AuronParquetVectorizedSuite]

  override def getSQLQueryTestSettings: SQLQueryTestSettings = new SQLQueryTestSettings {
    override def getResourceFilePath: String = ""
    override def getSupportedSQLQueryTests: Set[String] = Set.empty
    override def getOverwriteSQLQueryTests: Set[String] = Set.empty
  }
}
