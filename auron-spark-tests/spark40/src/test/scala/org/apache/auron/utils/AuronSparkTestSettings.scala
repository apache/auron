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
    .disable("Native execution can crash after ParquetQuery in Spark 4")

  enableSuite[AuronDateFunctionsSuite]
    .disable(
      "Native execution can crash in Spark 4 date/partition suites causing cascade failures")

  enableSuite[AuronMathFunctionsSuite]
    .disable("Native execution can crash in Spark 4")

  enableSuite[AuronMiscFunctionsSuite]
    .exclude("reflect and java_method")

  enableSuite[AuronStringFunctionsSuite]
    .exclude("string concat")
    .exclude("string concat_ws")
    // Spark 4 adds the threshold argument, but native levenshtein currently supports only
    // two arguments.
    .exclude("string Levenshtein distance")
    .exclude("UTF-8 string validate")
    .exclude("RegExpReplace throws the right exception when replace fails on a particular row")

  enableSuite[AuronDataFrameAggregateSuite]
    .disable("Native execution can crash in Spark 4")

  enableSuite[AuronDatasetAggregatorSuite]
    .disable("Native dataset aggregators fail in Spark 4")

  enableSuite[AuronTypedImperativeAggregateSuite]
    .disable("Native execution can crash after ParquetQuery in Spark 4")

  enableSuite[AuronDataFrameSuite]
    .disable("Native execution can crash in Spark 4")

  enableSuite[AuronParquetAvroCompatibilitySuite]
    .exclude("required primitives")
    .exclude("optional primitives")
    .exclude("non-nullable arrays")
    .exclude("SPARK-10136 array of primitive array")
    .exclude("map of primitive array")
    .exclude("various complex types")
    .exclude("SPARK-9407 Push down predicates involving Parquet ENUM columns")
  enableSuite[AuronParquetColumnIndexSuite]
    .exclude("reading from unaligned pages - test filters")
    .exclude("test reading unaligned pages - test all types (dict encode)")
    .exclude("SPARK-36123: reading from unaligned pages - test filters with nulls")
    .exclude("test reading unaligned pages - test all types")
    .exclude("reading unaligned pages - struct type")
  enableSuite[AuronParquetCompatibilityTest]
  enableSuite[AuronParquetCompressionCodecPrecedenceSuite]
  enableSuite[AuronParquetEncodingSuite]
    .disable("Native execution can crash in Spark 4")
  enableSuite[AuronParquetFieldIdIOSuite]
    .disable("Native parquet field id reads fail in Spark 4")
  enableSuite[AuronParquetFieldIdSchemaSuite]
  enableSuite[AuronParquetFileFormatSuite]
  enableSuite[AuronParquetFileFormatV1Suite]
  enableSuite[AuronParquetFileFormatV2Suite]
  enableSuite[AuronParquetIOSuite]
    .disable("Native execution can crash in Spark 4")
  enableSuite[AuronParquetInteroperabilitySuite]
    .disable("Native execution can crash in Spark 4")
  enableSuite[AuronParquetPartitionDiscoverySuite]
    .disable("Native execution can crash in Spark 4 Parquet partition discovery")
  enableSuite[AuronParquetProtobufCompatibilitySuite]
    .exclude("unannotated array of primitive type")
    .exclude("unannotated array of struct")
    .exclude("struct with unannotated array")
    .exclude("unannotated array of struct with unannotated array")
    .exclude("unannotated array of string")
  enableSuite[AuronParquetQuerySuite]
    .exclude("simple select queries")
    .exclude("appending")
    .exclude("SPARK-10634 timestamp written and read as INT64 - truncation")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
    .exclude("Migration from INT96 to TIMESTAMP_MICROS timestamp type")
    .exclude("SPARK-34212 Parquet should read decimals correctly")
  enableSuite[AuronParquetRebaseDatetimeSuite]
    .exclude(
      "SPARK-31159, SPARK-37705: compatibility with Spark 2.4/3.2 in reading dates/timestamps")
    .exclude("SPARK-31159, SPARK-37705: rebasing timestamps in write")
    .exclude("SPARK-31159: rebasing dates in write")
    .exclude("SPARK-35427: datetime rebasing in the EXCEPTION mode")
  enableSuite[AuronParquetRebaseDatetimeV1Suite]
    .disable("Spark 4 test resources use jar paths unsupported by Hadoop Path")
  enableSuite[AuronParquetRebaseDatetimeV2Suite]
    .disable("Spark 4 test resources use jar paths unsupported by Hadoop Path")
  enableSuite[AuronParquetSchemaInferenceSuite]
  enableSuite[AuronParquetSchemaPruningSuite]
    .disable("Native parquet schema pruning reads fail in Spark 4")
  enableSuite[AuronParquetSchemaSuite]
    .disable("Native execution can crash in Spark 4")
  enableSuite[AuronParquetTest]
  enableSuite[AuronParquetThriftCompatibilitySuite]
    .disable("Spark 4 test resources use jar paths unsupported by Hadoop Path")
  enableSuite[AuronParquetV1FilterSuite]
    .disable("Native execution can crash in Spark 4")
  enableSuite[AuronParquetV1PartitionDiscoverySuite]
    .disable("Native execution can crash in Spark 4 Parquet partition discovery")
  enableSuite[AuronParquetV1QuerySuite]
    .exclude("simple select queries")
    .exclude("appending")
    .exclude("SPARK-10634 timestamp written and read as INT64 - truncation")
    .exclude("Enabling/disabling ignoreCorruptFiles")
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
    .exclude("Migration from INT96 to TIMESTAMP_MICROS timestamp type")
    .exclude("SPARK-34212 Parquet should read decimals correctly")
    .exclude("returning batch for wide table")
    .exclude("SPARK-39833: pushed filters with count()")
    .exclude("SPARK-39833: pushed filters with project without filter columns")
  enableSuite[AuronParquetV1SchemaPruningSuite]
    .disable("Native parquet schema pruning reads fail in Spark 4")
  enableSuite[AuronParquetV2FilterSuite]
    .disable("Native execution can crash in Spark 4")
  enableSuite[AuronParquetV2PartitionDiscoverySuite]
    .disable("Native execution can crash in Spark 4 Parquet partition discovery")
  enableSuite[AuronParquetV2QuerySuite]
    .exclude("simple select queries")
    .exclude("appending")
    .exclude("self-join")
    .exclude("SPARK-10634 timestamp written and read as INT64 - truncation")
    .exclude(
      "SPARK-26677: negated null-safe equality comparison should not filter matched row groups")
    .exclude("Migration from INT96 to TIMESTAMP_MICROS timestamp type")
    .exclude("returning batch for wide table")
  enableSuite[AuronParquetV2SchemaPruningSuite]
    .disable("Native parquet schema pruning reads fail in Spark 4")
  enableSuite[AuronParquetVectorizedSuite]

  override def getSQLQueryTestSettings: SQLQueryTestSettings = new SQLQueryTestSettings {
    override def getResourceFilePath: String = ""
    override def getSupportedSQLQueryTests: Set[String] = Set.empty
    override def getOverwriteSQLQueryTests: Set[String] = Set.empty
  }
}
