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

class AuronSparkTestSettings extends SparkTestSettings {
  {
    // Use Arrow's unsafe implementation.
    System.setProperty("arrow.allocation.manager.type", "Unsafe")
  }

  enableSuite[AuronDataFrameFunctionsSuite]
    // Native execution wraps SparkRuntimeException from map construction in SparkException.
    .exclude("map with arrays")
    // Native execution wraps SparkRuntimeException from map_concat input validation in
    // SparkException.
    .exclude("map_concat function")
    // Native reverse only supports string inputs; array inputs fail with unsupported List type.
    .exclude("reverse function - array for primitive type not containing null")
    .exclude("reverse function - array for primitive type containing null")
    .exclude("reverse function - array for non-primitive type")
    // Native flatten can fail when child arrays have different containsNull metadata.
    .exclude("flatten function")
    // Native execution wraps SparkRuntimeException from null map keys in SparkException.
    .exclude("SPARK-24734: Fix containsNull of Concat for array type")

  enableSuite[AuronDateFunctionsSuite]
    // Native execution wraps SparkUpgradeException from parsing validation in SparkException.
    .exclude("function to_date")
    // Native date_trunc does not support all Spark granularity aliases.
    .exclude("function date_trunc")
    // Native date_trunc throws for unsupported fields instead of returning NULL as Spark does.
    .exclude("unsupported fmt fields for trunc/date_trunc results null")
    // Native execution wraps SparkUpgradeException from parsing validation in SparkException.
    .exclude("unix_timestamp")
    // Native execution wraps IllegalArgumentException from format validation in SparkException.
    .exclude("to_unix_timestamp")
    // Native date_trunc may produce incorrect results for historical timestamps with
    // non-UTC timezones due to timezone handling differences in the DataFusion engine.
    .exclude("SPARK-30766: date_trunc of old timestamps to hours and days")

  enableSuite[AuronMathFunctionsSuite]
    // Native acosh uses a different floating-point formula than Spark's StrictMath.log,
    // producing results that differ at the last ULP for certain edge-case inputs.
    .exclude("acosh")

  enableSuite[AuronMiscFunctionsSuite]

  enableSuite[AuronStringFunctionsSuite]
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

  enableSuite[AuronDatasetAggregatorSuite]

  enableSuite[AuronTypedImperativeAggregateSuite]

  override def getSQLQueryTestSettings: SQLQueryTestSettings = new SQLQueryTestSettings {
    override def getResourceFilePath: String = ""
    override def getSupportedSQLQueryTests: Set[String] = Set.empty
    override def getOverwriteSQLQueryTests: Set[String] = Set.empty
  }
}
