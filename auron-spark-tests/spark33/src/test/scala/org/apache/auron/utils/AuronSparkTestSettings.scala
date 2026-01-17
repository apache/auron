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

  // Will be implemented in the future.
  override def getSQLQueryTestSettings = new SQLQueryTestSettings {
    override def getResourceFilePath: String = ???

    override def getSupportedSQLQueryTests: Set[String] = ???

    override def getOverwriteSQLQueryTests: Set[String] = ???
  }
}
