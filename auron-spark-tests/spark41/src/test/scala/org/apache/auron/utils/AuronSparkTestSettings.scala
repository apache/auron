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

  enableSuite[AuronDataFrameAggregateSuite]
    // See https://github.com/apache/auron/issues/1840
    .excludeByPrefix("collect functions")
    // A custom version of the SPARK-19471 test has been added to AuronDataFrameAggregateSuite
    // with modified plan checks for Auron's native aggregates, so we exclude the original here.
    .exclude(
      "SPARK-19471: AggregationIterator does not initialize the generated result projection before using it")
    .exclude(
      "SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail")
    // The fast-hashmap test asserts on WholeStageCodegen output, but Auron replaces the
    // HashAggregate with a native aggregate so no Spark-generated class is emitted.
    .exclude("SPARK-43876: Enable fast hashmap for distinct queries")
    // MapType isn't a supported shuffle key in Auron's shuffle writer, so grouping by a
    // MapType column fails when writing the shuffle index file.
    .exclude("SPARK-47430 Support GROUP BY MapType")
    // Auron's native sum/avg use wrapping arithmetic on decimals and don't honor
    // spark.sql.ansi.enabled, so the ArithmeticException these tests expect is never
    // raised. Same exclusion is in place for Spark 3.3-3.5 (under AuronDataFrameSuite there).
    .exclude("SPARK-28067: Aggregate sum should not return wrong results for decimal overflow")
    .exclude("SPARK-35955: Aggregate avg should not return wrong results for decimal overflow")
    // Spark 4.1 added an allowDifferentLgConfigK=true variant on hll_union_agg. With that
    // flag set, the NativeHashAggregate placed after a Union returns the pre-Union per-side
    // rows (count=7 and count=8 for the same id) instead of collapsing into one row
    // (count=15). Even the plain sum(count) column fails to merge, so this looks like a
    // post-Union aggregate bug rather than missing HLL coverage. The same test passes on
    // Spark 3.5 and 4.0, so this is a 4.1-specific divergence; tracking as a follow-up.
    .exclude("SPARK-16484: hll_*_agg + hll_union + hll_sketch_estimate positive tests")
    .exclude("hll_union_agg")
    // Spark 4.1 introduced theta_sketch_*; Auron has no native UDAF support and
    // SparkUDAFWrapper doesn't recognise the new function family yet.
    .excludeByPrefix("SPARK-52407")
    // Spark 4.1 introduced the TIME type; not in NativeConverters.isTypeSupported.
    .exclude("SPARK-52626: Support group by Time column")
    .exclude("SPARK-52660: Support aggregation of Time column when codegen is split")

  enableSuite[AuronDatasetAggregatorSuite]
    // Dataset encoder fails to materialize tuple-of-struct results that round-trip through
    // Auron's native execution; tracked as a follow-up.
    .exclude("typed aggregation: complex result type")

  enableSuite[AuronTypedImperativeAggregateSuite]
    // ObjectHashAggregateExec without a grouping falls back to a JVM path whose closure
    // can't be deserialized in the executor classloader (RemoteClassLoaderError on
    // catalyst.expressions.Object). Investigating this fallback path is out of scope for #2170.
    .exclude("dataframe aggregate with object aggregate buffer, no group by")

  override def getSQLQueryTestSettings: SQLQueryTestSettings = new SQLQueryTestSettings {
    override def getResourceFilePath: String = ""
    override def getSupportedSQLQueryTests: Set[String] = Set.empty
    override def getOverwriteSQLQueryTests: Set[String] = Set.empty
  }
}
