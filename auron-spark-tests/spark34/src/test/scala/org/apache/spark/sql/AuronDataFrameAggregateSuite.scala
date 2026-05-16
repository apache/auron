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
package org.apache.spark.sql

import scala.util.Random

import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.auron.plan.NativeAggBase
import org.apache.spark.sql.functions.{collect_list, monotonically_increasing_id, rand, randn, spark_partition_id, sum}
import org.apache.spark.sql.internal.SQLConf

class AuronDataFrameAggregateSuite extends DataFrameAggregateSuite with SparkQueryTestsBase {
  import testImplicits._

  // Ported from spark DataFrameAggregateSuite only with plan check changed.
  private def assertNoExceptions(c: Column): Unit = {
    for ((wholeStage, useObjectHashAgg) <-
        Seq((true, true), (true, false), (false, true), (false, false))) {
      withSQLConf(
        (SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, wholeStage.toString),
        (SQLConf.USE_OBJECT_HASH_AGG.key, useObjectHashAgg.toString)) {

        val df = Seq(("1", 1), ("1", 2), ("2", 3), ("2", 4)).toDF("x", "y")

        val hashAggDF = df.groupBy("x").agg(c, sum("y"))
        hashAggDF.collect()
        val hashAggPlan = hashAggDF.queryExecution.executedPlan
        if (wholeStage) {
          assert(find(hashAggPlan) {
            case WholeStageCodegenExec(_: HashAggregateExec) => true
            // If offloaded, Spark whole stage codegen takes no effect and a native hash agg is
            // expected to be used.
            case _: NativeAggBase => true
            case _ => false
          }.isDefined)
        } else {
          assert(
            stripAQEPlan(hashAggPlan).isInstanceOf[HashAggregateExec] ||
              stripAQEPlan(hashAggPlan).find {
                case _: NativeAggBase => true
                case _ => false
              }.isDefined)
        }

        val objHashAggOrSortAggDF = df.groupBy("x").agg(c, collect_list("y"))
        objHashAggOrSortAggDF.collect()
        assert(stripAQEPlan(objHashAggOrSortAggDF.queryExecution.executedPlan).find {
          case _: NativeAggBase => true
          case _ => false
        }.isDefined)
      }
    }
  }

  testAuron(
    "SPARK-19471: AggregationIterator does not initialize the generated result projection before using it") {
    Seq(
      monotonically_increasing_id(),
      spark_partition_id(),
      rand(Random.nextLong()),
      randn(Random.nextLong())).foreach(assertNoExceptions)
  }
}
