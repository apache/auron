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
import org.apache.spark.sql.functions.{collect_list, last, monotonically_increasing_id, rand, randn, spark_partition_id, sum}
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

  testAuron("native last / last(ignoreNulls) aggregate") {
    // The grouped aggregate is reliably offloaded to NativeAggBase, and the data
    // is deterministic by construction (no intra-group ordering dependence):
    //   k=1 -> all values 10   => last=10,   last(ignoreNulls)=10
    //   k=2 -> all values null => last=null, last(ignoreNulls)=null
    //   k=3 -> single row 30   => last=30,   last(ignoreNulls)=30
    val df = Seq[(Int, Option[Int])](
      (1, Some(10)),
      (1, Some(10)),
      (2, None),
      (2, None),
      (3, Some(30)))
      .toDF("k", "v")

    val aggDF = df
      .groupBy("k")
      .agg(last($"v").as("last_v"), last($"v", ignoreNulls = true).as("last_v_ign"))

    checkAnswer(aggDF, Seq(Row(1, 10, 10), Row(2, null, null), Row(3, 30, 30)))

    // the aggregate must be offloaded to the native engine
    assert(getExecutedPlan(aggDF).exists {
      case _: NativeAggBase => true
      case _ => false
    })
  }
}
