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
import org.apache.spark.sql.functions.{collect_list, expr, monotonically_increasing_id, rand, randn, spark_partition_id, sum}
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

  testAuron("native bit_and / bit_or / bit_xor aggregate") {
    // bit_* are integral-only, skip nulls, and are order-independent
    // (associative + commutative), so the grouped result is deterministic.
    //   k=1: v = [3, 5, 1]      => bit_and=1, bit_or=7, bit_xor=7
    //   k=2: v = [12, null, 10] => bit_and=8, bit_or=14, bit_xor=6
    //   k=3: v = [null, null]   => bit_and=null, bit_or=null, bit_xor=null
    val df = Seq[(Int, Option[Int])](
      (1, Some(3)),
      (1, Some(5)),
      (1, Some(1)),
      (2, Some(12)),
      (2, None),
      (2, Some(10)),
      (3, None),
      (3, None))
      .toDF("k", "v")

    val aggDF = df
      .groupBy("k")
      .agg(
        expr("bit_and(v)").as("ba"),
        expr("bit_or(v)").as("bo"),
        expr("bit_xor(v)").as("bx"))

    checkAnswer(aggDF, Seq(Row(1, 1, 7, 7), Row(2, 8, 14, 6), Row(3, null, null, null)))

    // the aggregate must be offloaded to the native engine
    assert(getExecutedPlan(aggDF).exists {
      case _: NativeAggBase => true
      case _ => false
    })
  }
}
