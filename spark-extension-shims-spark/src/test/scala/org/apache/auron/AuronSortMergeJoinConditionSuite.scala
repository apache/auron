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
package org.apache.auron

import org.apache.spark.sql.catalyst.expressions.LessThan
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.execution.auron.plan.NativeSortMergeJoinBase

class AuronSortMergeJoinConditionSuite extends AuronJoinConditionTestBase {
  private def withSortMergeJoin(f: => Unit): Unit = {
    withSparkConf("spark.auron.forceShuffledHashJoin" -> "false") {
      withSQLConf(
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.shuffle.partitions" -> "2") {
        withJoinInputs(f)
      }
    }
  }

  for (joinType <- Seq(
      "inner",
      "left outer",
      "right outer",
      "full outer",
      "left semi",
      "left anti")) {
    test(s"SMJ $joinType evaluates residual conditions before marking matches") {
      withSortMergeJoin {
        checkJoinPlan(
          s"""
            |SELECT /*+ MERGE(l, r) */ *
            |FROM condition_left l $joinType JOIN condition_right r
            |ON l.k = r.k AND l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeSortMergeJoinBase])
      }
    }
  }

  test("SMJ existence condition and condition-only columns") {
    withSortMergeJoin {
      // OR preserves an ExistenceJoin without projecting EXISTS, unsupported by Spark 3.0.
      for (predicate <- Seq("EXISTS", "NOT EXISTS")) {
        checkJoinPlan(
          s"""
            |SELECT l.k FROM condition_left l
            |WHERE l.k = 3 OR $predicate (
            |  SELECT 1 FROM condition_right r WHERE l.k = r.k AND l.v < r.v)
            |""".stripMargin,
          {
            case join: NativeSortMergeJoinBase =>
              join.productIterator.exists(_.isInstanceOf[ExistenceJoin]) &&
              join.expressions.exists(_.isInstanceOf[LessThan])
            case _ => false
          })
      }
    }
  }

  test("SMJ outer residual condition respects the configuration switch") {
    withSortMergeJoin {
      withSQLConf("spark.auron.enable.native.join.condition" -> "false") {
        checkJoinPlan(
          """
            |SELECT /*+ MERGE(l, r) */ *
            |FROM condition_left l LEFT OUTER JOIN condition_right r
            |ON l.k = r.k AND l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeSortMergeJoinBase],
          native = false)
      }
    }
  }
}
