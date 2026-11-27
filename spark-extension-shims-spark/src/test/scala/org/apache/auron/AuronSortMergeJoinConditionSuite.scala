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

import org.apache.spark.sql.Row
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.catalyst.expressions.{And, LessThan}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.execution.auron.plan.NativeSortMergeJoinBase
import org.apache.spark.sql.execution.joins.SortMergeJoinExec

import org.apache.auron.util.AuronTestUtils

class AuronSortMergeJoinConditionSuite extends AuronJoinConditionTestBase {
  private def withSortMergeJoin(f: => Unit): Unit = withSortMergeJoinConf(withJoinInputs(f))

  private def withSortMergeJoinConf(f: => Unit): Unit = {
    withSparkConf("spark.auron.forceShuffledHashJoin" -> "false") {
      withSQLConf(
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.shuffle.partitions" -> "2") {
        f
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

  test("SMJ evaluates timestamp bounds with day-time interval literals natively") {
    if (AuronTestUtils.isSparkV32OrGreater) {
      withSortMergeJoinConf {
        withSQLConf("spark.sql.session.timeZone" -> "UTC") {
          val inputs =
            """
              |SELECT /*+ MERGE(l, r) */ l.id
              |FROM VALUES
              |  (0, 1, TIMESTAMP '2026-01-02 00:00:00'),
              |  (1, 1, TIMESTAMP '2026-01-02 00:02:00'),
              |  (2, 1, TIMESTAMP '2026-01-02 00:02:00.000001'),
              |  (3, 1, TIMESTAMP '2026-01-01 23:59:59.999999'),
              |  (4, 1, TIMESTAMP '2026-01-01 00:00:00'),
              |  (5, 1, TIMESTAMP '2025-12-31 23:59:59.999999'),
              |  (6, 1, CAST(NULL AS TIMESTAMP)),
              |  (7, 2, TIMESTAMP '2026-01-02 00:00:00'),
              |  (8, 3, TIMESTAMP '2026-01-03 00:00:00.000001')
              |AS l(id, k, ts)
              |JOIN VALUES (1, TIMESTAMP '2026-01-02 00:00:00'), (2, CAST(NULL AS TIMESTAMP)),
              |            (3, TIMESTAMP '2026-01-03 00:00:00') AS r(k, ts)
              |ON l.k = r.k AND
              |""".stripMargin
          val bounds = Seq(
            ("l.ts >= r.ts AND l.ts <= r.ts + INTERVAL '2' MINUTE", Seq(0, 1, 8)),
            ("l.ts <= r.ts AND l.ts >= r.ts - INTERVAL '1' DAY", Seq(0, 3, 4)))
          for {
            (predicate, expected) <- bounds
            enabled <- Seq(false, true)
            fallbackEnabled <- Seq("false", "true")
          } {
            withSQLConf(
              "spark.auron.enabled" -> enabled.toString,
              "spark.auron.udf.singleChildFallback.enabled" -> fallbackEnabled) {
              val df = sql(inputs + predicate)
              val plan = df.queryExecution.executedPlan
              if (enabled) {
                val join = plan
                  .collectFirst { case j: NativeSortMergeJoinBase => j }
                  .getOrElse(fail(plan.toString))
                val condition = join.expressions
                  .collectFirst { case expr: And => expr }
                  .getOrElse(fail(join.toString))
                val filter = NativeConverters.convertJoinFilter(
                  condition,
                  join.left.output,
                  join.right.output)
                assert(!filter.getExpression.toString.contains("spark_udf_wrapper_expr"), filter)
              } else {
                assert(plan.collectFirst { case j: SortMergeJoinExec => j }.isDefined, plan)
              }
              checkAnswer(df, expected.map(Row(_)))
            }
          }
        }
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
