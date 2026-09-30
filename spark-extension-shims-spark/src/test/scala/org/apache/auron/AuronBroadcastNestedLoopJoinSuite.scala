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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.auron.join.JoinBuildSides.{JoinBuildLeft, JoinBuildRight}
import org.apache.spark.sql.catalyst.expressions.{Ascending, SortOrder}
import org.apache.spark.sql.execution.SortExec
import org.apache.spark.sql.execution.joins.BroadcastNestedLoopJoinExec
import org.apache.spark.sql.execution.joins.auron.plan.NativeBroadcastJoinExec

import org.apache.auron.util.AuronTestUtils

class AuronBroadcastNestedLoopJoinSuite extends AuronJoinConditionTestBase {
  import testImplicits._

  for ((joinType, buildAlias, native) <- Seq(
      ("inner", "l", true),
      ("inner", "r", true),
      ("left outer", "r", true),
      ("right outer", "l", true),
      ("left semi", "r", true),
      ("left anti", "r", true),
      ("left outer", "l", false),
      ("right outer", "r", false),
      ("left semi", "l", false),
      ("left anti", "l", false),
      ("full outer", "l", false),
      ("full outer", "r", false))) {
    test(s"conditional BNLJ $joinType building $buildAlias with multiple probe partitions") {
      withSQLConf(
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.shuffle.partitions" -> "2",
        // Keep the explicit probe repartition below semi/anti joins.
        "spark.sql.optimizer.excludedRules" ->
          "org.apache.spark.sql.catalyst.optimizer.PushDownLeftSemiAntiJoin",
        "spark.auron.enable.bnlj" -> "true",
        "spark.auron.enable.broadcastExchange" -> "true") {
        withTempView("bnlj_left", "bnlj_right") {
          spark.sparkContext
            .parallelize(Seq[Integer](null, 0, 1, 3), 2)
            .toDF("k")
            .repartition(2)
            .createOrReplaceTempView("bnlj_left")
          spark.sparkContext
            .parallelize(Seq[Integer](null, 1, 2), 2)
            .toDF("k")
            .repartition(2)
            .createOrReplaceTempView("bnlj_right")

          val df = checkSparkAnswer(s"""
              |SELECT /*+ BROADCAST($buildAlias) */ *
              |FROM bnlj_left l $joinType JOIN bnlj_right r ON l.k < r.k
              |""".stripMargin)
          val plan = df.queryExecution.executedPlan
          val nativeJoin = collectFirst(plan) { case join: NativeBroadcastJoinExec => join }
          val sparkJoin = collectFirst(plan) { case join: BroadcastNestedLoopJoinExec => join }
          val expectedBuildSide = if (buildAlias == "l") JoinBuildLeft else JoinBuildRight
          if (native) {
            assert(nativeJoin.isDefined, plan.toString)
            assert(nativeJoin.get.leftKeys.isEmpty && nativeJoin.get.rightKeys.isEmpty)
            assert(nativeJoin.get.condition.isDefined)
            assert(nativeJoin.get.broadcastSide == expectedBuildSide)
            val probe = if (buildAlias == "l") nativeJoin.get.right else nativeJoin.get.left
            assert(NativeHelper.executeNative(probe).getNumPartitions == 2)
            val orderedProbe =
              SortExec(Seq(SortOrder(probe.output.head, Ascending)), global = false, probe)
            val orderedJoin = if (buildAlias == "l") {
              nativeJoin.get.copy(right = orderedProbe)
            } else {
              nativeJoin.get.copy(left = orderedProbe)
            }
            assert(orderedProbe.outputOrdering.nonEmpty)
            assert(orderedJoin.outputOrdering.isEmpty)
            assert(sparkJoin.isEmpty, plan.toString)
          } else {
            assert(nativeJoin.isEmpty, plan.toString)
            assert(sparkJoin.isDefined, plan.toString)
            assert(Shims.get.getJoinBuildSide(sparkJoin.get) == expectedBuildSide)
          }
        }
      }
    }
  }

  test("BNLJ count-only projection retains condition columns") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withJoinInputs {
        checkJoinPlan(
          """
            |SELECT /*+ BROADCAST(r) */ COUNT(*)
            |FROM condition_left l LEFT OUTER JOIN condition_right r ON l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeBroadcastJoinExec])
      }
    }
  }

  test("BNLJ residual condition respects the configuration switch") {
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.auron.enable.native.join.condition" -> "false") {
      withJoinInputs {
        checkJoinPlan(
          """
            |SELECT /*+ BROADCAST(r) */ *
            |FROM condition_left l LEFT OUTER JOIN condition_right r ON l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeBroadcastJoinExec],
          native = false)
      }
    }
  }

  for ((joinType, buildAlias) <- Seq(("inner", "l"), ("left outer", "r"))) {
    test(s"BNLJ $joinType building $buildAlias preserves residuals in forced SMJ fallback") {
      withSparkConf(
        "spark.auron.smjfallback.enable" -> "true",
        "spark.auron.smjfallback.rows.threshold" -> "0") {
        withSQLConf(
          "spark.sql.adaptive.enabled" -> "false",
          "spark.sql.shuffle.partitions" -> "2") {
          withJoinInputs {
            val df = checkSparkAnswer(s"""
                |SELECT /*+ BROADCAST($buildAlias) */ *
                |FROM condition_left l $joinType JOIN condition_right r ON l.v < r.v
                |""".stripMargin)
            val plan = df.queryExecution.executedPlan
            val join = collectFirst(plan) { case native: NativeBroadcastJoinExec => native }
            assert(join.isDefined, plan.toString)
            assert(join.get.leftKeys.isEmpty && join.get.rightKeys.isEmpty)
            assert(join.get.metrics("fallback_sort_merge_join_time").value > 0)
            assert(join.get.outputOrdering.isEmpty)
          }
        }
      }
    }
  }

  test("broadcast hash join ordering accounts for enabled SMJ fallback") {
    withSQLConf("spark.sql.adaptive.enabled" -> "false") {
      withTempView("ordering_left", "ordering_right") {
        Seq((1, 0), (1, 1))
          .toDF("k", "v")
          .repartition(2)
          .createOrReplaceTempView("ordering_left")
        Seq((1, 0))
          .toDF("k", "v")
          .repartition(2)
          .createOrReplaceTempView("ordering_right")
        val df = sql("""
            |SELECT /*+ BROADCAST(r) */ *
            |FROM ordering_left l LEFT OUTER JOIN ordering_right r
            |ON l.k = r.k AND l.v > r.v
            |""".stripMargin)
        val plan = df.queryExecution.executedPlan
        val join = collectFirst(plan) { case native: NativeBroadcastJoinExec => native }
        assert(join.isDefined, plan.toString)
        assert(join.get.leftKeys.nonEmpty)
        val orderedProbe = SortExec(
          Seq(SortOrder(join.get.left.output.last, Ascending)),
          global = false,
          join.get.left)
        val orderedJoin = join.get.copy(left = orderedProbe)
        val conf = SparkEnv.get.conf
        val key = "spark.auron.smjfallback.enable"
        val previous = conf.getOption(key)
        try {
          conf.set(key, "false")
          // Spark 3.0 HashJoin does not expose probe-side ordering.
          val expectedOrdering =
            if (AuronTestUtils.isSparkV31OrGreater) orderedProbe.outputOrdering else Nil
          assert(orderedJoin.outputOrdering == expectedOrdering)
          conf.set(key, "true")
          assert(orderedJoin.outputOrdering.isEmpty)
        } finally {
          previous match {
            case Some(value) => conf.set(key, value)
            case None => conf.remove(key)
          }
        }
      }
    }
  }
}
