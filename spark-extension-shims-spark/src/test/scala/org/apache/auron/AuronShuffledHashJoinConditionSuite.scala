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
import org.apache.spark.sql.execution.auron.plan.NativeShuffledHashJoinBase

import org.apache.auron.util.AuronTestUtils

class AuronShuffledHashJoinConditionSuite extends AuronJoinConditionTestBase {
  private def withShuffledJoin(force: Boolean = false)(f: => Unit): Unit = {
    withSparkConf("spark.auron.forceShuffledHashJoin" -> force.toString) {
      withSQLConf(
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.shuffle.partitions" -> "2") {
        withJoinInputs(f)
      }
    }
  }

  for ((joinType, buildAlias) <- Seq(
      ("inner", "l"),
      ("inner", "r"),
      ("left outer", "r"),
      ("right outer", "l"),
      ("left semi", "r"),
      ("left anti", "r"))) {
    test(s"SHJ $joinType building $buildAlias evaluates residual conditions") {
      withShuffledJoin() {
        checkJoinPlan(
          s"""
            |SELECT /*+ SHUFFLE_HASH($buildAlias) */ *
            |FROM condition_left l $joinType JOIN condition_right r
            |ON l.k = r.k AND l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeShuffledHashJoinBase])
      }
    }
  }

  for (buildAlias <- Seq("l", "r")) {
    test(s"SHJ full outer residual condition building $buildAlias") {
      if (AuronTestUtils.isSparkV32OrGreater) {
        withShuffledJoin() {
          checkJoinPlan(
            s"""
              |SELECT /*+ SHUFFLE_HASH($buildAlias) */ *
              |FROM condition_left l FULL OUTER JOIN condition_right r
              |ON l.k = r.k AND l.v < r.v
              |""".stripMargin,
            _.isInstanceOf[NativeShuffledHashJoinBase])
        }
      }
    }
  }

  for (joinType <- Seq("left outer", "left semi", "left anti")) {
    test(s"forced SMJ to SHJ conversion preserves $joinType residual condition") {
      withShuffledJoin(force = true) {
        checkJoinPlan(
          s"""
            |SELECT /*+ MERGE(l, r) */ *
            |FROM condition_left l $joinType JOIN condition_right r
            |ON l.k = r.k AND l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeShuffledHashJoinBase])
      }
    }
  }

  test("SHJ existence condition uses columns outside its output") {
    withShuffledJoin(force = true) {
      // OR preserves an ExistenceJoin without projecting EXISTS, unsupported by Spark 3.0.
      for (predicate <- Seq("EXISTS", "NOT EXISTS")) {
        checkJoinPlan(
          s"""
            |SELECT l.k FROM condition_left l
            |WHERE l.k = 3 OR $predicate (
            |  SELECT 1 FROM condition_right r WHERE l.k = r.k AND l.v < r.v)
            |""".stripMargin,
          {
            case join: NativeShuffledHashJoinBase =>
              join.productIterator.exists(_.isInstanceOf[ExistenceJoin]) &&
              join.expressions.exists(_.isInstanceOf[LessThan])
            case _ => false
          })
      }
    }
  }

  for (force <- Seq(false, true)) {
    test(s"SHJ residual condition respects configuration with force=$force") {
      withShuffledJoin(force) {
        withSQLConf("spark.auron.enable.native.join.condition" -> "false") {
          val hint = if (force) "MERGE(l, r)" else "SHUFFLE_HASH(r)"
          checkJoinPlan(
            s"""
              |SELECT /*+ $hint */ *
              |FROM condition_left l LEFT OUTER JOIN condition_right r
              |ON l.k = r.k AND l.v < r.v
              |""".stripMargin,
            _.isInstanceOf[NativeShuffledHashJoinBase],
            native = false)
        }
      }
    }
  }
}
