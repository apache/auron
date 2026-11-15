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

import org.apache.spark.sql.auron.join.JoinBuildSides.{JoinBuildLeft, JoinBuildRight}
import org.apache.spark.sql.catalyst.plans.ExistenceJoin
import org.apache.spark.sql.execution.joins.auron.plan.NativeBroadcastJoinExec

class AuronBroadcastHashJoinConditionSuite extends AuronJoinConditionTestBase {
  private def withBroadcastJoin(f: => Unit): Unit = {
    withSQLConf("spark.sql.adaptive.enabled" -> "false", "spark.sql.shuffle.partitions" -> "2") {
      withJoinInputs(f)
    }
  }

  for ((joinType, buildAlias) <- Seq(
      ("inner", "l"),
      ("inner", "r"),
      ("left outer", "r"),
      ("right outer", "l"),
      ("left semi", "r"),
      ("left anti", "r"))) {
    test(s"BHJ $joinType building $buildAlias evaluates residual conditions") {
      withBroadcastJoin {
        val expectedSide = if (buildAlias == "l") JoinBuildLeft else JoinBuildRight
        checkJoinPlan(
          s"""
            |SELECT /*+ BROADCAST($buildAlias) */ *
            |FROM condition_left l $joinType JOIN condition_right r
            |ON l.k = r.k AND l.v < r.v
            |""".stripMargin,
          {
            case join: NativeBroadcastJoinExec =>
              join.broadcastSide == expectedSide && join.leftKeys.nonEmpty && join.condition.isDefined
            case _ => false
          })
      }
    }
  }

  test("BHJ existence condition uses columns outside its output") {
    withBroadcastJoin {
      // OR preserves an ExistenceJoin without projecting EXISTS, unsupported by Spark 3.0.
      for (predicate <- Seq("EXISTS", "NOT EXISTS")) {
        checkJoinPlan(
          s"""
            |SELECT l.k FROM condition_left l
            |WHERE l.k = 3 OR $predicate (
            |  SELECT /*+ BROADCAST(r) */ 1 FROM condition_right r WHERE l.k = r.k AND l.v < r.v)
            |""".stripMargin,
          {
            case join: NativeBroadcastJoinExec =>
              join.joinType.isInstanceOf[ExistenceJoin] && join.condition.isDefined
            case _ => false
          })
      }
    }
  }

  test("BHJ count-only projection retains the residual condition columns") {
    withBroadcastJoin {
      checkJoinPlan(
        """
          |SELECT /*+ BROADCAST(r) */ COUNT(*)
          |FROM condition_left l LEFT OUTER JOIN condition_right r
          |ON l.k = r.k AND l.v < r.v
          |""".stripMargin,
        _.isInstanceOf[NativeBroadcastJoinExec])
    }
  }

  test("BHJ outer residual condition respects the configuration switch") {
    withBroadcastJoin {
      withSQLConf("spark.auron.enable.native.join.condition" -> "false") {
        checkJoinPlan(
          """
            |SELECT /*+ BROADCAST(r) */ *
            |FROM condition_left l LEFT OUTER JOIN condition_right r
            |ON l.k = r.k AND l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeBroadcastJoinExec],
          native = false)
      }
    }
  }
}
