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

import org.apache.spark.sql.{AuronQueryTest, Row}
import org.apache.spark.sql.execution.auron.plan.{NativeBroadcastExchangeBase, NativeBroadcastJoinBase, NativeShuffledHashJoinBase, NativeShuffleExchangeBase}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf

class AuronReusedExchangeSuite extends AuronQueryTest with BaseAuronSQLSuite {
  test("shuffled hash join reads a reused shuffle with the consumer's exprIds") {
    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val df = spark.sql("""
            |WITH dim AS (SELECT id, id + 10 AS value FROM range(3))
            |SELECT /*+ SHUFFLE_HASH(x, y) */ p.id, x.value, y.value
            |FROM (SELECT /*+ REPARTITION(2, id) */ id FROM range(4)) p
            |LEFT JOIN dim x ON p.id = x.id
            |LEFT JOIN dim y ON p.id + 1 = y.id
            |""".stripMargin)
        withClue(s"AQE=$aqe: ") {
          checkAnswer(
            df,
            Seq(Row(0L, 10L, 11L), Row(1L, 11L, 12L), Row(2L, 12L, null), Row(3L, null, null)))
        }
        val plan = df.queryExecution.executedPlan
        assert(
          collect(plan) { case join: NativeShuffledHashJoinBase => join }.size == 2,
          s"AQE=$aqe\n${plan.treeString}")
        assert(
          find(plan) {
            case ReusedExchangeExec(output, exchange: NativeShuffleExchangeBase) =>
              output.map(_.exprId) != exchange.output.map(_.exprId)
            case _ => false
          }.isDefined,
          s"AQE=$aqe\n${plan.treeString}")
      }
    }
  }

  test("broadcast hash join reads a reused broadcast with the consumer's exprIds") {
    Seq(true, false).foreach { aqe =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString,
        SQLConf.EXCHANGE_REUSE_ENABLED.key -> "true",
        SQLConf.SHUFFLE_PARTITIONS.key -> "2") {
        val df = spark.sql("""
            |WITH dim AS (SELECT id, id + 10 AS value FROM range(3))
            |SELECT /*+ BROADCAST(x, y) */ p.id, x.value, y.value
            |FROM (SELECT /*+ REPARTITION(2, id) */ id FROM range(4)) p
            |LEFT JOIN dim x ON p.id = x.id
            |LEFT JOIN dim y ON p.id + 1 = y.id
            |""".stripMargin)
        withClue(s"AQE=$aqe: ") {
          checkAnswer(
            df,
            Seq(Row(0L, 10L, 11L), Row(1L, 11L, 12L), Row(2L, 12L, null), Row(3L, null, null)))
        }
        val plan = df.queryExecution.executedPlan
        assert(
          collect(plan) { case join: NativeBroadcastJoinBase => join }.size == 2,
          s"AQE=$aqe\n${plan.treeString}")
        assert(
          find(plan) {
            case ReusedExchangeExec(output, exchange: NativeBroadcastExchangeBase) =>
              output.map(_.exprId) != exchange.output.map(_.exprId)
            case _ => false
          }.isDefined,
          s"AQE=$aqe\n${plan.treeString}")
      }
    }
  }
}
