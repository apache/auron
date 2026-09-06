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
import org.apache.spark.sql.{AuronQueryTest, Row}
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec

class AuronCheckConvertBroadcastExchangeSuite extends AuronQueryTest with BaseAuronSQLSuite {
  import testImplicits._

  test("do not serialize the broadcast relation with Spark tasks") {
    withSQLConf(
      "spark.auron.enable.broadcastExchange" -> "true",
      "spark.auron.enable.bhj" -> "false") {
      val payload = "x" * 4096
      (0 until 256)
        .map(i => (i, s"$i$payload"))
        .toDF("key", "payload")
        .createOrReplaceTempView("broad_cast_table1")
      Seq(0, 255).toDF("key").createOrReplaceTempView("broad_cast_table2")

      val df = spark.sql(
        "select /*+ broadcast(a)*/ b.key, a.payload from broad_cast_table1 a " +
          "inner join broad_cast_table2 b on a.key = b.key")

      checkAnswer(df, Seq(Row(0, s"0$payload"), Row(255, s"255$payload")))
      val exchange = collectFirst(df.queryExecution.executedPlan) {
        case broadcastExchangeExec: NativeBroadcastExchangeExec => broadcastExchangeExec
      }.get
      val broadcast = exchange.executeBroadcast[Any]()
      try {
        assert(broadcast eq exchange.executeBroadcast[Any]())
        val serialized = SparkEnv.get.closureSerializer.newInstance().serialize(broadcast)
        assert(serialized.remaining() < 64 * 1024)
      } finally {
        broadcast.destroy()
      }
    }
  }

  test(
    "test bhj broadcastExchange to native where spark.auron.enable.broadcastExchange is true") {
    withSQLConf("spark.auron.enable.broadcastExchange" -> "true") {
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("broad_cast_table1")
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("broad_cast_table2")
      val df =
        spark.sql(
          "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b on a.c1 = b.c1")

      checkAnswer(df, Seq(Row(1, 2)))
      assert(collectFirst(df.queryExecution.executedPlan) {
        case broadcastExchangeExec: NativeBroadcastExchangeExec =>
          broadcastExchangeExec
      }.isDefined)
    }
  }

  test(
    "test bnlj broadcastExchange to native where spark.auron.enable.broadcastExchange is true") {
    withSQLConf("spark.auron.enable.broadcastExchange" -> "true") {
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("broad_cast_table1")
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("broad_cast_table2")
      val df =
        spark.sql(
          "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b ")

      checkAnswer(df, Seq(Row(1, 2)))
      assert(collectFirst(df.queryExecution.executedPlan) {
        case broadcastExchangeExec: NativeBroadcastExchangeExec =>
          broadcastExchangeExec
      }.isDefined)
    }
  }

  test(
    "test do not convert broadcastExchange to native when set spark.auron.enable.broadcastExchange is false") {
    withSQLConf("spark.auron.enable.broadcastExchange" -> "false") {
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("broad_cast_table1")
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("broad_cast_table2")
      val df =
        spark.sql(
          "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b on a.c1 = b.c1")

      checkAnswer(df, Seq(Row(1, 2)))
      val plan = df.queryExecution.executedPlan
      assert(collectFirst(plan) { case broadcastExchangeExec: NativeBroadcastExchangeExec =>
        broadcastExchangeExec
      }.isEmpty)
      assert(collectFirst(plan) { case broadcastExchangeExec: BroadcastExchangeExec =>
        broadcastExchangeExec
      }.isDefined)
    }
  }

  test(
    "test bnlj broadcastExchange to native where spark.auron.enable.broadcastExchange is false") {
    withSQLConf("spark.auron.enable.broadcastExchange" -> "false") {
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("broad_cast_table1")
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
        .createOrReplaceTempView("broad_cast_table2")
      val df =
        spark.sql(
          "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b ")

      checkAnswer(df, Seq(Row(1, 2)))
      val plan = df.queryExecution.executedPlan
      assert(collectFirst(plan) { case broadcastExchangeExec: NativeBroadcastExchangeExec =>
        broadcastExchangeExec
      }.isEmpty)
      assert(collectFirst(plan) { case broadcastExchangeExec: BroadcastExchangeExec =>
        broadcastExchangeExec
      }.isDefined)
    }
  }
}
