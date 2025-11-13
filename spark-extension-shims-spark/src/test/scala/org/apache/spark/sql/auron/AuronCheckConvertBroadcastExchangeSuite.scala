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
package org.apache.spark.sql.auron

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

class AuronCheckConvertBroadcastExchangeSuite
    extends QueryTest
    with SharedSparkSession
    with AuronSQLTestHelper
    with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .setMaster("local[2]")
      .setAppName("checkConvertToBroadcast")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.auron.enable", "true")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()

    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table1")
    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table2")
  }

  test(
    "test bhj broadcastExchange to native where spark.auron.enable.broadcastexchange is true") {
    val df =
      spark.sql(
        "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b on a.c1 = b.c1")

    val broadcastExchangeExec = collectFirst(df.queryExecution.executedPlan) {
      case broadcastExchangeExec: BroadcastExchangeExec =>
        broadcastExchangeExec
    }

    val afterConvertPlan = AuronConverters.convertSparkPlan(broadcastExchangeExec.get)
    assert(afterConvertPlan.isInstanceOf[NativeBroadcastExchangeExec])
    checkAnswer(df, Seq(Row(1, 2)))
  }

  test(
    "test bnlj broadcastExchange to native where spark.auron.enable.broadcastexchange is true") {
    val df =
      spark.sql(
        "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b ")

    val broadcastExchangeExec = collectFirst(df.queryExecution.executedPlan) {
      case broadcastExchangeExec: BroadcastExchangeExec =>
        broadcastExchangeExec
    }

    val afterConvertPlan = AuronConverters.convertSparkPlan(broadcastExchangeExec.get)
    assert(afterConvertPlan.isInstanceOf[NativeBroadcastExchangeExec])
    checkAnswer(df, Seq(Row(1, 2)))
  }

  test(
    "test do not convert broadcastExchange to native when set spark.auron.enable.broadcastexchange is false") {
    withSQLConf("spark.auron.enable.broadcastExchange" -> "false") {
      val df =
        spark.sql(
          "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b on a.c1 = b.c1")

      val broadcastExchangeExec = collectFirst(df.queryExecution.executedPlan) {
        case broadcastExchangeExec: BroadcastExchangeExec =>
          broadcastExchangeExec
      }

      val afterConvertPlan = AuronConverters.convertSparkPlan(broadcastExchangeExec.get)
      assert(afterConvertPlan.isInstanceOf[BroadcastExchangeExec])
      checkAnswer(df, Seq(Row(1, 2)))
    }
  }

  test(
    "test bnlj broadcastExchange to native where spark.auron.enable.broadcastexchange is false") {
    withSQLConf("spark.auron.enable.broadcastExchange" -> "false") {
      val df =
        spark.sql(
          "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b ")

      val broadcastExchangeExec = collectFirst(df.queryExecution.executedPlan) {
        case broadcastExchangeExec: BroadcastExchangeExec =>
          broadcastExchangeExec
      }

      val afterConvertPlan = AuronConverters.convertSparkPlan(broadcastExchangeExec.get)
      assert(afterConvertPlan.isInstanceOf[BroadcastExchangeExec])
      checkAnswer(df, Seq(Row(1, 2)))
    }
  }
}
