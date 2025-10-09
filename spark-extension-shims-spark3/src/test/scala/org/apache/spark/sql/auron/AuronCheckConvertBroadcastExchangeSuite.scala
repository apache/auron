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

import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.auron.plan.NativeBroadcastExchangeExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

class AuronCheckConvertBroadcastExchangeSuite
    extends QueryTest
    with SharedSparkSession
    with AuronSQLTestHelper
    with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {
  import testImplicits._

  test(
    "test bhj broadcastExchange to native where spark.auron.enable.broadcastexchange is true") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkConvertToBroadcast")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()

    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table1")
    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table2")
    val executePlan =
      spark.sql(
        "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b on a.c1 = b.c1")

    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val broadcastExchangeExec =
      plan.executedPlan
        .collectFirst { case broadcastExchangeExec: BroadcastExchangeExec =>
          broadcastExchangeExec
        }

    val afterConvertPlan = AuronConverters.convertSparkPlan(broadcastExchangeExec.get)
    assert(afterConvertPlan.isInstanceOf[NativeBroadcastExchangeExec])
    checkAnswer(executePlan, Seq(Row(1, 2)))
  }

  test(
    "test bnlj broadcastExchange to native where spark.auron.enable.broadcastexchange is true") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkConvertToBroadcast")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()

    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table1")
    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table2")
    val executePlan =
      spark.sql(
        "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b ")

    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val broadcastExchangeExec =
      plan.executedPlan
        .collectFirst { case broadcastExchangeExec: BroadcastExchangeExec =>
          broadcastExchangeExec
        }

    val afterConvertPlan = AuronConverters.convertSparkPlan(broadcastExchangeExec.get)
    assert(afterConvertPlan.isInstanceOf[NativeBroadcastExchangeExec])
    checkAnswer(executePlan, Seq(Row(1, 2)))
  }

  test(
    "test do not convert broadcastExchange to native when set spark.auron.enable.broadcastexchange is false") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkConvertToBroadcast")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.auron.enable.broadcastExchange", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()

    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table1")
    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table2")
    val executePlan =
      spark.sql(
        "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b on a.c1 = b.c1")

    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val broadcastExchangeExec =
      plan.executedPlan
        .collectFirst { case broadcastExchangeExec: BroadcastExchangeExec =>
          broadcastExchangeExec
        }

    val afterConvertPlan = AuronConverters.convertSparkPlan(broadcastExchangeExec.get)
    assert(afterConvertPlan.isInstanceOf[BroadcastExchangeExec])
    checkAnswer(executePlan, Seq(Row(1, 2)))
  }

  test(
    "test bnlj broadcastExchange to native where spark.auron.enable.broadcastexchange is false") {
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("checkConvertToBroadcast")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .config(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .config("spark.memory.offHeap.enabled", "false")
      .config("spark.auron.enable.broadcastExchange", "false")
      .config("spark.auron.enable", "true")
      .getOrCreate()

    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table1")
    Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("broad_cast_table2")
    val executePlan =
      spark.sql(
        "select /*+ broadcast(a)*/ a.c1, a.c2 from broad_cast_table1 a inner join broad_cast_table2 b ")

    val plan = executePlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
    val broadcastExchangeExec =
      plan.executedPlan
        .collectFirst { case broadcastExchangeExec: BroadcastExchangeExec =>
          broadcastExchangeExec
        }

    val afterConvertPlan = AuronConverters.convertSparkPlan(broadcastExchangeExec.get)
    assert(afterConvertPlan.isInstanceOf[BroadcastExchangeExec])
    checkAnswer(executePlan, Seq(Row(1, 2)))
  }

}
