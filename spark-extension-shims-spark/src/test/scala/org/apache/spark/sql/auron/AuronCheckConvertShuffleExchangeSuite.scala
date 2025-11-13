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
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

class AuronCheckConvertShuffleExchangeSuite
    extends QueryTest
    with SharedSparkSession
    with AuronSQLTestHelper
    with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .setMaster("local[2]")
      .setAppName("checkConvertToNativeShuffleManger")
      .set("spark.sql.shuffle.partitions", "4")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.codegen.wholeStage", "false")
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.auron.enable", "true")
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
  }

  test(
    "test shuffleManager convert to native where set spark.auron.enable.shuffleExchange is true") {
    withSQLConf("spark.auron.enable.shuffleExchange" -> "true") {
      spark.sql("drop table if exists test_shuffle")
      spark.sql(
        "create table if not exists test_shuffle using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val df =
        spark.sql("select c1, count(1) from test_shuffle group by c1")

      checkAnswer(df, Seq(Row(1, 1)))
      assert(collect(df.queryExecution.executedPlan) { case e: NativeShuffleExchangeExec =>
        e
      }.size == 1)
    }
  }

  test(
    "test shuffleManager convert to native where set spark.auron.enable.shuffleExchange is false") {
    withSQLConf("spark.auron.enable.shuffleExchange" -> "false") {
      spark.sql("drop table if exists test_shuffle")
      spark.sql(
        "create table if not exists test_shuffle using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val df =
        spark.sql(
          "select a.c1, b.c2 from test_shuffle a inner join test_shuffle b on a.c1 = b.c1")

      checkAnswer(df, Seq(Row(1, 2)))
      assert(collect(df.queryExecution.executedPlan) { case e: NativeShuffleExchangeExec =>
        e
      }.isEmpty)
      assert(collect(df.queryExecution.executedPlan) { case e: ShuffleExchangeExec =>
        e
      }.size == 2)
    }
  }
}
