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
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

class AuronCheckConvertShuffleExchangeSuite
    extends QueryTest
    with SharedSparkSession
    with AuronSQLTestHelper
    with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {

  test(
    "test set auron shuffle manager convert to native shuffle exchange where set spark.auron.enable is true") {
    withTable("test_shuffle") {
      val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("checkConvertToNativeShuffleManger")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
        .config(
          "spark.shuffle.manager",
          "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
        .config("spark.memory.offHeap.enabled", "false")
        .config("spark.auron.enable", "true")
        .getOrCreate()

      spark.sql("drop table if exists test_shuffle")
      spark.sql(
        "create table if not exists test_shuffle using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val executePlan =
        spark.sql("select c1, count(1) from test_shuffle group by c1")

      val shuffleExchangeExec =
        executePlan.queryExecution.executedPlan
          .collectFirst { case shuffleExchangeExec: ShuffleExchangeExec =>
            shuffleExchangeExec
          }
      val afterConvertPlan = AuronConverters.convertSparkPlan(shuffleExchangeExec.get)
      assert(afterConvertPlan.isInstanceOf[NativeShuffleExchangeExec])
      checkAnswer(executePlan, Seq(Row(1, 1)))
    }
  }

  test(
    "test set non auron shuffle manager do not convert to native shuffle exchange where set spark.auron.enable is true") {
    withTable("test_shuffle") {
      val spark = SparkSession
        .builder()
        .master("local[2]")
        .appName("checkConvertToNativeShuffleManger")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
        .config("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
        .config("spark.memory.offHeap.enabled", "false")
        .config("spark.auron.enable", "true")
        .getOrCreate()
      spark.sql("drop table if exists test_shuffle")
      spark.sql(
        "create table if not exists test_shuffle using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val executePlan =
        spark.sql("select c1, count(1) from test_shuffle group by c1")

      val shuffleExchangeExec =
        executePlan.queryExecution.executedPlan
          .collectFirst { case shuffleExchangeExec: ShuffleExchangeExec =>
            shuffleExchangeExec
          }
      val afterConvertPlan = AuronConverters.convertSparkPlan(shuffleExchangeExec.get)
      assert(afterConvertPlan.isInstanceOf[ShuffleExchangeExec])
      checkAnswer(executePlan, Seq(Row(1, 1)))

    }
  }

}
