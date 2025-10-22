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

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.test.SharedSparkSession

class AuronCheckConvertShuffleExchangeSuite
    extends QueryTest
    with SharedSparkSession
    with AuronSQLTestHelper
    with org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper {
  import testImplicits._

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

      Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("test_shuffle")
      val executePlan =
        spark
          .sql("select c1, count(1) from test_shuffle group by c1")
          .queryExecution
          .executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec]

      val shuffleExchangeExec =
        executePlan.executedPlan
          .collectFirst { case shuffleExchangeExec: ShuffleExchangeExec =>
            shuffleExchangeExec
          }
      val afterConvertPlan =
        AuronColumnarOverrides.apply(spark).preColumnarTransitions(shuffleExchangeExec.get)
      assert(afterConvertPlan.isInstanceOf[NativeShuffleExchangeExec])
    }
  }

  test("test set non auron shuffle manager and spark.auron.enable is true") {
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
      Seq((1, 2, "test test")).toDF("c1", "c2", "part").createOrReplaceTempView("test_shuffle")
      val executePlan =
        spark
          .sql("select c1, count(1) from test_shuffle group by c1")
          .queryExecution
          .executedPlan
          .asInstanceOf[AdaptiveSparkPlanExec]

      val shuffleExchangeExec =
        executePlan.executedPlan
          .collectFirst { case shuffleExchangeExec: ShuffleExchangeExec =>
            shuffleExchangeExec
          }

      var checkAssertError = false
      try {
        AuronColumnarOverrides.apply(spark).preColumnarTransitions(shuffleExchangeExec.get)
      } catch {
        case e: AssertionError =>
          assert(!e.getMessage.equals(
            "When spark.auron.enable.shuffleExchange is true, please set the shuffle manager to the Auron implementation — either AuronShuffleManager, AuronUniffleShuffleManager, or AuronCelebornShuffleManager" +
              ". Eg: spark.shuffle.manager=org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager"))
          checkAssertError = true
        case _ =>
          assert(
            false,
            "There is no error message for checking the shuffle manager exception; it needs to be validated.")
      }

      assert(
        checkAssertError,
        "There is no exception message, and this scenario does not meet expectations.")
    }
  }

}
