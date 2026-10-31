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

import org.apache.spark.SparkConf
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.execution.auron.plan.NativeCartesianProductBase
import org.apache.spark.sql.execution.auron.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.joins.CartesianProductExec

class AuronCartesianProductSuite extends AuronJoinConditionTestBase {
  import testImplicits._

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.auron.batchSize", "2")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
      .set("spark.sql.adaptive.autoBroadcastJoinThreshold", "-1")

  private def checkCartesian(query: String): NativeCartesianProductBase = {
    val df = checkSparkAnswer(query)
    val plan = df.queryExecution.executedPlan
    val join = collectFirst(plan) { case join: NativeCartesianProductBase => join }
    assert(join.isDefined, plan.toString)
    join.get
  }

  test("Cartesian product evaluates residual conditions natively") {
    withJoinInputs {
      checkCartesian("SELECT l.k, r.k FROM condition_left l JOIN condition_right r ON l.v < r.v")
      checkCartesian(
        "SELECT l.k, r.k FROM condition_left l JOIN condition_right r ON l.v < r.v OR l.k IS NULL")
    }
  }

  for (adaptive <- Seq(false, true)) {
    test(s"Cartesian product preserves all partition pairs with AQE=$adaptive") {
      withSQLConf(
        "spark.sql.adaptive.enabled" -> adaptive.toString,
        "spark.sql.adaptive.coalescePartitions.enabled" -> "false") {
        withTempView("cartesian_left", "cartesian_right") {
          Seq(1, 1, 2, 3)
            .toDF("k")
            .repartition(2)
            .createOrReplaceTempView("cartesian_left")
          Seq(3, 4, 5)
            .toDF("v")
            .repartition(3)
            .createOrReplaceTempView("cartesian_right")
          val join = checkCartesian("SELECT * FROM cartesian_left CROSS JOIN cartesian_right")
          assert(NativeHelper.executeNative(join).getNumPartitions == 6)
          assert(join.outputOrdering.isEmpty)
          checkCartesian("""SELECT * FROM cartesian_left l CROSS JOIN cartesian_right r
              |CROSS JOIN cartesian_right s
              |""".stripMargin)
        }
      }
    }
  }

  test("Cartesian product counts exclude nulls") {
    withJoinInputs {
      checkCartesian(
        "SELECT COUNT(l.k), COUNT(r.v) FROM condition_left l CROSS JOIN condition_right r")
    }
  }

  test("Cartesian product handles empty input and nested scalar aggregates") {
    withJoinInputs {
      withTable("cartesian_empty") {
        sql("CREATE TABLE cartesian_empty(v int) USING parquet")
        for (inputs <- Seq(
            "condition_left CROSS JOIN cartesian_empty",
            "cartesian_empty CROSS JOIN condition_right")) {
          checkSparkAnswer(s"SELECT * FROM $inputs")
        }
      }
      checkCartesian("""SELECT * FROM
          |(SELECT COUNT(k) FROM condition_left) l CROSS JOIN
          |(SELECT COUNT(v) FROM condition_right) r CROSS JOIN
          |(SELECT COUNT(v) FROM condition_left) s
          |""".stripMargin)
    }
  }

  test("Cartesian product maps native Parquet scans to their source partitions") {
    withTempPath { leftPath =>
      withTempPath { rightPath =>
        spark.range(0, 4, 1, 2).write.parquet(leftPath.getCanonicalPath)
        spark.range(0, 6, 1, 3).write.parquet(rightPath.getCanonicalPath)
        withSQLConf(
          "spark.sql.files.maxPartitionBytes" -> "1024",
          "spark.sql.files.openCostInBytes" -> "1024") {
          withTempView("cartesian_parquet_left", "cartesian_parquet_right") {
            spark.read
              .parquet(leftPath.getCanonicalPath)
              .createOrReplaceTempView("cartesian_parquet_left")
            spark.read
              .parquet(rightPath.getCanonicalPath)
              .createOrReplaceTempView("cartesian_parquet_right")
            val join = checkCartesian(
              "SELECT * FROM cartesian_parquet_left CROSS JOIN cartesian_parquet_right")
            assert(join.children.map(NativeHelper.executeNative(_).getNumPartitions) == Seq(2, 3))
            assert(NativeHelper.executeNative(join).getNumPartitions == 6)
            assert(join.collect { case scan: NativeParquetScanBase => scan }.size == 2)
            checkCartesian(
              "SELECT COUNT(*) FROM cartesian_parquet_left CROSS JOIN cartesian_parquet_right")
          }
        }
      }
    }
  }

  for (setting <- Seq("cartesianProduct", "native.join.condition")) {
    test(s"Cartesian product respects spark.auron.enable.$setting") {
      withSQLConf(s"spark.auron.enable.$setting" -> "false") {
        withJoinInputs {
          checkJoinPlan(
            "SELECT l.k, r.k FROM condition_left l JOIN condition_right r ON l.v < r.v",
            _.isInstanceOf[CartesianProductExec])
        }
      }
    }
  }
}
