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

import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec

import org.apache.auron.util.AuronTestUtils

class AuronFloatSortSuite extends AuronQueryTest with BaseAuronSQLSuite with AuronSQLTestHelper {
  import testImplicits._

  // Rows are (id, k, d, neg). Negating NaN at query time produces a sign-bit NaN, as runtime
  // arithmetic such as sqrt(-1) does on x86. Parquet stores NaN in canonical form.
  private val fileA = Seq(
    (0, 0, Some(Double.NaN), true),
    (1, 1, Some(Double.NegativeInfinity), false),
    (2, 0, Some(-1.0), false),
    (3, 1, Some(Double.NaN), false),
    (4, 0, Some(0.0), true))
  private val fileB = Seq(
    (5, 1, Some(1.0), false),
    (6, 0, Some(Double.PositiveInfinity), false),
    (7, 1, None, false),
    (8, 1, Some(0.0), false))

  private def withFloatView(
      files: Seq[(Int, Int, Option[Double], Boolean)]*)(f: => Unit): Unit = {
    withTable("t1") {
      files.foreach { rows =>
        rows.toDF("id", "k", "d", "neg").coalesce(1).write.mode("append").saveAsTable("t1")
      }
      sql("create or replace temp view v1 as select id, k, if(neg, -d, d) as d from t1")
      f
    }
  }

  test("sort NaN and signed zero like Spark") {
    withFloatView(fileA, fileB) {
      withSQLConf("spark.auron.enable" -> "false") {
        val d = sql("select d from v1 where id = 0").head().getDouble(0)
        assert(java.lang.Double.doubleToRawLongBits(d) == 0xfff8000000000000L)
      }
      checkSparkAnswerAndOperator("select id, d from v1 order by d, id")
      checkSparkAnswerAndOperator("select id, d from v1 order by d desc nulls last, id")
    }
  }

  test("range-partitioned sort of NaN and signed zero like Spark") {
    val values = (10 until 210).map(i => (i, i % 2, Some(i - 110.0), false))
    withSQLConf("spark.sql.shuffle.partitions" -> "4", "spark.sql.adaptive.enabled" -> "false") {
      withFloatView(fileA ++ fileB ++ values) {
        val df = checkSparkAnswerAndOperator("select id, d from v1 order by d, id")
        assert(collect(df.queryExecution.executedPlan) { case e: NativeShuffleExchangeExec =>
          e
        }.nonEmpty)
      }
    }
  }

  test("top-k of NaN and signed zero across partitions like Spark") {
    withFloatView(fileA, fileB) {
      withSQLConf("spark.auron.enable" -> "false") {
        assert(spark.table("t1").rdd.getNumPartitions == 2)
      }
      // A root limit merges partial results on the driver, and a nested limit merges natively.
      checkSparkAnswerAndOperator("select id, d from v1 order by d, id limit 3")
      checkSparkAnswerAndOperator("select id, d from v1 order by d desc nulls last, id limit 3")
      checkSparkAnswerAndOperator(
        "select * from (select id, d from v1 order by d, id limit 3) where id >= 0")
      checkSparkAnswerAndOperator(
        "select * from (select id, d from v1 order by d desc nulls last, id limit 3) " +
          "where id >= 0")
    }
  }

  test("rank NaN and signed zero peers like Spark") {
    withFloatView(fileA, fileB) {
      checkSparkAnswerAndOperator(
        "select id, d, rank() over (order by d) as r, dense_rank() over (order by d) as dr " +
          "from v1")
    }
  }

  test("partitioned row_number of NaN and signed zero like Spark") {
    withFloatView(fileA, fileB) {
      val df = checkSparkAnswerAndOperator("""select * from (
          |  select id, k, d, row_number() over (
          |    partition by k order by d desc nulls last, id) as rn
          |  from v1
          |) where rn <= 2""".stripMargin)
      if (AuronTestUtils.isSparkV35OrGreater) {
        assert(collect(df.queryExecution.executedPlan) {
          case e if e.nodeName == "NativeWindowGroupLimit" => e
        }.nonEmpty)
      }
    }
  }

  test("sort-merge join on NaN and signed zero keys like Spark") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withFloatView(fileA, fileB) {
        checkSparkAnswerAndOperator(
          "select a.id, b.id from v1 a join v1 b on a.d = b.d where a.d is not null")
      }
    }
  }
}
