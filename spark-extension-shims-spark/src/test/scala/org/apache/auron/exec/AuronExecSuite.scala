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
package org.apache.auron.exec

import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.execution.auron.plan.{NativeCollectLimitExec, NativeGlobalLimitExec, NativeLocalLimitExec, NativeTakeOrderedExec}

import org.apache.auron.BaseAuronSQLSuite
import org.apache.auron.util.AuronTestUtils

class AuronExecSuite extends AuronQueryTest with BaseAuronSQLSuite {

  test("CollectLimit") {
    withTable("t1") {
      sql("create table t1(id INT) using parquet")
      sql("insert into t1 values(1),(2),(3),(3),(3),(4),(5),(6),(7),(8),(9),(10)")
      Seq(1, 3, 8, 12, 20).foreach { limit =>
        val df = checkSparkAnswerAndOperator(s"SELECT id FROM t1 limit $limit")
        assert(collectFirst(df.queryExecution.executedPlan) { case e: NativeCollectLimitExec =>
          e
        }.isDefined)
      }
    }
  }

  test("CollectLimit with offset") {
    if (AuronTestUtils.isSparkV34OrGreater) {
      withTempView("t1") {
        sql("create table if not exists t1(id INT) using parquet")
        sql("insert into t1 values(1),(2),(3),(3),(3),(4),(5),(6),(7),(8),(9),(10)")
        Seq((5, 0), (10, 0), (5, 2), (10, 2), (1, 5), (3, 8)).foreach {
          case (limit, offset) => {
            val query = s"select * from t1 limit $limit offset $offset"
            val df = checkSparkAnswerAndOperator(() => spark.sql(query))
            assert(collect(df.queryExecution.executedPlan) { case e: NativeCollectLimitExec =>
              e
            }.size == 1)
          }
        }
      }
    }
  }

  test("GlobalLimit and LocalLimit") {
    withTempView("t1") {
      sql("create table if not exists t1(id INT) using parquet")
      sql("insert into t1 values(1),(2),(3),(3),(3),(4),(5),(6),(7),(8),(9),(10)")
      val df = checkSparkAnswerAndOperator(() =>
        spark
          .sql(s"""
               |select id from (
               |  select * from t1 limit 5
               |) where id > 0 limit 10;
               |""".stripMargin)
          .groupBy("id")
          .count())
      assert(collect(df.queryExecution.executedPlan) {
        case e: NativeGlobalLimitExec => e
        case e: NativeLocalLimitExec => e
      }.size >= 2)
    }
  }

  test("GlobalLimit with offset") {
    if (AuronTestUtils.isSparkV34OrGreater) {
      withTempView("t1") {
        sql("create table if not exists t1(id INT) using parquet")
        sql("insert into t1 values(1),(2),(3),(3),(3),(4),(5),(6),(7),(8),(9),(10)")
        Seq((5, 0), (10, 0), (5, 2), (10, 2), (1, 5), (3, 8)).foreach {
          case (limit, offset) => {
            val query = s"select * from t1 limit $limit offset $offset"
            val df = checkSparkAnswerAndOperator(() => spark.sql(query).groupBy("id").count())
            assert(collect(df.queryExecution.executedPlan) { case e: NativeGlobalLimitExec =>
              e
            }.size == 1)
          }
        }
      }
    }
  }

  test("TakeOrderedAndProject") {
    withTempView("t1") {
      sql("create table if not exists t1(id INT) using parquet")
      sql("insert into t1 values(1),(2),(3),(3),(3),(4),(5),(6),(7),(8),(9),(10)")
      val df = checkSparkAnswerAndOperator(() =>
        spark
          .sql(s"""
               | select id from t1 order by id limit 5
               |""".stripMargin)
          .groupBy("id")
          .count())
      assert(collect(df.queryExecution.executedPlan) { case e: NativeTakeOrderedExec =>
        e
      }.size == 1)
    }
  }

  test("TakeOrderedAndProject with offset") {
    if (AuronTestUtils.isSparkV34OrGreater) {
      withTempView("t1") {
        sql("create table if not exists t1(id INT) using parquet")
        sql("insert into t1 values(1),(2),(3),(3),(3),(4),(5),(6),(7),(8),(9),(10)")
        Seq((5, 0), (10, 0), (5, 2), (10, 2), (1, 5), (3, 8)).foreach {
          case (limit, offset) => {
            val query = s"select * from t1 order by id limit $limit offset $offset"
            val df = checkSparkAnswerAndOperator(() => spark.sql(query).groupBy("id").count())
            assert(collect(df.queryExecution.executedPlan) { case e: NativeTakeOrderedExec =>
              e
            }.size == 1)
          }
        }
      }
    }
  }
}
