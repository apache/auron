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
import org.apache.spark.sql.auron.Shims

class AuronExpressionSuite extends AuronQueryTest with BaseAuronSQLSuite {

  test("EqualNullSafe") {
    withTable("t1") {
      sql("CREATE TABLE t1(id INT, flag BOOLEAN) USING parquet")
      sql("INSERT INTO t1 VALUES (1, true), (2, false), (3, null), (null, true)")

      checkSparkAnswerAndOperator(
        "SELECT id <=> 1, id <=> null, flag <=> true, flag <=> null FROM t1 WHERE flag <=> true")
      checkSparkAnswerAndOperator(
        "SELECT id <=> 2, id <=> null, flag <=> false, flag <=> null FROM t1 WHERE NOT flag <=> true")
    }
  }

  test("UnaryMinus") {
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      withTable("t1") {
        sql("create table t1(col1 int) using parquet")
        sql("""
            |insert into t1 values
            |  (1),
            |  (0),
            |  (-2147483648)
            |""".stripMargin)

        withSQLConf("spark.auron.enable" -> "false") {
          assertArithmeticOverflow(sql("SELECT negative(col1), -(col1) FROM t1"), "overflow")
        }
        withSQLConf("spark.auron.enable" -> "true") {
          val df = sql("SELECT negative(col1), -(col1) FROM t1")
          assertArithmeticOverflow(df, "[ARITHMETIC_OVERFLOW]")
          assertNativePlan(df)
        }
      }
    }
  }

  test("UnaryMinusLong") {
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      withTable("t1") {
        sql("create table t1(col1 bigint) using parquet")
        sql("""
            |insert into t1 values
            |  (1),
            |  (0),
            |  (cast(-9223372036854775808 as bigint))
            |""".stripMargin)

        withSQLConf("spark.auron.enable" -> "false") {
          assertArithmeticOverflow(sql("SELECT negative(col1), -(col1) FROM t1"), "overflow")
        }
        withSQLConf("spark.auron.enable" -> "true") {
          val df = sql("SELECT negative(col1), -(col1) FROM t1")
          assertArithmeticOverflow(df, "[ARITHMETIC_OVERFLOW]")
          assertNativePlan(df)
        }
      }
    }
  }

  test("UnaryMinus without ANSI") {
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      withTable("t1") {
        sql("create table t1(col1 int) using parquet")
        sql(
          "insert into t1 values(1), (2), (3), (3), (-1), (0), (null), (2147483647), (-2147483648)")
        checkSparkAnswerAndOperator("SELECT negative(col1), -(col1) FROM t1")
      }
    }
  }

  test("UnaryMinus honors Spark's default ANSI setting") {
    withTable("t1") {
      sql("create table t1(col1 int) using parquet")
      sql("insert into t1 values(-2147483648)")

      if (spark.conf.get("spark.sql.ansi.enabled").toBoolean) {
        val df = sql("SELECT negative(col1), -(col1) FROM t1")
        assertArithmeticOverflow(df, "[ARITHMETIC_OVERFLOW]")
        assertNativePlan(df)
      } else {
        checkSparkAnswerAndOperator("SELECT negative(col1), -(col1) FROM t1")
      }
    }
  }

  if (Shims.get.shimVersion != "spark-3.0") {
    test("UnaryMinus preserves analyzed ANSI behavior") {
      withSQLConf("spark.sql.ansi.enabled" -> "false") {
        withTable("t1") {
          sql("create table t1(col1 int) using parquet")
          sql("insert into t1 values(-2147483648)")

          spark.conf.set("spark.sql.ansi.enabled", "true")
          val df =
            try {
              val query = sql("SELECT negative(col1), -(col1) FROM t1")
              query.queryExecution.analyzed
              query
            } finally {
              spark.conf.set("spark.sql.ansi.enabled", "false")
            }

          assertArithmeticOverflow(df, "[ARITHMETIC_OVERFLOW]")
          assertNativePlan(df)
        }
      }
    }
  }

  private def assertArithmeticOverflow(
      df: => org.apache.spark.sql.DataFrame,
      expectedMessage: String): Unit = {
    val err = intercept[Exception] {
      df.collect()
    }
    assert(allCauseMessages(err).toLowerCase.contains(expectedMessage.toLowerCase))
  }

  private def allCauseMessages(err: Throwable): String = {
    val messages = scala.collection.mutable.ArrayBuffer.empty[String]
    var current = err
    while (current != null) {
      Option(current.getMessage).foreach(messages += _)
      current = current.getCause
    }
    messages.mkString(" | caused by: ")
  }

  private def assertNativePlan(df: org.apache.spark.sql.DataFrame): Unit = {
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    plan
      .collectFirst { case op if !isNativeOrPassThrough(op) => op }
      .foreach { op =>
        fail(s"""
             |Found non-native operator: ${op.nodeName}
             |plan:
             |${plan}""".stripMargin)
      }
  }
}
