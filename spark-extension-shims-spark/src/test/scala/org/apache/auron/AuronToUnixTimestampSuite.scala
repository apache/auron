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

import java.time.{Instant, ZoneOffset}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AuronQueryTest, DataFrame, Row}
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.catalyst.expressions.ToUnixTimestamp
import org.apache.spark.sql.execution.ProjectExec
import org.apache.spark.sql.execution.auron.plan.NativeProjectBase
import org.apache.spark.sql.internal.SQLConf

import org.apache.auron.protobuf.PhysicalExprNode
import org.apache.auron.util.{AuronTestUtils, SparkVersionUtil}

class AuronToUnixTimestampSuite extends AuronQueryTest with BaseAuronSQLSuite {
  private val nativeStringParsing = SparkVersionUtil.SPARK_RUNTIME_VERSION >= "3.2"

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set("spark.auron.enableNativeDateTimeFunctions", "true")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.sql.session.timeZone", "UTC")
      .set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
      .set("spark.sql.ansi.enabled", "false")

  private def expressions(df: DataFrame): Seq[ToUnixTimestamp] =
    df.queryExecution.analyzed.expressions.flatMap(_.collect { case e: ToUnixTimestamp => e })

  private def assertNativeFunction(df: DataFrame): Unit = {
    assertPlanIsNative(df)
    val expressions = stripAQEPlan(df.queryExecution.executedPlan).collect {
      case project: NativeProjectBase =>
        project.expressions.flatMap(_.collect { case e: ToUnixTimestamp => e })
    }.flatten
    assert(expressions.nonEmpty, df.queryExecution.executedPlan.toString)
    expressions.foreach { expression =>
      val converted = NativeConverters.convertExpr(expression)
      assert(converted.hasScalarFunction)
      assert(converted.getScalarFunction.getName == "Spark_ToUnixTimestamp")
      assert(!converted.getScalarFunction.getArgs(0).hasSparkUdfWrapperExpr)
    }
  }

  private def checkNative(query: String): DataFrame = {
    val df = checkSparkAnswerAndOperator(query)
    assertNativeFunction(df)
    df
  }

  private def checkString(query: String): Unit = {
    if (nativeStringParsing) {
      checkNative(query)
    } else {
      checkFallback(query)
    }
  }

  private def checkFallback(query: String): Unit =
    assertFallbackFunction(checkSparkAnswer(query))

  private def assertFallbackFunction(df: DataFrame): Unit = {
    assert(stripAQEPlan(df.queryExecution.executedPlan).collect { case _: ProjectExec =>
      true
    }.nonEmpty)
    expressions(df).foreach { expression =>
      var fellBack = false
      NativeConverters.convertExprWithFallback(
        expression,
        isPruningExpr = false,
        e => {
          assert(e == expression)
          fellBack = true
          PhysicalExprNode.getDefaultInstance
        })
      assert(fellBack)
    }
    assert(expressions(df).nonEmpty)
  }

  private def causeMessages(error: Throwable): String = {
    val messages = scala.collection.mutable.ArrayBuffer.empty[String]
    var cause = error
    while (cause != null) {
      messages += Option(cause.getMessage).getOrElse("")
      cause = cause.getCause
    }
    messages.mkString("\n")
  }

  test("timestamp input truncates toward zero and ignores format and parser settings") {
    withTable("t_unix_timestamp") {
      sql("create table t_unix_timestamp(ts timestamp, fmt string) using parquet")
      sql("""insert into t_unix_timestamp values
          |  (timestamp'1969-12-31 23:59:59.999999', 'invalid'),
          |  (timestamp'1969-12-31 23:59:58.999999', null),
          |  (timestamp'1970-01-01 00:00:00', 'yyyy-MM-dd'),
          |  (timestamp'2024-02-29 12:34:56.123456', 'invalid'),
          |  (null, 'invalid')
          |""".stripMargin)
      for (zone <- Seq("UTC", "Asia/Shanghai", "America/New_York");
        policy <- Seq("CORRECTED", "LEGACY", "EXCEPTION");
        ansi <- Seq("false", "true")) {
        withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone,
          SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy,
          SQLConf.ANSI_ENABLED.key -> ansi) {
          checkNative("""select to_unix_timestamp(ts), to_unix_timestamp(ts, fmt),
              |  to_unix_timestamp(ts, cast(null as string)),
              |  to_unix_timestamp(ts, 'invalid') from t_unix_timestamp
              |""".stripMargin)
        }
      }
    }
  }

  test("date input applies fixed offsets and ignores format") {
    withTable("t_unix_date") {
      sql("create table t_unix_date(dt date, fmt string) using parquet")
      sql("""insert into t_unix_date values
          |  (date'1970-01-01', 'invalid'), (date'1969-12-31', null),
          |  (date'2024-02-29', 'invalid'), (null, 'invalid')
          |""".stripMargin)
      for (zone <- Seq("UTC", "+08:00", "-05:30", "+05:45:30")) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
          checkNative("""select to_unix_timestamp(dt), to_unix_timestamp(dt, fmt),
              |  to_unix_timestamp(dt, cast(null as string)) from t_unix_date
              |""".stripMargin)
        }
      }
    }
  }

  test("corrected string parsing matches strict Spark dates and extended years") {
    withTable("t_unix_string") {
      sql("create table t_unix_string(s string) using parquet")
      sql("""insert into t_unix_string values
          |  ('1970-01-01 00:00:00'), ('1969-12-31 23:59:59'),
          |  ('2024-02-29 12:34:56'), ('1500-03-01 00:00:00'),
          |  ('1582-10-04 00:00:00'), ('0000-01-01 00:00:00'),
          |  ('-0001-01-01 00:00:00'), ('+10000-01-01 00:00:00'),
          |  ('+0000000001-01-01 00:00:00'), ('-0000-01-01 00:00:00'),
          |  ('2021-02-29 00:00:00'), ('2020-13-01 00:00:00'),
          |  ('2020-1-01 00:00:00'), ('2020-01-01 24:00:00'),
          |  ('2020-01-01 00:00:60'), ('2020-01-01 00:00:00XYZ'),
          |  (' 2020-01-01 00:00:00'), (''), (null)
          |""".stripMargin)
      for (zone <- Seq("UTC", "+05:45:30", "-18:00")) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
          checkString("select to_unix_timestamp(s) from t_unix_string")
          checkString("select to_unix_timestamp(s, cast(null as string)) from t_unix_string")
        }
      }
      checkString("""select to_unix_timestamp(substring(s, 1, 10), 'yyyy-MM-dd')
          |from t_unix_string""".stripMargin)
      checkString("""select to_unix_timestamp(s, concat('yyyy-MM-dd ', 'HH:mm:ss'))
          |from t_unix_string""".stripMargin)
    }
  }

  test("fractional seconds and quoted T preserve negative timestamp truncation") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("t_unix_fraction") {
        sql("create table t_unix_fraction(s string, iso string) using parquet")
        sql("""insert into t_unix_fraction values
            |  ('1969-12-31 23:59:59.999999999', '1969-12-31T23:59:59.999999999'),
            |  ('1970-01-01 00:00:01.123456789', '1970-01-01T00:00:01.123456789'),
            |  (null, null)
            |""".stripMargin)
        for (digits <- 1 to 9) {
          val format = "yyyy-MM-dd HH:mm:ss." + "S" * digits
          checkString(
            s"select to_unix_timestamp(substring(s, 1, ${20 + digits}), '$format') " +
              "from t_unix_fraction")
        }
        checkString("""select to_unix_timestamp(iso, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS")
            |from t_unix_fraction""".stripMargin)
      }
    }
  }

  test("quoted T accepts lowercase input with and without fractional seconds") {
    withTable("t_unix_lowercase_t") {
      sql("create table t_unix_lowercase_t(s string, fraction string) using parquet")
      sql("""insert into t_unix_lowercase_t values
          |  ('1970-01-01T00:00:01', '1970-01-01T00:00:01.123456789'),
          |  ('1970-01-01t00:00:01', '1969-12-31t23:59:59.999999999'),
          |  (null, null)
          |""".stripMargin)
      for (ansi <- Seq("false", "true")) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          val query = """select
              |  to_unix_timestamp(s, "yyyy-MM-dd'T'HH:mm:ss"),
              |  to_unix_timestamp(fraction, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS")
              |from t_unix_lowercase_t""".stripMargin
          checkString(query)
          checkAnswer(sql(query), Seq(Row(1L, 1L), Row(1L, 0L), Row(null, null)))
        }
      }
    }
  }

  test("invalid strings return null or fail according to expression failOnError") {
    withTable("t_unix_invalid") {
      sql("create table t_unix_invalid(s string) using parquet")
      sql("insert into t_unix_invalid values ('2021-02-29 00:00:00')")
      val query = "select to_unix_timestamp(s) from t_unix_invalid"
      checkString(query)
      checkAnswer(sql(query), Seq(Row(null)))
      if (AuronTestUtils.isSparkV31OrGreater) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          withSQLConf("spark.auron.enable" -> "false") {
            intercept[Exception](sql(query).collect())
          }
          val df = sql(query)
          val error = intercept[Exception](df.collect())
          if (nativeStringParsing) {
            assertNativeFunction(df)
            assert(causeMessages(error).contains("to_unix_timestamp cannot parse"))
          } else {
            assertFallbackFunction(df)
          }
        }
      }
    }
  }

  test("special timestamp strings retain Spark version semantics") {
    withTable("t_unix_special") {
      sql("create table t_unix_special(s string) using parquet")
      sql("""insert into t_unix_special values
          |  ('epoch'), ('now'), ('today'), ('tomorrow'), ('yesterday'), (null)
          |""".stripMargin)
      val query = "select s, to_unix_timestamp(s) from t_unix_special"
      if (nativeStringParsing) {
        checkNative(query)
        checkAnswer(
          sql(query),
          Seq("epoch", "now", "today", "tomorrow", "yesterday", null).map(Row(_, null)))
      } else {
        for (ansi <- Seq("false", "true")) {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            val before = Instant.now()
            val df = sql(query)
            val rows = df.collect()
            val after = Instant.now()
            assertFallbackFunction(df)
            assert(rows.contains(Row("epoch", 0L)))
            assert(rows.contains(Row(null, null)))
            val values =
              rows.filterNot(_.isNullAt(0)).map(r => r.getString(0) -> r.getLong(1)).toMap
            assert(values("now") >= before.getEpochSecond)
            assert(values("now") <= after.getEpochSecond)
            for ((name, days) <- Seq("today" -> 0, "tomorrow" -> 1, "yesterday" -> -1)) {
              val expected = Seq(before, after).map { instant =>
                instant
                  .atZone(ZoneOffset.UTC)
                  .toLocalDate
                  .plusDays(days)
                  .atStartOfDay
                  .toEpochSecond(ZoneOffset.UTC)
              }
              assert(expected.contains(values(name)))
            }
          }
        }
      }
    }
  }

  test("unsupported formats policies time zones and disabled native datetime fall back") {
    withTable("t_unix_fallback") {
      sql("create table t_unix_fallback(s string, fmt string, dt date) using parquet")
      sql("""insert into t_unix_fallback values
          |  ('2024-02-29', 'yyyy-MM-dd', date'2024-02-29'), (null, null, null)
          |""".stripMargin)
      checkFallback("select to_unix_timestamp(s, fmt) from t_unix_fallback")
      checkFallback("select to_unix_timestamp(s, 'yyyy-M-d') from t_unix_fallback")
      for (policy <- Seq("LEGACY", "EXCEPTION")) {
        withSQLConf(SQLConf.LEGACY_TIME_PARSER_POLICY.key -> policy) {
          checkFallback("select to_unix_timestamp(s, 'yyyy-MM-dd') from t_unix_fallback")
        }
      }
      for (zone <- Seq("Asia/Shanghai", "America/New_York", "Pacific/Apia")) {
        withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> zone) {
          checkFallback("select to_unix_timestamp(s, 'yyyy-MM-dd') from t_unix_fallback")
          checkFallback("select to_unix_timestamp(dt) from t_unix_fallback")
        }
      }
      withSQLConf("spark.auron.enableNativeDateTimeFunctions" -> "false") {
        checkFallback("select to_unix_timestamp(dt) from t_unix_fallback")
      }
      withSQLConf("spark.auron.udf.singleChildFallback.enabled" -> "true") {
        val df = checkSparkAnswer("select to_unix_timestamp(s, fmt) from t_unix_fallback")
        assertPlanIsNative(df)
        assert(NativeConverters.convertExpr(expressions(df).head).hasSparkUdfWrapperExpr)
      }
    }
  }

  test("date and string microsecond overflow is not converted to null") {
    withTable("t_unix_overflow") {
      sql("create table t_unix_overflow(days int, s string) using parquet")
      sql("insert into t_unix_overflow values (2147483647, '+300000-01-01')")
      val dateQuery =
        "select to_unix_timestamp(date_add(date'1970-01-01', days)) from t_unix_overflow"
      val queries = if (nativeStringParsing) {
        Seq(dateQuery, "select to_unix_timestamp(s, 'yyyy-MM-dd') from t_unix_overflow")
      } else {
        Seq(dateQuery)
      }
      for (ansi <- Seq("false", "true"); query <- queries) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          withSQLConf("spark.auron.enable" -> "false") {
            intercept[Exception](sql(query).collect())
          }
          val df = sql(query)
          val error = intercept[Exception](df.collect())
          assertNativeFunction(df)
          assert(causeMessages(error).contains("long overflow in to_unix_timestamp"))
        }
      }
    }
  }

  test("string overflow preserves Spark behavior without code generation") {
    withSQLConf(
      "spark.sql.codegen.factoryMode" -> "NO_CODEGEN",
      "spark.sql.codegen.wholeStage" -> "false") {
      withTable("t_unix_string_overflow") {
        sql("create table t_unix_string_overflow(s string) using parquet")
        sql("insert into t_unix_string_overflow values ('+300000-01-01')")
        val query = "select to_unix_timestamp(s, 'yyyy-MM-dd') from t_unix_string_overflow"
        for (ansi <- Seq("false", "true")) {
          withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
            if (!AuronTestUtils.isSparkV31OrGreater) {
              checkFallback(query)
              checkAnswer(sql(query), Seq(Row(null)))
            } else {
              withSQLConf("spark.auron.enable" -> "false") {
                intercept[Exception](sql(query).collect())
              }
              val df = sql(query)
              val error = intercept[Exception](df.collect())
              if (nativeStringParsing) {
                assertNativeFunction(df)
              } else {
                assertFallbackFunction(df)
              }
              assert(causeMessages(error).contains("long overflow"))
            }
          }
        }
      }
    }
  }

  test("conversion uses resolved expression timezone and ANSI flag") {
    withTable("t_unix_bound") {
      sql("create table t_unix_bound(dt date, s string) using parquet")
      val date = expressions(sql("select to_unix_timestamp(dt) from t_unix_bound")).head
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/New_York") {
        val converted = NativeConverters.convertExpr(date).getScalarFunction
        assert(converted.getName == "Spark_ToUnixTimestamp")
        assert(converted.getArgsCount == 2)
      }
      if (nativeStringParsing) {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          val string = expressions(sql("select to_unix_timestamp(s) from t_unix_bound")).head
          withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
            val flag = NativeConverters.convertExpr(string).getScalarFunction.getArgs(3)
            assert(
              flag == NativeConverters.convertExpr(
                org.apache.spark.sql.catalyst.expressions.Literal(true)))
          }
        }
      }
    }
  }

  test("timestamp_ntz retains Spark fallback") {
    if (SparkVersionUtil.SPARK_RUNTIME_VERSION >= "3.3") {
      withTable("t_unix_ntz") {
        sql("create table t_unix_ntz(ts timestamp_ntz) using parquet")
        sql("insert into t_unix_ntz values (timestamp_ntz'1969-12-31 23:59:59.999999')")
        checkFallback("select to_unix_timestamp(ts) from t_unix_ntz")
      }
    }
  }
}
