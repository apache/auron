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
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, GreaterThan, Literal}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, StringType, StructType}

import org.apache.auron.protobuf.ScalarFunction
import org.apache.auron.spark.configuration.SparkAuronConfiguration
import org.apache.auron.util.{AuronTestUtils, SparkVersionUtil}

class NativeConvertersSuite
    extends AuronQueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {

  private def dateTimeExpressions: Seq[Expression] = {
    val functions = Seq(
      "year(date'2024-03-15')",
      "month(date'2024-03-15')",
      "dayofmonth(date'2024-03-15')",
      "dayofyear(date'2024-03-15')",
      "dayofweek(date'2024-03-15')",
      "weekofyear(date'2024-03-15')",
      "quarter(date'2024-03-15')",
      "hour(timestamp'2024-03-15 14:30:45')",
      "minute(timestamp'2024-03-15 14:30:45')",
      "second(timestamp'2024-03-15 14:30:45')",
      "last_day(date'2024-03-15')",
      "next_day(date'2024-03-15', 'MON')",
      "datediff(date'2024-03-15', date'2024-03-01')",
      "date_add(date'2024-03-15', 1)",
      "date_sub(date'2024-03-15', 1)",
      "add_months(date'2024-03-15', 1)",
      "months_between(timestamp'2024-03-15 14:30:45', timestamp'2024-02-15 14:30:45')",
      "date_trunc('day', timestamp'2024-03-15 14:30:45')") ++
      (if (AuronTestUtils.isSparkV30OrGreater) {
         Seq("make_date(2024, 3, 15)", "make_timestamp(2024, 3, 15, 12, 30, 45.123456)")
       } else Nil) ++
      (if (AuronTestUtils.isSparkV31OrGreater) {
         Seq("unix_date(date'2024-03-15')", "date_from_unix_date(1)")
       } else Nil)
    functions.map { function =>
      sql(s"select $function").queryExecution.analyzed.expressions.head.children.head
    }
  }

  test("datetime native conversion is disabled by default") {
    assert(!SparkAuronConfiguration.ENABLE_NATIVE_DATETIME_FUNCTIONS.defaultValue())
    assert(!NativeConverters.nativeDateTimeFunctionsEnabled)
    withSQLConf(
      "spark.auron.udf.singleChildFallback.enabled" -> "true",
      "spark.auron.datetime.extract.enabled" -> "true") {
      dateTimeExpressions.foreach { expr =>
        assert(NativeConverters.convertExpr(expr).hasSparkUdfWrapperExpr)
      }
    }
  }

  test("datetime expressions fall back for regular and pruning conversion when disabled") {
    withSQLConf(
      "spark.auron.enableNativeDateTimeFunctions" -> "false",
      "spark.auron.datetime.extract.enabled" -> "true") {
      for (expr <- dateTimeExpressions; isPruningExpr <- Seq(false, true)) {
        var fallbackCalled = false
        NativeConverters.convertExprWithFallback(
          expr,
          isPruningExpr,
          e => {
            assert(e == expr)
            fallbackCalled = true
            org.apache.auron.protobuf.PhysicalExprNode.getDefaultInstance
          })
        assert(fallbackCalled)
      }
      assert(NativeConverters.convertExpr(Literal(1)).hasLiteral)
    }
  }

  test("datetime expressions keep their native conversion when enabled") {
    withSQLConf(
      "spark.auron.enableNativeDateTimeFunctions" -> "true",
      "spark.auron.datetime.extract.enabled" -> "true",
      SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      dateTimeExpressions.foreach { expr =>
        val converted = NativeConverters.convertExpr(expr)
        assert(converted.hasScalarFunction || converted.hasTryCast)
      }
    }
  }

  test("datetime master switch does not override the time-part extract switch") {
    withSQLConf(
      "spark.auron.enableNativeDateTimeFunctions" -> "true",
      "spark.auron.datetime.extract.enabled" -> "false",
      "spark.auron.udf.singleChildFallback.enabled" -> "true") {
      dateTimeExpressions
        .filter(e => Seq("Hour", "Minute", "Second").contains(e.getClass.getSimpleName))
        .foreach { expr =>
          assert(NativeConverters.convertExpr(expr).hasSparkUdfWrapperExpr)
        }
    }
  }

  test("make_timestamp converts session and explicit time zones natively") {
    for (ansi <- Seq("false", "true")) {
      withSQLConf(
        "spark.auron.enableNativeDateTimeFunctions" -> "true",
        "spark.sql.ansi.enabled" -> ansi,
        SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles") {
        for (timezone <- Seq("", ", 'PST'", ", 'GMT+05:30:15'", ", cast(null as string)")) {
          val expr = sql(
            s"select make_timestamp(2024, 3, 15, 12, 30, 45.123456$timezone)").queryExecution.analyzed.expressions.head.children.head
          val converted = NativeConverters.convertExpr(expr)
          assert(converted.hasScalarFunction)
          assert(converted.getScalarFunction.getName == "Spark_MakeTimestamp")
          assert(converted.getScalarFunction.getArgsCount == 8)
        }
      }
    }
  }

  test("make_timestamp keeps unsupported time zones on the fallback path") {
    withSQLConf(
      "spark.auron.enableNativeDateTimeFunctions" -> "true",
      "spark.auron.udf.singleChildFallback.enabled" -> "true") {
      for (timezone <- Seq("cast(id as string)", "'SystemV/PST8PDT'", "'invalid/zone'")) {
        val expr = sql(
          s"select make_timestamp(2024, 3, 15, 12, 30, 0, $timezone) from range(1)").queryExecution.analyzed.expressions.head.children.head
        assert(NativeConverters.convertExpr(expr).hasSparkUdfWrapperExpr)
      }
    }
  }

  test("make_timestamp guards years outside the native timezone range") {
    withSQLConf(
      "spark.auron.enableNativeDateTimeFunctions" -> "true",
      "spark.auron.udf.singleChildFallback.enabled" -> "true") {
      val expr = sql(
        "select make_timestamp(y, 7, 1, 12, 0, 0, 'PST') from (select 2024 as y)").queryExecution.analyzed.expressions.head.children.head
      val converted = NativeConverters.convertExpr(expr)
      assert(converted.hasCase)
      assert(
        converted.getCase
          .getWhenThenExpr(0)
          .getThenExpr
          .getScalarFunction
          .getName == "Spark_MakeTimestamp")
      assert(converted.getCase.getElseExpr.hasSparkUdfWrapperExpr)
      val future = sql(
        "select make_timestamp(9999, 7, 1, 12, 0, 0, 'PST')").queryExecution.analyzed.expressions.head.children.head
      assert(NativeConverters.convertExpr(future).hasSparkUdfWrapperExpr)
    }
  }

  test("make_timestamp leaves narrowing casts inside its Spark fallback") {
    withSQLConf(
      "spark.auron.enableNativeDateTimeFunctions" -> "true",
      "spark.auron.udf.singleChildFallback.enabled" -> "true") {
      for (month <- Seq("7", "java_method('java.lang.String', 'valueOf', 1)")) {
        val expr = sql(
          s"select make_timestamp(cast(id as int), $month, 1, 12, 0, 0, 'UTC') from range(1)").queryExecution.analyzed.expressions.head.children.head
        val converted = NativeConverters.convertExpr(expr)
        assert(converted.hasSparkUdfWrapperExpr)
        val (bound, _) = NativeConverters.deserializeExpression[Expression, StructType](
          converted.getSparkUdfWrapperExpr.getSerialized.toByteArray)
        assert(Seq("Cast", "AnsiCast").contains(bound.children.head.getClass.getSimpleName))
        assert(converted.getSparkUdfWrapperExpr.getParamsList.toArray.forall { param =>
          !param.asInstanceOf[org.apache.auron.protobuf.PhysicalExprNode].hasTryCast
        })
      }
    }
  }

  test("make_timestamp with NTZ result keeps its existing fallback") {
    if (SparkVersionUtil.SPARK_RUNTIME_VERSION >= "3.4") {
      withSQLConf(
        "spark.auron.enableNativeDateTimeFunctions" -> "true",
        "spark.sql.timestampType" -> "TIMESTAMP_NTZ") {
        val expr = sql(
          "select make_timestamp(2024, 3, 15, 12, 30, 0)").queryExecution.analyzed.expressions.head.children.head
        var fallbackCalled = false
        NativeConverters.convertExprWithFallback(
          expr,
          isPruningExpr = false,
          _ => {
            fallbackCalled = true
            org.apache.auron.protobuf.PhysicalExprNode.getDefaultInstance
          })
        assert(fallbackCalled)
      }
    }
  }

  private def assertTrimmedCast(rawValue: String, targetType: DataType): Unit = {
    val expr = Cast(Literal.create(rawValue, StringType), targetType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(childExpr.hasScalarFunction)
    val scalarFn = childExpr.getScalarFunction
    assert(scalarFn.getFun == ScalarFunction.Trim)
    assert(scalarFn.getArgsCount == 1 && scalarFn.getArgs(0).hasLiteral)
  }

  private def assertNonTrimmedCast(rawValue: String, targetType: DataType): Unit = {
    val expr = Cast(Literal.create(rawValue, StringType), targetType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(!childExpr.hasScalarFunction)
    assert(childExpr.hasLiteral)
  }

  test("cast from string to numeric adds trim wrapper before native cast when enabled") {
    withSQLConf("spark.auron.cast.trimString" -> "true") {
      assertTrimmedCast(" 42 ", IntegerType)
    }
  }

  test("cast from string to boolean adds trim wrapper before native cast when enabled") {
    withSQLConf("spark.auron.cast.trimString" -> "true") {
      assertTrimmedCast(" true ", BooleanType)
    }
  }

  test("cast trim disabled via auron conf") {
    withSQLConf("spark.auron.cast.trimString" -> "false") {
      assertNonTrimmedCast(" 42 ", IntegerType)
    }
  }

  test("cast trim disabled via auron conf for boolean cast") {
    withSQLConf("spark.auron.cast.trimString" -> "false") {
      assertNonTrimmedCast(" true ", BooleanType)
    }
  }

  test("cast with non-string child remains unchanged") {
    val expr = Cast(Literal(1.5), IntegerType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(!childExpr.hasScalarFunction)
    assert(childExpr.hasLiteral)
  }

  test("float scan pruning keeps column and literal comparisons") {
    val comparisons = Seq(
      GreaterThan(AttributeReference("d", DoubleType)(), Literal(100.0)),
      GreaterThan(AttributeReference("f", FloatType)(), Literal(100.0f)))

    comparisons.foreach { expr =>
      val pruning = NativeConverters.convertScanPruningExpr(expr).getBinaryExpr
      assert(pruning.getL.hasColumn)
      assert(pruning.getR.hasLiteral)

      val rowFilter = NativeConverters.convertExpr(expr).getBinaryExpr
      assert(rowFilter.getL.getScalarFunction.getName == "Spark_NormalizeNanAndZero")
      assert(rowFilter.getR.getScalarFunction.getName == "Spark_NormalizeNanAndZero")
    }
  }
}
