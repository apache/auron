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
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, StringType}

import org.apache.auron.protobuf.ScalarFunction
import org.apache.auron.spark.configuration.SparkAuronConfiguration
import org.apache.auron.util.AuronTestUtils

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
      "to_unix_timestamp(timestamp'2024-03-15 14:30:45')",
      "date_trunc('day', timestamp'2024-03-15 14:30:45')") ++
      (if (AuronTestUtils.isSparkV30OrGreater) Seq("make_date(2024, 3, 15)") else Nil) ++
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
