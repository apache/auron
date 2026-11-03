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

import org.apache.spark.sql.{AnalysisException, AuronQueryTest, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf

import org.apache.auron.util.SparkVersionUtil

class AuronMathCompatibilitySuite extends AuronQueryTest with BaseAuronSQLSuite {
  // QueryTest compares raw bits; JVM and native math may use different NaN signs/payloads.
  private def canonicalizeNaNs(rows: Seq[Row]): Seq[Row] = {
    rows.map(row =>
      Row.fromSeq(row.toSeq.map {
        case value: Double if value.isNaN => Double.NaN
        case value => value
      }))
  }

  test("power result comparison ignores only NaN encodings") {
    val expected = Seq(Row(Double.NaN, -0.0d, Double.PositiveInfinity, null))
    for (bits <- Seq(0x7ff8000000000001L, 0xfff8000000000000L)) {
      val actual =
        Seq(Row(java.lang.Double.longBitsToDouble(bits), -0.0d, Double.PositiveInfinity, null))
      QueryTest
        .sameRows(canonicalizeNaNs(expected), canonicalizeNaNs(actual))
        .foreach(message => fail(message))
    }
    for ((expected, actual) <- Seq(
        (Row(0.0d), Row(-0.0d)),
        (Row(1.0d), Row(java.lang.Math.nextUp(1.0d))),
        (Row(Double.PositiveInfinity), Row(Double.NegativeInfinity)),
        (Row(null), Row(Double.NaN)),
        (Row(Double.NaN), Row(0.0d)))) {
      assert(
        QueryTest
          .sameRows(canonicalizeNaNs(Seq(expected)), canonicalizeNaNs(Seq(actual)))
          .nonEmpty)
    }
  }

  test("power preserves Spark NaN, infinity, signed zero and null semantics") {
    withTable("math_inputs") {
      sql("create table math_inputs (x double, y double) using parquet")
      sql("""insert into math_inputs values
          |(1, cast('NaN' as double)), (1, cast('Infinity' as double)),
          |(-1, cast('-Infinity' as double)), (cast('NaN' as double), 0),
          |(cast('-0.0' as double), -3), (cast('-0.0' as double), 3),
          |(cast('-0.0' as double), -2.5), (-2, 0.5), (null, 0), (1, null)
          |""".stripMargin)
      val query = """select pow(x, y), power(x, y),
          |pow(1.0D, y), pow(x, cast('NaN' as double)),
          |pow(x, cast('Infinity' as double)) from math_inputs""".stripMargin
      var expected = Seq.empty[Row]
      withSQLConf("spark.auron.enable" -> "false") {
        expected = canonicalizeNaNs(sql(query).collect().toSeq)
      }
      val df = sql(query)
      QueryTest
        .sameRows(expected, canonicalizeNaNs(df.collect().toSeq))
        .foreach(message => fail(message))
      assertPlanIsNative(df)
    }
  }

  test("round and bround preserve decimal values and result types") {
    withTable("math_inputs") {
      sql("create table math_inputs (d decimal(10, 3)) using parquet")
      sql("insert into math_inputs values (2.675), (2.685), (-2.675), (9.995), (125), (null)")
      for (scale <- Seq(-2, -1, 0, 1, 2, 3, 8); function <- Seq("round", "bround")) {
        val query = s"select $function(d, $scale) from math_inputs"
        if (scale < 0 && SparkVersionUtil.SPARK_RUNTIME_VERSION < "3.3") {
          for (enabled <- Seq("false", "true")) {
            withSQLConf(
              "spark.sql.legacy.allowNegativeScaleOfDecimal" -> "false",
              "spark.auron.enable" -> enabled) {
              val error = intercept[AnalysisException](sql(query).collect())
              assert(error.getMessage.contains("Negative scale is not allowed"))
            }
          }
        } else {
          checkSparkAnswerAndOperator(query)
        }
      }
    }
  }

  test("round and bround preserve legacy negative-scale decimals") {
    withSQLConf("spark.sql.legacy.allowNegativeScaleOfDecimal" -> "true") {
      withTable("math_inputs") {
        sql("create table math_inputs (d decimal(10, 3)) using parquet")
        sql("insert into math_inputs values (125), (-125), (250), (0), (null)")
        for (scale <- Seq(-2, -1); function <- Seq("round", "bround")) {
          checkSparkAnswerAndOperator(s"select $function(d, $scale) from math_inputs")
        }
      }
    }
  }

  test("round and bround use decimal rounding for floating point values") {
    withTable("math_inputs") {
      sql("create table math_inputs (d double, f float) using parquet")
      sql("""insert into math_inputs values
          |(1.015, 2.55), (2.675, 2.675), (2.685, 2.685), (-1.015, -2.55),
          |(cast('-0.0' as double), cast('-0.0' as float)),
          |(cast('NaN' as double), cast('Infinity' as float)), (null, null)
          |""".stripMargin)
      for (scale <- Seq(-1, 0, 1, 2, 3); function <- Seq("round", "bround")) {
        checkSparkAnswerAndOperator(
          s"select $function(d, $scale), $function(f, $scale) from math_inputs")
      }
    }
  }

  test("round and bround preserve long rounding beyond the i64 power of ten") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withTable("math_inputs") {
        sql("create table math_inputs (n bigint) using parquet")
        sql(
          "insert into math_inputs values (9223372036854775807), (-9223372036854775808), (null)")
        for (function <- Seq("round", "bround")) {
          checkSparkAnswerAndOperator(
            s"select $function(n, -19), $function(n, -20) from math_inputs")
        }
      }
    }
  }

  test("round and bround preserve null and foldable scales") {
    withTable("math_inputs") {
      sql("create table math_inputs (d double) using parquet")
      sql("insert into math_inputs values (2.675), (0), (null)")
      withSQLConf(
        SQLConf.ANSI_ENABLED.key -> "true",
        "spark.sql.optimizer.excludedRules" ->
          "org.apache.spark.sql.catalyst.optimizer.ConstantFolding") {
        for (function <- Seq("round", "bround")) {
          checkSparkAnswerAndOperator(
            s"select $function(d, 1 + 1), $function(d, cast(null as int)), " +
              s"$function(cast(1 / d as int), cast(null as int)) from math_inputs")
        }
      }
    }
  }

  test("round and bround preserve decimal precision limits") {
    withTable("math_inputs") {
      sql("create table math_inputs (d decimal(38, 38), n decimal(38, 0)) using parquet")
      sql("""insert into math_inputs values
          |(0.99999999999999999999999999999999999999,
          | 99999999999999999999999999999999999999)
          |""".stripMargin)
      checkSparkAnswerAndOperator("""select round(d, 37), bround(d, 37),
          |round(d, 0), bround(d, 0) from math_inputs""".stripMargin)
      for (function <- Seq("round", "bround"); ansi <- Seq("false", "true")) {
        val query = s"select $function(n, -1) from math_inputs"
        if (SparkVersionUtil.SPARK_RUNTIME_VERSION < "3.3") {
          // Legacy Decimal(38, -1) can represent 10^38; modern Decimal(38, 0) cannot.
          withSQLConf(
            SQLConf.ANSI_ENABLED.key -> ansi,
            "spark.sql.legacy.allowNegativeScaleOfDecimal" -> "true") {
            checkSparkAnswerAndOperator(query)
          }
        } else {
          for (enabled <- Seq("false", "true")) {
            withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi, "spark.auron.enable" -> enabled) {
              val df = sql(query)
              intercept[Exception](df.collect())
              if (enabled == "true") {
                assertPlanIsNative(df)
              }
            }
          }
        }
      }
    }
  }

  test("round and bround honor ANSI integer overflow") {
    if (SparkVersionUtil.SPARK_RUNTIME_VERSION >= "3.4") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        withTable("math_inputs") {
          sql("create table math_inputs (n bigint) using parquet")
          sql("insert into math_inputs values (9223372036854775807)")
          for (function <- Seq("round", "bround"); enabled <- Seq("false", "true")) {
            withSQLConf("spark.auron.enable" -> enabled) {
              val df = sql(s"select $function(n, -1) from math_inputs")
              intercept[Exception](df.collect())
              if (enabled == "true") {
                assertPlanIsNative(df)
              }
            }
          }
        }
      }
    }
  }
}
