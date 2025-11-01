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

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

import org.apache.auron.util.AuronTestUtils

class AuronFunctionSuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronSQLSuite
    with AdaptiveSparkPlanHelper {

  test("sum function with float input") {
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t1") {
        sql("create table t1 using parquet as select 1.0f as c1")
        val df = sql("select sum(c1) from t1")
        checkAnswer(df, Seq(Row(1.0)))
      }
    }
  }

  test("sha2 function") {
    withTable("t1") {
      sql("create table t1 using parquet as select 'spark' as c1, '3.x' as version")
      val functions =
        """
          |select
          |  sha2(concat(c1, version), 256) as sha0,
          |  sha2(concat(c1, version), 256) as sha256,
          |  sha2(concat(c1, version), 224) as sha224,
          |  sha2(concat(c1, version), 384) as sha384,
          |  sha2(concat(c1, version), 512) as sha512
          |from t1
          |""".stripMargin
      val df = sql(functions)
      checkAnswer(
        df,
        Seq(
          Row(
            "562d20689257f3f3a04ee9afb86d0ece2af106cf6c6e5e7d266043088ce5fbc0",
            "562d20689257f3f3a04ee9afb86d0ece2af106cf6c6e5e7d266043088ce5fbc0",
            "d0c8e9ccd5c7b3fdbacd2cfd6b4d65ca8489983b5e8c7c64cd77b634",
            "77c1199808053619c29e9af2656e1ad2614772f6ea605d5757894d6aec2dfaf34ff6fd662def3b79e429e9ae5ecbfed1",
            "c4e27d35517ca62243c1f322d7922dac175830be4668e8a1cf3befdcd287bb5b6f8c5f041c9d89e4609c8cfa242008c7c7133af1685f57bac9052c1212f1d089")))
    }
  }

  test("spark hash function") {
    withTable("t1") {
      sql("create table t1 using parquet as select array(1, 2) as arr")
      val functions =
        """
          |select hash(arr) from t1
          |""".stripMargin
      val df = sql(functions)
      checkAnswer(df, Seq(Row(-222940379)))
    }
  }

  test("expm1 function") {
    withTable("t1") {
      sql("create table t1(c1 double) using parquet")
      sql("insert into t1 values(0.0), (1.1), (2.2)")
      val df = sql("select expm1(c1) from t1")
      checkAnswer(df, Seq(Row(0.0), Row(2.0041660239464334), Row(8.025013499434122)))
    }
  }

  test("factorial function") {
    withTable("t1") {
      sql("create table t1(c1 int) using parquet")
      sql("insert into t1 values(5)")
      val df = sql("select factorial(c1) from t1")
      checkAnswer(df, Seq(Row(120)))
    }
  }

  test("hex function") {
    withTable("t1") {
      sql("create table t1(c1 int, c2 string) using parquet")
      sql("insert into t1 values(17, 'Spark SQL')")
      val df = sql("select hex(c1), hex(c2) from t1")
      checkAnswer(df, Seq(Row("11", "537061726B2053514C")))
    }
  }

  test("stddev_samp function with UDAF fallback") {
    withSQLConf("spark.auron.udafFallback.enable" -> "true") {
      withTable("t1") {
        sql("create table t1(c1 double) using parquet")
        sql("insert into t1 values(10.0), (20.0), (30.0), (31.0), (null)")
        val df = sql("select stddev_samp(c1) from t1")
        checkAnswer(df, Seq(Row(9.844626283748239)))
      }
    }
  }

  test("regexp_extract function with UDF failback") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values('Auron Spark SQL')")
      val df = sql("select regexp_extract(c1, '^A(.*)L$', 1) from t1")
      checkAnswer(df, Seq(Row("uron Spark SQ")))
    }
  }

  test("round function with varying scales for intPi") {
    withTable("t2") {
      sql("CREATE TABLE t2 (c1 INT) USING parquet")

      val intPi: Int = 314159265
      sql(s"INSERT INTO t2 VALUES($intPi)")

      val scales = -6 to 6
      val expectedResults = Map(
        -6 -> 314000000,
        -5 -> 314200000,
        -4 -> 314160000,
        -3 -> 314159000,
        -2 -> 314159300,
        -1 -> 314159270,
        0 -> 314159265,
        1 -> 314159265,
        2 -> 314159265,
        3 -> 314159265,
        4 -> 314159265,
        5 -> 314159265,
        6 -> 314159265)

      scales.foreach { scale =>
        val df = sql(s"SELECT round(c1, $scale) AS xx FROM t2")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("round function with varying scales for doublePi") {
    withTable("t1") {
      sql("create table t1(c1 double) using parquet")

      val doublePi: Double = math.Pi
      sql(s"insert into t1 values($doublePi)")
      val scales = -6 to 6
      val expectedResults = Map(
        -6 -> 0.0,
        -5 -> 0.0,
        -4 -> 0.0,
        -3 -> 0.0,
        -2 -> 0.0,
        -1 -> 0.0,
        0 -> 3.0,
        1 -> 3.1,
        2 -> 3.14,
        3 -> 3.142,
        4 -> 3.1416,
        5 -> 3.14159,
        6 -> 3.141593)

      scales.foreach { scale =>
        val df = sql(s"select round(c1, $scale) from t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("round function with varying scales for floatPi") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 FLOAT) USING parquet")

      val floatPi: Float = 3.1415f
      sql(s"INSERT INTO t1 VALUES($floatPi)")

      val scales = -6 to 6
      val expectedResults = Map(
        -6 -> 0.0f,
        -5 -> 0.0f,
        -4 -> 0.0f,
        -3 -> 0.0f,
        -2 -> 0.0f,
        -1 -> 0.0f,
        0 -> 3.0f,
        1 -> 3.1f,
        2 -> 3.14f,
        3 -> 3.142f,
        4 -> 3.1415f,
        5 -> 3.1415f,
        6 -> 3.1415f)

      scales.foreach { scale =>
        val df = sql(s"select round(c1, $scale) from t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("round function with varying scales for shortPi") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 SMALLINT) USING parquet")

      val shortPi: Short = 31415
      sql(s"INSERT INTO t1 VALUES($shortPi)")

      val scales = -6 to 6
      val expectedResults = Map(
        -6 -> 0.toShort,
        -5 -> 0.toShort,
        -4 -> 30000.toShort,
        -3 -> 31000.toShort,
        -2 -> 31400.toShort,
        -1 -> 31420.toShort,
        0 -> 31415.toShort,
        1 -> 31415.toShort,
        2 -> 31415.toShort,
        3 -> 31415.toShort,
        4 -> 31415.toShort,
        5 -> 31415.toShort,
        6 -> 31415.toShort)

      scales.foreach { scale =>
        val df = sql(s"SELECT round(c1, $scale) FROM t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("round function with varying scales for longPi") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 BIGINT) USING parquet")

      val longPi: Long = 31415926535897932L
      sql(s"INSERT INTO t1 VALUES($longPi)")

      val scales = -6 to 6
      val expectedResults = Map(
        -6 -> 31415926536000000L,
        -5 -> 31415926535900000L,
        -4 -> 31415926535900000L,
        -3 -> 31415926535898000L,
        -2 -> 31415926535897900L,
        -1 -> 31415926535897930L,
        0 -> 31415926535897932L,
        1 -> 31415926535897932L,
        2 -> 31415926535897932L,
        3 -> 31415926535897932L,
        4 -> 31415926535897932L,
        5 -> 31415926535897932L,
        6 -> 31415926535897932L)

      scales.foreach { scale =>
        val df = sql(s"SELECT round(c1, $scale) FROM t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("pow and power functions should return identical results") {
    withTable("t1") {
      sql("create table t1 using parquet as select 2 as base, 3 as exponent")

      val functions =
        """
          |select
          |  power(base, exponent) as power_result,
          |  pow(base, exponent) as pow_result
          |from t1
            """.stripMargin

      val df = sql(functions)

      checkAnswer(df, Seq(Row(8.0, 8.0)))
    }
  }

  test("pow/power should accept mixed numeric types and return double") {
    val df = sql("select pow(2, 3.0), pow(2.0, 3), power(1.5, 2)")
    checkAnswer(df, Seq(Row(8.0, 8.0, 2.25)))
  }

  test("pow: zero base with negative exponent yields +infinity") {
    val df = sql("select pow(0.0, -2.5), power(0.0, -3)")
    // Spark prints Infinity as Double.PositiveInfinity
    checkAnswer(df, Seq(Row(Double.PositiveInfinity, Double.PositiveInfinity)))
  }

  test("pow: zero to the zero equals one") {
    val df = sql("select pow(0.0, 0.0)")
    checkAnswer(df, Seq(Row(1.0)))
  }

  test("pow: negative base with fractional exponent is NaN") {
    val df = sql("select pow(-2, 0.5)")
    assert(df.collect().head.getDouble(0).isNaN)
  }

  test("pow null propagation") {
    val df = sql("select pow(null, 2), power(2, null), pow(null, null)")
    val row = df.collect().head
    assert(row.isNullAt(0) && row.isNullAt(1) && row.isNullAt(2))
  }

  test("nanvl NaN") {
    withTable("t1") {
      sql(
        "create table t1 using parquet as select 'NaN'" +
          " as base, 3 as exponent")
      val functions =
        """
          |select
          |  nanvl(base, 123), base, nanvl(exponent, 123)
          |from t1
            """.stripMargin

      val df = sql(functions)
      checkAnswer(df, Seq(Row(123.0, "NaN", 3)))
    }
  }
}
