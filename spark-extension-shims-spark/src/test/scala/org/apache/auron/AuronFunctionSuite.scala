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

import java.text.SimpleDateFormat

import org.apache.spark.sql.{AuronQueryTest, Row}

import org.apache.auron.util.AuronTestUtils

class AuronFunctionSuite extends AuronQueryTest with BaseAuronSQLSuite {

  test("sum function with float input") {
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t1") {
        sql("create table t1 using parquet as select 1.0f as c1")
        checkSparkAnswerAndOperator("select sum(c1) from t1")
      }
    }
  }

  test("sha2 function") {
    // SPARK-36836: In Spark 3.0/3.1, sha2(..., 224) may produce garbled UTF instead of hex.
    // For < 3.2, compare against known hex outputs directly; for >= 3.2, compare to Spark baseline.
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
      if (AuronTestUtils.isSparkV32OrGreater) {
        checkSparkAnswerAndOperator(functions)
      } else {
        val df = sql(functions)
        // Expected hex for input concat('spark','3.x')
        val expected = Seq(
          Row(
            "562d20689257f3f3a04ee9afb86d0ece2af106cf6c6e5e7d266043088ce5fbc0", // sha0 (256)
            "562d20689257f3f3a04ee9afb86d0ece2af106cf6c6e5e7d266043088ce5fbc0", // sha256
            "d0c8e9ccd5c7b3fdbacd2cfd6b4d65ca8489983b5e8c7c64cd77b634", // sha224
            "77c1199808053619c29e9af2656e1ad2614772f6ea605d5757894d6aec2dfaf34ff6fd662def3b79e429e9ae5ecbfed1", // sha384
            "c4e27d35517ca62243c1f322d7922dac175830be4668e8a1cf3befdcd287bb5b6f8c5f041c9d89e4609c8cfa242008c7c7133af1685f57bac9052c1212f1d089" // sha512
          ))
        checkAnswer(df, expected)
      }
    }
  }

  test("md5 function") {
    withTable("t1") {
      sql("create table t1 using parquet as select 'spark' as c1, '3.x' as version")
      val functions =
        """
          |select b.md5
          |from (
          |  select c1, version from t1
          |) a join (
          |  select md5(concat(c1, version)) as md5 from t1
          |) b on md5(concat(a.c1, a.version)) = b.md5
          |""".stripMargin
      checkSparkAnswerAndOperator(functions)
    }
  }

  test("spark hash function") {
    withTable("t1") {
      sql("create table t1 using parquet as select array(1, 2) as arr")
      val functions =
        """
          |select hash(arr) from t1
          |""".stripMargin
      checkSparkAnswerAndOperator(functions)
    }
  }

  test("expm1 function") {
    withTable("t1") {
      sql("create table t1(c1 double) using parquet")
      sql("insert into t1 values(0.0), (1.1), (2.2)")
      checkSparkAnswerAndOperator("select expm1(c1) from t1")
    }
  }

  test("factorial function") {
    withTable("t1") {
      sql("create table t1(c1 int) using parquet")
      sql("insert into t1 values(5)")
      checkSparkAnswerAndOperator("select factorial(c1) from t1")
    }
  }

  test("hex function") {
    withTable("t1") {
      sql("create table t1(c1 int, c2 string) using parquet")
      sql("insert into t1 values(17, 'Spark SQL')")
      checkSparkAnswerAndOperator("select hex(c1), hex(c2) from t1")
    }
  }

  test("stddev_samp function with UDAF fallback") {
    withSQLConf("spark.auron.udafFallback.enable" -> "true") {
      withTable("t1") {
        sql("create table t1(c1 double) using parquet")
        sql("insert into t1 values(10.0), (20.0), (30.0), (31.0), (null)")
        checkSparkAnswerAndOperator("select stddev_samp(c1) from t1")
      }
    }
  }

  test("regexp_extract function with UDF failback") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values('Auron Spark SQL')")
      checkSparkAnswerAndOperator("select regexp_extract(c1, '^A(.*)L$', 1) from t1")
    }
  }

  test("round function with varying scales for intPi") {
    withTable("t2") {
      sql("CREATE TABLE t2 (c1 INT) USING parquet")

      val intPi: Int = 314159265
      sql(s"INSERT INTO t2 VALUES($intPi)")

      val scales = -6 to 6

      scales.foreach { scale =>
        checkSparkAnswerAndOperator(s"SELECT round(c1, $scale) AS xx FROM t2")
      }
    }
  }

  test("round function with varying scales for doublePi") {
    withTable("t1") {
      sql("create table t1(c1 double) using parquet")

      val doublePi: Double = math.Pi
      sql(s"insert into t1 values($doublePi)")
      val scales = -6 to 6

      scales.foreach { scale =>
        checkSparkAnswerAndOperator(s"select round(c1, $scale) from t1")
      }
    }
  }

  test("round function with varying scales for floatPi") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 FLOAT) USING parquet")

      val floatPi: Float = math.Pi.toFloat
      sql(s"INSERT INTO t1 VALUES($floatPi)")

      val scales = -6 to 6

      scales.foreach { scale =>
        checkSparkAnswerAndOperator(s"select round(c1, $scale) from t1")
      }
    }
  }

  test("round function with varying scales for shortPi") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 SMALLINT) USING parquet")

      val shortPi: Short = 31415
      sql(s"INSERT INTO t1 VALUES($shortPi)")

      val scales = -6 to 6

      scales.foreach { scale =>
        checkSparkAnswerAndOperator(s"SELECT round(c1, $scale) FROM t1")
      }
    }
  }

  test("round function with varying scales for longPi") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 BIGINT) USING parquet")

      val longPi: Long = 31415926535897932L
      sql(s"INSERT INTO t1 VALUES($longPi)")

      val scales = -6 to 6
      scales.foreach { scale =>
        checkSparkAnswerAndOperator(s"SELECT round(c1, $scale) FROM t1")
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

      checkSparkAnswerAndOperator(functions)
    }
  }

  test("pow/power should accept mixed numeric types and return double") {
    withTable("t1") {
      sql("create table t1(c1 double, c2 double) using parquet")
      sql("insert into t1 values(2, 3.0), (2.0, 3), (1.5, 2)")
      checkSparkAnswerAndOperator("select pow(c1, c2) from t1")
    }
  }

  test("pow: zero base with negative exponent yields +infinity") {
    withTable("t1") {
      sql("create table t1(c1 double, c2 double) using parquet")
      sql("insert into t1 values(0.0, -2.5), (0.0, -3)")
      checkSparkAnswerAndOperator("select pow(c1, c2) from t1")
    }
  }

  test("pow: zero to the zero equals one") {
    withTable("t1") {
      sql("create table t1(c1 double, c2 double) using parquet")
      sql("insert into t1 values(0.0, 0.0)")
      checkSparkAnswerAndOperator("select pow(c1, c2) from t1")
    }
  }

  test("pow: negative base with fractional exponent is NaN") {
    withTable("t1") {
      sql("create table t1(c1 double, c2 double) using parquet")
      sql("insert into t1 values(-2, 0.5)")
      checkSparkAnswerAndOperator("select pow(c1, c2) from t1")
    }
  }

  test("pow null propagation") {
    withTable("t1") {
      sql("create table t1(c1 double, c2 double) using parquet")
      sql("insert into t1 values(null, 2),(2, null),(null, null)")
      checkSparkAnswerAndOperator("select pow(c1, c2) from t1")
    }
  }

  test("test function least") {
    withTable("test_least") {
      sql(
        "create table test_least using parquet as select 1 as c1, 2 as c2, 'a' as c3, 'b' as c4, 'c' as c5")

      val maxValue = Long.MaxValue
      val minValue = Long.MinValue

      val dateStringMin = "2015-01-01 08:00:00"
      val dateStringMax = "2015-01-01 11:00:00"
      var format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateTimeStampMin = format.parse(dateStringMin).getTime
      val dateTimeStampMax = format.parse(dateStringMax).getTime
      format = new SimpleDateFormat("yyyy-MM-dd")
      val dateString = "2015-01-01"
      val date = format.parse(dateString)

      val functions =
        s"""
          |select
          |    least(c4, c3, c5),
          |    least(c1, c2, 1),
          |    least(c1, c2, (-1)),
          |    least(c4, c5, c3, c3, 'a'),
          |    least(null, null),
          |    least(c4, c3, c5, null),
          |    least((-1.0), 2.5),
          |    least((-1.0), 2),
          |    least(CAST(-1.0 AS FLOAT), CAST(2.5 AS FLOAT)),
          |    least(cast(1 as byte), cast(2 as byte)),
          |    least('abc', 'aaaa'),
          |    least(true, false),
          |    least(cast("2015-01-01" as date), cast("2015-07-01" as date)),
          |    least(${dateTimeStampMin}, ${dateTimeStampMax}),
          |    least(${minValue}, ${maxValue})
          |from
          |    test_least
        """.stripMargin

      checkSparkAnswerAndOperator(functions)
    }
  }

  test("test function greatest") {
    withTable("t1") {
      sql(
        "create table t1 using parquet as select 1 as c1, 2 as c2, 'a' as c3, 'b' as c4, 'c' as c5")

      val longMax = Long.MaxValue
      val longMin = Long.MinValue
      val dateStringMin = "2015-01-01 08:00:00"
      val dateStringMax = "2015-01-01 11:00:00"
      var format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateTimeStampMin = format.parse(dateStringMin).getTime
      val dateTimeStampMax = format.parse(dateStringMax).getTime
      format = new SimpleDateFormat("yyyy-MM-dd")
      val dateString = "2015-07-01"
      val date = format.parse(dateString)

      val functions =
        s"""
          |select
          |    greatest(c3, c4, c5),
          |    greatest(c2, c1),
          |    greatest(c1, c2, 2),
          |    greatest(c4, c5, c3, 'ccc'),
          |    greatest(null, null),
          |    greatest(c3, c4, c5, null),
          |    greatest((-1.0), 2.5),
          |    greatest((-1), 2),
          |    greatest(CAST(-1.0 AS FLOAT), CAST(2.5 AS FLOAT)),
          |    greatest(${longMax}, ${longMin}),
          |    greatest(cast(1 as byte), cast(2 as byte)),
          |    greatest(cast(1 as short), cast(2 as short)),
          |    greatest("abc", "aaaa"),
          |    greatest(true, false),
          |    greatest(
          |        cast("2015-01-01" as date),
          |        cast("2015-07-01" as date)
          |    ),
          |    greatest(
          |        ${dateTimeStampMin},
          |        ${dateTimeStampMax}
          |    )
          |from
          |    t1
        """.stripMargin

      checkSparkAnswerAndOperator(functions)
    }
  }

  test("test function FindInSet") {
    withTable("t1") {
      sql(
        "create table t1_find_in_set using parquet as select 'ab' as a, 'b' as b, '' as c, 'def' as d")

      val functions =
        """
          |select
          |   find_in_set(a, 'ab'),
          |   find_in_set(b, 'a,b'),
          |   find_in_set(a, 'abc,b,ab,c,def'),
          |   find_in_set(a, 'ab,abc,b,ab,c,def'),
          |   find_in_set(a, ',,,ab,abc,b,ab,c,def'),
          |   find_in_set(c, ',ab,abc,b,ab,c,def'),
          |   find_in_set(a, '数据砖头,abc,b,ab,c,def'),
          |   find_in_set(d, '数据砖头,abc,b,ab,c,def'),
          |   find_in_set(d, null)
          |from t1_find_in_set
        """.stripMargin

      checkSparkAnswerAndOperator(functions)
    }
  }

  test("test function IsNaN") {
    withTable("t1") {
      sql("""
          |create table test_is_nan using parquet as select
          |  cast('NaN' as double) as c1,
          |  cast('NaN' as float) as c2,
          |  cast(null as double) as c3,
          |  cast(null as double) as c4,
          |  cast(5.5 as float) as c5,
          |  cast(null as float) as c6
          |""".stripMargin)
      val functions =
        """
          |select
          |    isnan(c1),
          |    isnan(c2),
          |    isnan(c3),
          |    isnan(c4),
          |    isnan(c5),
          |    isnan(c6)
          |from
          |    test_is_nan
        """.stripMargin

      checkSparkAnswerAndOperator(functions)
    }
  }

  test("test function nvl2") {
    withTable("t1") {
      sql(s"""CREATE TABLE t1 USING PARQUET AS SELECT
             |'X'                      AS str_val,
             |100                      AS int_val,
             |array(1,2,3)             AS arr_val,
             |CAST(NULL AS STRING)     AS null_str,
             |CAST(NULL AS INT)        AS null_int
             |""".stripMargin)

      val sqlStr = s"""SELECT
                      |nvl2(null_int, int_val, 999)          AS int_only,
                      |nvl2(1,  str_val, int_val)            AS has_str,
                      |nvl2(null_int, int_val, str_val)      AS str_in_false,
                      |nvl2(1,  arr_val, array(888))         AS has_array,
                      |nvl2(null_int, null_str,  null_str)   AS all_null
                      |FROM  t1""".stripMargin

      checkSparkAnswerAndOperator(sqlStr)
    }
  }

  test("test function nvl") {
    withTable("t1") {
      sql(
        "create table t1 using parquet as select 'base'" +
          " as base, 3 as exponent")
      val functions =
        """
          |select
          |  nvl(null, base), base, nvl(4, exponent)
          |from t1
                    """.stripMargin

      checkSparkAnswerAndOperator(functions)
    }
  }

  test("test function Levenshtein") {
    withTable("t1") {
      sql(
        "create table test_levenshtein using parquet as select '' as a, 'abc' as b, 'kitten' as c, 'frog' as d, '千世' as i, '世界千世' as j")
      val functions =
        """
          |select
          |   levenshtein(null, a),
          |   levenshtein(a, null),
          |   levenshtein(a, a),
          |   levenshtein(b, b),
          |   levenshtein(c, 'sitting'),
          |   levenshtein(d, 'fog'),
          |   levenshtein(i, 'fog'),
          |   levenshtein(j, '大a界b')
          |from test_levenshtein
        """.stripMargin

      checkSparkAnswerAndOperator(functions)
    }
  }

  test("upper and lower functions") {
    withSQLConf() {
      withTable("t1") {
        sql(s"CREATE TABLE t1(id INT, name STRING) USING parquet")
        sql(s"""
             |INSERT INTO t1 VALUES
             | (1, 'fooBar'),
             | (2, 'foo Bar foo-bar FOO-BAR foO-barR'),
             | (3, 'straße'),
             | (4, 'CAFÉ'),
             | (5, '世界'),
             | (6, '世 界'),
             | (7, ''),
             | (8, NULL)
            """.stripMargin)

        checkSparkAnswerAndOperator(
          s"SELECT id, name, UPPER(name) AS up, LOWER(name) AS low FROM t1")
      }
    }
  }
}
