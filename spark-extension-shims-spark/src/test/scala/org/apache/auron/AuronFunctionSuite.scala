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
import org.apache.spark.sql.internal.SQLConf

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

  test("dayofweek function") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      withTable("t1") {
        sql("create table t1(c1 date, c2 timestamp) using parquet")
        sql("""
            |insert into t1 values
            |  (date'2009-07-30', timestamp'2009-07-30 12:34:56'),
            |  (date'2024-02-29', timestamp'2024-02-29 23:59:59'),
            |  (null, null)
            |""".stripMargin)

        // DATE column
        checkSparkAnswerAndOperator("select dayofweek(c1) from t1 where c1 is not null")

        // NULL DATE input should return NULL
        checkSparkAnswerAndOperator("select dayofweek(c1) from t1 where c1 is null")

        // TIMESTAMP column
        checkSparkAnswerAndOperator("select dayofweek(c2) from t1 where c2 is not null")

        // NULL TIMESTAMP input should return NULL
        checkSparkAnswerAndOperator("select dayofweek(c2) from t1 where c2 is null")
      }
    }
  }

  test("date-part functions with non-UTC timezone") {
    withTable("t1") {
      sql("create table t1(c1 timestamp) using parquet")
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
        sql("insert into t1 values(timestamp'2021-01-04 04:30:00')")
      }
      withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/New_York") {
        checkSparkAnswerAndOperator(
          "select year(c1), month(c1), dayofmonth(c1), dayofweek(c1), quarter(c1) from t1")
      }
    }
  }

  test("date-part functions with date input unchanged across timezones") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "Asia/Shanghai") {
      withTable("t1") {
        sql("create table t1(c1 date) using parquet")
        sql("insert into t1 values(date'2021-01-04')")
        checkSparkAnswerAndOperator(
          "select year(c1), month(c1), dayofmonth(c1), dayofweek(c1), quarter(c1) from t1")
      }
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
    withSQLConf("spark.auron.udf.singleChildFallback.enabled" -> "true") {
      withTable("t1") {
        sql("create table t1(c1 string) using parquet")
        sql("insert into t1 values('Auron Spark SQL')")
        checkSparkAnswerAndOperator("select regexp_extract(c1, '^A(.*)L$', 1) from t1")
      }
    }
  }

  test("weekofyear function") {
    withSQLConf("spark.sql.session.timeZone" -> "America/Los_Angeles") {
      withTable("t1") {
        sql(
          "create table t1(c1 date, c2 date, c3 date, c4 date, c5 timestamp, c6 string) using parquet")
        sql("""insert into t1 values (
            |  date'2009-07-30',
            |  date'1980-12-31',
            |  date'2016-01-01',
            |  date'2017-01-01',
            |  timestamp'2016-01-03 23:30:00',
            |  '2016-01-01'
            |)""".stripMargin)

        val query =
          """select
            |  weekofyear(c1),
            |  weekofyear(c2),
            |  weekofyear(c3),
            |  weekofyear(c4),
            |  weekofyear(c5),
            |  weekofyear(c6)
            |from t1
            |""".stripMargin
        checkSparkAnswerAndOperator(query)
      }
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

  test("map_concat function") {
    withTable("t1") {
      sql(
        "create table t1(c1 map<string, int>, c2 map<string, int>, c3 map<string, int>) using parquet")
      sql("""
          |insert into t1 values
          |  (map('a', 1), map('b', 2), map('c', 3)),
          |  (map('d', 4), map('e', 5), map('f', 6)),
          |  (null, map('x', 10), map('f', 20))
          |""".stripMargin)
      checkSparkAnswerAndOperator("select map_concat(c1, c2, c3) from t1")
    }
  }

  test("map_from_arrays function") {
    withTable("t1") {
      sql("create table t1(c1 array<string>, c2 array<int>) using parquet")
      sql("""
          |insert into t1 values
          |  (array('a', 'b'), array(1, 2)),
          |  (array('x'), array(10)),
          |  (null, array(20)),
          |  (array('m', 'n'), array(null, 30))
          |""".stripMargin)
      checkSparkAnswerAndOperator("select map_from_arrays(c1, c2) from t1")
    }
  }

  test("map_from_arrays rejects null keys") {
    withTable("t1") {
      sql("create table t1(c1 array<string>, c2 array<int>) using parquet")
      sql("insert into t1 values (array('a', cast(null as string)), array(1, 2))")
      val df = sql("select map_from_arrays(c1, c2) from t1")
      val err = intercept[Exception] {
        df.collect()
      }
      assert(allCauseMessages(err).toLowerCase.contains("null map keys"))
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

  test("map_from_entries function") {
    withTable("t1") {
      sql("create table t1(c1 array<struct<k:string,v:int>>) using parquet")
      sql("""
          |insert into t1 values
          |  (array(named_struct('k', 'a', 'v', 1), named_struct('k', 'b', 'v', 2))),
          |  (array(named_struct('k', 'x', 'v', 10))),
          |  (cast(null as array<struct<k:string,v:int>>)),
          |  (array(cast(null as struct<k:string,v:int>), named_struct('k', 'z', 'v', 30))),
          |  (array(named_struct('k', 'm', 'v', cast(null as int))))
          |""".stripMargin)
      checkSparkAnswerAndOperator("select map_from_entries(c1) from t1")
    }
  }

  test("map_from_entries rejects null keys") {
    withTable("t1") {
      sql("create table t1(c1 array<struct<k:string,v:int>>) using parquet")
      sql("""
          |insert into t1 values
          |  (array(named_struct('k', 'a', 'v', 1), named_struct('k', cast(null as string), 'v', 2)))
          |""".stripMargin)
      val df = sql("select map_from_entries(c1) from t1")
      val err = intercept[Exception] {
        df.collect()
      }
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      plan
        .collectFirst { case op if !isNativeOrPassThrough(op) => op }
        .foreach { op =>
          fail(s"""
               |Found non-native operator: ${op.nodeName}
               |plan:
               |${plan}""".stripMargin)
        }
      assert(allCauseMessages(err).toLowerCase.contains("null map key"))
    }
  }

  test("map_from_entries duplicate keys") {
    withTable("t1") {
      sql("create table t1(c1 array<struct<k:string,v:int>>) using parquet")
      sql("""
          |insert into t1 values
          |  (array(named_struct('k', 'a', 'v', 1), named_struct('k', 'a', 'v', 2)))
          |""".stripMargin)
      val df = sql("select map_from_entries(c1) from t1")
      val err = intercept[Exception] {
        df.collect()
      }
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      plan
        .collectFirst { case op if !isNativeOrPassThrough(op) => op }
        .foreach { op =>
          fail(s"""
               |Found non-native operator: ${op.nodeName}
               |plan:
               |${plan}""".stripMargin)
        }
      assert(allCauseMessages(err).toLowerCase.contains("duplicate key"))
    }
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

  test("map_from_entries last win dedup policy") {
    withTable("t1") {
      sql("create table t1(c1 array<struct<k:string,v:int>>) using parquet")
      sql("""
          |insert into t1 values
          |  (array(
          |    named_struct('k', 'a', 'v', 1),
          |    named_struct('k', 'b', 'v', 2),
          |    named_struct('k', 'a', 'v', 3)))
          |""".stripMargin)
      withSQLConf(
        SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
        checkSparkAnswerAndOperator("select map_from_entries(c1) from t1")
      }
    }
  }

  test("str_to_map function") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("""
          |insert into t1 values
          |  ('a:1,b:2'),
          |  ('a:1:2,b'),
          |  (null)
          |""".stripMargin)
      checkSparkAnswerAndOperator("select str_to_map(c1) from t1")
    }
  }

  test("str_to_map regex delimiters") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values ('a::1,,b:::2')")
      checkSparkAnswerAndOperator("select str_to_map(c1, ',+', ':+') from t1")
    }
  }

  test("str_to_map Java regex delimiters") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values ('a:1,b:2,c:3')")
      checkSparkAnswerAndOperator("select str_to_map(c1, ',(?=b:|c:)', ':') from t1")
    }
  }

  test("str_to_map duplicate keys") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values ('a:1,a:2')")
      val df = sql("select str_to_map(c1) from t1")
      val err = intercept[Exception] {
        df.collect()
      }
      val plan = stripAQEPlan(df.queryExecution.executedPlan)
      plan
        .collectFirst { case op if !isNativeOrPassThrough(op) => op }
        .foreach { op =>
          fail(s"""
               |Found non-native operator: ${op.nodeName}
               |plan:
               |${plan}""".stripMargin)
        }
      assert(allCauseMessages(err).toLowerCase.contains("duplicate key"))
    }
  }

  test("str_to_map last win dedup policy") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values ('a:1,b:2,a:3')")
      withSQLConf(
        SQLConf.MAP_KEY_DEDUP_POLICY.key -> SQLConf.MapKeyDedupPolicy.LAST_WIN.toString) {
        checkSparkAnswerAndOperator("select str_to_map(c1) from t1")
      }
    }
  }

  test("acosh null propagation") {
    withTable("t1") {
      sql("create table t1(c1 double) using parquet")
      sql("insert into t1 values(null), (0.0), (1.0), (2.0)")
      checkSparkAnswerAndOperator("select acosh(c1) from t1")
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
                      |nvl2(1,  str_val, cast(int_val AS STRING))            AS has_str,
                      |nvl2(null_int, cast(int_val AS STRING), str_val)      AS str_in_false,
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

  test("bround function with varying scales for doublePi") {
    withTable("t1") {
      val doublePi: Double = math.Pi
      sql(s"CREATE TABLE t1(c1 DOUBLE) USING parquet")
      sql(s"INSERT INTO t1 VALUES($doublePi)")

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
        val df = sql(s"SELECT bround(c1, $scale) FROM t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("bround function with varying scales for floatPi") {
    withTable("t1") {
      val floatPi: Float = 3.1415f
      sql(s"CREATE TABLE t1(c1 FLOAT) USING parquet")
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
        val df = sql(s"SELECT bround(c1, $scale) FROM t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("bround function with varying scales for shortPi") {
    withTable("t1") {
      val shortPi: Short = 31415
      sql(s"CREATE TABLE t1(c1 SMALLINT) USING parquet")
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
        val df = sql(s"SELECT bround(c1, $scale) FROM t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("bround function with varying scales for intPi") {
    withTable("t1") {
      val intPi: Int = 314159265
      sql(s"CREATE TABLE t1(c1 INT) USING parquet")
      sql(s"INSERT INTO t1 VALUES($intPi)")

      val scales = -6 to 6
      val expectedResults = Map(
        -6 -> 314000000,
        -5 -> 314200000,
        -4 -> 314160000,
        -3 -> 314159000,
        -2 -> 314159300,
        -1 -> 314159260,
        0 -> 314159265,
        1 -> 314159265,
        2 -> 314159265,
        3 -> 314159265,
        4 -> 314159265,
        5 -> 314159265,
        6 -> 314159265)

      scales.foreach { scale =>
        val df = sql(s"SELECT bround(c1, $scale) FROM t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("bround function with varying scales for longPi") {
    withTable("t1") {
      val longPi: Long = 31415926535897932L
      sql(s"CREATE TABLE t1(c1 BIGINT) USING parquet")
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
        val df = sql(s"SELECT bround(c1, $scale) FROM t1")
        val expected = expectedResults(scale)
        checkAnswer(df, Seq(Row(expected)))
      }
    }
  }

  test("bround function for null values") {
    withTable("t1") {
      sql("CREATE TABLE t1(c1 DOUBLE) USING parquet")
      sql("INSERT INTO t1 VALUES(NULL)")

      val scales = -6 to 6
      scales.foreach { scale =>
        val df = sql(s"SELECT bround(c1, $scale) FROM t1")
        checkAnswer(df, Seq(Row(null)))
      }
    }
  }

  test("ascii function") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values('A'), ('abc'), (''), (null)")
      checkSparkAnswerAndOperator("select ascii(c1) from t1")
    }
  }

  test("bit_length function") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values('hello'), (''), (null), ('caf\\u00e9')")
      checkSparkAnswerAndOperator("select bit_length(c1) from t1")
    }
  }

  test("chr function") {
    withTable("t1") {
      sql("create table t1(c1 bigint) using parquet")
      sql("insert into t1 values(65), (97), (48), (null)")
      checkSparkAnswerAndOperator("select chr(c1) from t1")
    }
  }

  test("translate function") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values('AaBbCc'), ('hello'), (''), (null)")
      checkSparkAnswerAndOperator("select translate(c1, 'ABC', 'xyz') from t1")
    }
  }

  test("replace function") {
    withTable("t1") {
      sql("create table t1(c1 string) using parquet")
      sql("insert into t1 values('hello world'), ('aaa'), (''), (null)")
      checkSparkAnswerAndOperator("select replace(c1, 'world', 'spark') from t1")
      checkSparkAnswerAndOperator("select replace(c1, 'a', '') from t1")
    }
  }

  test("date_trunc function") {
    withSQLConf(SQLConf.SESSION_LOCAL_TIMEZONE.key -> "UTC") {
      withTable("t1") {
        sql("create table t1(c1 timestamp) using parquet")
        sql("""insert into t1 values
            |  (timestamp'2024-03-15 14:30:45'),
            |  (timestamp'2024-12-31 23:59:59'),
            |  (null)
            |""".stripMargin)
        checkSparkAnswerAndOperator("select date_trunc('year', c1) from t1")
        checkSparkAnswerAndOperator("select date_trunc('month', c1) from t1")
        checkSparkAnswerAndOperator("select date_trunc('day', c1) from t1")
        checkSparkAnswerAndOperator("select date_trunc('hour', c1) from t1")
      }
    }
  }

  test("months_between function") {
    withSQLConf("spark.sql.session.timeZone" -> "UTC") {
      withTable("t1") {
        sql("""
              |create table t1(
              |  same_day_of_month_later_ts timestamp,
              |  same_day_of_month_earlier_ts timestamp,
              |  last_day_of_month_later_dt date,
              |  last_day_of_month_earlier_dt date,
              |  fractional_later_ts timestamp,
              |  fractional_earlier_ts timestamp,
              |  null_ts timestamp
              |) using parquet
              |""".stripMargin)
        sql("""
              |insert into t1 values (
              |  timestamp'2024-03-15 23:59:59',
              |  timestamp'2024-01-15 00:00:00',
              |  date'2024-02-29',
              |  date'2024-01-31',
              |  timestamp'2024-03-02 12:00:00',
              |  timestamp'2024-01-01 00:00:00',
              |  null
              |)
              |""".stripMargin)
        val query =
          """
              |select
              |  months_between(same_day_of_month_later_ts, same_day_of_month_earlier_ts)
              |    as same_day_of_month_ignores_time_positive,
              |  months_between(last_day_of_month_later_dt, last_day_of_month_earlier_dt)
              |    as last_day_of_month_ignores_time,
              |  months_between(fractional_later_ts, fractional_earlier_ts)
              |    as rounded_fractional_positive,
              |  months_between(same_day_of_month_earlier_ts, same_day_of_month_later_ts)
              |    as same_day_of_month_ignores_time_negative,
              |  months_between(fractional_earlier_ts, fractional_later_ts)
              |    as rounded_fractional_negative,
              |  months_between(fractional_later_ts, fractional_earlier_ts, false)
              |    as unrounded_fractional_positive,
              |  months_between(null_ts, fractional_earlier_ts)
              |    as null_propagation
              |from t1
              |""".stripMargin
        checkSparkAnswerAndOperator(query)
      }
    }
  }
}
