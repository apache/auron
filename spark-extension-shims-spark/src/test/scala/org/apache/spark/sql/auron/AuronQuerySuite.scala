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

import java.util.Locale

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row

import org.apache.auron.util.AuronTestUtils

class AuronQuerySuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {
  import testImplicits._

  test("test partition path has url encoded character") {
    withTable("t1") {
      sql(
        "create table t1 using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val df = sql("select * from t1")
      checkAnswer(df, Seq(Row(1, 2, "test test")))
    }
  }

  test("empty output in bnlj") {
    withTable("t1", "t2") {
      sql("create table t1 using parquet as select 1 as c1, 2 as c2")
      sql("create table t2 using parquet as select 1 as c1, 3 as c3")
      val df = sql("select 1 from t1 left join t2")
      checkAnswer(df, Seq(Row(1)))
    }
  }

  test("test filter with year function") {
    withTable("t1") {
      sql("create table t1 using parquet as select '2024-12-18' as event_time")
      checkAnswer(
        sql("""
            |select year, count(*)
            |from (select event_time, year(event_time) as year from t1) t
            |where year <= 2024
            |group by year
            |""".stripMargin),
        Seq(Row(2024, 1)))
    }
  }

  test("test select multiple spark ext functions with the same signature") {
    withTable("t1") {
      sql("create table t1 using parquet as select '2024-12-18' as event_time")
      checkAnswer(sql("select year(event_time), month(event_time) from t1"), Seq(Row(2024, 12)))
    }
  }

  test("test parquet/orc format table with complex data type") {
    def createTableStatement(format: String): String = {
      s"""create table test_with_complex_type(
         |id bigint comment 'pk',
         |m map<string, string> comment 'test read map type',
         |l array<string> comment 'test read list type',
         |s string comment 'string type'
         |) USING $format
         |""".stripMargin
    }
    Seq("parquet", "orc").foreach(format =>
      withTable("test_with_complex_type") {
        sql(createTableStatement(format))
        sql(
          "insert into test_with_complex_type select 1 as id, map('zero', '0', 'one', '1') as m, array('test','auron') as l, 'auron' as s")
        checkAnswer(
          sql("select id,l,m from test_with_complex_type"),
          Seq(Row(1, ArrayBuffer("test", "auron"), Map("one" -> "1", "zero" -> "0"))))
      })
  }

  test("binary type in range partitioning") {
    withTable("t1", "t2") {
      sql("create table t1(c1 binary, c2 int) using parquet")
      sql("insert into t1 values (cast('test1' as binary), 1), (cast('test2' as binary), 2)")
      val df = sql("select c2 from t1 order by c1")
      checkAnswer(df, Seq(Row(1), Row(2)))
    }
  }

  test("repartition over MapType") {
    withTable("t_map") {
      sql("create table t_map using parquet as select map('a', '1', 'b', '2') as data_map")
      val df = sql("SELECT /*+ repartition(10) */ data_map FROM t_map")
      checkAnswer(df, Seq(Row(Map("a" -> "1", "b" -> "2"))))
    }
  }

  test("repartition over MapType with ArrayType") {
    withTable("t_map_struct") {
      sql(
        "create table t_map_struct using parquet as select named_struct('m', map('x', '1')) as data_struct")
      val df = sql("SELECT /*+ repartition(10) */ data_struct FROM t_map_struct")
      checkAnswer(df, Seq(Row(Row(Map("x" -> "1")))))
    }
  }

  test("repartition over ArrayType with MapType") {
    withTable("t_array_map") {
      sql("""
          |create table t_array_map using parquet as
          |select array(map('k1', 1, 'k2', 2), map('k3', 3)) as array_of_map
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ array_of_map FROM t_array_map")
      checkAnswer(df, Seq(Row(Seq(Map("k1" -> 1, "k2" -> 2), Map("k3" -> 3)))))
    }
  }

  test("repartition over StructType with MapType") {
    withTable("t_struct_map") {
      sql("""
          |create table t_struct_map using parquet as
          |select named_struct('id', 101, 'metrics', map('ctr', 0.123d, 'cvr', 0.045d)) as user_metrics
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ user_metrics FROM t_struct_map")
      checkAnswer(df, Seq(Row(Row(101, Map("ctr" -> 0.123, "cvr" -> 0.045)))))
    }
  }

  test("repartition over MapType with StructType") {
    withTable("t_map_struct_value") {
      sql("""
          |create table t_map_struct_value using parquet as
          |select map(
          |  'item1', named_struct('count', 3, 'score', 4.5d),
          |  'item2', named_struct('count', 7, 'score', 9.1d)
          |) as map_struct_value
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ map_struct_value FROM t_map_struct_value")
      checkAnswer(df, Seq(Row(Map("item1" -> Row(3, 4.5), "item2" -> Row(7, 9.1)))))
    }
  }

  test("repartition over nested MapType") {
    withTable("t_nested_map") {
      sql("""
          |create table t_nested_map using parquet as
          |select map(
          |  'outer1', map('inner1', 10, 'inner2', 20),
          |  'outer2', map('inner3', 30)
          |) as nested_map
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ nested_map FROM t_nested_map")
      checkAnswer(
        df,
        Seq(Row(
          Map("outer1" -> Map("inner1" -> 10, "inner2" -> 20), "outer2" -> Map("inner3" -> 30)))))
    }
  }

  test("repartition over ArrayType of StructType with MapType") {
    withTable("t_array_struct_map") {
      sql("""
          |create table t_array_struct_map using parquet as
          |select array(
          |  named_struct('name', 'user1', 'features', map('f1', 1.0d, 'f2', 2.0d)),
          |  named_struct('name', 'user2', 'features', map('f3', 3.5d))
          |) as user_feature_array
          |""".stripMargin)
      val df = sql("SELECT /*+ repartition(10) */ user_feature_array FROM t_array_struct_map")
      checkAnswer(
        df,
        Seq(
          Row(
            Seq(Row("user1", Map("f1" -> 1.0f, "f2" -> 2.0f)), Row("user2", Map("f3" -> 3.5f))))))
    }
  }

  test("log function with negative input") {
    withTable("t1") {
      sql("create table t1 using parquet as select -1 as c1")
      val df = sql("select ln(c1) from t1")
      checkAnswer(df, Seq(Row(null)))
    }
  }

  test("floor function with long input") {
    withTable("t1") {
      sql("create table t1 using parquet as select 1L as c1, 2.2 as c2")
      val df = sql("select floor(c1), floor(c2) from t1")
      checkAnswer(df, Seq(Row(1, 2)))
    }
  }

  test("SPARK-32234 read ORC table with column names all starting with '_col'") {
    withTable("test_hive_orc_impl") {
      spark.sql(s"""
           | CREATE TABLE test_hive_orc_impl
           | (_col1 INT, _col2 STRING, _col3 INT)
           | USING ORC
               """.stripMargin)
      spark.sql(s"""
           | INSERT INTO
           | test_hive_orc_impl
           | VALUES(9, '12', 2020)
               """.stripMargin)

      val df = spark.sql("SELECT _col2 FROM test_hive_orc_impl")
      checkAnswer(df, Row("12"))
    }
  }

  test("SPARK-32864: Support ORC forced positional evolution") {
    if (AuronTestUtils.isSparkV32OrGreater) {
      Seq(true, false).foreach { forcePositionalEvolution =>
        withEnvConf(
          AuronConf.ORC_FORCE_POSITIONAL_EVOLUTION.key -> forcePositionalEvolution.toString) {
          withTempPath { f =>
            val path = f.getCanonicalPath
            Seq[(Integer, Integer)]((1, 2), (3, 4), (5, 6), (null, null))
              .toDF("c1", "c2")
              .write
              .orc(path)
            val correctAnswer = Seq(Row(1, 2), Row(3, 4), Row(5, 6), Row(null, null))
            checkAnswer(spark.read.orc(path), correctAnswer)

            withTable("t") {
              sql(s"CREATE EXTERNAL TABLE t(c3 INT, c2 INT) USING ORC LOCATION '$path'")

              val expected = if (forcePositionalEvolution) {
                correctAnswer
              } else {
                Seq(Row(null, 2), Row(null, 4), Row(null, 6), Row(null, null))
              }

              checkAnswer(spark.table("t"), expected)
            }
          }
        }
      }
    }
  }

  test("SPARK-32864: Support ORC forced positional evolution with partitioned table") {
    if (AuronTestUtils.isSparkV32OrGreater) {
      Seq(true, false).foreach { forcePositionalEvolution =>
        withEnvConf(
          AuronConf.ORC_FORCE_POSITIONAL_EVOLUTION.key -> forcePositionalEvolution.toString) {
          withTempPath { f =>
            val path = f.getCanonicalPath
            Seq[(Integer, Integer, Integer)]((1, 2, 1), (3, 4, 2), (5, 6, 3), (null, null, 4))
              .toDF("c1", "c2", "p")
              .write
              .partitionBy("p")
              .orc(path)
            val correctAnswer = Seq(Row(1, 2, 1), Row(3, 4, 2), Row(5, 6, 3), Row(null, null, 4))
            checkAnswer(spark.read.orc(path), correctAnswer)

            withTable("t") {
              sql(s"""
                     |CREATE TABLE t(c3 INT, c2 INT)
                     |USING ORC
                     |PARTITIONED BY (p int)
                     |LOCATION '$path'
                     |""".stripMargin)
              sql("MSCK REPAIR TABLE t")
              val expected = if (forcePositionalEvolution) {
                correctAnswer
              } else {
                Seq(Row(null, 2, 1), Row(null, 4, 2), Row(null, 6, 3), Row(null, null, 4))
              }

              checkAnswer(spark.table("t"), expected)
            }
          }
        }
      }
    }
  }

  test("test filter with quarter function") {
    withTable("t1") {
      sql("""
          |create table t1 using parquet as
          |select '2024-02-10' as event_time
          |union all select '2024-04-11'
          |union all select '2024-07-20'
          |union all select '2024-12-18'
          |""".stripMargin)

      checkAnswer(
        sql("""
            |select q, count(*)
            |from (select event_time, quarter(event_time) as q from t1) t
            |where q <= 3
            |group by q
            |order by q
            |""".stripMargin),
        Seq(Row(1, 1), Row(2, 1), Row(3, 1)))
    }
  }

  test("lpad/rpad basic") {
    Seq(
      ("select lpad('abc', 5, '*')", Row("**abc")),
      ("select rpad('abc', 5, '*')", Row("abc**")),
      ("select lpad('spark', 2, '0')", Row("sp")),
      ("select rpad('spark', 2, '0')", Row("sp")),
      ("select lpad('9', 5, 'ab')", Row("abab9")),
      ("select rpad('9', 5, 'ab')", Row("9abab")),
      ("select lpad('hi', 5, '')", Row("hi")),
      ("select rpad('hi', 5, '')", Row("hi")),
      ("select lpad('x', 0, 'a')", Row("")),
      ("select rpad('x', -1, 'a')", Row("")),
      ("select lpad('Z', 3, '++')", Row("++Z")),
      ("select rpad('Z', 3, 'AB')", Row("ZAB"))).foreach { case (q, expected) =>
      checkAnswer(sql(q), Seq(expected))
    }
  }

  test("reverse basic") {
    Seq(
      ("select reverse('abc')", Row("cba")),
      ("select reverse('spark')", Row("kraps")),
      ("select reverse('hello world')", Row("dlrow olleh")),
      ("select reverse('12345')", Row("54321")),
      ("select reverse('a')", Row("a")), // Edge case: single character
      ("select reverse('')", Row("")), // Edge case: empty string
      ("select reverse('hello' || ' world')", Row("dlrow olleh"))).foreach { case (q, expected) =>
      checkAnswer(sql(q), Seq(expected))
    }
  }

  test("initcap basic") {
    Seq(
      ("select initcap('spark sql')", Row("Spark Sql")),
      ("select initcap('SPARK')", Row("Spark")),
      ("select initcap('sPaRk')", Row("Spark")),
      ("select initcap('')", Row("")),
      ("select initcap(null)", Row(null))).foreach { case (q, expected) =>
      checkAnswer(sql(q), Seq(expected))
    }
  }

  test("initcap: word boundaries and punctuation") {
    Seq(
      ("select initcap('hello world')", Row("Hello World")),
      ("select initcap('hello_world')", Row("Hello_world")),
      ("select initcap('über-alles')", Row("Über-alles")),
      ("select initcap('foo.bar/baz')", Row("Foo.bar/baz")),
      ("select initcap('v2Ray is COOL')", Row("V2ray Is Cool")),
      ("select initcap('rock''n''roll')", Row("Rocknroll")),
      ("select initcap('hi\\tthere')", Row("Hi\tthere")),
      ("select initcap('hi\\nthere')", Row("Hi\nthere"))).foreach { case (q, expected) =>
      checkAnswer(sql(q), Seq(expected))
    }
  }

  test("initcap: mixed cases and edge cases") {
    Seq(
      ("select initcap('a1b2 c3D4')", Row("A1b2 C3d4")),
      ("select initcap('---abc---')", Row("---abc---")),
      ("select initcap('  multiple   spaces ')", Row("  Multiple   Spaces "))).foreach {
      case (q, expected) =>
        checkAnswer(sql(q), Seq(expected))
    }
  }

  test("radians/degrees scalar constants") {
    checkAnswer(
      sql("""
          |select
          |  radians(0.0)  as r0,
          |  degrees(0.0)  as d0,
          |  radians(180.0) as r180,
          |  degrees(pi()) as dpi
          |""".stripMargin),
      Seq(Row(0.0, 0.0, Math.PI, 180.0)))
  }

  test("radians on column doubles") {
    withTable("t_rad") {
      sql("create table t_rad(c double) using parquet")
      sql("insert into t_rad values (0.0), (30.0), (45.0), (90.0), (180.0), (-45.0)")
      val df = sql("select radians(c) from t_rad")
      checkAnswer(
        df,
        Seq(
          Row(0.0),
          Row(Math.toRadians(30.0)),
          Row(Math.toRadians(45.0)),
          Row(Math.toRadians(90.0)),
          Row(Math.toRadians(180.0)),
          Row(Math.toRadians(-45.0))))
    }
  }

  test("degrees on literals (with tolerance, no temp table)") {
    val df = sql("""
        |SELECT degrees(c) AS deg
        |FROM VALUES
        |  (0.0D),
        |  (PI()/6),
        |  (PI()/4),
        |  (PI()/2),
        |  (PI()),
        |  (-PI()/4)
        |AS t(c)
        |""".stripMargin)

    val actual = df.collect().map(_.getDouble(0)).toSeq
    val expected = Seq(0.0, 30.0, 45.0, 90.0, 180.0, -45.0)
    val tol = 1e-12

    assert(actual.size == expected.size)
    actual.zip(expected).foreach { case (a, e) =>
      assert(math.abs(a - e) <= tol, s"expected ~$e, got $a")
    }
  }

  test("radians/degrees with nulls and empties") {
    val df = sql("""
        |select
        |  radians(c) as r,
        |  degrees(c) as d
        |from values
        |  (cast(null as double)),
        |  (0.0),
        |  (cast(null as int)),
        |  (180.0)
        |as t(c)
        |""".stripMargin)
    checkAnswer(
      df,
      Seq(Row(null, null), Row(0.0, 0.0), Row(null, null), Row(Math.PI, 10313.240312354817)))
  }

  test("radians/degrees with integral and decimal types - string compare via format_number") {
    withTable("t_mix") {
      sql("CREATE TABLE t_mix(ci INT, cl LONG, cf FLOAT, cd DECIMAL(10,2)) USING parquet")
      sql("INSERT INTO t_mix VALUES (30, 45, 60.0, 90.00)")

      val df = sql("""
          |SELECT
          |  format_number(radians(ci), 12)  AS r_ci,
          |  format_number(degrees(ci), 12)  AS d_ci,
          |  format_number(radians(cl), 12)  AS r_cl,
          |  format_number(degrees(cl), 12)  AS d_cl,
          |  format_number(radians(cf), 12)  AS r_cf,
          |  format_number(degrees(cf), 12)  AS d_cf,
          |  format_number(radians(cd), 12)  AS r_cd,
          |  format_number(degrees(cd), 12)  AS d_cd
          |FROM t_mix
          |""".stripMargin)

      def f(x: Double): String = String.format(Locale.US, "%,.12f", x)

      checkAnswer(
        df,
        Seq(
          Row(
            f(Math.toRadians(30.0)),
            f(Math.toDegrees(30.0)),
            f(Math.toRadians(45.0)),
            f(Math.toDegrees(45.0)),
            f(Math.toRadians(60.0)),
            f(Math.toDegrees(60.0)),
            f(Math.toRadians(90.0)),
            f(Math.toDegrees(90.0)))))
    }
  }

  test("sinh/cosh/tanh scalar basics") {
    checkAnswer(
      sql("select round(sinh(0.0), 9), round(cosh(0.0), 9), round(tanh(0.0), 9)"),
      Seq(Row(0.0, 1.0, 0.0)))

    checkAnswer(
      sql("""
        |select
        |  round(sinh(1.0), 9) as s1,
        |  round(cosh(1.0), 9) as c1,
        |  round(tanh(1.0), 9) as t1
        |""".stripMargin),
      Seq(Row(1.175201194, 1.543080635, 0.761594156)))

    checkAnswer(
      sql("""
        |select
        |  round(sinh(-1.25), 9) = -round(sinh(1.25), 9),
        |  round(cosh(-1.25), 9) =  round(cosh(1.25), 9),
        |  round(tanh(-1.25), 9) = -round(tanh(1.25), 9)
        |""".stripMargin),
      Seq(Row(true, true, true)))
  }

  test("sinh/cosh/tanh null propagation") {
    withTable("t_null") {
      sql("create table t_null(c double) using parquet")
      sql("insert into t_null values (null)")
      checkAnswer(sql("select sinh(c), cosh(c), tanh(c) from t_null"), Seq(Row(null, null, null)))
    }
  }

  test("acosh/asinh/atanh scalar basics (with tolerance)") {
    val tol = 1e-12

    checkAnswer(
      sql(
        s"select abs(acosh(1.0) - 0.0) < $tol, abs(asinh(0.0) - 0.0) < $tol, abs(atanh(0.0) - 0.0) < $tol"),
      Seq(Row(true, true, true)))

    checkAnswer(
      sql(s"""
           |select
           |  abs(asinh(1.0) - 0.881373587019543) < $tol,
           |  abs(acosh(2.0) - 1.3169578969248166) < $tol,
           |  abs(atanh(0.5) - 0.5493061443340549) < $tol
           |""".stripMargin),
      Seq(Row(true, true, true)))

    checkAnswer(
      sql(s"""
           |select
           |  abs(asinh(-1.25) + asinh(1.25)) < $tol as asinh_odd,
           |  abs(atanh(-0.8)  + atanh(0.8))  < $tol as atanh_odd
           |""".stripMargin),
      Seq(Row(true, true)))
  }

  test("acosh/asinh/atanh implicit cast from integers / decimals (rounded + tolerance)") {
    val tol = 1e-12
    checkAnswer(
      sql(s"""
           |select
           |  abs(round(asinh(2), 12) - 1.443635475179) < $tol              as asinh_int_ok,
           |  abs(round(atanh(0.5D), 12) - 0.549306144334) < $tol           as atanh_double_ok,
           |  abs(round(acosh(cast(2 as decimal(10,0))), 12) - 1.316957896925) < $tol as acosh_decimal_ok
           |""".stripMargin),
      Seq(Row(true, true, true)))
  }

  test("cot basic values & nulls") {
    val pi = math.Pi
    Seq(
      ("select round(cot(PI() / 4), 6)", Row(1.0)), // cot(π/4) = 1
      ("select round(cot(PI() / 3), 6)", Row(1.0 / math.tan(pi / 3))), // ≈ 0.577350
      ("select round(cot(-PI() / 4), 6)", Row(-1.0)), // cot(-π/4) = -1
      ("select round(cot(PI() / 6), 6)", Row(1.0 / math.tan(pi / 6))) // ≈ 1.732051
    ).foreach { case (q, Row(expected: Double)) =>
      val expectedRounded =
        BigDecimal(expected).setScale(6, BigDecimal.RoundingMode.HALF_UP).toDouble
      checkAnswer(sql(q), Seq(Row(expectedRounded)))
    }
    checkAnswer(sql("select cot(NULL)"), Seq(Row(null)))
  }

  test("atan2: axes, quadrants, and null semantics") {
    Seq(
      // x>0, y=0 → 0
      ("select round(atan2(0.0, 1.0), 6)", Row(0.000000)),
      // x<0, y=0 → π
      ("select round(atan2(0.0, -1.0), 6)", Row(3.141593)),
      // x=0, y>0 →  π/2
      ("select round(atan2(1.0, 0.0), 6)", Row(1.570796)),
      // x=0, y<0 → -π/2
      ("select round(atan2(-1.0, 0.0), 6)", Row(-1.570796))).foreach { case (q, expected) =>
      checkAnswer(sql(q), Seq(expected))
    }

    Seq(
      // Q1: (y=+1, x=+1) →  π/4
      ("select round(atan2(1.0, 1.0), 6)", Row(0.785398)),
      // Q2: (y=+1, x=-1) → 3π/4
      ("select round(atan2(1.0, -1.0), 6)", Row(2.356194)),
      // Q3: (y=-1, x=-1) → -3π/4
      ("select round(atan2(-1.0, -1.0), 6)", Row(-2.356194)),
      // Q4: (y=-1, x=+1) → -π/4
      ("select round(atan2(-1.0, 1.0), 6)", Row(-0.785398))).foreach { case (q, expected) =>
      checkAnswer(sql(q), Seq(expected))
    }

    Seq("select atan2(NULL, 1.0)", "select atan2(1.0, NULL)", "select atan2(NULL, NULL)")
      .foreach { q =>
        checkAnswer(sql(q), Seq(Row(null)))
      }
  }

  test("cbrt: literals and basic identities") {
    Seq(
      ("select cbrt(27.0D)", Row(3.0)),
      ("select cbrt(0.0D)", Row(0.0)),
      ("select cbrt(-8.0D)", Row(-2.0)),
      ("select round(cbrt(79.0D), 6)", Row(4.29084))).foreach { case (q, expected) =>
      checkAnswer(sql(q), Seq(expected))
    }
  }

  test("cbrt: wide range sanity") {
    val q =
      """
        |select
        |  round(cbrt(1e9D), 6),      -- 1000
        |  round(cbrt(1e-9D), 12)     -- 0.001
        |""".stripMargin
    checkAnswer(sql(q), Seq(Row(1000.000000, 0.001)))
  }

  test("log: column (from VALUES) incl. zero, negative, null") {
    val q =
      """
        |select log(x)
        |from values
        |  (1.0D),              -- 0.0
        |  (0.0D),              -- NULL
        |  (-1.0D),             -- NULL
        |  (NULL)               -- NULL
        |as t(x)
        |""".stripMargin
    val rows = sql(q).collect().toSeq

    assert(rows.length == 4)
    assert(rows(0).getDouble(0) == 0.0)
    assert(rows(1).get(0) == null)
    assert(rows(2).get(0) == null)
    assert(rows(3).get(0) == null)
  }

  test("log: rounding tolerance on typical set") {
    val q =
      """
        |select round(log(v), 6)
        |from values (2.0D), (3.0D), (10.0D) as t(v)
        |""".stripMargin
    checkAnswer(sql(q), Seq(Row(0.693147), Row(1.098612), Row(2.302585)))
  }
}
