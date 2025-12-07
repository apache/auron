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

import org.apache.spark.sql.{AuronQueryTest, Row}
import org.apache.spark.sql.auron.AuronConf

import org.apache.auron.util.AuronTestUtils

class AuronQuerySuite extends AuronQueryTest with BaseAuronSQLSuite with AuronSQLTestHelper {
  import testImplicits._

  test("test partition path has url encoded character") {
    withTable("t1") {
      sql(
        "create table t1 using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      checkSparkAnswerAndOperator("select * from t1")
    }
  }

  test("empty output in bnlj") {
    withTable("t1", "t2") {
      sql("create table t1 using parquet as select 1 as c1, 2 as c2")
      sql("create table t2 using parquet as select 1 as c1, 3 as c3")
      checkSparkAnswerAndOperator("select 1 from t1 left join t2")
    }
  }

  test("test filter with year function") {
    withTable("t1") {
      sql("create table t1 using parquet as select '2024-12-18' as event_time")
      checkSparkAnswerAndOperator(s"""
            |select year, count(*)
            |from (select event_time, year(event_time) as year from t1) t
            |where year <= 2024
            |group by year
            |""".stripMargin)
    }
  }

  test("test select multiple spark ext functions with the same signature") {
    withTable("t1") {
      sql("create table t1 using parquet as select '2024-12-18' as event_time")
      checkSparkAnswerAndOperator("select year(event_time), month(event_time) from t1")
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
        checkSparkAnswerAndOperator("select id,l,m from test_with_complex_type")
      })
  }

  test("binary type in range partitioning") {
    withTable("t1", "t2") {
      sql("create table t1(c1 binary, c2 int) using parquet")
      sql("insert into t1 values (cast('test1' as binary), 1), (cast('test2' as binary), 2)")
      checkSparkAnswerAndOperator("select c2 from t1 order by c1")
    }
  }

  test("repartition over MapType") {
    withTable("t_map") {
      sql("create table t_map using parquet as select map('a', '1', 'b', '2') as data_map")
      checkSparkAnswerAndOperator("SELECT /*+ repartition(10) */ data_map FROM t_map")
    }
  }

  test("repartition over MapType with ArrayType") {
    withTable("t_map_struct") {
      sql(
        "create table t_map_struct using parquet as select named_struct('m', map('x', '1')) as data_struct")
      checkSparkAnswerAndOperator("SELECT /*+ repartition(10) */ data_struct FROM t_map_struct")
    }
  }

  test("repartition over ArrayType with MapType") {
    withTable("t_array_map") {
      sql("""
          |create table t_array_map using parquet as
          |select array(map('k1', 1, 'k2', 2), map('k3', 3)) as array_of_map
          |""".stripMargin)
      checkSparkAnswerAndOperator("SELECT /*+ repartition(10) */ array_of_map FROM t_array_map")
    }
  }

  test("repartition over StructType with MapType") {
    withTable("t_struct_map") {
      sql("""
          |create table t_struct_map using parquet as
          |select named_struct('id', 101, 'metrics', map('ctr', 0.123d, 'cvr', 0.045d)) as user_metrics
          |""".stripMargin)
      checkSparkAnswerAndOperator("SELECT /*+ repartition(10) */ user_metrics FROM t_struct_map")
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
      checkSparkAnswerAndOperator(
        "SELECT /*+ repartition(10) */ map_struct_value FROM t_map_struct_value")
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
      checkSparkAnswerAndOperator("SELECT /*+ repartition(10) */ nested_map FROM t_nested_map")
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
      checkSparkAnswerAndOperator(
        "SELECT /*+ repartition(10) */ user_feature_array FROM t_array_struct_map")
    }
  }

  test("log function with negative input") {
    withTable("t1") {
      sql("create table t1 using parquet as select -1 as c1")
      checkSparkAnswerAndOperator("select ln(c1) from t1")
    }
  }

  test("floor function with long input") {
    withTable("t1") {
      sql("create table t1 using parquet as select 1L as c1, 2.2 as c2")
      checkSparkAnswerAndOperator("select floor(c1), floor(c2) from t1")
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
      checkSparkAnswerAndOperator("SELECT _col2 FROM test_hive_orc_impl")
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
            checkSparkAnswerAndOperator(() => spark.read.orc(path))

            withTable("t") {
              sql(s"CREATE EXTERNAL TABLE t(c3 INT, c2 INT) USING ORC LOCATION '$path'")

              val expected = if (forcePositionalEvolution) {
                correctAnswer
              } else {
                Seq(Row(null, 2), Row(null, 4), Row(null, 6), Row(null, null))
              }

              checkSparkAnswerAndOperator(() => spark.table("t"))
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
            checkSparkAnswerAndOperator(() => spark.read.orc(path))

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

              checkSparkAnswerAndOperator(() => spark.table("t"))
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

      checkSparkAnswerAndOperator("""
            |select q, count(*)
            |from (select event_time, quarter(event_time) as q from t1) t
            |where q <= 3
            |group by q
            |order by q
            |""".stripMargin)
    }
  }

  test("lpad/rpad basic") {
    withTable("pad_tbl") {
      sql(s"CREATE TABLE pad_tbl(id INT, txt STRING, len INT, pad STRING) USING parquet")
      sql(s"""
             |INSERT INTO pad_tbl VALUES
             | (1, 'abc', 5, ''),
             | (2, 'abc', 5, ' '),
             | (3, 'spark', 2, '0'),
             | (4, 'spark', 2, '0'),
             | (5, '9', 5, 'ab'),
             | (6, '9', 5, 'ab'),
             | (7, 'hi', 5, ''),
             | (8, 'hi', 5, ''),
             | (9, 'x', 0, 'a'),
             | (10,'x', -1, 'a'),
             | (11,'Z', 3, '++'),
             | (12,'Z', 3, 'AB')
      """.stripMargin)
      checkSparkAnswerAndOperator("SELECT LPAD(txt, len, pad), RPAD(txt, len, pad) FROM pad_tbl")
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
    withTable("initcap_basic_tbl") {
      sql(s"CREATE TABLE initcap_basic_tbl(id INT, txt STRING) USING parquet")
      sql(s"""
           |INSERT INTO initcap_basic_tbl VALUES
           | (1, 'spark sql'),
           | (2, 'SPARK'),
           | (3, 'sPaRk'),
           | (4, ''),
           | (5, NULL)
        """.stripMargin)
      checkSparkAnswerAndOperator("select id, initcap(txt) from initcap_basic_tbl")
    }
  }

  test("initcap: word boundaries and punctuation") {
    withTable("initcap_bound_tbl") {
      sql(s"CREATE TABLE initcap_bound_tbl(id INT, txt STRING) USING parquet")
      sql(s"""
           |INSERT INTO initcap_bound_tbl VALUES
           | (1, 'hello world'),
           | (2, 'hello_world'),
           | (3, 'über-alles'),
           | (4, 'foo.bar/baz'),
           | (5, 'v2Ray is COOL'),
           | (6, 'rock''n''roll'),
           | (7, 'hi\tthere'),
           | (8, 'hi\nthere')
        """.stripMargin)
      checkSparkAnswerAndOperator("select id, initcap(txt) from initcap_bound_tbl")
    }
  }

  test("initcap: mixed cases and edge cases") {
    withTable("initcap_mixed_tbl") {
      sql(s"CREATE TABLE initcap_mixed_tbl(id INT, txt STRING) USING parquet")
      sql(s"""
           |INSERT INTO initcap_mixed_tbl VALUES
           | (1, 'a1b2 c3D4'),
           | (2, '---abc--- ABC --ABC-- 世界 世 界 '),
           | (3, ' multiple   spaces '),
           | (4, 'AbCdE aBcDe'),
           | (5, ' A B A b '),
           | (6, 'aBćDe  ab世De AbĆdE aB世De ÄBĆΔE'),
           | (7, 'i\u0307onic  FIDELİO'),
           | (8, 'a🙃B🙃c  😄 😆')
        """.stripMargin)
      checkSparkAnswerAndOperator("select id, initcap(txt) from initcap_mixed_tbl")
    }
  }

  test("test filter with hour function") {
    withEnvConf("spark.auron.datetime.extract.enabled" -> "true") {
      withTable("t_hour") {
        sql("""
              |create table t_hour using parquet as
              |select to_timestamp('2024-12-18 01:23:45') as event_time union all
              |select to_timestamp('2024-12-18 08:00:00') union all
              |select to_timestamp('2024-12-18 08:59:59')
              |""".stripMargin)

        // Keep rows where HOUR >= 8, then group by hour
        checkSparkAnswerAndOperator("""
                |select h, count(*)
                |from (select hour(event_time) as h from t_hour) t
                |where h >= 8
                |group by h
                |order by h
                |""".stripMargin)
      }
    }
  }

  test("test filter with minute function") {
    withEnvConf("spark.auron.datetime.extract.enabled" -> "true") {
      withTable("t_minute") {
        sql("""
              |create table t_minute using parquet as
              |select to_timestamp('2024-12-18 00:00:00') as event_time union all
              |select to_timestamp('2024-12-18 00:30:00') union all
              |select to_timestamp('2024-12-18 12:30:59')
              |""".stripMargin)

        // Keep rows where MINUTE = 30, then group by minute
        checkSparkAnswerAndOperator("""
                |select m, count(*)
                |from (select minute(event_time) as m from t_minute) t
                |where m = 30
                |group by m
                |""".stripMargin)
      }
    }
  }

  test("test filter with second function") {
    withEnvConf("spark.auron.datetime.extract.enabled" -> "true") {
      withTable("t_second") {
        sql("""
              |create table t_second using parquet as
              |select to_timestamp('2024-12-18 00:00:00') as event_time union all
              |select to_timestamp('2024-12-18 01:23:00') union all
              |select to_timestamp('2024-12-18 23:59:45')
              |""".stripMargin)

        // Keep rows where SECOND = 0, then group by second
        checkSparkAnswerAndOperator("""
                |select s, count(*)
                |from (select second(event_time) as s from t_second) t
                |where s = 0
                |group by s
                |""".stripMargin)
      }
    }
  }

  // For Date input: hour/minute/second should all be 0
  test("timeparts on Date input return zeros") {
    withEnvConf("spark.auron.datetime.extract.enabled" -> "true") {
      withTable("t_date_parts") {
        sql(
          "create table t_date_parts using parquet as select date'2024-12-18' as d union all select date'2024-12-19'")
        checkSparkAnswerAndOperator("""
                |select
                |  hour(d)   as h,
                |  minute(d) as m,
                |  second(d) as s
                |from t_date_parts
                |order by d
                |""".stripMargin)
      }
    }
  }

  test("hour/minute/second respect timezone via from_utc_timestamp") {
    withEnvConf("spark.auron.datetime.extract.enabled" -> "true") {
      withTable("t_tz") {
        // Construct: UTC 1970-01-01 00:00:00 → Asia/Shanghai => local 08:00:00
        sql("""
              |create table t_tz using parquet as
              |select from_utc_timestamp(to_timestamp('1970-01-01 00:00:00'), 'Asia/Shanghai') as ts
              |""".stripMargin)

        checkSparkAnswerAndOperator("""
                |select hour(ts), minute(ts), second(ts)
                |from t_tz
                |""".stripMargin)
      }
    }
  }

  test("minute/second with non-whole-hour offsets") {
    withEnvConf("spark.auron.datetime.extract.enabled" -> "true") {
      withTable("t_tz2") {
        sql("""
              |create table t_tz2 using parquet as
              |select from_utc_timestamp(to_timestamp('2000-01-01 00:00:00'), 'Asia/Kolkata')   as ts1,  -- +05:30
              |       from_utc_timestamp(to_timestamp('2000-01-01 00:00:00'), 'Asia/Kathmandu') as ts2   -- +05:45
              |""".stripMargin)

        // Kolkata -> 05:30:00; Kathmandu -> 05:45:00
        checkSparkAnswerAndOperator(
          "select minute(ts1), second(ts1), minute(ts2), second(ts2) from t_tz2")
      }
    }
  }

  test("cast struct to string") {
    // SPARK-32499 SPARK-32501 SPARK-33291
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t_struct") {
        sql("""
              |create table t_struct using parquet as
              |select named_struct('a', 1, 'b', 'hello', 'c', true) as s
              |union all select named_struct('a', 2, 'b', 'world', 'c', false)
              |union all select named_struct('a', null, 'b', 'test', 'c', null)
              |""".stripMargin)

        checkSparkAnswerAndOperator("select cast(s as string) from t_struct")
      }
    }
  }

  test("cast nested struct to string") {
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t_nested_struct") {
        sql("""
              |create table t_nested_struct using parquet as
              |select named_struct('id', 1, 'inner', named_struct('x', 'a', 'y', 10)) as s
              |union all select named_struct('id', 2, 'inner', named_struct('x', 'b', 'y', 20))
              |""".stripMargin)

        checkSparkAnswerAndOperator("select cast(s as string) from t_nested_struct")
      }
    }
  }

  test("cast struct with null fields to string") {
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t_struct_nulls") {
        sql("""
              |create table t_struct_nulls using parquet as
              |select named_struct('f1', cast(null as int), 'f2', cast(null as string)) as s
              |union all select named_struct('f1', 100, 'f2', 'value')
              |""".stripMargin)

        checkSparkAnswerAndOperator("select cast(s as string) from t_struct_nulls")
      }
    }
  }

  test("cast map to string") {
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t_map") {
        sql("""
              |create table t_map using parquet as
              |select map('a', 1, 'b', 2) as m
              |union all select map('x', 10, 'y', 20, 'z', 30)
              |union all select map('key', null)
              |""".stripMargin)

        checkSparkAnswerAndOperator("select cast(m as string) from t_map")
      }
    }
  }

  test("cast nested map to string") {
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t_nested_map") {
        sql("""
              |create table t_nested_map using parquet as
              |select map('outer1', map('inner1', 1, 'inner2', 2)) as m
              |union all select map('outer2', map('inner3', 3))
              |""".stripMargin)

        checkSparkAnswerAndOperator("select cast(m as string) from t_nested_map")
      }
    }
  }

  test("cast map with struct value to string") {
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t_map_struct") {
        sql("""
              |create table t_map_struct using parquet as
              |select map('k1', named_struct('x', 'a', 'y', 10)) as m
              |union all select map('k2', named_struct('x', 'b', 'y', 20))
              |""".stripMargin)

        checkSparkAnswerAndOperator("select cast(m as string) from t_map_struct")
      }
    }
  }

  test("cast empty map to string") {
    if (AuronTestUtils.isSparkV31OrGreater) {
      withTable("t_empty_map") {
        sql("""
              |create table t_empty_map using parquet as
              |select map() as m
              |union all select map('a', 1)
              |""".stripMargin)

        checkSparkAnswerAndOperator("select cast(m as string) from t_empty_map")
      }
    }
  }
}
