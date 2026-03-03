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
import org.apache.spark.sql.execution.joins.auron.plan.NativeBroadcastJoinExec

import org.apache.auron.spark.configuration.SparkAuronConfiguration
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
    withSQLConf("spark.auron.udf.singleChildFallback.enabled" -> "true") {
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
  }

  test("test select multiple spark ext functions with the same signature") {
    withSQLConf("spark.auron.udf.singleChildFallback.enabled" -> "true") {
      withTable("t1") {
        sql("create table t1 using parquet as select '2024-12-18' as event_time")
        checkSparkAnswerAndOperator("select year(event_time), month(event_time) from t1")
      }
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
    withSQLConf("spark.auron.udf.singleChildFallback.enabled" -> "true") {
      withTable("t1") {
        sql("create table t1 using parquet as select 1L as c1, 2.2 as c2")
        checkSparkAnswerAndOperator("select floor(c1), floor(c2) from t1")
      }
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
          SparkAuronConfiguration.ORC_FORCE_POSITIONAL_EVOLUTION.key -> forcePositionalEvolution.toString) {
          withTempPath { f =>
            val path = f.getCanonicalPath
            Seq[(Integer, Integer)]((1, 2), (3, 4), (5, 6), (null, null))
              .toDF("c1", "c2")
              .write
              .orc(path)
            checkSparkAnswerAndOperator(() => spark.read.orc(path))

            withTable("t") {
              sql(s"CREATE EXTERNAL TABLE t(c3 INT, c2 INT) USING ORC LOCATION '$path'")
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
          SparkAuronConfiguration.ORC_FORCE_POSITIONAL_EVOLUTION.key -> forcePositionalEvolution.toString) {
          withTempPath { f =>
            val path = f.getCanonicalPath
            Seq[(Integer, Integer, Integer)]((1, 2, 1), (3, 4, 2), (5, 6, 3), (null, null, 4))
              .toDF("c1", "c2", "p")
              .write
              .partitionBy("p")
              .orc(path)
            checkSparkAnswerAndOperator(() => spark.read.orc(path))

            withTable("t") {
              sql(s"""
                     |CREATE TABLE t(c3 INT, c2 INT)
                     |USING ORC
                     |PARTITIONED BY (p int)
                     |LOCATION '$path'
                     |""".stripMargin)
              sql("MSCK REPAIR TABLE t")
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

  test("standard LEFT ANTI JOIN includes NULL keys") {
    // This test verifies that standard LEFT ANTI JOIN correctly includes NULL keys
    // NULL keys should be in the result because NULL never matches anything
    withTable("left_table", "right_table") {
      sql("""
            |CREATE TABLE left_table using parquet AS
            |SELECT * FROM VALUES
            |  (1, 2.0),
            |  (1, 2.0),
            |  (2, 1.0),
            |  (2, 1.0),
            |  (3, 3.0),
            |  (null, null),
            |  (null, 5.0),
            |  (6, null)
            |AS t(a, b)
            |""".stripMargin)

      sql("""
            |CREATE TABLE right_table using parquet AS
            |SELECT * FROM VALUES
            |  (2, 3.0),
            |  (2, 3.0),
            |  (3, 2.0),
            |  (4, 1.0),
            |  (null, null),
            |  (null, 5.0),
            |  (6, null)
            |AS t(c, d)
            |""".stripMargin)

      // Standard LEFT ANTI JOIN should include rows with NULL keys
      // Expected: (1, 2.0), (1, 2.0), (null, null), (null, 5.0)
      checkSparkAnswer(
        "SELECT * FROM left_table LEFT ANTI JOIN right_table ON left_table.a = right_table.c")
    }
  }

  test("left join with NOT IN subquery should filter NULL values") {
    // This test verifies the fix for the NULL handling issue in Anti join.
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val query =
        """
          |WITH t2 AS (
          |  -- Large table: 100000 rows (0..99999)
          |  SELECT id AS loan_req_no
          |  FROM range(0, 100000)
          |),
          |t1 AS (
          |  -- Small table: 10 rows that can match t2
          |  SELECT * FROM VALUES
          |    (1, 'A'),
          |    (2, 'B'),
          |    (3, 'C'),
          |    (4, 'D'),
          |    (5, 'E'),
          |    (6, 'F'),
          |    (7, 'G'),
          |    (8, 'H'),
          |    (9, 'I'),
          |    (10,'J')
          |  AS t1(loan_req_no, partner_code)
          |),
          |blk AS (
          |  SELECT * FROM VALUES
          |    ('B'),
          |    ('Z')
          |  AS blk(code)
          |)
          |SELECT
          |  COUNT(*) AS cnt
          |FROM t2
          |LEFT JOIN t1
          |  ON t1.loan_req_no = t2.loan_req_no
          |WHERE t1.partner_code NOT IN (SELECT code FROM blk)
          |""".stripMargin

      checkSparkAnswer(query)
    }
  }

  test("NOT IN subquery with NULL values") {
    val row = identity[(java.lang.Integer, java.lang.Integer)] _
    Seq(row((1, 1)), row((2, 2)), row((3, null)))
      .toDF("a", "b")
      .createOrReplaceTempView("tbl")
    val df = checkSparkAnswer("select * from tbl where a not in (select b from tbl)")

    // Spark 3.0: NOT IN subquery is converted to BroadcastNestedLoopJoinExec, and falls back due to unsupported join condition
    if (AuronTestUtils.isSparkV31OrGreater) {
      assert(collectFirst(df.queryExecution.executedPlan) { case bhj: NativeBroadcastJoinExec =>
        assert(bhj.isNullAwareAntiJoin)
        bhj
      }.isDefined)
    }
  }

  test("instr function - basic queries") {
    withTable("employees") {
      sql("""
        |create table employees(id int, name string, email string) using parquet
        |""".stripMargin)
      sql("""
        |insert into employees values
        | (1, 'Alice Smith', 'alice@example.com'),
        | (2, 'Bob Johnson', 'bob.johnson@company.org'),
        | (3, 'Charlie Brown', 'charlie.b@test.co'),
        | (4, 'David Wilson', 'david.w@example.com')
        |""".stripMargin)

      // Test basic instr usage
      checkSparkAnswerAndOperator("""
        |select id, name, instr(email, '@') as at_pos from employees
        |""".stripMargin)

      // Test finding domain part using instr
      checkSparkAnswerAndOperator("""
        |select
        |  id,
        |  name,
        |  substring(email, instr(email, '@') + 1) as domain
        |from employees
        |""".stripMargin)
    }
  }

  test("instr function - filter with substring") {
    withTable("products") {
      sql("""
        |create table products(id int, description string, category string) using parquet
        |""".stripMargin)
      sql("""
        |insert into products values
        | (1, 'Premium wireless headphones', 'Electronics'),
        | (2, 'Organic green tea', 'Beverages'),
        | (3, 'Cotton t-shirt', 'Clothing'),
        | (4, 'Stainless steel water bottle', 'Kitchen'),
        | (5, 'Leather wallet', 'Accessories')
        |""".stripMargin)

      // Find products where description contains 'wireless'
      checkSparkAnswerAndOperator("""
        |select id, description
        |from products
        |where instr(description, 'wireless') > 0
        |""".stripMargin)

      // Find products where description contains 'organic'
      checkSparkAnswerAndOperator("""
        |select id, description
        |from products
        |where instr(description, 'organic') > 0
        |""".stripMargin)
    }
  }

  test("instr function - complex expressions") {
    withTable("logs") {
      sql("""
        |create table logs(id int, log_message string) using parquet
        |""".stripMargin)
      sql("""
        |insert into logs values
        | (1, 'ERROR: Connection timeout'),
        | (2, 'WARNING: Memory usage high'),
        | (3, 'INFO: Task completed'),
        | (4, 'ERROR: Disk full'),
        | (5, 'INFO: Starting service')
        |""".stripMargin)

      // Extract error codes after 'ERROR: '
      checkSparkAnswerAndOperator("""
        |select
        |  id,
        |  log_message,
        |  instr(log_message, 'ERROR:') as error_pos,
        |  case
        |    when instr(log_message, 'ERROR:') > 0 then substring(log_message, instr(log_message, 'ERROR:') + 7)
        |    else null
        |  end as error_detail
        |from logs
        |""".stripMargin)

      // Count errors and warnings
      checkSparkAnswerAndOperator("""
        |select
        |  case
        |    when instr(log_message, 'ERROR:') > 0 then 'ERROR'
        |    when instr(log_message, 'WARNING:') > 0 then 'WARNING'
        |    else 'INFO'
        |  end as log_level,
        |  count(*) as count
        |from logs
        |group by
        |  case
        |    when instr(log_message, 'ERROR:') > 0 then 'ERROR'
        |    when instr(log_message, 'WARNING:') > 0 then 'WARNING'
        |    else 'INFO'
        |  end
        |order by log_level
        |""".stripMargin)
    }
  }

  test("instr function - join conditions") {
    withTable("orders", "customers") {
      sql("""
        |create table orders(id int, customer_id int, product_code string) using parquet
        |""".stripMargin)
      sql("""
        |create table customers(id int, email string, notes string) using parquet
        |""".stripMargin)

      sql("""
        |insert into orders values
        | (1, 101, 'PROD-A'),
        | (2, 102, 'PROD-B'),
        | (3, 101, 'PROD-C'),
        | (4, 103, 'PROD-A')
        |""".stripMargin)

      sql("""
        |insert into customers values
        | (101, 'user101@example.com', 'VIP customer'),
        | (102, 'user102@test.org', 'Regular'),
        | (103, 'user103@demo.com', 'New customer')
        |""".stripMargin)

      // Join and filter using instr
      checkSparkAnswerAndOperator("""
        |select o.id as order_id, c.id as customer_id, c.email
        |from orders o
        |join customers c on o.customer_id = c.id
        |where instr(c.notes, 'VIP') > 0
        |""".stripMargin)
    }
  }

  test("instr function - array and scalar combinations") {
    withTable("texts") {
      sql("""
        |create table texts(id int, content string) using parquet
        |""".stripMargin)
      sql("""
        |insert into texts values
        | (1, 'apple banana apple cherry'),
        | (2, 'apple orange grape'),
        | (3, 'banana apple pear')
        |""".stripMargin)

      // Count occurrences using combination of instr
      checkSparkAnswerAndOperator("""
        |select
        |  id,
        |  content,
        |  instr(content, 'apple') as first_apple_pos,
        |  case
        |    when instr(substring(content, instr(content, 'apple') + 1), 'apple') > 0 then 'multiple'
        |    else 'single or none'
        |  end as occurrence_count
        |from texts
        |""".stripMargin)
    }
  }

  test("instr function - with unicode and chinese in real queries") {
    withTable("chinese_products") {
      sql("""
        |create table chinese_products(id int, name string, description string) using parquet
        |""".stripMargin)
      sql("""
        |insert into chinese_products values
        | (1, '智能手机', '高端智能手机'),
        | (2, '笔记本电脑', '轻薄笔记本电脑'),
        | (3, '无线耳机', '蓝牙无线耳机'),
        | (4, '平板电脑', '超薄平板电脑')
        |""".stripMargin)

      // Search for keywords in Chinese
      checkSparkAnswerAndOperator("""
        |select id, name
        |from chinese_products
        |where instr(name, '智能') > 0
        |""".stripMargin)

      checkSparkAnswerAndOperator("""
        |select id, name, description
        |from chinese_products
        |where instr(description, '无线') > 0
        |""".stripMargin)
    }
  }

  test("instr function - aggregation and window functions") {
    withTable("search_logs") {
      sql("""
        |create table search_logs(id int, user_id int, query string) using parquet
        |""".stripMargin)
      sql("""
        |insert into search_logs values
        | (1, 1, 'spark tutorial'),
        | (2, 1, 'spark sql'),
        | (3, 2, 'hadoop guide'),
        | (4, 1, 'spark dataframe'),
        | (5, 2, 'hadoop mapreduce'),
        | (6, 3, 'spark streaming'),
        | (7, 3, 'hadoop hdfs')
        |""".stripMargin)

      // Count searches containing 'spark' per user
      checkSparkAnswerAndOperator("""
        |select
        |  user_id,
        |  sum(case when instr(query, 'spark') > 0 then 1 else 0 end) as spark_searches
        |from search_logs
        |group by user_id
        |order by user_id
        |""".stripMargin)

      // Find position of first spark search per user
      checkSparkAnswerAndOperator("""
        |select
        |  user_id,
        |  min(case when instr(query, 'spark') > 0 then id else null end) as first_spark_search_id
        |from search_logs
        |group by user_id
        |having min(case when instr(query, 'spark') > 0 then id else null end) is not null
        |order by user_id
        |""".stripMargin)
    }
  }
}
