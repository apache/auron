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
import org.apache.spark.sql.execution.auron.plan.NativeWindowBase

import org.apache.auron.util.AuronTestUtils

class AuronWindowSuite extends AuronQueryTest with BaseAuronSQLSuite with AuronSQLTestHelper {

  test("min/max/count cumulative ROWS windows") {
    withSQLConf("spark.auron.enable.window" -> "true", "spark.auron.batchSize" -> "2") {
      withTable("window_aggregates") {
        sql("create table window_aggregates(id int, k int, v int) using parquet")
        sql(
          "insert into window_aggregates values " +
            "(1,1,null),(2,1,20),(3,1,10),(4,2,null),(5,2,null),(6,3,7),(7,null,13)")
        for {
          partition <- Seq("partition by k", "")
          value <- Seq("v", "cast(v as double)", "cast(v as decimal(12,2))", "cast(v as string)")
        } {
          val frame = s"$partition order by id rows between unbounded preceding and current row"
          checkSparkAnswer(
            s"select id,min($value) over ($frame),max($value) over ($frame)," +
              s"count($value) over ($frame),count(*) over ($frame),count(1) over ($frame) " +
              "from window_aggregates")
        }
      }
    }
  }

  for (dataType <- Seq("float", "double")) {
    test(s"min/max/count cumulative ROWS $dataType NaN ordering") {
      withSQLConf("spark.auron.enable.window" -> "true", "spark.auron.batchSize" -> "2") {
        withTable("window_floats") {
          sql(s"create table window_floats(id int, k int, v $dataType) using parquet")
          sql("""insert into window_floats values
              |(1,1,cast('NaN' as double)),(2,1,null),(3,1,1),
              |(4,2,1),(5,2,null),(6,2,cast('NaN' as double)),
              |(7,3,cast('-Infinity' as double)),(8,3,cast('Infinity' as double))
              |""".stripMargin)
          val frame =
            "partition by k order by id rows between unbounded preceding and current row"
          checkSparkAnswer(
            s"select id,min(v) over ($frame),max(v) over ($frame),count(v) over ($frame) " +
              "from window_floats")
          checkSparkAnswer("select k,min(v),max(v),count(v) from window_floats group by k")
        }
      }
    }
  }

  test("min/max/count cumulative ROWS additional data types") {
    withSQLConf("spark.auron.enable.window" -> "true", "spark.auron.batchSize" -> "2") {
      withTable("window_types") {
        sql("""create table window_types(id int, k int, dec decimal(38,18),
            |b boolean, d date, bytes binary) using parquet
            |""".stripMargin)
        sql("""insert into window_types values
            |(1,1,null,null,null,null),
            |(2,1,99999999999999999999.123456789012345678,
            |true,date '2020-02-01',X'FF'),
            |(3,1,99999999999999999999.123456789012345679,
            |false,date '1969-12-31',X'00'),
            |(4,1,-99999999999999999999.123456789012345678,null,null,null),
            |(5,2,null,null,null,null)
            |""".stripMargin)
        val frame = "partition by k order by id rows between unbounded preceding and current row"
        for (value <- Seq("dec", "b", "d", "bytes")) {
          checkSparkAnswer(
            s"select id,min($value) over ($frame),max($value) over ($frame)," +
              s"count($value) over ($frame) from window_types")
        }
      }
    }
  }

  test("aggregate windows with other frames and functions") {
    withSQLConf("spark.auron.enable.window" -> "true") {
      withTable("window_fallbacks") {
        sql("create table window_fallbacks(id int, k int, v int) using parquet")
        sql("insert into window_fallbacks values (1,1,null),(2,1,4),(3,null,5),(4,2,null)")
        for {
          function <- Seq("min(v)", "max(v)", "count(v)")
          frame <- Seq(
            "range between unbounded preceding and current row",
            "rows between 1 preceding and current row",
            "rows between unbounded preceding and unbounded following")
        } {
          checkSparkAnswer(s"select id,$function over (order by id $frame) from window_fallbacks")
        }
        val frame = "order by id rows between unbounded preceding and current row"
        for (columns <- Seq(
            s"count(null) over ($frame)",
            s"count(v,k) over ($frame)",
            s"min(v) over ($frame),sum(v) over ($frame)",
            s"min(v) over ($frame),avg(v) over ($frame)")) {
          checkSparkAnswer(s"select id,$columns from window_fallbacks")
        }
      }
    }
  }

  test("lead window function") {
    withSQLConf("spark.auron.enable.window" -> "true") {
      withTable("t1") {
        sql("create table t1(id int, grp int, v string) using parquet")
        sql("insert into t1 values (1, 1, 'a'), (2, 1, null), (3, 1, 'c'), (4, 2, 'x')")

        checkSparkAnswerAndOperator("""select
            |  id,
            |  grp,
            |  v,
            |  lead(v) over (partition by grp order by id) as next_v,
            |  lead(v, 2, 'fallback') over (partition by grp order by id) as next2_v
            |from t1
            |""".stripMargin)
      }
    }
  }

  test("lead window function with ignore nulls falls back") {
    if (AuronTestUtils.isSparkV32OrGreater) {
      withSQLConf("spark.auron.enable.window" -> "true") {
        withTable("t1") {
          sql("create table t1(id int, grp int, v string) using parquet")
          sql("insert into t1 values (1, 1, 'a'), (2, 1, null), (3, 1, 'c'), (4, 2, 'x')")

          val df = checkSparkAnswer("""select
              |  id,
              |  grp,
              |  lead(v, 1, 'fallback') ignore nulls
              |    over (partition by grp order by id) as next_non_null_v
              |from t1
              |""".stripMargin)
          val plan = stripAQEPlan(df.queryExecution.executedPlan)
          assert(plan.collectFirst { case _: NativeWindowBase => true }.isEmpty)
        }
      }
    }
  }
}
