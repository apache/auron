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
