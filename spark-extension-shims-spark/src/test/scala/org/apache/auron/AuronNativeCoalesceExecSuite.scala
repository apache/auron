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
import org.apache.spark.sql.execution.auron.plan.NativeCoalesceExec

class AuronNativeCoalesceExecSuite extends AuronQueryTest with BaseAuronSQLSuite {
  import testImplicits._

  test("test CoalesceExec to native") {
    withSQLConf("spark.auron.enable.coalesce" -> "true") {
      Seq((1, 2, "test test"))
        .toDF("c1", "c2", "part")
//        .coalesce(2)
        .createOrReplaceTempView("coalesce_table1")
      val df = {
//        spark.sql("select count(a.c1), count(a.c2) from coalesce_table1 a ")
        spark.sql("select /*+ coalesce(2)*/ a.c1, a.c2 from coalesce_table1 a ")
      }
      df.show()

      checkAnswer(df, Seq(Row(1, 2)))
      assert(collectFirst(df.queryExecution.executedPlan) {
        case coalesceExec: NativeCoalesceExec =>
          coalesceExec
      }.isDefined)

    }
  }

  test("123") {
    val random = new java.util.Random()
    val data = (0 until 1000).map { _ =>
      (random.nextInt(10), random.nextInt(100))
    }
    data.toDF("key", "value").coalesce(2).createOrReplaceTempView("coalesce_table1")

    val df =
      spark.sql("select count(key), count(value) from coalesce_table1 a ")

    checkAnswer(df, Seq(Row(1, 2)))

  }
}
