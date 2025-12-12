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
import org.apache.spark.sql.execution.auron.plan.{NativeFilterExec, NativeTakeOrderedAndProjectExec}

class AuronExecSuite extends AuronQueryTest with BaseAuronSQLSuite {

  test("TakeOrderedAndProject") {
    withTempView("t1") {
      sql("create table t1(id INT, name STRING) using parquet")
      sql("insert into t1 values(1, 'a'),(2, 'b'),(3, 'c'),(3, 'c'),(4, 'd'),(5, 'e')")

      // executeCollect (collect rows directly to driver)
      var df = checkSparkAnswerAndOperator(
        "SELECT id + 42, name, length(name) FROM t1 order by id limit 4")
      val firstNode = collect(stripAQEPlan(df.queryExecution.executedPlan)) { case exec =>
        exec
      }.head
      assert(firstNode.isInstanceOf[NativeTakeOrderedAndProjectExec])

      // doExecuteNative
      df = checkSparkAnswerAndOperator(
        "select * from (SELECT id, id + 42, length(name) FROM t1 order by id limit 4) where id > 1")
      assert(collectFirst(df.queryExecution.executedPlan) {
        case f: NativeFilterExec if f.child.isInstanceOf[NativeTakeOrderedAndProjectExec] => true
      }.isDefined)
    }
  }
}
