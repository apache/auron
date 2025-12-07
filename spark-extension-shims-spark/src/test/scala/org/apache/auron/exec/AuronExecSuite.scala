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
package org.apache.auron.exec

import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.execution.auron.plan.NativeCollectLimitExec

import org.apache.auron.BaseAuronSQLSuite

class AuronExecSuite extends AuronQueryTest with BaseAuronSQLSuite {

  test("Collect Limit") {
    withTable("t1") {
      sql("create table t1(id INT) using parquet")
      sql("insert into t1 values(1),(2),(3),(3),(3),(4),(5),(6),(7),(8),(9),(10)")
      Seq(1, 3, 8, 12, 20).foreach { limit =>
        val df = checkSparkAnswerAndOperator(s"SELECT id FROM t1 limit $limit")
        assert(collectFirst(df.queryExecution.executedPlan) { case e: NativeCollectLimitExec =>
          e
        }.isDefined)
      }
    }
  }

}
