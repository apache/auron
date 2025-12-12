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
import org.apache.spark.sql.auron.{AuronColumnarOverrides, AuronConverters}
import org.apache.spark.sql.catalyst.plans.physical.UnknownPartitioning
import org.apache.spark.sql.execution.auron.plan.NativeShuffleExchangeExec

class AuronCheckConvertShuffleExchangeSuite extends AuronQueryTest with BaseAuronSQLSuite {

  test(
    "test set auron shuffle manager convert to native shuffle exchange where set spark.auron.enable is true") {
    withTable("test_shuffle") {
      spark.sql("drop table if exists test_shuffle")
      spark.sql(
        "create table if not exists test_shuffle using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")

      withSQLConf(
        "spark.shuffle.manager" -> "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager") {
        val df = spark.sql("select c1, count(1) from test_shuffle group by c1")
        checkAnswer(df, Seq(Row(1, 1)))
        assert(collectFirst(df.queryExecution.executedPlan) { case e: NativeShuffleExchangeExec =>
          e
        }.isDefined)
      }
    }
  }

  test("fails when non-Auron shuffle manager with set spark.auron.enable is true") {
    withSQLConf("spark.shuffle.manager" -> "org.apache.spark.shuffle.sort.SortShuffleManager") {
      val emptyExec =
        AuronConverters.createEmptyExec(Seq.empty, UnknownPartitioning(1), Seq.empty)
      // With a non-Auron shuffle manager, preColumnarTransitions should fail fast
      intercept[AssertionError] {
        AuronColumnarOverrides(spark).preColumnarTransitions.apply(emptyExec)
      }
    }
  }
}
