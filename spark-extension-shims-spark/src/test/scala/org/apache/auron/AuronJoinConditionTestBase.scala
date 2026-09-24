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
import org.apache.spark.sql.execution.SparkPlan

abstract class AuronJoinConditionTestBase
    extends AuronQueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {
  import testImplicits._

  protected def withJoinInputs(f: => Unit): Unit = {
    withTempView("condition_left", "condition_right") {
      Seq[(Integer, Integer)]((1, 5), (1, 1), (1, null), (2, 3), (3, 7), (null, 8))
        .toDF("k", "v")
        .repartition(2)
        .createOrReplaceTempView("condition_left")
      Seq[(Integer, Integer)]((1, 4), (1, 2), (1, null), (2, 2), (4, 9), (null, 10))
        .toDF("k", "v")
        .repartition(2)
        .createOrReplaceTempView("condition_right")
      f
    }
  }

  protected def checkJoinPlan(
      query: String,
      isNativeJoin: SparkPlan => Boolean,
      native: Boolean = true): Unit = {
    val df = checkSparkAnswer(query)
    val plan = stripAQEPlan(df.queryExecution.executedPlan)
    assert(
      plan.collectFirst { case join if isNativeJoin(join) => join }.isDefined == native,
      plan.toString)
  }
}
