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
package org.apache.spark.sql

import org.apache.spark.sql.auron.{AuronConf, NativeSupports}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnaryExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, AQEShuffleReadExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.auron.plan.{NativeAggBase, NativeBroadcastJoinBase, NativeFilterBase, NativeFilterExec, NativeProjectBase, NativeRenameColumnsBase, NativeShuffleExchangeBase, NativeSortBase}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.test.SQLTestUtils
import org.scalatest.BeforeAndAfterEach

/**
 * Base test class under org.apache.spark.sql to use package-private [[SQLTestUtils]]; extends
 * [[QueryTest]] for comparisons and checks.
 */
abstract class AuronQueryTest
    extends QueryTest
    with SQLTestUtils
    with BeforeAndAfterEach
    with AdaptiveSparkPlanHelper {
  import testImplicits._

  /**
   * Assert results match vanilla Spark, skip operator checks.
   */
  protected def checkSparkAnswer(sqlStr: String): DataFrame = {
    checkSparkAnswerAndOperator(sqlStr, requireNative = false)
  }

  /**
   * Assert results match vanilla Spark, fail if any operator is not native.
   */
  protected def checkSparkAnswerAndOperator(
      sqlStr: String,
      requireNative: Boolean = true): DataFrame = {
    checkSparkAnswerAndOperator(() => sql(sqlStr), requireNative)
  }

  /**
   * Assert results match vanilla Spark, fail if any operator is not native.
   */
  protected def checkSparkAnswerAndOperator(
      dataframe: () => DataFrame,
      requireNative: Boolean = true): DataFrame = {

    var expected: Seq[Row] = null
    var sparkPlan = null.asInstanceOf[SparkPlan]
    withSQLConf("spark.auron.enable" -> "false") {
      val dfSpark = dataframe()
      expected = dfSpark.collect()
    }

    val dfAuron = dataframe()
    checkAnswer(dfAuron, expected)

    if (requireNative) {
      val plan = stripAQEPlan(dfAuron.queryExecution.executedPlan)
      plan
        .collectFirst { case op if !isNativeOrPassThrough(op) => op }
        .foreach { op: SparkPlan =>
          fail(s"""
               |Found non-native operator: ${op.nodeName}
               |plan: ${plan}""".stripMargin)
        }
    }

    dfAuron
  }

  protected def isNativeOrPassThrough(op: SparkPlan): Boolean = op match {
    case _: NativeSupports => true
    case _: WholeStageCodegenExec => true
    case _: BroadcastQueryStageExec => true
    case _: AQEShuffleReadExec => true
    case _: ShuffleQueryStageExec => true
    case _ => false
  }
}
