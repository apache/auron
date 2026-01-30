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
package org.apache.auron.integration

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.FormattedMode

case class QueryExecutionResult(
    queryId: String,
    rowCount: Long,
    durationSec: Double,
    rows: Array[Row],
    success: Boolean,
    plan: String,
    errorMsg: Option[String] = None)

class QueryRunner(readQuery: String => String) {

  def runQueries(spark: SparkSession, queries: Seq[String]): Map[String, QueryExecutionResult] = {
    queries
      .map { qid => executeQuery(spark, qid) }
      .map(r => r.queryId -> r)
      .toMap
  }

  private def executeQuery(spark: SparkSession, queryId: String): QueryExecutionResult = {
    val startTime = System.currentTimeMillis()

    val result = Try {
      val sql = readQuery(queryId)
      val df = spark.sql(sql)
      val rows: Array[Row] = df.collect()
      val rowCount: Long = rows.length
      val planStr: String = df.queryExecution.explainString(FormattedMode)
      (rows, rowCount, planStr)
    }

    val durationSec = (System.currentTimeMillis() - startTime) / 1000.0
    // scalastyle:off println
    result match {
      case Success((rows, rowCount, planStr)) =>
        println(s"Query $queryId executed successfully in $durationSec seconds.")
        QueryExecutionResult(
          queryId = queryId,
          rowCount = rowCount,
          durationSec = durationSec,
          rows = rows,
          success = true,
          plan = planStr)

      case Failure(e) =>
        println(s"Query $queryId failed after $durationSec seconds: ${e.getMessage}")
        QueryExecutionResult(
          queryId = queryId,
          rowCount = 0L,
          durationSec = durationSec,
          rows = Array.empty,
          success = false,
          plan = "",
          errorMsg = Some(e.getMessage))
    }
    // scalastyle:on
  }
}
