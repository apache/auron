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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.execution.FormattedMode

case class SingleQueryResult(
    queryId: String,
    rowCount: Long,
    durationSec: Double,
    rows: Array[Row],
    plan: String,
    success: Boolean,
    errorMsg: Option[String] = None)

class QueryRunner(loadQuerySql: String => String) {

  def runQueries(spark: SparkSession, queries: Seq[String]): Map[String, SingleQueryResult] = {
    queries
      .map { qid => executeSingleQuery(spark, qid) }
      .map(r => r.queryId -> r)
      .toMap
  }

  def executeSingleQuery(spark: SparkSession, queryId: String): SingleQueryResult = {
    val startTime = System.currentTimeMillis()
    try {
      val sql = loadQuerySql(queryId)
      val df = spark.sql(sql)
      val rows = df.collect()
      val rowCount = rows.length
      val planStr = df.queryExecution.explainString(FormattedMode)

      val duration = (System.currentTimeMillis() - startTime) / 1000.0
      println(s"queryId: $queryId, duration: $duration s")
      SingleQueryResult(
        queryId = queryId,
        rowCount = rowCount,
        durationSec = duration,
        rows = rows,
        plan = planStr,
        success = true,
        errorMsg = None)
    } catch {
      case e: Exception =>
        println(s"queryId: $queryId, executeSingleQuery failed, Exception: $e")
        val duration = (System.currentTimeMillis() - startTime) / 1000.0
        SingleQueryResult(
          queryId,
          0L,
          duration,
          Array.empty,
          "",
          success = false,
          Some(e.getMessage))
    }
  }

}
