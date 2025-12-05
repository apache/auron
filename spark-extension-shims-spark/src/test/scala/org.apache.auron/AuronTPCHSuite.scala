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
package org.apache.spark.auron

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

abstract class AuronTPCHSuite extends QueryTest with SharedSparkSession {

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val tpchDataPath: String = rootPath + "/tpch-data-parquet"
  protected val tpchQueriesPath: String = rootPath + "/tpch-queries"
  protected val tpchResultsPath: String = rootPath + "/tpch-query-results"
  protected val tpchPlanPath: String = rootPath + "/tpch-plan-stability"

  protected val colSep: String = "<|COL|>"

  protected val tpchQueries: Seq[String] = (1 to 22).map("q" + _)

  protected val tpchTables: Seq[String] =
    Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.auron.enable", "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createTPCHTables()
  }

  protected def createTPCHTables(): Unit = {
    tpchTables
      .foreach { tableName =>
        val path = s"$tpchDataPath/$tableName"
        spark.read.parquet(path).createOrReplaceTempView(tableName)
        val count = spark.table(tableName).count()
        logInfo(s"Registered TPCH temp view '$tableName' with $count rows from $path")
      }
  }

  def shouldVerifyPlan(): Boolean = {
    Shims.get.shimVersion match {
      case "spark-3.5" => true
      case _ => false // Support for other Spark versions in the future
    }
  }

  protected def verifyResult(df: DataFrame, sqlNum: String, resultsPath: String): Unit = {
    val result = df.collect()
    if (df.schema.exists(_.dataType == DoubleType)) {
      compareResultWithDoubleTolerance(sqlNum, result, resultsPath)
    } else {
      compareResultAsString(sqlNum, result, resultsPath)
    }
  }

  protected def compareResultAsString(
      sqlNum: String,
      result: Array[Row],
      resultsPath: String): Unit = {
    val actualBuilder = new StringBuffer()
    actualBuilder.append(result.length).append("\n")
    result.foreach(r => actualBuilder.append(r.mkString(colSep)).append("\n"))

    val expectedResult =
      FileUtils.readFileToString(
        new File(resultsPath + "/" + sqlNum + ".out"),
        StandardCharsets.UTF_8)

    if (expectedResult != actualBuilder.toString) {
      fail(s"""
              |=== $sqlNum result does NOT match expected ===
              |[Expected]
              |${expectedResult}
              |[Actual]
              |${actualBuilder.toString}
              |""".stripMargin)
    }
  }

  protected def compareResultWithDoubleTolerance(
      sqlNum: String,
      result: Array[Row],
      resultsPath: String,
      tol: Double = 1e-6): Unit = {
    val expectedResult =
      FileUtils
        .readLines(new File(s"$resultsPath/$sqlNum.out"), StandardCharsets.UTF_8)
        .iterator()
    val expectedCount = expectedResult.next().toInt
    assert(result.length == expectedCount)

    result.zipWithIndex.foreach { case (row, rowIndex) =>
      assert(expectedResult.hasNext)
      val expectedRow = expectedResult.next().split(Regex.quote(colSep))

      row.schema.zipWithIndex.foreach { case (field, idx) =>
        field.dataType match {
          case DoubleType =>
            val actualValue = row.getDouble(idx)
            val expectedValue = expectedRow(idx).toDouble
            assert(
              Math.abs(actualValue - expectedValue) < tol,
              formatRowMismatchMessage(
                sqlNum,
                rowIndex,
                expectedRow.mkString(colSep),
                row.toString))
          case _ =>
            val actualValue = row.get(idx).toString
            val expectedValue = expectedRow(idx)
            assert(
              actualValue.equals(expectedValue),
              formatRowMismatchMessage(
                sqlNum,
                rowIndex,
                expectedRow.mkString(colSep),
                row.toString))
        }
      }
    }
  }

  private def formatRowMismatchMessage(
      sqlNum: String,
      rowIndex: Int,
      expected: String,
      actual: String): String = {
    s"""
       |=== Row $rowIndex - $sqlNum result does NOT match expected ===
       |[Expected]
       |$expected
       |[Actual]
       |$actual
       |""".stripMargin
  }

  private def normalizeExplainPlan(plan: String): String = {
    val exprIdRegex = "#\\d+L?".r
    val planIdRegex = "plan_id=\\d+".r

    // Normalize file location
    def normalizeFileLocations(plan: String): String = {
      plan.replaceAll("""file:/[^,\s\]\)]+""", "file:/<warehouse_dir>")
    }

    // Build a stable map from found tokens to 1-based integer strings in order of appearance
    def stableRenumber(regex: Regex, input: String): Map[String, String] = {
      val map = new mutable.HashMap[String, String]()
      regex
        .findAllMatchIn(input)
        .map(_.toString)
        .foreach { token =>
          if (!map.contains(token)) map.put(token, (map.size + 1).toString)
        }
      map.toMap
    }

    // Replace tokens using a precomputed normalized mapping while keeping the key prefix
    def replaceTokens(
        plan: String,
        regex: Regex,
        mapping: Map[String, String],
        prefix: String): String = {
      regex.replaceAllIn(plan, m => s"$prefix${mapping(m.toString)}")
    }

    // Normalize exprId
    val exprIdMap = stableRenumber(exprIdRegex, plan)
    val exprIdNormalized = replaceTokens(plan, exprIdRegex, exprIdMap, "#")

    // Normalize plan_id
    val planIdMap = stableRenumber(planIdRegex, exprIdNormalized)
    val planIdNormalized =
      replaceTokens(exprIdNormalized, planIdRegex, planIdMap, "plan_id=")

    // Mask QueryStageExec arguments which can be non-deterministic
    val argumentsNormalized = planIdNormalized
      .replaceAll("Arguments: [0-9]+, [0-9]+", "Arguments: X, X")
      .replaceAll("Arguments: [0-9]+", "Arguments: X")

    normalizeFileLocations(argumentsNormalized)
  }

  protected def verifyPlan(df: DataFrame, sqlNum: String, planPath: String): Unit = {
    if (!shouldVerifyPlan()) {
      return
    }

    val expectedFile = new File(planPath, s"$sqlNum.txt")
    val expected = FileUtils.readFileToString(expectedFile, StandardCharsets.UTF_8)
    val actual = normalizeExplainPlan(df.queryExecution.explainString(FormattedMode))
    val actualFile = new File(FileUtils.getTempDirectory, s"tpch.plan.actual.$sqlNum.txt")
    FileUtils.writeStringToFile(actualFile, actual, StandardCharsets.UTF_8)

    if (expected != actual) {
      fail(s"""
              |Plans did not match for query: $sqlNum
              |Expected explain plan: ${expectedFile.getAbsolutePath}
              |
              |$expected
              |Actual explain plan: ${actualFile.getAbsolutePath}
              |
              |$actual
              |""".stripMargin)
    }
  }

  tpchQueries.foreach { sqlNum =>
    test("TPC-H " + sqlNum) {
      val sqlText = FileUtils.readFileToString(
        new File(s"$tpchQueriesPath/$sqlNum.sql"),
        StandardCharsets.UTF_8)
      val df = spark.sql(sqlText)
      verifyResult(df, sqlNum, tpchResultsPath)
      verifyPlan(df, sqlNum, tpchPlanPath)
    }
  }
}

/**
 * Variant of the TPCH suite that forces V1 Parquet data source and disables auto broadcast joins.
 */
class AuronTPCHV1Suite extends AuronTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}
