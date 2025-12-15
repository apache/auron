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
package org.apache.spark.sql.auron

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

abstract class AuronTPCHSuite extends QueryTest with SharedSparkSession {

  protected val regenGoldenFiles: Boolean =
    sys.env.getOrElse("REGEN_TPCH_GOLDEN_FILES", "0") == "1"

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val tpchDataPath: String = s"$rootPath/tpch-data-parquet"
  protected val tpchQueriesPath: String = s"$rootPath/tpch-queries"
  protected val tpchResultsPath: String = s"$rootPath/tpch-query-results"
  protected val tpchPlanPath: String = s"$rootPath/tpch-plan-stability"

  protected val colSep: String = "<|COL|>"

  protected val tpchQueryIds: Seq[String] = (1 to 22).map("q" + _)

  protected val tpchTables: Seq[String] =
    Seq("customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.ui.enabled", "false")
      .set("spark.auron.enable", "true")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
    createTPCHTables()
  }

  protected def createTPCHTables(): Unit = {
    tpchTables
      .foreach { tableName =>
        spark.read.parquet(s"$tpchDataPath/$tableName").createOrReplaceTempView(tableName)
        tableName -> spark.table(tableName).count()
      }
  }

  def shouldVerifyPhysicalPlan(): Boolean = {
    Shims.get.shimVersion match {
      case "spark-3.5" => true
      case _ => false // TODO: Support for other Spark versions in the future
    }
  }

  protected def checkQueryResult(df: DataFrame, queryId: String): Unit = {
    val goldenFile = new File(s"$tpchResultsPath/$queryId.out")
    val rows = df.collect()

    if (regenGoldenFiles) {
      writeGoldenFile(goldenFile, formatResultContent(rows))
      return
    }

    if (df.schema.exists(_.dataType == DoubleType)) {
      compareDoubleResult(queryId, rows, goldenFile)
    } else {
      compareResultStr(queryId, rows, goldenFile)
    }
  }

  private def formatResultContent(rows: Array[Row]): String = {
    val rowStrings = rows.map(_.mkString(colSep))
    s"${rows.length}\n${rowStrings.mkString("\n")}\n"
  }

  private def writeGoldenFile(file: File, content: String): Unit = {
    Option(file.getParentFile).foreach(_.mkdirs())
    FileUtils.writeStringToFile(file, content, StandardCharsets.UTF_8)
  }

  protected def compareResultStr(sqlNum: String, rows: Array[Row], goldenFile: File): Unit = {
    val actualContent = formatResultContent(rows)

    val expectedResult = FileUtils.readFileToString(goldenFile, StandardCharsets.UTF_8)
    if (expectedResult != actualContent) {
      fail(s"""
           |=== $sqlNum result does NOT match expected ===
           |[Expected]
           |${expectedResult}
           |[Actual]
           |${actualContent}
           |""".stripMargin)
    }
  }

  protected def compareDoubleResult(
      queryId: String,
      rows: Array[Row],
      goldenFile: File,
      tolerance: Double = 1e-6): Unit = {

    val expectedRowIter = FileUtils.readLines(goldenFile, StandardCharsets.UTF_8).iterator()
    val expectedRowCount = expectedRowIter.next().toInt
    assert(
      rows.length == expectedRowCount,
      s"Row count mismatch in $queryId: expected $expectedRowCount, got ${rows.length}")

    rows.zipWithIndex.foreach { case (actualRow, rowIdx) =>
      assert(expectedRowIter.hasNext)
      val expectedRow = expectedRowIter.next().split(Regex.quote(colSep))

      actualRow.schema.zipWithIndex.foreach { case (field, colIdx) =>
        field.dataType match {
          case DoubleType =>
            val actualValue = actualRow.getDouble(colIdx)
            val expectedValue = expectedRow(colIdx).toDouble
            val diff = math.abs(actualValue - expectedValue)
            assert(
              diff < tolerance,
              s"Floating-point mismatch in $queryId row $rowIdx col $colIdx: " +
                s"expected ${expectedValue}, got ${actualValue} (diff=$diff)")
          case _ =>
            val actualValue = actualRow.get(colIdx).toString
            val expectedValue = expectedRow(colIdx)
            assert(
              actualValue == expectedValue,
              s"Mismatch in $queryId row $rowIdx col $colIdx: " +
                s"expected $expectedValue, got $actualValue")
        }
      }
    }
  }

  private def normalizePhysicalPlan(plan: String): String = {
    val exprIdRegex = "#\\d+L?".r
    val planIdRegex = "plan_id=\\d+".r

    // Normalize file location
    def normalizeLocation(plan: String): String = {
      plan.replaceAll("""file:/[^,\s\]\)]+""", "file:/<warehouse_dir>")
    }

    // Create a normalized map for regex matches
    def createNormalizedMap(regex: Regex, plan: String): Map[String, String] = {
      val map = new mutable.HashMap[String, String]()
      regex
        .findAllMatchIn(plan)
        .map(_.toString)
        .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
      map.toMap
    }

    // Replace occurrences in the plan using the normalized map
    def replaceWithNormalizedValues(
        plan: String,
        regex: Regex,
        normalizedMap: Map[String, String],
        format: String): String = {
      regex.replaceAllIn(plan, regexMatch => s"$format${normalizedMap(regexMatch.toString)}")
    }

    // Normalize the entire plan step by step
    val exprIdMap = createNormalizedMap(exprIdRegex, plan)
    val exprIdNormalized = replaceWithNormalizedValues(plan, exprIdRegex, exprIdMap, "#")

    val planIdMap = createNormalizedMap(planIdRegex, exprIdNormalized)
    val planIdNormalized =
      replaceWithNormalizedValues(exprIdNormalized, planIdRegex, planIdMap, "plan_id=")

    // QueryStageExec will take its id as argument, replace it with X
    val argumentsNormalized = planIdNormalized
      .replaceAll("Arguments: [0-9]+, [0-9]+", "Arguments: X, X")
      .replaceAll("Arguments: [0-9]+", "Arguments: X")

    normalizeLocation(argumentsNormalized)
  }

  protected def checkPhysicalPlan(df: DataFrame, queryId: String): Unit = {
    if (!shouldVerifyPhysicalPlan()) {
      return
    }

    val goldenPlanFile = new File(s"$tpchPlanPath/$queryId.txt")
    val actualPlan = normalizePhysicalPlan(df.queryExecution.explainString(FormattedMode))

    if (regenGoldenFiles) {
      writeGoldenFile(goldenPlanFile, actualPlan)
      return
    }

    val expectedPlan = FileUtils.readFileToString(goldenPlanFile, StandardCharsets.UTF_8)
    if (expectedPlan != actualPlan) {
      val actualTempFile = new File(FileUtils.getTempDirectory, s"tpch.actual.plan.$queryId.txt")
      FileUtils.writeStringToFile(actualTempFile, actualPlan, StandardCharsets.UTF_8)
      fail(s"""
           |Physical plan mismatch for query $queryId
           |Expected: ${goldenPlanFile.getAbsolutePath}
           |Actual  : ${actualTempFile.getAbsolutePath}
           |
           |--- Expected ---
           |$expectedPlan
           |
           |--- Actual ---
           |$actualPlan
           |""".stripMargin)
    }
  }

  tpchQueryIds.foreach { queryId =>
    test(s"TPC-H $queryId") {
      val queryFile = new File(s"$tpchQueriesPath/$queryId.sql")
      val sqlText = FileUtils.readFileToString(queryFile, StandardCharsets.UTF_8).trim

      val resultDf = spark.sql(sqlText)

      checkQueryResult(resultDf, queryId)
      checkPhysicalPlan(resultDf, queryId)
    }
  }
}

/**
 * Variant that forces usage of the legacy V1 Parquet reader and disables broadcast joins
 * (ensuring sort-merge joins are used).
 */
class AuronTPCHV1Suite extends AuronTPCHSuite {
  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}
