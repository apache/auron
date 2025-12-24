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

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

abstract class AuronTPCDSSuite extends QueryTest with SharedSparkSession {
  protected val regenGoldenFiles: Boolean =
    sys.env.getOrElse("REGEN_TPCDS_GOLDEN_FILES", "0") == "1"

  protected val tpcdsDataPath: String =
    //sys.env.getOrElse("SPARK_TPCDS_DATA", "")
    sys.env.getOrElse("SPARK_TPCDS_DATA", "/Users/yew1eb/workspaces/tpcds-validator/tpcds_1g")

  protected val execTpcdsQueries: String =
    sys.env.getOrElse("SPARK_TPCDS_QUERY", "")

  protected val tpcdsExtraSparkConfs: String =
    sys.env.getOrElse(
      "SPARK_TPCDS_EXTRA_CONF",
      "--conf spark.auron.ui.enabled=false  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer")

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val tpcdsQueriesPath: String = rootPath + "/tpcds-queries"
  protected val tpcdsResultsPath: String = rootPath + "/tpcds-query-results"
  protected val tpcdsPlanPath: String = rootPath + "/tpcds-plan-stability"

  protected val RE_GENERATE: Boolean = (sys.env.getOrElse("SPARK_TPCDS_REGENERATE", "0") == "1")

  protected val colSep: String = "<|COL|>"

  val tpcdsQueries: Seq[String] = Seq(
    "q1",
    "q2",
    "q3",
    "q4",
    "q5",
    "q6",
    "q7",
    "q8",
    "q9",
    "q10",
    "q11",
    "q12",
    "q13",
    "q14a",
    "q14b",
    "q15",
    "q16",
    "q17",
    "q18",
    "q19",
    "q20",
    "q21",
    "q22",
    "q23a",
    "q23b",
    "q24a",
    "q24b",
    "q25",
    "q26",
    "q27",
    "q28",
    "q29",
    "q30",
    "q31",
    "q32",
    "q33",
    "q34",
    "q35",
    "q36",
    "q37",
    "q38",
    "q39a",
    "q39b",
    "q40",
    "q41",
    "q42",
    "q43",
    "q44",
    "q45",
    "q46",
    "q47",
    "q48",
    "q49",
    "q50",
    "q51",
    "q52",
    "q53",
    "q54",
    "q55",
    "q56",
    "q57",
    "q58",
    "q59",
    "q60",
    "q61",
    "q62",
    "q63",
    "q64",
    "q65",
    "q66",
    "q67",
    "q68",
    "q69",
    "q70",
    "q71",
    "q72",
    "q73",
    "q74",
    "q75",
    "q76",
    "q77",
    "q78",
    "q79",
    "q80",
    "q81",
    "q82",
    "q83",
    "q84",
    "q85",
    "q86",
    "q87",
    "q88",
    "q89",
    "q90",
    "q91",
    "q92",
    "q93",
    "q94",
    "q95",
    "q96",
    "q97",
    "q98",
    "q99")

  protected var queryTables: Map[String, DataFrame] = _

  var needRunQueries: Seq[String] = Seq.empty
  var joinConfs: Map[String, String] = Map.empty

  def parseEnvironmentVaribles(): Unit = {
    logInfo(s"""
         |===============================================================
         |[TPCDS] Environment:
         | SPARK_TPCDS_DATA = $tpcdsDataPath
         | SPARK_TPCDS_QUERY = $execTpcdsQueries
         | SPARK_TPCDS_EXTRA_CONF = $tpcdsExtraSparkConfs
         |===============================================================
         |""".stripMargin)

    assume(tpcdsDataPath.nonEmpty, "Skipping all tests as env `SPARK_TPCDS_DATA` is not set")

    val queryFilter: Set[String] =
      Option(execTpcdsQueries)
        .filter(_.trim.nonEmpty)
        .map(_.toLowerCase(Locale.ROOT).split(",").map(_.trim).toSet)
        .getOrElse(Set.empty)
    needRunQueries = filterQueries(tpcdsQueries, queryFilter)

    val properties = new java.util.Properties()

    if (tpcdsExtraSparkConfs.nonEmpty) {
      tpcdsExtraSparkConfs.split("--conf").filter(_.trim.nonEmpty).foreach { conf =>
        val keyValue = conf.split("=", 2).map(_.trim)
        if (keyValue.length == 2) {
          properties.setProperty(keyValue(0), keyValue(1))
        } else {
          logInfo(s"Invalid config format: --conf$conf")
        }
      }
    }

    joinConfs = properties.asScala.toMap
    logInfo(s"""
         |===============================================================
         |[TPCDS] Coverted:
         | SPARK_TPCDS_DATA = $tpcdsDataPath
         | SPARK_TPCDS_QUERY = $needRunQueries
         | SPARK_TPCDS_EXTRA_CONF = $joinConfs
         |===============================================================
         |""".stripMargin)
  }

  parseEnvironmentVaribles()

  private def filterQueries(
      origQueries: Seq[String],
      queryFilter: Set[String],
      nameSuffix: String = ""): Seq[String] = {
    if (queryFilter.nonEmpty) {
      if (nameSuffix.nonEmpty) {
        origQueries.filter { name => queryFilter.contains(s"$name$nameSuffix") }
      } else {
        origQueries.filter(queryFilter.contains)
      }
    } else {
      origQueries
    }
  }

  override protected def sparkConf: SparkConf = {
    val baseConf = super.sparkConf
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.ui.enabled", "false")
      .set("spark.auron.ui.enabled", "false")
      .set("spark.auron.enable", "true")

    joinConfs.foreach { case (key, value) =>
      baseConf.set(key, value)
    }

    baseConf
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
    createQueryTables()
  }

  protected def createQueryTables(): Unit = {
    queryTables = Seq(
      "call_center",
      "catalog_page",
      "catalog_returns",
      "catalog_sales",
      "customer",
      "customer_address",
      "customer_demographics",
      "date_dim",
      "household_demographics",
      "income_band",
      "inventory",
      "item",
      "promotion",
      "reason",
      "ship_mode",
      "store",
      "store_returns",
      "store_sales",
      "time_dim",
      "warehouse",
      "web_page",
      "web_returns",
      "web_sales",
      "web_site").map { tableName =>
      val tablePath = new File(tpcdsDataPath, tableName).getAbsolutePath
      val tableDF = spark.read.format("parquet").load(tablePath)
      tableDF.createOrReplaceTempView(tableName)
      (tableName, tableDF)
    }.toMap
  }

  def shouldVerifyPhysicalPlan(): Boolean = {
    Shims.get.shimVersion match {
      case "spark-3.5" => true
      case _ => false // TODO: Support for other Spark versions in the future
    }
  }

  protected def checkQueryResult(df: DataFrame, queryId: String): Unit = {
    val goldenFile = new File(s"$tpcdsResultsPath/$queryId.out")
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
        if (actualRow.isNullAt(colIdx)) {
          assert("null".equals(expectedRow(colIdx)))
        } else {
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

    val goldenPlanFile = new File(s"$tpcdsPlanPath/$queryId.txt")
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

  needRunQueries.foreach { queryId =>
    test(s"TPC-DS $queryId") {
      val queryFile = new File(s"$tpcdsQueriesPath/$queryId.sql")
      val sqlText = FileUtils.readFileToString(queryFile, StandardCharsets.UTF_8).trim

      val resultDf = spark.sql(sqlText)

      checkQueryResult(resultDf, queryId)
      checkPhysicalPlan(resultDf, queryId)
    }
  }

}

class AuronTPCDSV1Suite extends AuronTPCDSSuite {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.sources.useV1SourceList", "parquet")
      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  }
}
