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

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.execution.FormattedMode
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.DoubleType

/**
 */
abstract class AuronTPCDSSuite extends AuronQueryTest with SharedSparkSession {

  protected val tpcdsDataPath: String =
    sys.env.getOrElse("SPARK_TPCDS_DATA", "/Users/yew1eb/workspaces/tpcds-validator/tpcds_1g")

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
    // TODO: https://github.com/apache/datafusion-comet/issues/392
    //  comment out 39a and 39b for now because the expected result for stddev failed:
    //  expected: 1.5242630430075292, actual: 1.524263043007529.
    //  Will change the comparison logic to detect floating-point numbers and compare
    //  with epsilon
    //"q39a",
    //"q39b",
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
    // "q72",  //q72 very slow
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

  def shouldVerifyPlan(): Boolean = {
    Shims.get.shimVersion match {
      case "spark-3.5" => true
      case _ => false // TODO: Support for other Spark versions in the future
    }
  }

  protected def verifyResult(df: DataFrame, sqlNum: String, resultsPath: String): Unit = {
    val result = df.collect()
    if (df.schema.exists(_.dataType == DoubleType)) {
      compareDoubleResult(sqlNum, result, resultsPath)
    } else {
      compareResultStr(sqlNum, result, resultsPath)
    }
  }

  protected def compareResultStr(
      sqlNum: String,
      result: Array[Row],
      resultsPath: String): Unit = {
    val resultStr = new StringBuffer()
    resultStr.append(result.length).append("\n")
    result.foreach(r => resultStr.append(r.mkString(colSep)).append("\n"))

    if (RE_GENERATE) {
      FileUtils.writeStringToFile(
        new File(resultsPath + "/" + sqlNum + ".out"),
        resultStr.toString,
        StandardCharsets.UTF_8)
      return
    }

    val expectedResult =
      FileUtils.readFileToString(
        new File(resultsPath + "/" + sqlNum + ".out"),
        StandardCharsets.UTF_8)

    if (expectedResult != resultStr.toString) {
      fail(s"""
           |=== $sqlNum result does NOT match expected ===
           |[Expected]
           |${expectedResult}
           |[Actual]
           |${resultStr.toString}
           |""".stripMargin)
    }
  }

  protected def compareDoubleResult(
      sqlNum: String,
      result: Array[Row],
      resultsPath: String): Unit = {

    if (RE_GENERATE) {
      var resultStr = new StringBuilder()
      resultStr.append(result.length + "\n")
      result.foreach { row => resultStr.append(row.mkString(colSep)).append("\n") }
      FileUtils.writeStringToFile(
        new File(resultsPath + "/" + sqlNum + ".out"),
        resultStr.toString,
        StandardCharsets.UTF_8)
      return
    }

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
        if (row.isNullAt(idx)) {
          assert("null".equals(expectedRow(idx)))
        } else {
          field.dataType match {
            case DoubleType =>
              val actualValue = row.getDouble(idx)
              val expectedValue = expectedRow(idx).toDouble
              assert(
                Math.abs(actualValue - expectedValue) < 0.000001,
                failureMessage(sqlNum, rowIndex, expectedRow.mkString(colSep), row.toString))
            case _ =>
              val actualValue = row.get(idx).toString
              val expectedValue = expectedRow(idx)
              assert(
                actualValue.equals(expectedValue),
                failureMessage(sqlNum, rowIndex, expectedRow.mkString(colSep), row.toString))
          }
        }
      }
    }
  }

  private def failureMessage(
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

  protected def verifyPlan(df: DataFrame, sqlNum: String, planPath: String): Unit = {
    if (!shouldVerifyPlan()) {
      return
    }
    val actual = normalizeExplainPlan(df.queryExecution.explainString(FormattedMode))

    val expectedFile = new File(planPath, s"$sqlNum.txt")

    if (RE_GENERATE) {
      FileUtils.writeStringToFile(expectedFile, actual, StandardCharsets.UTF_8)
      return
    }

    val expected = FileUtils.readFileToString(expectedFile, StandardCharsets.UTF_8)
    val actualFile = new File(FileUtils.getTempDirectory, s"tpcds.plan.actual.$sqlNum.txt")
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

  // only run single query locally test
  val sqlNum = "q1"
  test(s"TPC-DS query: $sqlNum") {
    val sqlStr = FileUtils.readFileToString(
      new File(s"$tpcdsQueriesPath/$sqlNum.sql"),
      StandardCharsets.UTF_8)
    verifyResult(sql(sqlStr), sqlNum, tpcdsResultsPath)
    verifyPlan(sql(sqlStr), sqlNum, tpcdsPlanPath)
    checkSparkAnswer(sqlStr)
  }

  if (false) {
    if (tpcdsDataPath.nonEmpty) {
      tpcdsQueries.foreach { sqlNum =>
        test(s"TPC-DS: ${sqlNum}") {
          val sqlStr = FileUtils.readFileToString(
            new File(s"$tpcdsQueriesPath/$sqlNum.sql"),
            StandardCharsets.UTF_8)
          val df = spark.sql(sqlStr)
          verifyResult(df, sqlNum, tpcdsResultsPath)
          verifyPlan(df, sqlNum, tpcdsPlanPath)
          checkSparkAnswer(sqlStr)
        }
      }
    } else {
      ignore("skipped because env `SPARK_TPCDS_DATA` is not set") {}
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
