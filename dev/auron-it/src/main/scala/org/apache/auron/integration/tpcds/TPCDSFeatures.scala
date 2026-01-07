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
package org.apache.auron.integration.tpcds

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.auron.Shims

trait TPCDSFeatures {

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

  val tpcdsTables = Seq(
    "catalog_page",
    "catalog_returns",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "inventory",
    "item",
    "promotion",
    "store",
    "store_returns",
    "catalog_sales",
    "web_sales",
    "store_sales",
    "web_returns",
    "web_site",
    "reason",
    "call_center",
    "warehouse",
    "ship_mode",
    "income_band",
    "time_dim",
    "web_page")

  def setupTables(dataLocation: String, spark: SparkSession): Map[String, Long] = {
    println(s"Setting up TPC-DS tables from: $dataLocation")
    tpcdsTables.map { tableName =>
      val tablePath = s"$dataLocation/$tableName"
      spark.read.parquet(tablePath).createOrReplaceTempView(tableName)
      val count = spark.table(tableName).count()
      println(s"Registered TPC-DS temp view '$tableName' with $count rows from $tablePath")
      tableName -> count
    }.toMap
  }

  protected def filterQueries(queryFilter: Seq[String]): Seq[String] = {
    if (queryFilter.isEmpty) tpcdsQueries
    else {
      queryFilter.filter(q => tpcdsQueries.contains(q))
    }
  }

  def readQuery(queryId: String): String = {
    val resourcePath = s"tpcds-queries/$queryId.sql"
    val is =
      Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(resourcePath))
        .getOrElse(
          throw new IllegalArgumentException(s"TPC-DS query resource not found: $resourcePath"))
    val src = scala.io.Source.fromInputStream(is, "UTF-8")
    try src.mkString.trim
    finally src.close()
  }

  def readGolden(queryId: String): String = {
    val resourcePath = s"tpcds-plan-stability/${Shims.get.shimVersion}/$queryId.txt"
    val is =
      Option(Thread.currentThread().getContextClassLoader.getResourceAsStream(resourcePath))
        .getOrElse(
          throw new IllegalArgumentException(s"TPC-DS golden plan not found: $resourcePath"))
    val src = scala.io.Source.fromInputStream(is, "UTF-8")
    try src.mkString
    finally src.close()
  }

  lazy val goldenOutputDir: File = {
    val dir = new File(
      System.getProperty("java.io.tmpdir"),
      s"tpcds-plan-stability/${Shims.get.shimVersion}")
    if (!dir.exists()) dir.mkdirs()
    dir
  }

  def writeGolden(queryId: String, normalized: String): File = {
    val path: Path = new File(goldenOutputDir, s"$queryId.txt").toPath
    println(s"[GoldenPlan] queryId=$queryId, saved to ${path.toAbsolutePath}")
    Files.write(path, normalized.getBytes(StandardCharsets.UTF_8))
    path.toFile
  }
}
