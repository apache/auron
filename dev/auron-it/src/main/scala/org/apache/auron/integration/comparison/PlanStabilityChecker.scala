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
package org.apache.auron.integration.comparison

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.util.matching.Regex

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.auron.Shims

import org.apache.auron.integration.QueryExecutionResult

class PlanStabilityChecker(
    readGolden: String => String,
    writeGolden: (String, String) => Unit,
    regenGoldenFiles: Boolean = false,
    planCheck: Boolean = false) {

  // scalastyle:off println
  private lazy val isSupported: Boolean = Shims.get.shimVersion match {
    case "spark-3.5" => true
    case other =>
      println(s"[PlanCheck] Unsupported Spark version: $other. Skipping.")
      false
  }

  def validate(test: QueryExecutionResult): Boolean = {
    if (!isSupported) return true

    if (regenGoldenFiles) {
      generatePlanGolden(test.queryId, test.plan)
    } else if (planCheck) {
      return comparePlanGolden(test.queryId, test.plan)
    }
    true
  }

  private def generatePlanGolden(queryId: String, rawPlan: String): Unit = {
    val normalized = normalizePlan(rawPlan)
    writeGolden(queryId, normalized)
  }

  private def comparePlanGolden(queryId: String, rawPlan: String): Boolean = {
    val expectedPlan = readGolden(queryId)
    val actualPlan = normalizePlan(rawPlan)
    if (expectedPlan == actualPlan) {
      true
    } else {
      val actualTempFile = new File(FileUtils.getTempDirectory, s"actual.golden.$queryId.txt")
      FileUtils.writeStringToFile(actualTempFile, actualPlan, StandardCharsets.UTF_8)
      println(s"""
                 |Physical plan mismatch for query $queryId
                 |Actual  : ${actualTempFile.getAbsolutePath}
                 |--- Expected ---
                 |$expectedPlan
                 |
                 |--- Actual ---
                 |$actualPlan
                 |""".stripMargin)
      false
    }
  }

  private def normalizePlan(plan: String): String = {
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

    val argumentsNormalized = planIdNormalized
      .replaceAll("Arguments: [0-9]+, [0-9]+", "Arguments: X, X")
      .replaceAll("Arguments: [0-9]+", "Arguments: X")
      .replaceAll("Scan parquet ", "Scan parquet")
      .replaceAll("Statistics[(A-Za-z0-9=. ,+)]*", "Statistics(X)")
      .replaceAll("\\[codegen id : [0-9]+\\]", "[codegen id : X]")

    normalizeLocation(argumentsNormalized)
  }
  // scalastyle:on
}
