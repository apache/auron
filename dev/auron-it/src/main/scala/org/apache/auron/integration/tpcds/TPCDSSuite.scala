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
package org.apache.auron.integration.runner

import org.apache.spark.sql.auron.Shims

import org.apache.auron.integration.{QueryRunner, Suite, SuiteArgs}
import org.apache.auron.integration.comparison.{ComparisonResult, PlanStabilityChecker, QueryResultComparator}
import org.apache.auron.integration.tpcds.TPCDSFeatures

class TPCDSSuite(args: SuiteArgs) extends Suite(args) with TPCDSFeatures {

  val queryRunner = new QueryRunner(readQuery = (qid: String) => this.readQuery(qid))

  val resutComparator = new QueryResultComparator()

  val planStabilityChecker = new PlanStabilityChecker(
    readGolden = (qid: String) => this.readGolden(qid),
    writeGolden = (qid: String, plan: String) => this.writeGolden(qid, plan),
    regenGoldenFiles = args.regenGoldenFiles,
    planCheck = args.enablePlanCheck)

  // scalastyle:off println
  override def run(): Int = {
    val queries = filterQueries(args.queryFilter)
    if (queries.isEmpty) {
      println("No valid queries specified")
      return 1
    } else {
      println(s"Ready to execute queries: ${queries.mkString(", ")}")
    }

    val result = executeAndCompare(queries)
    reportResults(result)
  }

  private def executeAndCompare(queries: Seq[String]): Seq[ComparisonResult] = {
    println("Execute Auron ...")
    setupTables(args.dataLocation, sessions.auronSession)
    val auronResults = queryRunner.runQueries(sessions.auronSession, queries)

    val baseComparisons: Seq[ComparisonResult] =
      if (args.auronOnly) {
        queries.map { qid =>
          val a = auronResults(qid)
          ComparisonResult(
            queryId = qid,
            baselineRows = 0L,
            testRows = a.rowCount,
            baselineTime = 0.0,
            testTime = a.durationSec,
            rowMatch = true,
            dataMatch = true,
            passed = true,
            planStable = true)
        }
      } else {
        println("Execute Baseline (Vanilla Spark) ...")
        setupTables(args.dataLocation, sessions.baselineSession)
        val baselineResults = queryRunner.runQueries(sessions.baselineSession, queries)
        queries.map { queryId =>
          resutComparator.compare(baselineResults(queryId), auronResults(queryId))
        }
      }

    if (args.enablePlanCheck || args.regenGoldenFiles) {
      baseComparisons.foreach(comparisonResult => {
        val testResult = auronResults(comparisonResult.queryId)
        val planStable = planStabilityChecker.validate(testResult)
        comparisonResult.planStable = planStable
      })
    }
    baseComparisons
  }

  private def reportResults(results: Seq[ComparisonResult]): Int = {
    if (!args.auronOnly) printResultComparison(results)

    if (args.enablePlanCheck) printPlanStability(results)

    if (args.regenGoldenFiles) printGoldenRegenSummary(results)

    val total = results.length
    val failed = results.count(r => !r.passed || !r.planStable)
    if (failed > 0) {
      println(s"\nTPC-DS Test FAILED ($failed out of $total queries failed)")
      1
    } else {
      println(s"\nTPC-DS Test PASSED (All $total queries succeeded)")
      0
    }
  }

  private def printResultComparison(results: Seq[ComparisonResult]): Unit = {
    println("\n" + "=" * 60)
    println(s"TPC-DS Result Consistency (Vanilla Spark vs Auron) (${Shims.get.shimVersion})")
    println("-" * 60)
    println(
      f"${"Query"}%-6s ${"Result"}%-8s ${"Vanilla(s)"}%8s ${"Auron(s)"}%8s ${"Speedup(x)"}%8s")
    println("-" * 60)

    results.foreach { r =>
      val speedup = if (r.testTime > 0 && r.baselineTime > 0) {
        r.baselineTime / r.testTime
      } else 0.0

      val resultStr = if (r.passed) "PASS" else "FAIL"

      val vanillaTime = f"${r.baselineTime}%.2f"
      val auronTime = f"${r.testTime}%.2f"
      val speedupStr = f"$speedup%.2f"

      println(f"${r.queryId}%-6s $resultStr%-8s $vanillaTime%8s $auronTime%8s $speedupStr%8s")
    }

    val total = results.length
    val passedCount = results.count(_.passed)
    println("-" * 60)
    println(s"Passed: $passedCount/$total")
  }

  private def printPlanStability(results: Seq[ComparisonResult]): Unit = {
    println("\n" + "=" * 60)
    println(s"Auron Plan Stability (${Shims.get.shimVersion})")
    println("-" * 60)
    println(f"${"Query"}%-6s ${"Stable"}%-7s")
    println("-" * 60)
    results.foreach { r =>
      val stable = if (r.planStable) "PASS" else "FAIL"
      println(f"${r.queryId}%-6s $stable%-7s")
    }
    val total = results.length
    val stableCount = results.count(_.planStable)
    println("-" * 60)
    println(s"Passed: $stableCount/$total")
  }

  private def printGoldenRegenSummary(results: Seq[ComparisonResult]): Unit = {
    val ids = results.map(_.queryId)
    val totalUpdated = ids.size
    println("\n" + "=" * 60)
    println(s"Auron Golden Files (${Shims.get.shimVersion})")
    println("-" * 60)
    println(s"Updated queries: $totalUpdated")
    println(ids.mkString("- ", ", ", ""))
    println(s"Output directory: ${goldenOutputDir}")
  }
  // scalastyle:on
}

object TPCDSSuite {
  def apply(args: SuiteArgs): Suite = new TPCDSSuite(args)
}
