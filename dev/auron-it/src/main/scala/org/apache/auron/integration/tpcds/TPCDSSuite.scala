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

import org.apache.auron.integration.{QueryRunner, SingleQueryResult, Suite, SuiteArgs}
import org.apache.auron.integration.comparator.{ComparisonResult, PlanStabilityChecker, QueryResultComparator}
import org.apache.auron.integration.tpcds.TPCDSFeatures

class TPCDSSuite(args: SuiteArgs) extends Suite(args) with TPCDSFeatures {

  val queryRunner = new QueryRunner(readQuery = (qid: String) => this.readQuery(qid))

  val resutComparator = new QueryResultComparator()

  val planStability = new PlanStabilityChecker(
    readGolden = (qid: String) => this.readGolden(qid),
    writeGolden = (qid: String, plan: String) => this.writeGolden(qid, plan),
    regenGoldenFiles = args.regenGoldenFiles,
    planCheck = args.enablePlanCheck)

  override def run(): Int = {
    val queries = filterQueries(args.queryFilter)
    if (queries.isEmpty) {
      println("No valid queries specified")
      return 1
    } else {
      println(s"Ready to execute queries: ${queries.mkString(", ")}")
    }

    val result = executeAndCompare(queries)
    summarizeAndReport(result)
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
            success = true,
            planStable = true)
        }
      } else {
        println("Execute baseline (Vanilla Spark) ...")
        setupTables(args.dataLocation, sessions.baselineSession)
        val baselineResults = queryRunner.runQueries(sessions.baselineSession, queries)
        queries.map { queryId =>
          resutComparator.compare(baselineResults(queryId), auronResults(queryId))
        }
      }

    if (args.enablePlanCheck || args.regenGoldenFiles) {
      baseComparisons.foreach(comparisonResult => {
        val testResult = auronResults(comparisonResult.queryId)
        val planStable = planStability.validate(testResult)
        comparisonResult.planStable = planStable
      })
    }
    baseComparisons
  }

  private def summarizeAndReport(results: Seq[ComparisonResult]): Int = {
    if (!args.auronOnly) {
      printResultComparison(results)
    }
    if (args.enablePlanCheck || args.regenGoldenFiles) {
      printPlanStability(results)
    }

    val total = results.length
    val resultOk: ComparisonResult => Boolean =
      if (args.auronOnly) (_: ComparisonResult) => true
      else (r: ComparisonResult) => r.success

    val planOk: ComparisonResult => Boolean =
      if (!args.enablePlanCheck) (_: ComparisonResult) => true
      else (r: ComparisonResult) => r.planStable

    val failed = results.count(r => !(resultOk(r) && planOk(r)))

    if (failed > 0) {
      println(s"\nTPC-DS test FAILED: $failed/$total queries failed")
      1
    } else {
      println(s"\nTPC-DS test PASSED: $total/$total")
      0
    }
  }

  private def printResultComparison(results: Seq[ComparisonResult]): Unit = {
    if (args.auronOnly) return
    println("\n" + "=" * 100)
    println("TPC-DS Result Comparison (Vanilla Spark vs Auron)")
    println("=" * 100)
    println("Query | Rows(V/A) | Time(V/A) | Speedup | Result")
    println("-" * 100)

    results.foreach { r =>
      val speedup = if (r.testTime > 0) r.baselineTime / r.testTime else 0.0
      val speedupStr = if (r.testTime <= 0) " inf " else f"${speedup}%6.2fx"
      val status = if (r.success) "✅" else "❌"
      val dataCell = if (r.dataMatch) "✓" else "✗"
      println(
        f"$status ${r.queryId}%-6s | ${r.baselineRows}%5d/${r.testRows}%5d | " +
          f"${r.baselineTime}%7.2f/${r.testTime}%7.2f s | $speedupStr | $dataCell")
    }

    val total = results.length
    val resultPass = results.count(_.success)
    println("-" * 100)
    println(s"Result comparison passed: $resultPass/$total")
  }

  private def printPlanStability(results: Seq[ComparisonResult]): Unit = {
    println("\n" + "=" * 100)
    println("Auron Plan Stability")
    println("=" * 100)
    println("Query | Stable")
    println("-" * 100)

    results.foreach { r =>
      val stableCell =
        if (args.enablePlanCheck) { if (r.planStable) "✓" else "✗" }
        else if (args.regenGoldenFiles) "regenerated"
        else "-"
      println(f"${r.queryId}%-6s | $stableCell")
    }

    if (args.enablePlanCheck) {
      val total = results.length
      val stableCnt = results.count(_.planStable)
      println("-" * 100)
      println(s"Plan stability passed: $stableCnt/$total")
    }
  }
}

object TPCDSSuite {
  def apply(args: SuiteArgs): Suite = new TPCDSSuite(args)
}
