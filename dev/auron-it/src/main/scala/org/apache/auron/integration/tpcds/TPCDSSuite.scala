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

  val queryRunner = new QueryRunner(loadQuerySql = (qid: String) => this.loadQuerySql(qid))

  val resutComparator = new QueryResultComparator()

  val planStability = new PlanStabilityChecker(
    readGoldenPlan = (qid: String) => this.readGoldenPlan(qid),
    writeGoldenPlan = (qid: String, plan: String) => this.writeGoldenPlan(qid, plan),
    regenGoldenFiles = args.regenGoldenFiles,
    planCheck = args.enablePlanCheck)

  override def run(): Int = {
    val queries = if (args.queryFilter == Nil) {
      tpcdsQueries
    } else {
      filterQueries(args.queryFilter)
    }

    if (queries.isEmpty) {
      println("No valid queries specified")
      return 1
    } else {
      println(s"AuronTPCDSSuite queries: $queries")
    }

    val comparisonResults = executeBenchmark(queries)

    if (!args.disableResultCheck) {
      printResultComparison(comparisonResults)
    }

    if (args.enablePlanCheck) {
      printPlanStability(comparisonResults)
    }

    val failed = comparisonResults.count(r => !r.success || !r.planStable)
    if (failed > 0) {
      println(s"\nTPC-DS test FAILED: $failed/${comparisonResults.length} queries failed")
      1
    } else {
      println(s"\nTPC-DS test PASSED: ${comparisonResults.length}/${comparisonResults.length}")
      0
    }
  }

  private def executeBenchmark(queries: Seq[String]): Seq[ComparisonResult] = {
    println("execute baseline ....")
    var baselineResults: Map[String, SingleQueryResult] = Map.empty
    if (!args.disableResultCheck) {
      setupTables(args.dataLocation, sessions.baselineSession)
      baselineResults = queryRunner.runQueries(sessions.baselineSession, queries)
    }

    println("execute auron ....")
    setupTables(args.dataLocation, sessions.auronSession)
    val auronResults = queryRunner.runQueries(sessions.auronSession, queries)

    if (args.disableResultCheck) {
      baselineResults = auronResults
    }

    val comparisonResults = queries.map { queryId =>
      resutComparator.compare(baselineResults(queryId), auronResults(queryId))
    }

    if (args.enablePlanCheck || args.regenGoldenFiles) {
      comparisonResults.foreach(comparisonResult => {
        val testResult = auronResults(comparisonResult.queryId)
        val planStable = planStability.validate(testResult)
        comparisonResult.planStable = planStable
      })
    }
    comparisonResults
  }

  private def printResultComparison(results: Seq[ComparisonResult]): Unit = {
    println("\n" + "=" * 100)
    println("TPC-DS Result Comparison (Vanilla Spark vs Auron)")
    println("=" * 100)
    println("Query | Rows(V/A) | Time(V/A) | Speedup | Result")
    println("-" * 100)

    results.foreach { r =>
      val rowsV = r.baselineRows
      val rowsA = r.testRows
      val timeV = r.baselineTime
      val timeA = r.testTime
      val speedup = if (timeA > 0) timeV / timeA else 0.0

      val dataCell =
        if (args.disableResultCheck) "-" else (if (r.dataMatch) "✓" else "✗")
      val status =
        if (args.disableResultCheck) "✅"
        else if (r.success) "✅"
        else "❌"

      println(
        f"$status ${r.queryId}%-6s | $rowsV%5d/$rowsA%5d | " +
          f"$timeV%7.2f/$timeA%7.2f s | ${speedup}%6.2fx | $dataCell")
    }

    val total = results.length
    val resultPass =
      if (args.disableResultCheck) total
      else results.count(_.success)

    println("-" * 100)
    println(s"Result comparison passed: $resultPass/$total")
  }

  private def printPlanStability(results: Seq[ComparisonResult]): Unit = {
    if (!(args.enablePlanCheck || args.regenGoldenFiles)) return

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
