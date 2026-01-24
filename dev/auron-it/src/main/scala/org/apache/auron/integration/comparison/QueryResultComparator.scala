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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DoubleType

import org.apache.auron.integration.QueryExecutionResult

case class ComparisonResult(
    queryId: String,
    baselineRows: Long,
    testRows: Long,
    baselineTime: Double,
    testTime: Double,
    rowMatch: Boolean,
    dataMatch: Boolean,
    passed: Boolean = false,
    var planStable: Boolean = true)

trait QueryComparator {
  def compare(baseline: QueryExecutionResult, test: QueryExecutionResult): ComparisonResult
}

class QueryResultComparator extends QueryComparator {
  private val colSep = "<|COL|>"

  // scalastyle:off println
  override def compare(
      baseline: QueryExecutionResult,
      test: QueryExecutionResult): ComparisonResult = {
    val rowMatch = baseline.rowCount == test.rowCount
    val dataMatch = checkQueryResult(baseline.queryId, baseline.rows, test.rows)
    val passed = rowMatch && dataMatch && (baseline.success == test.success)

    ComparisonResult(
      queryId = baseline.queryId,
      baselineRows = baseline.rowCount,
      testRows = test.rowCount,
      baselineTime = baseline.durationSec,
      testTime = test.durationSec,
      rowMatch = rowMatch,
      dataMatch = dataMatch,
      passed = passed)
  }

  protected def checkQueryResult(
      queryId: String,
      baseline: Array[Row],
      test: Array[Row]): Boolean = {

    if (baseline.nonEmpty && baseline.length > 0 && baseline(0).schema.exists(
        _.dataType == DoubleType)) {
      compareDoubleResult(queryId, baseline, test)
    } else {
      compareResultStr(queryId, baseline, test)
    }
  }

  private def formatResultContent(rows: Array[Row]): String = {
    val rowStrings = rows.map(_.mkString(colSep))
    s"${rows.length}\n${rowStrings.mkString("\n")}\n"
  }

  protected def compareResultStr(
      queryId: String,
      baseline: Array[Row],
      test: Array[Row]): Boolean = {
    val expectedResult = formatResultContent(baseline)
    val actualContent = formatResultContent(test)
    if (expectedResult != actualContent) {
      println(s"""
              |=== $queryId result does NOT match expected ===
              |[Expected]
              |${expectedResult}
              |[Actual]
              |${actualContent}
              |""".stripMargin)
      return false
    }
    true
  }

  protected def compareDoubleResult(
      queryId: String,
      baseline: Array[Row],
      test: Array[Row],
      tolerance: Double = 1e-6): Boolean = {

    baseline.zipWithIndex.foreach { case (actualRow, rowIdx) =>
      val expectedRow: Row = test(rowIdx)
      actualRow.schema.zipWithIndex.foreach { case (field, colIdx) =>
        if (actualRow.isNullAt(colIdx) || expectedRow.isNullAt(colIdx)) {
          if (actualRow.isNullAt(colIdx) != expectedRow.isNullAt(colIdx)) {
            println(
              s"Mismatch in $queryId row $rowIdx col $colIdx: " +
                s"expected null, got ${expectedRow(colIdx)}")
            return false
          }
        } else {
          field.dataType match {
            case DoubleType =>
              val actualValue = actualRow.getDouble(colIdx)
              val expectedValue = expectedRow.getDouble(colIdx)
              val diff = math.abs(actualValue - expectedValue)
              if (diff > tolerance) {
                println(
                  s"Floating-point mismatch in $queryId row $rowIdx col $colIdx: " +
                    s"expected ${expectedValue}, got ${actualValue} (diff=$diff)")
                return false
              }
            case _ =>
              val actualValue = actualRow.get(colIdx).toString
              val expectedValue = expectedRow.get(colIdx).toString
              if (actualValue != expectedValue) {
                println(
                  s"Mismatch in $queryId row $rowIdx col $colIdx: " +
                    s"expected $expectedValue, got $actualValue")
                return false
              }
          }
        }
      }
    }
    true
  }
  // scalastyle:on
}
