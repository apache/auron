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
package org.apache.spark.sql.execution.ui

import scala.xml.{Node, NodeSeq}

import org.apache.spark.ui.WebUIPage

import org.apache.auron.spark.ui.AuronPlanFallbackEvent
import org.apache.auron.sparkver

private[ui] object AuronExecutionPage {
  private def displayOperator(label: String): String = {
    val separator = label.indexOf(' ')
    if (separator > 0 && label.substring(0, separator).forall(_.isDigit)) {
      s"(${label.substring(0, separator)}) ${label.substring(separator + 1)}"
    } else {
      label
    }
  }

  private[ui] def renderContent(
      execution: Option[AuronPlanFallbackEvent],
      physicalPlan: Option[String]): NodeSeq = execution match {
    case Some(report) =>
      val omitted = math.max(0, report.numFallbackNodes - report.fallbackNodeToReason.size)
      <div>
        <h4>Physical Plan</h4>
        {
        physicalPlan.filter(_.nonEmpty) match {
          case Some(plan) => <pre>{plan}</pre>
          case None => <p>Spark did not retain a physical plan for this query.</p>
        }
      }
        <h4>Fallback Conversions</h4>
        {
        if (report.numFallbackNodes == 0) {
          <p>No fallback was recorded for this query.</p>
        } else if (report.fallbackNodeToReason.isEmpty) {
          <p>No fallback details were retained for this query.</p>
        } else {
          <table class="table table-bordered table-striped">
            <thead><tr><th>Operator</th><th>Fallback Reason</th></tr></thead>
            <tbody>{
            report.fallbackNodeToReason.toSeq
              .sortBy { case (label, _) =>
                scala.util.Try(label.takeWhile(_.isDigit).toInt).getOrElse(Int.MaxValue)
              }
              .map { case (label, reason) =>
                <tr><td>{displayOperator(label)}</td><td>{reason}</td></tr>
              }
          }</tbody>
          </table>
        }
      }
        {
        if (omitted > 0) {
          <p>{omitted} additional fallback details were omitted by the event size limit.</p>
        } else NodeSeq.Empty
      }
      </div>
    case None =>
      <p>No legacy fallback report is available for this query.</p>
  }
}

/** Preserves existing links to the legacy query-analysis route. */
private[ui] class AuronExecutionPage(parent: AuronSQLTab) extends WebUIPage("execution") {
  @sparkver("3.0 / 3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  override def render(request: javax.servlet.http.HttpServletRequest): Seq[Node] =
    new AuronAllExecutionsPage(parent).render(request)

  @sparkver("4.0 / 4.1 / 4.2")
  override def render(request: jakarta.servlet.http.HttpServletRequest): Seq[Node] =
    new AuronAllExecutionsPage(parent).render(request)
}
