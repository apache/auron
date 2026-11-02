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

import scala.util.Try
import scala.xml.NodeSeq

import org.apache.spark.ui.UIUtils

/** Shared rendering for live and historical executions, independent of servlet API versions. */
private[ui] class AuronDiagnosticsView(sqlStore: AuronSQLAppStatusStore) {
  def render(id: String, page: String, base: String): NodeSeq = {
    if (id != null) {
      Try(id.toLong).toOption.filter(_ >= 0).flatMap(sqlStore.execution) match {
        case Some(execution) => executionDetails(execution, base)
        case None =>
          <div class="alert alert-info">Execution unavailable or no longer retained.</div>
      }
    } else {
      val size = 20
      val count = sqlStore.executionsCount()
      val last = math.max(1L, (count + size - 1) / size).min(Int.MaxValue / size).toInt
      val current = Try(Option(page).getOrElse("1").toInt).getOrElse(1).max(1).min(last)
      val rows = sqlStore.executions((current - 1) * size, size)
      <div>
        <h3>SQL conversion diagnostics ({count})</h3>
        <p>Counts describe the latest captured conversion snapshot, not the entire AQE query.</p>
        <table class="table table-bordered table-striped">
          <thead><tr><th>SQL ID</th><th>Description</th><th>Lifecycle</th>
            <th>Native</th><th>Fallback with reason</th><th>Other Spark</th><th>Snapshots</th></tr></thead>
          <tbody>{
        rows.map { e =>
          val nodes = e.snapshots.lastOption.toSeq.flatMap(_.nodes)
          def count(status: String): String = {
            if (e.snapshots.isEmpty) "Not available" else nodes.count(_.status == status).toString
          }
          <tr><td><a href={s"$base/auron/?id=${e.executionId}"}>{e.executionId}</a></td>
              <td>{e.description}</td><td>{
            if (e.endedAt > 0) "Finished" else "Running / planning"
          }</td>
              <td>{count("Native")}</td>
              <td>{count("Fallback")}</td>
              <td>{count("Spark (no reason recorded)")}</td>
              <td>{e.snapshots.size}</td></tr>
        }
      }</tbody>
        </table>
        {if (rows.isEmpty) <p>No SQL executions recorded.</p> else NodeSeq.Empty}
        <p><a href={s"$base/auron/?page=${math.max(1, current - 1)}"}>Previous</a>
          {s" - Page $current of $last - "}
          <a href={s"$base/auron/?page=${math.min(last, current + 1)}"}>Next</a></p>
      </div>
    }
  }

  private def executionDetails(e: AuronExecutionUIData, base: String): NodeSeq = {
    val metrics = sqlStore.metrics(e.executionId)
    <div>
      <p><a href={s"$base/auron/"}>All executions</a>{" | "}
        <a href={
      s"$base/SQL/execution/?id=${e.executionId}"
    }>Spark SQL execution, jobs and stages</a></p>
      <h3>SQL {e.executionId}: {e.description}</h3>
      <p>{
      if (e.startedAt > 0 && e.endedAt > 0) {
        s"Elapsed: ${e.endedAt - e.startedAt} ms"
      } else {
        "Execution timing is not available yet."
      }
    }</p>
      <h4>Conversion snapshots</h4>
      <p>The latest 20 distinct snapshots are retained. AQE may convert individual stages separately.
        Node IDs are local to each snapshot; wrappers are excluded from conversion counts.
        Other Spark means no fallback reason was recorded; it does not indicate native conversion.
        Plans are limited to 65,536 characters and individual reasons to 8,192 characters.</p>
      {
      if (e.snapshots.isEmpty) {
        <p>No conversion snapshot was captured for this execution.</p>
      } else {
        e.snapshots.reverse.map { snapshot =>
          <details><summary>Snapshot {new java.util.Date(snapshot.timestamp).toString}
            {
            s" (${snapshot.nodes.count(_.status == "Native")} native, " +
              s"${snapshot.nodes.count(_.status == "Fallback")} fallback, " +
              s"${snapshot.nodes.count(_.status == "Spark (no reason recorded)")} other Spark, " +
              s"${snapshot.nodes.count(_.status == "Wrapper")} wrappers)"
          }</summary>
            <h4>Operator diagnostics</h4>
            <table class="table table-bordered"><thead><tr><th>ID</th><th>Operator</th>
              <th>Conversion</th><th>Reason</th></tr></thead>
              <tbody>{
            snapshot.nodes.map(n =>
              <tr><td>{n.id}</td><td>{n.name}</td>
                <td>{n.status}</td><td><pre>{
                if (n.status == "Wrapper") {
                  "Planning/execution wrapper; excluded from conversion counts. See child operators."
                } else {
                  n.reason
                }
              }</pre></td></tr>)
          }</tbody></table>
            <h4>Physical plan</h4><pre>{snapshot.physicalPlan}</pre>
            <h4>Configuration at conversion</h4>
            {
            UIUtils.listingTable(
              propertyHeader,
              propertyRow,
              snapshot.configurations.toSeq.sortBy(_._1),
              fixedWidth = true)
          }
          </details>
        }
      }
    }
      <h4>Execution metrics</h4>
      <p>Metrics reported by Spark, including available native timing, row counts, memory,
        spill and shuffle measurements. Values retain Spark's units and aggregation semantics;
        they are not summed across operators. Metric IDs belong to Spark's plan graph.</p>
      {
      if (metrics.isEmpty) {
        <p>Metrics are not available yet or have been removed by Spark retention.</p>
      } else {
        <table class="table table-bordered"><thead><tr><th>Operator</th><th>Metric</th>
          <th>Value</th></tr></thead><tbody>{
          metrics.map { case (operator, name, value) =>
            <tr><td>{operator}</td><td>{name}</td><td><pre>{value}</pre></td></tr>
          }
        }</tbody></table>
      }
    }
    </div>
  }

  private def propertyHeader = Seq("Name", "Value")

  private def propertyRow(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td>
  </tr>

}
