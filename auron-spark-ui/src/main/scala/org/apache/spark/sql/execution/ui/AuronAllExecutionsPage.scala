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

import java.net.URLEncoder
import java.nio.charset.StandardCharsets.UTF_8
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters.mapAsScalaMapConverter
import scala.collection.mutable
import scala.xml.{Node, NodeSeq, Unparsed}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.StringUtils.PlanStringConcat
import org.apache.spark.ui.{PagedDataSource, PagedTable, UIUtils, WebUIPage}
import org.apache.spark.util.Utils

private[ui] class AuronAllExecutionsPage(parent: AuronSQLTab) extends WebUIPage("") with Logging {

  private val sqlStore = parent.sqlStore

  override def render(request: HttpServletRequest): Seq[Node] = {
    val buildInfo = sqlStore.buildInfo()
    val data = sqlStore.executionsList()

    val content = {
      val _content = mutable.ListBuffer[Node]()

      val auronPageTable =
        executionsTable(request, "auron", data)

      _content ++=
        <span id="auron" class="collapse-aggregated-runningExecutions collapse-table"
              onClick="collapseTable('collapse-aggregated-runningExecutions',
              'aggregated-runningExecutions')">
          <h4>
            <span class="collapse-table-arrow arrow-open"></span>
            <a href="#auron">
              Queries:
            </a>{data.size}
          </h4>
        </span> ++
          <div class="aggregated-runningExecutions collapsible-table">
            {auronPageTable}
          </div>

      _content
    }
    content ++=
      <script>
        function clickDetail(details) {{
        details.parentNode.querySelector('.stage-details').classList.toggle('collapsed')
        }}
      </script>

    val infos =
      UIUtils.listingTable(propertyHeader, propertyRow, buildInfo.info, fixedWidth = true)
    val summary: NodeSeq =
      <div>
        <div>
          <span class="collapse-sql-properties collapse-table"
                onClick="collapseTable('collapse-sql-properties', 'sql-properties')">
            <h4>
              <span class="collapse-table-arrow arrow-open"></span>
              <a>Auron Build Information</a>
            </h4>
          </span>
          <div class="sql-properties collapsible-table">
            {infos}
          </div>
        </div>
        <br/>
      </div>

    UIUtils.headerSparkPage(request, "Auron", summary ++ content, parent)
  }

  private def propertyHeader = Seq("Name", "Value")

  private def propertyRow(kv: (String, String)) = <tr>
    <td>
      {kv._1}
    </td> <td>
      {kv._2}
    </td>
  </tr>

  private def executionsTable(
      request: HttpServletRequest,
      executionTag: String,
      executionData: Seq[AuronSQLExecutionUIData]): Seq[Node] = {

    val executionPage =
      Option(request.getParameter(s"$executionTag.page")).map(_.toInt).getOrElse(1)

    val tableHeaderId = executionTag

    try {
      new AuronExecutionPagedTable(
        request,
        parent,
        executionData,
        tableHeaderId,
        executionTag,
        UIUtils.prependBaseUri(request, parent.basePath),
        "auron").table(executionPage)
    } catch {
      case e @ (_: IllegalArgumentException | _: IndexOutOfBoundsException) =>
        <div class="alert alert-error">
          <p>Error while rendering execution table:</p>
          <pre>
            {Utils.exceptionString(e)}
          </pre>
        </div>
    }
  }
}

private[ui] class AuronExecutionPagedTable(
    request: HttpServletRequest,
    parent: AuronSQLTab,
    data: Seq[AuronSQLExecutionUIData],
    tableHeaderId: String,
    executionTag: String,
    basePath: String,
    subPath: String)
    extends PagedTable[AuronExecutionTableRowData] {

  private val (sortColumn, desc, pageSize) = getAuronTableParameters(request, executionTag, "ID")

  private val encodedSortColumn = URLEncoder.encode(sortColumn, UTF_8.name())

  override val dataSource = new AuronExecutionDataSource(data, pageSize, sortColumn, desc)

  private val parameterPath =
    s"$basePath/$subPath/?${getAuronParameterOtherTable(request, executionTag)}"

  override def tableId: String = s"$executionTag-table"

  override def tableCssClass: String =
    "table table-bordered table-sm table-striped table-head-clickable table-cell-width-limited"

  override def pageLink(page: Int): String = {
    parameterPath +
      s"&$pageNumberFormField=$page" +
      s"&$executionTag.sort=$encodedSortColumn" +
      s"&$executionTag.desc=$desc" +
      s"&$pageSizeFormField=$pageSize" +
      s"#$tableHeaderId"
  }

  /**
   * Returns parameters of other tables in the page.
   */
  def getAuronParameterOtherTable(request: HttpServletRequest, tableTag: String): String = {
    request.getParameterMap.asScala
      .filterNot(_._1.startsWith(tableTag))
      .map(parameter => parameter._1 + "=" + parameter._2(0))
      .mkString("&")
  }

  /**
   * Returns parameter of this table.
   */
  def getAuronTableParameters(
      request: HttpServletRequest,
      tableTag: String,
      defaultSortColumn: String): (String, Boolean, Int) = {
    val parameterSortColumn = request.getParameter(s"$tableTag.sort")
    val parameterSortDesc = request.getParameter(s"$tableTag.desc")
    val parameterPageSize = request.getParameter(s"$tableTag.pageSize")
    val sortColumn = Option(parameterSortColumn)
      .map { sortColumn =>
        UIUtils.decodeURLParameter(sortColumn)
      }
      .getOrElse(defaultSortColumn)
    val desc =
      Option(parameterSortDesc).map(_.toBoolean).getOrElse(sortColumn == defaultSortColumn)
    val pageSize = Option(parameterPageSize).map(_.toInt).getOrElse(100)

    (sortColumn, desc, pageSize)
  }

  override def pageSizeFormField: String = s"$executionTag.pageSize"

  override def pageNumberFormField: String = s"$executionTag.page"

  override def goButtonFormPath: String =
    s"$parameterPath&$executionTag.sort=$encodedSortColumn&$executionTag.desc=$desc#$tableHeaderId"

  // Information for each header: title, sortable, tooltip
  private val headerInfo: Seq[(String, Boolean, Option[String])] = {
    Seq(
      ("ID", true, None),
      ("Description", true, None),
      ("Num Auron Nodes", true, None),
      ("Num Fallback Nodes", true, None))
  }

  override def headers: Seq[Node] = {
    isAuronSortColumnValid(headerInfo, sortColumn)

    headerAuronRow(
      headerInfo,
      desc,
      pageSize,
      sortColumn,
      parameterPath,
      executionTag,
      tableHeaderId)
  }

  def headerAuronRow(
      headerInfo: Seq[(String, Boolean, Option[String])],
      desc: Boolean,
      pageSize: Int,
      sortColumn: String,
      parameterPath: String,
      tableTag: String,
      headerId: String): Seq[Node] = {
    val row: Seq[Node] = {
      headerInfo.map { case (header, sortable, tooltip) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            parameterPath +
              s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
              s"&$tableTag.desc=${!desc}" +
              s"&$tableTag.pageSize=$pageSize" +
              s"#$headerId")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN

          <th>
            <a href={headerLink}>
              <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                {header}&nbsp;{Unparsed(arrow)}
              </span>
            </a>
          </th>
        } else {
          if (sortable) {
            val headerLink = Unparsed(
              parameterPath +
                s"&$tableTag.sort=${URLEncoder.encode(header, UTF_8.name())}" +
                s"&$tableTag.pageSize=$pageSize" +
                s"#$headerId")

            <th>
              <a href={headerLink}>
                <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                  {header}
                </span>
              </a>
            </th>
          } else {
            <th>
              <span data-toggle="tooltip" data-placement="top" title={tooltip.getOrElse("")}>
                {header}
              </span>
            </th>
          }
        }
      }
    }
    <thead>
      <tr>
        {row}
      </tr>
    </thead>
  }

  def isAuronSortColumnValid(
      headerInfo: Seq[(String, Boolean, Option[String])],
      sortColumn: String): Unit = {
    if (!headerInfo.filter(_._2).map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"Unknown column: $sortColumn")
    }
  }

  override def row(executionTableRow: AuronExecutionTableRowData): Seq[Node] = {
    val executionUIData = executionTableRow.executionUIData

    <tr>
      <td>
        {executionUIData.executionId.toString}
      </td>
      <td>
        {descriptionCell(executionUIData)}
      </td>
      <td sorttable_customkey={executionUIData.numAuronNodes.toString}>
        {executionUIData.numAuronNodes.toString}
      </td>
      <td sorttable_customkey={executionUIData.numFallbackNodes.toString}>
        {executionUIData.numFallbackNodes.toString}
      </td>
    </tr>
  }

  private def descriptionCell(execution: AuronSQLExecutionUIData): Seq[Node] = {
    val details = if (execution.description != null && execution.description.nonEmpty) {
      val concat = new PlanStringConcat()
      concat.append("== Fallback Summary ==\n")
      val fallbackSummary = execution.fallbackNodeToReason
        .map { case (name, reason) =>
          val id = name.substring(0, 3)
          val nodeName = name.substring(4)
          s"(${id.toInt}) $nodeName: $reason"
        }
        .mkString("\n")
      concat.append(fallbackSummary)
      if (execution.fallbackNodeToReason.isEmpty) {
        concat.append("No fallback nodes")
      }
      concat.append("\n\n")
      concat.append(execution.fallbackDescription)

      <span onclick="this.parentNode.querySelector('.stage-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stage-details collapsed">
          <pre>{concat.toString()}</pre>
        </div>
    } else {
      Nil
    }

    val desc = if (execution.description != null && execution.description.nonEmpty) {
      <a href={executionURL(execution.executionId)} class="description-input">
        {execution.description}</a>
    } else {
      <a href={executionURL(execution.executionId)}>{execution.executionId}</a>
    }

    <div>{desc}{details}</div>
  }

  private def executionURL(executionID: Long): String =
    s"${UIUtils.prependBaseUri(request, parent.basePath)}/SQL/execution/?id=$executionID"
}

private[ui] class AuronExecutionTableRowData(val executionUIData: AuronSQLExecutionUIData)

private[ui] class AuronExecutionDataSource(
    executionData: Seq[AuronSQLExecutionUIData],
    pageSize: Int,
    sortColumn: String,
    desc: Boolean)
    extends PagedDataSource[AuronExecutionTableRowData](pageSize) {

  // Convert ExecutionData to ExecutionTableRowData which contains the final contents to show
  // in the table so that we can avoid creating duplicate contents during sorting the data
  private val data = executionData.map(executionRow).sorted(ordering(sortColumn, desc))

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[AuronExecutionTableRowData] =
    data.slice(from, to)

  private def executionRow(
      executionUIData: AuronSQLExecutionUIData): AuronExecutionTableRowData = {
    new AuronExecutionTableRowData(executionUIData)
  }

  /** Return Ordering according to sortColumn and desc. */
  private def ordering(
      sortColumn: String,
      desc: Boolean): Ordering[AuronExecutionTableRowData] = {
    val ordering: Ordering[AuronExecutionTableRowData] = sortColumn match {
      case "ID" => Ordering.by(_.executionUIData.executionId)
      case "Description" => Ordering.by(_.executionUIData.fallbackDescription)
      case "Num Auron Nodes" => Ordering.by(_.executionUIData.numAuronNodes)
      case "Num Fallback Nodes" => Ordering.by(_.executionUIData.numFallbackNodes)
      case unknownColumn => throw new IllegalArgumentException(s"Unknown column: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }
}
