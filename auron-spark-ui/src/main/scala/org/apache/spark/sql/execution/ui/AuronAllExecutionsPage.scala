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

import javax.servlet.http.HttpServletRequest

import scala.xml.{Node, NodeSeq}

import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}

private[ui] class AuronAllExecutionsPage(parent: AuronSQLTab) extends WebUIPage("") with Logging {

  private val sqlStore = parent.sqlStore

  override def render(request: HttpServletRequest): Seq[Node] = {
    val buildInfo = sqlStore.buildInfo()
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

    UIUtils.headerSparkPage(request, "Auron", summary, parent)
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
