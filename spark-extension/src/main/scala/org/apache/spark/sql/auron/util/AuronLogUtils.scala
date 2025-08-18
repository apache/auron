/*
 * Copyright 2022 The Auron Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.auron.util

import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.execution.SparkPlan

object AuronLogUtils extends Logging {

  def logDebugPlanConversion(plan: SparkPlan, fields: => Seq[(String, Any)] = Nil): Unit = {
    if (log.isDebugEnabled) {
      val header = s"Converting ${plan.nodeName}: ${Shims.get.simpleStringWithNodeId(plan)}"
      val body = fields.map { case (k, v) => s"  $k: $v" }.mkString("\n")
      logDebug(s"$header\n$body".trim)
    }
  }
}
