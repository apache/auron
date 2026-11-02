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
package org.apache.auron.spark.ui

import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import org.apache.spark.scheduler.SparkListenerEvent

sealed trait AuronEvent extends SparkListenerEvent {}

case class AuronBuildInfoEvent(info: mutable.LinkedHashMap[String, String]) extends AuronEvent {}

/** A conversion snapshot; AQE can publish several snapshots for one SQL execution. */
case class AuronPlanDiagnosticEvent(
    executionId: Long,
    timestamp: Long,
    physicalPlan: String,
    nodes: Seq[AuronPlanDiagnosticNode],
    configurations: Map[String, String])
    extends AuronEvent

case class AuronPlanDiagnosticNode(id: Int, name: String, status: String, reason: String)

/** Compatibility with legacy fallback-report event logs. */
@JsonIgnoreProperties(ignoreUnknown = true)
case class AuronPlanFallbackEvent(
    executionId: Long,
    numAuronNodes: Int,
    numFallbackNodes: Int,
    physicalPlanDescription: String,
    fallbackNodeToReason: Map[String, String])
    extends AuronEvent

object AuronFallbackReason {
  val keptOnJvm = "Auron kept this operator on the JVM."

  def unsupportedOperator(operator: String): String =
    s"Auron does not support $operator."

  def conversionFailure(operator: String, failure: String): String = {
    val detail =
      Option(failure).getOrElse("").stripPrefix("assertion failed:").trim.stripSuffix(".")
    if (detail.nonEmpty) s"Auron could not convert $operator: $detail."
    else s"Auron could not convert $operator."
  }

  def followOn(operators: Seq[String]): String = {
    val inputs = operators.distinct
    if (inputs.size == 1) s"Kept on the JVM because ${inputs.head} runs there."
    else s"Kept on the JVM because these inputs run there: ${inputs.mkString(", ")}."
  }
}
