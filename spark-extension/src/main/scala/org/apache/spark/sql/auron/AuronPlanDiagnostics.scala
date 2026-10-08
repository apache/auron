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
package org.apache.spark.sql.auron

import java.util.IdentityHashMap

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{InputAdapter, ReusedSubqueryExec, SparkPlan, SQLExecution, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.ui.AuronEventUtils
import org.apache.spark.util.Utils

import org.apache.auron.configuration.AuronConfiguration
import org.apache.auron.spark.configuration.SparkAuronConfiguration
import org.apache.auron.spark.ui.{AuronPlanDiagnosticEvent, AuronPlanDiagnosticNode}

/** Diagnostics must neither mutate Spark's operator IDs nor prevent query execution. */
object AuronPlanDiagnostics extends Logging {
  private val configurationKeys = Seq(
    "spark.auron.enabled",
    "spark.auron.enable",
    "spark.auron.parquet.enable.pageFiltering",
    "spark.auron.parquet.enable.bloomFilter",
    "spark.auron.process.vmrss.memoryFraction",
    "spark.auron.ui.enabled",
    "spark.shuffle.manager",
    "spark.sql.adaptive.enabled",
    "spark.sql.session.timeZone",
    "spark.sql.ansi.enabled",
    "spark.sql.shuffle.partitions",
    "spark.memory.offHeap.enabled",
    "spark.memory.offHeap.size")

  def collect(plan: SparkPlan): Seq[AuronPlanDiagnosticNode] = {
    val visited = new IdentityHashMap[SparkPlan, java.lang.Boolean]()
    val nodes = ArrayBuffer.empty[AuronPlanDiagnosticNode]
    def visit(p: SparkPlan): Unit = {
      if (visited.put(p, java.lang.Boolean.TRUE) != null) {
        return
      }
      val reason = p.getTagValue(AuronConvertStrategy.neverConvertReasonTag).getOrElse("")
      val status = p match {
        case _: NativeSupports => "Native"
        case _: AdaptiveSparkPlanExec | _: QueryStageExec | _: ReusedExchangeExec |
            _: ReusedSubqueryExec | _: InputAdapter | _: WholeStageCodegenExec =>
          "Wrapper"
        case _ if reason.nonEmpty => "Fallback"
        case _ => "Spark (no reason recorded)"
      }
      nodes += AuronPlanDiagnosticNode(
        nodes.size + 1,
        p.nodeName,
        status,
        if (status == "Wrapper") "" else reason)
      p match {
        case a: AdaptiveSparkPlanExec => visit(a.executedPlan)
        case q: QueryStageExec => visit(q.plan)
        case r: ReusedExchangeExec => visit(r.child)
        case r: ReusedSubqueryExec => visit(r.child)
        case _ => p.children.foreach(visit)
      }
      p.innerChildren.foreach {
        case child: SparkPlan => visit(child)
        case _ =>
      }
    }
    visit(plan)
    nodes.toVector
  }

  def publish(session: SparkSession, plan: SparkPlan): Unit = {
    if (!SparkAuronConfiguration.AURON_ENABLED.get() ||
      !SparkAuronConfiguration.UI_ENABLED.get()) {
      return
    }
    val sc = session.sparkContext
    Option(sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)).foreach { id =>
      try {
        // Only non-secret diagnostic settings are captured, never arbitrary session options.
        val effectiveAuronSettings = Seq(
          SparkAuronConfiguration.AURON_ENABLED,
          SparkAuronConfiguration.UI_ENABLED,
          SparkAuronConfiguration.ENABLE_SHUFFLE_EXCHANGE,
          SparkAuronConfiguration.ENABLE_SCAN_PARQUET,
          SparkAuronConfiguration.ENABLE_DATA_WRITING,
          SparkAuronConfiguration.ENABLE_DATA_WRITING_PARQUET,
          SparkAuronConfiguration.ENABLE_DATA_WRITING_ORC,
          SparkAuronConfiguration.PARQUET_ENABLE_PAGE_FILTERING,
          SparkAuronConfiguration.PARQUET_ENABLE_BLOOM_FILTER,
          SparkAuronConfiguration.PROCESS_MEMORY_FRACTION,
          SparkAuronConfiguration.SPILL_COMPRESSION_CODEC,
          SparkAuronConfiguration.RSS_SPILL_MEMORY_FRACTION,
          SparkAuronConfiguration.RSS_SPILL_MEMORY_SIZE,
          SparkAuronConfiguration.ON_HEAP_SPILL_MEM_FRACTION,
          SparkAuronConfiguration.ORC_BATCH_SIZE,
          AuronConfiguration.BATCH_SIZE,
          AuronConfiguration.MEMORY_FRACTION,
          AuronConfiguration.SUGGESTED_BATCH_MEM_SIZE)
          .map(option => s"spark.${option.key}" -> option.get().toString)
          .toMap
        val settings = configurationKeys.flatMap { key =>
          session.conf.getOption(key).orElse(sc.getConf.getOption(key)).map(key -> _)
        }.toMap ++ effectiveAuronSettings
        val redaction = session.sessionState.conf.stringRedactionPattern
        val nodes =
          collect(plan).map(n => n.copy(reason = Utils.redact(redaction, n.reason).take(8192)))
        AuronEventUtils.post(
          sc,
          AuronPlanDiagnosticEvent(
            id.toLong,
            System.currentTimeMillis(),
            Utils.redact(redaction, plan.treeString).take(65536),
            nodes,
            settings))
      } catch {
        case NonFatal(e) => logWarning("Unable to record Auron plan diagnostics", e)
      }
    }
  }
}
