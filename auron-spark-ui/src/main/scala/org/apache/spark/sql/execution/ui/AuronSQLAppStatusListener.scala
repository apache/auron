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

import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.status.ElementTrackingStore

import org.apache.auron.spark.ui.{AuronBuildInfoEvent, AuronPlanDiagnosticEvent}

class AuronSQLAppStatusListener(conf: SparkConf, kvstore: ElementTrackingStore)
    extends SparkListener
    with Logging {

  private val store = new AuronSQLAppStatusStore(kvstore)
  private val retained = math.max(0, conf.getInt("spark.sql.ui.retainedExecutions", 1000))

  kvstore.addTrigger(classOf[AuronExecutionUIData], retained) { count =>
    val iter = kvstore.view(classOf[AuronExecutionUIData]).closeableIterator()
    val ids =
      try {
        iter.asScala
          .filter(_.endedAt > 0)
          .take(math.max(0L, count - retained).toInt)
          .map(_.executionId)
          .toVector
      } finally iter.close()
    ids.foreach(id => kvstore.delete(classOf[AuronExecutionUIData], id))
  }

  private def current(id: Long): AuronExecutionUIData =
    store.execution(id).getOrElse(new AuronExecutionUIData(id, "", 0L, 0L, Seq.empty))

  def getAuronBuildInfo(): Long = {
    kvstore.count(classOf[AuronBuildInfoUIData])
  }

  private def onAuronBuildInfo(event: AuronBuildInfoEvent): Unit = {
    val uiData = new AuronBuildInfoUIData(event.info.toSeq)
    kvstore.write(uiData)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: AuronBuildInfoEvent => onAuronBuildInfo(e)
    case e: SparkListenerSQLExecutionStart =>
      val old = current(e.executionId)
      kvstore.write(
        new AuronExecutionUIData(
          e.executionId,
          e.description,
          e.time,
          old.endedAt,
          old.snapshots),
        true)
    case e: SparkListenerSQLExecutionEnd =>
      val old = current(e.executionId)
      kvstore.write(
        new AuronExecutionUIData(
          e.executionId,
          old.description,
          old.startedAt,
          e.time,
          old.snapshots),
        true)
    case e: AuronPlanDiagnosticEvent =>
      val old = current(e.executionId)
      // Keep bounded history; repeated identical snapshots do not consume space.
      val snapshots =
        if (old.snapshots.lastOption.exists(s =>
            s.physicalPlan == e.physicalPlan && s.nodes == e.nodes &&
              s.configurations == e.configurations)) {
          old.snapshots
        } else {
          (old.snapshots :+ e).takeRight(20)
        }
      kvstore.write(
        new AuronExecutionUIData(
          e.executionId,
          old.description,
          old.startedAt,
          old.endedAt,
          snapshots),
        true)
    case _ => // Ignore
  }

}
object AuronSQLAppStatusListener {
  def register(sc: SparkContext): Unit = {
    val kvStore = sc.statusStore.store.asInstanceOf[ElementTrackingStore]
    val listener = new AuronSQLAppStatusListener(sc.conf, kvStore)
    sc.listenerBus.addToStatusQueue(listener)
  }
}
