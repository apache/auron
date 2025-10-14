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

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.status.ElementTrackingStore

import org.apache.auron.spark.ui.AuronBuildInfoEvent

class AuronSQLAppStatusListener(conf: SparkConf, kvstore: ElementTrackingStore)
    extends SparkListener
    with Logging {

  def getAuronBuildInfo(): Long = {
    kvstore.count(classOf[AuronBuildInfoUIData])
  }

  private def onAuronBuildInfo(event: AuronBuildInfoEvent): Unit = {
    val uiData = new AuronBuildInfoUIData(event.info.toSeq)
    kvstore.write(uiData)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: AuronBuildInfoEvent => onAuronBuildInfo(e)
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
