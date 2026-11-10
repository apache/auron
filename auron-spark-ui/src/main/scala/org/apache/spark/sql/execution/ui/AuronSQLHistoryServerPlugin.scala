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

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.status.{AppHistoryServerPlugin, ElementTrackingStore}
import org.apache.spark.ui.SparkUI

class AuronSQLHistoryServerPlugin extends AppHistoryServerPlugin with Logging {
  import AuronSQLHistoryServerPlugin.AuronEnabled

  override def createListeners(
      conf: SparkConf,
      store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new AuronSQLAppStatusListener(conf, store))
  }

  override def setupUI(ui: SparkUI): Unit = {
    val properties =
      try ui.store.environmentInfo().sparkProperties.toMap
      catch {
        case _: NoSuchElementException =>
          logWarning(
            "Application environment information is unavailable; using Auron's default enabled state")
          Map.empty[String, String]
      }

    val auronEnabled = properties
      .get(AuronEnabled.key)
      .orElse(properties.get(AuronEnabled.alternative))
      .map(java.lang.Boolean.parseBoolean)
      .getOrElse(AuronEnabled.defaultValue)
    if (auronEnabled) {
      val sqlStatusStore = new AuronSQLAppStatusStore(ui.store.store)
      new AuronSQLTab(sqlStatusStore, ui)
    }
  }

  override def displayOrder: Int = 0
}

object AuronSQLHistoryServerPlugin {
  // Keep this UI-local definition in sync with SparkAuronConfiguration.AURON_ENABLED.
  // spark-extension depends on this module, so the UI cannot import that definition.
  private[ui] object AuronEnabled {
    val key = "spark.auron.enabled"
    val alternative = "spark.auron.enable"
    val defaultValue = true
  }
}
