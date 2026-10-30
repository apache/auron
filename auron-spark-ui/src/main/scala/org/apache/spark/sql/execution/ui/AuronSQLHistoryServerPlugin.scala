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

  override def createListeners(
      conf: SparkConf,
      store: ElementTrackingStore): Seq[SparkListener] = {
    Seq(new AuronSQLAppStatusListener(conf, store))
  }

  override def setupUI(ui: SparkUI): Unit = {
    val sqlStatusStore = new AuronSQLAppStatusStore(ui.store.store)
    val properties =
      try ui.store.environmentInfo().sparkProperties.toMap
      catch {
        case _: NoSuchElementException =>
          logWarning(
            "Application environment information is unavailable; using recorded Auron data")
          Map.empty[String, String]
      }

    def enabled(key: String, altKey: String): Boolean =
      !properties.get(key).orElse(properties.get(altKey)).exists(_.equalsIgnoreCase("false"))

    val configured = properties
      .get("spark.sql.extensions")
      .exists(
        _.split(",").exists(_.trim == "org.apache.spark.sql.auron.AuronSparkSessionExtension")) ||
      sqlStatusStore.buildInfo().isDefined
    val auronEnabled = enabled("spark.auron.enabled", "spark.auron.enable")
    val uiEnabled = enabled("spark.auron.ui.enabled", "spark.auron.ui.enable")

    // Session-level settings can enable Auron even when the application default is disabled.
    if ((configured && auronEnabled && uiEnabled) || sqlStatusStore.hasPlanDiagnostics()) {
      new AuronSQLTab(sqlStatusStore, ui)
    }
  }

  override def displayOrder: Int = 0
}
