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

import scala.collection.mutable

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.scheduler.{SparkListenerEnvironmentUpdate, SparkListenerEvent}
import org.apache.spark.status.{AppStatusListener, AppStatusStore, ElementTrackingStore}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.kvstore.InMemoryStore

import org.apache.auron.spark.ui.{AuronBuildInfoEvent, AuronPlanDiagnosticEvent, AuronPlanDiagnosticNode}

class AuronSQLHistoryServerPluginSuite extends SparkFunSuite {
  import AuronSQLHistoryServerPlugin.AuronEnabled

  private val buildInfo = AuronBuildInfoEvent(mutable.LinkedHashMap("version" -> "test"))
  private val snapshot = AuronPlanDiagnosticEvent(
    1,
    100,
    "plan",
    Seq(AuronPlanDiagnosticNode(1, "LocalTableScan", "Spark (no reason recorded)", "")),
    Map.empty)

  private def withHistoryUI(
      properties: Map[String, String],
      events: Seq[SparkListenerEvent] = Seq.empty,
      environmentAvailable: Boolean = true)(check: SparkUI => Unit): Unit = {
    // These are SHS settings, not the application's settings.
    val conf = new SparkConf(false)
      .set(AuronEnabled.key, "false")
    val kv = new ElementTrackingStore(new InMemoryStore, conf)
    try {
      if (environmentAvailable) {
        new AppStatusListener(kv, conf, live = false).onEnvironmentUpdate(
          SparkListenerEnvironmentUpdate(
            Map("JVM Information" -> Seq.empty, "Spark Properties" -> properties.toSeq)))
      }
      val plugin = new AuronSQLHistoryServerPlugin
      val listeners = plugin.createListeners(conf, kv)
      events.foreach(event => listeners.foreach(_.onOtherEvent(event)))
      val ui = SparkUI.create(
        None,
        new AppStatusStore(kv),
        conf,
        new SecurityManager(conf),
        "test",
        "/history/test",
        0L)
      plugin.setupUI(ui)
      check(ui)
    } finally kv.close()
  }

  private def hasAuronTab(ui: SparkUI): Boolean = ui.getTabs.exists(_.isInstanceOf[AuronSQLTab])

  test("missing enabled property uses Auron's default") {
    withHistoryUI(Map.empty) { ui =>
      assert(hasAuronTab(ui) == AuronEnabled.defaultValue)
    }
  }

  test(
    "enabled property alone shows the tab even without native conversions or build information") {
    Seq(Seq.empty[SparkListenerEvent], Seq(snapshot)).foreach { events =>
      withHistoryUI(Map(AuronEnabled.key -> "true"), events) { ui =>
        assert(hasAuronTab(ui))
      }
    }
  }

  test("disabled property hides the tab even when Auron data was recorded") {
    Seq(AuronEnabled.key, AuronEnabled.alternative).foreach { key =>
      withHistoryUI(Map(key -> "false"), Seq(buildInfo, snapshot)) { ui =>
        assert(!hasAuronTab(ui), key)
      }
    }
  }

  test("primary enabled property takes precedence over the legacy alias") {
    Seq(true, false).foreach { enabled =>
      withHistoryUI(
        Map(
          AuronEnabled.key -> enabled.toString,
          AuronEnabled.alternative -> (!enabled).toString)) { ui =>
        assert(hasAuronTab(ui) == enabled)
      }
    }
  }

  test("missing environment information uses Auron's default enabled state") {
    withHistoryUI(Map.empty, environmentAvailable = false) { ui =>
      assert(hasAuronTab(ui) == AuronEnabled.defaultValue)
    }
  }
}
