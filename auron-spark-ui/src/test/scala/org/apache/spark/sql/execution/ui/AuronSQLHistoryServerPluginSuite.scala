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

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.scheduler.{SparkListenerEnvironmentUpdate, SparkListenerEvent}
import org.apache.spark.status.{AppStatusListener, AppStatusStore, ElementTrackingStore}
import org.apache.spark.ui.SparkUI
import org.apache.spark.util.kvstore.InMemoryStore
import org.scalatest.funsuite.AnyFunSuite

import org.apache.auron.spark.ui.{AuronBuildInfoEvent, AuronPlanDiagnosticEvent}

class AuronSQLHistoryServerPluginSuite extends AnyFunSuite {
  private val extension = "org.apache.spark.sql.auron.AuronSparkSessionExtension"
  private val buildInfo = AuronBuildInfoEvent(mutable.LinkedHashMap("version" -> "test"))

  private def withHistoryUI(
      properties: Map[String, String],
      events: Seq[SparkListenerEvent] = Seq.empty,
      environmentAvailable: Boolean = true)(check: SparkUI => Unit): Unit = {
    // These are SHS settings, not the application's settings.
    val conf = new SparkConf(false)
      .set("spark.sql.extensions", extension)
      .set("spark.auron.enabled", "false")
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

  test("applications without Auron do not get a tab even when they have SQL executions") {
    Seq(Map.empty[String, String], Map("spark.auron.enabled" -> "true")).foreach { properties =>
      withHistoryUI(properties, Seq(SparkListenerSQLExecutionEnd(1, 100))) { ui =>
        assert(!hasAuronTab(ui))
      }
    }
  }

  test("configured Auron applications keep the tab even when build information is missing") {
    withHistoryUI(Map("spark.sql.extensions" -> s"other.Extension, $extension ")) { ui =>
      assert(hasAuronTab(ui))
    }
  }

  test("disabled Auron or UI settings hide the tab despite build information and SQL records") {
    Seq(
      "spark.auron.enabled",
      "spark.auron.enable",
      "spark.auron.ui.enabled",
      "spark.auron.ui.enable").foreach { key =>
      withHistoryUI(
        Map("spark.sql.extensions" -> extension, key -> "false"),
        Seq(buildInfo, SparkListenerSQLExecutionEnd(1, 100))) { ui =>
        assert(!hasAuronTab(ui), key)
      }
    }
  }

  test("primary enabled settings take precedence over legacy aliases") {
    Seq("spark.auron", "spark.auron.ui").foreach { prefix =>
      Seq(true, false).foreach { enabled =>
        withHistoryUI(
          Map(
            "spark.sql.extensions" -> extension,
            s"$prefix.enabled" -> enabled.toString,
            s"$prefix.enable" -> (!enabled).toString)) { ui =>
          assert(hasAuronTab(ui) == enabled)
        }
      }
    }
  }

  test("recorded diagnostics preserve the tab when Auron was enabled for individual queries") {
    val snapshot = AuronPlanDiagnosticEvent(2, 100, "plan", Seq.empty, Map.empty)
    withHistoryUI(
      Map("spark.auron.enabled" -> "false"),
      Seq(SparkListenerSQLExecutionEnd(1, 100), snapshot)) { ui =>
      assert(hasAuronTab(ui))
    }
  }

  test("build information supports extensions registered without spark.sql.extensions") {
    withHistoryUI(Map.empty, Seq(buildInfo)) { ui =>
      assert(hasAuronTab(ui))
    }
  }

  test("missing environment information uses recorded Auron data") {
    withHistoryUI(Map.empty, environmentAvailable = false) { ui =>
      assert(!hasAuronTab(ui))
    }
    withHistoryUI(Map.empty, Seq(buildInfo), environmentAvailable = false) { ui =>
      assert(hasAuronTab(ui))
    }
  }
}
