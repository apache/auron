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
package org.apache.auron.integration

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Manages SparkSessions for baseline and auron modes in a single-driver process. Ensures only one
 * active session at a time, switching by stopping the current before creating a new one.
 */
class SessionManager(val extraSparkConf: Map[String, String]) {

  private var currentSession: Option[SparkSession] = None
  private var currentMode: Option[String] = None

  private def resolveMaster(fallback: String = "local[4]"): String = {
    sys.props.get("spark.master").filter(_.nonEmpty).getOrElse(fallback)
  }

  private lazy val commonConf: Map[String, String] = Map(
    "spark.master" -> resolveMaster(),
    "spark.sql.shuffle.partitions" -> "100",
    "spark.ui.enabled" -> "false",
    "spark.sql.sources.useV1SourceList" -> "parquet",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1")

  private lazy val auronSpecificConf: Map[String, String] = Map(
    "spark.sql.extensions" -> "org.apache.spark.sql.auron.AuronSparkSessionExtension",
    "spark.shuffle.manager" -> "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager",
    "spark.auron.native.log.level" -> "warn",
    "spark.auron.enable" -> "true")

  private lazy val modeConfigs: Map[String, Map[String, String]] = Map(
    "baseline" -> (commonConf ++ extraSparkConf),
    "auron" -> (commonConf ++ extraSparkConf ++ auronSpecificConf))

  def baselineSession: SparkSession = getOrSwitch("baseline", "baseline-app")

  def auronSession: SparkSession = getOrSwitch("auron", "auron-app")

  // scalastyle:off println
  private def getOrSwitch(mode: String, appName: String): SparkSession = synchronized {
    if (currentMode.contains(mode) && currentSession.isDefined) {
      currentSession.get
    } else {
      stopCurrentSession()
      val conf = new SparkConf()
      modeConfigs
        .getOrElse(mode, throw new IllegalArgumentException(s"Unknown mode: $mode"))
        .foreach { case (k, v) => conf.set(k, v) }
      val session = createAndConfigureSession(conf, appName)
      currentSession = Some(session)
      currentMode = Some(mode)
      println(s"Switched to SparkSession for mode: $mode")
      session
    }
  }

  private def createAndConfigureSession(conf: SparkConf, appName: String): SparkSession = {
    val session = SparkSession
      .builder()
      .appName(appName)
      .config(conf)
      .getOrCreate()
    session.sparkContext.setLogLevel("WARN")
    session
  }

  private def stopCurrentSession(): Unit = {
    currentSession.foreach { session =>
      try {
        session.sessionState.catalog.reset()
      } finally {
        session.stop()
      }
    }
    currentSession = None
    currentMode = None
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
  }

  def close(): Unit = {
    stopCurrentSession()
    println("SparkSession closed.")
  }
  // scalastyle:on
}
