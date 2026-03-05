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
package org.apache.spark.sql.auron.iceberg

import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.{AuronConverters, AuronConvertProvider}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.auron.plan.NativeIcebergTableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

import org.apache.auron.spark.configuration.SparkAuronConfiguration
import org.apache.auron.util.SemanticVersion

class IcebergConvertProvider extends AuronConvertProvider with Logging {

  override def isEnabled: Boolean = {
    val enabled = SparkAuronConfiguration.ENABLE_ICEBERG_SCAN.get()
    if (!enabled) {
      return false
    }
    if (!sparkCompatible) {
      logWarning(
        s"Disable Iceberg native scan: Spark $SPARK_VERSION is not supported. " +
          s"Supported Spark versions: 3.4 to 4.0 (Iceberg ${icebergVersionOrUnknown}).")
      return false
    }
    true
  }

  override def isSupported(exec: SparkPlan): Boolean = {
    exec match {
      case e: BatchScanExec => IcebergScanSupport.plan(e).nonEmpty
      case _ => false
    }
  }

  override def convert(exec: SparkPlan): SparkPlan = {
    exec match {
      case e: BatchScanExec =>
        IcebergScanSupport.plan(e) match {
          case Some(plan) =>
            AuronConverters.addRenameColumnsExec(NativeIcebergTableScanExec(e, plan))
          case None =>
            exec
        }
      case _ => exec
    }
  }

  private lazy val sparkCompatible: Boolean = {
    SemanticVersion(SPARK_VERSION) >= "3.4" && SemanticVersion(SPARK_VERSION) < "4.1"
  }

  private def icebergVersionOrUnknown: String = {
    val pkg = classOf[org.apache.iceberg.Table].getPackage
    val version = if (pkg != null) pkg.getImplementationVersion else null
    Option(version).getOrElse("unknown")
  }
}
