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
package org.apache.spark.sql.auron.hudi

import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.{AuronConverters, AuronConvertProvider, NativeConverters, Shims}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.SparkPlan

import org.apache.auron.spark.configuration.SparkAuronConfiguration

class HudiConvertProvider extends AuronConvertProvider with Logging {

  override def isEnabled: Boolean = {
    val sparkVersion = org.apache.spark.SPARK_VERSION
    val versionParts = sparkVersion.split("[\\.-]", 3)
    val maybeMajor = versionParts.headOption.flatMap(part => Try(part.toInt).toOption)
    val maybeMinor =
      if (versionParts.length >= 2) Try(versionParts(1).toInt).toOption else None
    val supported = (for {
      major <- maybeMajor
      minor <- maybeMinor
    } yield major == 3 && minor >= 0 && minor <= 5).getOrElse(false)
    SparkAuronConfiguration.ENABLE_HUDI_SCAN.get() && supported
  }

  override def isSupported(exec: SparkPlan): Boolean = {
    exec match {
      case scan: FileSourceScanExec =>
        // Only handle Hudi-backed file scans; other scans fall through.
        HudiScanSupport.isSupported(scan)
      case _ => false
    }
  }

  override def convert(exec: SparkPlan): SparkPlan = {
    exec match {
      case scan: FileSourceScanExec if HudiScanSupport.isSupported(scan) =>
        HudiScanSupport.fileFormat(scan) match {
          case Some(HudiScanSupport.ParquetFormat) =>
            if (!SparkAuronConfiguration.ENABLE_SCAN_PARQUET.get()) {
              return exec
            }
            // Hudi falls back to Spark when timestamp scanning is disabled.
            if (!SparkAuronConfiguration.ENABLE_SCAN_PARQUET_TIMESTAMP.get()) {
              if (scan.requiredSchema.exists(e =>
                  NativeConverters.existTimestampType(e.dataType))) {
                return exec
              }
            }
            logDebug(s"Applying native parquet scan for Hudi: ${scan.relation.location}")
            AuronConverters.addRenameColumnsExec(Shims.get.createNativeParquetScanExec(scan))
          case Some(HudiScanSupport.OrcFormat) =>
            if (!SparkAuronConfiguration.ENABLE_SCAN_ORC.get()) {
              return exec
            }
            // ORC follows the same timestamp fallback rule as Parquet.
            if (!SparkAuronConfiguration.ENABLE_SCAN_ORC_TIMESTAMP.get()) {
              if (scan.requiredSchema.exists(e =>
                  NativeConverters.existTimestampType(e.dataType))) {
                return exec
              }
            }
            logDebug(s"Applying native ORC scan for Hudi: ${scan.relation.location}")
            AuronConverters.addRenameColumnsExec(Shims.get.createNativeOrcScanExec(scan))
          case None => exec
        }
      case _ => exec
    }
  }
}
