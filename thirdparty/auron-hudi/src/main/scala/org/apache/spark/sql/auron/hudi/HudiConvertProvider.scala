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

  override def isEnabled(exec: SparkPlan): Boolean = {
    exec match {
      case _: FileSourceScanExec =>
        // Only handle Hudi-backed file scans; other scans fall through.
        val sparkVersion = org.apache.spark.SPARK_VERSION
        val versionParts = sparkVersion.split("[\\.-]", 3)
        val maybeMajor = versionParts.headOption.flatMap(part => Try(part.toInt).toOption)
        val maybeMinor =
          if (versionParts.length >= 2) Try(versionParts(1).toInt).toOption else None
        val supported = (for {
          major <- maybeMajor
          minor <- maybeMinor
        } yield major == 3 && minor >= 0 && minor <= 5).getOrElse(false)
        assert(
          SparkAuronConfiguration.ENABLE_HUDI_SCAN.get(),
          "Conversion disabled: auron.enable.hudi.scan=false")
        assert(supported, "Supported Spark versions: 3.0 to 3.5.")
        SparkAuronConfiguration.ENABLE_HUDI_SCAN.get() && supported
      case _ => false
    }
  }

  override def isSupported(exec: SparkPlan): Boolean = {
    exec match {
      case scan: FileSourceScanExec =>
        // Only handle Hudi-backed file scans; other scans fall through.
        val isSupportedFileFormat = HudiScanSupport.supportedFileFormat(scan).nonEmpty
        assert(!isSupportedFileFormat, "FileFormat is not supported.")
        isSupportedFileFormat
      case _ => false
    }
  }

  override def convert(exec: SparkPlan): SparkPlan = {
    exec match {
      case scan: FileSourceScanExec =>
        HudiScanSupport.supportedFileFormat(scan) match {
          case Some(HudiScanSupport.ParquetFormat) =>
            assert(
              SparkAuronConfiguration.ENABLE_SCAN_PARQUET.get(),
              "Conversion disabled: auron.enable.scan.parquet=false")
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
            assert(
              SparkAuronConfiguration.ENABLE_SCAN_PARQUET.get(),
              "Conversion disabled: auron.enable.scan.orc=false")
            // ORC follows the same timestamp fallback rule as Parquet.
            assert(
              SparkAuronConfiguration.ENABLE_SCAN_ORC_TIMESTAMP.get(),
              "Conversion disabled: auron.enable.scan.orc.timestamp=false.")
            if (!SparkAuronConfiguration.ENABLE_SCAN_ORC_TIMESTAMP.get()) {
              if (scan.requiredSchema.exists(e =>
                  NativeConverters.existTimestampType(e.dataType))) {
                assert(
                  false,
                  "Conversion disabled: ORC follows the same timestamp fallback rule as Parquet.")
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
