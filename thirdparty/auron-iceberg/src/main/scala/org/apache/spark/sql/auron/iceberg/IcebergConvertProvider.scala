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

import org.apache.iceberg.spark.source.IcebergSourceUtil
import org.apache.spark.sql.auron.{AuronConverters, AuronConvertProvider}
import org.apache.spark.sql.auron.util.AuronLogUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.auron.plan.NativeIcebergBatchScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class IcebergConvertProvider extends AuronConvertProvider {

  override def isEnabled: Boolean = {
    AuronConverters.getBooleanConf("spark.auron.enable.iceberg.scan", defaultValue = false)
  }

  override def isSupported(exec: SparkPlan): Boolean = {
    exec match {
      case e: BatchScanExec =>
        IcebergSourceUtil.isIcebergScan(e.scan)
      case _ => false
    }
  }

  override def convert(exec: SparkPlan): SparkPlan = {
    exec match {
      case batchScanExec: BatchScanExec =>
        convertIcebergBatchScanExec(batchScanExec)
      case _ => exec
    }
  }

  private def convertIcebergBatchScanExec(batchScanExec: BatchScanExec): SparkPlan = {
    // TODO: Validate table mode (COW support initially, MOR later)
    val scan = IcebergSourceUtil.getScanAsSparkBatchQueryScan(batchScanExec.scan)
    val table = IcebergSourceUtil.getTableFromScan(scan)

    AuronLogUtils.logDebugPlanConversion(
      batchScanExec,
      Seq("scan" -> scan.getClass, "table" -> table.getClass, "output" -> batchScanExec.output))
    AuronConverters.addRenameColumnsExec(NativeIcebergBatchScanExec(batchScanExec))
  }
}
