package org.apache.spark.sql.auron.iceberg

import org.apache.iceberg.spark.source.IcebergSourceUtil
import org.apache.spark.sql.auron.{AuronConvertProvider, AuronConverters}
import org.apache.spark.sql.execution.SparkPlan
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

  }
}
