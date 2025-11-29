package org.apache.spark.sql.auron.iceberg

import org.apache.spark.sql.auron.{AuronConvertProvider, AuronConverters}
import org.apache.spark.sql.execution.SparkPlan

class IcebergConvertProvider extends AuronConvertProvider {

  override def isEnabled: Boolean = {
    AuronConverters.getBooleanConf("spark.auron.enable.iceberg.scan", defaultValue = false)
  }

  override def isSupported(exec: SparkPlan): Boolean = {
    false
  }

  override def convert(exec: SparkPlan): SparkPlan = {
    false
  }
}
