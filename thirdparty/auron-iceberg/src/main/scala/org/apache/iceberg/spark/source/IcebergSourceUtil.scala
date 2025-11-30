package org.apache.iceberg.spark.source

import org.apache.spark.sql.connector.read.Scan

object IcebergSourceUtil {

  def isIcebergScan(scan: Scan): Boolean = {
    scan match {
      case _ : org.apache.iceberg.spark.source.SparkBatchQueryScan => true
      case _ => false
    }
  }
}
