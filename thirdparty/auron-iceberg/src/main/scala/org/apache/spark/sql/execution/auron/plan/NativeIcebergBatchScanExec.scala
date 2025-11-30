package org.apache.spark.sql.execution.auron.plan

import org.apache.spark.sql.auron.{NativeRDD, NativeSupports}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.SQLMetric

case class NativeIcebergBatchScanExec(batchScanExec: BatchScanExec) extends LeafExecNode with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = ???

  override protected def doExecuteNative(): NativeRDD = {
    // TODO:???
  }

  override def output: Seq[Attribute] = batchScanExec.output
}
