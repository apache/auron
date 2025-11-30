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
package org.apache.spark.sql.execution.auron.plan

import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.{IcebergPartitionValueConverter, IcebergSourceUtil}
import org.apache.iceberg.{FileScanTask, ScanTask}
import org.apache.spark.sql.auron.{NativeHelper, NativeRDD, NativeSupports, Shims}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

case class NativeIcebergBatchScanExec(batchScanExec: BatchScanExec)
  extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = NativeHelper.getNativeFileScanMetrics(sparkContext)

  override protected def doExecuteNative(): NativeRDD = ???

  override def output: Seq[Attribute] = batchScanExec.output

  override def outputPartitioning: Partitioning = batchScanExec.outputPartitioning

  private lazy val icebergScan = IcebergSourceUtil.getScanAsSparkBatchQueryScan(batchScanExec.scan)

  private lazy val icebergTable = IcebergSourceUtil.getTableFromScan(icebergScan)

  private lazy val filePartitions: Seq[FilePartition] = getFilePartitions

  private lazy val readDataSchema: StructType = icebergScan.readSchema

  private val partitionValueConverter = new IcebergPartitionValueConverter(icebergTable)

  private def getFilePartitions: Seq[FilePartition] = {
    val sparkSession = Shims.get.getSqlContext(batchScanExec).sparkSession
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes

    val inputPartitions = icebergScan.toBatch.planInputPartitions()
    val partitionedFiles = inputPartitions.flatMap { partition =>
      val fileScanTasks = extractFileScanTasks(partition)
      fileScanTasks.map { fileScanTask =>
        val filePath = fileScanTask.file().location();
        val start = fileScanTask.start();
        val length = fileScanTask.length();

        val partitionValues = partitionValueConverter.convert(fileScanTask)
        Shims.get.getPartitionedFile(
          partitionValues,
          filePath,
          start,
          length,
        )
      }
    }
    FilePartition.getFilePartitions(
      sparkSession,
      partitionedFiles,
      maxSplitBytes
    )
  }

  private def extractFileScanTasks(partition: InputPartition): Seq[FileScanTask] = {
    val sparkInputPartitions = IcebergSourceUtil.getInputPartitionAsSparkInputPartition(partition)
    val tasks = sparkInputPartitions.taskGroup[ScanTask]().tasks().asScala
    IcebergSourceUtil.getFileScanTasks(tasks.toList)
  }
}
