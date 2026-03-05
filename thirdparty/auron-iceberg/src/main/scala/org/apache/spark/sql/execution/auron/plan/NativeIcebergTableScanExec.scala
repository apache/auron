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

import java.net.URI
import java.security.PrivilegedExceptionAction
import java.util.Locale
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileSystem
import org.apache.iceberg.{FileFormat, FileScanTask}
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.auron.{EmptyNativeRDD, NativeConverters, NativeHelper, NativeRDD, NativeSupports, Shims}
import org.apache.spark.sql.auron.iceberg.IcebergScanPlan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.apache.auron.{protobuf => pb}
import org.apache.auron.jni.JniBridge
import org.apache.auron.metric.SparkMetricNode

case class NativeIcebergTableScanExec(basedScan: BatchScanExec, plan: IcebergScanPlan)
    extends LeafExecNode
    with NativeSupports
    with Logging {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeHelper.getNativeFileScanMetrics(sparkContext)

  override val output = basedScan.output
  override val outputPartitioning = basedScan.outputPartitioning

  private lazy val readSchema: StructType = plan.readSchema
  private lazy val fileTasks: Seq[FileScanTask] = plan.fileTasks

  private lazy val partitions: Array[FilePartition] = buildFilePartitions()
  private lazy val fileSizes: Map[String, Long] = buildFileSizes()

  private lazy val nativeFileSchema: pb.Schema = NativeConverters.convertSchema(readSchema)
  private lazy val nativePartitionSchema: pb.Schema =
    NativeConverters.convertSchema(StructType(Nil))

  private lazy val caseSensitive: Boolean = SQLConf.get.caseSensitiveAnalysis

  private lazy val fieldIndexByName: Map[String, Int] = {
    if (caseSensitive) {
      readSchema.fieldNames.zipWithIndex.toMap
    } else {
      readSchema.fieldNames.map(_.toLowerCase(Locale.ROOT)).zipWithIndex.toMap
    }
  }

  private def fieldIndexFor(name: String): Int = {
    if (caseSensitive) {
      fieldIndexByName.getOrElse(name, readSchema.fieldIndex(name))
    } else {
      fieldIndexByName.getOrElse(name.toLowerCase(Locale.ROOT), readSchema.fieldIndex(name))
    }
  }

  private lazy val projection: Seq[Integer] =
    output.map(attr => Integer.valueOf(fieldIndexFor(attr.name)))

  private lazy val nativeFileGroups: FilePartition => pb.FileGroup = (partition: FilePartition) =>
    {
      val nativePartitionedFile = (file: PartitionedFile) => {
        val filePath = file.filePath.toString
        val size = fileSizes.getOrElse(filePath, file.length)
        pb.PartitionedFile
          .newBuilder()
          .setPath(filePath)
          .setSize(size)
          .setLastModifiedNs(0)
          .setRange(
            pb.FileRange
              .newBuilder()
              .setStart(file.start)
              .setEnd(file.start + file.length)
              .build())
          .build()
      }
      pb.FileGroup
        .newBuilder()
        .addAllFiles(partition.files.map(nativePartitionedFile).toList.asJava)
        .build()
    }

  override def doExecuteNative(): NativeRDD = {
    if (partitions.isEmpty) {
      return new EmptyNativeRDD(sparkContext)
    }

    val nativeMetrics = SparkMetricNode(
      metrics,
      Nil,
      Some({
        case ("bytes_scanned", v) =>
          val inputMetric = TaskContext.get.taskMetrics().inputMetrics
          inputMetric.incBytesRead(v)
        case ("output_rows", v) =>
          val inputMetric = TaskContext.get.taskMetrics().inputMetrics
          inputMetric.incRecordsRead(v)
        case _ =>
      }))

    val fileFormat = plan.fileFormat
    val broadcastedHadoopConf = this.broadcastedHadoopConf
    val numPartitions = partitions.length

    // Build per-partition native scan plans and execute them via NativeRDD.
    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      None,
      Nil,
      rddShuffleReadFull = true,
      (partition, _) => {
        // Register the Hadoop conf for native readers via a per-task resource id.
        val resourceId = s"NativeIcebergTableScan:${UUID.randomUUID().toString}"
        putJniBridgeResource(resourceId, broadcastedHadoopConf)

        // Convert Spark FilePartition to native FileGroup.
        val nativeFileGroup = nativeFileGroups(partition.asInstanceOf[FilePartition])
        val nativeFileScanConf = pb.FileScanExecConf
          .newBuilder()
          .setNumPartitions(numPartitions)
          .setPartitionIndex(partition.index)
          .setStatistics(pb.Statistics.getDefaultInstance)
          .setSchema(nativeFileSchema)
          .setFileGroup(nativeFileGroup)
          .addAllProjection(projection.asJava)
          .setPartitionSchema(nativePartitionSchema)
          .build()

        // Choose a native scan node based on file format.
        if (fileFormat == FileFormat.ORC) {
          val nativeOrcScanExecBuilder = pb.OrcScanExecNode
            .newBuilder()
            .setBaseConf(nativeFileScanConf)
            .setFsResourceId(resourceId)
            // No pruning predicates are pushed down in the native scan yet.
            .addAllPruningPredicates(new java.util.ArrayList())

          pb.PhysicalPlanNode
            .newBuilder()
            .setOrcScan(nativeOrcScanExecBuilder.build())
            .build()
        } else {
          val nativeParquetScanExecBuilder = pb.ParquetScanExecNode
            .newBuilder()
            .setBaseConf(nativeFileScanConf)
            .setFsResourceId(resourceId)
            // No pruning predicates are pushed down in the native scan yet.
            .addAllPruningPredicates(new java.util.ArrayList())

          pb.PhysicalPlanNode
            .newBuilder()
            .setParquetScan(nativeParquetScanExecBuilder.build())
            .build()
        }
      },
      friendlyName = "NativeRDD.IcebergScan")
  }

  override val nodeName: String = "NativeIcebergTableScan"

  // Delegate canonicalization to the original scan to keep plan equivalence checks consistent.
  override protected def doCanonicalize(): SparkPlan = basedScan.canonicalized

  private def buildFileSizes(): Map[String, Long] = {
    // Map file path to full file size; tasks may split a file into multiple ranges.
    fileTasks
      .map(task => task.file().location() -> task.file().fileSizeInBytes())
      .groupBy(_._1)
      .mapValues(_.head._2)
      .toMap
  }

  private def buildFilePartitions(): Array[FilePartition] = {
    // Convert Iceberg file tasks into Spark FilePartition groups for execution.
    if (fileTasks.isEmpty) {
      return Array.empty
    }

    val sparkSession = Shims.get.getSqlContext(basedScan).sparkSession
    val maxSplitBytes = getMaxSplitBytes(sparkSession, fileTasks)
    val partitionedFiles = fileTasks
      .map { task =>
        val filePath = task.file().location()
        Shims.get.getPartitionedFile(InternalRow.empty, filePath, task.start(), task.length())
      }
      .sortBy(_.length)(Ordering[Long].reverse)
      .toSeq

    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes).toArray
  }

  private def getMaxSplitBytes(sparkSession: SparkSession, tasks: Seq[FileScanTask]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = Shims.get.getMinPartitionNum(sparkSession)
    val totalBytes = tasks
      .map(task => task.file().fileSizeInBytes() + openCostInBytes)
      .sum
    val bytesPerCore = if (minPartitionNum > 0) totalBytes / minPartitionNum else totalBytes

    Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
  }

  private def putJniBridgeResource(
      resourceId: String,
      broadcastedHadoopConf: Broadcast[SerializableConfiguration]): Unit = {
    val sharedConf = broadcastedHadoopConf.value.value
    JniBridge.putResource(
      resourceId,
      (location: String) => {
        val getFsTimeMetric = metrics("io_time_getfs")
        val currentTimeMillis = System.currentTimeMillis()
        val fs = NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
          override def run(): FileSystem = FileSystem.get(new URI(location), sharedConf)
        })
        getFsTimeMetric.add((System.currentTimeMillis() - currentTimeMillis) * 1000000)
        fs
      })
  }

  private def broadcastedHadoopConf: Broadcast[SerializableConfiguration] = {
    val sparkSession = Shims.get.getSqlContext(basedScan).sparkSession
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(Map.empty)
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
  }
}
