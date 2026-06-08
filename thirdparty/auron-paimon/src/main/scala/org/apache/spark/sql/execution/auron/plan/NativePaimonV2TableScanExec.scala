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
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.auron.{EmptyNativeRDD, NativeConverters, NativeHelper, NativeRDD, NativeSupports, Shims}
import org.apache.spark.sql.auron.paimon.{PaimonFile, PaimonScanPlan}
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.hive.auron.paimon.PaimonUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import org.apache.auron.{protobuf => pb}
import org.apache.auron.jni.JniBridge
import org.apache.auron.metric.SparkMetricNode

case class NativePaimonV2TableScanExec(basedScan: BatchScanExec, plan: PaimonScanPlan)
    extends LeafExecNode
    with NativeSupports
    with Logging {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeHelper.getNativeFileScanMetrics(sparkContext) ++ Seq(
      "numPartitions" -> SQLMetrics.createMetric(sparkContext, "Native.partitions_read"),
      "numFiles" -> SQLMetrics.createMetric(sparkContext, "Native.files_read"))

  override val output = basedScan.output
  override val outputPartitioning = basedScan.outputPartitioning

  private lazy val fileSchema: StructType = plan.fileSchema
  private lazy val partitionSchema: StructType = plan.partitionSchema
  private lazy val files: Seq[PaimonFile] = plan.files

  private lazy val partitions: Array[FilePartition] = {
    val filePartitions = buildFilePartitions()
    postDriverMetrics(filePartitions)
    filePartitions
  }
  private lazy val fileSizes: Map[String, Long] =
    files.map(f => f.filePath -> f.fileSize).toMap

  private lazy val nativeFileSchema: pb.Schema = NativeConverters.convertSchema(fileSchema)
  private lazy val nativePartitionSchema: pb.Schema =
    NativeConverters.convertSchema(partitionSchema)

  // Project the output attributes onto the (fileSchema ++ partitionSchema) layout used by the
  // native scan. Index lookup follows SQLConf.caseSensitiveAnalysis so that the projection
  // remains correct under case-insensitive analysis (mirrors NativeIcebergTableScanExec).
  private lazy val combinedSchema: StructType =
    StructType(fileSchema.fields ++ partitionSchema.fields)

  private lazy val caseSensitive: Boolean = SQLConf.get.caseSensitiveAnalysis

  private lazy val fieldIndexByName: Map[String, Int] = {
    if (caseSensitive) {
      combinedSchema.fieldNames.zipWithIndex.toMap
    } else {
      combinedSchema.fieldNames.map(_.toLowerCase(Locale.ROOT)).zipWithIndex.toMap
    }
  }

  private def fieldIndexFor(name: String): Int = {
    if (caseSensitive) {
      fieldIndexByName.getOrElse(name, combinedSchema.fieldIndex(name))
    } else {
      fieldIndexByName.getOrElse(name.toLowerCase(Locale.ROOT), combinedSchema.fieldIndex(name))
    }
  }

  private lazy val projection: Seq[Integer] =
    output.map(attr => Integer.valueOf(fieldIndexFor(attr.name)))

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
    val nativeFileGroups = this.nativeFileGroups
    val nativeFileSchema = this.nativeFileSchema
    val nativePartitionSchema = this.nativePartitionSchema
    val projection = this.projection

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      None,
      Nil,
      rddShuffleReadFull = true,
      (partition, _) => {
        val resourceId = s"NativePaimonV2TableScan:${UUID.randomUUID().toString}"
        putJniBridgeResource(resourceId, broadcastedHadoopConf)

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

        if (fileFormat.equalsIgnoreCase(PaimonUtil.orcFormat)) {
          val nativeOrcScanExecBuilder = pb.OrcScanExecNode
            .newBuilder()
            .setBaseConf(nativeFileScanConf)
            .setFsResourceId(resourceId)
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
            .addAllPruningPredicates(new java.util.ArrayList())

          pb.PhysicalPlanNode
            .newBuilder()
            .setParquetScan(nativeParquetScanExecBuilder.build())
            .build()
        }
      },
      friendlyName = "NativeRDD.PaimonV2Scan")
  }

  override val nodeName: String = "NativePaimonV2TableScan"

  override protected def doCanonicalize(): SparkPlan = basedScan.canonicalized

  private lazy val nativeFileGroups: FilePartition => pb.FileGroup = (partition: FilePartition) =>
    {
      val nativePartitionedFile = (file: PartitionedFile) => {
        val filePath = file.filePath.toString
        val size = fileSizes.getOrElse(filePath, file.length)
        val nativePartitionValues = partitionSchema.zipWithIndex.map { case (field, index) =>
          NativeConverters
            .convertExpr(
              org.apache.spark.sql.catalyst.expressions
                .Literal(file.partitionValues.get(index, field.dataType), field.dataType))
            .getLiteral
        }
        pb.PartitionedFile
          .newBuilder()
          .setPath(filePath)
          .setSize(size)
          .setLastModifiedNs(0)
          .addAllPartitionValues(nativePartitionValues.asJava)
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

  private def postDriverMetrics(filePartitions: Array[FilePartition]): Unit = {
    val numPartitions = filePartitions.length
    metrics("numPartitions").add(numPartitions)
    val numFiles = filePartitions.foldLeft(0L)(_ + _.files.length)
    metrics("numFiles").add(numFiles)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext,
      executionId,
      Seq(metrics("numPartitions"), metrics("numFiles")))
  }

  private def buildFilePartitions(): Array[FilePartition] = {
    if (files.isEmpty) {
      return Array.empty
    }

    val sparkSession = Shims.get.getSqlContext(basedScan).sparkSession
    val isSplitable =
      plan.fileFormat.equalsIgnoreCase(PaimonUtil.parquetFormat) ||
        plan.fileFormat.equalsIgnoreCase(PaimonUtil.orcFormat)
    val maxSplitBytes = getMaxSplitBytes(sparkSession, files)
    val partitionedFiles = files
      .flatMap { f =>
        if (isSplitable) {
          (0L until f.fileSize by maxSplitBytes).map { offset =>
            val remaining = f.fileSize - offset
            val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
            Shims.get.getPartitionedFile(f.partitionValues, f.filePath, offset, size)
          }
        } else {
          Seq(Shims.get.getPartitionedFile(f.partitionValues, f.filePath, 0, f.fileSize))
        }
      }
      .sortBy(_.length)(Ordering[Long].reverse)
      .toSeq

    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes).toArray
  }

  private def getMaxSplitBytes(sparkSession: SparkSession, fs: Seq[PaimonFile]): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    val minPartitionNum = Shims.get.getMinPartitionNum(sparkSession)
    val totalBytes = fs.map(_.fileSize + openCostInBytes).sum
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
