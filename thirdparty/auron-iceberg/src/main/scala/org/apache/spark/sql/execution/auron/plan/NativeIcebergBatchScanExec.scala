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
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileSystem
import org.apache.iceberg.FileScanTask
import org.apache.iceberg.spark.source.{IcebergPartitionValueConverter, IcebergSourceUtil}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.auron._
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.{NullType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import org.apache.auron.{protobuf => pb}
import org.apache.auron.jni.JniBridge
import org.apache.auron.metric.SparkMetricNode

/**
 * Native execution wrapper for Iceberg batch scans.
 *
 * Translates a Spark V2 Iceberg scan (SparkBatchQueryScan) into Auron's native
 * file scan plan and executes it via a NativeRDD. It constructs the corresponding
 * protobuf PhysicalPlanNode (ParquetScanExecNode or OrcScanExecNode), registers
 * Hadoop FS resources over the JNI bridge, and wires Spark input metrics to
 * native metrics.
 *
 *
 * @param batchScanExec underlying Spark V2 BatchScanExec for the Iceberg source
 */
case class NativeIcebergBatchScanExec(batchScanExec: BatchScanExec)
    extends LeafExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] =
    NativeHelper.getNativeFileScanMetrics(sparkContext)

  override protected def doExecuteNative(): NativeRDD = {
    val partitions = filePartitions
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

    val nativePruningPredicateFilters = this.nativePruningPredicateFilters
    val nativeFileSchema = this.nativeFileSchema
    val nativeFileGroups = this.nativeFileGroups
    val nativePartitionSchema = this.nativePartitionSchema
    val projection = computeProjection()
    val broadcastedHadoopConf = this.broadcastedHadoopConf
    val numPartitions = partitions.length
    val fileFormat = detectFileFormat()

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.toArray.asInstanceOf[Array[Partition]],
      None,
      Nil,
      rddShuffleReadFull = true,
      (partition, _) => {
        val resourceId = s"NativeIcebergBatchScan:${UUID.randomUUID().toString}"
        putJniBridgeResource(resourceId, broadcastedHadoopConf)

        buildNativePlanNode(
          resourceId,
          partition.asInstanceOf[FilePartition],
          nativeFileSchema,
          nativePartitionSchema,
          nativeFileGroups,
          nativePruningPredicateFilters,
          projection,
          numPartitions,
          fileFormat)
      },
      friendlyName = "NativeRDD.IcebergBatchScanExec")
  }

  override def output: Seq[Attribute] = batchScanExec.output

  override def outputPartitioning: Partitioning = batchScanExec.outputPartitioning

  private lazy val icebergScan =
    IcebergSourceUtil.getScanAsSparkBatchQueryScan(batchScanExec.scan)

  private lazy val icebergTable = IcebergSourceUtil.getTableFromScan(icebergScan)

  private lazy val filePartitions: Seq[FilePartition] = getFilePartitions

  private lazy val readDataSchema: StructType =
    IcebergSourceUtil.getReadSchema(batchScanExec.scan)

  private val partitionValueConverter = new IcebergPartitionValueConverter(icebergTable)

  private def getFilePartitions: Seq[FilePartition] = {
    val sparkSession = Shims.get.getSqlContext(batchScanExec).sparkSession
    val maxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes

    val inputPartitions = IcebergSourceUtil.planInputPartitions(batchScanExec.scan)
    val partitionedFiles = inputPartitions.flatMap { partition =>
      val fileScanTasks = extractFileScanTasks(partition)
      fileScanTasks.map { fileScanTask =>
        val filePath = fileScanTask.file().location();
        val start = fileScanTask.start();
        val length = fileScanTask.length();

        val partitionValues = partitionValueConverter.convert(fileScanTask)
        Shims.get.getPartitionedFile(partitionValues, filePath, start, length)
      }
    }
    FilePartition.getFilePartitions(sparkSession, partitionedFiles, maxSplitBytes)
  }

  private def buildNativePlanNode(
      resourceId: String,
      partition: FilePartition,
      nativeFileSchema: pb.Schema,
      nativePartitionSchema: pb.Schema,
      nativeFileGroups: FilePartition => pb.FileGroup,
      nativePruningPredicates: Seq[pb.PhysicalExprNode],
      projection: Seq[Int],
      numPartitions: Int,
      fileFormat: String): pb.PhysicalPlanNode = {

    val nativeFileGroup = nativeFileGroups(partition)

    val nativeFileScanConf = pb.FileScanExecConf
      .newBuilder()
      .setNumPartitions(numPartitions)
      .setPartitionIndex(partition.index)
      .setStatistics(pb.Statistics.getDefaultInstance)
      .setSchema(nativeFileSchema)
      .setFileGroup(nativeFileGroup)
      .addAllProjection(projection.map(Integer.valueOf).asJava)
      .setPartitionSchema(nativePartitionSchema)
      .build()

    fileFormat match {
      case "parquet" =>
        val parquetScanNode = pb.ParquetScanExecNode
          .newBuilder()
          .setBaseConf(nativeFileScanConf)
          .setFsResourceId(resourceId)
          .addAllPruningPredicates(nativePruningPredicates.asJava)
          .build()

        pb.PhysicalPlanNode
          .newBuilder()
          .setParquetScan(parquetScanNode)
          .build()

      case "orc" =>
        val orcScanNode = pb.OrcScanExecNode
          .newBuilder()
          .setBaseConf(nativeFileScanConf)
          .setFsResourceId(resourceId)
          .addAllPruningPredicates(nativePruningPredicates.asJava)
          .build()

        pb.PhysicalPlanNode
          .newBuilder()
          .setOrcScan(orcScanNode)
          .build()

      case other =>
        throw new UnsupportedOperationException(s"Unsupported file format for Iceberg: $other")
    }
  }

  private def nativeFileSchema: pb.Schema = {
    // Convert read schema, mark non-used fields as NullType
    val adjustedSchema = StructType(readDataSchema.fields.map { field =>
      if (output.exists(_.name == field.name)) {
        field.copy(nullable = true)
      } else {
        StructField(field.name, NullType, nullable = true)
      }
    })
    NativeConverters.convertSchema(adjustedSchema)
  }

  private def nativePartitionSchema: pb.Schema = {
    val partitionSchema = extractPartitionSchema()
    NativeConverters.convertSchema(partitionSchema)
  }

  private def nativeFileGroups: FilePartition => pb.FileGroup = { partition =>
    val nativePartitionedFile = (file: PartitionedFile) => {
      val partitionSchema = extractPartitionSchema()
      val nativePartitionValues = partitionSchema.zipWithIndex.map { case (field, index) =>
        NativeConverters
          .convertExpr(Literal(file.partitionValues.get(index, field.dataType), field.dataType))
          .getLiteral
      }

      pb.PartitionedFile
        .newBuilder()
        .setPath(s"${file.filePath}")
        .setSize(file.length)
        .addAllPartitionValues(nativePartitionValues.asJava)
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

  private def nativePruningPredicateFilters: Seq[pb.PhysicalExprNode] = {
    // TODO: Extract residual predicates from Iceberg scan; add incremental support
    Seq.empty
  }

  private def extractPartitionSchema(): StructType = {
    partitionValueConverter.schema
  }

  private def detectFileFormat(): String = {
    if (filePartitions.isEmpty) "parquet"
    else {
      val firstPath = filePartitions.head.files.head.filePath
      val pathStr = firstPath.toString
      if (pathStr.endsWith(".parquet")) "parquet"
      else if (pathStr.endsWith(".orc")) "orc"
      else "parquet" // default
    }
  }

  private def computeProjection(): Seq[Int] = {
    output.map(attr => readDataSchema.fieldIndex(attr.name))
  }

  protected def putJniBridgeResource(
      resourceId: String,
      broadcastedHadoopConf: Broadcast[SerializableConfiguration]): Unit = {
    val sharedConf = broadcastedHadoopConf.value.value
    JniBridge.putResource(
      resourceId,
      (location: String) => {
        val getFsTimeMetric = metrics("io_time_getfs")
        val currentTimeMillis = System.currentTimeMillis()
        val fs = NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
          override def run(): FileSystem = {
            FileSystem.get(new URI(location), sharedConf)
          }
        })
        getFsTimeMetric.add((System.currentTimeMillis() - currentTimeMillis) * 1000000)
        fs
      })
  }

  protected def broadcastedHadoopConf: Broadcast[SerializableConfiguration] = {
    val sparkSession = Shims.get.getSqlContext(batchScanExec).sparkSession
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
  }

  override def nodeName: String =
    s"NativeIcebergBatchScan ${icebergTable.name()}"

  override protected def doCanonicalize(): SparkPlan =
    batchScanExec.canonicalized

  private def extractFileScanTasks(partition: InputPartition): Seq[FileScanTask] = {
    IcebergSourceUtil.getFileScanTasksFromInputPartition(partition)
  }
}
