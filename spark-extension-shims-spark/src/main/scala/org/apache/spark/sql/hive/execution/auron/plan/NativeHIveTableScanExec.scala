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
package org.apache.spark.sql.hive.execution.auron.plan

import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.{protobuf => pb}
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.hive.ql.exec.Utilities
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat
import org.apache.hadoop.hive.ql.metadata.{Table => HiveTable}
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorUtils, StructObjectInspector}
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.{FileSplit, InputFormat, JobConf}
import org.apache.hadoop.mapreduce.{InputFormat => newInputClass}
import org.apache.hadoop.util.ReflectionUtils
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.HADOOP_RDD_IGNORE_EMPTY_SPLITS
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.auron.{NativeRDD, Shims}
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, GenericInternalRow}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.hive.{HadoopTableReader, HiveShim}
import org.apache.spark.{Partition, TaskContext}

import java.util.UUID
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

case class NativeHiveTableScanExec(basedHiveScan: HiveTableScanExec)
  extends NativeHiveTableScanBase(basedHiveScan)
    with Logging {

  @transient private lazy val nativeTable: HiveTable = HiveClientImpl.toHiveTable(relation.tableMeta)
  @transient private lazy val fileFormat = HiveTableUtil.getFileFormat(nativeTable.getInputFormatClass)
  @transient private lazy val nativeTableDesc = new TableDesc(
    nativeTable.getInputFormatClass,
    nativeTable.getOutputFormatClass,
    nativeTable.getMetadata)

  @transient private lazy val nativeHadoopConf = {
    val hiveConf = SparkSession.getActiveSession.get.sessionState.newHadoopConf()
    // append columns ids and names before broadcast
    val columnOrdinals = AttributeMap(relation.dataCols.zipWithIndex)
    val neededColumnIDs = output.flatMap(columnOrdinals.get).map(o => o: Integer)
    val neededColumnNames = output.filter(columnOrdinals.contains).map(_.name)

    HiveShim.appendReadColumns(hiveConf, neededColumnIDs, neededColumnNames)

    val deserializer = nativeTableDesc.getDeserializerClass.getConstructor().newInstance()
    deserializer.initialize(hiveConf, nativeTableDesc.getProperties)

    // Specifies types and object inspectors of columns to be scanned.
    val structOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        deserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val columnTypeNames = structOI
      .getAllStructFieldRefs.asScala
      .map(_.getFieldObjectInspector)
      .map(TypeInfoUtils.getTypeInfoFromObjectInspector(_).getTypeName)
      .mkString(",")

    hiveConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypeNames)
    hiveConf.set(serdeConstants.LIST_COLUMNS, relation.dataCols.map(_.name).mkString(","))
    hiveConf
  }

  private val minPartitions = if (SparkSession.getActiveSession.get.sparkContext.isLocal) {
    0 // will splitted based on block by default.
  } else {
    math.max(nativeHadoopConf.getInt("mapreduce.job.maps", 1),
      SparkSession.getActiveSession.get.sparkContext.defaultMinPartitions)
  }

  private val ignoreEmptySplits =
    SparkSession.getActiveSession.get.sparkContext.conf.get(HADOOP_RDD_IGNORE_EMPTY_SPLITS)

  override val nodeName: String =
    s"NativeHiveTableScan $tableName"

  override def doExecuteNative(): NativeRDD = {
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
    val nativeFileSchema = this.nativeFileSchema
    val nativeFileGroups = this.nativeFileGroups
    val nativePartitionSchema = this.nativePartitionSchema

    val projection = schema.map(field => relation.schema.fieldIndex(field.name))
    val broadcastedHadoopConf = this.broadcastedHadoopConf
    val numPartitions = partitions.length

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      partitions.asInstanceOf[Array[Partition]],
      None,
      Nil,
      rddShuffleReadFull = true,
      (partition, _) => {
        val resourceId = s"NativeHiveTableScan:${UUID.randomUUID().toString}"
        putJniBridgeResource(resourceId, broadcastedHadoopConf)

        val nativeFileGroup = nativeFileGroups(partition.asInstanceOf[FilePartition])
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
          case "orc" =>
            val nativeOrcScanExecBuilder = pb.OrcScanExecNode
              .newBuilder()
              .setBaseConf(nativeFileScanConf)
              .setFsResourceId(resourceId)
              .addAllPruningPredicates(new java.util.ArrayList()) // not support this filter
            pb.PhysicalPlanNode
              .newBuilder()
              .setOrcScan(nativeOrcScanExecBuilder.build())
              .build()
          case "parquet" =>
            val nativeParquetScanExecBuilder = pb.ParquetScanExecNode
              .newBuilder()
              .setBaseConf(nativeFileScanConf)
              .setFsResourceId(resourceId)
              .addAllPruningPredicates(new java.util.ArrayList()) // not support this filter

            pb.PhysicalPlanNode
              .newBuilder()
              .setParquetScan(nativeParquetScanExecBuilder.build())
              .build()
        }
      },
      friendlyName = "NativeRDD.HiveTableScan")
  }

  override def getFilePartitions(): Array[FilePartition] = {
    val newJobConf = new JobConf(nativeHadoopConf)
    val arrayFilePartition = ArrayBuffer[FilePartition]()
    val partitionedFiles = if (relation.isPartitioned) {
      val partitions = basedHiveScan.prunedPartitions
      val arrayPartitionedFile = ArrayBuffer[PartitionedFile]()
      partitions.foreach { partition =>
        val partDesc = Utilities.getPartitionDescFromTableDesc(nativeTableDesc, partition, true)
        val partPath = partition.getDataLocation
        HadoopTableReader.initializeLocalJobConfFunc(partPath.toString, nativeTableDesc)(newJobConf)
        val partitionValues = partition.getTPartition.getValues

        val partitionInternalRow = new GenericInternalRow(partitionValues.size())
        for (partitionIndex <- 0 until partitionValues.size) {
          partitionInternalRow.update(partitionIndex, partitionValues.get(partitionIndex))
        }

        val inputFormatClass = partDesc.getInputFileFormatClass
          .asInstanceOf[Class[newInputClass[Writable, Writable]]]
        arrayPartitionedFile += getArrayPartitionedFile(newJobConf, inputFormatClass, partitionInternalRow)
      }
      arrayPartitionedFile
        .sortBy(_.length)(implicitly[Ordering[Long]].reverse)
        .toArray
    } else {
      val inputFormatClass = nativeTable.getInputFormatClass.asInstanceOf[Class[newInputClass[Writable, Writable]]]
      getArrayPartitionedFile(newJobConf, inputFormatClass, new GenericInternalRow(0))
        .sortBy(_.length)(implicitly[Ordering[Long]].reverse)
        .toArray
    }
    arrayFilePartition += FilePartition.getFilePartitions(SparkSession.getActiveSession.get,
      partitionedFiles,
      getMaxSplitBytes(SparkSession.getActiveSession.get)).toArray
    arrayFilePartition.toArray
  }

  private def getMaxSplitBytes(sparkSession: SparkSession): Long = {
    val defaultMaxSplitBytes = sparkSession.sessionState.conf.filesMaxPartitionBytes
    val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
    Math.min(defaultMaxSplitBytes, openCostInBytes)
  }

  private def getArrayPartitionedFile(newJobConf: JobConf,
                               inputFormatClass: Class[newInputClass[Writable, Writable]],
                               partitionInternalRow: GenericInternalRow): ArrayBuffer[PartitionedFile] = {
    val allInputSplits = getInputFormat(newJobConf, inputFormatClass).getSplits(newJobConf, minPartitions)
    val inputSplits = if (ignoreEmptySplits) {
      allInputSplits.filter(_.getLength > 0)
    } else {
      allInputSplits
    }
    inputFormatClass match {
      case OrcInputFormat =>
      case MapredParquetInputFormat =>
      case _ =>
    }
    val arrayFilePartition = ArrayBuffer[PartitionedFile]()
    for (i <- 0 until inputSplits.size) {
      val inputSplit = inputSplits(i)
      inputSplit match {
        case FileSplit =>
          val orcInputSplit = inputSplit.asInstanceOf[FileSplit]
          arrayFilePartition +=
            Shims.get.getPartitionedFile(partitionInternalRow, orcInputSplit.getPath.toString,
              orcInputSplit.getStart, orcInputSplit.getLength)
      }
    }
    arrayFilePartition
  }

  private def getInputFormat(conf: JobConf, inputFormatClass: Class[newInputClass[Writable, Writable]]):
  InputFormat[Writable, Writable] = {
    val newInputFormat = ReflectionUtils.newInstance(inputFormatClass.asInstanceOf[Class[_]], conf)
      .asInstanceOf[InputFormat[Writable, Writable]]
    newInputFormat match {
      case c: Configurable => c.setConf(conf)
      case _ =>
    }
    newInputFormat
  }

}

object HiveTableUtil {
  private val orcFormat = "OrcInputFormat"
  private val parquetFormat = "MapredParquetInputFormat"

  def getFileFormat(inputFormatClass: Class[_ <: InputFormat[_, _]]): String = {
    if (inputFormatClass.getSimpleName.equalsIgnoreCase(orcFormat)) {
      "orc"
    } else if (inputFormatClass.getSimpleName.equalsIgnoreCase(parquetFormat)) {
      "parquet"
    } else {
      "other"
    }
  }

}