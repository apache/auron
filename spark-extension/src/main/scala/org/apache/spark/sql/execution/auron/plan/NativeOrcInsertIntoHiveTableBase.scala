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

import java.util.Locale
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingDeque

import scala.collection.immutable.SortedMap

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.FileSinkOperator
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.apache.hadoop.hive.ql.io.orc.OrcSerde
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.FileOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.RecordWriter
import org.apache.hadoop.util.Progressable
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.auron.AuronConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.BasicWriteJobStatsTracker
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.StructType

object NativeOrcInsertIntoHiveTableBase {
  def dataSchema(table: CatalogTable, partition: Map[String, Option[String]]): StructType =
    StructType(table.schema.dropRight(partition.size))
}

abstract class NativeOrcInsertIntoHiveTableBase(
    cmd: InsertIntoHiveTable,
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++
    BasicWriteJobStatsTracker.metrics ++
    Map(
      NativeHelper
        .getDefaultNativeMetrics(sparkContext)
        .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
        .toSeq
        :+ ("io_time", SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time"))
        :+ ("bytes_written",
        SQLMetrics
          .createSizeMetric(sparkContext, "Native.bytes_written")): _*)

  def check(): Unit = {
    val tblStorage = cmd.table.storage
    val outputFormatClassName = tblStorage.outputFormat.getOrElse("").toLowerCase(Locale.ROOT)

    assert(outputFormatClassName.endsWith("orcoutputformat"), "not orc format")
    val dataSchema = NativeOrcInsertIntoHiveTableBase.dataSchema(cmd.table, cmd.partition)
    assert(
      AuronConverters.isOrcWriteSchemaSupported(dataSchema),
      AuronConverters.unsupportedOrcWriteSchemaMessage(dataSchema))
  }
  check()

  @transient
  val wrapped: DataWritingCommandExec = {
    val transformedTable = {
      val tblStorage = cmd.table.storage
      cmd.table.withNewStorage(
        tblStorage.locationUri,
        tblStorage.inputFormat,
        outputFormat = Some(classOf[AuronOrcOutputFormat].getName),
        tblStorage.compressed,
        serde = Some(classOf[OrcSerde].getName),
        tblStorage.properties)
    }

    val transformedCmd = getInsertIntoHiveTableCommand(
      transformedTable,
      cmd.partition,
      cmd.query,
      cmd.overwrite,
      cmd.ifPartitionNotExists,
      cmd.outputColumnNames,
      metrics)
    DataWritingCommandExec(transformedCmd, child)
  }

  override def output: Seq[Attribute] = wrapped.output
  override def outputPartitioning: Partitioning = wrapped.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = wrapped.outputOrdering
  override def doExecute(): RDD[InternalRow] = wrapped.execute()

  override def executeCollect(): Array[InternalRow] = wrapped.executeCollect()
  override def executeTake(n: Int): Array[InternalRow] = wrapped.executeTake(n)
  override def executeToIterator(): Iterator[InternalRow] = wrapped.executeToIterator()

  override def doExecuteNative(): NativeRDD = {
    Shims.get.createConvertToNativeExec(wrapped).executeNative()
  }

  override def nodeName: String =
    s"NativeOrcInsert ${cmd.table.identifier.unquotedString}"

  protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable
}

// A dummy output format which does not write anything but only pass output path to native OrcSinkExec.
class AuronOrcOutputFormat
    extends FileOutputFormat[NullWritable, NullWritable]
    with HiveOutputFormat[NullWritable, NullWritable] {

  override def getRecordWriter(
      fileSystem: FileSystem,
      jobConf: JobConf,
      name: String,
      progressable: Progressable): RecordWriter[NullWritable, NullWritable] =
    throw new NotImplementedError()

  override def getHiveRecordWriter(
      jobConf: JobConf,
      finalOutPath: Path,
      valueClass: Class[_ <: Writable],
      isCompressed: Boolean,
      tableProperties: Properties,
      progress: Progressable): FileSinkOperator.RecordWriter = {

    new FileSinkOperator.RecordWriter {
      override def write(w: Writable): Unit = {
        OrcSinkTaskContext.get.processingOutputFiles.offer(finalOutPath.toString)
      }

      override def close(abort: Boolean): Unit = {}
    }
  }
}

class OrcSinkTaskContext {
  var isNative: Boolean = false
  val processingOutputFiles = new LinkedBlockingDeque[String]()
  val processedOutputFiles = new LinkedBlockingDeque[OutputFileStat]()
}

object OrcSinkTaskContext {
  private val instances = new ConcurrentHashMap[Long, OrcSinkTaskContext]()

  def get: OrcSinkTaskContext = {
    val taskContext = TaskContext.get()
    val taskId = taskContext.taskAttemptId()
    val existing = instances.get(taskId)
    if (existing != null) {
      existing
    } else {
      val created = new OrcSinkTaskContext
      val previous = instances.putIfAbsent(taskId, created)
      if (previous == null) {
        taskContext.addTaskCompletionListener[Unit](_ => instances.remove(taskId, created))
        created
      } else {
        previous
      }
    }
  }
}
