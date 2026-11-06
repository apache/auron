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
import java.util.{Locale, UUID}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileSystem
import org.apache.paimon.spark.write.InnerTableWriteTaskResult
import org.apache.paimon.table.FileStoreTable
import org.apache.spark.{OneToOneDependency, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.{AuronConverters, NativeConverters, NativeHelper, NativePartition, NativeRDD, Shims}
import org.apache.spark.sql.auron.paimon.PaimonSinkWriter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.{Write, WriterCommitMessage}
import org.apache.spark.sql.execution.{ColumnarToRowExec, InputAdapter, ProjectExec, SparkPlan, UnaryExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.V2CommandExec
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

import org.apache.auron.{protobuf => pb}
import org.apache.auron.jni.JniBridge
import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.spark.configuration.SparkAuronConfiguration

/**
 * Replaces a Paimon DataSourceV2 append/overwrite node. Data files are written by the native
 * PaimonSinkExec, while commit (including overwrite semantics and schema evolution) is still
 * performed by Paimon's own BatchWrite, so snapshots are identical to a Paimon write.
 *
 * With AQE, `child` is an AdaptiveSparkPlanExec whose stages are converted to native by the Auron
 * columnar rule while being planned; the final plan is converted to native here if needed.
 */
case class NativePaimonWriteExec(
    child: SparkPlan,
    write: Write,
    table: FileStoreTable,
    refreshCache: () => Unit,
    originalNodeName: String)
    extends V2CommandExec
    with UnaryExecNode {

  override def output: Seq[Attribute] = Nil

  override lazy val metrics: Map[String, SQLMetric] =
    NativeHelper.getDefaultNativeMetrics(
      sparkContext,
      Set("stage_id", "output_rows", "elapsed_compute")) ++ Map(
      "io_time" -> SQLMetrics.createNanoTimingMetric(sparkContext, "Native.io_time"),
      "bytes_written" -> SQLMetrics.createSizeMetric(sparkContext, "Native.bytes_written"),
      "files_written" -> SQLMetrics.createMetric(sparkContext, "Native.files_written"))

  override def nodeName: String = s"NativePaimonWrite($originalNodeName) ${table.fullName()}"

  override protected def run(): Seq[InternalRow] = {
    val batchWrite = write.toBatch
    val inputRDD = NativeHelper.executeNative(nativeInput)
    val sinkRDD = buildSinkRDD(inputRDD)
    val messages = new Array[WriterCommitMessage](sinkRDD.partitions.length)
    try {
      sparkContext.runJob(
        sinkRDD,
        (_: TaskContext, iter: Iterator[InternalRow]) => {
          while (iter.hasNext) iter.next()
          val writer = PaimonSinkTaskContext.get.asInstanceOf[PaimonSinkWriter]
          val result = InnerTableWriteTaskResult.fromCommitMessages(writer.commitMessages())
          writer.cleanupStaging()
          result: WriterCommitMessage
        },
        sinkRDD.partitions.indices,
        (index: Int, result: WriterCommitMessage) => messages(index) = result)
      logInfo(s"Committing native paimon write to ${table.fullName()}")
      batchWrite.commit(messages)
    } catch {
      case cause: Throwable =>
        logError(s"Native paimon write to ${table.fullName()} is aborting.", cause)
        try {
          batchWrite.abort(messages.filter(_ != null))
        } catch {
          case t: Throwable => cause.addSuppressed(t)
        }
        throw cause
    }
    refreshCache()
    Nil
  }

  private lazy val nativeInput: SparkPlan = {
    val finalPlan = child match {
      case a: AdaptiveSparkPlanExec => a.finalPhysicalPlan
      case p => p
    }
    if (NativeHelper.isNative(finalPlan)) {
      finalPlan
    } else {
      NativePaimonWriteExec.convertToNativeNarrow(finalPlan)
    }
  }

  /** The native plan that feeds the sink, available after execution. Visible for tests. */
  def convertedInputForTesting: SparkPlan = nativeInput

  private def buildSinkRDD(inputRDD: NativeRDD): NativeRDD = {
    val sparkSession = session
    val hadoopConf = new SerializableConfiguration(sparkSession.sessionState.newHadoopConf())
    val options = table.coreOptions()
    val tableOptions = table.options().asScala.toMap
    val fieldNames = table.rowType().getFieldNames.asScala
    val dataSchema = StructType(child.output.zip(fieldNames).map { case (attr, name) =>
      StructField(name, attr.dataType, attr.nullable)
    })
    val fieldIds = table.rowType().getFields.asScala.map(f => f.name() -> f.id()).toMap
    val nativeDataSchema = NativeConverters.convertSchema(dataSchema, fieldIds)
    val partitionColIndices =
      table.partitionKeys().asScala.map(k => fieldNames.indexOf(k)).map(Int.box)
    val targetFileSize = options.targetFileSize(false)
    val parquetProps = PaimonNativeWriteProps(tableOptions, options.fileCompression())
    val maxOpenFiles =
      sparkSession.conf.get("spark.auron.paimon.write.maxOpenFiles", "32").toInt
    val memoryBudget =
      sparkSession.conf.get("spark.auron.paimon.write.memoryBudget", s"${256L << 20}").toLong
    val tableRef = table

    val nativeMetrics = SparkMetricNode(metrics, inputRDD.metrics :: Nil)
    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions =
        inputRDD.partitions.map(p => NativePartition[org.apache.spark.Partition](p.index, p)),
      inputRDD.partitioner,
      new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, context) => {
        val writer = new PaimonSinkWriter(tableRef)
        PaimonSinkTaskContext.register(writer)
        context.addTaskFailureListener((_, _) => writer.abort())

        val resourceId = s"NativePaimonSinkExec:${UUID.randomUUID().toString}"
        JniBridge.putResource(
          resourceId,
          (location: String) => {
            NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
              override def run(): FileSystem = FileSystem.get(new URI(location), hadoopConf.value)
            })
          })
        val inputPartition = NativePartition.unwrap(partition)
        pb.PhysicalPlanNode
          .newBuilder()
          .setPaimonSink(
            pb.PaimonSinkExecNode
              .newBuilder()
              .setInput(inputRDD.nativePlan(inputPartition, context))
              .setFsResourceId(resourceId)
              .addAllPartitionColIndices(partitionColIndices.asJava)
              .setDataSchema(nativeDataSchema)
              .setTargetFileSize(targetFileSize)
              .setMaxOpenFiles(maxOpenFiles)
              .setMemoryBudget(memoryBudget)
              .addAllProp(parquetProps.map { case (k, v) =>
                pb.ParquetProp.newBuilder().setKey(k).setValue(v).build()
              }.asJava))
          .build()
      },
      friendlyName = "NativeRDD.PaimonSink")
  }

  override protected def withNewChildInternal(newChild: SparkPlan): NativePaimonWriteExec =
    copy(child = newChild)
}

object NativePaimonWriteExec extends Logging {

  /**
   * Converts a non-native write input to native. A JVM row-to-Arrow conversion costs roughly in
   * proportion to the number of columns it carries, so when the input is a projection that only
   * renames, casts or fills in constants (typical for `INSERT ... SELECT` into a wide table),
   * convert the narrower projection child and run the projection natively instead. Falls back to
   * converting the whole input when the projection cannot be executed natively.
   */
  def convertToNativeNarrow(plan: SparkPlan): SparkPlan = plan match {
    // a whole-stage-codegen'ed projection over a non-codegen input: the projection is
    // replaced by a native one, so the codegen wrapper is not needed any more
    case WholeStageCodegenExec(project @ ProjectExec(_, CodegenInput(input)))
        if isNarrowing(project) =>
      narrow(plan, project.withNewChildren(input :: Nil).asInstanceOf[ProjectExec])
    case project: ProjectExec if isNarrowing(project) =>
      narrow(plan, project)
    case p if NativeHelper.isNative(p) => p
    case p => Shims.get.createConvertToNativeExec(p)
  }

  private def isNarrowing(project: ProjectExec): Boolean =
    SparkAuronConfiguration.ENABLE_PAIMON_WRITE_NARROW_CONVERT.get() &&
      project.child.output.size < project.output.size

  /**
   * The input of a codegen stage: `InputAdapter(p)`, or `ColumnarToRow(InputAdapter(p))` for a
   * columnar `p` (ConvertToNative reads columnar input directly).
   */
  private object CodegenInput {
    def unapply(plan: SparkPlan): Option[SparkPlan] = plan match {
      case InputAdapter(input) => Some(input)
      case ColumnarToRowExec(InputAdapter(input)) => Some(input)
      case _ => None
    }
  }

  private def narrow(original: SparkPlan, project: ProjectExec): SparkPlan = {
    try {
      val narrowed = Shims.get.createNativeProjectExec(
        project.projectList,
        AuronConverters.addRenameColumnsExec(convertToNativeNarrow(project.child)))
      logInfo(
        s"Native paimon write converts ${project.child.output.size} input columns " +
          s"instead of ${project.output.size} projected columns")
      narrowed
    } catch {
      case e @ (_: NotImplementedError | _: AssertionError | _: Exception) =>
        logWarning(s"Converting the whole write input to native: ${e.getMessage}")
        Shims.get.createConvertToNativeExec(original)
    }
  }
}

/** Maps Paimon table options to the parquet writer properties understood by the native sink. */
object PaimonNativeWriteProps {
  def apply(tableOptions: Map[String, String], fileCompression: String): Seq[(String, String)] = {
    val compression = fileCompression.toLowerCase(Locale.ROOT) match {
      case "none" | "uncompressed" => "UNCOMPRESSED"
      case other => other.toUpperCase(Locale.ROOT)
    }
    val zstdLevel = tableOptions.get("file.compression.zstd-level").getOrElse("1")
    // Paimon forwards `parquet.*` table options to the parquet writer as is
    val parquetOptions = tableOptions.filter(_._1.startsWith("parquet."))
    Seq(
      "parquet.compression" -> compression,
      "parquet.compression.codec.zstd.level" -> zstdLevel) ++
      parquetOptions.toSeq.filterNot(kv =>
        kv._1 == "parquet.compression" || kv._1 == "parquet.compression.codec.zstd.level")
  }
}
