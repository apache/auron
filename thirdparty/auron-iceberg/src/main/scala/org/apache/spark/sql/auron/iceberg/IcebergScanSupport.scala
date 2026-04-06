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
package org.apache.spark.sql.auron.iceberg

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.iceberg.{FileFormat, FileScanTask, MetadataColumns}
import org.apache.iceberg.expressions.Expressions
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.types.StructType

// fileSchema is read from the data files. partitionSchema carries supported metadata columns
// (for example _file) that are materialized as per-file constant values in the native scan.
final case class IcebergScanPlan(
    fileTasks: Seq[FileScanTask],
    fileFormat: FileFormat,
    readSchema: StructType,
    fileSchema: StructType,
    partitionSchema: StructType)

object IcebergScanSupport extends Logging {

  def plan(exec: BatchScanExec): Option[IcebergScanPlan] = {
    val scan = exec.scan
    val scanClassName = scan.getClass.getName
    // Only handle Iceberg scans; other sources must stay on Spark's path.
    if (!scanClassName.startsWith("org.apache.iceberg.spark.source.")) {
      return None
    }

    // Changelog scan carries row-level changes; not supported by native COW-only path.
    if (scanClassName == "org.apache.iceberg.spark.source.SparkChangelogScan") {
      return None
    }

    val readSchema = scan.readSchema
    val unsupportedMetadataColumns = collectUnsupportedMetadataColumns(readSchema)
    // Native scan can project file-level metadata columns such as _file via partition values.
    // Metadata columns that require per-row materialization (for example _pos) still fallback.
    if (unsupportedMetadataColumns.nonEmpty) {
      return None
    }

    val fileSchema = StructType(readSchema.fields.filterNot(isSupportedMetadataColumn))
    // Supported metadata columns are materialized via per-file constant values rather than
    // read from the Iceberg data file payload.
    val partitionSchema = StructType(readSchema.fields.filter(isSupportedMetadataColumn))

    if (!fileSchema.fields.forall(field => NativeConverters.isTypeSupported(field.dataType))) {
      return None
    }

    if (!partitionSchema.fields.forall(field =>
        NativeConverters.isTypeSupported(field.dataType))) {
      return None
    }

    val partitions = inputPartitions(exec)
    // Empty scan (e.g. empty table) should still build a plan to return no rows.
    if (partitions.isEmpty) {
      logWarning(s"Native Iceberg scan planned with empty partitions for $scanClassName.")
      return Some(
        IcebergScanPlan(Seq.empty, FileFormat.PARQUET, readSchema, fileSchema, partitionSchema))
    }

    val icebergPartitions = partitions.flatMap(icebergPartition)
    // All partitions must be Iceberg SparkInputPartition; otherwise fallback.
    if (icebergPartitions.size != partitions.size) {
      return None
    }

    val fileTasks = icebergPartitions.flatMap(_.fileTasks)

    // Native scan does not apply delete files; only allow pure data files (COW).
    if (!fileTasks.forall(task => task.deletes() == null || task.deletes().isEmpty)) {
      return None
    }

    // Residual filters require row-level evaluation, not supported in native scan.
    if (!fileTasks.forall(task => Expressions.alwaysTrue().equals(task.residual()))) {
      return None
    }

    // Native scan handles a single file format; mixed formats must fallback.
    val formats = fileTasks.map(_.file().format()).distinct
    if (formats.size > 1) {
      return None
    }

    val format = formats.headOption.getOrElse(FileFormat.PARQUET)
    if (format != FileFormat.PARQUET && format != FileFormat.ORC) {
      return None
    }

    Some(IcebergScanPlan(fileTasks, format, readSchema, fileSchema, partitionSchema))
  }

  private def collectUnsupportedMetadataColumns(schema: StructType): Seq[String] =
    schema.fields.collect {
      case field
          if MetadataColumns.isMetadataColumn(field.name) &&
            !isSupportedMetadataColumn(field) =>
        field.name
    }

  private def isSupportedMetadataColumn(field: org.apache.spark.sql.types.StructField): Boolean =
    field.name == MetadataColumns.FILE_PATH.name()

  private def inputPartitions(exec: BatchScanExec): Seq[InputPartition] = {
    // Prefer DataSource V2 batch API; if not available, fallback to exec methods via reflection.
    val fromBatch =
      try {
        val batch = exec.scan.toBatch
        if (batch != null) {
          batch.planInputPartitions().toSeq
        } else {
          Seq.empty
        }
      } catch {
        case t: Throwable =>
          logWarning(
            s"Failed to plan input partitions via DataSource V2 batch API for " +
              s"${exec.getClass.getName}; falling back to reflective methods.",
            t)
          Seq.empty
      }
    if (fromBatch.nonEmpty) {
      return fromBatch
    }

    // Some Spark versions expose partitions through inputPartitions/partitions methods on BatchScanExec.
    val methods = exec.getClass.getMethods
    val inputPartitionsMethod = methods.find(_.getName == "inputPartitions")
    val partitionsMethod = methods.find(_.getName == "partitions")

    try {
      val raw = inputPartitionsMethod
        .orElse(partitionsMethod)
        .map(_.invoke(exec))
        .getOrElse(Seq.empty)

      // Normalize to Seq[InputPartition], flattening nested Seq if needed.
      raw match {
        case seq: scala.collection.Seq[_]
            if seq.nonEmpty &&
              seq.head.isInstanceOf[scala.collection.Seq[_]] =>
          seq
            .asInstanceOf[scala.collection.Seq[scala.collection.Seq[InputPartition]]]
            .flatten
            .toSeq
        case seq: scala.collection.Seq[_] =>
          seq.asInstanceOf[scala.collection.Seq[InputPartition]].toSeq
        case _ =>
          Seq.empty
      }
    } catch {
      case NonFatal(t) =>
        logWarning(
          s"Failed to obtain input partitions via reflection for ${exec.getClass.getName}.",
          t)
        Seq.empty
    }
  }

  private case class IcebergPartitionView(fileTasks: Seq[FileScanTask])

  private def icebergPartition(partition: InputPartition): Option[IcebergPartitionView] = {
    val className = partition.getClass.getName
    // Only accept Iceberg SparkInputPartition to access task groups.
    if (className != "org.apache.iceberg.spark.source.SparkInputPartition") {
      return None
    }

    try {
      // SparkInputPartition is package-private; use reflection to read its task group.
      val taskGroupField = partition.getClass.getDeclaredField("taskGroup")
      taskGroupField.setAccessible(true)
      val taskGroup = taskGroupField.get(partition)

      // Extract tasks and keep only file scan tasks.
      val tasksMethod = taskGroup.getClass.getDeclaredMethod("tasks")
      tasksMethod.setAccessible(true)
      val tasks = tasksMethod.invoke(taskGroup).asInstanceOf[java.util.Collection[_]].asScala
      val fileTasks = tasks.collect { case task: FileScanTask => task }.toSeq

      // If any task is not a FileScanTask, fallback.
      if (fileTasks.size != tasks.size) {
        return None
      }

      Some(IcebergPartitionView(fileTasks))
    } catch {
      case NonFatal(t) =>
        logDebug(s"Failed to read Iceberg SparkInputPartition via reflection for $className.", t)
        None
    }
  }
}
