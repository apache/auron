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
package org.apache.spark.sql.auron.paimon

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.source.{DataSplit, Split}
import org.apache.paimon.utils.RowDataToObjectArrayConverter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.hive.auron.paimon.PaimonUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

final case class PaimonFile(filePath: String, fileSize: Long, partitionValues: InternalRow)

final case class PaimonScanPlan(
    table: FileStoreTable,
    files: Seq[PaimonFile],
    fileFormat: String,
    readSchema: StructType,
    fileSchema: StructType,
    partitionSchema: StructType)

object PaimonScanSupport extends Logging {

  private val PaimonBaseScanClassName = "org.apache.paimon.spark.PaimonBaseScan"
  private val PaimonInputPartitionClassName = "org.apache.paimon.spark.PaimonInputPartition"

  def isPaimonScan(exec: BatchScanExec): Boolean = isPaimonScan(exec.scan)

  private def isPaimonScan(scan: AnyRef): Boolean = {
    isInstanceOfClass(scan, PaimonBaseScanClassName)
  }

  def plan(exec: BatchScanExec): Option[PaimonScanPlan] = {
    val scan = exec.scan
    if (!isPaimonScan(scan)) {
      return None
    }

    val table = paimonTable(scan) match {
      case Some(t) => t
      case None =>
        logDebug("Skip native Paimon scan: cannot resolve FileStoreTable from PaimonScan.")
        return None
    }

    // Append-only tables (no primary key) have no merge process and can always be read
    // from raw files; primary-key tables are only supported in COW mode. Either way the
    // per-split rawConvertible/deletion-files checks below remain the final safety net.
    val isAppendOnlyTable = table.primaryKeys().isEmpty
    if (!isAppendOnlyTable && !PaimonUtil.isPaimonCowTable(table)) {
      logDebug(
        "Skip native Paimon scan: only append-only or COW primary-key tables are supported.")
      return None
    }

    val fileFormat = PaimonUtil.paimonFileFormat(table)
    if (!fileFormat.equalsIgnoreCase(PaimonUtil.parquetFormat) &&
      !fileFormat.equalsIgnoreCase(PaimonUtil.orcFormat)) {
      logDebug(s"Skip native Paimon scan: unsupported file format $fileFormat.")
      return None
    }

    val readSchema = scan.readSchema()
    if (!readSchema.fields.forall(f => NativeConverters.isTypeSupported(f.dataType))) {
      logDebug("Skip native Paimon scan: unsupported column data type in read schema.")
      return None
    }

    val partitionKeys = table.schema().partitionKeys().asScala.toSet
    val partitionFields = readSchema.fields.filter(f => containsName(partitionKeys, f.name))
    val fileFields = readSchema.fields.filterNot(f => containsName(partitionKeys, f.name))
    val partitionSchema = StructType(partitionFields)
    val fileSchema = StructType(fileFields)

    val partitions = inputPartitions(exec) match {
      case Some(p) => p
      case None =>
        logDebug("Skip native Paimon scan: failed to obtain input partitions.")
        return None
    }
    if (partitions.isEmpty) {
      logDebug("Paimon scan planned with empty input partitions.")
      return Some(
        PaimonScanPlan(table, Seq.empty, fileFormat, readSchema, fileSchema, partitionSchema))
    }

    val splitsOpt = collectSplits(partitions)
    val splits = splitsOpt match {
      case Some(s) => s
      case None =>
        logDebug("Skip native Paimon scan: cannot extract splits from input partitions.")
        return None
    }

    // Only allow COW-style raw-readable splits; reject MOR/MOW or splits with deletion vectors.
    val unsupported = splits.find { s =>
      !s.rawConvertible() ||
      (s.deletionFiles().isPresent && {
        val list = s.deletionFiles().get()
        list != null && list.asScala.exists(_ != null)
      })
    }
    if (unsupported.isDefined) {
      logDebug("Skip native Paimon scan: split is not raw-convertible or has deletion files.")
      return None
    }

    val partitionConverter = new RowDataToObjectArrayConverter(
      table.schema().logicalPartitionType())
    val sessionLocalTimeZone = SQLConf.get.sessionLocalTimeZone
    val tzOption: String = {
      val props = CaseInsensitiveMap(table.options().asScala.toMap)
      props.getOrElse(DateTimeUtils.TIMEZONE_OPTION, sessionLocalTimeZone)
    }

    val files = splits.flatMap { split =>
      val partitionValues = if (partitionSchema.isEmpty) {
        InternalRow.empty
      } else {
        toPartitionRow(
          partitionConverter.convert(split.partition()),
          partitionSchema,
          table.schema().partitionKeys().asScala.toSeq,
          tzOption)
      }
      split.dataFiles().asScala.map { dataFile =>
        val filePath = s"${split.bucketPath()}/${dataFile.fileName()}"
        PaimonFile(filePath, dataFile.fileSize(), partitionValues)
      }
    }

    Some(PaimonScanPlan(table, files, fileFormat, readSchema, fileSchema, partitionSchema))
  }

  private def containsName(names: Set[String], target: String): Boolean = {
    val resolver = SQLConf.get.resolver
    names.exists(n => resolver(n, target))
  }

  // Build a Spark InternalRow for partition values matching partitionSchema's data types.
  // Partition values from Paimon are returned in the table's partition-key order; we reorder
  // them to match partitionSchema and cast strings/temporals into the requested types.
  private def toPartitionRow(
      paimonValues: Array[AnyRef],
      partitionSchema: StructType,
      partitionKeys: Seq[String],
      timeZoneId: String): InternalRow = {
    val resolver = SQLConf.get.resolver
    val indexByName = partitionKeys.zipWithIndex.toMap
    InternalRow.fromSeq(partitionSchema.fields.map { field =>
      val idx = indexByName
        .find { case (k, _) => resolver(k, field.name) }
        .map(_._2)
        .getOrElse(-1)
      val raw = if (idx >= 0 && idx < paimonValues.length) paimonValues(idx) else null
      val literal: Literal = raw match {
        case null => Literal(null, field.dataType)
        case v => Literal(v.toString)
      }
      Cast(literal, field.dataType, Option(timeZoneId)).eval()
    })
  }

  private def collectSplits(partitions: Seq[InputPartition]): Option[Seq[DataSplit]] = {
    val buf = scala.collection.mutable.ArrayBuffer.empty[DataSplit]
    partitions.foreach { p =>
      if (!isInstanceOfClass(p, PaimonInputPartitionClassName)) {
        return None
      }
      val splits = invokeMethod(p, "splits") match {
        case Some(s: scala.collection.Seq[_]) => s.toSeq
        case _ => return None
      }
      splits.foreach {
        case ds: DataSplit => buf += ds
        case _: Split => return None
        case _ => return None
      }
    }
    Some(buf.toSeq)
  }

  private def paimonTable(scan: AnyRef): Option[FileStoreTable] = {
    invokeMethod(scan, "table") match {
      case Some(t: FileStoreTable) => Some(t)
      case Some(other) =>
        logDebug(s"Unexpected Paimon table type: ${other.getClass.getName}")
        None
      case None => None
    }
  }

  // DSv2 BatchScanExec exposes input partitions via Scan.toBatch (preferred) or a method on
  // the exec itself; the latter varies across Spark versions, so we attempt both.
  // Returns Some(partitions) on success (possibly empty if the table is empty), or None when
  // partition planning fails - the caller falls back to Spark execution on None.
  private def inputPartitions(exec: BatchScanExec): Option[Seq[InputPartition]] = {
    try {
      val batch = exec.scan.toBatch
      if (batch != null) {
        val parts = batch.planInputPartitions()
        if (parts != null) return Some(parts.toSeq)
        logWarning("Paimon Scan.toBatch.planInputPartitions() returned null.")
        return None
      }
      logWarning("Paimon Scan.toBatch returned null.")
    } catch {
      case NonFatal(t) =>
        logWarning("Failed to plan Paimon input partitions via Scan.toBatch.", t)
        return None
    }

    val methods = exec.getClass.getMethods
    val m =
      methods.find(_.getName == "inputPartitions").orElse(methods.find(_.getName == "partitions"))
    if (m.isEmpty) {
      logWarning(
        "BatchScanExec exposes no inputPartitions/partitions method; cannot plan Paimon scan.")
      return None
    }
    try {
      m.map(_.invoke(exec)) match {
        case Some(s: scala.collection.Seq[_])
            if s.nonEmpty && s.head.isInstanceOf[scala.collection.Seq[_]] =>
          Some(
            s.asInstanceOf[scala.collection.Seq[scala.collection.Seq[InputPartition]]]
              .flatten
              .toSeq)
        case Some(s: scala.collection.Seq[_]) =>
          Some(s.asInstanceOf[scala.collection.Seq[InputPartition]].toSeq)
        case other =>
          logWarning(
            s"Unexpected return type from BatchScanExec partitions method: ${other.getClass}.")
          None
      }
    } catch {
      case NonFatal(t) =>
        logWarning("Failed to read Paimon input partitions via reflection.", t)
        None
    }
  }

  private def isInstanceOfClass(obj: AnyRef, className: String): Boolean = {
    if (obj == null) return false
    var c: Class[_] = obj.getClass
    while (c != null) {
      if (c.getName == className) return true
      c.getInterfaces.foreach { i =>
        if (i.getName == className) return true
      }
      c = c.getSuperclass
    }
    false
  }

  private def invokeMethod(target: AnyRef, methodName: String): Option[Any] = {
    try {
      var cls: Class[_] = target.getClass
      while (cls != null) {
        cls.getDeclaredMethods.find(m =>
          m.getName == methodName && m.getParameterCount == 0) match {
          case Some(m) =>
            m.setAccessible(true)
            return Some(m.invoke(target))
          case None =>
            cls = cls.getSuperclass
        }
      }
      None
    } catch {
      case NonFatal(t) =>
        logDebug(s"Failed to invoke $methodName on ${target.getClass.getName}", t)
        None
    }
  }
}
