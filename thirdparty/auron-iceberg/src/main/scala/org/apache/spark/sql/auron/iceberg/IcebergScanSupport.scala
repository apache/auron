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

import org.apache.iceberg.{AddedRowsScanTask, ChangelogOperation, ChangelogScanTask, FileFormat, FileScanTask, MetadataColumns, ScanTask}
import org.apache.iceberg.expressions.{And => IcebergAnd, BoundPredicate, Expression => IcebergExpression, Not => IcebergNot, Or => IcebergOr, UnboundPredicate}
import org.apache.iceberg.spark.source.AuronIcebergSourceUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.catalyst.expressions.{And => SparkAnd, AttributeReference, EqualTo, Expression => SparkExpression, GreaterThan, GreaterThanOrEqual, In, IsNaN, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not => SparkNot, Or => SparkOr}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, DecimalType, StringType, StructField, StructType}

import org.apache.auron.{protobuf => pb}

// fileSchema is read from the data files. partitionSchema carries supported metadata columns
// (for example _file and _spec_id) that are materialized as per-file constant values in
// the native scan.
final case class IcebergNativeScanTask(
    location: String,
    start: Long,
    length: Long,
    fileSizeInBytes: Long,
    partitionValues: Seq[Any])

final case class IcebergScanPlan(
    scanTasks: Seq[IcebergNativeScanTask],
    fileFormat: FileFormat,
    readSchema: StructType,
    fileSchema: StructType,
    partitionSchema: StructType,
    pruningPredicates: Seq[pb.PhysicalExprNode],
    fieldIdsByName: Map[String, Int])

object IcebergScanSupport extends Logging {
  private val scanPlanTag: TreeNodeTag[Option[IcebergScanPlan]] = TreeNodeTag(
    "auron.iceberg.scan.plan")

  private val SparkChangelogScanClassName =
    "org.apache.iceberg.spark.source.SparkChangelogScan"

  private val ChangelogMetadataColumnNames = Set(
    MetadataColumns.CHANGE_TYPE.name(),
    MetadataColumns.CHANGE_ORDINAL.name(),
    MetadataColumns.COMMIT_SNAPSHOT_ID.name())

  def isIcebergScan(scan: Scan): Boolean =
    scan.getClass.getName == SparkChangelogScanClassName ||
      AuronIcebergSourceUtil.getClassOfSparkBatchQueryScan.isInstance(scan)

  def fallbackReason(exec: BatchScanExec): Option[String] = {
    val scan = exec.scan
    if (!isIcebergScan(scan)) {
      return None
    }

    val isChangelogScan = scan.getClass.getName == SparkChangelogScanClassName
    if (collectUnsupportedMetadataColumns(scan.readSchema, isChangelogScan).nonEmpty) {
      Some("Has per-row materialization (for example _pos).")
    } else {
      None
    }
  }

  def plan(exec: BatchScanExec): Option[IcebergScanPlan] = {
    exec.getTagValue(scanPlanTag) match {
      case Some(cached) => cached
      case None =>
        val planned = planUncached(exec)
        exec.setTagValue(scanPlanTag, planned)
        planned
    }
  }

  private def planUncached(exec: BatchScanExec): Option[IcebergScanPlan] = {
    val scan = exec.scan
    val scanClassName = scan.getClass.getName
    // Only handle Iceberg scans; other sources must stay on Spark's path.
    if (scanClassName == SparkChangelogScanClassName) {
      return planChangelogScan(exec, scan)
    }

    if (!AuronIcebergSourceUtil.getClassOfSparkBatchQueryScan.isInstance(scan)) {
      return None
    }

    planFileScan(exec, scan, scanClassName)
  }

  private def planFileScan(
      exec: BatchScanExec,
      scan: Scan,
      scanClassName: String): Option[IcebergScanPlan] = {
    val readSchema = scan.readSchema
    val schemas = supportedSchemas(readSchema, isChangelogScan = false)
    if (schemas.isEmpty) {
      return None
    }
    val (fileSchema, partitionSchema) = schemas.get

    val (fieldIdsByName, renameOrDrop) =
      inspectFieldIdSupport(
        fileSchema,
        scan.asInstanceOf[AnyRef],
        AuronIcebergSourceUtil.expectedFieldIds,
        AuronIcebergSourceUtil.detectRenameOrDrop) match {
        case Some(fieldIdSupport) => fieldIdSupport
        case None => return None
      }

    val partitions = inputPartitions(exec)
    // Empty scan (e.g. empty table) should still build a plan to return no rows.
    if (partitions.isEmpty) {
      logWarning(s"Native Iceberg scan planned with empty partitions for $scanClassName.")
      return Some(
        IcebergScanPlan(
          Seq.empty,
          FileFormat.PARQUET,
          readSchema,
          fileSchema,
          partitionSchema,
          Seq.empty,
          fieldIdsByName))
    }

    val icebergPartitions = partitions.flatMap(icebergPartition)
    // All partitions must be Iceberg SparkInputPartition with file scan tasks; otherwise fallback.
    if (icebergPartitions.size != partitions.size) {
      return None
    }

    val rawTasks = icebergPartitions.flatMap(_.tasks)
    val fileTasks = rawTasks.collect { case task: FileScanTask => task }
    if (fileTasks.size != rawTasks.size) {
      return None
    }

    // Native scan does not apply delete files; only allow pure data files (COW).
    if (!fileTasks.forall(task => deletesEmpty(task.deletes()))) {
      return None
    }

    // Native scan handles a single file format; mixed formats must fallback.
    val formats = fileTasks.map(_.file().format()).distinct
    if (formats.size > 1) {
      return None
    }

    val format = formats.headOption.getOrElse(FileFormat.PARQUET)
    // ORC cannot match Iceberg columns by field-id yet, so any historical top-level
    // rename/drop may make older ORC files unsafe for native name/position matching.
    val supportedFormat =
      format == FileFormat.PARQUET ||
        (format == FileFormat.ORC && !renameOrDrop.topLevel)
    if (!supportedFormat) {
      return None
    }

    val pruningPredicates = collectPruningPredicates(scan.asInstanceOf[AnyRef], readSchema)
    val nativeTasks = fileTasks.map(task => toNativeScanTask(task, partitionSchema))
    Some(
      IcebergScanPlan(
        nativeTasks,
        format,
        readSchema,
        fileSchema,
        partitionSchema,
        pruningPredicates,
        fieldIdsByName))
  }

  private def planChangelogScan(exec: BatchScanExec, scan: Scan): Option[IcebergScanPlan] = {
    val readSchema = scan.readSchema
    val schemas = supportedSchemas(readSchema, isChangelogScan = true)
    if (schemas.isEmpty) {
      return None
    }
    val (fileSchema, partitionSchema) = schemas.get

    val (fieldIdsByName, renameOrDrop) =
      inspectFieldIdSupport(
        fileSchema,
        scan.asInstanceOf[AnyRef],
        AuronIcebergSourceUtil.expectedFieldIdsForChangelogScan,
        AuronIcebergSourceUtil.detectRenameOrDropForChangelogScan) match {
        case Some(fieldIdSupport) => fieldIdSupport
        case None => return None
      }

    val partitions = inputPartitions(exec)
    if (partitions.isEmpty) {
      return Some(
        IcebergScanPlan(
          Seq.empty,
          FileFormat.PARQUET,
          readSchema,
          fileSchema,
          partitionSchema,
          Seq.empty,
          fieldIdsByName))
    }

    val icebergPartitions = partitions.flatMap(icebergPartition)
    if (icebergPartitions.size != partitions.size) {
      return None
    }

    val rawTasks = icebergPartitions.flatMap(_.tasks)
    val changelogTasks = rawTasks.collect { case task: ChangelogScanTask => task }
    if (changelogTasks.size != rawTasks.size) {
      return None
    }

    val addedRowsTasks = changelogTasks.collect { case task: AddedRowsScanTask => task }
    // First native changelog support is insert-only. Delete and update images need Iceberg
    // delete-file handling, so keep them on Spark's reader for now.
    if (addedRowsTasks.size != changelogTasks.size) {
      return None
    }

    if (!addedRowsTasks.forall(_.operation() == ChangelogOperation.INSERT)) {
      return None
    }

    if (!addedRowsTasks.forall(task => deletesEmpty(task.deletes()))) {
      return None
    }

    val formats = addedRowsTasks.map(_.file().format()).distinct
    if (formats.size > 1) {
      return None
    }

    val format = formats.headOption.getOrElse(FileFormat.PARQUET)
    // ORC cannot match Iceberg columns by field-id yet, so any historical top-level
    // rename/drop may make older ORC files unsafe for native name/position matching.
    val supportedFormat =
      format == FileFormat.PARQUET ||
        (format == FileFormat.ORC && !renameOrDrop.topLevel)
    if (!supportedFormat) {
      return None
    }

    val pruningPredicates = collectPruningPredicates(scan.asInstanceOf[AnyRef], readSchema)
    val nativeTasks = addedRowsTasks.map(task => toNativeScanTask(task, partitionSchema))
    Some(
      IcebergScanPlan(
        nativeTasks,
        format,
        readSchema,
        fileSchema,
        partitionSchema,
        pruningPredicates,
        fieldIdsByName))
  }

  private def inspectFieldIdSupport(
      fileSchema: StructType,
      scan: AnyRef,
      expectedFieldIds: AnyRef => Map[String, Int],
      detectRenameOrDrop: AnyRef => AuronIcebergSourceUtil.RenameOrDrop)
      : Option[(Map[String, Int], AuronIcebergSourceUtil.RenameOrDrop)] = {
    val scanClassName = scan.getClass.getName
    val fieldIdsByName =
      try {
        expectedFieldIds(scan)
      } catch {
        case NonFatal(t) =>
          logWarning(s"Failed to inspect Iceberg field ids for $scanClassName.", t)
          return None
      }

    val renameOrDrop =
      try {
        detectRenameOrDrop(scan)
      } catch {
        case NonFatal(t) =>
          logWarning(s"Failed to inspect Iceberg schema history for $scanClassName.", t)
          return None
      }
    assert(!renameOrDrop.nested, "Nested Iceberg rename or drop is not supported.")

    val missingFieldIds =
      fileSchema.fields.filterNot(field => fieldIdsByName.contains(field.name)).map(_.name)
    assert(
      missingFieldIds.isEmpty,
      s"Missing Iceberg field ids for columns: ${missingFieldIds.mkString(", ")}")

    Some((fieldIdsByName, renameOrDrop))
  }

  private def supportedSchemas(
      readSchema: StructType,
      isChangelogScan: Boolean): Option[(StructType, StructType)] = {
    val unsupportedMetadataColumns =
      collectUnsupportedMetadataColumns(readSchema, isChangelogScan)
    // Supported metadata columns are materialized via per-file/per-task constant values rather
    // than read from the Iceberg data file payload. Metadata columns that require per-row
    // materialization (for example _pos) still fallback.
    if (unsupportedMetadataColumns.nonEmpty) {
      return None
    }

    val fileSchema =
      StructType(readSchema.fields.filterNot(isSupportedMetadataColumn(_, isChangelogScan)))
    val partitionSchema =
      StructType(readSchema.fields.filter(isSupportedMetadataColumn(_, isChangelogScan)))

    if (!fileSchema.fields.forall(field => NativeConverters.isTypeSupported(field.dataType))) {
      return None
    }

    if (!partitionSchema.fields.forall(field =>
        NativeConverters.isTypeSupported(field.dataType))) {
      return None
    }

    Some(fileSchema -> partitionSchema)
  }

  private def collectUnsupportedMetadataColumns(
      schema: StructType,
      isChangelogScan: Boolean): Seq[String] =
    schema.fields.collect {
      case field
          if isIcebergMetadataColumn(field.name, isChangelogScan) &&
            !isSupportedMetadataColumn(field, isChangelogScan) =>
        field.name
    }

  private def isIcebergMetadataColumn(name: String, isChangelogScan: Boolean): Boolean =
    MetadataColumns.isMetadataColumn(name) ||
      (isChangelogScan && ChangelogMetadataColumnNames.contains(name))

  private def isSupportedMetadataColumn(
      field: org.apache.spark.sql.types.StructField,
      isChangelogScan: Boolean): Boolean =
    field.name == MetadataColumns.FILE_PATH.name() ||
      field.name == MetadataColumns.SPEC_ID.name() ||
      (isChangelogScan && ChangelogMetadataColumnNames.contains(field.name))

  private def deletesEmpty(deletes: java.util.List[_]): Boolean =
    deletes == null || deletes.isEmpty

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

  private case class IcebergPartitionView(tasks: Seq[ScanTask])

  private def icebergPartition(partition: InputPartition): Option[IcebergPartitionView] = {
    val className = partition.getClass.getName
    // Only accept Iceberg SparkInputPartition to access task groups.
    if (!AuronIcebergSourceUtil.getClassOfSparkInputPartition().isInstance(partition)) {
      return None
    }

    try {
      // SparkInputPartition is package-private; use reflection to read its task group.
      val taskGroupField = partition.getClass.getDeclaredField("taskGroup")
      taskGroupField.setAccessible(true)
      val taskGroup = taskGroupField.get(partition)

      // Extract the Iceberg scan tasks. The caller decides which concrete task type is supported.
      val tasksMethod = taskGroup.getClass.getDeclaredMethod("tasks")
      tasksMethod.setAccessible(true)
      val tasks = tasksMethod.invoke(taskGroup).asInstanceOf[java.util.Collection[_]].asScala
      val icebergTasks = tasks.collect { case task: ScanTask => task }.toSeq

      if (icebergTasks.size != tasks.size) {
        return None
      }

      Some(IcebergPartitionView(icebergTasks))
    } catch {
      case NonFatal(t) =>
        logDebug(s"Failed to read Iceberg SparkInputPartition via reflection for $className.", t)
        None
    }
  }

  private def toNativeScanTask(
      task: FileScanTask,
      partitionSchema: StructType): IcebergNativeScanTask = {
    val file = task.file()
    IcebergNativeScanTask(
      file.location(),
      task.start(),
      task.length(),
      file.fileSizeInBytes(),
      metadataPartitionValues(file.location(), file.specId(), None, partitionSchema))
  }

  private def toNativeScanTask(
      task: AddedRowsScanTask,
      partitionSchema: StructType): IcebergNativeScanTask = {
    val file = task.file()
    IcebergNativeScanTask(
      file.location(),
      task.start(),
      task.length(),
      file.fileSizeInBytes(),
      metadataPartitionValues(file.location(), file.specId(), Some(task), partitionSchema))
  }

  private def metadataPartitionValues(
      filePath: String,
      specId: Int,
      changelogTask: Option[ChangelogScanTask],
      partitionSchema: StructType): Seq[Any] = {
    def requiredChangelogTask(columnName: String): ChangelogScanTask =
      changelogTask.getOrElse {
        throw new IllegalStateException(
          s"Iceberg changelog metadata column requires a changelog scan task: $columnName")
      }

    partitionSchema.fields.map { field =>
      field.name match {
        case name if name == MetadataColumns.FILE_PATH.name() =>
          filePath
        case name if name == MetadataColumns.SPEC_ID.name() =>
          specId
        case name if name == MetadataColumns.CHANGE_TYPE.name() =>
          requiredChangelogTask(name).operation().name()
        case name if name == MetadataColumns.CHANGE_ORDINAL.name() =>
          requiredChangelogTask(name).changeOrdinal()
        case name if name == MetadataColumns.COMMIT_SNAPSHOT_ID.name() =>
          requiredChangelogTask(name).commitSnapshotId()
        case name =>
          throw new IllegalStateException(
            s"unsupported Iceberg metadata column in native scan: $name")
      }
    }
  }

  private def collectPruningPredicates(
      scan: AnyRef,
      readSchema: StructType): Seq[pb.PhysicalExprNode] = {
    scanFilterExpressions(scan).flatMap { expr =>
      convertIcebergFilterExpression(expr, readSchema) match {
        case Some(converted) =>
          Some(NativeConverters.convertScanPruningExpr(converted))
        case None =>
          logDebug(s"Skip unsupported Iceberg pruning expression: $expr")
          None
      }
    }
  }

  private def scanFilterExpressions(scan: AnyRef): Seq[IcebergExpression] = {
    invokeDeclaredMethod(scan, "filterExpressions") match {
      case Some(values: java.util.Collection[_]) =>
        values.asScala.collect { case expr: IcebergExpression => expr }.toSeq
      case Some(values: Seq[_]) =>
        values.collect { case expr: IcebergExpression => expr }
      case _ =>
        Seq.empty
    }
  }

  private def invokeDeclaredMethod(target: AnyRef, methodName: String): Option[Any] = {
    try {
      var cls: Class[_] = target.getClass
      while (cls != null) {
        cls.getDeclaredMethods.find(_.getName == methodName) match {
          case Some(method) =>
            method.setAccessible(true)
            return Some(method.invoke(target))
          case None =>
            cls = cls.getSuperclass
        }
      }
      None
    } catch {
      case NonFatal(t) =>
        logDebug(s"Failed to invoke $methodName on ${target.getClass.getName}.", t)
        None
    }
  }

  private def convertIcebergFilterExpression(
      expr: IcebergExpression,
      readSchema: StructType): Option[SparkExpression] = {
    expr match {
      case and: IcebergAnd =>
        for {
          left <- convertIcebergFilterExpression(and.left(), readSchema)
          right <- convertIcebergFilterExpression(and.right(), readSchema)
        } yield SparkAnd(left, right)
      case or: IcebergOr =>
        for {
          left <- convertIcebergFilterExpression(or.left(), readSchema)
          right <- convertIcebergFilterExpression(or.right(), readSchema)
        } yield SparkOr(left, right)
      case not: IcebergNot =>
        convertIcebergFilterExpression(not.child(), readSchema).map(SparkNot)
      case predicate: UnboundPredicate[_] =>
        convertUnboundPredicate(predicate, readSchema)
      case predicate: BoundPredicate[_] =>
        convertBoundPredicate(predicate, readSchema)
      case _ =>
        expr.op() match {
          case org.apache.iceberg.expressions.Expression.Operation.TRUE =>
            Some(Literal(true))
          case org.apache.iceberg.expressions.Expression.Operation.FALSE =>
            Some(Literal(false))
          case _ =>
            None
        }
    }
  }

  private def convertUnboundPredicate(
      predicate: UnboundPredicate[_],
      readSchema: StructType): Option[SparkExpression] = {
    findField(predicate.ref().name(), readSchema).flatMap { field =>
      val attr = toAttribute(field)
      val op = predicate.op()

      op match {
        case org.apache.iceberg.expressions.Expression.Operation.IS_NULL =>
          Some(IsNull(attr))
        case org.apache.iceberg.expressions.Expression.Operation.NOT_NULL =>
          Some(IsNotNull(attr))
        case org.apache.iceberg.expressions.Expression.Operation.IS_NAN =>
          Some(IsNaN(attr))
        case org.apache.iceberg.expressions.Expression.Operation.NOT_NAN =>
          Some(SparkNot(IsNaN(attr)))
        case org.apache.iceberg.expressions.Expression.Operation.IN =>
          convertInPredicate(
            attr,
            field.dataType,
            predicate.literals().asScala.map(_.value()).toSeq)
        case org.apache.iceberg.expressions.Expression.Operation.NOT_IN =>
          convertInPredicate(
            attr,
            field.dataType,
            predicate.literals().asScala.map(_.value()).toSeq).map(SparkNot)
        case _ =>
          convertBinaryPredicate(attr, field.dataType, op, predicate.literal().value())
      }
    }
  }

  private def convertBoundPredicate(
      predicate: BoundPredicate[_],
      readSchema: StructType): Option[SparkExpression] = {
    findField(predicate.ref().name(), readSchema).flatMap { field =>
      val attr = toAttribute(field)
      val op = predicate.op()

      if (predicate.isUnaryPredicate()) {
        op match {
          case org.apache.iceberg.expressions.Expression.Operation.IS_NULL =>
            Some(IsNull(attr))
          case org.apache.iceberg.expressions.Expression.Operation.NOT_NULL =>
            Some(IsNotNull(attr))
          case org.apache.iceberg.expressions.Expression.Operation.IS_NAN =>
            Some(IsNaN(attr))
          case org.apache.iceberg.expressions.Expression.Operation.NOT_NAN =>
            Some(SparkNot(IsNaN(attr)))
          case _ =>
            None
        }
      } else if (predicate.isLiteralPredicate()) {
        val literalValue = predicate.asLiteralPredicate().literal().value()
        op match {
          case _ =>
            convertBinaryPredicate(attr, field.dataType, op, literalValue)
        }
      } else if (predicate.isSetPredicate()) {
        val values = predicate.asSetPredicate().literalSet().asScala.toSeq
        op match {
          case org.apache.iceberg.expressions.Expression.Operation.IN =>
            convertInPredicate(attr, field.dataType, values)
          case org.apache.iceberg.expressions.Expression.Operation.NOT_IN =>
            convertInPredicate(attr, field.dataType, values).map(SparkNot)
          case _ =>
            None
        }
      } else {
        None
      }
    }
  }

  private def convertBinaryPredicate(
      attr: AttributeReference,
      dataType: DataType,
      op: org.apache.iceberg.expressions.Expression.Operation,
      literalValue: Any): Option[SparkExpression] = {
    if (!supportsScanPruningLiteralType(dataType)) {
      return None
    }
    toLiteral(literalValue, dataType).flatMap { literal =>
      op match {
        case org.apache.iceberg.expressions.Expression.Operation.EQ =>
          Some(EqualTo(attr, literal))
        case org.apache.iceberg.expressions.Expression.Operation.NOT_EQ =>
          Some(SparkNot(EqualTo(attr, literal)))
        case org.apache.iceberg.expressions.Expression.Operation.LT =>
          Some(LessThan(attr, literal))
        case org.apache.iceberg.expressions.Expression.Operation.LT_EQ =>
          Some(LessThanOrEqual(attr, literal))
        case org.apache.iceberg.expressions.Expression.Operation.GT =>
          Some(GreaterThan(attr, literal))
        case org.apache.iceberg.expressions.Expression.Operation.GT_EQ =>
          Some(GreaterThanOrEqual(attr, literal))
        case _ =>
          None
      }
    }
  }

  private def convertInPredicate(
      attr: AttributeReference,
      dataType: DataType,
      values: Seq[Any]): Option[SparkExpression] = {
    if (!supportsScanPruningLiteralType(dataType)) {
      return None
    }
    val literals = values.map(toLiteral(_, dataType))
    if (literals.forall(_.nonEmpty)) {
      Some(In(attr, literals.flatten))
    } else {
      None
    }
  }

  private def supportsScanPruningLiteralType(dataType: DataType): Boolean = {
    dataType match {
      case StringType | BinaryType => false
      case _: DecimalType => false
      case _ => true
    }
  }

  private def toLiteral(value: Any, dataType: DataType): Option[Literal] = {
    if (value == null) {
      return Some(Literal.create(null, dataType))
    }
    dataType match {
      case _: DecimalType =>
        None
      case BinaryType =>
        value match {
          case bytes: Array[Byte] =>
            Some(Literal(bytes, BinaryType))
          case byteBuffer: java.nio.ByteBuffer =>
            val duplicated = byteBuffer.duplicate()
            val bytes = new Array[Byte](duplicated.remaining())
            duplicated.get(bytes)
            Some(Literal(bytes, BinaryType))
          case _ =>
            None
        }
      case StringType =>
        Some(Literal.create(value.toString, StringType))
      case _ =>
        Some(Literal.create(value, dataType))
    }
  }

  private def toAttribute(field: StructField): AttributeReference =
    AttributeReference(field.name, field.dataType, nullable = true)()

  private def findField(name: String, readSchema: StructType): Option[StructField] = {
    val resolver = SQLConf.get.resolver
    readSchema.fields.find(field => resolver(field.name, name))
  }
}
