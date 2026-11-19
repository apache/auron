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

import java.util.Locale

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.paimon.CoreOptions
import org.apache.paimon.table.{BucketMode, FileStoreTable}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.v2.{AppendDataExec, OverwriteByExpressionExec, OverwritePartitionsDynamicExec, V2ExistingTableWriteExec}
import org.apache.spark.sql.hive.auron.paimon.PaimonUtil
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}

/**
 * Decides whether a DataSourceV2 Paimon write can be executed by the native Paimon sink.
 *
 * Only plain append / overwrite of bucket-unaware append-only parquet tables is supported. Every
 * table feature that changes what a data file must contain or how it is committed (primary keys,
 * fixed or dynamic buckets, deletion vectors, row tracking, data evolution, clustering, file
 * indexes, external paths, blob/vector fields, copy-on-write row level operations) falls back to
 * the Paimon writer.
 */
object PaimonWriteSupport extends Logging {

  private val PaimonV2WriteClassName = "org.apache.paimon.spark.write.PaimonV2Write"

  final case class PaimonWritePlan(
      table: FileStoreTable,
      write: Write,
      overwritePartitions: Option[Map[String, String]])

  def isPaimonWrite(exec: SparkPlan): Boolean = exec match {
    case e: AppendDataExec => isPaimonV2Write(e.write)
    case e: OverwriteByExpressionExec => isPaimonV2Write(e.write)
    case e: OverwritePartitionsDynamicExec => isPaimonV2Write(e.write)
    case _ => false
  }

  def plan(exec: V2ExistingTableWriteExec): Either[String, PaimonWritePlan] = {
    val write = exec.write
    if (!isPaimonV2Write(write)) {
      return Left(s"not a Paimon V2 write: ${write.getClass.getName}")
    }
    val table = readField[FileStoreTable](write, "originTable")
      .getOrElse(return Left("cannot resolve FileStoreTable from PaimonV2Write"))
    if (readField[Option[_]](write, "copyOnWriteScan").exists(_.isDefined)) {
      return Left("copy-on-write row level operations are not supported")
    }
    if (readField[Option[_]](write, "operationType").exists(_.isDefined)) {
      return Left("row level operations are not supported")
    }
    val overwritePartitions = readField[Option[Map[String, String]]](write, "overwritePartitions")
      .getOrElse(return Left("cannot resolve overwrite partitions from PaimonV2Write"))
    val writeOptions = readField[org.apache.paimon.options.Options](write, "options")
    val mergeSchema = writeOptions.exists(_.getBoolean("write.merge-schema", false)) ||
      SQLConf.get.getConfString("spark.paimon.write.merge-schema", "false").toBoolean
    if (mergeSchema) {
      return Left("write.merge-schema is not supported")
    }
    checkTable(table, exec.query.schema).map(Left(_)).getOrElse {
      Right(PaimonWritePlan(table, write, overwritePartitions))
    }
  }

  private def checkTable(table: FileStoreTable, querySchema: StructType): Option[String] = {
    val options = table.coreOptions()
    val tableOptions = table.options().asScala
    def opt(key: String): Option[String] =
      tableOptions.get(key).orElse(tableOptions.get(key.toLowerCase(Locale.ROOT)))

    if (!table.primaryKeys().isEmpty) return Some("primary-key tables are not supported")
    if (table.bucketMode() != BucketMode.BUCKET_UNAWARE) {
      return Some(s"bucket mode ${table.bucketMode()} is not supported, only bucket = -1")
    }
    val format = PaimonUtil.paimonFileFormat(table).toLowerCase(Locale.ROOT)
    if (format != PaimonUtil.parquetFormat) return Some(s"file format $format is not supported")
    if (options.deletionVectorsEnabled()) return Some("deletion vectors are not supported")
    if (options.writeSequenceNumberInitMode() == CoreOptions.SequenceNumberInitMode.SNAPSHOT) {
      return Some("write.sequence-number-init-mode = snapshot is not supported")
    }
    if (options.rowTrackingEnabled()) return Some("row tracking is not supported")
    if (options.dataEvolutionEnabled()) return Some("data evolution is not supported")
    if (!options.clusteringColumns().isEmpty) return Some("clustering is not supported")
    if (!options.indexColumnsOptions().isEmpty) return Some("file index is not supported")
    if (Option(options.dataFileExternalPaths()).exists(_.trim.nonEmpty)) {
      return Some("data file external paths are not supported")
    }
    if (options.fileSuffixIncludeCompression()) {
      return Some("file.suffix.include.compression is not supported")
    }
    if (opt(CoreOptions.FILE_FORMAT_PER_LEVEL.key()).exists(_.trim.nonEmpty)) {
      return Some("file.format.per.level is not supported")
    }
    if (opt(CoreOptions.FILE_COMPRESSION_PER_LEVEL.key()).exists(_.trim.nonEmpty)) {
      return Some("file.compression.per.level is not supported")
    }
    if (!supportedCompressions.contains(options.fileCompression().toLowerCase(Locale.ROOT))) {
      return Some(s"file compression ${options.fileCompression()} is not supported")
    }

    val tableFields = table.rowType().getFields.asScala
    if (tableFields.exists(f => !isSupportedPaimonType(f.`type`().getTypeRoot.name()))) {
      return Some("unsupported column type in table schema")
    }
    // Paimon applies column DEFAULT values in its JVM writer (TableWriteImpl wraps every row in a
    // DefaultValueRow that replaces NULL fields), not in the query plan; the native sink would
    // write NULL instead
    tableFields.find(f => f.defaultValue() != null).foreach { f =>
      return Some(s"column ${f.name()} has a default value")
    }
    if (querySchema.length != tableFields.length) {
      return Some(
        s"query output has ${querySchema.length} columns, table has ${tableFields.length}")
    }
    if (!querySchema.fields.forall(f => isSupportedSparkType(f.dataType))) {
      return Some("unsupported column type in query output")
    }
    None
  }

  private val supportedCompressions = Set("snappy", "zstd", "gzip", "lz4", "none", "uncompressed")

  // types whose parquet encoding is identical between Paimon and the native writer
  private val supportedTypeRoots = Set(
    "CHAR",
    "VARCHAR",
    "BOOLEAN",
    "BINARY",
    "VARBINARY",
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "DECIMAL")

  private def isSupportedPaimonType(typeRoot: String): Boolean = supportedTypeRoots(typeRoot)

  private def isSupportedSparkType(dataType: DataType): Boolean = dataType match {
    case _: StructType => false
    case BinaryType => true
    case t => NativeConverters.isTypeSupported(t)
  }

  private def isPaimonV2Write(write: AnyRef): Boolean = {
    if (write == null) return false
    var cls: Class[_] = write.getClass
    while (cls != null) {
      if (cls.getName == PaimonV2WriteClassName) return true
      cls = cls.getSuperclass
    }
    false
  }

  private def readField[T](target: AnyRef, name: String): Option[T] = {
    try {
      Option(FieldUtils.readField(target, name, true)).map(_.asInstanceOf[T])
    } catch {
      case NonFatal(e) =>
        logDebug(s"cannot read field $name of ${target.getClass.getName}", e)
        None
    }
  }
}
