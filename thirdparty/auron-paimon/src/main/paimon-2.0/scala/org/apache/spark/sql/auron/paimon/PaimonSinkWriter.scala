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

import java.io.ByteArrayInputStream
import java.util.{Collections, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.arrow.vector.ipc.ArrowStreamReader
import org.apache.paimon.data.BinaryRow
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.format.{FileFormat, SimpleStatsExtractor}
import org.apache.paimon.fs.{FileIO, Path}
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataFilePathFactory, DataIncrement}
import org.apache.paimon.manifest.FileSource
import org.apache.paimon.spark.{SparkInternalRowWrapper, SparkTypeUtils}
import org.apache.paimon.stats.SimpleStatsConverter
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.StatsCollectorFactories
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.auron.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.auron.columnar.ColumnarHelper
import org.apache.spark.sql.execution.auron.plan.PaimonSinkTaskHandler

/**
 * Task side of the native Paimon sink. Hands out data file paths and turns the files written by
 * the native sink into Paimon commit messages.
 *
 * The native sink passes the partition values of every file as a one row Arrow IPC stream of the
 * partition columns. They are not derived from the footer statistics, which parquet truncates (64
 * bytes for strings by default).
 */
class PaimonSinkWriter(table: FileStoreTable) extends PaimonSinkTaskHandler with Logging {

  private val options = table.coreOptions()
  private val rowType: RowType = table.rowType()
  private val fileIO: FileIO = table.fileIO()
  private val schemaId: Long = table.schema().id()
  private val partitionKeys: Seq[String] = table.partitionKeys().asScala.toSeq
  private val partitionType: RowType = table.schema().logicalPartitionType()
  private val partitionIndexes: Array[Int] =
    partitionKeys.map(k => rowType.getFieldNames.indexOf(k)).toArray
  private val sparkPartitionType = SparkTypeUtils.fromPaimonRowType(partitionType)

  // files are first written into a task scoped staging directory and moved into their
  // partition/bucket directory once the partition is known
  private val stagingDir: Path =
    new Path(table.location(), s".auron-staging/${UUID.randomUUID().toString}")
  private val stagingFileFactory: DataFilePathFactory =
    table.store().pathFactory().createDataFilePathFactory(stagingDir, null)

  private val statsExtractor: SimpleStatsExtractor = {
    val factories = StatsCollectorFactories.createStatsFactories(
      options.statsMode(),
      options,
      rowType.getFieldNames)
    FileFormat
      .fromIdentifier(options.fileFormatString(), options.toConfiguration)
      .createStatsExtractor(rowType, factories)
      .orElseThrow(() => new IllegalStateException("parquet stats extractor is not available"))
  }
  private val statsConverter = new SimpleStatsConverter(rowType, options.statsDenseStore())
  private val partitionSerializer = new InternalRowSerializer(partitionType)

  private val newFiles = mutable.LinkedHashMap[BinaryRow, mutable.ArrayBuffer[DataFileMeta]]()
  // every file moved out of the staging directory, deleted when the task is aborted
  private val movedFiles = mutable.ArrayBuffer[Path]()
  // sequence numbers are only meaningful per bucket for unaware-bucket append tables and are
  // assigned at commit time for row tracking; keep them monotonic within the task like Paimon
  private var nextSequenceNumber = 0L

  override def nextOutputPath(): String = stagingFileFactory.newPath().toString

  override def completeFile(
      path: String,
      numRows: Long,
      numBytes: Long,
      partitionValues: Array[Byte]): Unit = {
    val stagingPath = new Path(path)
    if (numRows == 0) {
      fileIO.deleteQuietly(stagingPath)
      return
    }
    // validate the file while it is still in the staging directory, so that a file failing
    // validation is removed together with the staging directory
    val fileInfo = statsExtractor.extractWithFileInfo(fileIO, stagingPath, numBytes)
    val rowCount = fileInfo.getRight.getRowCount
    if (rowCount != numRows) {
      throw new IllegalStateException(
        s"row count mismatch for $stagingPath: native sink reported $numRows, footer $rowCount")
    }
    val statsPair = statsConverter.toBinary(fileInfo.getLeft)
    val partition = decodePartition(partitionValues)

    val bucketPath = table.store().pathFactory().bucketPath(partition, 0)
    val finalPath = new Path(bucketPath, stagingPath.getName)
    fileIO.mkdirs(bucketPath)
    if (!fileIO.rename(stagingPath, finalPath)) {
      throw new IllegalStateException(s"failed to move $stagingPath to $finalPath")
    }
    movedFiles += finalPath

    val minSeq = nextSequenceNumber
    nextSequenceNumber += rowCount
    val meta = DataFileMeta.forAppend(
      finalPath.getName,
      numBytes,
      rowCount,
      statsPair.getRight,
      minSeq,
      nextSequenceNumber - 1,
      schemaId,
      Collections.emptyList[String](),
      null,
      FileSource.APPEND,
      statsPair.getLeft,
      null,
      null,
      null)
    newFiles.getOrElseUpdate(partition, mutable.ArrayBuffer()) += meta
  }

  /** Builds one commit message per partition, bucket 0 (unaware bucket). */
  def commitMessages(): Seq[CommitMessage] = {
    // same as Paimon's writer: totalBuckets is the configured bucket number (-1)
    val totalBuckets = Integer.valueOf(options.bucket())
    newFiles.map { case (partition, files) =>
      new CommitMessageImpl(
        partition,
        0,
        totalBuckets,
        new DataIncrement(
          files.asJava,
          Collections.emptyList[DataFileMeta](),
          Collections.emptyList[DataFileMeta]()),
        CompactIncrement.emptyIncrement()): CommitMessage
    }.toSeq
  }

  /** Deletes staged and moved files of this task, used when the task fails. */
  def abort(): Unit = {
    movedFiles.foreach(fileIO.deleteQuietly)
    movedFiles.clear()
    newFiles.clear()
    cleanupStaging()
  }

  def cleanupStaging(): Unit = {
    try {
      fileIO.delete(stagingDir, true)
    } catch {
      case e: Exception => logWarning(s"failed to delete staging dir $stagingDir", e)
    }
  }

  private def decodePartition(partitionValues: Array[Byte]): BinaryRow = {
    if (partitionIndexes.isEmpty) {
      return BinaryRow.EMPTY_ROW
    }
    val allocator = ArrowUtils.CHILD_ALLOCATOR("PaimonSinkWriter.decodePartition")
    try {
      val reader = new ArrowStreamReader(new ByteArrayInputStream(partitionValues), allocator)
      try {
        val root = reader.getVectorSchemaRoot
        if (!reader.loadNextBatch() || root.getRowCount != 1) {
          throw new IllegalStateException("expected one row of partition values")
        }
        val sparkRow = ColumnarHelper.rootRowReusable(root)
        val paimonRow =
          new SparkInternalRowWrapper(sparkPartitionType, partitionIndexes.length)
            .replace(sparkRow)
        // copies the values out of the arrow buffers before they are released
        partitionSerializer.toBinaryRow(paimonRow).copy()
      } finally {
        reader.close()
      }
    } finally {
      allocator.close()
    }
  }
}
