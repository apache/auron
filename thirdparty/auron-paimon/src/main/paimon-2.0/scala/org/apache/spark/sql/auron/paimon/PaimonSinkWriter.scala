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

import java.util.{Collections, UUID}

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.paimon.data.{BinaryRow, GenericRow}
import org.apache.paimon.data.serializer.InternalRowSerializer
import org.apache.paimon.format.{FileFormat, SimpleStatsExtractor}
import org.apache.paimon.fs.{FileIO, Path}
import org.apache.paimon.io.{CompactIncrement, DataFileMeta, DataFilePathFactory, DataIncrement}
import org.apache.paimon.manifest.FileSource
import org.apache.paimon.stats.SimpleStatsConverter
import org.apache.paimon.table.FileStoreTable
import org.apache.paimon.table.sink.{CommitMessage, CommitMessageImpl}
import org.apache.paimon.types.RowType
import org.apache.paimon.utils.StatsCollectorFactories
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.auron.plan.PaimonSinkTaskHandler

/**
 * Task side of the native Paimon sink. Hands out data file paths and turns the files written by
 * the native sink into Paimon commit messages.
 *
 * The partition of a file is not passed from native code: Paimon data files contain the partition
 * columns, so it is read back from the file statistics (min == max) together with the column
 * statistics Paimon stores in the manifest. This keeps the native/JVM interface free of any
 * Paimon specific encoding.
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
  // partition stats are always collected so the partition can be derived from the file
  private val partitionStatsExtractor: SimpleStatsExtractor = FileFormat
    .fromIdentifier(options.fileFormatString(), options.toConfiguration)
    .createStatsExtractor(
      rowType,
      StatsCollectorFactories.createStatsFactories("full", options, rowType.getFieldNames))
    .orElseThrow(() => new IllegalStateException("parquet stats extractor is not available"))

  private val partitionSerializer = new InternalRowSerializer(partitionType)

  private val newFiles = mutable.LinkedHashMap[BinaryRow, mutable.ArrayBuffer[DataFileMeta]]()
  // sequence numbers are only meaningful per bucket for unaware-bucket append tables and are
  // assigned at commit time for row tracking; keep them monotonic within the task like Paimon
  private var nextSequenceNumber = 0L

  override def nextOutputPath(): String = stagingFileFactory.newPath().toString

  override def completeFile(path: String, numRows: Long, numBytes: Long): Unit = {
    val stagingPath = new Path(path)
    if (numRows == 0) {
      fileIO.deleteQuietly(stagingPath)
      return
    }
    val partition = derivePartition(stagingPath, numBytes)
    val bucketPath = table.store().pathFactory().bucketPath(partition, 0)
    val finalPath = new Path(bucketPath, stagingPath.getName)
    fileIO.mkdirs(bucketPath)
    if (!fileIO.rename(stagingPath, finalPath)) {
      throw new IllegalStateException(s"failed to move $stagingPath to $finalPath")
    }

    val fileInfo = statsExtractor.extractWithFileInfo(fileIO, finalPath, numBytes)
    val statsPair = statsConverter.toBinary(fileInfo.getLeft)
    val rowCount = fileInfo.getRight.getRowCount
    if (rowCount != numRows) {
      throw new IllegalStateException(
        s"row count mismatch for $finalPath: native sink reported $numRows, footer $rowCount")
    }
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
    newFiles.foreach { case (partition, files) =>
      val bucketPath = table.store().pathFactory().bucketPath(partition, 0)
      files.foreach(f => fileIO.deleteQuietly(new Path(bucketPath, f.fileName())))
    }
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

  private def derivePartition(path: Path, numBytes: Long): BinaryRow = {
    if (partitionIndexes.isEmpty) {
      return BinaryRow.EMPTY_ROW
    }
    val stats = partitionStatsExtractor.extract(fileIO, path, numBytes)
    val values = partitionIndexes.map { idx =>
      val s = stats(idx)
      val nullCount = Option(s.nullCount()).map(_.longValue())
      val allNull = s.min() == null && s.max() == null && nullCount.exists(_ > 0)
      val single = s.min() != null && s.min() == s.max() && nullCount.contains(0L)
      if (!allNull && !single) {
        throw new IllegalStateException(
          s"data file $path does not contain exactly one value of partition column " +
            s"${rowType.getFieldNames.get(idx)}: $s")
      }
      if (allNull) null else s.min()
    }
    val row = new GenericRow(values.length)
    values.zipWithIndex.foreach { case (v, i) => row.setField(i, v) }
    partitionSerializer.toBinaryRow(row).copy()
  }
}
