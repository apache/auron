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
package org.apache.iceberg.spark.source

import java.util.Locale

import scala.jdk.CollectionConverters.{asScalaBufferConverter, collectionAsScalaIterableConverter}

import org.apache.iceberg.{CombinedScanTask, FileFormat, FileScanTask, ScanTask}
import org.apache.spark.sql.connector.read.Scan

object IcebergResolveFileFormatUtil {
  private val PARQUET_STRING = "parquet"
  private val ORC_STRING = "orc"
  private val FORMAT_STRING = "format"
  private val WRITE_FORMAT_DEFAULT = "write.format.default"

  def resolveFileFormat(scan: Scan): FileFormat = scan match {
    case scan: SparkBatchQueryScan =>
      val tasks = scan.tasks().asScala.toList
      // get format info from tasks
      if (tasks.nonEmpty) {
        val formats = asFileScanTask(tasks).map(_.file().format()).distinct
        formats match {
          case Seq(FileFormat.PARQUET) =>
            FileFormat.PARQUET
          case Seq(FileFormat.ORC) =>
            FileFormat.ORC
          case Seq(other) =>
            throw new UnsupportedOperationException(s"Unsupported Iceberg file format: $other")
          case multi =>
            throw new IllegalStateException(s"Mixed file formats are not supported: $multi")
        }
      } else {
        // if tasks is empty, get format info from table properties
        val props = scan.table().properties()
        val formatStr =
          Option(props.get(WRITE_FORMAT_DEFAULT))
            .orElse(Option(props.get(FORMAT_STRING)))
            .getOrElse(PARQUET_STRING)

        formatStr.toLowerCase(Locale.ROOT) match {
          case PARQUET_STRING => FileFormat.PARQUET
          case ORC_STRING => FileFormat.ORC
          case other =>
            throw new UnsupportedOperationException(s"Unsupported Iceberg file format: $other")
        }
      }
    case _ =>
      throw new UnsupportedOperationException("Only support iceberg SparkBatchQueryScan.")
  }

  private def asFileScanTask(tasks: List[ScanTask]): List[FileScanTask] = {
    if (tasks.forall(_.isFileScanTask)) {
      tasks.map(_.asFileScanTask())
    } else if (tasks.forall(_.isInstanceOf[CombinedScanTask])) {
      tasks.flatMap(_.asCombinedScanTask().tasks().asScala)
    } else {
      throw new UnsupportedOperationException(
        "Only support iceberg CombinedScanTask and FileScanTask.")
    }
  }
}
