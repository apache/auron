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

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

import org.apache.iceberg._
import org.apache.spark.sql.connector.read.{InputPartition, Scan}
import org.apache.spark.sql.types.StructType

object IcebergSourceUtil {

  def isIcebergScan(scan: Scan): Boolean = {
    scan match {
      case _: org.apache.iceberg.spark.source.SparkBatchQueryScan => true
      case _ => false
    }
  }

  def getScanAsSparkBatchQueryScan(scan: Scan): SparkBatchQueryScan = {
    scan match {
      case s: SparkBatchQueryScan => s
      case _ => throw new IllegalArgumentException("Scan is not a SparkBatchQueryScan")
    }
  }

  def getTableFromScan(scan: Scan): Table = {
    getScanAsSparkBatchQueryScan(scan).table()
  }

  def getInputPartitionAsSparkInputPartition(
      inputPartition: InputPartition): SparkInputPartition = {
    inputPartition match {
      case s: SparkInputPartition => s
      case _ => throw new IllegalArgumentException("InputPartition is not a SparkInputPartition")
    }
  }

  def getFileScanTasks(tasks: List[ScanTask]): List[FileScanTask] = tasks match {
    case t if t.forall(_.isFileScanTask) =>
      t.map(_.asFileScanTask())
    case t if t.forall(_.isInstanceOf[CombinedScanTask]) =>
      t.iterator.flatMap(_.asCombinedScanTask().tasks().asScala).toList
    case _ =>
      throw new UnsupportedOperationException("Unsupported iceberg scan task type")
  }

  def getReadSchema(scan: Scan): StructType = {
    getScanAsSparkBatchQueryScan(scan).readSchema
  }

  def planInputPartitions(scan: Scan): Array[InputPartition] = {
    getScanAsSparkBatchQueryScan(scan).toBatch.planInputPartitions()
  }

  def getFileScanTasksFromInputPartition(inputPartition: InputPartition): Seq[FileScanTask] = {
    val sip = getInputPartitionAsSparkInputPartition(inputPartition)
    val tasks = sip.taskGroup[ScanTask]().tasks().asScala
    getFileScanTasks(tasks.toList)
  }
}
