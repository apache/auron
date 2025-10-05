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

import org.apache.spark.sql.Row
import org.apache.spark.sql.auron.Shims
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

import org.apache.auron.sparkver

case class NativeParquetInsertIntoHiveTableExec(
    cmd: InsertIntoHiveTable,
    override val child: SparkPlan)
    extends NativeParquetInsertIntoHiveTableBase(cmd, child) {

  @sparkver("4.0")
  override protected def getInsertIntoHiveTableCommand(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      metrics: Map[String, SQLMetric]): InsertIntoHiveTable = {
    new AuronInsertIntoHiveTable40(
      table,
      partition,
      query,
      overwrite,
      ifPartitionNotExists,
      outputColumnNames,
      metrics)
  }

  @sparkver("4.0")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  @sparkver("4.0")
  class AuronInsertIntoHiveTable40(
      table: CatalogTable,
      partition: Map[String, Option[String]],
      query: LogicalPlan,
      overwrite: Boolean,
      ifPartitionNotExists: Boolean,
      outputColumnNames: Seq[String],
      outerMetrics: Map[String, SQLMetric])
      extends {
        private val insertIntoHiveTable = InsertIntoHiveTable(
          table,
          partition,
          query,
          overwrite,
          ifPartitionNotExists,
          outputColumnNames)
        private val initPartitionColumns = insertIntoHiveTable.partitionColumns
        private val initBucketSpec = insertIntoHiveTable.bucketSpec
        private val initOptions = insertIntoHiveTable.options
        private val initFileFormat = insertIntoHiveTable.fileFormat
        private val initHiveTmpPath = insertIntoHiveTable.hiveTmpPath

      }
      with InsertIntoHiveTable(
        table,
        partition,
        query,
        overwrite,
        ifPartitionNotExists,
        outputColumnNames,
        initPartitionColumns,
        initBucketSpec,
        initOptions,
        initFileFormat,
        initHiveTmpPath) {

    override lazy val metrics: Map[String, SQLMetric] = outerMetrics

    override def run(sparkSession: SparkSession, child: SparkPlan): Seq[Row] = {
      val nativeParquetSink =
        Shims.get.createNativeParquetSinkExec(sparkSession, table, partition, child, metrics)
      super.run(sparkSession, nativeParquetSink)
    }
  }
}
