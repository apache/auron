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
import java.util.UUID

import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.hive.auron.HiveClientHelper
import org.apache.spark.util.SerializableConfiguration

import org.apache.auron.jni.JniBridge
import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.OrcProp
import org.apache.auron.protobuf.OrcSinkExecNode
import org.apache.auron.protobuf.PhysicalPlanNode

abstract class NativeOrcSinkBase(
    sparkSession: SparkSession,
    table: CatalogTable,
    partition: Map[String, Option[String]],
    override val child: SparkPlan,
    override val metrics: Map[String, SQLMetric])
    extends UnaryExecNode
    with NativeSupports {

  private val dataSchema = NativeOrcInsertIntoHiveTableBase.dataSchema(table, partition)

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def doExecuteNative(): NativeRDD = {
    val hiveQlTable = HiveClientHelper.toHiveTable(table)
    val tableDesc = new TableDesc(
      hiveQlTable.getInputFormatClass,
      hiveQlTable.getOutputFormatClass,
      hiveQlTable.getMetadata)
    val hadoopConf = newHadoopConf(tableDesc)
    val job = Job.getInstance(hadoopConf)
    val orcFileFormat = new OrcFileFormat()
    orcFileFormat.prepareWrite(sparkSession, job, Map(), dataSchema)

    val serializableConf = new SerializableConfiguration(job.getConfiguration)
    val numDynParts = partition.count(_._2.isEmpty)

    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = SparkMetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeDependencies = new OneToOneDependency(inputRDD) :: Nil
    new NativeRDD(
      sparkSession.sparkContext,
      nativeMetrics,
      inputRDD.partitions,
      inputRDD.partitioner,
      nativeDependencies,
      inputRDD.isShuffleReadFull,
      (partition, context) => {

        OrcSinkTaskContext.get.isNative = true

        val resourceId = s"NativeOrcSinkExec:${UUID.randomUUID().toString}"
        JniBridge.putResource(
          resourceId,
          (location: String) => {
            NativeHelper.currentUser.doAs(new PrivilegedExceptionAction[FileSystem] {
              override def run(): FileSystem =
                FileSystem.get(new URI(location), serializableConf.value)
            })
          })

        val job = Job.getInstance(new JobConf(serializableConf.value))
        val nativeProps = job.getConfiguration.asScala
          .filter(_.getKey.startsWith("orc."))
          .map(entry =>
            OrcProp
              .newBuilder()
              .setKey(entry.getKey)
              .setValue(entry.getValue)
              .build())

        val inputPartition = inputRDD.partitions(partition.index)
        val orcSink = OrcSinkExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, context))
          .setFsResourceId(resourceId)
          .setNumDynParts(numDynParts)
          .setSchema(NativeConverters.convertSchema(dataSchema))
          .addAllProp(nativeProps.asJava)
        PhysicalPlanNode.newBuilder().setOrcSink(orcSink).build()
      },
      friendlyName = "NativeRDD.OrcSink")
  }

  @nowarn("cat=unused") // _tableDesc temporarily unused
  protected def newHadoopConf(_tableDesc: TableDesc): Configuration =
    sparkSession.sessionState.newHadoopConf()
}
