/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.blaze.plan

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark.RangeDependency
import org.apache.spark.rdd.UnionPartition
import org.apache.spark.sql.blaze.MetricNode
import org.apache.spark.sql.blaze.NativeHelper
import org.apache.spark.sql.blaze.NativeRDD
import org.apache.spark.sql.blaze.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.blaze.protobuf.PhysicalPlanNode
import org.blaze.protobuf.Schema
import org.blaze.protobuf.UnionExecNode

abstract class NativeUnionBase(
    override val children: Seq[SparkPlan],
    override val output: Seq[Attribute])
    extends SparkPlan
    with NativeSupports {

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows"))
      .toSeq: _*)

  override def doExecuteNative(): NativeRDD = {
    val rdds = children.map(c => NativeHelper.executeNative(c))
    val nativeMetrics = MetricNode(metrics, rdds.map(_.metrics))

    def unionedPartitions: Array[UnionPartition[InternalRow]] = {
      val array = new Array[UnionPartition[InternalRow]](rdds.map(_.partitions.length).sum)
      var pos = 0
      for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
        array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
        pos += 1
      }
      array
    }

    def dependencies: Seq[Dependency[_]] = {
      val deps = new ArrayBuffer[Dependency[_]]
      var pos = 0
      for (rdd <- rdds) {
        deps += new RangeDependency(rdd, 0, pos, rdd.partitions.length)
        pos += rdd.partitions.length
      }
      deps
    }

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      unionedPartitions.asInstanceOf[Array[Partition]],
      dependencies,
      rdds.forall(_.isShuffleReadFull),
      (partition, taskContext) => {
        val unionPartition = unionedPartitions(partition.index)
        val inputPlan = rdds(unionPartition.parentRddIndex)
          .nativePlan(unionPartition.parentPartition, taskContext)

        val union = UnionExecNode
          .newBuilder()
          .setNumPartitions(unionedPartitions.length)
          .setInPartition(unionPartition.parentPartition.index)
          .setNumChildren(rdds.length)
          .setCurrentChildIndex(unionPartition.parentRddIndex)
          .setSchema(nativeSchema)
          .setInput(inputPlan)
        PhysicalPlanNode.newBuilder().setUnion(union).build()
      },
      friendlyName = "NativeRDD.Union")
  }

  val nativeSchema: Schema = Util.getNativeSchema(output)
}
