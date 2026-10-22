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

import org.apache.spark.NarrowDependency
import org.apache.spark.rdd.CartesianPartition
import org.apache.spark.rdd.CartesianRDD
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.ExplainUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.auron.{protobuf => pb}
import org.apache.auron.metric.SparkMetricNode

abstract class NativeCartesianProductBase(
    override val left: SparkPlan,
    override val right: SparkPlan,
    condition: Option[Expression])
    extends BinaryExecNode
    with NativeSupports {

  override def output: Seq[Attribute] = left.output ++ right.output

  override def verboseStringWithOperatorId(): String = {
    val joinCondition = condition.map(_.toString).getOrElse("None")
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Left output", left.output)}
       |${ExplainUtils.generateFieldString("Right output", right.output)}
       |${ExplainUtils.generateFieldString("Join type", Inner.toString)}
       |${ExplainUtils.generateFieldString("Join condition", joinCondition)}
       |""".stripMargin
  }

  override lazy val metrics: Map[String, SQLMetric] = NativeHelper.getDefaultNativeMetrics(
    sparkContext,
    Set(
      "stage_id",
      "output_rows",
      "elapsed_compute",
      "mem_spill_count",
      "mem_spill_size",
      "mem_spill_iotime",
      "disk_spill_size",
      "disk_spill_iotime",
      "input_batch_count",
      "input_batch_mem_size",
      "input_row_count"))

  private val nativeSchema = Util.getNativeSchema(output)
  private val nativeJoinFilter =
    condition.map(NativeConverters.convertJoinFilter(_, left.output, right.output))

  override def doExecuteNative(): NativeRDD = {
    val leftRDD = NativeHelper.executeNative(left)
    val rightRDD = NativeHelper.executeNative(right)
    val cartesianRDD = new CartesianRDD[InternalRow, InternalRow](sparkContext, leftRDD, rightRDD)
    val partitions = cartesianRDD.partitions
    val numPartitions = partitions.length
    val numRightPartitions = rightRDD.partitions.length
    // CartesianRDD's anonymous dependencies capture that RDD. Keep only the parent
    // mapping to avoid a cycle through NativeRDD's serialized dependency list.
    val dependencies = Seq(
      new CartesianProductDependency(leftRDD, numRightPartitions, isLeft = true),
      new CartesianProductDependency(rightRDD, numRightPartitions, isLeft = false))
    val nativeMetrics = SparkMetricNode(metrics, leftRDD.metrics :: rightRDD.metrics :: Nil)
    val nativeSchema = this.nativeSchema
    val nativeJoinFilter = this.nativeJoinFilter

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = partitions,
      rddPartitioner = None,
      rddDependencies = dependencies,
      rddShuffleReadFull = false,
      (partition, taskContext) => {
        val pair = partition.asInstanceOf[CartesianPartition]
        val cartesian = pb.CartesianProductExecNode
          .newBuilder()
          .setSchema(nativeSchema)
          .setLeft(leftRDD.nativePlan(pair.s1, taskContext))
          .setRight(rightRDD.nativePlan(pair.s2, taskContext))
          .setLeftPartition(pair.s1.index)
          .setRightPartition(pair.s2.index)
          .setNumPartitions(numPartitions)
        nativeJoinFilter.foreach(cartesian.setFilter)
        pb.PhysicalPlanNode.newBuilder().setCartesianProduct(cartesian).build()
      },
      friendlyName = "NativeRDD.CartesianProduct")
  }
}

private class CartesianProductDependency(rdd: NativeRDD, numRightPartitions: Int, isLeft: Boolean)
    extends NarrowDependency(rdd) {
  override def getParents(partitionId: Int): Seq[Int] =
    Seq(if (isLeft) partitionId / numRightPartitions else partitionId % numRightPartitions)
}
