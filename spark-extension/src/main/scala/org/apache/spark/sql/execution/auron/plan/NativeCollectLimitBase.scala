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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.auron.{NativeHelper, NativeRDD, NativeSupports, Shims}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{SinglePartition, UnknownPartitioning}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.{LimitExecNode, PhysicalPlanNode}

abstract class NativeCollectLimitBase(limit: Int, override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {
  override def output: Seq[Attribute] = child.output

  override lazy val metrics: Map[String, SQLMetric] =
    (mutable.LinkedHashMap[String, SQLMetric]() ++
      Map(
        "dataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size"),
        "numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions"))).toMap

  override def executeCollect(): Array[InternalRow] = {
    val partial = Shims.get.createNativeLocalLimitExec(limit, child)
    val buf = new ArrayBuffer[InternalRow]

    // collect rows partition-by-partition up to 'limit', avoiding full-partition collect.
    val it = partial.execute().toLocalIterator
    while (buf.size < limit && it.hasNext) {
      val row = it.next().copy()
      buf += row
    }
    buf.toArray
  }

  override def doExecuteNative(): NativeRDD = {
    val partial = Shims.get.createNativeLocalLimitExec(limit, child)
    if (!partial.outputPartitioning.isInstanceOf[UnknownPartitioning]
      && partial.outputPartitioning.numPartitions <= 1) {
      return NativeHelper.executeNative(partial)
    }

    // merge all LocalLimit child partitions into a single partition
    val shuffled = Shims.get.createNativeShuffleExchangeExec(SinglePartition, partial)
    val singlePartitionRDD = NativeHelper.executeNative(shuffled)

    new NativeRDD(
      sparkContext,
      SparkMetricNode(metrics, singlePartitionRDD.metrics :: Nil),
      singlePartitionRDD.partitions,
      singlePartitionRDD.partitioner,
      new OneToOneDependency(singlePartitionRDD) :: Nil,
      rddShuffleReadFull = false,
      (partition, taskContext) => {
        val inputPartition = singlePartitionRDD.partitions(partition.index)
        val nativeLimitExec = LimitExecNode
          .newBuilder()
          .setInput(singlePartitionRDD.nativePlan(inputPartition, taskContext))
          .setLimit(limit)
          .build()
        PhysicalPlanNode.newBuilder().setLimit(nativeLimitExec).build()
      },
      friendlyName = "NativeRDD.CollectLimit")
  }

  override val nodeName: String = "NativeCollectLimit"
}
