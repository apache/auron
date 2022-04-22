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

package org.apache.spark.sql.blaze

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.Dependency
import org.apache.spark.Partition
import org.apache.spark.SparkContext
import org.apache.spark.TaskContext
import org.blaze.protobuf.PhysicalPlanNode

class NativeRDD(
    @transient private val rddSparkContext: SparkContext,
    val metrics: MetricNode,
    private val rddPartitions: Array[Partition],
    private val rddDependencies: Seq[Dependency[_]],
    val nativePlan: (Partition, TaskContext) => PhysicalPlanNode)
    extends RDD[InternalRow](rddSparkContext, rddDependencies)
    with Logging {

  override protected def getPartitions: Array[Partition] = rddPartitions
  override protected def getDependencies: Seq[Dependency[_]] = rddDependencies

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val computingNativePlan = nativePlan(split, context)
    NativeSupports.executeNativePlan(computingNativePlan, metrics, context)
  }
}

object NativeRDD {
  def getNativeShuffleId(context: TaskContext, shuffleId: Int): String =
    s"nativeShuffleId:${context.stageId}:${context.stageAttemptNumber}:${context.partitionId}:${context.taskAttemptId}:${shuffleId}"
}
