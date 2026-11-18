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
package org.apache.auron

import org.apache.spark.TaskContext

import org.apache.auron.metric.SparkMetricNode

class SparkMetricNodeSuite extends BaseAuronSQLSuite {

  test("native disk spill size is added to task disk bytes spilled") {
    val (diskBytesSpilled, memoryBytesSpilled) = spark.sparkContext
      .parallelize(Seq(1), 1)
      .map { _ =>
        val child = SparkMetricNode(Map.empty, Nil)
        val node = SparkMetricNode(Map.empty, Seq(child))
        node.add("disk_spill_size", 100)
        child.add("disk_spill_size", 20)
        node.add("disk_spill_size", 0)
        node.add("disk_spill_size", -1)
        node.add("mem_spill_size", 50)
        val taskMetrics = TaskContext.get.taskMetrics()
        (taskMetrics.diskBytesSpilled, taskMetrics.memoryBytesSpilled)
      }
      .collect()
      .head
    assert(diskBytesSpilled == 120)
    assert(memoryBytesSpilled == 0)
  }

  test("native disk spill size is ignored outside a task") {
    SparkMetricNode(Map.empty, Nil).add("disk_spill_size", 100)
  }
}
