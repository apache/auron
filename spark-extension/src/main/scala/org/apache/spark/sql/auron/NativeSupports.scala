/*
 * Copyright 2022 The Auron Authors
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
package org.apache.spark.sql.auron

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan

trait NativeSupports extends SparkPlan {
  protected def doExecuteNative(): NativeRDD

  protected override def doExecute(): RDD[InternalRow] = doExecuteNative()

  def executeNative(): NativeRDD = executeQuery {
    doExecuteNative()
  }

  def shuffleReadFull: Boolean = Shims.get.getRDDShuffleReadFull(this.doExecuteNative())
}
