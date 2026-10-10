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

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.auron.join.JoinBuildSides.{JoinBuildLeft, JoinBuildRight, JoinBuildSide}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, GreaterThan, SortOrder}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, FullOuter, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.joins.auron.plan.NativeShuffledHashJoinExecProvider
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class AuronShuffledHashJoinOrderingSuite extends SparkFunSuite {
  private val leftKey = AttributeReference("lk", IntegerType)()
  private val rightKey = AttributeReference("rk", IntegerType)()
  private case class OrderedInput(key: AttributeReference) extends LeafExecNode {
    override def output: Seq[AttributeReference] = Seq(key)
    override def outputOrdering: Seq[SortOrder] = Seq(SortOrder(key, Ascending))
    override protected def doExecute(): RDD[InternalRow] =
      throw new UnsupportedOperationException("ordering test does not execute rows")
  }
  private val left = OrderedInput(leftKey)
  private val right = OrderedInput(rightKey)
  private val existence = ExistenceJoin(AttributeReference("exists", BooleanType)())

  private def ordering(joinType: JoinType, buildSide: JoinBuildSide): Seq[SortOrder] = {
    NativeShuffledHashJoinExecProvider
      .provide(
        left,
        right,
        Seq(leftKey),
        Seq(rightKey),
        joinType,
        Some(GreaterThan(leftKey, rightKey)),
        buildSide,
        isSkewJoin = false)
      .outputOrdering
  }

  for ((joinType, buildSide) <- Seq(
      (LeftOuter, JoinBuildLeft),
      (LeftSemi, JoinBuildLeft),
      (LeftAnti, JoinBuildLeft),
      (existence, JoinBuildLeft),
      (RightOuter, JoinBuildRight))) {
    test(s"Spark 3.1 forced $joinType building $buildSide has no ordering guarantee") {
      if (org.apache.spark.SPARK_VERSION.startsWith("3.1.")) {
        assert(ordering(joinType, buildSide).isEmpty)
      }
    }
  }

  test("Spark 3.1 retains valid probe-side ordering") {
    if (org.apache.spark.SPARK_VERSION.startsWith("3.1.")) {
      for (joinType <- Seq(Inner, LeftOuter, LeftSemi, LeftAnti, existence)) {
        assert(ordering(joinType, JoinBuildRight) == left.outputOrdering)
      }
      for (joinType <- Seq(Inner, RightOuter)) {
        assert(ordering(joinType, JoinBuildLeft) == right.outputOrdering)
      }
      assert(ordering(FullOuter, JoinBuildLeft).isEmpty)
      assert(ordering(FullOuter, JoinBuildRight).isEmpty)
    }
  }
}
