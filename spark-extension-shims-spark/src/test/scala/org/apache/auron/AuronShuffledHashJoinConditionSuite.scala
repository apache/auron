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

import org.apache.spark.sql.auron.{AuronConverters, AuronConvertStrategy, Shims}
import org.apache.spark.sql.auron.AuronConvertStrategy.childOrderingRequiredTag
import org.apache.spark.sql.auron.join.JoinBuildSides.{JoinBuildLeft, JoinBuildRight}
import org.apache.spark.sql.catalyst.expressions.{Attribute, LessThan}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, LeftAnti, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LeafNode, Statistics}
import org.apache.spark.sql.execution.{InputAdapter, SortExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.auron.plan.{NativeShuffledHashJoinBase, NativeSortBase, NativeSortMergeJoinBase}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.internal.SQLConf

import org.apache.auron.util.AuronTestUtils

class AuronShuffledHashJoinConditionSuite extends AuronJoinConditionTestBase {
  private def withShuffledJoin(force: Boolean = false)(f: => Unit): Unit = {
    withSparkConf("spark.auron.forceShuffledHashJoin" -> force.toString) {
      withSQLConf(
        "spark.sql.adaptive.enabled" -> "false",
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.shuffle.partitions" -> "2") {
        withJoinInputs(f)
      }
    }
  }

  // withSQLConf/withSparkConf return Unit, so the result of their blocks is
  // captured explicitly instead of being taken as the block's value.
  private def withDisabledAuron[T](f: => T): T = {
    var result: Option[T] = None
    withSQLConf("spark.auron.enabled" -> "false") {
      result = Some(f)
    }
    result.get
  }

  private def withoutForcedShuffledHashJoin[T](f: => T): T = {
    var result: Option[T] = None
    withSparkConf("spark.auron.forceShuffledHashJoin" -> "false") {
      result = Some(f)
    }
    result.get
  }

  // Collect the vanilla sort-merge join planned with auron disabled. Query
  // preparation wraps non-codegen children in InputAdapter, which hides the
  // real operators from conversion marking, so unwrap them first.
  private def originalSmj(): SortMergeJoinExec = {
    val plan = withDisabledAuron {
      spark
        .sql("""
          |SELECT /*+ MERGE(l, r) */ *
          |FROM condition_left l JOIN condition_right r ON l.k = r.k
          |""".stripMargin)
        .queryExecution
        .executedPlan
        .collectFirst { case join: SortMergeJoinExec => join }
        .get
    }
    val unwrapped = plan
      .transformUp {
        case InputAdapter(child) => child
        case codegen: WholeStageCodegenExec => codegen.child
      }
      .asInstanceOf[SortMergeJoinExec]
    plan.logicalLink.foreach(unwrapped.setLogicalLink)
    unwrapped
  }

  for ((joinType, buildAlias) <- Seq(
      ("inner", "l"),
      ("inner", "r"),
      ("left outer", "r"),
      ("right outer", "l"),
      ("left semi", "r"),
      ("left anti", "r"))) {
    test(s"SHJ $joinType building $buildAlias evaluates residual conditions") {
      withShuffledJoin() {
        checkJoinPlan(
          s"""
            |SELECT /*+ SHUFFLE_HASH($buildAlias) */ *
            |FROM condition_left l $joinType JOIN condition_right r
            |ON l.k = r.k AND l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeShuffledHashJoinBase])
      }
    }
  }

  for (buildAlias <- Seq("l", "r")) {
    test(s"SHJ full outer residual condition building $buildAlias") {
      if (AuronTestUtils.isSparkV32OrGreater) {
        withShuffledJoin() {
          checkJoinPlan(
            s"""
              |SELECT /*+ SHUFFLE_HASH($buildAlias) */ *
              |FROM condition_left l FULL OUTER JOIN condition_right r
              |ON l.k = r.k AND l.v < r.v
              |""".stripMargin,
            _.isInstanceOf[NativeShuffledHashJoinBase])
        }
      }
    }
  }

  for (joinType <- Seq("left outer", "left semi", "left anti")) {
    test(s"forced SMJ to SHJ conversion preserves $joinType residual condition") {
      withShuffledJoin(force = true) {
        checkJoinPlan(
          s"""
            |SELECT /*+ MERGE(l, r) */ *
            |FROM condition_left l $joinType JOIN condition_right r
            |ON l.k = r.k AND l.v < r.v
            |""".stripMargin,
          _.isInstanceOf[NativeShuffledHashJoinBase])
      }
    }
  }

  private case class SizedJoinInput(output: Seq[Attribute], size: BigInt) extends LeafNode {
    override def computeStats(): Statistics = Statistics(sizeInBytes = size)
  }

  test("forced SHJ selects by valid statistics and defaults to BuildRight otherwise") {
    withShuffledJoin(force = true) {
      val defaultSize = BigInt(SQLConf.get.defaultSizeInBytes)
      val cases = Seq(
        (Some((BigInt(8), BigInt(80))), JoinBuildLeft),
        (Some((BigInt(80), BigInt(8))), JoinBuildRight),
        (Some((BigInt(8), BigInt(8))), JoinBuildRight),
        (Some((BigInt(0), BigInt(80))), JoinBuildLeft),
        (Some((BigInt(-1), BigInt(80))), JoinBuildRight),
        (Some((BigInt(8), BigInt(-1))), JoinBuildRight),
        (Some((defaultSize, BigInt(80))), JoinBuildRight),
        (Some((BigInt(8), defaultSize)), JoinBuildRight),
        (Some((BigInt(8), BigInt(Long.MaxValue))), JoinBuildRight),
        (None, JoinBuildRight))
      for ((sizes, expected) <- cases) {
        val original = originalSmj()
        val logicalJoin = original.logicalLink.get.asInstanceOf[Join]
        val join = original
          .makeCopy(original.productIterator.map(_.asInstanceOf[AnyRef]).toArray)
          .asInstanceOf[SortMergeJoinExec]
        sizes match {
          case Some((leftSize, rightSize)) =>
            join.setLogicalLink(
              logicalJoin.copy(
                left = SizedJoinInput(logicalJoin.left.output, leftSize),
                right = SizedJoinInput(logicalJoin.right.output, rightSize)))
          case None =>
            join.unsetTagValue(SparkPlan.LOGICAL_PLAN_TAG)
            join.unsetTagValue(SparkPlan.LOGICAL_PLAN_INHERITED_TAG)
        }
        AuronConvertStrategy.apply(join)
        val converted = AuronConverters.convertSparkPlanRecursively(join)
        assert(converted.isInstanceOf[NativeShuffledHashJoinBase], converted.toString)
        assert(converted.productIterator.contains(expected), s"sizes=$sizes, plan=$converted")
      }
    }
  }

  for (adaptive <- Seq(false, true); (leftSize, rightSize) <- Seq((4, 100), (100, 4))) {
    test(s"forced SHJ builds the smaller side: AQE=$adaptive, left=$leftSize, right=$rightSize") {
      withShuffledJoin(force = true) {
        withSQLConf("spark.sql.adaptive.enabled" -> adaptive.toString) {
          val expected = if (leftSize < rightSize) JoinBuildLeft else JoinBuildRight
          checkJoinPlan(
            s"""
              |SELECT /*+ MERGE(l, r) */ l.id
              |FROM range($leftSize) l JOIN range($rightSize) r ON l.id = r.id
              |""".stripMargin,
            {
              case join: NativeShuffledHashJoinBase => join.productIterator.contains(expected)
              case _ => false
            })
        }
      }
    }
  }

  test("cascaded forced SHJ builds the smaller side at each level") {
    withShuffledJoin(force = true) {
      withSQLConf("spark.sql.adaptive.enabled" -> "true") {
        val df = checkSparkAnswer("""
          |SELECT /*+ MERGE(ab, c) */ ab.id
          |FROM (
          |  SELECT /*+ MERGE(a, b) */ a.id
          |  FROM (SELECT id FROM range(0, 10000, 1, 2) WHERE id % 1000 = 0) a
          |  JOIN range(0, 2000, 1, 2) b ON a.id = b.id
          |) ab
          |JOIN range(0, 100, 1, 2) c ON ab.id = c.id
          |""".stripMargin)
        val plan = stripAQEPlan(df.queryExecution.executedPlan)
        def collectJoins(plan: SparkPlan): Seq[NativeShuffledHashJoinBase] = {
          val current = plan match {
            case join: NativeShuffledHashJoinBase => Seq(join)
            case _ => Nil
          }
          val children = if (Shims.get.isQueryStageInput(plan)) {
            Seq(Shims.get.getChildStage(plan))
          } else {
            plan.children
          }
          current ++ children.flatMap(collectJoins)
        }
        val joins = collectJoins(plan)
        assert(joins.size == 2, plan.toString)
        assert(joins.forall(_.productIterator.contains(JoinBuildRight)), plan.toString)
      }
    }
  }

  test("forced SHJ accepts a direct SMJ input without discarding its children") {
    withShuffledJoin(force = true) {
      val original = originalSmj()
      val semi = original.copy(joinType = LeftSemi)
      val lower = withoutForcedShuffledHashJoin {
        AuronConvertStrategy.apply(semi)
        AuronConverters.convertSparkPlanRecursively(semi)
      }
      assert(lower.isInstanceOf[NativeSortMergeJoinBase], lower.toString)
      val upper = original.copy(left = lower)
      val converted = AuronConverters.convertSortMergeJoinExec(upper)
      assert(converted.isInstanceOf[NativeShuffledHashJoinBase], converted.toString)
      assert(converted.output == upper.output)
      assert(converted.collect { case join: NativeSortMergeJoinBase => join }.size == 1)
      assert(converted.collect { case _: NativeSortBase | _: SortExec => 1 }.size == 2)
    }
  }

  for (requireOrdering <- Seq(false, true)) {
    test(
      s"cascaded SMJ resets child ordering after conversion: requireOrdering=$requireOrdering") {
      withShuffledJoin(force = true) {
        val original = originalSmj()
        val semi = original.copy(joinType = LeftSemi)
        val anti = original.copy(left = semi, joinType = LeftAnti)
        val upper = original.copy(left = anti, joinType = LeftSemi)
        AuronConvertStrategy.apply(upper)
        assert(semi.getTagValue(childOrderingRequiredTag).contains(true))
        assert(anti.getTagValue(childOrderingRequiredTag).contains(true))
        upper.setTagValue(childOrderingRequiredTag, requireOrdering)
        if (requireOrdering) {
          anti.setTagValue(childOrderingRequiredTag, true)
        }
        val converted = AuronConverters.convertSparkPlanRecursively(upper)
        assert(converted.output == upper.output)
        val hashJoins = converted.collect { case join: NativeShuffledHashJoinBase => join }
        val mergeJoins = converted.collect { case join: NativeSortMergeJoinBase => join }
        if (requireOrdering) {
          assert(hashJoins.isEmpty, converted.toString)
          assert(mergeJoins.size == 3, converted.toString)
          assert(converted.collect { case _: NativeSortBase | _: SortExec => 1 }.size == 4)
        } else {
          assert(hashJoins.size == 3, converted.toString)
          assert(mergeJoins.isEmpty, converted.toString)
          assert(converted.collect { case _: NativeSortBase | _: SortExec => 1 }.isEmpty)
          assert(!semi.getTagValue(childOrderingRequiredTag).contains(true))
          assert(!anti.getTagValue(childOrderingRequiredTag).contains(true))
        }
      }
    }
  }

  test("SHJ existence condition uses columns outside its output") {
    withShuffledJoin(force = true) {
      // OR preserves an ExistenceJoin without projecting EXISTS, unsupported by Spark 3.0.
      for (predicate <- Seq("EXISTS", "NOT EXISTS")) {
        checkJoinPlan(
          s"""
            |SELECT l.k FROM condition_left l
            |WHERE l.k = 3 OR $predicate (
            |  SELECT 1 FROM condition_right r WHERE l.k = r.k AND l.v < r.v)
            |""".stripMargin,
          {
            case join: NativeShuffledHashJoinBase =>
              join.productIterator.exists(_.isInstanceOf[ExistenceJoin]) &&
              join.expressions.exists(_.isInstanceOf[LessThan])
            case _ => false
          })
      }
    }
  }

  for (force <- Seq(false, true)) {
    test(s"SHJ residual condition respects configuration with force=$force") {
      withShuffledJoin(force) {
        withSQLConf("spark.auron.enable.native.join.condition" -> "false") {
          val hint = if (force) "MERGE(l, r)" else "SHUFFLE_HASH(r)"
          checkJoinPlan(
            s"""
              |SELECT /*+ $hint */ *
              |FROM condition_left l LEFT OUTER JOIN condition_right r
              |ON l.k = r.k AND l.v < r.v
              |""".stripMargin,
            _.isInstanceOf[NativeShuffledHashJoinBase],
            native = false)
        }
      }
    }
  }
}
