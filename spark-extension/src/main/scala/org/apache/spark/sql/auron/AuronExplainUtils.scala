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
package org.apache.spark.sql.auron

import java.util.Collections.newSetFromMap

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, BitSet}

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.auron.AuronConvertStrategy.neverConvertReasonTag
import org.apache.spark.sql.catalyst.expressions.{Expression, PlanExpression}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.{BaseSubqueryExec, InputAdapter, ReusedSubqueryExec, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.ExplainUtils.getOpId
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}

import org.apache.auron.sparkver

object AuronExplainUtils {
  private def generateOperatorIDs(
      plan: QueryPlan[_],
      startOperatorID: Int,
      visited: java.util.Set[QueryPlan[_]],
      reusedExchanges: ArrayBuffer[ReusedExchangeExec],
      addReusedExchanges: Boolean): Int = {
    var currentOperationID = startOperatorID
    if (plan.isInstanceOf[BaseSubqueryExec]) {
      return currentOperationID
    }

    def setOpId(plan: QueryPlan[_]): Unit = if (!visited.contains(plan)) {
      plan match {
        case r: ReusedExchangeExec if addReusedExchanges =>
          reusedExchanges.append(r)
        case _ =>
      }
      visited.add(plan)
      currentOperationID += 1
      plan.setTagValue(TreeNodeTag[Int]("operatorId"), currentOperationID)
    }

    plan.foreachUp {
      case _: WholeStageCodegenExec =>
      case _: InputAdapter =>
      case p: AdaptiveSparkPlanExec =>
        currentOperationID = generateOperatorIDs(
          p.executedPlan,
          currentOperationID,
          visited,
          reusedExchanges,
          addReusedExchanges)
        setOpId(p)
      case p: QueryStageExec =>
        currentOperationID = generateOperatorIDs(
          p.plan,
          currentOperationID,
          visited,
          reusedExchanges,
          addReusedExchanges)
        setOpId(p)
      case other: QueryPlan[_] =>
        setOpId(other)
        currentOperationID = other.innerChildren.foldLeft(currentOperationID) { (curId, plan) =>
          generateOperatorIDs(plan, curId, visited, reusedExchanges, addReusedExchanges)
        }
    }
    currentOperationID
  }

  private def getSubqueries(
      plan: => QueryPlan[_],
      subqueries: ArrayBuffer[(SparkPlan, Expression, BaseSubqueryExec)]): Unit = {
    plan.foreach {
      case a: AdaptiveSparkPlanExec =>
        getSubqueries(a.executedPlan, subqueries)
      case q: QueryStageExec =>
        getSubqueries(q.plan, subqueries)
      case p: SparkPlan =>
        p.expressions.foreach(_.collect { case e: PlanExpression[_] =>
          e.plan match {
            case s: BaseSubqueryExec =>
              subqueries += ((p, e, s))
              getSubqueries(s, subqueries)
            case _ =>
          }
        })
    }
  }

  private def processPlanSkippingSubqueries[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      collectedOperators: BitSet): Unit = {
    try {

      QueryPlan.append(plan, append, verbose = false, addSuffix = false, printOperatorId = true)

      append("\n")
    } catch {
      case e: AnalysisException => append(e.toString)
    }
  }

  private def collectFallbackNodes(plan: QueryPlan[_]): (Int, Map[String, String]) = {
    var numAuronNodes = 0
    val fallbackNodeToReason = new mutable.HashMap[String, String]

    def collect(tmp: QueryPlan[_]): Unit = {
      tmp.foreachUp {
        case p: ExecutedCommandExec =>
          handleVanillaSparkPlan(p, fallbackNodeToReason)
        case p: AdaptiveSparkPlanExec =>
          handleVanillaSparkPlan(p, fallbackNodeToReason)
          collect(p.executedPlan)
        case p: QueryStageExec =>
          handleVanillaSparkPlan(p, fallbackNodeToReason)
          collect(p.plan)
        case p: NativeSupports =>
          numAuronNodes += 1
          p.innerChildren.foreach(collect)
        case p: SparkPlan =>
          handleVanillaSparkPlan(p, fallbackNodeToReason)
          p.innerChildren.foreach(collect)
        case _ =>
      }
    }

    collect(plan)
    (numAuronNodes, fallbackNodeToReason.toMap)
  }

  def handleVanillaSparkPlan(
      p: SparkPlan,
      fallbackNodeToReason: mutable.HashMap[String, String]): Unit = {
    if (p.getTagValue(neverConvertReasonTag).isDefined) {
      addFallbackNodeWithReason(p, p.getTagValue(neverConvertReasonTag).get, fallbackNodeToReason)
    }
  }

  def addFallbackNodeWithReason(
      p: SparkPlan,
      reason: String,
      fallbackNodeToReason: mutable.HashMap[String, String]): Unit = {
    p.getTagValue(TreeNodeTag[Int]("operatorId")).foreach { opId =>
      // e.g., 002 project, it is used to help analysis by `substring(4)`
      val formattedNodeName = f"$opId%03d ${p.nodeName}"
      fallbackNodeToReason.put(formattedNodeName, reason)
    }
  }

  def processPlan[T <: QueryPlan[T]](
      plan: T,
      append: String => Unit,
      collectFallbackFunc: Option[QueryPlan[_] => (Int, Map[String, String])] = None)
      : (Int, Map[String, String]) = synchronized {
    try {
      val operators = newSetFromMap[QueryPlan[_]](new java.util.IdentityHashMap())
      val reusedExchanges = ArrayBuffer.empty[ReusedExchangeExec]

      var currentOperatorID = 0
      currentOperatorID =
        generateOperatorIDs(plan, currentOperatorID, operators, reusedExchanges, true)

      val subqueries = ArrayBuffer.empty[(SparkPlan, Expression, BaseSubqueryExec)]
      getSubqueries(plan, subqueries)

      currentOperatorID = subqueries.foldLeft(currentOperatorID) { (curId, plan) =>
        generateOperatorIDs(plan._3.child, curId, operators, reusedExchanges, true)
      }

      val optimizedOutExchanges = ArrayBuffer.empty[Exchange]
      reusedExchanges.foreach { reused =>
        val child = reused.child
        if (!operators.contains(child)) {
          optimizedOutExchanges.append(child)
          currentOperatorID =
            generateOperatorIDs(child, currentOperatorID, operators, reusedExchanges, false)
        }
      }

      val collectedOperators = mutable.BitSet.empty
      processPlanSkippingSubqueries(plan, append, collectedOperators)

      var i = 0
      for (sub <- subqueries) {
        if (i == 0) {
          append("\n===== Subqueries =====\n\n")
        }
        i = i + 1
        append(
          s"Subquery:$i Hosting operator id = " +
            s"${getOpId(sub._1)} Hosting Expression = ${sub._2}\n")

        if (!sub._3.isInstanceOf[ReusedSubqueryExec]) {
          processPlanSkippingSubqueries(sub._3.child, append, collectedOperators)
        }
        append("\n")
      }

      i = 0
      optimizedOutExchanges.foreach { exchange =>
        if (i == 0) {
          append("\n===== Adaptively Optimized Out Exchanges =====\n\n")
        }
        i = i + 1
        append(s"Subplan:$i\n")
        processPlanSkippingSubqueries[SparkPlan](exchange, append, collectedOperators)
        append("\n")
      }

      (subqueries.filter(!_._3.isInstanceOf[ReusedSubqueryExec]).map(_._3.child) :+ plan)
        .map { plan =>
          if (collectFallbackFunc.isEmpty) {
            collectFallbackNodes(plan)
          } else {
            collectFallbackFunc.get.apply(plan)
          }
        }
        .reduce((a, b) => (a._1 + b._1, a._2 ++ b._2))
    } finally {
      removeTags(plan)
    }
  }

  @sparkver("3.1/ 3.2 / 3.3/ 3.4/ 3.5")
  private def removeTags(plan: QueryPlan[_]): Unit = {
    def remove(p: QueryPlan[_], children: Seq[QueryPlan[_]]): Unit = {
      p.unsetTagValue(TreeNodeTag[Int]("operatorId"))
      children.foreach(removeTags)
    }

    plan.foreach {
      case p: AdaptiveSparkPlanExec => remove(p, Seq(p.executedPlan))
      case p: QueryStageExec => remove(p, Seq(p.plan))
      case plan: QueryPlan[_] => remove(plan, plan.innerChildren)
    }
  }

  @sparkver("3.0")
  private def removeTags(plan: QueryPlan[_]): Unit = {
    def remove(p: QueryPlan[_], children: Seq[QueryPlan[_]]): Unit = {
      p.unsetTagValue(TreeNodeTag[Int]("operatorId"))
      children.foreach(removeTags)
    }

    plan.foreach {
      case p: AdaptiveSparkPlanExec => remove(p, Seq(p.executedPlan, p.initialPlan))
      case p: QueryStageExec => remove(p, Seq(p.plan))
      case plan: QueryPlan[_] => remove(plan, plan.innerChildren)
    }
  }
}
