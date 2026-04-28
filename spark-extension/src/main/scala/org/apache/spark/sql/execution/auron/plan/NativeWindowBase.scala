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

import scala.collection.immutable.SortedMap
import scala.jdk.CollectionConverters._

import org.apache.spark.OneToOneDependency
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.auron.NativeHelper
import org.apache.spark.sql.auron.NativeRDD
import org.apache.spark.sql.auron.NativeSupports
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.CumeDist
import org.apache.spark.sql.catalyst.expressions.DenseRank
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Lead
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.PercentRank
import org.apache.spark.sql.catalyst.expressions.Rank
import org.apache.spark.sql.catalyst.expressions.RowNumber
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.WindowExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.expressions.aggregate.Max
import org.apache.spark.sql.catalyst.expressions.aggregate.Min
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.metric.SQLMetric

import org.apache.auron.{protobuf => pb}
import org.apache.auron.metric.SparkMetricNode
import org.apache.auron.protobuf.WindowGroupLimit

abstract class NativeWindowBase(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    groupLimit: Option[Int],
    override val child: SparkPlan)
    extends UnaryExecNode
    with NativeSupports {

  override val nodeName: String = groupLimit match {
    case Some(_) => "NativeWindowGroupLimit"
    case None => "NativeWindow"
  }

  override lazy val metrics: Map[String, SQLMetric] = SortedMap[String, SQLMetric]() ++ Map(
    NativeHelper
      .getDefaultNativeMetrics(sparkContext)
      .filterKeys(Set("stage_id", "output_rows", "elapsed_compute"))
      .toSeq: _*)

  override def output: Seq[Attribute] = groupLimit match {
    case Some(_) => child.output
    case None => child.output ++ windowExpression.map(_.toAttribute)
  }
  override def outputPartitioning: Partitioning = child.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(partitionSpec) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  private def leadIgnoreNulls(expr: Lead): Boolean =
    expr.getClass.getMethods
      .find(method => method.getName == "ignoreNulls" && method.getParameterCount == 0)
      .exists(method => method.invoke(expr).asInstanceOf[Boolean])

  private def invokeNoArg[T](expr: Expression, methodName: String): T =
    expr.getClass.getMethod(methodName).invoke(expr).asInstanceOf[T]

  private def isNthValue(expr: Expression): Boolean = expr.getClass.getSimpleName == "NthValue"

  private def nthValueInput(expr: Expression): Expression = invokeNoArg[Expression](expr, "input")

  private def nthValueOffset(expr: Expression): Expression =
    invokeNoArg[Expression](expr, "offset")

  private def nthValueIgnoreNulls(expr: Expression): Boolean =
    invokeNoArg[Boolean](expr, "ignoreNulls")

  private def nthValueOffsetLiteral(expr: Expression): Literal = {
    val offset = nthValueOffset(expr)
    offset match {
      case literal: Literal => literal
      case foldable if foldable.foldable =>
        Literal.create(foldable.eval(), foldable.dataType)
      case other =>
        throw new NotImplementedError(
          s"window function not supported: nth_value offset must be foldable, got: $other")
    }
  }

  private def nativeWindowExprs = windowExpression.map { named =>
    val field = NativeConverters.convertField(Util.getSchema(named :: Nil).fields(0))
    val windowExprBuilder = pb.WindowExprNode.newBuilder().setField(field)
    windowExprBuilder.setReturnType(NativeConverters.convertDataType(named.dataType))

    named.children.head match {
      case WindowExpression(function, spec) =>
        function match {
          case e @ RowNumber() =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.ROW_NUMBER)

          case e: Rank =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.RANK)

          case e: DenseRank =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.DENSE_RANK)

          case e: PercentRank =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            assert(
              spec.orderSpec.nonEmpty,
              "window function not supported: percent_rank requires ORDER BY")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.PERCENT_RANK)
          case e if isNthValue(e) =>
            assert(
              // Spark defaults ordered nth_value() to a RANGE frame. The current native executor
              // only supports cumulative ROW frames, so keep the conversion scoped to the
              // explicit ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW case.
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(if (nthValueIgnoreNulls(e)) {
              pb.WindowFunction.NTH_VALUE_IGNORE_NULLS
            } else {
              pb.WindowFunction.NTH_VALUE
            })
            windowExprBuilder.addChildren(NativeConverters.convertExpr(nthValueInput(e)))
            windowExprBuilder.addChildren(NativeConverters.convertExpr(nthValueOffsetLiteral(e)))

          case e: Lead =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            assert(!leadIgnoreNulls(e), "window function not supported: lead with IGNORE NULLS")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.LEAD)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.input))
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.offset))
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.default))

          case e: CumeDist =>
            assert(
              spec.frameSpecification == e.frame,
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Window)
            windowExprBuilder.setWindowFunc(pb.WindowFunction.CUME_DIST)

          case e: Sum =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounde, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.SUM)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.child))

          case e: Average =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.AVG)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.child))

          case e: Max =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.MAX)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.child))

          case e: Min =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.MIN)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(e.child))

          case Count(child :: Nil) =>
            assert(
              spec.frameSpecification == RowNumber().frame, // only supports RowFrame(Unbounded, CurrentRow)
              s"window frame not supported: ${spec.frameSpecification}")
            windowExprBuilder.setFuncType(pb.WindowFunctionType.Agg)
            windowExprBuilder.setAggFunc(pb.AggFunction.COUNT)
            windowExprBuilder.addChildren(NativeConverters.convertExpr(child))

          case other =>
            throw new NotImplementedError(s"window function not supported: $other")
        }
      case other =>
        throw new NotImplementedError(s"expect WindowExpression, got: $other")
    }
    windowExprBuilder.build()
  }

  private def nativePartitionSpecExprs = partitionSpec.map { partition =>
    NativeConverters.convertExpr(partition)
  }

  private def nativeOrderSpecExprs = orderSpec.map { sortOrder =>
    pb.PhysicalExprNode
      .newBuilder()
      .setSort(
        pb.PhysicalSortExprNode
          .newBuilder()
          .setExpr(NativeConverters.convertExpr(sortOrder.child))
          .setAsc(sortOrder.direction == Ascending)
          .setNullsFirst(sortOrder.nullOrdering == NullsFirst)
          .build())
      .build()
  }

  // check whether native converting is supported
  nativeWindowExprs
  nativeOrderSpecExprs
  nativePartitionSpecExprs

  override def doExecuteNative(): NativeRDD = {
    val inputRDD = NativeHelper.executeNative(child)
    val nativeMetrics = SparkMetricNode(metrics, inputRDD.metrics :: Nil)
    val nativeWindowExprs = this.nativeWindowExprs
    val nativeOrderSpecExprs = this.nativeOrderSpecExprs
    val nativePartitionSpecExprs = this.nativePartitionSpecExprs

    new NativeRDD(
      sparkContext,
      nativeMetrics,
      rddPartitions = inputRDD.partitions,
      rddPartitioner = inputRDD.partitioner,
      rddDependencies = new OneToOneDependency(inputRDD) :: Nil,
      inputRDD.isShuffleReadFull,
      (partition, taskContext) => {
        val inputPartition = inputRDD.partitions(partition.index)
        val nativeWindowExec = pb.WindowExecNode
          .newBuilder()
          .setInput(inputRDD.nativePlan(inputPartition, taskContext))
          .addAllWindowExpr(nativeWindowExprs.asJava)
          .addAllPartitionSpec(nativePartitionSpecExprs.asJava)
          .addAllOrderSpec(nativeOrderSpecExprs.asJava)

        // WindowGroupLimitExec does not output window cols
        groupLimit match {
          case Some(limit) =>
            nativeWindowExec.setGroupLimit(WindowGroupLimit.newBuilder().setK(limit))
            nativeWindowExec.setOutputWindowCols(false)
          case None =>
            nativeWindowExec.setOutputWindowCols(true)
        }

        pb.PhysicalPlanNode.newBuilder().setWindow(nativeWindowExec).build()
      },
      friendlyName = "NativeRDD.Window")
  }
}
