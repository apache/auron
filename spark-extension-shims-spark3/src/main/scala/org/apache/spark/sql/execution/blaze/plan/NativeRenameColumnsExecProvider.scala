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

import org.apache.spark.sql.execution.SparkPlan

import com.thoughtworks.enableIf

case object NativeRenameColumnsExecProvider {
  @enableIf(Seq("spark-3.4", "spark-3.5").contains(System.getProperty("blaze.shim")))
  def provide(child: SparkPlan, renamedColumnNames: Seq[String]): NativeRenameColumnsBase = {
    import org.apache.spark.sql.catalyst.expressions.NamedExpression
    import org.apache.spark.sql.catalyst.expressions.SortOrder
    import org.apache.spark.sql.execution.OrderPreservingUnaryExecNode
    import org.apache.spark.sql.execution.PartitioningPreservingUnaryExecNode

    case class NativeRenameColumnsExec(
        override val child: SparkPlan,
        renamedColumnNames: Seq[String])
        extends NativeRenameColumnsBase(child, renamedColumnNames)
        with PartitioningPreservingUnaryExecNode
        with OrderPreservingUnaryExecNode {

      override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
        copy(child = newChild)

      override protected def outputExpressions: Seq[NamedExpression] = output

      override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering
    }
    NativeRenameColumnsExec(child, renamedColumnNames)
  }

  @enableIf(Seq("spark-3.1", "spark-3.2", "spark-3.3").contains(System.getProperty("blaze.shim")))
  def provide(child: SparkPlan, renamedColumnNames: Seq[String]): NativeRenameColumnsBase = {
    import org.apache.spark.sql.catalyst.expressions.NamedExpression
    import org.apache.spark.sql.catalyst.expressions.SortOrder
    import org.apache.spark.sql.execution.AliasAwareOutputOrdering
    import org.apache.spark.sql.execution.AliasAwareOutputPartitioning

    case class NativeRenameColumnsExec(
        override val child: SparkPlan,
        renamedColumnNames: Seq[String])
        extends NativeRenameColumnsBase(child, renamedColumnNames)
        with AliasAwareOutputPartitioning
        with AliasAwareOutputOrdering {

      @enableIf(Seq("spark-3.2", "spark-3.3").contains(System.getProperty("blaze.shim")))
      override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
        copy(child = newChild)

      @enableIf(Seq("spark-3.1").contains(System.getProperty("blaze.shim")))
      override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
        copy(child = newChildren.head)

      override protected def outputExpressions: Seq[NamedExpression] = output

      override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering
    }
    NativeRenameColumnsExec(child, renamedColumnNames)
  }

  @enableIf(Seq("spark-3.0").contains(System.getProperty("blaze.shim")))
  def provide(child: SparkPlan, renamedColumnNames: Seq[String]): NativeRenameColumnsBase = {
    case class NativeRenameColumnsExec(
        override val child: SparkPlan,
        renamedColumnNames: Seq[String])
        extends NativeRenameColumnsBase(child, renamedColumnNames) {

      override def withNewChildren(newChildren: Seq[SparkPlan]): SparkPlan =
        copy(child = newChildren.head)
    }
    NativeRenameColumnsExec(child, renamedColumnNames)
  }
}
