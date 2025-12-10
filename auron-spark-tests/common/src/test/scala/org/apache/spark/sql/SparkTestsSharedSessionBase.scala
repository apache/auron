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
package org.apache.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.test.SharedSparkSession

trait SparkTestsSharedSessionBase extends SharedSparkSession with SparkTestsBase {
  override def sparkConf: SparkConf = {
    val conf = super.sparkConf
      .setAppName("Auron-UT")
      .set("spark.sql.warehouse.dir", warehouse)

    for ((key, value) <- sparkConfList) {
      conf.set(key, value)
    }

    conf
  }

  /**
   * Get all the children(and descendents) plan of plans.
   *
   * @param plans
   *   the input plans.
   * @return
   *   all the children(and descendents) plan
   */
  private def getChildrenPlan(plans: Seq[SparkPlan]): Seq[SparkPlan] = {
    if (plans.isEmpty) {
      return Seq()
    }

    val inputPlans: Seq[SparkPlan] = plans.map {
      case stage: QueryStageExec => stage.plan
      case plan => plan
    }

    var newChildren: Seq[SparkPlan] = Seq()
    inputPlans.foreach { plan =>
      newChildren = newChildren ++ getChildrenPlan(plan.children)
      // To avoid duplication of WholeStageCodegenXXX and its children.
      if (!plan.nodeName.startsWith("WholeStageCodegen")) {
        newChildren = newChildren :+ plan
      }
    }
    newChildren
  }

  /**
   * Get the executed plan of a data frame.
   *
   * @param df
   *   dataframe.
   * @return
   *   A sequence of executed plans.
   */
  def getExecutedPlan(df: DataFrame): Seq[SparkPlan] = {
    df.queryExecution.executedPlan match {
      case exec: AdaptiveSparkPlanExec =>
        getChildrenPlan(Seq(exec.executedPlan))
      case plan =>
        getChildrenPlan(Seq(plan))
    }
  }
}
