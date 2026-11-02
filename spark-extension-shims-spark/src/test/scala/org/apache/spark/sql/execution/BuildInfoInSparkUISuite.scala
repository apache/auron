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
package org.apache.spark.sql.execution

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.auron.{AuronConverters, AuronConvertStrategy, AuronPlanDiagnostics}
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.ui.{AuronSQLAppStatusListener, AuronSQLAppStatusStore}
import org.apache.spark.util.Utils

import org.apache.auron.{AuronSQLTestHelper, BaseAuronSQLSuite}
import org.apache.auron.sparkver

class BuildInfoInSparkUISuite
    extends AuronQueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {

  var testDir: File = _

  @sparkver("3.0")
  private def buildLeft = org.apache.spark.sql.execution.joins.BuildLeft

  @sparkver("3.1 / 3.2 / 3.3 / 3.4 / 3.5 / 4.0 / 4.1 / 4.2")
  private def buildLeft = org.apache.spark.sql.catalyst.optimizer.BuildLeft

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.eventLog.dir", testDir.toString)
  }

  override def beforeAll(): Unit = {
    testDir = Utils.createTempDir(namePrefix = "spark-events")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    try super.afterAll()
    finally Utils.deleteRecursively(testDir)
  }

  test("test build info in spark UI ") {
    val listeners = spark.sparkContext.listenerBus.findListenersByClass[AuronSQLAppStatusListener]
    assert(listeners.size === 1)
    val listener = listeners(0)
    spark.sparkContext.listenerBus.waitUntilEmpty()
    assert(listener.getAuronBuildInfo() == 1)
  }

  test("native conversion publishes diagnostics to the UI store") {
    withTable("auron_ui_diagnostics") {
      sql("create table auron_ui_diagnostics (value int) using parquet")
      sql("insert into auron_ui_diagnostics values (1), (2)")
      sql("select sum(value) from auron_ui_diagnostics").collect()
      spark.sparkContext.listenerBus.waitUntilEmpty()
      val store = new AuronSQLAppStatusStore(spark.sparkContext.statusStore.store)
      val executions = store.executions(0, 100)
      val snapshots = executions.flatMap(_.snapshots)
      assert(executions.exists(e => store.metrics(e.executionId).nonEmpty))
      assert(snapshots.nonEmpty)
      assert(snapshots.exists(_.nodes.headOption.exists(_.name == "AdaptiveSparkPlan")))
      assert(snapshots.exists(_.nodes.exists(_.status == "Native")))
      assert(snapshots.forall(_.physicalPlan.nonEmpty))
      assert(snapshots.exists(_.configurations.contains("spark.sql.session.timeZone")))
      assert(snapshots.exists(_.configurations.contains("spark.auron.memoryFraction")))
      assert(
        snapshots.forall(
          _.configurations
            .get("spark.auron.enable.data.writing")
            .contains("false")))
      assert(
        snapshots.forall(_.configurations.contains("spark.auron.enable.data.writing.parquet")))
      assert(snapshots.forall(_.configurations.contains("spark.auron.enable.data.writing.orc")))

      val beforeFallback = store.executions(0, 100).map(_.executionId).toSet
      spark.udf.register("auron_ui_passthrough", (value: Int) => value)
      sql("select auron_ui_passthrough(value) from auron_ui_diagnostics").collect()
      spark.sparkContext.listenerBus.waitUntilEmpty()
      val fallbackSnapshots = store
        .executions(0, 100)
        .filterNot(e => beforeFallback.contains(e.executionId))
        .flatMap(_.snapshots)
      assert(fallbackSnapshots.exists(_.nodes.exists(n =>
        n.name == "Project" && n.status == "Fallback" && n.reason.contains("ScalaUDF"))))

      val beforeResultProjection = store.executions(0, 100).map(_.executionId).toSet
      val result =
        sql("select auron_ui_passthrough(cast(sum(value) as int)) from auron_ui_diagnostics")
      assert(result.collect().head.getInt(0) == 3)
      spark.sparkContext.listenerBus.waitUntilEmpty()
      val resultSnapshots = store
        .executions(0, 100)
        .filterNot(e => beforeResultProjection.contains(e.executionId))
        .flatMap(_.snapshots)
      assert(resultSnapshots.exists(_.nodes.exists(_.status == "Native")))
      assert(resultSnapshots.exists(_.nodes.exists(n =>
        n.name == "Project" && n.status == "Fallback" && n.reason.contains("ScalaUDF"))))

      val beforeDisabled = store.executions(0, 100).map(_.executionId).toSet
      withSQLConf("spark.auron.enabled" -> "false") {
        sql("select sum(value) from auron_ui_diagnostics").collect()
      }
      spark.sparkContext.listenerBus.waitUntilEmpty()
      assert(
        store
          .executions(0, 100)
          .filterNot(e => beforeDisabled.contains(e.executionId))
          .forall(_.snapshots.isEmpty))
    }
  }

  test("default strategy does not infer why native conversion was not selected") {
    val plan = spark.range(1).selectExpr("id + 1 AS value").queryExecution.sparkPlan
    AuronConvertStrategy(plan)
    val project = plan.collectFirst { case p: ProjectExec => p }.get
    assert(
      project
        .getTagValue(AuronConvertStrategy.neverConvertReasonTag)
        .contains("No native conversion strategy was selected for ProjectExec."))
  }

  test("forced hash-join rewrites preserve the rejected condition reason through conversion") {
    withSparkConf("spark.auron.forceShuffledHashJoin" -> "true") {
      withSQLConf("spark.auron.enable.native.join.condition" -> "false") {
        val left = spark.range(1).queryExecution.sparkPlan
        val right = spark.range(1).queryExecution.sparkPlan
        val join = ShuffledHashJoinExec(
          left.output,
          right.output,
          Inner,
          buildLeft,
          Some(Literal(true)),
          left,
          right)
        AuronConvertStrategy(join)
        assert(
          join
            .getTagValue(AuronConvertStrategy.convertToNonNativeReasonTag)
            .exists(_.contains("native join condition is disabled")))
        val converted = AuronConverters.convertSparkPlanRecursively(join)
        assert(converted.isInstanceOf[SortMergeJoinExec])
        val node = AuronPlanDiagnostics.collect(converted).head
        assert(node.status == "Fallback")
        assert(node.reason.contains("SortMergeJoin"))
        assert(node.reason.contains("native join condition is disabled"))
      }
    }
  }

}
