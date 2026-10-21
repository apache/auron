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
package org.apache.spark.sql.execution.ui

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.auron.{AuronConvertStrategy, AuronPlanDiagnostics, ForceNativeExecutionWrapper}
import org.apache.spark.sql.execution.{LocalTableScanExec, UnionExec}
import org.apache.spark.status.ElementTrackingStore
import org.apache.spark.util.JsonProtocol
import org.apache.spark.util.kvstore.InMemoryStore

import org.apache.auron.spark.ui.{AuronPlanDiagnosticEvent, AuronPlanDiagnosticNode}
import org.apache.auron.sparkver

class AuronDiagnosticsSuite extends SparkFunSuite {
  @sparkver("3.0 / 3.1 / 3.2 / 3.3")
  private def writeEvent(event: org.apache.spark.scheduler.SparkListenerEvent): String =
    org.json4s.jackson.JsonMethods.compact(JsonProtocol.sparkEventToJson(event))

  @sparkver("3.4 / 3.5 / 4.0 / 4.1 / 4.2")
  private def writeEvent(event: org.apache.spark.scheduler.SparkListenerEvent): String =
    JsonProtocol.sparkEventToJsonString(event)

  @sparkver("3.0 / 3.1 / 3.2 / 3.3")
  private def readEvent(json: String): org.apache.spark.scheduler.SparkListenerEvent =
    JsonProtocol.sparkEventFromJson(org.json4s.jackson.JsonMethods.parse(json))

  @sparkver("3.4 / 3.5 / 4.0 / 4.1 / 4.2")
  private def readEvent(json: String): org.apache.spark.scheduler.SparkListenerEvent =
    JsonProtocol.sparkEventFromJson(json)

  @sparkver("3.0 / 3.1 / 3.2 / 3.3 / 3.4 / 3.5")
  private def emptyScan(): LocalTableScanExec = LocalTableScanExec(Nil, Nil)

  @sparkver("4.0 / 4.1 / 4.2")
  private def emptyScan(): LocalTableScanExec = LocalTableScanExec(Nil, Nil, None)

  private def snapshot(id: Long, index: Int = 0): AuronPlanDiagnosticEvent =
    AuronPlanDiagnosticEvent(
      id,
      index.toLong,
      s"plan $index",
      Seq(AuronPlanDiagnosticNode(1, "Project", "Fallback", "unsupported expression")),
      Map("spark.sql.session.timeZone" -> "UTC"))

  test("diagnostic events survive JSON replay and out-of-order lifecycle events") {
    val conf = new SparkConf(false)
    val kv = new ElementTrackingStore(new InMemoryStore, conf)
    try {
      val listener = new AuronSQLAppStatusListener(conf, kv)
      val store = new AuronSQLAppStatusStore(kv)
      val event = snapshot(7)
      val replayed = readEvent(writeEvent(event))
      assert(replayed == event)
      listener.onOtherEvent(replayed)
      listener.onOtherEvent(SparkListenerSQLExecutionEnd(7, 200))
      // This also checks that a late start does not erase an end or plan snapshot.
      listener.onOtherEvent(
        readEvent("""{"Event":"org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart",
        "executionId":7,"description":"query","details":"","physicalPlanDescription":"",
        "time":100,"modifiedConfigs":{},"jobTags":[]}"""))
      val stored = store.execution(7).get
      assert(stored.description == "query")
      assert(stored.startedAt == 100 && stored.endedAt == 200)
      assert(stored.snapshots == Seq(event))
      assert(stored.snapshots.head.nodes.head.reason == "unsupported expression")
    } finally kv.close()
  }

  test("bounded snapshots preserve AQE updates and pagination reads the requested page") {
    val conf = new SparkConf(false)
    val kv = new ElementTrackingStore(new InMemoryStore, conf)
    try {
      val listener = new AuronSQLAppStatusListener(conf, kv)
      val store = new AuronSQLAppStatusStore(kv)
      (0 until 25).foreach(i => listener.onOtherEvent(snapshot(1, i)))
      listener.onOtherEvent(snapshot(1, 24))
      assert(store.execution(1).get.snapshots.size == 20)
      assert(store.execution(1).get.snapshots.head.physicalPlan == "plan 5")
      (2L to 5L).foreach(id => listener.onOtherEvent(snapshot(id)))
      assert(store.executionsCount() == 5)
      assert(store.executions(1, 2).map(_.executionId) == Seq(4L, 3L))
      assert(store.execution(99).isEmpty)
      assert(store.metrics(99).isEmpty)
    } finally kv.close()
  }

  test("shared plan instances are visited once and operator tags are left unchanged") {
    val leaf = emptyScan()
    val tag = org.apache.spark.sql.catalyst.trees.TreeNodeTag[Int]("operatorId")
    leaf.setTagValue(tag, 1234)
    leaf.setTagValue(AuronConvertStrategy.neverConvertReasonTag, "test fallback")
    val plan = UnionExec(Seq(leaf, leaf))
    val nodes = AuronPlanDiagnostics.collect(plan)
    assert(nodes.size == 2)
    assert(nodes.count(_.reason == "test fallback") == 1)
    assert(leaf.getTagValue(tag).contains(1234))
  }

  test("retention deletes completed diagnostics but preserves active executions") {
    val conf = new SparkConf(false)
      .set("spark.sql.ui.retainedExecutions", "2")
      .set("spark.appStateStore.asyncTracking.enable", "false")
    val kv = new ElementTrackingStore(new InMemoryStore, conf)
    try {
      val listener = new AuronSQLAppStatusListener(conf, kv)
      val store = new AuronSQLAppStatusStore(kv)
      listener.onOtherEvent(snapshot(0))
      (1L to 4L).foreach { id =>
        listener.onOtherEvent(snapshot(id))
        listener.onOtherEvent(SparkListenerSQLExecutionEnd(id, 100))
      }
      assert(store.execution(0).isDefined)
      assert(store.executionsCount() == 2)
      assert(store.execution(4).isDefined)
    } finally kv.close()
  }

  test("diagnostic HTML escapes plan content and accepts malformed navigation parameters") {
    val conf = new SparkConf(false)
    val kv = new ElementTrackingStore(new InMemoryStore, conf)
    try {
      val listener = new AuronSQLAppStatusListener(conf, kv)
      listener.onOtherEvent(
        snapshot(1).copy(
          physicalPlan = "<script>alert(1)</script>",
          nodes = Seq(
            AuronPlanDiagnosticNode(1, "AdaptiveSparkPlan", "Wrapper", "old unsupported reason"),
            AuronPlanDiagnosticNode(2, "Range", "Spark (no reason recorded)", ""))))
      val view = new AuronDiagnosticsView(new AuronSQLAppStatusStore(kv))
      val details = view.render("1", null, "/history/app-1").toString
      assert(details.contains("&lt;script&gt;"))
      assert(!details.contains("<script>"))
      assert(details.contains("0 native, 0 fallback, 1 other Spark, 1 wrappers"))
      assert(details.contains("Planning/execution wrapper; excluded from conversion counts."))
      assert(!details.contains("old unsupported reason"))
      assert(details.contains("/history/app-1/SQL/execution/?id=1"))
      assert(view.render(null, "invalid", "").text.contains("Page 1"))
      assert(view.render(null, "-12", "").text.contains("Page 1"))
      assert(view.render("bad", null, "").text.contains("unavailable"))
    } finally kv.close()
  }

  test("execution records retain nested diagnostic data through KVStore serialization") {
    val serializer = new org.apache.spark.status.KVUtils.KVStoreScalaSerializer
    val record = new AuronExecutionUIData(5, "query", 10, 20, Seq(snapshot(5)))
    val restored =
      serializer.deserialize(serializer.serialize(record), classOf[AuronExecutionUIData])
    assert(restored.executionId == 5)
    assert(restored.snapshots == record.snapshots)
    assert(restored.description == record.description)
  }

  test("reused exchanges expose their child without counting a shared subtree twice") {
    val leaf = emptyScan()
    val exchange = org.apache.spark.sql.execution.exchange.BroadcastExchangeExec(
      org.apache.spark.sql.catalyst.plans.physical.IdentityBroadcastMode,
      leaf)
    val reused = org.apache.spark.sql.execution.exchange.ReusedExchangeExec(Nil, exchange)
    reused.setTagValue(AuronConvertStrategy.neverConvertReasonTag, "unsupported wrapper")
    val nodes = AuronPlanDiagnostics.collect(UnionExec(Seq(reused, exchange)))
    assert(nodes.size == 4)
    assert(nodes.count(_.name == leaf.nodeName) == 1)
    assert(nodes.find(_.name == reused.nodeName).get.status == "Wrapper")
    assert(nodes.find(_.name == reused.nodeName).get.reason.isEmpty)
    assert(
      reused
        .getTagValue(AuronConvertStrategy.neverConvertReasonTag)
        .contains("unsupported wrapper"))
  }

  test(
    "Auron execution wrappers are excluded from native counts and preserve child diagnostics") {
    val leaf = emptyScan()
    leaf.setTagValue(AuronConvertStrategy.neverConvertReasonTag, "test fallback")
    val native = org.apache.spark.sql.execution.auron.plan.NativeUnionExec(Seq(leaf), Nil)
    val wrapper = ForceNativeExecutionWrapper(native)
    val nodes = AuronPlanDiagnostics.collect(UnionExec(Seq(wrapper, native)))
    assert(nodes.size == 4)
    assert(nodes.count(_.status == "Native") == 1)
    assert(nodes.count(_.status == "Wrapper") == 1)
    assert(
      nodes
        .find(_.name == wrapper.nodeName)
        .exists(n => n.status == "Wrapper" && n.reason.isEmpty))
    assert(nodes.count(_.reason == "test fallback") == 1)
  }

}
