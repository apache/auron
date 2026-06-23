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
package org.apache.auron.paimon

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.execution.auron.plan.NativePaimonV2TableScanExec
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates

class AuronPaimonV2IntegrationSuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronPaimonSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("create database if not exists paimon.db")
  }

  test("paimon v2 native scan runs simple append-only select") {
    withTable("paimon.db.t1") {
      sql("create table paimon.db.t1 (id int, v string) using paimon")
      sql("insert into paimon.db.t1 values (1, 'a'), (2, 'b')")
      val df = sql("select * from paimon.db.t1")
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))
      assertNativePaimonScanApplied(df)
    }
  }

  test("paimon v2 native scan supports projection") {
    withTable("paimon.db.t_proj") {
      sql("create table paimon.db.t_proj (id int, v string) using paimon")
      sql("insert into paimon.db.t_proj values (1, 'a'), (2, 'b')")
      val df = sql("select id from paimon.db.t_proj")
      checkAnswer(df, Seq(Row(1), Row(2)))
      assertNativePaimonScanApplied(df)
    }
  }

  test("paimon v2 native scan supports partitioned table with predicate") {
    withTable("paimon.db.t_part") {
      sql("""
            |create table paimon.db.t_part (id int, v string, p string)
            |using paimon
            |partitioned by (p)
            |""".stripMargin)
      sql("insert into paimon.db.t_part values (1, 'a', 'p1'), (2, 'b', 'p2')")
      val df = sql("select * from paimon.db.t_part where p = 'p1'")
      checkAnswer(df, Seq(Row(1, "a", "p1")))
      assertNativePaimonScanApplied(df)
    }
  }

  test("paimon v2 native scan supports non-string partition columns") {
    withTable("paimon.db.t_part_typed") {
      sql("""
            |create table paimon.db.t_part_typed (id int, v string, dt date, pid int)
            |using paimon
            |partitioned by (dt, pid)
            |""".stripMargin)
      sql("""
            |insert into paimon.db.t_part_typed values
            |(1, 'a', date'2023-01-01', 10),
            |(2, 'b', date'2023-01-02', 20)
            |""".stripMargin)
      val df = sql("select id, v, dt, pid from paimon.db.t_part_typed where pid = 10")
      checkAnswer(df, Seq(Row(1, "a", java.sql.Date.valueOf("2023-01-01"), 10)))
      assertNativePaimonScanApplied(df)
    }
  }

  test("paimon v2 native scan supports ORC COW table") {
    withTable("paimon.db.t_orc") {
      sql("""
            |create table paimon.db.t_orc (id int, v string)
            |using paimon
            |tblproperties ('file.format' = 'orc')
            |""".stripMargin)
      sql("insert into paimon.db.t_orc values (1, 'a'), (2, 'b')")
      val df = sql("select * from paimon.db.t_orc")
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))
      assertNativePaimonScanApplied(df)
    }
  }

  test("paimon v2 native scan supports COW primary-key table") {
    withTable("paimon.db.t_cow") {
      sql("""
            |create table paimon.db.t_cow (id int, v string)
            |using paimon
            |tblproperties (
            |  'primary-key' = 'id',
            |  'bucket' = '2',
            |  'full-compaction.delta-commits' = '1'
            |)
            |""".stripMargin)
      sql("insert into paimon.db.t_cow values (1, 'a'), (2, 'b')")
      val df = sql("select * from paimon.db.t_cow")
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))
      assertNativePaimonScanApplied(df)
    }
  }

  test("paimon v2 native scan handles empty table") {
    withTable("paimon.db.t_empty") {
      sql("create table paimon.db.t_empty (id int, v string) using paimon")
      val df = sql("select * from paimon.db.t_empty")
      checkAnswer(df, Seq.empty)
      assertNativePaimonScanApplied(df)
    }
  }

  test("paimon v2 scan exposes file scan driver metrics") {
    withTable("paimon.db.t_metrics") {
      sql("create table paimon.db.t_metrics (id int, v string) using paimon")
      sql("insert into paimon.db.t_metrics values (1, 'a')")
      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        val df = sql("select * from paimon.db.t_metrics")
        val nativeScan = executedNativeScan(df)
        val metricIds = Map(
          "numPartitions" -> nativeScan.metrics("numPartitions").id,
          "numFiles" -> nativeScan.metrics("numFiles").id)
        val driverMetricUpdates = new ConcurrentLinkedQueue[(Long, Long)]()
        val driverMetricUpdatesPosted = new CountDownLatch(1)
        val listener = new SparkListener {
          override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
            case SparkListenerDriverAccumUpdates(_, updates) =>
              updates.foreach { case (metricId, value) =>
                driverMetricUpdates.add(metricId -> value)
              }
              val updatedMetricIds = driverMetricUpdates.iterator().asScala.map(_._1).toSet
              if (metricIds.values.forall(updatedMetricIds.contains)) {
                driverMetricUpdatesPosted.countDown()
              }
            case _ =>
          }
        }

        spark.sparkContext.addSparkListener(listener)
        try {
          checkAnswer(df, Seq(Row(1, "a")))
          assert(driverMetricUpdatesPosted.await(30, TimeUnit.SECONDS))
        } finally {
          spark.sparkContext.removeSparkListener(listener)
        }

        val driverMetricValues = driverMetricUpdates
          .iterator()
          .asScala
          .toSeq
          .groupBy(_._1)
          .mapValues(_.map(_._2).sum)
          .toMap
        assert(driverMetricValues.getOrElse(metricIds("numPartitions"), 0L) > 0)
        assert(driverMetricValues.getOrElse(metricIds("numFiles"), 0L) > 0)
      }
    }
  }

  test("paimon v2 native scan falls back when spark.auron.enable.paimon.scan=false") {
    withTable("paimon.db.t_disable") {
      sql("create table paimon.db.t_disable (id int, v string) using paimon")
      sql("insert into paimon.db.t_disable values (1, 'a')")
      withSQLConf("spark.auron.enable.paimon.scan" -> "false") {
        val df = sql("select * from paimon.db.t_disable")
        val plan = df.queryExecution.executedPlan.toString()
        assert(!plan.contains("NativePaimonV2TableScan"))
        // Ensure the Spark fallback path still returns correct results.
        checkAnswer(df, Seq(Row(1, "a")))
      }
    }
  }

  test("paimon v2 native scan falls back for MOR (merge-on-read) table") {
    withTable("paimon.db.t_mor") {
      sql("""
            |create table paimon.db.t_mor (id int, v string)
            |using paimon
            |tblproperties (
            |  'primary-key' = 'id',
            |  'bucket' = '2'
            |)
            |""".stripMargin)
      sql("insert into paimon.db.t_mor values (1, 'a'), (2, 'b')")
      val df = sql("select * from paimon.db.t_mor")
      val plan = df.queryExecution.executedPlan.toString()
      assert(!plan.contains("NativePaimonV2TableScan"))
      // Ensure the Spark fallback path still returns correct results.
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))
    }
  }

  private def assertNativePaimonScanApplied(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    assert(
      plan.contains("NativePaimonV2TableScan"),
      s"plan should use native paimon scan:\n$plan")
  }

  private def executedNativeScan(df: DataFrame): NativePaimonV2TableScanExec = {
    val nativeScan = df.queryExecution.executedPlan.collectFirst {
      case scan: NativePaimonV2TableScanExec => scan
    }
    assert(nativeScan.nonEmpty, "expected NativePaimonV2TableScanExec in executed plan")
    nativeScan.get
  }
}
