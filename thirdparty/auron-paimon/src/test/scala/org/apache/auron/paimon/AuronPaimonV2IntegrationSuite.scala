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

import org.apache.paimon.spark.PaimonInputPartition
import org.apache.paimon.table.source.DataSplit
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.auron.paimon.PaimonScanSupport
import org.apache.spark.sql.execution.auron.plan.NativePaimonV2TableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
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

  test("paimon v2 native scan supports native filter on renamed output columns") {
    withTable("paimon.db.t_rename_columns") {
      sql("create table paimon.db.t_rename_columns (id int, v string) using paimon")
      sql("insert into paimon.db.t_rename_columns values (1, 'a'), (2, 'b')")

      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        val df = sql("select id from paimon.db.t_rename_columns where id = 1")
        assertNativePaimonScanApplied(df)

        val plan = df.queryExecution.executedPlan
        assert(
          plan.exists(_.nodeName == "NativeFilter"),
          s"expected a native filter consuming the Paimon scan:\n$plan")

        checkAnswer(df, Seq(Row(1)))
      }
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

  test("paimon v2 native scan supports file-level metadata columns") {
    withTable("paimon.db.t_metadata") {
      sql("create table paimon.db.t_metadata (id int, v string) using paimon")
      sql("insert into paimon.db.t_metadata values (1, 'a')")

      checkSparkAnswerAndNativePaimonScan(
        "select id, __paimon_file_path, __paimon_bucket from paimon.db.t_metadata")
    }
  }

  test("paimon v2 native scan supports metadata-only projection") {
    withTable("paimon.db.t_metadata_only") {
      sql("create table paimon.db.t_metadata_only (id int, v string) using paimon")
      sql("insert into paimon.db.t_metadata_only values (1, 'a'), (2, 'b')")
      sql("insert into paimon.db.t_metadata_only values (3, 'c'), (4, 'd')")

      withSQLConf(
        "spark.sql.files.maxPartitionBytes" -> "256",
        "spark.sql.files.openCostInBytes" -> "1") {
        checkSparkAnswerAndNativePaimonScan(
          "select __paimon_file_path from paimon.db.t_metadata_only")
      }
    }
  }

  test(
    "paimon v2 native scan planning supports metadata across multiple data files in one split") {
    withTable("paimon.db.t_metadata_multi_file_split") {
      sql("""
            |create table paimon.db.t_metadata_multi_file_split (id int, v string)
            |using paimon
            |tblproperties (
            |  'source.split.target-size' = '1 gb',
            |  'source.split.open-file-cost' = '1 b'
            |)
            |""".stripMargin)
      withSQLConf("spark.sql.shuffle.partitions" -> "2") {
        spark
          .range(0, 20)
          .repartition(2)
          .selectExpr("cast(id as int) as id", "cast(id as string) as v")
          .writeTo("paimon.db.t_metadata_multi_file_split")
          .append()
      }

      val sqlText =
        "select id, v, __paimon_file_path, __paimon_bucket " +
          "from paimon.db.t_metadata_multi_file_split"
      withSQLConf("spark.sql.files.minPartitionNum" -> "1") {
        var expected: Seq[Row] = Nil
        withSQLConf("spark.auron.enable.paimon.scan" -> "false") {
          expected = sql(sqlText).collect().toSeq
        }
        assert(
          expected.map(_.getString(2)).distinct.size > 1,
          s"expected rows from multiple data files, got $expected")

        val df = sql(sqlText)
        checkAnswer(df, expected)
        val plan = df.queryExecution.sparkPlan
        val batchScan = plan.collectFirst { case scan: BatchScanExec => scan }
        assert(batchScan.nonEmpty, s"expected BatchScanExec in spark plan:\n$plan")
        val splits = paimonDataSplits(batchScan.get)
        assert(
          splits.exists(_.dataFiles().size() > 1),
          s"expected at least one Paimon split with multiple data files, got " +
            s"${splits.map(_.dataFiles().size()).mkString("[", ", ", "]")}")

        val scanPlan = PaimonScanSupport.plan(batchScan.get)
        assert(scanPlan.nonEmpty, s"expected native Paimon scan plan for:\n$batchScan")
        val filePathIndex = scanPlan.get.partitionSchema.fieldIndex("__paimon_file_path")
        val plannedFilePaths = scanPlan.get.files.map { file =>
          file.partitionValues.getUTF8String(filePathIndex).toString
        }
        assert(
          plannedFilePaths.distinct.size > 1,
          s"expected per-file metadata paths in native scan plan, got $plannedFilePaths")
      }
    }
  }

  test("paimon v2 native scan reads physical columns that share metadata names") {
    withTable("paimon.db.t_metadata_name_collision") {
      sql("""
            |create table paimon.db.t_metadata_name_collision
            |(`__paimon_bucket` int, id int)
            |using paimon
            |""".stripMargin)
      sql("insert into paimon.db.t_metadata_name_collision values (10, 1), (20, 2)")

      checkSparkAnswerAndNativePaimonScan(
        "select `__paimon_bucket`, id from paimon.db.t_metadata_name_collision")
    }
  }

  test("paimon v2 native scan reads partition columns that share metadata names") {
    withTable("paimon.db.t_metadata_partition_name_collision") {
      sql("""
            |create table paimon.db.t_metadata_partition_name_collision
            |(`__paimon_bucket` int, `__paimon_file_path` string, id int)
            |using paimon
            |partitioned by (`__paimon_bucket`, `__paimon_file_path`)
            |""".stripMargin)
      sql("""
            |insert into paimon.db.t_metadata_partition_name_collision values
            |(10, 'path-a', 1),
            |(20, 'path-b', 2)
            |""".stripMargin)

      checkSparkAnswerAndNativePaimonScan(
        "select `__paimon_bucket`, `__paimon_file_path`, id " +
          "from paimon.db.t_metadata_partition_name_collision")
    }
  }

  test("paimon v2 native scan supports metadata columns with table partitions") {
    withTable("paimon.db.t_metadata_part") {
      sql("""
            |create table paimon.db.t_metadata_part (id int, v string, p string)
            |using paimon
            |partitioned by (p)
            |""".stripMargin)
      sql("insert into paimon.db.t_metadata_part values (1, 'a', 'p1'), (2, 'b', '50%')")

      checkSparkAnswerAndNativePaimonScan(
        "select p, __paimon_file_path, id, __paimon_bucket from paimon.db.t_metadata_part")
    }
  }

  test("paimon v2 native scan supports non-zero bucket metadata columns") {
    withTable("paimon.db.t_metadata_bucketed") {
      sql("""
            |create table paimon.db.t_metadata_bucketed (id int, v string)
            |using paimon
            |tblproperties (
            |  'primary-key' = 'id',
            |  'bucket' = '2',
            |  'full-compaction.delta-commits' = '1'
            |)
            |""".stripMargin)
      sql(
        "insert into paimon.db.t_metadata_bucketed " +
          "select cast(id as int), cast(id as string) from range(0, 100)")

      var expected: Seq[Row] = Nil
      withSQLConf("spark.auron.enable.paimon.scan" -> "false") {
        expected =
          sql("select id, __paimon_bucket from paimon.db.t_metadata_bucketed").collect().toSeq
      }
      assert(
        expected.exists(_.getInt(1) != 0),
        s"expected at least one non-zero Paimon bucket, got $expected")

      val df = sql("select id, __paimon_bucket from paimon.db.t_metadata_bucketed")
      checkAnswer(df, expected)
      assertNativePaimonScanApplied(df)
    }
  }

  test("paimon v2 native scan falls back for unsupported metadata columns") {
    withTable("paimon.db.t_metadata_unsupported") {
      sql("create table paimon.db.t_metadata_unsupported (id int, v string) using paimon")
      sql("insert into paimon.db.t_metadata_unsupported values (1, 'a')")

      val df = sql("select id, __paimon_row_index from paimon.db.t_metadata_unsupported")
      val plan = df.queryExecution.executedPlan.toString()

      assert(!plan.contains("NativePaimonV2TableScan"))
      assert(df.collect().length === 1)
    }
  }

  private def assertNativePaimonScanApplied(df: DataFrame): Unit = {
    val plan = df.queryExecution.executedPlan.toString()
    assert(
      plan.contains("NativePaimonV2TableScan"),
      s"plan should use native paimon scan:\n$plan")
  }

  private def checkSparkAnswerAndNativePaimonScan(sqlText: String): Unit = {
    var expected: Seq[Row] = Nil
    withSQLConf("spark.auron.enable.paimon.scan" -> "false") {
      expected = sql(sqlText).collect().toSeq
    }

    val df = sql(sqlText)
    checkAnswer(df, expected)
    assertNativePaimonScanApplied(df)
  }

  private def executedNativeScan(df: DataFrame): NativePaimonV2TableScanExec = {
    val plan = df.queryExecution.executedPlan
    val nativeScan = plan.collectFirst { case scan: NativePaimonV2TableScanExec => scan }
    assert(nativeScan.nonEmpty, s"expected NativePaimonV2TableScanExec in executed plan:\n$plan")
    nativeScan.get
  }

  private def paimonDataSplits(batchScan: BatchScanExec): Seq[DataSplit] = {
    batchScan.scan.toBatch.planInputPartitions().toSeq.flatMap { partition =>
      assert(
        partition.isInstanceOf[PaimonInputPartition],
        s"expected Paimon input partition, got $partition")
      partition
        .asInstanceOf[PaimonInputPartition]
        .splits
        .collect { case split: DataSplit => split }
    }
  }
}
