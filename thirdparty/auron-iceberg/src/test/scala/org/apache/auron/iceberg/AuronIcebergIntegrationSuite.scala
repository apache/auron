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
package org.apache.auron.iceberg

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

import org.apache.iceberg.{FileFormat, FileScanTask, MetadataColumns}
import org.apache.iceberg.data.{GenericAppenderFactory, Record}
import org.apache.iceberg.deletes.PositionDelete
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.auron.iceberg.IcebergScanSupport
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.ExplainUtils.collectFirst
import org.apache.spark.sql.execution.auron.plan.NativeIcebergTableScanExec
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates

class AuronIcebergIntegrationSuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronIcebergSuite {

  test("iceberg native scan with auron.enable.iceberg.scan=false") {
    withTable("local.db.t2") {
      withSQLConf("spark.auron.enable" -> "true", "spark.auron.enable.iceberg.scan" -> "false") {
        sql("create table local.db.t2 using iceberg as select 1 as id, 'a' as v")
        val df = sql("select * from local.db.t2")
        df.collect()
        val neverConvertReasonTag: TreeNodeTag[String] = TreeNodeTag("auron.never.convert.reason")
        assert(collectFirst(df.queryExecution.executedPlan) { case batchScanExec: BatchScanExec =>
          batchScanExec.getTagValue(neverConvertReasonTag)
        }.get.get.equals("Conversion disabled: auron.enable.iceberg.scan=false."))
      }
    }
  }

  test(
    "iceberg scan falls back when reading unsupported metadata columns and check never convert reason") {
    withTable("local.db.t4_pos") {
      sql("create table local.db.t4_pos using iceberg as select 1 as id, 'a' as v")
      withSQLConf("spark.auron.enable" -> "true", "spark.auron.enable.iceberg.scan" -> "true") {
        val df = sql("select _pos from local.db.t4_pos")
        df.collect()
        val neverConvertReasonTag: TreeNodeTag[String] = TreeNodeTag("auron.never.convert.reason")
        assert(collectFirst(df.queryExecution.executedPlan) { case batchScanExec: BatchScanExec =>
          batchScanExec.getTagValue(neverConvertReasonTag)
        }.get.get.equals("Has per-row materialization (for example _pos)."))
      }
    }
  }

  test("test iceberg integrate ") {
    withTable("local.db.t1") {
      sql(
        "create table local.db.t1 using iceberg PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val df = sql("select * from local.db.t1")
      checkAnswer(df, Seq(Row(1, 2, "test test")))
    }
  }

  test("iceberg native scan is applied for simple COW table") {
    withTable("local.db.t2") {
      sql("create table local.db.t2 using iceberg as select 1 as id, 'a' as v")
      val df = sql("select * from local.db.t2")
      df.collect()
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg native scan exposes file scan driver metrics") {
    withTable("local.db.t_metrics") {
      sql("create table local.db.t_metrics using iceberg as select 1 as id, 'a' as v")
      withSQLConf("spark.sql.adaptive.enabled" -> "false") {
        val df = sql("select * from local.db.t_metrics")
        val nativeScan = executedNativeIcebergTableScanExec(df)
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

  test("iceberg native scan is applied for empty COW table") {
    withTable("local.db.t_empty") {
      sql("""
            |create table local.db.t_empty (id int, v string)
            |using iceberg
            |tblproperties (
            |  'format-version' = '2'
            |)
            |""".stripMargin)
      val df = sql("select * from local.db.t_empty")
      checkAnswer(df, Seq.empty)
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg native scan is applied for projection on COW table") {
    withTable("local.db.t3") {
      sql("create table local.db.t3 using iceberg as select 1 as id, 'a' as v")
      val df = sql("select id from local.db.t3")
      checkAnswer(df, Seq(Row(1)))
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg native scan is applied for partitioned COW table with filter") {
    withTable("local.db.t_partition") {
      sql("""
            |create table local.db.t_partition (id int, v string, p string)
            |using iceberg
            |partitioned by (p)
            |""".stripMargin)
      sql("insert into local.db.t_partition values (1, 'a', 'p1'), (2, 'b', 'p2')")
      val df = sql("select * from local.db.t_partition where p = 'p1'")
      checkAnswer(df, Seq(Row(1, "a", "p1")))
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg native scan is applied for ORC COW table") {
    withTable("local.db.t_orc") {
      sql("""
            |create table local.db.t_orc (id int, v string)
            |using iceberg
            |tblproperties ('write.format.default' = 'orc')
            |""".stripMargin)
      sql("insert into local.db.t_orc values (1, 'a'), (2, 'b')")
      val df = sql("select * from local.db.t_orc")
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg native parquet scan reads top-level renamed columns by field id") {
    withTable("local.db.t_rename") {
      sql("create table local.db.t_rename (id int, old_name string) using iceberg")
      sql("insert into local.db.t_rename values (1, 'before')")
      sql("alter table local.db.t_rename rename column old_name to new_name")
      sql("insert into local.db.t_rename values (2, 'after')")

      val df = sql("select id, new_name from local.db.t_rename")
      checkAnswer(df, Seq(Row(1, "before"), Row(2, "after")))
      assert(df.queryExecution.executedPlan.toString().contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg native parquet scan does not reuse a dropped field id for an added column") {
    withTable("local.db.t_drop_add") {
      sql("create table local.db.t_drop_add (id int, value string) using iceberg")
      sql("insert into local.db.t_drop_add values (1, 'old')")
      sql("alter table local.db.t_drop_add drop column value")
      sql("alter table local.db.t_drop_add add column value string")
      sql("insert into local.db.t_drop_add values (2, 'new')")

      val df = sql("select id, value from local.db.t_drop_add")
      checkAnswer(df, Seq(Row(1, null), Row(2, "new")))
      assert(df.queryExecution.executedPlan.toString().contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg ORC scan falls back after a top-level column rename") {
    withTable("local.db.t_orc_rename") {
      sql("""
            |create table local.db.t_orc_rename (id int, old_name string)
            |using iceberg
            |tblproperties ('write.format.default' = 'orc')
            |""".stripMargin)
      sql("insert into local.db.t_orc_rename values (1, 'before')")
      sql("alter table local.db.t_orc_rename rename column old_name to new_name")

      val df = sql("select id, new_name from local.db.t_orc_rename")
      checkAnswer(df, Seq(Row(1, "before")))
      assert(!df.queryExecution.executedPlan.toString().contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg ORC scan falls back after top-level drop and add with the same name") {
    withTable("local.db.t_orc_drop_add") {
      sql("""
            |create table local.db.t_orc_drop_add (id int, value string)
            |using iceberg
            |tblproperties ('write.format.default' = 'orc')
            |""".stripMargin)
      sql("insert into local.db.t_orc_drop_add values (1, 'old')")
      sql("alter table local.db.t_orc_drop_add drop column value")
      sql("alter table local.db.t_orc_drop_add add column value string")
      sql("insert into local.db.t_orc_drop_add values (2, 'new')")

      val df = sql("select id, value from local.db.t_orc_drop_add")
      checkAnswer(df, Seq(Row(1, null), Row(2, "new")))
      assert(!df.queryExecution.executedPlan.toString().contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg ORC scan remains native for additive schema evolution") {
    withTable("local.db.t_orc_add") {
      sql("""
            |create table local.db.t_orc_add (id int)
            |using iceberg
            |tblproperties ('write.format.default' = 'orc')
            |""".stripMargin)
      sql("insert into local.db.t_orc_add values (1)")
      sql("alter table local.db.t_orc_add add column value string")

      val df = sql("select id, value from local.db.t_orc_add")
      checkAnswer(df, Seq(Row(1, null)))
      assert(df.queryExecution.executedPlan.toString().contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg scan falls back after a nested column rename") {
    withTable("local.db.t_nested_rename") {
      sql("""
            |create table local.db.t_nested_rename (
            |  id int,
            |  payload struct<old_name:string>
            |) using iceberg
            |""".stripMargin)
      sql("insert into local.db.t_nested_rename values (1, named_struct('old_name', 'before'))")
      sql("alter table local.db.t_nested_rename rename column payload.old_name to new_name")

      val df = sql("select id, payload.new_name from local.db.t_nested_rename")
      checkAnswer(df, Seq(Row(1, "before")))
      assert(!df.queryExecution.executedPlan.toString().contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg scan falls back when top-level and nested columns are both renamed") {
    withTable("local.db.t_top_and_nested_rename") {
      sql("""
            |create table local.db.t_top_and_nested_rename (
            |  old_id int,
            |  payload struct<old_name:string>
            |) using iceberg
            |""".stripMargin)
      sql("""insert into local.db.t_top_and_nested_rename
          |values (1, named_struct('old_name', 'before'))
          |""".stripMargin)
      sql("alter table local.db.t_top_and_nested_rename rename column old_id to new_id")
      sql("""
            |alter table local.db.t_top_and_nested_rename
            |rename column payload.old_name to new_name
            |""".stripMargin)

      val df =
        sql("select new_id, payload.new_name from local.db.t_top_and_nested_rename")
      checkAnswer(df, Seq(Row(1, "before")))
      assert(!df.queryExecution.executedPlan.toString().contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg native scan is applied when delete files are null (format v1)") {
    withTable("local.db.t_v1") {
      sql("""
            |create table local.db.t_v1 (id int, v string)
            |using iceberg
            |tblproperties ('format-version' = '1')
            |""".stripMargin)
      sql("insert into local.db.t_v1 values (1, 'a'), (2, 'b')")
      val icebergTable = Spark3Util.loadIcebergTable(spark, "local.db.t_v1")
      val scanTasks = icebergTable.newScan().planFiles()
      val allDeletesEmpty =
        try {
          scanTasks
            .iterator()
            .asScala
            .forall(task => task.deletes() == null || task.deletes().isEmpty)
        } finally {
          scanTasks.close()
        }
      assert(allDeletesEmpty)
      val df = sql("select * from local.db.t_v1")
      checkAnswer(df, Seq(Row(1, "a"), Row(2, "b")))
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg scan pushes residual filters into native scan pruning predicates") {
    withTable("local.db.t_residual") {
      sql("create table local.db.t_residual (id int, v string) using iceberg")
      sql("insert into local.db.t_residual values (1, 'a'), (2, 'b')")
      val df = sql("select * from local.db.t_residual where id = 1")
      checkAnswer(df, Seq(Row(1, "a")))
      val nativeScanPlan = icebergScanPlan(df)
      assert(nativeScanPlan.nonEmpty)
      assert(nativeScanPlan.get.pruningPredicates.nonEmpty)
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
      assert(plan.contains("NativeFilter"))
    }
  }

  test("iceberg scan pushes supported IN filters into native scan pruning predicates") {
    withTable("local.db.t_residual_supported") {
      sql("create table local.db.t_residual_supported (id int, v string) using iceberg")
      sql(
        "insert into local.db.t_residual_supported values (1, 'alpha'), (2, 'beta'), (3, 'atom')")
      val df = sql("""
          |select * from local.db.t_residual_supported
          |where id in (1, 3)
          |""".stripMargin)
      checkAnswer(df, Seq(Row(1, "alpha"), Row(3, "atom")))
      val nativeScanPlan = icebergScanPlan(df)
      assert(nativeScanPlan.nonEmpty)
      assert(nativeScanPlan.get.pruningPredicates.nonEmpty)
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
      assert(plan.contains("NativeFilter"))
    }
  }

  test("iceberg scan keeps native post-scan filter when only part of the predicate is pushed") {
    withTable("local.db.t_residual_partial_pushdown") {
      sql("create table local.db.t_residual_partial_pushdown (id int, v string) using iceberg")
      sql(
        "insert into local.db.t_residual_partial_pushdown values (1, 'alpha'), (2, 'beta'), (3, 'atom'), (4, 'delta')")
      val df = sql("""
          |select * from local.db.t_residual_partial_pushdown
          |where id in (1, 2, 3) and id % 2 = 1
          |""".stripMargin)
      checkAnswer(df, Seq(Row(1, "alpha"), Row(3, "atom")))
      val nativeScanPlan = icebergScanPlan(df)
      assert(nativeScanPlan.nonEmpty)
      assert(nativeScanPlan.get.pruningPredicates.nonEmpty)
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
      assert(plan.contains("NativeFilter"))
    }
  }

  test("iceberg scan keeps native string filter outside scan pruning") {
    withTable("local.db.t_residual_string") {
      sql("create table local.db.t_residual_string (id int, v string) using iceberg")
      sql("insert into local.db.t_residual_string values (1, 'a'), (2, 'b'), (3, null)")
      val df = sql("""
          |select * from local.db.t_residual_string
          |where v = 'a'
          |""".stripMargin)
      checkAnswer(df, Seq(Row(1, "a")))
      val nativeScanPlan = icebergScanPlan(df)
      assert(nativeScanPlan.nonEmpty)
      assert(nativeScanPlan.get.pruningPredicates.nonEmpty)
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
      assert(
        plan.contains("NativeFilter"),
        "string equality should remain on the post-scan native filter path")
    }
  }

  test("iceberg native scan supports _file metadata column") {
    withTable("local.db.t4") {
      sql("create table local.db.t4 using iceberg as select 1 as id, 'a' as v")
      checkSparkAnswerAndOperator("select _file from local.db.t4")
    }
  }

  test("iceberg native scan supports _spec_id metadata column") {
    withTable("local.db.t4_spec_id") {
      sql("create table local.db.t4_spec_id using iceberg as select 1 as id, 'a' as v")
      checkSparkAnswerAndOperator("select _spec_id from local.db.t4_spec_id")
    }
  }

  test("iceberg native scan supports data columns with _file and _spec_id metadata columns") {
    withTable("local.db.t4_metadata_mixed") {
      sql("create table local.db.t4_metadata_mixed using iceberg as select 1 as id, 'a' as v")
      checkSparkAnswerAndOperator("select id, _file, _spec_id from local.db.t4_metadata_mixed")
    }
  }

  test("iceberg native scan supports data columns with _file metadata column") {
    withTable("local.db.t4_mixed") {
      sql("create table local.db.t4_mixed using iceberg as select 1 as id, 'a' as v")
      checkSparkAnswerAndOperator("select id, _file from local.db.t4_mixed")
    }
  }

  test("iceberg native scan preserves projected order for _file metadata column") {
    withTable("local.db.t4_metadata_first") {
      sql("create table local.db.t4_metadata_first using iceberg as select 1 as id, 'a' as v")
      checkSparkAnswerAndOperator("select _file, id from local.db.t4_metadata_first")
    }
  }

  test("iceberg native scan supports insert-only changelog scan") {
    withTable("local.db.t_changelog_insert") {
      withTempView("t_changelog_insert_changes") {
        sql("""
              |create table local.db.t_changelog_insert (id int, v string)
              |using iceberg
              |tblproperties ('format-version' = '2')
              |""".stripMargin)
        sql("insert into local.db.t_changelog_insert values (1, 'a')")
        val startSnapshotId = currentSnapshotId("local.db.t_changelog_insert")
        sql("insert into local.db.t_changelog_insert values (2, 'b'), (3, 'c')")
        val endSnapshotId = currentSnapshotId("local.db.t_changelog_insert")
        createChangelogView(
          "local.db.t_changelog_insert",
          "t_changelog_insert_changes",
          startSnapshotId,
          endSnapshotId)

        val df = checkSparkAnswerAndOperator("""
            |select id, v, _change_type, _change_ordinal, _commit_snapshot_id
            |from t_changelog_insert_changes
            |order by id
            |""".stripMargin)
        val nativeScanPlan = icebergScanPlan(df)
        assert(nativeScanPlan.nonEmpty)
        assert(
          nativeScanPlan.get.partitionSchema.fieldNames
            .contains(MetadataColumns.CHANGE_TYPE.name()))
      }
    }
  }

  test("iceberg changelog scan reads renamed columns by field id") {
    withTable("local.db.t_changelog_rename") {
      withTempView("t_changelog_rename_changes") {
        sql("""
              |create table local.db.t_changelog_rename (id int, old_name string)
              |using iceberg
              |tblproperties ('format-version' = '2')
              |""".stripMargin)
        sql("insert into local.db.t_changelog_rename values (0, 'initial')")
        val startSnapshotId = currentSnapshotId("local.db.t_changelog_rename")
        sql("insert into local.db.t_changelog_rename values (1, 'before')")
        sql("alter table local.db.t_changelog_rename rename column old_name to new_name")
        sql("insert into local.db.t_changelog_rename values (2, 'after')")
        val endSnapshotId = currentSnapshotId("local.db.t_changelog_rename")
        createChangelogView(
          "local.db.t_changelog_rename",
          "t_changelog_rename_changes",
          startSnapshotId,
          endSnapshotId)

        checkSparkAnswerAndOperator("""
            |select id, new_name, _change_type, _change_ordinal, _commit_snapshot_id
            |from t_changelog_rename_changes
            |order by id
            |""".stripMargin)
      }
    }
  }

  test("iceberg changelog scan does not reuse dropped field id for an added column") {
    withTable("local.db.t_changelog_drop_add") {
      withTempView("t_changelog_drop_add_changes") {
        sql("""
              |create table local.db.t_changelog_drop_add (id int, value string)
              |using iceberg
              |tblproperties ('format-version' = '2')
              |""".stripMargin)
        sql("insert into local.db.t_changelog_drop_add values (0, 'initial')")
        val startSnapshotId = currentSnapshotId("local.db.t_changelog_drop_add")
        sql("insert into local.db.t_changelog_drop_add values (1, 'old')")
        sql("alter table local.db.t_changelog_drop_add drop column value")
        sql("alter table local.db.t_changelog_drop_add add column value string")
        sql("insert into local.db.t_changelog_drop_add values (2, 'new')")
        val endSnapshotId = currentSnapshotId("local.db.t_changelog_drop_add")
        createChangelogView(
          "local.db.t_changelog_drop_add",
          "t_changelog_drop_add_changes",
          startSnapshotId,
          endSnapshotId)

        checkSparkAnswerAndOperator("""
            |select id, value, _change_type, _change_ordinal, _commit_snapshot_id
            |from t_changelog_drop_add_changes
            |order by id
            |""".stripMargin)
      }
    }
  }

  test("iceberg changelog scan falls back when delete changes exist") {
    withTable("local.db.t_changelog_delete") {
      withTempView("t_changelog_delete_changes") {
        sql("""
              |create table local.db.t_changelog_delete (id int, v string)
              |using iceberg
              |tblproperties ('format-version' = '2')
              |""".stripMargin)
        sql("insert into local.db.t_changelog_delete values (1, 'a'), (2, 'b')")
        val startSnapshotId = currentSnapshotId("local.db.t_changelog_delete")
        sql("delete from local.db.t_changelog_delete where id = 1")
        val endSnapshotId = currentSnapshotId("local.db.t_changelog_delete")
        createChangelogView(
          "local.db.t_changelog_delete",
          "t_changelog_delete_changes",
          startSnapshotId,
          endSnapshotId)

        val query =
          """
            |select id, v, _change_type, _change_ordinal, _commit_snapshot_id
            |from t_changelog_delete_changes
            |order by id, _change_type
            |""".stripMargin
        var expected: Seq[Row] = Nil
        withSQLConf("spark.auron.enable" -> "false") {
          expected = sql(query).collect().toSeq
        }
        withSQLConf("spark.auron.enable" -> "true", "spark.auron.enable.iceberg.scan" -> "true") {
          val df = sql(query)
          checkAnswer(df, expected)
          val plan = df.queryExecution.executedPlan.toString()
          assert(!plan.contains("NativeIcebergTableScan"))
        }
      }
    }
  }

  test("iceberg scan falls back when reading unsupported metadata columns") {
    withTable("local.db.t4_pos") {
      sql("create table local.db.t4_pos using iceberg as select 1 as id, 'a' as v")
      withSQLConf("spark.auron.enable" -> "true", "spark.auron.enable.iceberg.scan" -> "true") {
        val df = sql("select _pos from local.db.t4_pos")
        df.collect()
        val plan = df.queryExecution.executedPlan.toString()
        assert(!plan.contains("NativeIcebergTableScan"))
      }
    }
  }

  test("iceberg scan falls back for unsupported decimal types") {
    withTable("local.db.t5") {
      sql("create table local.db.t5 (id int, amount decimal(38, 10)) using iceberg")
      sql("insert into local.db.t5 values (1, 123.45)")
      val df = sql("select * from local.db.t5")
      checkAnswer(df, Seq(Row(1, new java.math.BigDecimal("123.4500000000"))))
      val plan = df.queryExecution.executedPlan.toString()
      assert(!plan.contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg scan falls back when delete files exist") {
    withTable("local.db.t_delete") {
      sql("""
            |create table local.db.t_delete (id int, v string)
            |using iceberg
            |tblproperties (
            |  'format-version' = '2',
            |  'write.delete.mode' = 'merge-on-read'
            |)
            |""".stripMargin)
      sql("insert into local.db.t_delete values (1, 'a'), (2, 'b')")
      addPositionDeleteFile("local.db.t_delete")
      val icebergTable = Spark3Util.loadIcebergTable(spark, "local.db.t_delete")
      val scanTasks = icebergTable.newScan().planFiles()
      val hasDeletes =
        try {
          scanTasks
            .iterator()
            .asScala
            .exists(task => task.deletes() != null && !task.deletes().isEmpty)
        } finally {
          scanTasks.close()
        }
      assert(hasDeletes)
      val df = sql("select * from local.db.t_delete")
      df.collect()
      val plan = df.queryExecution.executedPlan.toString()
      assert(!plan.contains("NativeIcebergTableScan"))
    }
  }

  test("iceberg scan is disabled via spark.auron.enable.iceberg.scan") {
    withTable("local.db.t_disable") {
      sql("create table local.db.t_disable using iceberg as select 1 as id, 'a' as v")
      withSQLConf("spark.auron.enable.iceberg.scan" -> "false") {
        assert(
          !org.apache.auron.spark.configuration.SparkAuronConfiguration.ENABLE_ICEBERG_SCAN.get())
        val df = sql("select * from local.db.t_disable")
        df.collect()
        val plan = df.queryExecution.executedPlan.toString()
        assert(!plan.contains("NativeIcebergTableScan"))
      }
    }
  }

  private def addPositionDeleteFile(tableName: String): Unit = {
    val table = Spark3Util.loadIcebergTable(spark, tableName)
    val taskIterable = table.newScan().planFiles()
    val taskIter = taskIterable.iterator()
    if (!taskIter.hasNext) {
      taskIterable.close()
      return
    }

    try {
      val task = taskIter.next().asInstanceOf[FileScanTask]
      val deletePath =
        table.locationProvider().newDataLocation(s"delete-${UUID.randomUUID().toString}.parquet")
      val outputFile = table.io().newOutputFile(deletePath)
      val encryptedOutput = table.encryption().encrypt(outputFile)
      val appenderFactory = new GenericAppenderFactory(table.schema(), table.spec())
      val writer =
        appenderFactory.newPosDeleteWriter(encryptedOutput, FileFormat.PARQUET, task.partition())

      val delete = PositionDelete.create[Record]().set(task.file().location(), 0L, null)
      writer.write(delete)
      writer.close()

      val deleteFile = writer.toDeleteFile()
      table.newRowDelta().addDeletes(deleteFile).commit()
    } finally {
      taskIterable.close()
    }
  }

  private def createChangelogView(
      tableName: String,
      viewName: String,
      startSnapshotId: Long,
      endSnapshotId: Long): Unit = {
    val tableIdent = tableName.stripPrefix("local.")
    sql(s"""
         |CALL local.system.create_changelog_view(
         |  table => '$tableIdent',
         |  changelog_view => '$viewName',
         |  options => map(
         |    'start-snapshot-id', '$startSnapshotId',
         |    'end-snapshot-id', '$endSnapshotId'
         |  )
         |)
         |""".stripMargin)
  }

  private def currentSnapshotId(tableName: String): Long =
    Spark3Util.loadIcebergTable(spark, tableName).currentSnapshot().snapshotId()

  private def checkSparkAnswerAndOperator(sqlText: String): DataFrame = {
    var expected: Seq[Row] = Nil
    withSQLConf("spark.auron.enable" -> "false") {
      expected = sql(sqlText).collect().toSeq
    }

    var df: DataFrame = null
    withSQLConf("spark.auron.enable" -> "true", "spark.auron.enable.iceberg.scan" -> "true") {
      df = sql(sqlText)
      checkAnswer(df, expected)
      val plan = df.queryExecution.executedPlan.toString()
      assert(plan.contains("NativeIcebergTableScan"))
    }
    df
  }
  private def icebergScanPlan(df: DataFrame) =
    df.queryExecution.sparkPlan.collectFirst { case scan: BatchScanExec =>
      IcebergScanSupport.plan(scan)
    }.flatten

  private def executedNativeIcebergTableScanExec(df: DataFrame): NativeIcebergTableScanExec = {
    val nativeScan = df.queryExecution.executedPlan.collectFirst {
      case scan: NativeIcebergTableScanExec => scan
    }
    assert(nativeScan.nonEmpty)
    nativeScan.get
  }

  test("native iceberg scan respects SinglePartition for global sort correctness") {
    withTable("local.db.t_global_sort") {
      sql("create table local.db.t_global_sort (id int, value string) using iceberg")
      sql("insert into local.db.t_global_sort values (4, 'd'), (1, 'a')")
      sql("insert into local.db.t_global_sort values (3, 'c'), (2, 'b')")

      val df = sql("select id, value from local.db.t_global_sort order by id")
      val result = df.collect().map(_.getInt(0)).toSeq
      assert(result === Seq(1, 2, 3, 4), s"Global ORDER BY must produce [1,2,3,4], got: $result")
      assert(df.queryExecution.executedPlan.toString().contains("NativeIcebergTableScan"))
      assert(df.rdd.getNumPartitions === 1)
    }
  }
}
