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

import scala.collection.JavaConverters._

import org.apache.iceberg.{FileFormat, FileScanTask}
import org.apache.iceberg.data.{GenericAppenderFactory, Record}
import org.apache.iceberg.deletes.PositionDelete
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.auron.iceberg.IcebergScanSupport
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec

class AuronIcebergIntegrationSuite
    extends org.apache.spark.sql.QueryTest
    with BaseAuronIcebergSuite {

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
}
