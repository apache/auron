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
package org.apache.auron.exec

import java.io.File

import org.apache.hadoop.hive.ql.io.orc.{OrcInputFormat, OrcOutputFormat, OrcSerde}
import org.apache.spark.sql.{AuronQueryTest, Row}
import org.apache.spark.sql.auron.AuronConverters
import org.apache.spark.sql.auron.AuronConvertStrategy.neverConvertReasonTag
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.auron.plan.{NativeOrcInsertIntoHiveTableBase, NativeSortBase}
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import org.apache.auron.BaseAuronHiveSQLSuite

class AuronHiveExecSuite extends AuronQueryTest with BaseAuronHiveSQLSuite {

  private def withSqlExecutionId[T](body: => T): T = {
    val sparkContext = spark.sparkContext
    val previousExecutionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, System.nanoTime().toString)
    try {
      body
    } finally {
      sparkContext.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, previousExecutionId)
    }
  }

  private def buildOrcTable(
      tableName: String,
      schema: org.apache.spark.sql.types.StructType,
      partitionColumnNames: Seq[String] = Nil): CatalogTable = {
    val tableLocation = new File(warehouseDir, s"${spark.catalog.currentDatabase}.db/$tableName")
    CatalogTable(
      identifier = TableIdentifier(tableName, Some(spark.catalog.currentDatabase)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(tableLocation.toURI),
        inputFormat = Some(classOf[OrcInputFormat].getName),
        outputFormat = Some(classOf[OrcOutputFormat].getName),
        serde = Some(classOf[OrcSerde].getName)),
      schema = schema,
      provider = None,
      partitionColumnNames = partitionColumnNames)
  }

  private def buildOrcInsertExec(
      table: CatalogTable,
      queryDf: org.apache.spark.sql.DataFrame,
      partition: Map[String, Option[String]] = Map.empty,
      overwrite: Boolean = false): DataWritingCommandExec = {
    val analyzedQuery = queryDf.queryExecution.analyzed
    val partitionColumns = table.partitionSchema.fields.toSeq.map { field =>
      AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()
    }
    val outputColumnNames = queryDf.columns.toSeq
    val ctor = classOf[InsertIntoHiveTable].getConstructors
      .find(c => c.getParameterCount == 11 || c.getParameterCount == 6)
      .getOrElse(
        throw new IllegalStateException(s"Unsupported InsertIntoHiveTable constructor count: " +
          classOf[InsertIntoHiveTable].getConstructors.map(_.getParameterCount).mkString(",")))
    val args: Seq[Object] =
      if (ctor.getParameterCount == 11) {
        Seq(
          table,
          partition,
          analyzedQuery,
          Boolean.box(overwrite),
          Boolean.box(false),
          outputColumnNames,
          partitionColumns,
          None,
          Map.empty[String, String],
          new OrcFileFormat,
          null)
      } else {
        Seq(
          table,
          partition,
          analyzedQuery,
          Boolean.box(overwrite),
          Boolean.box(false),
          outputColumnNames)
      }
    val cmd = ctor.newInstance(args: _*).asInstanceOf[InsertIntoHiveTable]
    DataWritingCommandExec(cmd, queryDf.queryExecution.sparkPlan)
  }

  private def createHiveOrcTable(
      tableName: String,
      schema: StructType,
      partitionColumnNames: Seq[String]): CatalogTable = {
    val table = buildOrcTable(tableName, schema, partitionColumnNames)
    spark.sharedState.externalCatalog.createTable(table, ignoreIfExists = false)
    spark.sessionState.catalog.getTableMetadata(
      TableIdentifier(tableName, Some(spark.catalog.currentDatabase)))
  }

  test("convert ORC InsertIntoHiveTable to native ORC insert") {
    withSQLConf("spark.auron.enable.data.writing" -> "true") {
      withTable("src_orc_insert") {
        sql("""
          |create table src_orc_insert using parquet as
          |select 1 as id, 'a' as v
          |union all
          |select 2 as id, 'b' as v
          |""".stripMargin)

        val queryDf = sql("select id, v from src_orc_insert")
        val exec = buildOrcInsertExec(buildOrcTable("t_orc_native", queryDf.schema), queryDf)
        val converted = AuronConverters.convertDataWritingCommandExec(exec)

        assert(converted.isInstanceOf[NativeOrcInsertIntoHiveTableBase], converted.toString)
      }
    }
  }

  test("convert ORC InsertIntoHiveTable with dynamic partitions to native ORC insert") {
    withSQLConf(
      "spark.auron.enable.data.writing" -> "true",
      "hive.exec.dynamic.partition" -> "true",
      "hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withTable("src_orc_insert_part") {
        sql("""
          |create table src_orc_insert_part using parquet as
          |select 1 as id, 'a' as v, 'p1' as part
          |union all
          |select 2 as id, 'b' as v, 'p2' as part
          |""".stripMargin)

        val queryDf = sql("select id, v, part from src_orc_insert_part")
        val exec = buildOrcInsertExec(
          buildOrcTable("t_orc_native_part", queryDf.schema, Seq("part")),
          queryDf,
          partition = Map("part" -> None))
        val converted = AuronConverters.convertDataWritingCommandExec(exec)

        assert(converted.isInstanceOf[NativeOrcInsertIntoHiveTableBase], converted.toString)
        assert(collect(converted) { case e: NativeSortBase => e }.nonEmpty, converted.toString)
      }
    }
  }

  test(
    "convert ORC InsertIntoHiveTable with static and dynamic partitions to native ORC insert") {
    withSQLConf(
      "spark.auron.enable.data.writing" -> "true",
      "hive.exec.dynamic.partition" -> "true") {
      withTable("src_orc_insert_mixed_part") {
        sql("""
          |create table src_orc_insert_mixed_part using parquet as
          |select 1 as id, 'a' as v, 'p1' as part
          |union all
          |select 2 as id, 'b' as v, 'p2' as part
          |""".stripMargin)

        val queryDf = sql("select id, v, part from src_orc_insert_mixed_part")
        val mixedPartitionSchema = StructType(
          queryDf.schema.fields.filterNot(_.name == "part") ++
            Seq(StructField("ds", StringType, nullable = true), queryDf.schema("part")))
        val exec = buildOrcInsertExec(
          buildOrcTable("t_orc_native_mixed_part", mixedPartitionSchema, Seq("ds", "part")),
          queryDf,
          partition = Map("ds" -> Some("2026-04-13"), "part" -> None))
        val converted = AuronConverters.convertDataWritingCommandExec(exec)

        assert(converted.isInstanceOf[NativeOrcInsertIntoHiveTableBase], converted.toString)
        assert(collect(converted) { case e: NativeSortBase => e }.nonEmpty, converted.toString)
      }
    }
  }

  test("execute native ORC InsertIntoHiveTable without partitions") {
    withSQLConf("spark.auron.enable.data.writing" -> "true") {
      withTable("src_orc_insert_no_part_exec", "t_orc_native_no_part_exec") {
        sql("""
          |create table src_orc_insert_no_part_exec using parquet as
          |select 1 as id, 'a' as v
          |union all
          |select 2 as id, 'b' as v
          |""".stripMargin)

        val queryDf = sql("select id, v from src_orc_insert_no_part_exec")
        val targetTable = createHiveOrcTable("t_orc_native_no_part_exec", queryDf.schema, Nil)
        val exec = buildOrcInsertExec(targetTable, queryDf)
        val converted = AuronConverters.convertDataWritingCommandExec(exec)
        val plan = stripAQEPlan(converted)

        assert(
          collect(plan) { case e: NativeOrcInsertIntoHiveTableBase => e }.size == 1,
          plan.toString)

        withSqlExecutionId {
          converted.executeCollect()
        }
        val actualRows = spark.read
          .orc(targetTable.storage.locationUri.get.toString)
          .selectExpr("id", "v")
          .collect()
          .sortBy(_.getInt(0))
          .toSeq
        val expectedRows = Seq(Row(1, "a"), Row(2, "b"))
        assert(actualRows == expectedRows, s"actualRows=$actualRows expectedRows=$expectedRows")
      }
    }
  }

  test("execute native ORC InsertIntoHiveTable with static and dynamic partitions") {
    withSQLConf(
      "spark.auron.enable.data.writing" -> "true",
      "hive.exec.dynamic.partition" -> "true") {
      withTable("src_orc_insert_exec", "t_orc_native_exec") {
        sql("""
          |create table src_orc_insert_exec using parquet as
          |select 1 as id, 'a' as v, 'p1' as part
          |union all
          |select 2 as id, 'b' as v, 'p2' as part
          |""".stripMargin)

        val queryDf = sql("select id, v, part from src_orc_insert_exec")
        val mixedPartitionSchema = StructType(
          queryDf.schema.fields.filterNot(_.name == "part") ++
            Seq(StructField("ds", StringType, nullable = true), queryDf.schema("part")))
        val targetTable =
          createHiveOrcTable("t_orc_native_exec", mixedPartitionSchema, Seq("ds", "part"))
        val exec = buildOrcInsertExec(
          targetTable,
          queryDf,
          partition = Map("ds" -> Some("2026-04-13"), "part" -> None))
        val converted = AuronConverters.convertDataWritingCommandExec(exec)
        val plan = stripAQEPlan(converted)

        assert(
          collect(plan) { case e: NativeOrcInsertIntoHiveTableBase => e }.size == 1,
          plan.toString)
        assert(collect(plan) { case e: NativeSortBase => e }.nonEmpty, plan.toString)

        withSqlExecutionId {
          converted.executeCollect()
        }
        val actualRows = spark.read
          .orc(targetTable.storage.locationUri.get.toString)
          .selectExpr("id", "v", "cast(ds as string) as ds", "part")
          .collect()
          .sortBy(row => (row.getInt(0), row.getString(3)))
          .toSeq
        val expectedRows = Seq(Row(1, "a", "2026-04-13", "p1"), Row(2, "b", "2026-04-13", "p2"))
        assert(actualRows == expectedRows, s"actualRows=$actualRows expectedRows=$expectedRows")
      }
    }
  }

  test("execute native ORC InsertIntoHiveTable preserves overwrite semantics") {
    withSQLConf("spark.auron.enable.data.writing" -> "true") {
      withTable(
        "src_orc_insert_overwrite_initial",
        "src_orc_insert_overwrite_replacement",
        "t_orc_native_overwrite") {
        sql("""
          |create table src_orc_insert_overwrite_initial using parquet as
          |select 1 as id, 'old-a' as v
          |union all
          |select 2 as id, 'old-b' as v
          |""".stripMargin)
        sql("""
          |create table src_orc_insert_overwrite_replacement using parquet as
          |select 3 as id, 'new-c' as v
          |""".stripMargin)

        val initialDf = sql("select id, v from src_orc_insert_overwrite_initial")
        val targetTable = createHiveOrcTable("t_orc_native_overwrite", initialDf.schema, Nil)
        val appendExec = AuronConverters.convertDataWritingCommandExec(
          buildOrcInsertExec(targetTable, initialDf, overwrite = false))

        withSqlExecutionId {
          appendExec.executeCollect()
        }
        val appendedRows = spark.read
          .orc(targetTable.storage.locationUri.get.toString)
          .selectExpr("id", "v")
          .collect()
          .sortBy(_.getInt(0))
          .toSeq
        assert(appendedRows == Seq(Row(1, "old-a"), Row(2, "old-b")), appendedRows)

        val replacementDf = sql("select id, v from src_orc_insert_overwrite_replacement")
        val overwriteExec = AuronConverters.convertDataWritingCommandExec(
          buildOrcInsertExec(targetTable, replacementDf, overwrite = true))
        val overwritePlan = stripAQEPlan(overwriteExec)
        assert(
          collect(overwritePlan) { case e: NativeOrcInsertIntoHiveTableBase => e }.size == 1,
          overwritePlan.toString)

        withSqlExecutionId {
          overwriteExec.executeCollect()
        }
        val overwrittenRows = spark.read
          .orc(targetTable.storage.locationUri.get.toString)
          .selectExpr("id", "v")
          .collect()
          .sortBy(_.getInt(0))
          .toSeq
        assert(overwrittenRows == Seq(Row(3, "new-c")), overwrittenRows)
      }
    }
  }

  test("keep unsupported ORC InsertIntoHiveTable schema on Spark path") {
    withSQLConf("spark.auron.enable.data.writing" -> "true") {
      withTable("src_orc_insert_map") {
        sql("""
            |create table src_orc_insert_map using parquet as
            |select map('a', 1, 'b', 2) as m
            |""".stripMargin)

        val queryDf = sql("select m from src_orc_insert_map")
        val exec = buildOrcInsertExec(buildOrcTable("t_orc_native_map", queryDf.schema), queryDf)
        val converted = AuronConverters.convertSparkPlan(exec)

        assert(!converted.isInstanceOf[NativeOrcInsertIntoHiveTableBase], converted.toString)
        val reason = exec.getTagValue(neverConvertReasonTag).getOrElse("")
        assert(reason.contains("Unsupported ORC write schema"), reason)
        assert(reason.contains("m:") && reason.contains("map"), reason)
      }
    }
  }
}
