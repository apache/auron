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

import java.nio.file.Files

import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.paimon.table.FileStoreTable
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.auron.AuronConvertStrategy
import org.apache.spark.sql.auron.paimon.PaimonSinkWriter
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec
import org.apache.spark.sql.hive.auron.paimon.PaimonUtil

/**
 * Native write of Paimon append-only tables. Every test writes the same data once with the native
 * sink and once with the Paimon writer and compares the results read back with the Paimon reader,
 * so data file layout differences cannot hide wrong results.
 */
class AuronPaimonWriteSuite extends org.apache.spark.sql.QueryTest with BaseAuronPaimonSuite {

  private val nativeWriteConfs = Seq(
    "spark.paimon.write.use-v2-write" -> "true",
    "spark.auron.enable.data.writing" -> "true",
    "spark.auron.enable.paimon.write" -> "true")

  private val paimonWriteConfs =
    Seq("spark.paimon.write.use-v2-write" -> "true", "spark.auron.enable.paimon.write" -> "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
    sql("create database if not exists paimon.db")
  }

  /**
   * Runs the write statements and returns the names of the executed write nodes. Write nodes that
   * were not converted are reported together with the reason recorded by Auron.
   */
  private def runWrites(statements: Seq[String]): Seq[String] = {
    statements.flatMap { statement =>
      val executed = sql(statement).queryExecution.executedPlan match {
        // eagerly executed commands are wrapped, the real plan is kept for display
        case c: CommandResultExec => c.commandPhysicalPlan
        case p => p
      }
      executed.collect {
        case p: V2ExistingTableWriteExec =>
          // not converted: report the reason Auron recorded for the fallback
          val reason = p.getTagValue(AuronConvertStrategy.neverConvertReasonTag).getOrElse("")
          s"${p.nodeName}[$reason]"
        case p if p.nodeName.contains("NativePaimonWrite") => p.nodeName
      }
    }
  }

  private def read(table: String): DataFrame = {
    var rows: Seq[Row] = Nil
    withSQLConf("spark.auron.enable.paimon.scan" -> "false") {
      rows = sql(s"select * from $table").collect().toSeq
    }
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      sql(s"select * from $table").schema)
  }

  /**
   * Creates `<name>_native` and `<name>_paimon` with `ddl`, runs every statement of `writes`
   * (with `%t` replaced by the table name) on both, and compares the content.
   */
  private def checkSameAsPaimon(name: String, ddl: String, writes: Seq[String])(
      assertNative: Seq[String] => Unit = nodes =>
        assert(nodes.exists(_.contains("NativePaimonWrite")), s"not native: $nodes")): Unit = {
    val nativeTable = s"paimon.db.${name}_native"
    val paimonTable = s"paimon.db.${name}_paimon"
    withTable(nativeTable, paimonTable) {
      sql(ddl.replace("%t", nativeTable))
      sql(ddl.replace("%t", paimonTable))
      var nodes: Seq[String] = Nil
      withSQLConf(nativeWriteConfs: _*) {
        nodes = runWrites(writes.map(_.replace("%t", nativeTable)))
      }
      assertNative(nodes)
      withSQLConf(paimonWriteConfs: _*) {
        writes.foreach(w => sql(w.replace("%t", paimonTable)))
      }
      checkAnswer(read(nativeTable), read(paimonTable).collect().toSeq)
    }
  }

  test("native write into unpartitioned append table") {
    checkSameAsPaimon(
      "w_unpart",
      "create table %t (id int, name string, score double, flag boolean) using paimon",
      Seq(
        "insert into %t select cast(id as int), concat('n', id), id * 1.5, id % 2 = 0 " +
          "from range(0, 1000)"))()
  }

  test("native write into partitioned append table with dynamic partitions") {
    checkSameAsPaimon(
      "w_part",
      """create table %t (id bigint, v string, dt string, hr int) using paimon
        |partitioned by (dt, hr)""".stripMargin,
      Seq(
        "insert into %t select id, cast(id as string), " +
          "concat('2026010', cast(id % 3 as string)), cast(id % 4 as int) from range(0, 3000)"))()
  }

  test("native write appends to existing data and keeps previous snapshot files") {
    checkSameAsPaimon(
      "w_append",
      "create table %t (id int, v string, p string) using paimon partitioned by (p)",
      Seq(
        "insert into %t values (1, 'a', 'x'), (2, 'b', 'y')",
        "insert into %t values (3, 'c', 'x'), (4, null, 'z')"))()
  }

  test("native static partition overwrite only replaces the target partition") {
    checkSameAsPaimon(
      "w_overwrite",
      "create table %t (id int, v string, p string) using paimon partitioned by (p)",
      Seq(
        "insert into %t values (1, 'a', 'x'), (2, 'b', 'y')",
        "insert overwrite %t partition (p = 'x') select 10, 'aa'"))()
  }

  test("native write handles null partition values and special characters") {
    checkSameAsPaimon(
      "w_nullpart",
      "create table %t (id int, p string) using paimon partitioned by (p)",
      Seq("insert into %t values (1, null), (2, '50%'), (3, 'a b'), (4, null)"))()
  }

  test("native write keeps partition values longer than the parquet statistics truncation") {
    // parquet truncates string statistics to 64 bytes by default, so these partition values
    // cannot be recovered from the footer statistics
    val long1 = "a" * 80
    val long2 = "a" * 79 + "b"
    checkSameAsPaimon(
      "w_longpart",
      "create table %t (id int, p string, q int) using paimon partitioned by (p, q)",
      Seq(s"insert into %t values (1, '$long1', 1), (2, '$long2', 1), (3, '$long1', 2)"))()
  }

  test("native write partitions by date, decimal and small integer columns") {
    checkSameAsPaimon(
      "w_typedpart",
      """create table %t (id int, d date, c decimal(10, 2), s smallint) using paimon
        |partitioned by (d, c, s)""".stripMargin,
      Seq(
        "insert into %t values (1, date'2026-10-27', 1.50, cast(1 as smallint)), " +
          "(2, date'2026-10-28', 2.50, cast(2 as smallint)), (3, null, null, null)"))()
  }

  test("native write bounds the memory of a single open file") {
    val table = "paimon.db.w_budget"
    withTable(table) {
      sql(s"create table $table (id bigint, v string) using paimon")
      var nodes: Seq[String] = Nil
      withSQLConf(nativeWriteConfs :+ ("spark.auron.paimon.write.memoryBudget" -> "65536"): _*) {
        nodes = runWrites(
          Seq(s"insert into $table select id, repeat(cast(id as string), 20) " +
            "from range(0, 50000, 1, 1)"))
      }
      assert(nodes.exists(_.contains("NativePaimonWrite")), s"not native: $nodes")
      // the only open file flushes its row group whenever it exceeds the budget
      val files = sql(s"select file_path from `paimon`.`db`.`w_budget$$files`")
        .collect()
        .map(_.getString(0))
      assert(files.length == 1, s"expected one file, got ${files.toSeq}")
      assert(rowGroupCount(files.head) > 1, s"expected several row groups in ${files.head}")
      checkAnswer(sql(s"select count(*), sum(id) from $table"), Row(50000L, 1249975000L))
    }
  }

  test("files failing validation stay in staging and moved files are removed on abort") {
    val table = "paimon.db.w_abort"
    withTable(table) {
      sql(s"create table $table (id int, v string) using paimon")
      val paimonTable = loadTable("w_abort")
      val writer = new PaimonSinkWriter(paimonTable)
      val tablePath = new HadoopPath(paimonTable.location().toString)
      val fs = tablePath.getFileSystem(spark.sessionState.newHadoopConf())
      def dataFiles(): Seq[String] =
        listFiles(fs, tablePath).filter(_.contains("/bucket-0/")).sorted

      // a valid file is moved into the bucket directory
      val good = writeParquetTo(writer.nextOutputPath(), rows = 3)
      writer.completeFile(good, 3, fileSize(fs, good), Array.emptyByteArray)
      assert(dataFiles().size == 1, s"expected the moved file, got ${dataFiles()}")

      // a file whose footer does not match is rejected before it is moved
      val bad = writeParquetTo(writer.nextOutputPath(), rows = 2)
      intercept[IllegalStateException] {
        writer.completeFile(bad, 5, fileSize(fs, bad), Array.emptyByteArray)
      }
      assert(dataFiles().size == 1, s"rejected file must not be moved: ${dataFiles()}")

      writer.abort()
      assert(dataFiles().isEmpty, s"abort must remove moved files: ${dataFiles()}")
      assert(
        !listFiles(fs, tablePath).exists(_.contains(".auron-staging")),
        "abort must remove the staging directory")
    }
  }

  private def loadTable(name: String): FileStoreTable =
    PaimonUtil.loadTable(s"$paimonWarehouse/db.db/$name")

  private def writeParquetTo(path: String, rows: Int): String = {
    val tmp = Files.createTempDirectory("auron-paimon-parquet-").toFile
    spark
      .range(0, rows)
      .selectExpr("cast(id as int) as id", "cast(id as string) as v")
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(tmp.getAbsolutePath)
    val part = tmp.listFiles().find(_.getName.endsWith(".parquet")).get
    val target = new HadoopPath(path)
    val fs = target.getFileSystem(spark.sessionState.newHadoopConf())
    fs.mkdirs(target.getParent)
    fs.copyFromLocalFile(new HadoopPath(part.getAbsolutePath), target)
    path
  }

  private def fileSize(fs: FileSystem, path: String): Long =
    fs.getFileStatus(new HadoopPath(path)).getLen

  private def listFiles(fs: FileSystem, dir: HadoopPath): Seq[String] = {
    val it = fs.listFiles(dir, true)
    val out = scala.collection.mutable.ArrayBuffer[String]()
    while (it.hasNext) out += it.next().getPath.toString
    out.toSeq
  }

  private def rowGroupCount(path: String): Int = {
    val file = HadoopInputFile.fromPath(new HadoopPath(path), spark.sessionState.newHadoopConf())
    val reader = ParquetFileReader.open(file)
    try reader.getRowGroups.size()
    finally reader.close()
  }

  test("native write supports decimal, date, binary and small integer types") {
    checkSameAsPaimon(
      "w_types",
      """create table %t (a tinyint, b smallint, c decimal(10, 2), d date, e binary,
        |f float) using paimon""".stripMargin,
      Seq("insert into %t values (cast(1 as tinyint), cast(2 as smallint), 12.34, " +
        "date'2026-10-27', X'0102', cast(1.5 as float)), (null, null, null, null, null, null)"))()
  }

  test("native write rolls files at target-file-size") {
    val table = "paimon.db.w_roll"
    withTable(table) {
      sql(s"""create table $table (id bigint, v string) using paimon
             |tblproperties ('target-file-size' = '64 kb')""".stripMargin)
      var nodes: Seq[String] = Nil
      withSQLConf(nativeWriteConfs: _*) {
        nodes = runWrites(
          Seq(
            s"insert into $table select id, repeat(cast(id as string), 20) from range(0, 50000)"))
      }
      assert(nodes.exists(_.contains("NativePaimonWrite")), s"not native: $nodes")
      val numFiles = sql(s"select count(*) from `paimon`.`db`.`w_roll$$files`").head().getLong(0)
      assert(numFiles > 1, s"expected rolled files, got $numFiles")
      checkAnswer(sql(s"select count(*), sum(id) from $table"), Row(50000L, 1249975000L))
    }
  }

  test("primary-key tables fall back to the paimon writer") {
    checkSameAsPaimon(
      "w_pk",
      """create table %t (id int, v string) using paimon
        |tblproperties ('primary-key' = 'id', 'bucket' = '2')""".stripMargin,
      Seq("insert into %t values (1, 'a'), (2, 'b')"))(nodes =>
      assert(
        nodes.nonEmpty && nodes.forall(_.contains("primary-key tables are not supported")),
        s"expected fallback with reason: $nodes"))
  }

  test("tables with column default values fall back to the paimon writer") {
    // Paimon fills defaults in its JVM writer, also for explicitly written NULLs; the native sink
    // would write NULL, so such tables must not be converted
    val table = "paimon.db.w_default"
    withTable(table) {
      sql(
        s"create table $table (id int, v string default 'dflt', p string) using paimon " +
          "partitioned by (p)")
      var nodes: Seq[String] = Nil
      withSQLConf(nativeWriteConfs: _*) {
        nodes = runWrites(
          Seq(
            s"insert into $table (id, p) values (1, 'x')",
            s"insert into $table values (2, null, 'x'), (3, 'c', 'x')"))
      }
      assert(
        nodes.nonEmpty && nodes.forall(_.contains("column v has a default value")),
        s"expected fallback with reason: $nodes")
      checkAnswer(
        sql(s"select id, v from $table order by id"),
        Seq(Row(1, "dflt"), Row(2, "dflt"), Row(3, "c")))
    }
  }

  test("v1 writes are not converted") {
    val table = "paimon.db.w_v1"
    withTable(table) {
      sql(s"create table $table (id int) using paimon")
      var nodes: Seq[String] = Nil
      withSQLConf(
        "spark.paimon.write.use-v2-write" -> "false",
        "spark.auron.enable.data.writing" -> "true",
        "spark.auron.enable.paimon.write" -> "true") {
        nodes = runWrites(Seq(s"insert into $table values (1)"))
      }
      assert(!nodes.exists(_.contains("NativePaimonWrite")), s"unexpected native: $nodes")
      checkAnswer(sql(s"select * from $table"), Row(1))
    }
  }
}
