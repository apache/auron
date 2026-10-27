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

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.auron.AuronConvertStrategy
import org.apache.spark.sql.auron.paimon.PaimonWriteSupport
import org.apache.spark.sql.execution.CommandResultExec
import org.apache.spark.sql.execution.auron.plan.NativePaimonWriteExec
import org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec

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
          // not converted: keep the fallback reason so a failing assertion is self-explaining
          val reason = p
            .getTagValue(AuronConvertStrategy.neverConvertReasonTag)
            .orElse(PaimonWriteSupport.unsupportedReason(p))
            .getOrElse("unknown")
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
      assert(!nodes.exists(_.contains("NativePaimonWrite")), s"unexpected native: $nodes"))
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
        nodes.nonEmpty && !nodes.exists(_.startsWith("NativePaimonWrite")),
        s"expected fallback: $nodes")
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

  /**
   * `INSERT ... SELECT` from a source Auron cannot read natively into a wider table, padding the
   * missing columns with constants. Returns the names of all nodes below the native write.
   */
  private def writeWidePadded(narrowConvert: Boolean): (Seq[String], Seq[Row]) = {
    val src = "paimon_wide_src"
    val table = s"paimon.db.w_wide_${narrowConvert}"
    // withTempPath / withTable return Unit, so results are passed out through these vars
    var plan: Seq[String] = Nil
    var rows: Seq[Row] = Nil
    withTempPath { dir =>
      spark
        .range(0, 2000)
        .selectExpr("cast(id as string) as a", "cast(id % 7 as string) as b")
        .write
        .csv(dir.getCanonicalPath)
      withTable(table) {
        sql(
          s"create temporary view $src (a string, b string) using csv " +
            s"options (path '${dir.getCanonicalPath}')")
        sql(s"""create table $table (a string, c1 string, b string, c2 string, c3 int, k string)
               |using paimon partitioned by (k)""".stripMargin)
        withSQLConf(
          nativeWriteConfs :+ ("spark.auron.paimon.write.narrowConvert" -> narrowConvert.toString): _*) {
          val executed = sql(s"""insert overwrite $table partition (k)
                                |select a, cast(null as string), b, 'x', cast(null as int), b
                                |from $src""".stripMargin).queryExecution.executedPlan match {
            case c: CommandResultExec => c.commandPhysicalPlan
            case p => p
          }
          val write = executed.collectFirst { case w: NativePaimonWriteExec => w }
          assert(write.isDefined, s"not native: ${executed.treeString}")
          plan = write.get.convertedInputForTesting.collect { case p => p.nodeName }
        }
        rows = sql(s"select * from $table").collect().toSeq
        sql(s"drop view $src")
      }
    }
    (plan, rows)
  }

  test("native write converts only the read columns of a padding projection") {
    val (narrowPlan, narrowRows) = writeWidePadded(narrowConvert = true)
    val (widePlan, wideRows) = writeWidePadded(narrowConvert = false)
    // the projection runs natively above a conversion of the two source columns
    assert(narrowPlan.exists(_.contains("NativeProject")), s"no native project: $narrowPlan")
    assert(!widePlan.exists(_.contains("NativeProject")), s"unexpected native project: $widePlan")
    assert(narrowRows.size == 2000)
    assert(narrowRows.sortBy(_.toString) == wideRows.sortBy(_.toString))
    assert(narrowRows.forall(r => r.isNullAt(1) && r.getString(3) == "x" && r.isNullAt(4)))
  }
}
