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
package org.apache.spark.sql.auron.hudi

import java.io.File
import java.io.FileInputStream
import java.nio.file.Files
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.auron.plan.NativeOrcScanBase
import org.apache.spark.sql.execution.auron.plan.NativeParquetScanBase
import org.apache.spark.sql.execution.auron.plan.NativeParquetScanExec
import org.apache.spark.sql.test.SharedSparkSession

import org.apache.auron.util.SparkVersionUtil

class HudiScanSupportSuite extends SparkFunSuite with SharedSparkSession {

  private lazy val suiteWorkspace: File = {
    val base = new File("target/tmp")
    base.mkdirs()
    Files.createTempDirectory(base.toPath, "auron-hudi-tests").toFile
  }
  private lazy val warehouseDir: String =
    new File(suiteWorkspace, "spark-warehouse").getAbsolutePath

  private lazy val hudiCatalogClassAvailable: Boolean = {
    try {
      Class.forName(
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
        false,
        Thread.currentThread().getContextClassLoader)
      true
    } catch {
      case _: ClassNotFoundException => false
    }
  }

  override protected def sparkConf: SparkConf = {
    if (!suiteWorkspace.exists()) {
      suiteWorkspace.mkdirs()
    }
    new File(warehouseDir).mkdirs()
    val extraJavaOptions =
      "--add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
        "--add-opens=java.base/java.io=ALL-UNNAMED " +
        "--add-opens=java.base/java.net=ALL-UNNAMED " +
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--add-opens=java.base/java.util=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.tools.keytool=ALL-UNNAMED " +
        "--add-opens=java.base/sun.security.x509=ALL-UNNAMED " +
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
        "-Djdk.reflect.useDirectMethodHandle=false " +
        "-Dio.netty.tryReflectionSetAccessible=true"
    val conf = super.sparkConf
      .set(
        "spark.sql.extensions",
        "org.apache.spark.sql.auron.AuronSparkSessionExtension," +
          "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.auron.enable", "true")
      .set("spark.sql.warehouse.dir", warehouseDir)
      .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.driver.extraJavaOptions", extraJavaOptions)
      .set("spark.executor.extraJavaOptions", extraJavaOptions)
      // Disable native timestamp scan to validate fallback behavior in tests.
      .set("spark.auron.enable.scan.parquet.timestamp", "false")
      .set("spark.auron.ui.enabled", "false")
      .set("spark.ui.enabled", "false")
    if (hudiCatalogClassAvailable) {
      conf.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    } else {
      info(
        "Hudi HoodieCatalog not found on the classpath; leaving spark.sql.catalog.spark_catalog as default.")
    }
    conf
  }

  private def withTable(tableName: String)(f: => Unit): Unit = {
    try {
      f
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

  private def assertHasNativeParquetScan(plan: org.apache.spark.sql.execution.SparkPlan): Unit = {
    assert(plan.find(_.isInstanceOf[NativeParquetScanBase]).nonEmpty)
  }

  private def assertHasNativeOrcScan(plan: org.apache.spark.sql.execution.SparkPlan): Unit = {
    assert(plan.find(_.isInstanceOf[NativeOrcScanBase]).nonEmpty)
  }

  private def assertNoNativeParquetScan(df: org.apache.spark.sql.DataFrame): Unit = {
    assert(df.queryExecution.executedPlan.collect { case _: NativeParquetScanBase =>
      true
    }.isEmpty)
  }

  private def fileSourceScanFromPlan(
      plan: org.apache.spark.sql.execution.SparkPlan): Option[FileSourceScanExec] =
    plan.collectFirst {
      case scan: FileSourceScanExec =>
        scan
      case nativeScan: NativeParquetScanExec =>
        nativeScan.basedFileScan
    }

  private def assertProviderConvertsToNativeParquetScan(df: DataFrame): FileSourceScanExec = {
    val plan = materializedPlan(df)
    val preAqePlan = df.queryExecution.sparkPlan
    val scan = fileSourceScanFromPlan(plan)
      .orElse(fileSourceScanFromPlan(preAqePlan))
      .getOrElse {
        fail(
          "Expected FileSourceScanExec or NativeParquetScanExec in Hudi scan plan.\n" +
            s"Materialized plan:\n$plan\nPre-AQE plan:\n$preAqePlan")
      }
    val provider = new HudiConvertProvider
    assert(provider.isSupported(scan))
    val converted = provider.convert(scan)
    assertHasNativeParquetScan(converted)
    scan
  }

  private def assertFilterReferences(
      filters: Seq[Expression],
      attributeName: String,
      filterType: String): Unit = {
    assert(
      filters.exists(_.references.exists(_.name == attributeName)),
      s"Expected $filterType filters to reference $attributeName, but got: $filters")
  }

  private def logFileFormats(df: org.apache.spark.sql.DataFrame): Unit = {
    val plan = materializedPlan(df)
    val nodes = plan.collect { case p => p }
    val scans = plan.collect { case scan: org.apache.spark.sql.execution.FileSourceScanExec =>
      scan
    }
    val formats = scans.map(_.relation.fileFormat.getClass.getName)
    val nativeScans = plan.collect {
      case scan: NativeParquetScanBase => scan.nodeName
      case scan: NativeOrcScanBase => scan.nodeName
    }
    val nativeScanNames = nodes
      .map(_.nodeName)
      .filter(name => name.contains("NativeParquetScan") || name.contains("NativeOrcScan"))
    if (formats.nonEmpty) {
      info(s"Detected file formats: ${formats.distinct.mkString(", ")}")
      scans.foreach { scan =>
        info(
          s"Scan requiredSchema: ${scan.requiredSchema.simpleString}, " +
            s"options: ${scan.relation.options}")
      }
    }
    if (nativeScans.nonEmpty) {
      info(s"Detected native scans: ${nativeScans.distinct.mkString(", ")}")
    }
    if (nativeScanNames.nonEmpty && nativeScans.isEmpty) {
      info(s"Detected native scans (by nodeName): ${nativeScanNames.distinct.mkString(", ")}")
    }
    if (formats.isEmpty && nativeScans.isEmpty) {
      info(s"No FileSourceScanExec/Native scan found. Plan: ${plan.simpleString(2)}")
    }
  }

  private def materializedPlan(
      df: org.apache.spark.sql.DataFrame): org.apache.spark.sql.execution.SparkPlan = {
    // Ensure we inspect the post-AQE plan when adaptive execution is enabled.
    df.queryExecution.executedPlan match {
      case adaptive: org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec =>
        adaptive.executedPlan
      case other => other
    }
  }

  private def hudiBaseFileFormat(tableName: String): Option[String] = {
    // Read the base file format from Hudi table properties for assertions.
    val propsFile = new File(new File(warehouseDir, tableName), ".hoodie/hoodie.properties")
    if (!propsFile.exists()) {
      return None
    }
    val props = new Properties()
    val in = new FileInputStream(propsFile)
    try {
      props.load(in)
    } finally {
      in.close()
    }
    Option(props.getProperty("hoodie.table.base.file.format"))
  }

  private def hudiTablePath(tableName: String): String = {
    new File(warehouseDir, tableName).getAbsolutePath
  }

  private def assumeSparkAtLeast(version: String): Unit = {
    val current = SparkVersionUtil.SPARK_RUNTIME_VERSION
    assume(current >= version, s"Requires Spark >= $version, current Spark $current")
  }

  test("hudi fileFormat detects parquet and orc classes") {
    assert(
      HudiScanSupport
        .fileFormat("org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat")
        .contains(HudiScanSupport.ParquetFormat))
    assert(HudiScanSupport
      .fileFormat(
        "org.apache.spark.sql.execution.datasources.parquet.Spark35LegacyHoodieParquetFileFormat")
      .contains(HudiScanSupport.ParquetFormat))
    assert(
      HudiScanSupport
        .fileFormat("org.apache.spark.sql.execution.datasources.orc.HoodieOrcFileFormat")
        .contains(HudiScanSupport.OrcFormat))
  }

  test("hudi fileFormat rejects NewHoodie formats") {
    assert(
      HudiScanSupport
        .fileFormat(
          "org.apache.spark.sql.execution.datasources.parquet.NewHoodieParquetFileFormat")
        .isEmpty)
    assert(
      HudiScanSupport
        .fileFormat("org.apache.spark.sql.execution.datasources.orc.NewHoodieOrcFileFormat")
        .isEmpty)
  }

  test("hudi isSupported rejects MOR table types unless read optimized") {
    val options = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ")
    assert(
      !HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        options))
    assert(
      HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        options + ("hoodie.datasource.query.type" -> "read_optimized")))
    assert(
      HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        options + ("Hoodie.DataSource.View.Type" -> "READ_OPTIMIZED")))
    assert(
      !HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        options + ("hoodie.datasource.query.type" -> "snapshot")))
    assert(
      !HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        options + ("Hoodie.DataSource.View.Type" -> "REALTIME")))
    assert(
      !HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        options + ("hoodie.datasource.query.type" -> "incremental")))
  }

  test("hudi scan options are case-insensitive") {
    val options = Map(
      "Hoodie.DataSource.Write.Table.Type" -> "MERGE_ON_READ",
      "Hoodie.Table.Base.File.Format" -> "ORC")
    val readOptimizedOptions = options + ("Hoodie.DataSource.Query.Type" -> "READ_OPTIMIZED")
    val timeTravelOptions = Map("Hoodie.DataSource.Read.As.Of.Instant" -> "20240101010101")
    assert(
      !HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        options))
    assert(
      HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        readOptimizedOptions))
    assert(
      !HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        timeTravelOptions))
    assert(HudiScanSupport.baseFileFormatFromOptions(options).contains("ORC"))
  }

  test("hudi isSupported allows default COW") {
    assert(
      HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        Map.empty))
    assert(
      HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.HoodieParquetFileFormat",
        Map(
          "hoodie.datasource.write.table.type" -> "COPY_ON_WRITE",
          "hoodie.datasource.query.type" -> "snapshot")))
  }

  test("hudi isSupported rejects non-Hudi formats") {
    assert(
      !HudiScanSupport.isSupported(
        "org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat",
        Map.empty))
  }

  test("hudi: time travel falls back to Spark") {
    assumeSparkAtLeast("3.2")
    withTable("hudi_tm") {
      spark.sql("create table hudi_tm (id int, name string) using hudi")
      spark.sql("insert into hudi_tm values (1, 'v1'), (2, 'v2')")
      spark.sql("insert into hudi_tm values (3, 'v3'), (4, 'v4')")
      val value = spark
        .sql("select _hoodie_commit_time from hudi_tm")
        .collectAsList()
        .get(0)
        .getAs[String](0)

      val df1 = spark.sql(s"select id, name from hudi_tm timestamp AS OF $value")
      // Time travel uses metadata and should stay on Spark path.
      logFileFormats(df1)
      val rows1 = df1.collect().toSeq
      assert(rows1 == Seq(Row(1, "v1"), Row(2, "v2")))
      assertNoNativeParquetScan(df1)

      val df2 = spark.sql(s"select name from hudi_tm timestamp AS OF $value where id = 2")
      // Filters shouldn't affect fallback for time travel queries.
      logFileFormats(df2)
      val rows2 = df2.collect().toSeq
      assert(rows2 == Seq(Row("v2")))
      assertNoNativeParquetScan(df2)
    }
  }

  test("hudi: timestamp column falls back when native timestamp disabled") {
    withTable("hudi_ts") {
      spark.sql("create table hudi_ts (id int, ts timestamp) using hudi")
      spark.sql("insert into hudi_ts values (1, timestamp('2026-01-01 00:00:00'))")
      val df = spark.sql("select * from hudi_ts")
      // Timestamp columns are not supported when native timestamp scanning is disabled.
      logFileFormats(df)
      df.collect()
      assert(df.queryExecution.executedPlan.collect { case _: NativeParquetScanBase =>
        true
      }.isEmpty)
    }
  }

  test("hudi: ORC table scan converts to native (provider)") {
    withTable("hudi_orc") {
      spark.sql("""create table hudi_orc (id int, name string)
          |using hudi
          |tblproperties (
          |  'hoodie.datasource.write.table.type' = 'cow',
          |  'hoodie.datasource.write.storage.type' = 'ORC',
          |  'hoodie.datasource.write.base.file.format' = 'ORC',
          |  'hoodie.table.base.file.format' = 'ORC'
          |)""".stripMargin)
      spark.sql("insert into hudi_orc values (1, 'v1'), (2, 'v2')")
      val baseFormat = hudiBaseFileFormat("hudi_orc").getOrElse("unknown")
      info(s"Hudi base file format: $baseFormat")
      assume(
        baseFormat.equalsIgnoreCase("orc"),
        s"Expected ORC base file format but found: $baseFormat")
      val df = spark.sql("select id, name from hudi_orc where id = 2")
      // Validate provider conversion even if Spark reports generic OrcFileFormat.
      logFileFormats(df)
      val rows = df.collect().toSeq
      assert(rows == Seq(Row(2, "v2")))
      val scan = df.queryExecution.sparkPlan.collectFirst {
        case s: org.apache.spark.sql.execution.FileSourceScanExec => s
      }
      assert(scan.isDefined)
      val provider = new HudiConvertProvider
      assert(provider.isSupported(scan.get))
      val converted = provider.convert(scan.get)
      assertHasNativeOrcScan(converted)
    }
  }

  test("hudi: Parquet table scan converts to native (provider)") {
    withTable("hudi_native_simple") {
      spark.sql("create table hudi_native_simple (id int, name string) using hudi")
      spark.sql("insert into hudi_native_simple values (1, 'v1'), (2, 'v2')")
      val df = spark.sql("select id, name from hudi_native_simple order by id")
      df.explain(true)
      // Validate provider conversion and correctness for the common COW parquet path.
      logFileFormats(df)
      val rows = df.collect().toSeq
      assert(rows == Seq(Row(1, "v1"), Row(2, "v2")))
      val scan = df.queryExecution.sparkPlan.collectFirst {
        case s: org.apache.spark.sql.execution.FileSourceScanExec => s
      }
      assert(scan.isDefined)
      val provider = new HudiConvertProvider
      assert(provider.isSupported(scan.get))
      val converted = provider.convert(scan.get)
      assertHasNativeParquetScan(converted)
    }
  }

  test("hudi: MOR read-optimized table scan converts to native") {
    withTable("hudi_mor_read_optimized") {
      spark.sql("""create table hudi_mor_read_optimized (id int, name string)
          |using hudi
          |tblproperties (
          |  'hoodie.datasource.write.table.type' = 'MERGE_ON_READ'
          |)""".stripMargin)
      spark.sql("insert into hudi_mor_read_optimized values (1, 'v1'), (2, 'v2')")

      val df = spark.read
        .format("hudi")
        .option("hoodie.datasource.query.type", "read_optimized")
        .load(hudiTablePath("hudi_mor_read_optimized"))
        .select("id", "name")
        .orderBy("id")

      logFileFormats(df)
      val rows = df.collect().toSeq
      assert(rows == Seq(Row(1, "v1"), Row(2, "v2")))
      val scan = df.queryExecution.sparkPlan.collectFirst {
        case s: org.apache.spark.sql.execution.FileSourceScanExec => s
      }
      assert(scan.isDefined)
      val provider = new HudiConvertProvider
      assert(provider.isSupported(scan.get))
      val converted = provider.convert(scan.get)
      assertHasNativeParquetScan(converted)
    }
  }

  test("hudi: partitioned COW parquet scan converts to native with filters") {
    withTable("hudi_partitioned_cow") {
      spark.sql("""create table hudi_partitioned_cow (id int, name string, dt string)
          |using hudi
          |partitioned by (dt)
          |tblproperties (
          |  'hoodie.datasource.write.table.type' = 'cow'
          |)""".stripMargin)
      spark.sql("""insert into hudi_partitioned_cow values
          |  (1, 'v1', '2026-05-01'),
          |  (2, 'v2', '2026-05-01'),
          |  (3, 'v3', '2026-05-02')
          |""".stripMargin)

      val fullScan = spark.sql("select id, name, dt from hudi_partitioned_cow order by id")
      logFileFormats(fullScan)
      assert(
        fullScan.collect().toSeq == Seq(
          Row(1, "v1", "2026-05-01"),
          Row(2, "v2", "2026-05-01"),
          Row(3, "v3", "2026-05-02")))
      assertProviderConvertsToNativeParquetScan(fullScan)

      val partitionPruned = spark.sql("""
          |select id, name, dt
          |from hudi_partitioned_cow
          |where dt = '2026-05-01'
          |order by id
          |""".stripMargin)
      logFileFormats(partitionPruned)
      assert(
        partitionPruned.collect().toSeq == Seq(
          Row(1, "v1", "2026-05-01"),
          Row(2, "v2", "2026-05-01")))
      val partitionScan = assertProviderConvertsToNativeParquetScan(partitionPruned)
      assertFilterReferences(partitionScan.partitionFilters, "dt", "partition")

      val partitionAndDataFiltered = spark.sql("""
          |select id, name, dt
          |from hudi_partitioned_cow
          |where dt = '2026-05-01' and id = 2
          |""".stripMargin)
      logFileFormats(partitionAndDataFiltered)
      assert(partitionAndDataFiltered.collect().toSeq == Seq(Row(2, "v2", "2026-05-01")))
      val filteredScan = assertProviderConvertsToNativeParquetScan(partitionAndDataFiltered)
      assertFilterReferences(filteredScan.partitionFilters, "dt", "partition")
      assertFilterReferences(filteredScan.dataFilters, "id", "data")
    }
  }
}
