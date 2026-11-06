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

import scala.collection.JavaConverters._

import org.apache.iceberg.FileFormat
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.auron.iceberg.IcebergNativeScanTask
import org.apache.spark.sql.catalyst.plans.physical.SinglePartition
import org.apache.spark.sql.execution.auron.plan.NativeIcebergTableScanExec
import org.apache.spark.sql.execution.datasources.FilePartition

import org.apache.auron.jni.JniBridge

class NativeIcebergPartitioningSuite extends QueryTest with BaseAuronIcebergSuite {
  private val mib = 1024L * 1024

  Seq(4 -> Seq(2, 2, 2, 2), 8 -> Seq.fill(8)(1), 16 -> Seq.fill(8)(1)).foreach {
    case (minPartitions, expectedRanges) =>
      test(s"split file ranges respect minPartitionNum=$minPartitions") {
        withScan { scan =>
          withSQLConf("spark.sql.files.minPartitionNum" -> minPartitions.toString) {
            checkPartitions(scan, ranges("a", 1024, Seq.fill(8)(128)), expectedRanges)
          }
        }
      }
  }

  test("partition sizing combines ranges from differently sized files") {
    withScan { scan =>
      withSQLConf("spark.sql.files.minPartitionNum" -> "4") {
        val tasks = ranges("a", 1024, Seq.fill(4)(256)) ++
          ranges("b", 256, Seq(128, 128)) ++ ranges("c", 64, Seq(64))
        checkPartitions(scan, tasks, Seq(1, 1, 1, 1, 3))
      }
    }
  }

  test("partition sizing uses only retained ranges of a file") {
    withScan { scan =>
      withSQLConf("spark.sql.files.minPartitionNum" -> "2") {
        val tasks = ranges("a", 1024, Seq.fill(8)(128)).slice(3, 5)
        checkPartitions(scan, tasks, Seq(1, 1))
      }
    }
  }

  test("partition sizing charges open cost for each range of the same file") {
    withScan { scan =>
      withSQLConf("spark.sql.files.minPartitionNum" -> "2") {
        checkPartitions(scan, ranges("a", 8, Seq.fill(8)(1)), Seq(4, 4))
      }
    }
  }

  test("partition sizing preserves unsplit file packing") {
    withScan { scan =>
      withSQLConf("spark.sql.files.minPartitionNum" -> "2") {
        val tasks = ranges("a", 128, Seq(128)) ++ ranges("b", 64, Seq(64)) ++
          ranges("c", 32, Seq(32))
        checkPartitions(scan, tasks, Seq(1, 2))
      }
    }
  }

  test("partition sizing respects the maximum partition size") {
    withScan { scan =>
      withSQLConf(
        "spark.sql.files.minPartitionNum" -> "1",
        "spark.sql.files.maxPartitionBytes" -> (128 * mib).toString) {
        checkPartitions(scan, ranges("a", 256, Seq.fill(4)(64)), Seq.fill(4)(1))
      }
    }
  }

  test("small ranges retain per-range open cost at high minimum partition counts") {
    withScan { scan =>
      withSQLConf("spark.sql.files.minPartitionNum" -> "100") {
        checkPartitions(scan, ranges("a", 8, Seq.fill(8)(1)), Seq.fill(8)(1))
      }
    }
  }

  private def withScan(f: NativeIcebergTableScanExec => Unit): Unit = {
    withSQLConf(
      "spark.sql.adaptive.enabled" -> "false",
      "spark.sql.files.maxPartitionBytes" -> (4096 * mib).toString,
      "spark.sql.files.maxPartitionNum" -> "1000",
      "spark.sql.files.openCostInBytes" -> (4 * mib).toString) {
      withTable("local.db.t_partition_sizes") {
        sql("create table local.db.t_partition_sizes (id int) using iceberg")
        val scan = sql(
          "select * from local.db.t_partition_sizes").queryExecution.executedPlan.collectFirst {
          case native: NativeIcebergTableScanExec => native
        }.get
        assert(scan.outputPartitioning != SinglePartition)
        f(scan)
      }
    }
  }

  private def ranges(
      file: String,
      sizeInMiB: Long,
      lengthsInMiB: Seq[Int]): Seq[IcebergNativeScanTask] = {
    val starts = lengthsInMiB.scanLeft(0L)(_ + _).dropRight(1)
    starts.zip(lengthsInMiB).map { case (start, length) =>
      IcebergNativeScanTask(
        s"file:/$file.parquet",
        start * mib,
        length * mib,
        sizeInMiB * mib,
        Nil)
    }
  }

  private def checkPartitions(
      scan: NativeIcebergTableScanExec,
      tasks: Seq[IcebergNativeScanTask],
      expectedRanges: Seq[Int]): Unit = {
    Seq(FileFormat.PARQUET, FileFormat.ORC).foreach { format =>
      val rdd = scan
        .copy(staticPlan = scan.staticPlan.copy(scanTasks = tasks, fileFormat = format))
        .doExecuteNative()
      val partitions = rdd.partitions.map(_.asInstanceOf[FilePartition])
      assert(partitions.map(_.files.length).toSeq == expectedRanges)
      val actualRanges =
        partitions.flatMap(_.files).map(file => (file.filePath.toString, file.start, file.length))
      assert(actualRanges.toSeq.sorted == tasks.map(t => (t.location, t.start, t.length)).sorted)

      partitions.foreach { partition =>
        val nativePlan = rdd.nativePlan(partition, null)
        val (conf, resourceId) =
          if (format == FileFormat.PARQUET) {
            (nativePlan.getParquetScan.getBaseConf, nativePlan.getParquetScan.getFsResourceId)
          } else {
            (nativePlan.getOrcScan.getBaseConf, nativePlan.getOrcScan.getFsResourceId)
          }
        try {
          conf.getFileGroup.getFilesList.asScala.foreach { file =>
            val task =
              tasks.find(t => t.location == file.getPath && t.start == file.getRange.getStart).get
            assert(file.getSize == task.fileSizeInBytes)
            assert(file.getRange.getEnd == task.start + task.length)
          }
        } finally {
          JniBridge.getResource(resourceId)
        }
      }
    }
  }
}
