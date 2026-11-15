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
package org.apache.spark.sql.execution

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.auron.{NativePartition, Shims}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.test.SharedSparkSession

class NativeFileScanLocalitySuite extends QueryTest with SharedSparkSession {

  for (format <- Seq("orc", "parquet")) {
    test(s"native $format scan preserves file locality through narrow dependencies") {
      withSQLConf("spark.sql.sources.useV1SourceList" -> "orc,parquet") {
        withTempPath { path =>
          spark.range(1).coalesce(1).write.format(format).save(path.getCanonicalPath)
          val scan = spark.read
            .format(format)
            .load(path.getCanonicalPath)
            .queryExecution
            .executedPlan
            .collectFirst { case scan: FileSourceScanExec => scan }
            .get
          val nativeScan = format match {
            case "orc" => Shims.get.createNativeOrcScanExec(scan)
            case "parquet" => Shims.get.createNativeParquetScanExec(scan)
          }
          val nativeRDD = nativeScan.executeNative()
          assert(nativeRDD.partitions.nonEmpty)
          assert(nativeRDD.dependencies.isEmpty)
          val downstream = nativeRDD.mapPartitions(rows => rows)
          val renamed =
            Shims.get.createNativeRenameColumnsExec(nativeScan, nativeScan.output.map(_.name))
          val nativeDownstream = Shims.get
            .createNativeProjectExec(renamed.output, renamed)
            .executeNative()
          assert(nativeDownstream.partitions.forall(_.isInstanceOf[NativePartition[_]]))

          // The scan's anonymous RDD subclass must remain serializable for task submission.
          val serializer = spark.sparkContext.env.closureSerializer.newInstance()
          assert(serializer.serialize(nativeDownstream).hasRemaining)

          // Local files do not provide DataNode hosts. Supply deterministic location hints
          // on the driver without requiring an HDFS cluster or executing the native reader.
          for (hosts <- Seq(Array("worker-a", "localhost"), Array.empty[String])) {
            val expected = hosts.filterNot(_ == "localhost").toSeq
            nativeRDD.partitions.foreach { partition =>
              val filePartition = partition.asInstanceOf[FilePartition]
              assert(filePartition.files.nonEmpty)
              filePartition.files.indices.foreach { index =>
                filePartition.files(index) = filePartition.files(index).copy(locations = hosts)
              }
              assert(nativeRDD.preferredLocations(partition) == expected)
              for (rdd <- Seq(downstream, nativeDownstream)) {
                val locations = spark.sparkContext.dagScheduler
                  .getPreferredLocs(rdd, partition.index)
                assert(locations.map(_.host) == expected)
              }
            }
          }
        }
      }
    }
  }
}
