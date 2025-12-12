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
package org.apache.auron

import java.io.IOException
import java.nio.file.{Files, FileVisitResult, Path, SimpleFileVisitor}
import java.nio.file.attribute.BasicFileAttributes

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

trait BaseAuronSQLSuite extends SharedSparkSession {

  private lazy val suiteWarehouseDir: Path =
    Files.createTempDirectory("auron-spark-warehouse-")

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.extensions", "org.apache.spark.sql.auron.AuronSparkSessionExtension")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.memory.offHeap.enabled", "false")
      .set("spark.auron.enable", "true")
      .set("spark.sql.warehouse.dir", suiteWarehouseDir.toFile.getCanonicalPath)
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      // Best-effort cleanup of the per-suite warehouse dir
      try deleteRecursively(suiteWarehouseDir)
      catch {
        case _: Throwable => // ignore
      }
    }
  }

  private def deleteRecursively(root: Path): Unit = {
    if (root == null) return
    if (!Files.exists(root)) return
    Files.walkFileTree(
      root,
      new SimpleFileVisitor[Path]() {
        @throws[IOException]
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.deleteIfExists(file)
          FileVisitResult.CONTINUE
        }
        @throws[IOException]
        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.deleteIfExists(dir)
          FileVisitResult.CONTINUE
        }
      })
  }

}
