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

import java.io.File
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.sql.test.SharedSparkSession

trait BaseAuronPaimonSuite extends SharedSparkSession {

  protected lazy val paimonWarehouse: String = {
    val dir = Files.createTempDirectory("auron-paimon-warehouse-").toFile
    dir.deleteOnExit()
    dir.getAbsolutePath
  }

  override protected def sparkConf: SparkConf = {
    val extraJavaOptions =
      "--add-opens=java.base/java.lang=ALL-UNNAMED " +
        "--add-opens=java.base/java.nio=ALL-UNNAMED " +
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
        "-Dio.netty.tryReflectionSetAccessible=true"
    super.sparkConf
      .set(
        "spark.sql.extensions",
        "org.apache.spark.sql.auron.AuronSparkSessionExtension," +
          "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions")
      .set("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
      .set("spark.sql.catalog.paimon.warehouse", s"file:$paimonWarehouse")
      .set("spark.auron.enabled", "true")
      .set("spark.auron.enable.paimon.scan", "true")
      .set(
        "spark.shuffle.manager",
        "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
      .set("spark.auron.enable.shuffleExchange", "true")
      .set("spark.auron.enable.project", "false")
      .set("spark.auron.ui.enabled", "false")
      .set("spark.ui.enabled", "false")
      .set("spark.driver.extraJavaOptions", extraJavaOptions)
      .set("spark.executor.extraJavaOptions", extraJavaOptions)
  }

  override def afterAll(): Unit = {
    try {
      super.afterAll()
    } finally {
      deleteRecursively(new File(paimonWarehouse))
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }
}
