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
package org.apache.auron.integration

import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite

class MainTest extends AnyFunSuite {

  protected val tpcdsDataPath: String =
    sys.env.getOrElse("SPARK_TPCDS_DATA", "/tmp/tpcds_1g")

  private def assumePathExists(path: String): Unit = {
    assume(Files.exists(Paths.get(path)), s"Skip: directory $path does not exist")
  }

  test("parse mainArgs") {
    val mainArgs = Array[String](
      "--type",
      "tpcds",
      "--data-location",
      "dev/tpcds_1g",
      "--query-filter",
      "q1,q2,q3",
      "--conf",
      "spark.serializer=org.apache.spark.serializer.KryoSerializer",
      "--conf",
      "spark.celeborn.client.spark.shuffle.writer=hash",
      "--plan-check",
      "--regen-golden")
    Main.parseArgs(mainArgs) match {
      case Some(args) =>
        assertResult("tpcds")(args.benchType)
        assertResult("dev/tpcds_1g")(args.dataLocation)
        assertResult(Seq("q1", "q2", "q3"))(args.queryFilter)
        assertResult(
          Map(
            "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
            "spark.celeborn.client.spark.shuffle.writer" -> "hash"))(args.extraSparkConf)
        assertResult(true)(args.enablePlanCheck)
        assertResult(true)(args.regenGoldenFiles)
      case None =>
        fail(s"failed parse mainArgs: ${mainArgs.mkString("Array(", ", ", ")")}")
    }
  }

  test("plan check") {
    assumePathExists(tpcdsDataPath)
    val args = Array[String](
      "--type",
      "tpcds",
      "--data-location",
      tpcdsDataPath,
      "--query-filter",
      "q1",
      "--auron-only",
      "--plan-check")
    Main.main(args)
  }

  test("regen golden files") {
    assumePathExists(tpcdsDataPath)
    val args = Array[String](
      "--type",
      "tpcds",
      "--data-location",
      tpcdsDataPath,
      "--query-filter",
      "q1",
      "--auron-only",
      "--regen-golden")
    Main.main(args)
  }
}
