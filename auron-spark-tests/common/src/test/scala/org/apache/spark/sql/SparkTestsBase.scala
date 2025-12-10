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
package org.apache.spark.sql

import scala.collection.mutable

import org.scalactic.source.Position
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuiteLike

import org.apache.auron.utils.SparkTestSettings

/**
 * Base trait for all Spark tests.
 */
trait SparkTestsBase extends AnyFunSuiteLike {
  protected val IGNORE_ALL: String = "IGNORE_ALL"
  protected val AURON_TEST: String = "Auron - "

  protected val rootPath: String = getClass.getResource("/").getPath
  protected val basePath: String = rootPath + "unit-tests-working-home"
  protected val warehouse: String = basePath + "/spark-warehouse"
  protected val metaStorePathAbsolute: String = basePath + "/meta"

  /**
   * Returns a sequence of test names to be blacklisted (i.e., skipped) for this test suite.
   *
   * Any test whose name appears in the returned sequence will never be run, regardless of backend
   * test settings. This method can be overridden by subclasses to specify which tests to skip.
   *
   * Special behavior: If the sequence contains the value of `IGNORE_ALL` (case-insensitive), then
   * all tests in the suite will be skipped.
   *
   * @return
   *   a sequence of test names to blacklist, or a sequence containing `IGNORE_ALL` to skip all
   *   tests
   */
  def testNameBlackList: Seq[String] = Seq()

  val sparkConfList = {
    val conf = mutable.Map[String, String]()
    conf += ("spark.driver.memory" -> "1G")
    conf += ("spark.sql.adaptive.enabled" -> "true")
    conf += ("spark.sql.shuffle.partitions" -> "1")
    conf += ("spark.sql.files.maxPartitionBytes" -> "134217728")
    conf += ("spark.ui.enabled" -> "false")
    conf += ("auron.ui.enabled" -> "false")
    conf += ("spark.auron.enable" -> "true")
    conf += ("spark.executor.memory" -> "1G")
    conf += ("spark.executor.memoryOverhead" -> "1G")
    conf += ("spark.memory.offHeap.enabled" -> "false")
    conf += ("spark.sql.extensions" -> "org.apache.spark.sql.auron.AuronSparkSessionExtension")
    conf += ("spark.shuffle.manager" ->
      "org.apache.spark.sql.execution.auron.shuffle.AuronShuffleManager")
    conf += ("spark.unsafe.exceptionOnMemoryLeak" -> "true")
    // Avoid the code size overflow error in Spark code generation.
    conf += ("spark.sql.codegen.wholeStage" -> "false")

    conf
  }

  protected def shouldRun(testName: String): Boolean = {
    if (testNameBlackList.exists(_.equalsIgnoreCase(IGNORE_ALL))) {
      return false
    }

    if (testNameBlackList.contains(testName)) {
      return false
    }

    SparkTestSettings.shouldRun(getClass.getCanonicalName, testName)
  }

  protected def testAuron(testName: String, testTag: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    test(AURON_TEST + testName, testTag: _*)(testFun)
  }

  protected def ignoreAuron(testName: String, testTag: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    super.ignore(AURON_TEST + testName, testTag: _*)(testFun)
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit
      pos: Position): Unit = {
    if (shouldRun(testName)) {
      super.test(testName, testTags: _*)(testFun)
    } else {
      super.ignore(testName, testTags: _*)(testFun)
    }
  }
}
