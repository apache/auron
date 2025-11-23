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

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.execution.ui.AuronSQLAppStatusListener
import org.apache.spark.util.Utils

import org.apache.auron.BaseAuronSQLSuite

class BuildInfoInSparkUISuite extends AuronQueryTest with BaseAuronSQLSuite {

  var testDir: File = _

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set("spark.eventLog.dir", testDir.toString)
  }

  override protected def beforeAll(): Unit = {
    testDir = Utils.createTempDir(namePrefix = "spark-events")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    Utils.deleteRecursively(testDir)
  }

  test("test build info in spark UI ") {
    val listeners = spark.sparkContext.listenerBus.findListenersByClass[AuronSQLAppStatusListener]
    assert(listeners.size === 1)
    val listener = listeners(0)
    assert(listener.getAuronBuildInfo() == 1)
  }

}
