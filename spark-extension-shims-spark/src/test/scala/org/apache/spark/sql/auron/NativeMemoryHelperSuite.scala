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
package org.apache.spark.sql.auron

import org.apache.spark.{SparkConf, SparkFunSuite}

class NativeMemoryHelperSuite extends SparkFunSuite {
  private def conf(master: String): SparkConf = new SparkConf(false).setMaster(master)

  test("explicit executor overhead takes precedence without applying the minimum") {
    val sparkConf = conf("k8s://https://example:443")
      .set("spark.executor.memoryOverhead", "128m")
      .set("spark.executor.memoryOverheadFactor", "invalid")
      .set("spark.kubernetes.memoryOverheadFactor", "invalid")
    assert(NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 8192L) == 128L)
  }

  test("explicit generic factor takes precedence for every master") {
    Seq("k8s://https://example:443", "yarn", "spark://example:7077", "local").foreach { master =>
      val sparkConf = conf(master)
        .set("spark.executor.memoryOverheadFactor", "0.25")
        .set("spark.kubernetes.memoryOverheadFactor", "0.40")
      assert(NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 8192L) == 2048L)
    }
  }

  test("Kubernetes uses the configured or driver-propagated factor") {
    Seq("0.20" -> 1638L, "0.40" -> 3276L).foreach { case (factor, expected) =>
      val sparkConf = conf("k8s://https://example:443")
        .set("spark.kubernetes.memoryOverheadFactor", factor)
      assert(NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 8192L) == expected)
    }
  }

  test("default factor is ten percent and Kubernetes config is ignored elsewhere") {
    Seq("k8s://https://example:443", "yarn", "spark://example:7077", "local").foreach { master =>
      assert(NativeMemoryHelper.executorMemoryOverheadMiB(conf(master), 8192L) == 819L)
    }
    val sparkConf = conf("yarn").set("spark.kubernetes.memoryOverheadFactor", "0.40")
    assert(NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 8192L) == 819L)
  }

  test("inferred overhead retains the minimum and truncates fractional MiB") {
    val sparkConf = conf("yarn").set("spark.executor.memoryOverheadFactor", "0.25")
    assert(NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 1024L) == 384L)
    assert(NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 1536L) == 384L)
    assert(NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 1539L) == 384L)
    assert(NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 1540L) == 385L)
    val k8sConf = conf("k8s://https://example:443")
      .set("spark.kubernetes.memoryOverheadFactor", "0")
    assert(NativeMemoryHelper.executorMemoryOverheadMiB(k8sConf, 8192L) == 384L)
  }

  test("invalid factors fail rather than silently using a default") {
    Seq("0", "-0.1", "NaN", "invalid").foreach { factor =>
      val sparkConf = conf("yarn").set("spark.executor.memoryOverheadFactor", factor)
      intercept[IllegalArgumentException] {
        NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 8192L)
      }
    }
    Seq("-0.1", "NaN", "invalid").foreach { factor =>
      val sparkConf = conf("k8s://https://example:443")
        .set("spark.kubernetes.memoryOverheadFactor", factor)
      intercept[IllegalArgumentException] {
        NativeMemoryHelper.executorMemoryOverheadMiB(sparkConf, 8192L)
      }
    }
  }
}
