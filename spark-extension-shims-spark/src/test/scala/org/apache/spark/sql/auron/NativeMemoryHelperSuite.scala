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

import org.apache.auron.sparkver

class NativeMemoryHelperSuite extends SparkFunSuite {
  private val shims = Shims.get
  private def conf(master: String): SparkConf = new SparkConf(false).setMaster(master)

  @sparkver("3.0 / 3.1 / 3.2")
  private def checkGenericFactor(): Unit = {
    assert(Shims.get.executorMemoryOverheadFactor(new SparkConf(false)) == 0.10)
  }

  @sparkver("3.3 / 3.4 / 3.5 / 4.0 / 4.1 / 4.2")
  private def checkGenericFactor(): Unit = {
    import org.apache.spark.internal.config

    val sparkConf = new SparkConf(false)
    val expected = sparkConf.get(config.EXECUTOR_MEMORY_OVERHEAD_FACTOR)
    assert(Shims.get.executorMemoryOverheadFactor(sparkConf) == expected)
    Seq("yarn", "k8s://https://example:443").foreach { master =>
      assert(
        Shims.get.executorMemoryOverheadMiB(conf(master), 8192L) ==
          math.max((expected * 8192L).toLong, 384L))
    }
    Seq("0", "-0.1", "NaN", "invalid").foreach { factor =>
      sparkConf.set("spark.executor.memoryOverheadFactor", factor)
      val expectedError = intercept[IllegalArgumentException] {
        sparkConf.get(config.EXECUTOR_MEMORY_OVERHEAD_FACTOR)
      }
      val actualError = intercept[IllegalArgumentException] {
        Shims.get.executorMemoryOverheadMiB(sparkConf, 8192L)
      }
      assert(actualError.getMessage == expectedError.getMessage)
    }
  }

  test("generic factor uses Spark's typed default and validation when available") {
    checkGenericFactor()
  }

  test("explicit executor overhead takes precedence without applying the minimum") {
    val sparkConf = conf("k8s://https://example:443")
      .set("spark.executor.memoryOverhead", "128m")
      .set("spark.executor.memoryOverheadFactor", "invalid")
      .set("spark.kubernetes.memoryOverheadFactor", "invalid")
    assert(shims.executorMemoryOverheadMiB(sparkConf, 8192L) == 128L)
  }

  test("explicit generic factor takes precedence for every master") {
    Seq("k8s://https://example:443", "yarn", "spark://example:7077", "local").foreach { master =>
      val sparkConf = conf(master)
        .set("spark.executor.memoryOverheadFactor", "0.25")
        .set("spark.kubernetes.memoryOverheadFactor", "0.40")
      assert(shims.executorMemoryOverheadMiB(sparkConf, 8192L) == 2048L)
    }
  }

  test("Kubernetes uses the configured or driver-propagated factor") {
    Seq("0.20" -> 1638L, "0.40" -> 3276L).foreach { case (factor, expected) =>
      val sparkConf = conf("k8s://https://example:443")
        .set("spark.kubernetes.memoryOverheadFactor", factor)
      assert(shims.executorMemoryOverheadMiB(sparkConf, 8192L) == expected)
    }
  }

  test("default factor is ten percent and Kubernetes config is ignored elsewhere") {
    Seq("k8s://https://example:443", "yarn", "spark://example:7077", "local").foreach { master =>
      assert(shims.executorMemoryOverheadMiB(conf(master), 8192L) == 819L)
    }
    val sparkConf = conf("yarn").set("spark.kubernetes.memoryOverheadFactor", "0.40")
    assert(shims.executorMemoryOverheadMiB(sparkConf, 8192L) == 819L)
  }

  test("inferred overhead retains the minimum and truncates fractional MiB") {
    val sparkConf = conf("yarn").set("spark.executor.memoryOverheadFactor", "0.25")
    assert(shims.executorMemoryOverheadMiB(sparkConf, 1024L) == 384L)
    assert(shims.executorMemoryOverheadMiB(sparkConf, 1536L) == 384L)
    assert(shims.executorMemoryOverheadMiB(sparkConf, 1539L) == 384L)
    assert(shims.executorMemoryOverheadMiB(sparkConf, 1540L) == 385L)
    val k8sConf = conf("k8s://https://example:443")
      .set("spark.kubernetes.memoryOverheadFactor", "0")
    assert(shims.executorMemoryOverheadMiB(k8sConf, 8192L) == 384L)
  }

  test("invalid factors fail rather than silently using a default") {
    Seq("0", "-0.1", "NaN", "invalid").foreach { factor =>
      val sparkConf = conf("yarn").set("spark.executor.memoryOverheadFactor", factor)
      intercept[IllegalArgumentException] {
        shims.executorMemoryOverheadMiB(sparkConf, 8192L)
      }
    }
    Seq("-0.1", "NaN", "invalid").foreach { factor =>
      val sparkConf = conf("k8s://https://example:443")
        .set("spark.kubernetes.memoryOverheadFactor", factor)
      intercept[IllegalArgumentException] {
        shims.executorMemoryOverheadMiB(sparkConf, 8192L)
      }
    }
  }

  test("invalid Kubernetes factor identifies the config and value") {
    Seq("-0.1", "NaN").foreach { factor =>
      val sparkConf = conf("k8s://https://example:443")
        .set("spark.kubernetes.memoryOverheadFactor", factor)
      val error = intercept[IllegalArgumentException] {
        shims.executorMemoryOverheadMiB(sparkConf, 8192L)
      }
      assert(error.getMessage ==
        s"requirement failed: spark.kubernetes.memoryOverheadFactor must be >= 0, but was $factor")
    }
  }
}
