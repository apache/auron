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
package org.apache.iceberg.spark.source

import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

class IcebergSourceUtilSuite extends AnyFunSuite {
  test("isIcebergScan returns true for SparkBatchQueryScan") {
    val icebergScan =
      new SparkBatchQueryScan(null, null, null, null, null, new java.util.ArrayList(), null)
    assert(IcebergSourceUtil.isIcebergScan(icebergScan) === true)
  }

  test("isIcebergScan returns false for non-Iceberg Scan") {
    val nonIcebergScan = new Scan {
      override def description(): String = "NonIcebergScan"
      override def readSchema(): StructType = StructType(Seq())
    }
    assert(IcebergSourceUtil.isIcebergScan(nonIcebergScan) === false)
  }
}
