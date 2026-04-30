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
package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.hive.execution.auron.plan.NativeParquetHiveTableScanExec

class HiveParquetTableScanExecSuite extends AuronQueryTest with BaseAuronHiveSuite {

  test("test hive parquet table without partition to native") {
    withTempView("hive_table_without_partition") {
      spark.sql("create table hive_table_without_partition (a string) stored as parquet")
      spark.sql("insert into hive_table_without_partition values(1)")
      val df = spark.sql("select * from hive_table_without_partition")
      assert(df.collect().toList.head.get(0) == "1")
      val plan = df.queryExecution.executedPlan
      assert(collect(plan) { case e: NativeParquetHiveTableScanExec =>
        e
      }.size == 1)
    }
  }

  test("test hive parquet table partition to native") {
    withTempView("hive_table_with_partition") {
      spark.sql(
        "create table hive_table_with_partition (a string) stored as parquet partitioned by(pt string)")
      spark.sql("insert into hive_table_with_partition partition(pt='2026-03-10') values('1')")
      spark.sql("insert into hive_table_with_partition partition(pt='2026-03-11') values('1')")
      val df = spark.sql("select * from hive_table_with_partition where pt = '2026-03-10'")
      df.show()
      assert(df.collect().toList.head.get(0) == "1")
      assert(df.collect().toList.head.get(1) == "2026-03-10")
      val plan = df.queryExecution.executedPlan
      assert(collect(plan) { case e: NativeParquetHiveTableScanExec =>
        e
      }.size == 1)
    }
  }

}
