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

import org.apache.spark.sql.AuronQueryTest
import org.apache.spark.sql.auron.{AuronConverters, EmptyNativeRDD, NativeRDD}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.auron.plan.{NativeOrcScanExec, NativeParquetScanExec}

class AuronEmptyNativeRddSuite
    extends AuronQueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {

  test("test parquet table generate EmptyNativeRDD scenarios") {
    withTable("t1") {
      sql(
        "create table t1 using parquet PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val emptySparkPlan = sql("select * from t1 where part = '123'")
      val emptyExecutePlan = emptySparkPlan.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
      val emptyNativeParquetScanExec =
        AuronConverters.convertSparkPlan(emptyExecutePlan.executedPlan).collectFirst {
          case nativeParquetScanExec: NativeParquetScanExec =>
            nativeParquetScanExec
        }
      val emptyNativeRDD = emptyNativeParquetScanExec.get.doExecuteNative()
      assert(emptyNativeRDD.isInstanceOf[EmptyNativeRDD])

      val sparkPlan = sql("select * from t1")
      val executePlan = sparkPlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val nativeParquetScanExec =
        AuronConverters.convertSparkPlan(executePlan.executedPlan).collectFirst {
          case nativeParquetScanExec: NativeParquetScanExec =>
            nativeParquetScanExec
        }
      val nativeRDD = nativeParquetScanExec.get.doExecuteNative()
      assert(nativeRDD.isInstanceOf[NativeRDD])
    }
  }

  test("test orc table generate EmptyNativeRDD scenarios") {
    withTable("t1") {
      sql(
        "create table t1 using orc PARTITIONED BY (part) as select 1 as c1, 2 as c2, 'test test' as part")
      val emptySparkPlan =
        sql("select * from t1 where part = '123'")
      val emptyExecutePlan = emptySparkPlan.queryExecution.executedPlan
        .asInstanceOf[AdaptiveSparkPlanExec]
      val emptyNativeOrcScanExec =
        AuronConverters.convertSparkPlan(emptyExecutePlan.executedPlan).collectFirst {
          case nativeOrcScanExec: NativeOrcScanExec =>
            nativeOrcScanExec
        }
      val emptyNativeRDD = emptyNativeOrcScanExec.get.doExecuteNative()
      assert(emptyNativeRDD.isInstanceOf[EmptyNativeRDD])

      val sparkPlan = sql("select * from t1")
      val executePlan =
        sparkPlan.queryExecution.executedPlan.asInstanceOf[AdaptiveSparkPlanExec]
      val nativeOrcScanExec =
        AuronConverters.convertSparkPlan(executePlan.executedPlan).collectFirst {
          case nativeOrcScanExec: NativeOrcScanExec =>
            nativeOrcScanExec
        }
      val nativeRDD = nativeOrcScanExec.get.doExecuteNative()
      assert(nativeRDD.isInstanceOf[NativeRDD])
    }
  }
}
