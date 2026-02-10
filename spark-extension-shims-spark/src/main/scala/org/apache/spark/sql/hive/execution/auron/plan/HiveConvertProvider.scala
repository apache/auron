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

package org.apache.spark.sql.hive.execution.auron.plan

import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.AuronConverters.getBooleanConf
import org.apache.spark.sql.auron.{AuronConvertProvider, AuronConverters}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.execution.HiveTableScanExec

class HiveConvertProvider extends AuronConvertProvider with Logging {
  override def isEnabled: Boolean =
    getBooleanConf("spark.auron.enable.hiveTable", defaultValue = true)

  def enableHiveTableScanExec: Boolean =
    getBooleanConf("spark.auron.enable.hiveTableScanExec", defaultValue = false)

  override def isSupported(exec: SparkPlan): Boolean =
    exec match {
      case e: HiveTableScanExec if enableHiveTableScanExec &&
        e.relation.tableMeta.provider.isDefined &&
        e.relation.tableMeta.provider.get.equals("hive") =>
        true
    case _ => false
  }

  override def convert(exec: SparkPlan): SparkPlan = {
    exec match {
      case hiveExec: HiveTableScanExec if enableHiveTableScanExec =>
        convertHiveTableScanExec(hiveExec)
      case _ => exec
    }
  }

  def convertHiveTableScanExec(hiveExec: HiveTableScanExec): SparkPlan = {
    AuronConverters.addRenameColumnsExec(NativeHiveTableScanExec(hiveExec))
  }
}
