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
package org.apache.spark.sql.auron.paimon

import org.apache.spark.internal.Logging
import org.apache.spark.sql.auron.AuronConvertStrategy
import org.apache.spark.sql.auron.util.AuronLogUtils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.auron.plan.NativePaimonWriteExec
import org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec

/** Native Paimon write entry used by PaimonConvertProvider (Paimon 2.0 implementation). */
object PaimonWriteConverter extends Logging {

  def isPaimonWrite(exec: SparkPlan): Boolean = PaimonWriteSupport.isPaimonWrite(exec)

  def isSupported(exec: V2ExistingTableWriteExec): Boolean = {
    PaimonWriteSupport.unsupportedReason(exec) match {
      case Some(reason) =>
        logWarning(s"Skip native paimon write: $reason")
        false
      case None => true
    }
  }

  def convert(exec: V2ExistingTableWriteExec): SparkPlan = {
    PaimonWriteSupport.plan(exec) match {
      case Right(plan) =>
        AuronLogUtils.logDebugPlanConversion(
          exec,
          Seq("table" -> plan.table.fullName(), "overwrite" -> plan.overwritePartitions))
        val nativeWrite = NativePaimonWriteExec(
          exec.query,
          plan.write,
          plan.table,
          exec.refreshCache,
          exec.getClass.getSimpleName)
        // the write node is a command (not NativeSupports) whose input is converted at
        // execution time; mark it convertible so the convert strategy keeps it
        nativeWrite.setTagValue(AuronConvertStrategy.convertibleTag, true)
        nativeWrite
      case Left(reason) =>
        throw new NotImplementedError(s"native paimon write is not supported: $reason")
    }
  }
}
