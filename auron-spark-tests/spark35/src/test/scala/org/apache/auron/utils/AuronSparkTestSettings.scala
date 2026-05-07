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
package org.apache.auron.utils

import org.apache.spark.sql._

class AuronSparkTestSettings extends SparkTestSettings {
  {
    // Use Arrow's unsafe implementation.
    System.setProperty("arrow.allocation.manager.type", "Unsafe")
  }

  enableSuite[AuronDataFrameFunctionsSuite]
    .exclude("map with arrays")
    .exclude("map_concat function")
    .exclude("reverse function - array for primitive type not containing null")
    .exclude("reverse function - array for primitive type containing null")
    .exclude("reverse function - array for non-primitive type")
    .exclude("flatten function")
    .exclude("SPARK-24734: Fix containsNull of Concat for array type")
    .exclude("array_insert functions")
    .exclude("transform keys function - Invalid lambda functions and exceptions")

  enableSuite[AuronDateFunctionsSuite]
    .exclude("function to_date")
    .exclude("function date_trunc")
    .exclude("unsupported fmt fields for trunc/date_trunc results null")
    .exclude("unix_timestamp")
    .exclude("to_unix_timestamp")
    .exclude("SPARK-30766: date_trunc of old timestamps to hours and days")

  enableSuite[AuronMathFunctionsSuite]
    .exclude("acosh")

  enableSuite[AuronMiscFunctionsSuite]

  enableSuite[AuronStringFunctionsSuite]
    .exclude("string Levenshtein distance")
    .exclude("string / binary substring function")

  override def getSQLQueryTestSettings: SQLQueryTestSettings = new SQLQueryTestSettings {
    override def getResourceFilePath: String = ""
    override def getSupportedSQLQueryTests: Set[String] = Set.empty
    override def getOverwriteSQLQueryTests: Set[String] = Set.empty
  }
}
