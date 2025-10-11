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
package org.apache.auron.common

import java.util.Properties

import scala.util.Try

object AuronBuildInfo {

  private val buildFile = "auron-build-info.properties"
  private val buildFileStream =
    Thread.currentThread().getContextClassLoader.getResourceAsStream(buildFile)

  if (buildFileStream == null) {
    throw new Exception(s"Can not load the core build file: $buildFile")
  }

  private val unknown = "NULL"

  private val props = new Properties()

  try {
    props.load(buildFileStream)
  } finally {
    Try(buildFileStream.close())
  }

  val VERSION_STRING: String = "PROJECT VERSION"
  val JAVA_COMPILE_VERSION_STRING: String = "JAVA VERSION"
  val SCALA_COMPILE_VERSION_STRING: String = "SCALA VERSION"
  val SPARK_COMPILE_VERSION_STRING: String = "SPARK VERSION"
  val RUST_COMPILE_VERSION_STRING: String = "RUST VERSION"
  val CELEBORN_VERSION_STRING: String = "CELEBRON VERSION"
  val UNIFFLE_VERSION_STRING: String = "UNIFFLE VERSION"
  val PAIMON_VERSION_STRING: String = "PAIMON VERSION"
  val FLINK_VERSION_STRING: String = "FLINK VERSION"
  val BUILD_DATE_STRING: String = "BUILD TIMESTAMP"

  val VERSION: String = props.getProperty("project.version", unknown)
  val JAVA_COMPILE_VERSION: String = props.getProperty("java.version", unknown)
  val SCALA_COMPILE_VERSION: String = props.getProperty("scala.version", unknown)
  val SPARK_COMPILE_VERSION: String = props.getProperty("spark.version", unknown)
  val RUST_COMPILE_VERSION: String = props.getProperty("rust.version", unknown)
  val CELEBORN_VERSION: String = props.getProperty("celeborn.version", unknown)
  val UNIFFLE_VERSION: String = props.getProperty("uniffle.version", unknown)
  val PAIMON_VERSION: String = props.getProperty("paimon.version", unknown)
  val FLINK_VERSION: String = props.getProperty("flink.version", unknown)
  val BUILD_DATE: String = props.getProperty("build.timestamp", unknown)
}
