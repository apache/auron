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
package org.apache.spark.sql.auron.hudi

import java.net.URI
import java.util.{Locale, Properties}

import scala.collection.JavaConverters._
import scala.util.Using

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.HadoopFsRelation

object HudiScanSupport extends Logging {
  sealed trait HudiFileFormat
  case object ParquetFormat extends HudiFileFormat
  case object OrcFormat extends HudiFileFormat

  private val hudiParquetFileFormatSuffix = "HoodieParquetFileFormat"
  private val newHudiParquetFileFormatSuffix = "NewHoodieParquetFileFormat"
  private val hudiOrcFileFormatSuffix = "HoodieOrcFileFormat"
  private val newHudiOrcFileFormatSuffix = "NewHoodieOrcFileFormat"
  private val morTableTypes = Set("merge_on_read", "mor")
  private val hudiTableTypeKeys = Seq(
    "hoodie.datasource.write.table.type",
    "hoodie.datasource.read.table.type",
    "hoodie.table.type")
  private val hudiBaseFileFormatKeys = Seq(
    "hoodie.table.base.file.format",
    "hoodie.datasource.write.base.file.format",
    "hoodie.datasource.write.storage.type")

  def fileFormat(scan: FileSourceScanExec): Option[HudiFileFormat] = {
    lazy val catalog = catalogTable(scan.relation)
    lazy val tableProperties = hudiTablePropertiesFromMeta(scan.relation.options)
    fileFormat(scan, catalog, tableProperties)
  }

  private def fileFormat(
      scan: FileSourceScanExec,
      catalogTable: => Option[CatalogTable],
      tableProperties: => Option[Properties]): Option[HudiFileFormat] = {
    val fileFormatName = scan.relation.fileFormat.getClass.getName
    val fromClass = fileFormat(fileFormatName)
    if (fromClass.nonEmpty) {
      return fromClass
    }
    // Spark may report generic Orc/Parquet formats for Hudi; use metadata fallback
    // only when the underlying file index indicates a Hudi table.
    fileFormatFromMeta(scan, catalogTable, tableProperties, fileFormatName)
  }

  private[hudi] def fileFormat(fileFormatName: String): Option[HudiFileFormat] = {
    logDebug(s"Hudi fileFormat resolved to: ${fileFormatName}")
    if (fileFormatName.endsWith(newHudiParquetFileFormatSuffix) ||
      fileFormatName.endsWith(newHudiOrcFileFormatSuffix)) {
      return None
    }
    if (fileFormatName.endsWith(hudiParquetFileFormatSuffix)) {
      return Some(ParquetFormat)
    }
    if (fileFormatName.endsWith(hudiOrcFileFormatSuffix)) {
      return Some(OrcFormat)
    }
    None
  }

  def isSupported(scan: FileSourceScanExec): Boolean =
    supportedFileFormat(scan).nonEmpty

  def supportedFileFormat(scan: FileSourceScanExec): Option[HudiFileFormat] = {
    lazy val catalog = catalogTable(scan.relation)
    lazy val tableProperties = hudiTablePropertiesFromMeta(scan.relation.options)
    val resolvedFileFormat = fileFormat(scan, catalog, tableProperties)
    if (isSupported(resolvedFileFormat, scan.relation.options, catalog, tableProperties)) {
      resolvedFileFormat
    } else {
      None
    }
  }

  private[hudi] def isSupported(fileFormatName: String, options: Map[String, String]): Boolean = {
    isSupported(fileFormat(fileFormatName), options, None, None)
  }

  private[hudi] def isSupported(
      fileFormat: Option[HudiFileFormat],
      options: Map[String, String],
      catalogTable: => Option[CatalogTable],
      tableProperties: => Option[Properties]): Boolean = {
    if (fileFormat.isEmpty) {
      return false
    }
    if (hasTimeTravel(options)) {
      return false
    }

    val tableType = tableTypeFromOptions(options)
      .orElse(tableTypeFromCatalog(catalogTable))
      .orElse(tableTypeFromMeta(tableProperties))
      .map(_.toLowerCase(Locale.ROOT))

    logDebug(s"Hudi tableType resolved to: ${tableType.getOrElse("unknown")}")

    // Only support basic COW tables for the base version.
    !tableType.exists(morTableTypes.contains)
  }

  private def tableTypeFromOptions(options: Map[String, String]): Option[String] = {
    caseInsensitiveValue(options, hudiTableTypeKeys)
  }

  private[hudi] def baseFileFormatFromOptions(options: Map[String, String]): Option[String] = {
    caseInsensitiveValue(options, hudiBaseFileFormatKeys)
  }

  private def tableTypeFromMeta(tableProperties: Option[Properties]): Option[String] =
    tableProperties.flatMap(props =>
      caseInsensitivePropertyValue(props, Seq("hoodie.table.type")))

  private def baseFileFormatFromMeta(tableProperties: Option[Properties]): Option[String] =
    tableProperties.flatMap(props =>
      caseInsensitivePropertyValue(props, Seq("hoodie.table.base.file.format")))

  private def hudiTablePropertiesFromMeta(options: Map[String, String]): Option[Properties] = {
    val basePath = caseInsensitiveValue(options, Seq("path")).map(normalizePath)
    basePath.flatMap { path =>
      try {
        val hadoopConf = SparkSession.active.sessionState.newHadoopConf()
        val base = new Path(path)
        val fs = base.getFileSystem(hadoopConf)
        val propsPath = new Path(base, ".hoodie/hoodie.properties")
        if (!fs.exists(propsPath)) {
          if (log.isDebugEnabled()) {
            logDebug(s"Hudi table properties not found at: $propsPath")
          }
          None
        } else {
          val props = new Properties()
          Using.resource(fs.open(propsPath)) { in =>
            props.load(in)
          }
          Some(props)
        }
      } catch {
        case t: Throwable =>
          if (log.isDebugEnabled()) {
            logDebug(s"Failed to load Hudi table properties from $path", t)
          }
          None
      }
    }
  }

  private def baseFileFormatFromCatalog(catalogTable: Option[CatalogTable]): Option[String] = {
    catalogTable.flatMap { table =>
      val props = table.properties ++ table.storage.properties
      caseInsensitiveValue(props, hudiBaseFileFormatKeys)
    }
  }

  private def fileFormatFromMeta(
      scan: FileSourceScanExec,
      catalogTable: => Option[CatalogTable],
      tableProperties: => Option[Properties],
      fileFormatName: String): Option[HudiFileFormat] = {
    // Avoid treating non-Hudi tables as Hudi when Spark reports generic formats.
    if (!isHudiFileIndex(scan.relation.location)) {
      return None
    }
    val baseFormat = baseFileFormatFromOptions(scan.relation.options)
      .orElse(baseFileFormatFromCatalog(catalogTable))
      .orElse(baseFileFormatFromMeta(tableProperties))
      .map(_.toLowerCase(Locale.ROOT))
    baseFormat.flatMap {
      case "orc" if fileFormatName.contains("OrcFileFormat") => Some(OrcFormat)
      case "parquet" if fileFormatName.contains("ParquetFileFormat") => Some(ParquetFormat)
      case _ => None
    }
  }

  private def tableTypeFromCatalog(catalogTable: Option[CatalogTable]): Option[String] = {
    catalogTable.flatMap { table =>
      val props = table.properties ++ table.storage.properties
      caseInsensitiveValue(props, hudiTableTypeKeys)
    }
  }

  private def catalogTable(relation: HadoopFsRelation): Option[CatalogTable] = {
    val method = relation.getClass.getMethods.find(_.getName == "catalogTable")
    method.flatMap { m =>
      try {
        m.invoke(relation) match {
          case opt: Option[_] => opt.asInstanceOf[Option[CatalogTable]]
          case table: CatalogTable => Some(table)
          case _ => None
        }
      } catch {
        case _: Throwable => None
      }
    }
  }

  private def isHudiFileIndex(fileIndex: AnyRef): Boolean = {
    var current: Class[_] = fileIndex.getClass
    while (current != null) {
      if (current.getName.endsWith("HoodieFileIndex")) {
        return true
      }
      current = current.getSuperclass
    }
    false
  }

  private def hasTimeTravel(options: Map[String, String]): Boolean = {
    caseInsensitiveValue(
      options,
      Seq(
        "as.of.instant",
        "as.of.timestamp",
        "hoodie.datasource.read.as.of.instant",
        "hoodie.datasource.read.as.of.timestamp")).isDefined
  }

  private def normalizePath(rawPath: String): String = {
    try {
      val uri = new URI(rawPath)
      if (uri.getScheme == null) rawPath else uri.toString
    } catch {
      case _: Throwable => rawPath
    }
  }

  private def caseInsensitivePropertyValue(
      props: Properties,
      keys: Seq[String]): Option[String] = {
    keys.iterator
      .map { lookupKey =>
        Option(props.getProperty(lookupKey)).orElse {
          props.stringPropertyNames().asScala.iterator.collectFirst {
            case key if key.equalsIgnoreCase(lookupKey) => props.getProperty(key)
          }
        }
      }
      .collectFirst { case Some(value) => value }
  }

  private def caseInsensitiveValue(
      values: Map[String, String],
      keys: Seq[String]): Option[String] = {
    keys.iterator
      .map { lookupKey =>
        values.get(lookupKey).orElse {
          values.iterator.collectFirst {
            case (key, value) if key.equalsIgnoreCase(lookupKey) => value
          }
        }
      }
      .collectFirst { case Some(value) => value }
  }
}
