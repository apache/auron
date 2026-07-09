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

import scala.collection.JavaConverters._

import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.iceberg.Table
import org.apache.iceberg.types.TypeUtil

object AuronIcebergSourceUtil {

  final case class RenameOrDrop(topLevel: Boolean, nested: Boolean)

  def getClassOfSparkBatchQueryScan(): Class[SparkBatchQueryScan] = {
    classOf[SparkBatchQueryScan]
  }

  def getClassOfSparkInputPartition(): Class[SparkInputPartition] = {
    classOf[SparkInputPartition]
  }

  def expectedFieldIds(scan: AnyRef): Map[String, Int] = {
    val expectedSchema = asBatchQueryScan(scan).expectedSchema()
    expectedSchema.columns().asScala.map(field => field.name() -> field.fieldId()).toMap
  }

  def expectedFieldIdsForChangelogScan(scan: AnyRef): Map[String, Int] = {
    // SparkChangelogScan does not expose Iceberg expectedSchema/table accessors.
    // Keep the internal field-name assumptions localized here; callers fallback if they change.
    val expectedSchema =
      FieldUtils.readField(scan, "expectedSchema", true).asInstanceOf[org.apache.iceberg.Schema]
    expectedSchema.columns().asScala.map(field => field.name() -> field.fieldId()).toMap
  }

  def detectRenameOrDrop(scan: AnyRef): RenameOrDrop = {
    val table = asBatchQueryScan(scan).table()
    detectRenameOrDrop(table)
  }

  def detectRenameOrDropForChangelogScan(scan: AnyRef): RenameOrDrop = {
    // SparkChangelogScan does not expose its Iceberg table.
    val table = FieldUtils.readField(scan, "table", true).asInstanceOf[Table]
    detectRenameOrDrop(table)
  }

  private def detectRenameOrDrop(table: Table): RenameOrDrop = {
    val currentFields = collectFieldIdToName(table.schema())

    table
      .schemas()
      .asScala
      .filterNot(_._1 == table.schema().schemaId())
      .values
      .foldLeft(RenameOrDrop(topLevel = false, nested = false)) { (result, schema) =>
        collectFieldIdToName(schema).foldLeft(result) {
          case (currentResult, (fieldId, historicalField)) =>
            currentFields.get(fieldId) match {
              case Some(currentField) if currentField.name != historicalField.name =>
                if (historicalField.topLevel || currentField.topLevel) {
                  currentResult.copy(topLevel = true)
                } else {
                  currentResult.copy(nested = true)
                }
              case None =>
                if (historicalField.topLevel) {
                  currentResult.copy(topLevel = true)
                } else {
                  currentResult.copy(nested = true)
                }
              case _ =>
                currentResult
            }
        }
      }
  }

  final private case class FieldIdentity(name: String, topLevel: Boolean)

  private def collectFieldIdToName(schema: org.apache.iceberg.Schema): Map[Int, FieldIdentity] = {
    val topLevelFieldIds = schema.columns().asScala.map(_.fieldId()).toSet
    TypeUtil
      .indexById(schema.asStruct())
      .asScala
      .map { case (fieldId, field) =>
        fieldId.toInt -> FieldIdentity(field.name(), topLevelFieldIds.contains(fieldId.toInt))
      }
      .toMap
  }

  private def asBatchQueryScan(scan: AnyRef): SparkBatchQueryScan =
    scan.asInstanceOf[SparkBatchQueryScan]
}
