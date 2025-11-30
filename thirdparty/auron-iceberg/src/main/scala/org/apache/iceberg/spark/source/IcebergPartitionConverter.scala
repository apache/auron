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

import java.nio.ByteBuffer

import org.apache.iceberg.{FileScanTask, Table}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

// Converts Iceberg partition data to Spark InternalRow.
class IcebergPartitionConverter(table: Table) {

  private case class FieldAccessor(javaClass: Class[_], convert: Any => Any)

  private val partitionType = table.spec().partitionType()
  private val sparkPartitionSchema: StructType =
    SparkSchemaUtil.convert(partitionType.asSchema())

  require(
    partitionType.fields().size() == sparkPartitionSchema.fields.length,
    s"Mismatch between Iceberg partition fields (${partitionType.fields().size()}) " +
      s"and Spark partition schema (${sparkPartitionSchema.fields.length})")

  private val fieldAccessors: Array[FieldAccessor] = {
    val sFields = sparkPartitionSchema.fields

    def javaClassFor(dt: DataType): Class[_] = dt match {
      case BooleanType => classOf[java.lang.Boolean]
      case IntegerType | DateType => classOf[java.lang.Integer]
      case LongType | TimestampType => classOf[java.lang.Long]
      case FloatType => classOf[java.lang.Float]
      case DoubleType => classOf[java.lang.Double]
      case StringType => classOf[CharSequence]
      case BinaryType => classOf[java.nio.ByteBuffer]
      case _: DecimalType => classOf[java.math.BigDecimal]
      case other =>
        throw new UnsupportedOperationException(s"Unsupported Spark partition type: $other")
    }

    def converterFor(dt: DataType): Any => Any = dt match {
      case StringType =>
        (raw: Any) =>
          if (raw == null) null
          else
            raw match {
              case cs: CharSequence => UTF8String.fromString(cs.toString)
              case other => UTF8String.fromString(other.toString)
            }

      case IntegerType | BooleanType | LongType | FloatType | DoubleType =>
        (raw: Any) => raw

      case DateType =>
        (raw: Any) =>
          if (raw == null) null
          else raw.asInstanceOf[Integer].intValue()

      case TimestampType =>
        (raw: Any) =>
          if (raw == null) null
          else raw.asInstanceOf[Long]

      case BinaryType =>
        (raw: Any) =>
          if (raw == null) null
          else
            raw match {
              case bb: ByteBuffer =>
                val dup = bb.duplicate()
                val arr = new Array[Byte](dup.remaining())
                dup.get(arr)
                arr
              case arr: Array[Byte] => arr
              case other =>
                throw new IllegalArgumentException(
                  s"Unexpected binary partition value type: ${other.getClass}")
            }

      case d: DecimalType =>
        (raw: Any) =>
          if (raw == null) null
          else {
            val bd: java.math.BigDecimal = raw match {
              case bd: java.math.BigDecimal => bd
              case s: String => new java.math.BigDecimal(s)
              case other => new java.math.BigDecimal(other.toString)
            }
            Decimal(bd, d.precision, d.scale)
          }

      case other =>
        (_: Any) =>
          throw new UnsupportedOperationException(
            s"Unsupported Spark partition type in converter: $other")
    }

    sFields.map { field =>
      val dt = field.dataType
      FieldAccessor(javaClass = javaClassFor(dt), convert = converterFor(dt))
    }
  }

  def convert(task: FileScanTask): InternalRow = {
    val partitionData = task.file().partition()
    if (partitionData == null || fieldAccessors.isEmpty) {
      InternalRow.empty
    } else {
      val values = fieldAccessors.indices.map { i =>
        val accessor = fieldAccessors(i)
        val jcls = accessor.javaClass.asInstanceOf[Class[Any]]
        val raw = partitionData.get(i, jcls)
        accessor.convert(raw)
      }
      InternalRow.fromSeq(values)
    }
  }

  def schema: StructType = sparkPartitionSchema
}
