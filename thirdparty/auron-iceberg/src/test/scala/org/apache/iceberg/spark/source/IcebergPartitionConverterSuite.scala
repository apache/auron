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

import java.lang.reflect.{Method, Proxy}
import java.nio.ByteBuffer
import java.util

import org.apache.iceberg.{FileScanTask, PartitionSpec, Schema, StructLike}
import org.apache.iceberg.types.Types
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

class IcebergPartitionConverterSuite extends AnyFunSuite {

  private case class TestStruct(values: Array[Any]) extends StructLike {
    override def size(): Int = values.length
    override def get[T](pos: Int, clazz: Class[T]): T = values(pos).asInstanceOf[T]
    override def set[T](pos: Int, value: T): Unit = values(pos) = value
  }

  private def proxyFor[T](iface: Class[T])(
      pf: PartialFunction[(Method, Array[AnyRef]), AnyRef]): T = {
    Proxy
      .newProxyInstance(
        iface.getClassLoader,
        Array[Class[_]](iface),
        (proxy: Any, method: Method, args: Array[AnyRef]) => {
          val key = (method, Option(args).getOrElse(Array.empty[AnyRef]))
          if (pf.isDefinedAt(key)) pf(key)
          else throw new UnsupportedOperationException(s"Unexpected call: ${method.getName}")
        })
      .asInstanceOf[T]
  }

  private def buildSpec(): (Schema, PartitionSpec) = {
    val schema = new Schema(
      util.Arrays.asList(
        Types.NestedField.required(1, "b", Types.BooleanType.get()),
        Types.NestedField.required(2, "i", Types.IntegerType.get()),
        Types.NestedField.required(3, "l", Types.LongType.get()),
        Types.NestedField.required(4, "s", Types.StringType.get()),
        Types.NestedField.required(5, "bin", Types.BinaryType.get()),
        Types.NestedField.required(6, "d", Types.DateType.get()),
        Types.NestedField.required(7, "ts", Types.TimestampType.withZone()),
        Types.NestedField.required(8, "dec", Types.DecimalType.of(10, 2))))
    val spec = PartitionSpec
      .builderFor(schema)
      .identity("b")
      .identity("i")
      .identity("l")
      .identity("s")
      .identity("bin")
      .identity("d")
      .identity("ts")
      .identity("dec")
      .build()
    (schema, spec)
  }

  private def tableWithSpec(spec: PartitionSpec): org.apache.iceberg.Table = {
    proxyFor(classOf[org.apache.iceberg.Table]) {
      case (m, _) if m.getName == "spec" => spec
      case (m, _) if m.getName == "schema" => spec.schema()
      case (m, _) if m.getName == "name" => "tbl"
      case (m, _) if m.getName == "toString" => "tbl"
    }
  }

  private def fileScanTaskWithPartition(spec: PartitionSpec, struct: StructLike): FileScanTask = {
    val contentFile = proxyFor(classOf[org.apache.iceberg.ContentFile[_]]) {
      case (m, _) if m.getName == "partition" => struct
    }
    proxyFor(classOf[FileScanTask]) {
      case (m, _) if m.getName == "file" => contentFile
      case (m, _) if m.getName == "spec" => spec
    }
  }

  test("convert converts common partition value types correctly") {
    val (_, spec) = buildSpec()
    val table = tableWithSpec(spec)
    val converter = new IcebergPartitionConverter(table)

    val bb = ByteBuffer.wrap(Array[Byte](1, 2, 3))
    val jbd = new java.math.BigDecimal("1234.56")

    val struct = TestStruct(
      Array[Any](
        java.lang.Boolean.TRUE,
        Integer.valueOf(42),
        java.lang.Long.valueOf(7L),
        "hello",
        bb,
        Integer.valueOf(19358),
        java.lang.Long.valueOf(1234567890L),
        jbd))

    val task = fileScanTaskWithPartition(spec, struct)
    val row: InternalRow = converter.convert(task)

    assert(row.getBoolean(0))
    assert(row.getInt(1) == 42)
    assert(row.getLong(2) == 7L)
    assert(row.getUTF8String(3) == UTF8String.fromString("hello"))
    assert(row.getBinary(4).sameElements(Array[Byte](1, 2, 3)))
    assert(row.getInt(5) == 19358)
    assert(row.getLong(6) == 1234567890L)

    val dec = row.getDecimal(7, 10, 2)
    val expected = Decimal(jbd, 10, 2)
    assert(dec.equals(expected))
  }

  test("convert returns empty for unpartitioned table") {
    val schema =
      new Schema(util.Arrays.asList(Types.NestedField.required(1, "id", Types.IntegerType.get())))
    val spec = PartitionSpec.unpartitioned()
    val table = tableWithSpec(spec)
    val converter = new IcebergPartitionConverter(table)

    val task = fileScanTaskWithPartition(spec, null)
    val row = converter.convert(task)
    assert(row eq InternalRow.empty)
  }

  test("convert supports timestamp without zone (NTZ)") {
    val schema = new Schema(
      util.Arrays.asList(Types.NestedField.required(1, "ts", Types.TimestampType.withoutZone())))
    val spec = PartitionSpec.builderFor(schema).identity("ts").build()
    val table = tableWithSpec(spec)
    val converter = new IcebergPartitionConverter(table)

    val micros = java.lang.Long.valueOf(42L)
    val struct = TestStruct(Array[Any](micros))
    val task = fileScanTaskWithPartition(spec, struct)
    val row = converter.convert(task)
    assert(row.getLong(0) == 42L)
  }

  test("convert preserves nulls for all supported types") {
    val schema = new Schema(
      util.Arrays.asList(
        Types.NestedField.required(1, "b", Types.BooleanType.get()),
        Types.NestedField.required(2, "i", Types.IntegerType.get()),
        Types.NestedField.required(3, "l", Types.LongType.get()),
        Types.NestedField.required(4, "s", Types.StringType.get()),
        Types.NestedField.required(5, "bin", Types.BinaryType.get()),
        Types.NestedField.required(6, "d", Types.DateType.get()),
        Types.NestedField.required(7, "ts", Types.TimestampType.withZone()),
        Types.NestedField.required(8, "dec", Types.DecimalType.of(10, 2))))
    val spec = PartitionSpec
      .builderFor(schema)
      .identity("b")
      .identity("i")
      .identity("l")
      .identity("s")
      .identity("bin")
      .identity("d")
      .identity("ts")
      .identity("dec")
      .build()
    val table = tableWithSpec(spec)
    val converter = new IcebergPartitionConverter(table)

    val struct = TestStruct(Array[Any](null, null, null, null, null, null, null, null))
    val task = fileScanTaskWithPartition(spec, struct)
    val row = converter.convert(task)
    (0 until 8).foreach { i => assert(row.isNullAt(i)) }
  }

  test("convert handles partition evolution via per-task spec") {
    val schema = new Schema(
      util.Arrays.asList(
        Types.NestedField.required(1, "i", Types.IntegerType.get()),
        Types.NestedField.required(2, "l", Types.LongType.get())))
    val specI = PartitionSpec.builderFor(schema).identity("i").build()
    val specL = PartitionSpec.builderFor(schema).identity("l").build()
    val table = tableWithSpec(specI)
    val converter = new IcebergPartitionConverter(table)

    val taskI = fileScanTaskWithPartition(specI, TestStruct(Array[Any](Integer.valueOf(7))))
    val rowI = converter.convert(taskI)
    assert(rowI.getInt(0) == 7)

    val taskL =
      fileScanTaskWithPartition(specL, TestStruct(Array[Any](java.lang.Long.valueOf(9L))))
    val rowL = converter.convert(taskL)
    assert(rowL.getLong(0) == 9L)
  }

  test("decimal conversion enforces exact scale (no silent rounding)") {
    val schema = new Schema(
      util.Arrays.asList(Types.NestedField.required(1, "dec", Types.DecimalType.of(10, 2))))
    val spec = PartitionSpec.builderFor(schema).identity("dec").build()
    val table = tableWithSpec(spec)
    val converter = new IcebergPartitionConverter(table)

    val badScale = new java.math.BigDecimal("12.345") // scale 3 instead of 2
    val struct = TestStruct(Array[Any](badScale))
    val task = fileScanTaskWithPartition(spec, struct)
    assertThrows[ArithmeticException] { converter.convert(task) }
  }
}
