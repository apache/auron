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
package org.apache.spark.sql.auron.iceberg

import java.math.BigDecimal

import org.apache.iceberg.{FileFormat, Schema}
import org.apache.iceberg.expressions.{Binder, Expression, Expressions}
import org.apache.iceberg.types.Types
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{And, GreaterThanOrEqual, LessThanOrEqual, Literal}
import org.apache.spark.sql.types.{Decimal, DecimalType, StructField, StructType}

class IcebergDecimalPruningSuite extends SparkFunSuite {
  private val decimalType = DecimalType(18, 2)
  private val schema = StructType(Seq(StructField("d", decimalType)))

  test("decimal comparisons preserve the column's precision and scale") {
    val value = new BigDecimal("-12.340")
    val predicates = Seq(
      Expressions.equal("d", value),
      Expressions.lessThan("d", value),
      Expressions.lessThanOrEqual("d", value),
      Expressions.greaterThan("d", value),
      Expressions.greaterThanOrEqual("d", value))
    predicates.foreach { predicate =>
      val converted = convert(predicate).get
      val literals = converted.collect { case literal: Literal => literal }
      assert(literals.nonEmpty)
      literals.foreach { literal =>
        assert(literal.dataType == decimalType)
        assert(literal.value.asInstanceOf[Decimal].toJavaBigDecimal == new BigDecimal("-12.34"))
      }
    }
    val equality = convert(predicates.head).get
    assert(equality.isInstanceOf[And])
    assert(equality.children.head.isInstanceOf[GreaterThanOrEqual])
    assert(equality.children.last.isInstanceOf[LessThanOrEqual])
  }

  test("bound decimal predicates use the same conversion as unbound predicates") {
    val icebergSchema =
      new Schema(Types.NestedField.optional(1, "d", Types.DecimalType.of(18, 2)))
    val predicate = Expressions.equal("d", new BigDecimal("1.00"))
    val bound = Binder.bind(icebergSchema.asStruct(), predicate, true)
    assert(convert(bound).get.sql == convert(predicate).get.sql)
  }

  test("decimal conversion is exact at supported scale and precision boundaries") {
    Seq(
      (DecimalType(18, 0), "999999999999999999"),
      (DecimalType(18, 2), "-9999999999999999.99"),
      (DecimalType(18, 18), "0.999999999999999999"),
      (DecimalType(9, 2), "0.000")).foreach { case (dataType, value) =>
      val readSchema = StructType(Seq(StructField("d", dataType)))
      assert(
        IcebergScanSupport
          .convertIcebergFilterExpression(
            Expressions.equal("d", new BigDecimal(value)),
            readSchema,
            FileFormat.PARQUET)
          .nonEmpty)
    }
    Seq("1.001", "10000000000000000.00").foreach { value =>
      assert(convert(Expressions.equal("d", new BigDecimal(value))).isEmpty)
    }
    assert(convert(Expressions.equal("d", Int.box(1))).isEmpty)
    assert(
      IcebergScanSupport
        .convertIcebergFilterExpression(
          Expressions.equal("d", new BigDecimal("1")),
          StructType(Seq(StructField("d", DecimalType(19, 2)))),
          FileFormat.PARQUET)
        .isEmpty)
  }

  test("unsupported decimal operators and compound predicates remain conservative") {
    val value = new BigDecimal("1.00")
    val supported = Expressions.equal("d", value)
    val unsupported = Seq(
      Expressions.notEqual("d", value),
      Expressions.in("d", value, new BigDecimal("2.00")),
      Expressions.notIn("d", value, new BigDecimal("2.00")),
      Expressions.not(supported))
    unsupported.foreach { predicate =>
      assert(convert(predicate).isEmpty)
      assert(convert(Expressions.and(supported, predicate)).isEmpty)
      assert(convert(Expressions.or(supported, predicate)).isEmpty)
    }
    assert(convert(Expressions.or(supported, Expressions.lessThan("d", value))).nonEmpty)
  }

  test("ORC decimal comparisons stay disabled while existing null pruning is preserved") {
    assert(convert(Expressions.equal("d", new BigDecimal("1.00")), FileFormat.ORC).isEmpty)
    Seq(FileFormat.PARQUET, FileFormat.ORC).foreach { format =>
      assert(convert(Expressions.isNull("d"), format).nonEmpty)
      assert(convert(Expressions.notNull("d"), format).nonEmpty)
      assert(convert(Expressions.not(Expressions.isNull("d")), format).nonEmpty)
    }
  }

  private def convert(predicate: Expression, format: FileFormat = FileFormat.PARQUET) =
    IcebergScanSupport.convertIcebergFilterExpression(predicate, schema, format)
}
