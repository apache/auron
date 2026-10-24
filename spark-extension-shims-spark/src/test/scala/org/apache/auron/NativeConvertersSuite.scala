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
import org.apache.spark.sql.auron.NativeConverters
import org.apache.spark.sql.catalyst.expressions.{Cast, Literal, TruncTimestamp}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StringType, StructType, TimestampType}

import org.apache.auron.protobuf.ScalarFunction

class NativeConvertersSuite
    extends AuronQueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {

  test("timestamp truncation preserves Spark timezone semantics") {
    val mondayInShanghai = java.time.Instant.parse("2026-09-13T16:00:00Z").toEpochMilli * 1000
    val timestamp = Literal.create(mondayInShanghai, TimestampType)
    val expr = TruncTimestamp(Literal("week"), timestamp, Some("Asia/Shanghai"))

    assert(expr.eval() == mondayInShanghai)
    val nativeExpr = NativeConverters.convertExpr(expr)
    assert(nativeExpr.hasSparkUdfWrapperExpr)
    val (restored, _) = NativeConverters.deserializeExpression[TruncTimestamp, StructType](
      nativeExpr.getSparkUdfWrapperExpr.getSerialized.toByteArray)
    assert(restored.timeZoneId == Some("Asia/Shanghai"))
    assert(restored.eval() == mondayInShanghai)
  }

  test("timestamp truncation keeps Spark aliases and invalid format handling") {
    val timestamp = Literal.create(0L, TimestampType)
    Seq("yyyy", "mon", "invalid").foreach { format =>
      val expr = TruncTimestamp(Literal(format), timestamp, Some("UTC"))
      val nativeExpr = NativeConverters.convertExpr(expr)
      assert(nativeExpr.hasSparkUdfWrapperExpr)
      val (restored, _) = NativeConverters.deserializeExpression[TruncTimestamp, StructType](
        nativeExpr.getSparkUdfWrapperExpr.getSerialized.toByteArray)
      assert(restored.eval() == expr.eval())
    }
  }

  private def assertTrimmedCast(rawValue: String, targetType: DataType): Unit = {
    val expr = Cast(Literal.create(rawValue, StringType), targetType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(childExpr.hasScalarFunction)
    val scalarFn = childExpr.getScalarFunction
    assert(scalarFn.getFun == ScalarFunction.Trim)
    assert(scalarFn.getArgsCount == 1 && scalarFn.getArgs(0).hasLiteral)
  }

  private def assertNonTrimmedCast(rawValue: String, targetType: DataType): Unit = {
    val expr = Cast(Literal.create(rawValue, StringType), targetType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(!childExpr.hasScalarFunction)
    assert(childExpr.hasLiteral)
  }

  test("cast from string to numeric adds trim wrapper before native cast when enabled") {
    withSQLConf("spark.auron.cast.trimString" -> "true") {
      assertTrimmedCast(" 42 ", IntegerType)
    }
  }

  test("cast from string to boolean adds trim wrapper before native cast when enabled") {
    withSQLConf("spark.auron.cast.trimString" -> "true") {
      assertTrimmedCast(" true ", BooleanType)
    }
  }

  test("cast trim disabled via auron conf") {
    withSQLConf("spark.auron.cast.trimString" -> "false") {
      assertNonTrimmedCast(" 42 ", IntegerType)
    }
  }

  test("cast trim disabled via auron conf for boolean cast") {
    withSQLConf("spark.auron.cast.trimString" -> "false") {
      assertNonTrimmedCast(" true ", BooleanType)
    }
  }

  test("cast with non-string child remains unchanged") {
    val expr = Cast(Literal(1.5), IntegerType)
    val nativeExpr = NativeConverters.convertExpr(expr)

    assert(nativeExpr.hasTryCast)
    val childExpr = nativeExpr.getTryCast.getExpr
    assert(!childExpr.hasScalarFunction)
    assert(childExpr.hasLiteral)
  }
}
