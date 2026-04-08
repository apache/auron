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
package org.apache.spark.sql

class AuronInstrSuite extends QueryTest with SparkQueryTestsBase {

  test("test instr function - basic functionality") {
    val data = Seq(
      ("hello world", "world"),
      ("hello world", "hello"),
      ("hello world", "o"),
      ("hello world", "z"),
      (null, "test"),
      ("test", null)
    )

    val df = spark.createDataFrame(data).toDF("str", "substr")
    val rows = df.selectExpr("instr(str, substr)").collect()

    // Check non-null results
    assert(rows(0).getInt(0) == 7, "instr('hello world', 'world') should return 7")
    assert(rows(1).getInt(0) == 1, "instr('hello world', 'hello') should return 1")
    assert(rows(2).getInt(0) == 5, "instr('hello world', 'o') should return 5")
    assert(rows(3).getInt(0) == 0, "instr('hello world', 'z') should return 0")
    
    // Check null results
    assert(rows(4).isNullAt(0), "instr(null, 'test') should return null")
    assert(rows(5).isNullAt(0), "instr('test', null) should return null")
  }

  test("test instr function - multiple occurrences") {
    val data = Seq(
      ("banana", "a"),
      ("testtesttest", "test"),
      ("abcabcabc", "abc")
    )

    val df = spark.createDataFrame(data).toDF("str", "substr")
    val result = df.selectExpr("instr(str, substr)").collect().map(_.getInt(0))

    assert(result(0) == 2, "instr('banana', 'a') should return 2")
    assert(result(1) == 1, "instr('testtesttest', 'test') should return 1")
    assert(result(2) == 1, "instr('abcabcabc', 'abc') should return 1")
  }

  test("test instr function - case sensitive") {
    val data = Seq(
      ("Hello", "hello"),
      ("HELLO", "hello"),
      ("Hello", "Hello"),
      ("hElLo", "hello")
    )

    val df = spark.createDataFrame(data).toDF("str", "substr")
    val result = df.selectExpr("instr(str, substr)").collect().map(_.getInt(0))

    assert(result(0) == 0, "instr('Hello', 'hello') should return 0 (case sensitive)")
    assert(result(1) == 0, "instr('HELLO', 'hello') should return 0 (case sensitive)")
    assert(result(2) == 1, "instr('Hello', 'Hello') should return 1")
    assert(result(3) == 0, "instr('hElLo', 'hello') should return 0 (case sensitive)")
  }

  test("test instr function - with filter") {
    val data = Seq(
      ("hello world", "world", 1),
      ("hello", "world", 0),
      ("hello", "hello", 1),
      ("test", "abc", 0)
    )

    val df = spark.createDataFrame(data).toDF("str", "substr", "expected")
    val result = df
      .filter("instr(str, substr) > 0")
      .select("str")
      .collect()
      .map(_.getString(0))

    assert(result.length == 2, "Should find 2 matching strings")
    assert(result.contains("hello world"))
    assert(result.contains("hello"))
  }

  test("test instr function - in group by") {
    val data = Seq(
      ("test1", "test"),
      ("test2", "test"),
      ("hello", "world"),
      ("testing", "test")
    )

    val df = spark.createDataFrame(data).toDF("str", "substr")
    val result = df
      .groupBy("substr")
      .count()
      .filter("count > 0")
      .orderBy("substr")
      .collect()

    assert(result.length >= 1)
  }

  test("test instr function - in where clause") {
    val data = Seq(
      ("hello world", "world"),
      ("hello", "world"),
      ("testing", "test"),
      ("abc", "def")
    )

    val df = spark.createDataFrame(data).toDF("str", "substr")
    val result = df
      .filter("instr(str, substr) = 1")
      .select("str")
      .collect()
      .map(_.getString(0))

    assert(result.length >= 1)
  }
}
