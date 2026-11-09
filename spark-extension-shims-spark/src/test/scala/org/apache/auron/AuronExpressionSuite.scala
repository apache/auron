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

class AuronExpressionSuite extends AuronQueryTest with BaseAuronSQLSuite {

  test("Chr uses Spark byte-range character semantics") {
    withTable("chr_inputs") {
      sql("CREATE TABLE chr_inputs(n BIGINT) USING parquet")
      sql("""INSERT INTO chr_inputs VALUES
            |(-1), (0), (65), (233), (255), (256), (321), (9223372036854775807), (NULL)
            |""".stripMargin)
      checkSparkAnswerAndOperator("SELECT chr(n), hex(chr(n)) FROM chr_inputs")
    }
  }

  test("preserve literal parameters during expression fallback") {
    withSQLConf(
      "spark.auron.decimal.arithOp.enabled" -> "false",
      "spark.auron.udf.singleChildFallback.enabled" -> "true") {
      withTable("literal_fallback") {
        sql("CREATE TABLE literal_fallback(a DECIMAL(17,2), b DECIMAL(17,2)) USING parquet")
        sql("""INSERT INTO literal_fallback VALUES
              |(123.45, 7.00), (-123.45, 7.00), (1.00, 8.00), (NULL, 7.00), (1.00, NULL)
              |""".stripMargin)
        checkSparkAnswerAndOperator("SELECT round(a / b, 2) FROM literal_fallback")
        checkSparkAnswerAndOperator(
          "SELECT round(a, 4), round(a, 0), round(a, 2) FROM literal_fallback")
        // Spark 3.0-3.2 require legacy mode for a negative decimal result scale.
        withSQLConf("spark.sql.legacy.allowNegativeScaleOfDecimal" -> "true") {
          checkSparkAnswerAndOperator("SELECT round(a, -1) FROM literal_fallback")
        }
      }
    }
  }

  test("EqualNullSafe") {
    withTable("t1") {
      sql("CREATE TABLE t1(id INT, flag BOOLEAN) USING parquet")
      sql("INSERT INTO t1 VALUES (1, true), (2, false), (3, null), (null, true)")

      checkSparkAnswerAndOperator(
        "SELECT id <=> 1, id <=> null, flag <=> true, flag <=> null FROM t1 WHERE flag <=> true")
      checkSparkAnswerAndOperator(
        "SELECT id <=> 2, id <=> null, flag <=> false, flag <=> null FROM t1 WHERE NOT flag <=> true")
    }
  }

  test("UnaryMinus") {
    // Negating Int.MinValue overflows. Under ANSI mode (default in Spark 4.x) vanilla Spark
    // throws while the native engine wraps, so the comparison diverges. Disable ANSI so both
    // engines wrap consistently and the boundary value can still be exercised.
    withSQLConf("spark.sql.ansi.enabled" -> "false") {
      withTable("t1") {
        sql("create table t1(col1 int) using parquet")
        sql(
          "insert into t1 values(1), (2), (3), (3), (-1), (0), (null), (2147483647), (-2147483648)")
        checkSparkAnswerAndOperator("SELECT negative(col1), -(col1) FROM t1")
      }
    }
  }
}
