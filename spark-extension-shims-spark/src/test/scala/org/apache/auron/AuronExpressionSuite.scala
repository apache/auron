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
    withTable("t1") {
      sql("create table t1(col1 int) using parquet")
      sql(
        "insert into t1 values(1), (2), (3), (3), (-1), (0), (null), (2147483647), (-2147483648)")
      checkSparkAnswerAndOperator("SELECT negative(col1), -(col1) FROM t1")
    }
  }
}
