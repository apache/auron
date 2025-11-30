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

import org.apache.spark.SparkConf
import org.apache.spark.sql.AuronQueryTest

class AuronShuffleManagerCheckSuite
    extends AuronQueryTest
    with BaseAuronSQLSuite
    with AuronSQLTestHelper {
  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.SortShuffleManager")
  }

  test("test use unsupported ShuffleManager with Auron") {
    withTempView("v") {
      withSQLConf("spark.shuffle.manager" -> "org.apache.spark.shuffle.sort.SortShuffleManager") {
        val exception = intercept[IllegalStateException] {
          Seq((1, "james")).toDF("id", "name").createOrReplaceTempView("v")
          sql("select * from v")
        }

        assert(
          exception.getMessage == "spark.auron.enable is true, but the shuffle manager " +
            "isn't supported by Auron: org.apache.spark.shuffle.sort.SortShuffleManager")
      }
    }
  }
}
