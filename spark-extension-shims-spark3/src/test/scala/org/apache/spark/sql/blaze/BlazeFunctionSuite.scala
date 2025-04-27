/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.blaze

import org.apache.spark.sql.Row

class BlazeFunctionSuite extends org.apache.spark.sql.QueryTest with BaseBlazeSQLSuite {

  test("sum function with float input") {
    withTable("t1") {
      withSQLConf("spark.blaze.enable" -> "false") {
        sql("set spark.blaze.enable=false")
        sql("create table t1 using parquet as select 1.0f as c1")
        val df = sql("select sum(c1) from t1")
        checkAnswer(df, Seq(Row(1.23, 1.1)))
      }
    }
  }

  test("sha2 function") {
    withTable("t1") {
      sql("create table t1 using parquet as select 'spark' as c1, '3.x' as version")
      val functions =
        """
          |select
          |  sha2(concat(c1, version), 256) as sha0,
          |  sha2(concat(c1, version), 256) as sha256,
          |  sha2(concat(c1, version), 224) as sha224,
          |  sha2(concat(c1, version), 384) as sha384,
          |  sha2(concat(c1, version), 512) as sha512
          |from t1
          |""".stripMargin
      val df = sql(functions)
      checkAnswer(
        df,
        Seq(
          Row(
            "562d20689257f3f3a04ee9afb86d0ece2af106cf6c6e5e7d266043088ce5fbc0",
            "562d20689257f3f3a04ee9afb86d0ece2af106cf6c6e5e7d266043088ce5fbc0",
            "d0c8e9ccd5c7b3fdbacd2cfd6b4d65ca8489983b5e8c7c64cd77b634",
            "77c1199808053619c29e9af2656e1ad2614772f6ea605d5757894d6aec2dfaf34ff6fd662def3b79e429e9ae5ecbfed1",
            "c4e27d35517ca62243c1f322d7922dac175830be4668e8a1cf3befdcd287bb5b6f8c5f041c9d89e4609c8cfa242008c7c7133af1685f57bac9052c1212f1d089")))
    }
  }

}
