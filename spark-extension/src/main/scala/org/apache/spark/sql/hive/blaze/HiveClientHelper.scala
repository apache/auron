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
package org.apache.spark.sql.hive.blaze

import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.hive.client.HiveClientImpl

object HiveClientHelper {
  def toHiveTable(table: CatalogTable, userName: Option[String] = None): Table = {
    HiveClientImpl.toHiveTable(table, userName)
  }
}
