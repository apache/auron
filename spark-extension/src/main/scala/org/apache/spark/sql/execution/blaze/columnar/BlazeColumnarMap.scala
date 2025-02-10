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
package org.apache.spark.sql.execution.blaze.columnar

import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.catalyst.util.MapData

class BlazeColumnarMap(
    keys: BlazeColumnVector,
    values: BlazeColumnVector,
    offset: Int,
    private val length: Int)
    extends MapData {

  override def numElements: Int = length

  override def keyArray: ArrayData = new BlazeColumnarArray(keys, offset, length)

  override def valueArray: ArrayData = new BlazeColumnarArray(values, offset, length)

  override def copy = new ArrayBasedMapData(keyArray.copy, valueArray.copy)
}
