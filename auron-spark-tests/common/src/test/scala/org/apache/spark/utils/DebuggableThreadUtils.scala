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
package org.apache.spark.utils

import scala.util.{Failure, Success, Try}

import org.apache.spark.util.ThreadUtils

object DebuggableThreadUtils {

  /**
   * Applies a function to each element of the input sequence in parallel, logging any failures.
   *
   * @param in
   *   The input sequence of elements to process.
   * @param prefix
   *   The prefix to use for thread names.
   * @param maxThreads
   *   The maximum number of threads to use for parallel processing.
   * @param f
   *   The function to apply to each element of the input sequence.
   * @return
   *   A sequence containing the results of applying the function to each input element.
   */
  def parmap[I, O](in: Seq[I], prefix: String, maxThreads: Int)(f: I => O): Seq[O] = {
    ThreadUtils.parmap(in, prefix, maxThreads) { i =>
      Try(f(i)) match {
        case Success(result) => result
        case Failure(exception) =>
          // scalastyle:off println
          println(s"Test failed for case: ${i.toString}: ${exception.getMessage}")
          // scalastyle:on println
          throw exception
      }
    }
  }
}
