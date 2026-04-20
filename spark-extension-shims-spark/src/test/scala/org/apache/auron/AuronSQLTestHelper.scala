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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.internal.SQLConf

trait AuronSQLTestHelper {
  self: BaseAuronSQLSuite =>

  def withEnvConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SQLConf.get
    val (keys, values) = pairs.unzip
    val currentValues = keys.map { key =>
      if (conf.contains(key)) {
        Some(conf.getConfString(key))
      } else {
        None
      }
    }
    (keys, values).zipped.foreach { (k, v) =>
      conf.setConfString(k, v)
    }
    try f
    finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => conf.setConfString(key, value)
        case (key, None) => conf.unsetConf(key)
      }
    }
  }

  def withSparkConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val confs = Seq(spark.sparkContext.getConf, SparkEnv.get.conf).distinct
    val (keys, values) = pairs.unzip
    val currentValuesByConf = confs.map(conf => conf -> keys.map(conf.getOption))

    confs.foreach { conf =>
      (keys, values).zipped.foreach { (k, v) =>
        conf.set(k, v)
      }
    }

    try f
    finally {
      currentValuesByConf.foreach { case (conf, currentValues) =>
        keys.zip(currentValues).foreach {
          case (key, Some(value)) => conf.set(key, value)
          case (key, None) => conf.remove(key)
        }
      }
    }
  }
}
