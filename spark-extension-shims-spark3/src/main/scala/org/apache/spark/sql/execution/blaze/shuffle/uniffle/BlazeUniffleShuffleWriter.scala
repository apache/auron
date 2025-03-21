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
package org.apache.spark.sql.execution.blaze.shuffle.uniffle

import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.writer.RssShuffleWriter
import org.apache.spark.shuffle.{ShuffleHandle, ShuffleWriteMetricsReporter}
import org.apache.spark.sql.execution.blaze.shuffle.{BlazeRssShuffleWriterBase, RssPartitionWriterBase}

class BlazeUniffleShuffleWriter[K, V, C](
    rssShuffleWriter: RssShuffleWriter[K, V, C],
    metrics: ShuffleWriteMetricsReporter)
    extends BlazeRssShuffleWriterBase[K, V](metrics)
    with Logging {

  override def getRssPartitionWriter(
      _handle: ShuffleHandle,
      _mapId: Int,
      metrics: ShuffleWriteMetricsReporter,
      numPartitions: Int): RssPartitionWriterBase = {
    new UnifflePartitionWriter(numPartitions, metrics, rssShuffleWriter)
  }

  private def waitAndCheckBlocksSend(): Unit = {
    logInfo(s"Waiting all blocks sending to the remote shuffle servers...")
    val method = rssShuffleWriter.getClass.getDeclaredMethod("internalCheckBlockSendResult")
    method.setAccessible(true)
    method.invoke(rssShuffleWriter)
  }

  override def rssStop(success: Boolean): Unit = {
    waitAndCheckBlocksSend()
    logInfo(s"Reporting the shuffle result...")
    super.stop(success)
    rssShuffleWriter.stop(success)
  }
}
