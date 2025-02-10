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
package org.apache.spark.sql.execution.blaze.arrowio

import java.lang.Thread.UncaughtExceptionHandler

import org.apache.arrow.c.ArrowArray
import org.apache.arrow.c.ArrowSchema
import org.apache.arrow.c.Data
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils.ROOT_ALLOCATOR
import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowWriter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.blaze.{BlazeConf, NativeHelper}
import org.apache.spark.sql.blaze.util.Using
import org.apache.spark.TaskContext
import java.security.PrivilegedExceptionAction
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

import org.apache.spark.sql.execution.blaze.arrowio.util.ArrowUtils.CHILD_ALLOCATOR

class ArrowFFIExporter(rowIter: Iterator[InternalRow], schema: StructType) {
  private val maxBatchNumRows = BlazeConf.BATCH_SIZE.intConf()
  private val maxBatchMemorySize = BlazeConf.SUGGESTED_BATCH_MEM_SIZE.intConf()

  private val arrowSchema = ArrowUtils.toArrowSchema(schema)
  private val emptyDictionaryProvider = new MapDictionaryProvider()
  private val nativeCurrentUser = NativeHelper.currentUser

  private trait QueueState
  private case object NextBatch extends QueueState
  private case object Finished extends QueueState

  private val tc = TaskContext.get()
  private val outputQueue: BlockingQueue[QueueState] = new ArrayBlockingQueue[QueueState](16)
  private val processingQueue: BlockingQueue[Unit] = new ArrayBlockingQueue[Unit](16)
  private var currentRoot: VectorSchemaRoot = _
  startOutputThread()

  def exportSchema(exportArrowSchemaPtr: Long): Unit = {
    Using.resource(ArrowSchema.wrap(exportArrowSchemaPtr)) { exportSchema =>
      Data.exportSchema(ROOT_ALLOCATOR, arrowSchema, emptyDictionaryProvider, exportSchema)
    }
  }

  def exportNextBatch(exportArrowArrayPtr: Long): Boolean = {
    if (!hasNext) {
      return false
    }

    // export using root allocator
    Using.resource(ArrowArray.wrap(exportArrowArrayPtr)) { exportArray =>
      Data.exportVectorSchemaRoot(
        ROOT_ALLOCATOR,
        currentRoot,
        emptyDictionaryProvider,
        exportArray)
    }

    // to continue processing next batch
    processingQueue.put(())
    true
  }

  private def hasNext: Boolean = {
    if (tc != null && (tc.isCompleted() || tc.isInterrupted())) {
      return false
    }
    outputQueue.take() == NextBatch
  }

  private def startOutputThread(): Thread = {
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        if (tc != null) {
          TaskContext.setTaskContext(tc)
        }

        nativeCurrentUser.doAs(new PrivilegedExceptionAction[Unit] {
          override def run(): Unit = {
            while (tc == null || (!tc.isCompleted() && !tc.isInterrupted())) {
              if (!rowIter.hasNext) {
                outputQueue.put(Finished)
                return
              }

              Using.resource(CHILD_ALLOCATOR("ArrowFFIExporter")) { allocator =>
                Using.resource(VectorSchemaRoot.create(arrowSchema, allocator)) { root =>
                  val arrowWriter = ArrowWriter.create(root)
                  while (rowIter.hasNext
                    && allocator.getAllocatedMemory < maxBatchMemorySize
                    && arrowWriter.currentCount < maxBatchNumRows) {
                    arrowWriter.write(rowIter.next())
                  }
                  arrowWriter.finish()

                  // export root
                  currentRoot = root
                  outputQueue.put(NextBatch)

                  // wait for processing next batch
                  processingQueue.take()
                }
              }
            }
            outputQueue.put(Finished)
          }
        })
      }
    })

    def close(): Unit = {
      thread.interrupt()
      outputQueue.put(Finished) // to abort any pending call to exportNextBatch()
    }

    if (tc != null) {
      tc.addTaskCompletionListener[Unit]((_: TaskContext) => close())
      tc.addTaskFailureListener((_, _) => close())
    }

    thread.setDaemon(true)
    thread.setUncaughtExceptionHandler(new UncaughtExceptionHandler {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        close()
        throw e
      }
    })
    thread.start()
    thread
  }
}
