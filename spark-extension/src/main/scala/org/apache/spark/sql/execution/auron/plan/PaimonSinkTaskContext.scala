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
package org.apache.spark.sql.execution.auron.plan

import scala.collection.mutable

import org.apache.spark.TaskContext

/**
 * Task side callbacks of the native Paimon sink. Implemented by the auron-paimon module, which
 * owns data file naming, statistics and commit messages.
 */
trait PaimonSinkTaskHandler {

  /** Allocates the absolute path of the next data file written by this task. */
  def nextOutputPath(): String

  /** Called after the native sink closed a data file. */
  def completeFile(path: String, numRows: Long, numBytes: Long): Unit
}

/** Registry of the [[PaimonSinkTaskHandler]] of each running task attempt. */
object PaimonSinkTaskContext {
  private val handlers = mutable.Map[Long, PaimonSinkTaskHandler]()

  def register(handler: PaimonSinkTaskHandler): Unit = {
    val taskContext = TaskContext.get()
    val taskId = taskContext.taskAttemptId()
    handlers.synchronized {
      handlers.put(taskId, handler)
    }
    taskContext.addTaskCompletionListener[Unit] { _ =>
      handlers.synchronized {
        handlers.remove(taskId)
      }
    }
  }

  def get: PaimonSinkTaskHandler = {
    val taskId = TaskContext.get().taskAttemptId()
    handlers.synchronized {
      handlers.getOrElse(
        taskId,
        throw new IllegalStateException(s"no paimon sink handler registered for task $taskId"))
    }
  }
}
