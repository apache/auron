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
package org.apache.auron.flink.planner.runtime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.auron.protobuf.PartitionedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility for splitting files across parallel tasks.
 * Provides different strategies for distributing files to achieve load balancing.
 */
public class FileSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(FileSplitter.class);

    /**
     * Splits files across tasks using round-robin strategy.
     * This is the default strategy for simplicity and good load balance.
     *
     * @param allFiles All files to be split
     * @param taskIndex This task's index (0-based)
     * @param totalTasks Total number of parallel tasks
     * @return List of files assigned to this task
     */
    public static List<PartitionedFile> splitFiles(List<PartitionedFile> allFiles, int taskIndex, int totalTasks) {
        return splitFilesRoundRobin(allFiles, taskIndex, totalTasks);
    }

    /**
     * Round-robin distribution: Task i gets files [i, i+N, i+2N, ...]
     *
     * Example with 10 files and 3 tasks:
     * - Task 0: files [0, 3, 6, 9] (4 files)
     * - Task 1: files [1, 4, 7] (3 files)
     * - Task 2: files [2, 5, 8] (3 files)
     */
    public static List<PartitionedFile> splitFilesRoundRobin(
            List<PartitionedFile> allFiles, int taskIndex, int totalTasks) {

        if (allFiles == null || allFiles.isEmpty()) {
            LOG.warn("No files to split for task {}/{}", taskIndex, totalTasks);
            return Collections.emptyList();
        }

        if (taskIndex < 0 || taskIndex >= totalTasks) {
            throw new IllegalArgumentException(
                    String.format("Invalid taskIndex %d for totalTasks %d", taskIndex, totalTasks));
        }

        List<PartitionedFile> taskFiles = new ArrayList<>();

        for (int i = taskIndex; i < allFiles.size(); i += totalTasks) {
            taskFiles.add(allFiles.get(i));
        }

        LOG.info(
                "Round-robin split: Task {}/{} assigned {} files out of {} total",
                taskIndex,
                totalTasks,
                taskFiles.size(),
                allFiles.size());

        return taskFiles;
    }

    /**
     * Range-based distribution: Divides files into contiguous ranges.
     * Better for data locality but may have load imbalance if file sizes vary.
     *
     * Example with 10 files and 3 tasks:
     * - Task 0: files [0, 1, 2, 3] (4 files)
     * - Task 1: files [4, 5, 6] (3 files)
     * - Task 2: files [7, 8, 9] (3 files)
     */
    public static List<PartitionedFile> splitFilesRangeBased(
            List<PartitionedFile> allFiles, int taskIndex, int totalTasks) {

        if (allFiles == null || allFiles.isEmpty()) {
            LOG.warn("No files to split for task {}/{}", taskIndex, totalTasks);
            return Collections.emptyList();
        }

        if (taskIndex < 0 || taskIndex >= totalTasks) {
            throw new IllegalArgumentException(
                    String.format("Invalid taskIndex %d for totalTasks %d", taskIndex, totalTasks));
        }

        int filesPerTask = (allFiles.size() + totalTasks - 1) / totalTasks;
        int startIdx = taskIndex * filesPerTask;
        int endIdx = Math.min(startIdx + filesPerTask, allFiles.size());

        if (startIdx >= allFiles.size()) {
            LOG.debug("Range split: Task {}/{} has no files (uneven distribution)", taskIndex, totalTasks);
            return Collections.emptyList();
        }

        List<PartitionedFile> taskFiles = new ArrayList<>(allFiles.subList(startIdx, endIdx));

        LOG.info(
                "Range split: Task {}/{} assigned files [{}, {}) = {} files",
                taskIndex,
                totalTasks,
                startIdx,
                endIdx,
                taskFiles.size());

        return taskFiles;
    }
}
