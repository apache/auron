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

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.auron.protobuf.PartitionedFile;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for FileSplitter.
 */
public class FileSplitterTest {

    /**
     * Helper to create dummy PartitionedFile objects for testing.
     */
    private PartitionedFile createFile(int index) {
        org.apache.auron.protobuf.FileRange range = org.apache.auron.protobuf.FileRange.newBuilder()
                .setStart(0)
                .setEnd(1000)
                .build();
        return PartitionedFile.newBuilder()
                .setPath("/test/file_" + index + ".parquet")
                .setSize(1000)
                .setRange(range)
                .build();
    }

    @Test
    public void testRoundRobinSplit_ThreeTasks() {
        // Create 10 test files
        List<PartitionedFile> allFiles = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allFiles.add(createFile(i));
        }

        // Split across 3 tasks
        List<PartitionedFile> task0Files = FileSplitter.splitFilesRoundRobin(allFiles, 0, 3);
        List<PartitionedFile> task1Files = FileSplitter.splitFilesRoundRobin(allFiles, 1, 3);
        List<PartitionedFile> task2Files = FileSplitter.splitFilesRoundRobin(allFiles, 2, 3);

        // Task 0 should get files [0, 3, 6, 9] = 4 files
        assertEquals(4, task0Files.size());
        assertEquals("/test/file_0.parquet", task0Files.get(0).getPath());
        assertEquals("/test/file_3.parquet", task0Files.get(1).getPath());
        assertEquals("/test/file_6.parquet", task0Files.get(2).getPath());
        assertEquals("/test/file_9.parquet", task0Files.get(3).getPath());

        // Task 1 should get files [1, 4, 7] = 3 files
        assertEquals(3, task1Files.size());
        assertEquals("/test/file_1.parquet", task1Files.get(0).getPath());
        assertEquals("/test/file_4.parquet", task1Files.get(1).getPath());
        assertEquals("/test/file_7.parquet", task1Files.get(2).getPath());

        // Task 2 should get files [2, 5, 8] = 3 files
        assertEquals(3, task2Files.size());
        assertEquals("/test/file_2.parquet", task2Files.get(0).getPath());
        assertEquals("/test/file_5.parquet", task2Files.get(1).getPath());
        assertEquals("/test/file_8.parquet", task2Files.get(2).getPath());

        // Verify all files are accounted for (no duplicates, no missing)
        int totalFiles = task0Files.size() + task1Files.size() + task2Files.size();
        assertEquals(10, totalFiles);
    }

    @Test
    public void testRoundRobinSplit_SingleTask() {
        // Create 5 test files
        List<PartitionedFile> allFiles = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            allFiles.add(createFile(i));
        }

        // Single task should get all files
        List<PartitionedFile> taskFiles = FileSplitter.splitFilesRoundRobin(allFiles, 0, 1);

        assertEquals(5, taskFiles.size());
        for (int i = 0; i < 5; i++) {
            assertEquals("/test/file_" + i + ".parquet", taskFiles.get(i).getPath());
        }
    }

    @Test
    public void testRoundRobinSplit_MoreTasksThanFiles() {
        // Create 3 test files
        List<PartitionedFile> allFiles = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            allFiles.add(createFile(i));
        }

        // Split across 5 tasks - some tasks will get no files
        List<PartitionedFile> task0Files = FileSplitter.splitFilesRoundRobin(allFiles, 0, 5);
        List<PartitionedFile> task1Files = FileSplitter.splitFilesRoundRobin(allFiles, 1, 5);
        List<PartitionedFile> task2Files = FileSplitter.splitFilesRoundRobin(allFiles, 2, 5);
        List<PartitionedFile> task3Files = FileSplitter.splitFilesRoundRobin(allFiles, 3, 5);
        List<PartitionedFile> task4Files = FileSplitter.splitFilesRoundRobin(allFiles, 4, 5);

        // Tasks 0, 1, 2 get one file each
        assertEquals(1, task0Files.size());
        assertEquals(1, task1Files.size());
        assertEquals(1, task2Files.size());

        // Tasks 3, 4 get no files
        assertEquals(0, task3Files.size());
        assertEquals(0, task4Files.size());
    }

    @Test
    public void testRoundRobinSplit_EmptyFileList() {
        List<PartitionedFile> emptyFiles = new ArrayList<>();

        List<PartitionedFile> taskFiles = FileSplitter.splitFilesRoundRobin(emptyFiles, 0, 3);

        assertEquals(0, taskFiles.size());
    }

    @Test
    public void testRoundRobinSplit_InvalidTaskIndex() {
        List<PartitionedFile> allFiles = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            allFiles.add(createFile(i));
        }

        // Negative task index
        assertThrows(IllegalArgumentException.class, () -> {
            FileSplitter.splitFilesRoundRobin(allFiles, -1, 3);
        });

        // Task index >= total tasks
        assertThrows(IllegalArgumentException.class, () -> {
            FileSplitter.splitFilesRoundRobin(allFiles, 3, 3);
        });
    }

    @Test
    public void testRangeSplit_ThreeTasks() {
        // Create 10 test files
        List<PartitionedFile> allFiles = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allFiles.add(createFile(i));
        }

        // Split across 3 tasks
        List<PartitionedFile> task0Files = FileSplitter.splitFilesRangeBased(allFiles, 0, 3);
        List<PartitionedFile> task1Files = FileSplitter.splitFilesRangeBased(allFiles, 1, 3);
        List<PartitionedFile> task2Files = FileSplitter.splitFilesRangeBased(allFiles, 2, 3);

        // Task 0 should get files [0-3] = 4 files
        assertEquals(4, task0Files.size());
        assertEquals("/test/file_0.parquet", task0Files.get(0).getPath());
        assertEquals("/test/file_3.parquet", task0Files.get(3).getPath());

        // Task 1 should get files [4-7] = 4 files
        assertEquals(4, task1Files.size());
        assertEquals("/test/file_4.parquet", task1Files.get(0).getPath());
        assertEquals("/test/file_7.parquet", task1Files.get(3).getPath());

        // Task 2 should get files [8-9] = 2 files
        assertEquals(2, task2Files.size());
        assertEquals("/test/file_8.parquet", task2Files.get(0).getPath());
        assertEquals("/test/file_9.parquet", task2Files.get(1).getPath());

        // Verify all files are accounted for
        int totalFiles = task0Files.size() + task1Files.size() + task2Files.size();
        assertEquals(10, totalFiles);
    }

    @Test
    public void testRangeSplit_MoreTasksThanFiles() {
        // Create 2 test files
        List<PartitionedFile> allFiles = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            allFiles.add(createFile(i));
        }

        // Split across 5 tasks
        List<PartitionedFile> task0Files = FileSplitter.splitFilesRangeBased(allFiles, 0, 5);
        List<PartitionedFile> task1Files = FileSplitter.splitFilesRangeBased(allFiles, 1, 5);
        List<PartitionedFile> task2Files = FileSplitter.splitFilesRangeBased(allFiles, 2, 5);
        List<PartitionedFile> task3Files = FileSplitter.splitFilesRangeBased(allFiles, 3, 5);
        List<PartitionedFile> task4Files = FileSplitter.splitFilesRangeBased(allFiles, 4, 5);

        // First two tasks get one file each
        assertEquals(1, task0Files.size());
        assertEquals(1, task1Files.size());

        // Remaining tasks get nothing
        assertEquals(0, task2Files.size());
        assertEquals(0, task3Files.size());
        assertEquals(0, task4Files.size());
    }

    @Test
    public void testDefaultSplitUsesRoundRobin() {
        // Create 10 test files
        List<PartitionedFile> allFiles = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            allFiles.add(createFile(i));
        }

        // The default splitFiles() method should use round-robin
        List<PartitionedFile> defaultTask0 = FileSplitter.splitFiles(allFiles, 0, 3);
        List<PartitionedFile> roundRobinTask0 = FileSplitter.splitFilesRoundRobin(allFiles, 0, 3);

        assertEquals(roundRobinTask0.size(), defaultTask0.size());
        for (int i = 0; i < roundRobinTask0.size(); i++) {
            assertEquals(roundRobinTask0.get(i).getPath(), defaultTask0.get(i).getPath());
        }
    }
}
