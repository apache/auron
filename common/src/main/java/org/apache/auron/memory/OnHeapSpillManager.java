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
package org.apache.auron.memory;

import java.nio.ByteBuffer;

/**
 * Interface for managing on-heap spill operations.
 * This interface provides methods to handle memory spilling to disk when on-heap memory is insufficient.
 */
public abstract class OnHeapSpillManager {

    /**
     * Check if on-heap memory is available for allocation.
     *
     * @return true if on-heap memory is available, false otherwise
     */
    abstract boolean isOnHeapAvailable();

    /**
     * Create a new spill operation and return its identifier.
     *
     * @return spill identifier for the newly created spill
     */
    abstract int newSpill();

    /**
     * Write data from a ByteBuffer to the spill identified by spillId.
     *
     * @param spillId the identifier of the spill to write to
     * @param buffer the ByteBuffer containing data to be written
     */
    abstract void writeSpill(int spillId, ByteBuffer buffer);

    /**
     * Read data from the spill identified by spillId into the provided ByteBuffer.
     *
     * @param spillId the identifier of the spill to read from
     * @param buffer the ByteBuffer to read data into
     * @return the number of bytes actually read
     */
    abstract int readSpill(int spillId, ByteBuffer buffer);

    /**
     * Get the disk usage in bytes for the spill identified by spillId.
     *
     * @param spillId the identifier of the spill
     * @return the disk usage in bytes
     */
    abstract long getSpillDiskUsage(int spillId);

    /**
     * Get the total disk I/O time in nanoseconds for the spill identified by spillId.
     *
     * @param spillId the identifier of the spill
     * @return the disk I/O time in nanoseconds
     */
    abstract long getSpillDiskIOTime(int spillId);

    /**
     * Release and clean up resources associated with the spill identified by spillId.
     *
     * @param spillId the identifier of the spill to release
     */
    abstract void releaseSpill(int spillId);

    /**
     * Get the disabled on-heap spill manager instance.
     *
     * @return the disabled on-heap spill manager instance
     */
    public static OnHeapSpillManager getDisabledOnHeapSpillManager() {
        return new OnHeapSpillManager() {

            @Override
            boolean isOnHeapAvailable() {
                return false;
            }

            @Override
            int newSpill() {
                throw new UnsupportedOperationException();
            }

            @Override
            void writeSpill(int spillId, ByteBuffer buffer) {
                throw new UnsupportedOperationException();
            }

            @Override
            int readSpill(int spillId, ByteBuffer buffer) {
                throw new UnsupportedOperationException();
            }

            @Override
            long getSpillDiskUsage(int spillId) {
                throw new UnsupportedOperationException();
            }

            @Override
            long getSpillDiskIOTime(int spillId) {
                throw new UnsupportedOperationException();
            }

            @Override
            void releaseSpill(int spillId) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
