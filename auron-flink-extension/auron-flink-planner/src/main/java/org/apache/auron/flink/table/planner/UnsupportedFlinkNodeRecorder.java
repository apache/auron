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
package org.apache.auron.flink.table.planner;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.annotation.VisibleForTesting;

/**
 * Tracks unsupported Flink-side node classes encountered during plan conversion, deduplicated so
 * a given class is logged at most once per JVM. Used by ExecNode shadow classes (e.g. {@code
 * StreamExecCalc}) to record converter-coverage gaps without spamming the log; also tracks
 * plan-composition failures, which always log.
 *
 * <p>Lives in the Auron package — not in {@code org.apache.flink.*} — so unit tests can call the
 * {@link VisibleForTesting} seams directly without classpath-ordering hazards from FQCN-shadowed
 * classes.
 */
public final class UnsupportedFlinkNodeRecorder {

    private static final Set<Class<?>> WARN_DEDUP = ConcurrentHashMap.newKeySet();
    private static final AtomicInteger WARN_EMIT_COUNT = new AtomicInteger();

    private UnsupportedFlinkNodeRecorder() {}

    /**
     * Marks an unsupported Flink-side node class as seen and increments the emit counter. Returns
     * {@code true} on the first occurrence of the class (caller should emit the WARN line);
     * {@code false} on subsequent occurrences (caller should be silent).
     */
    public static boolean recordFallback(Class<?> unsupportedFlinkNodeClass) {
        if (WARN_DEDUP.add(unsupportedFlinkNodeClass)) {
            WARN_EMIT_COUNT.incrementAndGet();
            return true;
        }
        return false;
    }

    /** Increments the emit counter for plan-composition failures, which always log. */
    public static void recordCompositionFailure() {
        WARN_EMIT_COUNT.incrementAndGet();
    }

    @VisibleForTesting
    public static void resetForTest() {
        WARN_DEDUP.clear();
        WARN_EMIT_COUNT.set(0);
    }

    @VisibleForTesting
    public static int peekEmitCount() {
        return WARN_EMIT_COUNT.get();
    }
}
