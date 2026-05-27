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
package org.apache.flink.table.planner.plan.nodes.exec.stream;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.annotation.VisibleForTesting;

/**
 * Holds the per-fallback WARN dedup state for the shadowed {@link StreamExecCalc}. Lives in a
 * non-shadowed sibling class so unit tests can call the {@link VisibleForTesting} seams directly
 * — the shadow shares its FQCN with Flink's stock {@code StreamExecCalc}, which can confuse javac
 * when test sources reference symbols that exist only on the shadow.
 *
 * <p>Dedup is JVM-wide: a given unsupported {@link RexNode} class is logged at most once per JVM,
 * bounded by the small finite set of {@link RexNode} subclasses Flink can generate.
 */
final class StreamExecCalcWarnState {

    private static final Set<Class<? extends RexNode>> WARN_DEDUP = ConcurrentHashMap.newKeySet();
    private static final AtomicInteger WARN_EMIT_COUNT = new AtomicInteger();

    private StreamExecCalcWarnState() {}

    /**
     * Marks an unsupported {@link RexNode} class as seen and increments the emit counter. Returns
     * {@code true} on the first occurrence of the class (caller should emit the WARN line);
     * {@code false} on subsequent occurrences (caller should be silent).
     */
    static boolean recordFallback(Class<? extends RexNode> unsupportedRexClass) {
        if (WARN_DEDUP.add(unsupportedRexClass)) {
            WARN_EMIT_COUNT.incrementAndGet();
            return true;
        }
        return false;
    }

    /** Increments the emit counter for plan-composition failures, which always log. */
    static void recordCompositionFailure() {
        WARN_EMIT_COUNT.incrementAndGet();
    }

    @VisibleForTesting
    static void resetForTest() {
        WARN_DEDUP.clear();
        WARN_EMIT_COUNT.set(0);
    }

    @VisibleForTesting
    static int peekEmitCount() {
        return WARN_EMIT_COUNT.get();
    }
}
