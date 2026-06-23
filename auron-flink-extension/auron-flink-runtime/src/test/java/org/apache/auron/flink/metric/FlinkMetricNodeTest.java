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
package org.apache.auron.flink.metric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.auron.metric.MetricNode;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricGroup;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link FlinkMetricNode}. */
public class FlinkMetricNodeTest {

    @Test
    public void testAddCreatesCounterOnFirstCall() {
        FakeMetricGroup group = new FakeMetricGroup();
        FlinkMetricNode node = new FlinkMetricNode(group, new ArrayList<>());

        node.add("rows", 5L);

        assertEquals(1, group.counterCallCount("rows"), "MetricGroup.counter(\"rows\") should be invoked exactly once");
        FakeCounter created = group.lastCreatedFor("rows");
        assertEquals(1, created.incCallCount, "Counter.inc should be invoked once");
        assertEquals(5L, created.lastIncValue, "Counter.inc should be called with the added value");
        assertEquals(5L, created.totalIncSum);
    }

    @Test
    public void testAddReusesCounter() {
        FakeMetricGroup group = new FakeMetricGroup();
        FlinkMetricNode node = new FlinkMetricNode(group, new ArrayList<>());

        node.add("rows", 5L);
        node.add("rows", 3L);

        assertEquals(
                1,
                group.counterCallCount("rows"),
                "MetricGroup.counter(\"rows\") should be cached after the first call");
        FakeCounter created = group.lastCreatedFor("rows");
        assertEquals(2, created.incCallCount, "Counter.inc should be invoked twice");
        assertEquals(3L, created.lastIncValue, "Counter.inc should be called with the latest value");
        assertEquals(8L, created.totalIncSum, "Counter increments should accumulate (5 + 3)");
    }

    @Test
    public void testAddIgnoresZeroValue() {
        FakeMetricGroup group = new FakeMetricGroup();
        FlinkMetricNode node = new FlinkMetricNode(group, new ArrayList<>());

        node.add("rows", 0L);
        node.add("rows", -1L);

        assertEquals(0, group.counterCallCount("rows"), "No counter should be created when value <= 0");
        assertEquals(0, group.totalCounterCalls(), "No counter call at all should occur for non-positive values");
    }

    @Test
    public void testChildrenPropagation() {
        MetricNode childA = new RecordingMetricNode();
        MetricNode childB = new RecordingMetricNode();
        List<MetricNode> children = Arrays.asList(childA, childB);
        FlinkMetricNode node = new FlinkMetricNode(new FakeMetricGroup(), children);

        assertSame(childA, node.getChild(0));
        assertSame(childB, node.getChild(1));
        assertThrows(IndexOutOfBoundsException.class, () -> node.getChild(2));
    }

    // ---------------------------------------------------------------------
    // Hand-rolled fakes for Flink metric interfaces (no Mockito available).
    // ---------------------------------------------------------------------

    /** Recording fake of {@link Counter}: tracks inc() calls and accumulates totals. */
    private static final class FakeCounter implements Counter {
        int incCallCount;
        long lastIncValue;
        long totalIncSum;

        @Override
        public void inc() {
            inc(1L);
        }

        @Override
        public void inc(long n) {
            incCallCount++;
            lastIncValue = n;
            totalIncSum += n;
        }

        @Override
        public void dec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void dec(long n) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getCount() {
            return totalIncSum;
        }
    }

    /**
     * Recording fake of {@link MetricGroup}: only {@link #counter(String)} is implemented; tracks
     * how many times it was called per metric name and returns the same {@link FakeCounter}
     * instance for a given name across calls (so the production code's caching can be observed
     * by counting invocations).
     */
    private static final class FakeMetricGroup implements MetricGroup {
        private final Map<String, Integer> counterCalls = new HashMap<>();
        private final Map<String, FakeCounter> created = new HashMap<>();

        int counterCallCount(String name) {
            return counterCalls.getOrDefault(name, 0);
        }

        int totalCounterCalls() {
            int sum = 0;
            for (int v : counterCalls.values()) {
                sum += v;
            }
            return sum;
        }

        FakeCounter lastCreatedFor(String name) {
            return created.get(name);
        }

        @Override
        public Counter counter(String name) {
            counterCalls.merge(name, 1, Integer::sum);
            return created.computeIfAbsent(name, k -> new FakeCounter());
        }

        @Override
        public <C extends Counter> C counter(String name, C counter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T, G extends Gauge<T>> G gauge(String name, G gauge) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <H extends Histogram> H histogram(String name, H histogram) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <M extends Meter> M meter(String name, M meter) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MetricGroup addGroup(String name) {
            throw new UnsupportedOperationException();
        }

        @Override
        public MetricGroup addGroup(String key, String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] getScopeComponents() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> getAllVariables() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getMetricIdentifier(String metricName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getMetricIdentifier(String metricName, CharacterFilter filter) {
            throw new UnsupportedOperationException();
        }
    }

    /** Trivial {@link MetricNode} for child-propagation tests; never receives add() calls. */
    private static final class RecordingMetricNode extends MetricNode {
        RecordingMetricNode() {
            super(new ArrayList<>());
        }

        @Override
        public void add(String name, long value) {
            // no-op
        }
    }
}
