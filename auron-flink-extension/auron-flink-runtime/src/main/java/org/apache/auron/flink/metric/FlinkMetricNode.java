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

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.auron.metric.MetricNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

/**
 * Flink-side {@link MetricNode} that forwards metric updates published by the native engine to a
 * Flink {@link MetricGroup}.
 *
 * <p>Each distinct metric name is lazily mapped to a Flink {@link Counter} on first use. Counters
 * are cached in a {@link ConcurrentHashMap} because metric callbacks may originate from native
 * threads invoked via JNI, so creation must be thread-safe.
 *
 * <p>Only strictly positive values are forwarded. This mirrors the semantics of
 * {@code SparkMetricNode} (see {@code spark-extension/.../SparkMetricNode.scala}), where
 * non-positive deltas are ignored to avoid spurious counter activity from native callbacks that
 * report zero/negative deltas for unaffected metrics.
 */
public class FlinkMetricNode extends MetricNode {

    private static final long serialVersionUID = 1L;

    private final transient MetricGroup metricGroup;
    private final transient ConcurrentHashMap<String, Counter> counters = new ConcurrentHashMap<>();

    /**
     * Constructs a new {@link FlinkMetricNode}.
     *
     * @param metricGroup the Flink metric group counters are registered against; must not be null
     * @param children the child metric nodes used for hierarchical metric trees
     */
    public FlinkMetricNode(MetricGroup metricGroup, List<MetricNode> children) {
        super(children);
        this.metricGroup = Objects.requireNonNull(metricGroup, "metricGroup");
    }

    /**
     * Recursively constructs a {@link FlinkMetricNode} whose shape mirrors {@code node}'s plan
     * tree. Native code walks the execution plan during periodic metric updates and at
     * finalization, indexing into the parallel metric tree via {@link MetricNode#getChild(int)};
     * an empty children list would throw {@link IndexOutOfBoundsException} on the first {@code
     * getChild(0)} call. All levels share the same Flink {@link MetricGroup} so named counters
     * aggregate at the operator scope.
     *
     * <p>Supported leaves are {@code FFIReader} (standalone Calc) and {@code KafkaScan} (Kafka
     * source, including after Calc fusion). Supported unary nodes are {@code Projection} and
     * {@code Filter}.
     *
     * @param node the physical plan whose shape the metric tree must match; must not be null
     * @param metricGroup the Flink metric group all tree levels register counters against; must
     *     not be null
     * @return the root of the mirrored metric tree
     */
    public static FlinkMetricNode fromPlan(PhysicalPlanNode node, MetricGroup metricGroup) {
        Objects.requireNonNull(node, "node");
        Objects.requireNonNull(metricGroup, "metricGroup");
        final List<MetricNode> children;
        if (node.hasFfiReader() || node.hasKafkaScan()) {
            children = Collections.emptyList();
        } else if (node.hasProjection()) {
            children = Collections.singletonList(fromPlan(node.getProjection().getInput(), metricGroup));
        } else if (node.hasFilter()) {
            children = Collections.singletonList(fromPlan(node.getFilter().getInput(), metricGroup));
        } else {
            throw new IllegalArgumentException(
                    "Unexpected plan node type for metric tree: " + node.getPhysicalPlanTypeCase());
        }
        return new FlinkMetricNode(metricGroup, children);
    }

    /**
     * Adds {@code value} to the Flink {@link Counter} named {@code name}, creating the counter on
     * first use. Non-positive values are ignored.
     *
     * @param name the metric name
     * @param value the delta to apply; ignored when {@code <= 0}
     */
    @Override
    public void add(String name, long value) {
        if (value > 0) {
            counters.computeIfAbsent(name, metricGroup::counter).inc(value);
        }
    }
}
