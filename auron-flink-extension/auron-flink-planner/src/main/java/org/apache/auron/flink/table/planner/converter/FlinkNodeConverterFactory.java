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
package org.apache.auron.flink.table.planner.converter;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton registry of {@link FlinkNodeConverter} instances. Dispatches conversion requests to
 * the appropriate converter based on the input element's class.
 *
 * <p>Maintains separate registries for different converter categories:
 * <ul>
 *   <li>{@code rexConverterMap} for {@link FlinkRexNodeConverter} instances
 *       (keyed by {@code RexNode} subclass)
 *   <li>{@code aggConverterMap} for {@link FlinkAggCallConverter} instances
 *       (keyed by {@code AggregateCall} class)
 * </ul>
 *
 * <p>Usage:
 * <pre>
 *   FlinkNodeConverterFactory factory = FlinkNodeConverterFactory.getInstance();
 *   // Convert a RexNode expression
 *   Optional&lt;PhysicalExprNode&gt; result = factory.convertRexNode(rexNode, context);
 *   // Convert an AggregateCall
 *   Optional&lt;PhysicalExprNode&gt; aggResult = factory.convertAggCall(aggCall, context);
 * </pre>
 */
public class FlinkNodeConverterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkNodeConverterFactory.class);

    private static final FlinkNodeConverterFactory INSTANCE = new FlinkNodeConverterFactory();

    private final Map<Class<? extends RexNode>, FlinkRexNodeConverter> rexConverterMap;
    private final Map<Class<? extends AggregateCall>, FlinkAggCallConverter> aggConverterMap;

    // Package-private for test isolation (tests create fresh instances)
    FlinkNodeConverterFactory() {
        this.rexConverterMap = new HashMap<>();
        this.aggConverterMap = new HashMap<>();
    }

    /** Returns the singleton instance. */
    public static FlinkNodeConverterFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Registers a {@link FlinkRexNodeConverter} for its declared {@code RexNode} subclass.
     *
     * @param converter the converter to register
     * @throws IllegalArgumentException if a converter is already registered for the same class
     */
    public void registerRexConverter(FlinkRexNodeConverter converter) {
        Class<? extends RexNode> nodeClass = converter.getNodeClass();
        if (rexConverterMap.containsKey(nodeClass)) {
            throw new IllegalArgumentException("Duplicate RexNode converter for " + nodeClass.getName());
        }
        rexConverterMap.put(nodeClass, converter);
    }

    /**
     * Registers a {@link FlinkAggCallConverter} for its declared {@code AggregateCall} class.
     *
     * @param converter the converter to register
     * @throws IllegalArgumentException if a converter is already registered for the same class
     */
    public void registerAggConverter(FlinkAggCallConverter converter) {
        Class<? extends AggregateCall> nodeClass = converter.getNodeClass();
        if (aggConverterMap.containsKey(nodeClass)) {
            throw new IllegalArgumentException("Duplicate AggregateCall converter for " + nodeClass.getName());
        }
        aggConverterMap.put(nodeClass, converter);
    }

    /**
     * Attempts to convert the given {@link RexNode} to a native {@link PhysicalExprNode}.
     *
     * <p>Returns the native expression if a matching converter exists and supports the node.
     * Returns empty if no converter is registered, the converter does not support the node,
     * or conversion fails (fail-safe).
     *
     * @param node the RexNode to convert
     * @param context shared conversion state
     * @return the converted expression, or empty
     */
    public Optional<PhysicalExprNode> convertRexNode(RexNode node, ConverterContext context) {
        FlinkRexNodeConverter converter = rexConverterMap.get(node.getClass());
        if (converter == null) {
            return Optional.empty();
        }
        if (!converter.isSupported(node, context)) {
            return Optional.empty();
        }
        try {
            return Optional.of(converter.convert(node, context));
        } catch (Exception e) {
            LOG.warn("RexNode conversion failed for {}", node.getClass().getName(), e);
            return Optional.empty();
        }
    }

    /**
     * Attempts to convert the given {@link AggregateCall} to a native {@link PhysicalExprNode}.
     *
     * <p>Returns the native expression if a matching converter exists and supports the call.
     * Returns empty if no converter is registered, the converter does not support the call,
     * or conversion fails (fail-safe).
     *
     * @param aggCall the AggregateCall to convert
     * @param context shared conversion state
     * @return the converted expression, or empty
     */
    public Optional<PhysicalExprNode> convertAggCall(AggregateCall aggCall, ConverterContext context) {
        FlinkAggCallConverter converter = aggConverterMap.get(aggCall.getClass());
        if (converter == null) {
            return Optional.empty();
        }
        if (!converter.isSupported(aggCall, context)) {
            return Optional.empty();
        }
        try {
            return Optional.of(converter.convert(aggCall, context));
        } catch (Exception e) {
            LOG.warn(
                    "AggregateCall conversion failed for {}", aggCall.getClass().getName(), e);
            return Optional.empty();
        }
    }

    /**
     * Returns the converter registered for the given element's class, if any.
     *
     * <p>Dispatches by type hierarchy: checks {@code RexNode} converters first, then
     * {@code AggregateCall} converters.
     *
     * @param nodeClass the class to look up
     * @return the matching converter, or empty
     */
    @SuppressWarnings("unchecked")
    public Optional<FlinkNodeConverter<?>> getConverter(Class<?> nodeClass) {
        if (RexNode.class.isAssignableFrom(nodeClass)) {
            return Optional.ofNullable(rexConverterMap.get((Class<? extends RexNode>) nodeClass));
        } else if (AggregateCall.class.isAssignableFrom(nodeClass)) {
            return Optional.ofNullable(aggConverterMap.get((Class<? extends AggregateCall>) nodeClass));
        }
        return Optional.empty();
    }
}
