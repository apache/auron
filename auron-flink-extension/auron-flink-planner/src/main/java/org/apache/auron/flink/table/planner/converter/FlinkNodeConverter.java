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

import org.apache.auron.protobuf.PhysicalExprNode;

/**
 * Base interface for converting Flink plan elements to Auron native {@link PhysicalExprNode}
 * representations.
 *
 * <p>This interface is parameterized by the input type to support different categories of plan
 * elements. Two sub-interfaces are provided:
 * <ul>
 *   <li>{@link FlinkRexNodeConverter} for Calcite {@code RexNode} expressions
 *   <li>{@link FlinkAggCallConverter} for Calcite {@code AggregateCall} aggregates
 * </ul>
 *
 * @param <T> the type of plan element this converter handles
 */
public interface FlinkNodeConverter<T> {

    /**
     * Returns the concrete class this converter handles.
     *
     * <p>Used by {@link FlinkNodeConverterFactory} for lookup dispatch.
     */
    Class<? extends T> getNodeClass();

    /**
     * Checks whether the given element can be converted to native execution.
     *
     * <p>A converter may decline based on unsupported types, operand combinations, or
     * configuration. This method must not have side effects.
     *
     * @param node the plan element to check
     * @param context shared conversion state (input schema, configuration)
     * @return {@code true} if the element can be converted
     */
    boolean isSupported(T node, ConverterContext context);

    /**
     * Converts the given element to a native {@link PhysicalExprNode}.
     *
     * @param node the plan element to convert
     * @param context shared conversion state (input schema, configuration)
     * @return the native expression representation
     * @throws IllegalArgumentException if the element type does not match {@link #getNodeClass()}
     */
    PhysicalExprNode convert(T node, ConverterContext context);
}
