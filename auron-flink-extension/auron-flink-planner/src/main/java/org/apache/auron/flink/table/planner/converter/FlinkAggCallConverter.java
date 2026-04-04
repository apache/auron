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

import org.apache.calcite.rel.core.AggregateCall;

/**
 * Converts a Calcite {@link AggregateCall} to an Auron native
 * {@link org.apache.auron.protobuf.PhysicalExprNode} (wrapping a
 * {@code PhysicalAggExprNode}).
 *
 * <p>An {@code AggregateCall} represents an aggregate function invocation (e.g., SUM, COUNT, MAX)
 * with its argument references, return type, and distinctness. The converter translates this into
 * a {@code PhysicalAggExprNode} containing the aggregate function type, converted child
 * expressions, and return type.
 *
 * <p>Note: {@code AggregateCall} internally references input columns by index. The converter uses
 * {@link ConverterContext#getInputType()} to resolve these indices to concrete types for type
 * checking and cast insertion.
 */
public interface FlinkAggCallConverter extends FlinkNodeConverter<AggregateCall> {}
