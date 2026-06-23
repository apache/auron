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

import org.apache.calcite.rex.RexNode;

/**
 * Converts a Calcite {@link RexNode} expression to an Auron native
 * {@link org.apache.auron.protobuf.PhysicalExprNode}.
 *
 * <p>Implementations handle specific {@code RexNode} subtypes:
 * <ul>
 *   <li>{@code RexLiteral} — scalar literal values
 *   <li>{@code RexInputRef} — column references (resolved via
 *       {@link ConverterContext#getInputType()})
 *   <li>{@code RexCall} — function/operator calls (arithmetic, comparison, CAST, etc.)
 *   <li>{@code RexFieldAccess} — nested field access
 * </ul>
 *
 * <p>RexNode converters are reusable across operator types — the same {@code RexInputRef}
 * converter works for Calc projections, Agg grouping expressions, and future operators.
 */
public interface FlinkRexNodeConverter extends FlinkNodeConverter<RexNode> {}
