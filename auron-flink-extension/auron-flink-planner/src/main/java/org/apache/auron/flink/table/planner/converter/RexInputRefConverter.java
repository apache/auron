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

import org.apache.auron.protobuf.PhysicalColumn;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

/**
 * Converts a Calcite {@link RexInputRef} (column reference) to an Auron native {@link
 * PhysicalExprNode} containing a {@link PhysicalColumn}.
 *
 * <p>Column references are always supported — every {@code RexInputRef} maps directly to a named,
 * indexed column in the input schema provided by the {@link ConverterContext}.
 */
public class RexInputRefConverter implements FlinkRexNodeConverter {

    /** {@inheritDoc} */
    @Override
    public Class<? extends RexNode> getNodeClass() {
        return RexInputRef.class;
    }

    /**
     * Always returns {@code true} — column references are unconditionally supported.
     *
     * @param node the RexNode to check (must be a {@link RexInputRef})
     * @param context shared conversion state
     * @return {@code true}
     */
    @Override
    public boolean isSupported(RexNode node, ConverterContext context) {
        return true;
    }

    /**
     * Converts the given {@link RexInputRef} to a {@link PhysicalExprNode} with a {@link
     * PhysicalColumn}.
     *
     * <p>Resolves the column name from the input schema via {@link
     * ConverterContext#getInputType()}.
     *
     * @param node the RexNode to convert (must be a {@link RexInputRef})
     * @param context shared conversion state containing the input schema
     * @return a {@link PhysicalExprNode} wrapping a {@link PhysicalColumn} with name and index
     * @throws IllegalArgumentException if the node is not a {@link RexInputRef}
     */
    @Override
    public PhysicalExprNode convert(RexNode node, ConverterContext context) {
        RexInputRef inputRef = (RexInputRef) node;
        int index = inputRef.getIndex();
        String name = context.getInputType().getFieldNames().get(index);

        return PhysicalExprNode.newBuilder()
                .setColumn(PhysicalColumn.newBuilder().setName(name).setIndex(index))
                .build();
    }
}
