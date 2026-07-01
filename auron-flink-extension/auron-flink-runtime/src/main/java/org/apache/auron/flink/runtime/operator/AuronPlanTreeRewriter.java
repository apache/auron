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
package org.apache.auron.flink.runtime.operator;

import java.util.function.UnaryOperator;
import org.apache.auron.protobuf.PhysicalPlanNode;

/**
 * Shared rewriter for native Calc plan trees of shape {@code Project[Filter?[FFIReader]]}.
 *
 * <p>Both the standalone native Calc operator (which injects a runtime resource ID into the FFI
 * Reader leaf) and the Kafka source's Calc fusion (which replaces the FFI Reader placeholder leaf
 * with a {@code KafkaScan}) walk the same Project/Filter spine down to the FFI Reader leaf and
 * rebuild the path around a transformed leaf. This helper captures that single walk, parameterized
 * by the leaf transform.
 */
public final class AuronPlanTreeRewriter {

    private AuronPlanTreeRewriter() {}

    /**
     * Recursively walks {@code node} through {@code Projection} / {@code Filter} wrappers to the
     * {@code FFIReader} leaf, applies {@code leafTransform} to the FFI Reader-bearing node, and
     * rebuilds the wrappers around the transformed leaf. The transform may either edit the FFI
     * Reader in place (returning a node that still bears it) or replace it with a different node
     * (e.g. a {@code KafkaScan}).
     *
     * <p>Accepted shapes: {@code FFIReader}, {@code Filter[FFIReader]}, {@code Project[FFIReader]},
     * {@code Project[Filter[FFIReader]]}. Any other node type fails fast with an
     * {@link IllegalArgumentException} whose message starts with {@code unexpectedShapeMessage} so
     * misconfigurations surface at operator startup rather than as silent fallthrough.
     *
     * @param node the plan node to walk
     * @param leafTransform applied to the FFI Reader-bearing node to produce its replacement
     * @param unexpectedShapeMessage prefix for the {@link IllegalArgumentException} thrown on an
     *     unsupported node type; the offending {@code PhysicalPlanTypeCase} is appended
     * @return a new plan tree with the FFI Reader leaf transformed
     */
    public static PhysicalPlanNode rewriteFfiReaderLeaf(
            PhysicalPlanNode node, UnaryOperator<PhysicalPlanNode> leafTransform, String unexpectedShapeMessage) {
        if (node.hasFfiReader()) {
            return leafTransform.apply(node);
        }
        if (node.hasProjection()) {
            PhysicalPlanNode rewrittenInput =
                    rewriteFfiReaderLeaf(node.getProjection().getInput(), leafTransform, unexpectedShapeMessage);
            return node.toBuilder()
                    .setProjection(node.getProjection().toBuilder()
                            .setInput(rewrittenInput)
                            .build())
                    .build();
        }
        if (node.hasFilter()) {
            PhysicalPlanNode rewrittenInput =
                    rewriteFfiReaderLeaf(node.getFilter().getInput(), leafTransform, unexpectedShapeMessage);
            return node.toBuilder()
                    .setFilter(node.getFilter().toBuilder()
                            .setInput(rewrittenInput)
                            .build())
                    .build();
        }
        throw new IllegalArgumentException(unexpectedShapeMessage + node.getPhysicalPlanTypeCase());
    }
}
