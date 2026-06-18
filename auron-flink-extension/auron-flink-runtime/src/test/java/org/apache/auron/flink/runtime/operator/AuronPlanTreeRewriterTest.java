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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.FilterExecNode;
import org.apache.auron.protobuf.KafkaScanExecNode;
import org.apache.auron.protobuf.LimitExecNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the shared {@link AuronPlanTreeRewriter} used by both the standalone native Calc
 * operator and the Kafka source's Calc fusion to walk a {@code Project[Filter?[FFIReader]]} tree
 * down to its FFI Reader leaf.
 */
class AuronPlanTreeRewriterTest {

    private static PhysicalPlanNode ffiReader() {
        return PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();
    }

    private static PhysicalPlanNode kafkaScan() {
        return PhysicalPlanNode.newBuilder()
                .setKafkaScan(KafkaScanExecNode.newBuilder().setKafkaTopic("t").build())
                .build();
    }

    private static PhysicalPlanNode project(PhysicalPlanNode input) {
        return PhysicalPlanNode.newBuilder()
                .setProjection(ProjectionExecNode.newBuilder().setInput(input).build())
                .build();
    }

    private static PhysicalPlanNode filter(PhysicalPlanNode input) {
        return PhysicalPlanNode.newBuilder()
                .setFilter(FilterExecNode.newBuilder().setInput(input).build())
                .build();
    }

    @Test
    void replacesLeafUnderProjection() {
        PhysicalPlanNode out =
                AuronPlanTreeRewriter.rewriteFfiReaderLeaf(project(ffiReader()), leaf -> kafkaScan(), "bad: ");

        assertTrue(out.hasProjection());
        assertTrue(out.getProjection().getInput().hasKafkaScan());
    }

    @Test
    void replacesLeafUnderProjectionFilter() {
        PhysicalPlanNode out =
                AuronPlanTreeRewriter.rewriteFfiReaderLeaf(project(filter(ffiReader())), leaf -> kafkaScan(), "bad: ");

        assertTrue(out.hasProjection());
        assertTrue(out.getProjection().getInput().hasFilter());
        assertTrue(out.getProjection().getInput().getFilter().getInput().hasKafkaScan());
    }

    @Test
    void appliesLeafTransformToFfiReaderBearingNode() {
        // The transform receives the FFI Reader-bearing node, so an in-place edit (the Calc
        // operator's resource-ID injection) sees the FFI Reader and can rebuild it.
        PhysicalPlanNode out = AuronPlanTreeRewriter.rewriteFfiReaderLeaf(
                project(ffiReader()),
                leaf -> leaf.toBuilder()
                        .setFfiReader(leaf.getFfiReader().toBuilder()
                                .setExportIterProviderResourceId("rid")
                                .build())
                        .build(),
                "bad: ");

        assertEquals("rid", out.getProjection().getInput().getFfiReader().getExportIterProviderResourceId());
    }

    @Test
    void throwsWithMessagePrefixOnUnsupportedShape() {
        PhysicalPlanNode unsupported = PhysicalPlanNode.newBuilder()
                .setLimit(LimitExecNode.newBuilder().setLimit(1).build())
                .build();

        IllegalArgumentException ex = assertThrows(
                IllegalArgumentException.class,
                () -> AuronPlanTreeRewriter.rewriteFfiReaderLeaf(unsupported, leaf -> leaf, "unexpected shape: "));
        assertTrue(ex.getMessage().startsWith("unexpected shape: "));
        assertTrue(ex.getMessage().contains("LIMIT"));
    }
}
