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
package org.apache.auron.flink.connector.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Properties;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.FilterExecNode;
import org.apache.auron.protobuf.KafkaScanExecNode;
import org.apache.auron.protobuf.PhysicalColumn;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the pure merged-plan building helpers of {@link AuronKafkaSourceFunction}.
 *
 * <p>These tests exercise the splice + metadata-passthrough logic without any JNI, Arrow, or
 * Flink runtime: they build logical sub-plans and a {@code KafkaScan} leaf from raw protobuf
 * builders and assert the fused tree shape.
 */
class AuronKafkaSourceFunctionMergeTest {

    private static PhysicalPlanNode ffiReaderPlaceholder() {
        return PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setNumPartitions(1)
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();
    }

    private static PhysicalExprNode columnExpr(String name, int index) {
        return PhysicalExprNode.newBuilder()
                .setColumn(PhysicalColumn.newBuilder()
                        .setName(name)
                        .setIndex(index)
                        .build())
                .build();
    }

    /** Logical {@code Project[input]} over two logical columns "int" and "name". */
    private static PhysicalPlanNode logicalProjection(PhysicalPlanNode input) {
        return PhysicalPlanNode.newBuilder()
                .setProjection(ProjectionExecNode.newBuilder()
                        .setInput(input)
                        .addExpr(columnExpr("int", 0))
                        .addExprName("int")
                        .addDataType(SchemaConverters.convertToAuronArrowType(new IntType()))
                        .addExpr(columnExpr("name", 1))
                        .addExprName("name")
                        .addDataType(SchemaConverters.convertToAuronArrowType(new VarCharType()))
                        .build())
                .build();
    }

    private static PhysicalPlanNode kafkaScan() {
        return PhysicalPlanNode.newBuilder()
                .setKafkaScan(KafkaScanExecNode.newBuilder().setKafkaTopic("t").build())
                .build();
    }

    private static AuronKafkaSourceFunction newFunction() {
        return new AuronKafkaSourceFunction(
                RowType.of(new LogicalType[] {new IntType(), new VarCharType()}, new String[] {"int", "name"}),
                "op-1",
                "topic",
                new Properties(),
                "Json",
                new java.util.HashMap<>(),
                100,
                "EARLIEST",
                -1);
    }

    @Test
    void testSpliceProjectFilterReplacesLeafWithKafkaScan() {
        PhysicalPlanNode filter = PhysicalPlanNode.newBuilder()
                .setFilter(FilterExecNode.newBuilder()
                        .setInput(ffiReaderPlaceholder())
                        .build())
                .build();
        PhysicalPlanNode logical = logicalProjection(filter);

        PhysicalPlanNode merged = AuronKafkaSourceFunction.buildMergedPlan(logical, kafkaScan());

        // Outer is Projection -> Filter -> KafkaScan
        assertTrue(merged.hasProjection());
        PhysicalPlanNode filterNode = merged.getProjection().getInput();
        assertTrue(filterNode.hasFilter());
        assertTrue(filterNode.getFilter().getInput().hasKafkaScan());
    }

    @Test
    void testSpliceProjectOnlyReplacesLeafWithKafkaScan() {
        PhysicalPlanNode logical = logicalProjection(ffiReaderPlaceholder());

        PhysicalPlanNode merged = AuronKafkaSourceFunction.buildMergedPlan(logical, kafkaScan());

        assertTrue(merged.hasProjection());
        assertTrue(merged.getProjection().getInput().hasKafkaScan());
    }

    @Test
    void testMetadataPassthroughPrependedToOuterProjection() {
        PhysicalPlanNode logical = logicalProjection(ffiReaderPlaceholder());

        ProjectionExecNode proj =
                AuronKafkaSourceFunction.buildMergedPlan(logical, kafkaScan()).getProjection();

        // 3 meta + 2 logical
        assertEquals(5, proj.getExprCount());
        assertEquals(5, proj.getExprNameCount());
        assertEquals(5, proj.getDataTypeCount());

        // First three are the metadata passthroughs, in order.
        assertEquals(KafkaConstants.KAFKA_AURON_META_PARTITION_ID, proj.getExprName(0));
        assertEquals(KafkaConstants.KAFKA_AURON_META_OFFSET, proj.getExprName(1));
        assertEquals(KafkaConstants.KAFKA_AURON_META_TIMESTAMP, proj.getExprName(2));

        assertTrue(proj.getExpr(0).hasColumn());
        assertEquals(
                KafkaConstants.KAFKA_AURON_META_PARTITION_ID,
                proj.getExpr(0).getColumn().getName());
        assertEquals(
                KafkaConstants.KAFKA_AURON_META_OFFSET,
                proj.getExpr(1).getColumn().getName());
        assertEquals(
                KafkaConstants.KAFKA_AURON_META_TIMESTAMP,
                proj.getExpr(2).getColumn().getName());

        assertTrue(proj.getDataType(0).hasINT32());
        assertTrue(proj.getDataType(1).hasINT64());
        assertTrue(proj.getDataType(2).hasINT64());

        // Original logical projection exprs follow, in order.
        assertEquals("int", proj.getExprName(3));
        assertEquals("name", proj.getExprName(4));
        assertEquals("int", proj.getExpr(3).getColumn().getName());
        assertEquals("name", proj.getExpr(4).getColumn().getName());
    }

    @Test
    void testMetaFieldNegativeIndicesDeriveFromMetaFieldCount() {
        int metaCount = KafkaConstants.KAFKA_AURON_META_FIELDS.size();

        // The run() loop reads the metadata columns by row-end-relative index; those indices must
        // derive from KAFKA_AURON_META_FIELDS (its size and the per-field position), so adding a
        // metadata column shifts them automatically rather than misreading every row.
        assertEquals(
                -metaCount,
                AuronKafkaSourceFunction.metaFieldNegativeIndex(KafkaConstants.KAFKA_AURON_META_PARTITION_ID));
        assertEquals(
                -metaCount + 1,
                AuronKafkaSourceFunction.metaFieldNegativeIndex(KafkaConstants.KAFKA_AURON_META_OFFSET));
        assertEquals(
                -metaCount + 2,
                AuronKafkaSourceFunction.metaFieldNegativeIndex(KafkaConstants.KAFKA_AURON_META_TIMESTAMP));
    }

    @Test
    void testHasWatermarkReflectsWatermarkStrategy() {
        AuronKafkaSourceFunction fn = newFunction();
        assertFalse(fn.hasWatermark());
        fn.setWatermarkStrategy(WatermarkStrategy.<RowData>forMonotonousTimestamps());
        assertTrue(fn.hasWatermark());
    }

    @Test
    void testApplyMergedCalcPlanFusesWhenNoWatermark() {
        AuronKafkaSourceFunction fn = newFunction();
        RowType projected = RowType.of(new LogicalType[] {new IntType()}, new String[] {"int"});
        fn.setMergedCalcPlan(logicalProjection(ffiReaderPlaceholder()), projected);

        PhysicalPlanNode fused = fn.applyMergedCalcPlan(kafkaScan());

        // Plan was staged with no watermark: the leaf splices to KafkaScan and the meta passthroughs
        // (3) are prepended to the 2 logical exprs.
        assertTrue(fused.hasProjection());
        assertTrue(fused.getProjection().getInput().hasKafkaScan());
        assertEquals(5, fused.getProjection().getExprCount());
    }

    @Test
    void testApplyMergedCalcPlanThrowsWhenWatermarkedSourceHasStagedPlan() {
        AuronKafkaSourceFunction fn = newFunction();
        RowType projected = RowType.of(new LogicalType[] {new IntType()}, new String[] {"int"});
        fn.setMergedCalcPlan(logicalProjection(ffiReaderPlaceholder()), projected);
        fn.setWatermarkStrategy(WatermarkStrategy.<RowData>forMonotonousTimestamps());

        // The planner gate must never stage a plan on a watermarked source; if it ever does, open()
        // fails fast rather than fusing the Calc below the per-record watermark generator.
        assertThrows(IllegalStateException.class, () -> fn.applyMergedCalcPlan(kafkaScan()));
    }

    @Test
    void testApplyMergedCalcPlanReturnsSourcePlanWhenNoPlanStaged() {
        AuronKafkaSourceFunction fn = newFunction();
        PhysicalPlanNode source = kafkaScan();

        // No staged plan: the source plan passes through untouched, even with a watermark set.
        fn.setWatermarkStrategy(WatermarkStrategy.<RowData>forMonotonousTimestamps());
        assertEquals(source, fn.applyMergedCalcPlan(source));
    }
}
