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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Properties;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.PhysicalColumn;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link AuronKafkaDynamicTableSource}. */
class AuronKafkaDynamicTableSourceTest {

    private static AuronKafkaDynamicTableSource newSource() {
        DataType physicalDataType = DataTypes.ROW(DataTypes.FIELD("int", DataTypes.INT()));
        return new AuronKafkaDynamicTableSource(
                physicalDataType, "topic", new Properties(), "Json", new HashMap<>(), 100, "EARLIEST", null, -1);
    }

    /** A minimal valid {@code Project[FFIReader-placeholder]} logical Calc sub-plan. */
    private static PhysicalPlanNode logicalCalcPlan() {
        PhysicalPlanNode placeholder = PhysicalPlanNode.newBuilder()
                .setFfiReader(FFIReaderExecNode.newBuilder()
                        .setNumPartitions(1)
                        .setExportIterProviderResourceId("placeholder")
                        .build())
                .build();
        PhysicalExprNode intCol = PhysicalExprNode.newBuilder()
                .setColumn(
                        PhysicalColumn.newBuilder().setName("int").setIndex(0).build())
                .build();
        return PhysicalPlanNode.newBuilder()
                .setProjection(ProjectionExecNode.newBuilder()
                        .setInput(placeholder)
                        .addExpr(intCol)
                        .addExprName("int")
                        .build())
                .build();
    }

    private static RowType projectedOutputType() {
        return RowType.of(new LogicalType[] {new IntType()}, new String[] {"int"});
    }

    @Test
    void testCopyPreservesAppliedWatermarkStrategy() {
        AuronKafkaDynamicTableSource source = newSource();
        WatermarkStrategy<RowData> strategy = WatermarkStrategy.forMonotonousTimestamps();
        source.applyWatermark(strategy);

        DynamicTableSource copy = source.copy();

        assertSame(strategy, ((AuronKafkaDynamicTableSource) copy).watermarkStrategy);
    }

    @Test
    void testCopyCarriesStagedMergedPlanIntoBuiltFunction() {
        AuronKafkaDynamicTableSource source = newSource();
        source.setMergedCalcPlan(logicalCalcPlan(), projectedOutputType());

        AuronKafkaDynamicTableSource copy = (AuronKafkaDynamicTableSource) source.copy();

        // The copy still reports the staged plan, and forwards it into the function it builds.
        assertTrue(copy.isMergedCalcPlanSet());
        assertTrue(copy.buildSourceFunction().isMergedCalcPlanSet());
    }

    @Test
    void testStagedPlanForwardedIntoBuiltFunction() {
        AuronKafkaDynamicTableSource source = newSource();
        source.setMergedCalcPlan(logicalCalcPlan(), projectedOutputType());

        AuronKafkaSourceFunction function = source.buildSourceFunction();

        assertTrue(function.isMergedCalcPlanSet());
    }

    @Test
    void testNoStagedPlanLeavesBuiltFunctionUnfused() {
        AuronKafkaDynamicTableSource source = newSource();

        AuronKafkaSourceFunction function = source.buildSourceFunction();

        assertFalse(function.isMergedCalcPlanSet());
    }
}
