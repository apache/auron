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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.auron.flink.table.planner.UnsupportedFlinkNodeRecorder;
import org.apache.auron.flink.utils.SchemaConverters;
import org.apache.auron.jni.AuronAdaptor;
import org.apache.auron.protobuf.FFIReaderExecNode;
import org.apache.auron.protobuf.FilterExecNode;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.auron.protobuf.ProjectionExecNode;
import org.apache.calcite.rex.RexNode;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds a native {@code Project[Filter?[FFIReader-placeholder]]} {@link PhysicalPlanNode} from a
 * Calc's projection and condition.
 *
 * <p>This is the single source of truth for the planner-side Calc-to-native plan composition. Both
 * the shadowed {@code StreamExecCalc} (standalone native Calc) and the graph-level fusion processor
 * build the same sub-plan through {@link #buildNativeCalcPlan}; keeping it here avoids duplicating
 * the converter-framework walk and the {@code FFIReader} leaf assembly.
 */
public final class NativePlanFusionBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(NativePlanFusionBuilder.class);

    private NativePlanFusionBuilder() {
        // utility class
    }

    /**
     * Attempts to compose a native {@code Project[Filter?[FFIReader-placeholder]]} plan from the
     * given projection and condition. Returns {@link Optional#empty()} if any {@link RexNode} is
     * unsupported by the converter framework, or if plan composition throws — both signals are the
     * same for the caller: the Calc cannot run natively.
     *
     * <p>The outer {@link Throwable} catch is defence-in-depth: the converter framework already
     * catches per-{@link RexNode} {@link Exception}, but {@link SchemaConverters} can throw {@link
     * UnsupportedOperationException} on a {@link RowType} containing an unsupported logical type, and
     * protobuf composition can theoretically throw on invalid inputs. Treating any failure as an
     * empty result keeps the caller safe.
     *
     * @param tableConfig the table-level configuration used to seed the converter context
     * @param projection the projection expressions evaluated per row
     * @param condition the filter expression evaluated per row, or {@code null} for no filter
     * @param inputRowType the upstream row type used by the converter context
     * @param outputRowType the row type of this Calc's output
     * @return a composed plan, or empty if conversion failed
     */
    public static Optional<PhysicalPlanNode> buildNativeCalcPlan(
            ReadableConfig tableConfig,
            List<RexNode> projection,
            @Nullable RexNode condition,
            RowType inputRowType,
            RowType outputRowType) {
        try {
            final ConverterContext ctx = new ConverterContext(
                    tableConfig,
                    AuronAdaptor.getInstance().getAuronConfiguration(),
                    Thread.currentThread().getContextClassLoader(),
                    inputRowType);
            final FlinkNodeConverterFactory converters = FlinkNodeConverterFactory.getInstance();

            PhysicalExprNode filterExpr = null;
            if (condition != null) {
                final Optional<PhysicalExprNode> c = converters.convertRexNode(condition, ctx);
                if (!c.isPresent()) {
                    recordFallback(condition.getClass());
                    return Optional.empty();
                }
                filterExpr = c.get();
            }

            final List<PhysicalExprNode> projectExprs = new ArrayList<>(projection.size());
            for (RexNode rex : projection) {
                final Optional<PhysicalExprNode> c = converters.convertRexNode(rex, ctx);
                if (!c.isPresent()) {
                    recordFallback(rex.getClass());
                    return Optional.empty();
                }
                projectExprs.add(c.get());
            }

            // numPartitions = 1 because each parallel FlinkAuronCalcOperator subtask owns a
            // single Java-side exporter and one corresponding native partition; per-subtask
            // parallelism is governed by Flink's outer Transformation parallelism, not by the
            // FFI Reader's partition count.
            final FFIReaderExecNode ffiReader = FFIReaderExecNode.newBuilder()
                    .setNumPartitions(1)
                    .setSchema(SchemaConverters.convertToAuronSchema(inputRowType, false))
                    .setExportIterProviderResourceId("placeholder")
                    .build();
            PhysicalPlanNode current =
                    PhysicalPlanNode.newBuilder().setFfiReader(ffiReader).build();

            if (filterExpr != null) {
                final FilterExecNode filterNode = FilterExecNode.newBuilder()
                        .setInput(current)
                        .addExpr(filterExpr)
                        .build();
                current = PhysicalPlanNode.newBuilder().setFilter(filterNode).build();
            }

            final ProjectionExecNode.Builder proj =
                    ProjectionExecNode.newBuilder().setInput(current);
            for (int i = 0; i < projectExprs.size(); i++) {
                proj.addExpr(projectExprs.get(i));
                proj.addExprName(outputRowType.getFieldNames().get(i));
                proj.addDataType(SchemaConverters.convertToAuronArrowType(outputRowType.getTypeAt(i)));
            }
            return Optional.of(
                    PhysicalPlanNode.newBuilder().setProjection(proj.build()).build());

        } catch (Throwable t) {
            UnsupportedFlinkNodeRecorder.recordCompositionFailure();
            LOG.warn(
                    "Auron native Calc plan composition threw {}; the Calc cannot run natively.",
                    t.getClass().getName(),
                    t);
            return Optional.empty();
        }
    }

    /**
     * Logs a WARN line on the first occurrence of each unsupported {@link RexNode} class per JVM.
     * Subsequent occurrences are silent.
     */
    private static void recordFallback(Class<? extends RexNode> unsupportedRexClass) {
        if (UnsupportedFlinkNodeRecorder.recordFallback(unsupportedRexClass)) {
            LOG.warn(
                    "Auron native Calc fallback: unsupported RexNode {}; the Calc cannot run natively.",
                    unsupportedRexClass.getName());
        }
    }
}
