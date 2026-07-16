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

import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Table-source-layer marker for a native Auron {@code DynamicTableSource} that can host a fused
 * downstream Calc. A graph-level fusion pass detects the source through this marker (rather than a
 * concrete connector class, so any future native source is a fusion target), stages the merged
 * {@code Project[Filter?]} plan on the source instance, and the source forwards it into its source
 * function when the runtime provider is built.
 *
 * <p>This is the table-source parallel of {@link FlinkAuronFunction}, which carries the same fusion
 * hand-off one layer down at the source-function level.
 */
public interface FlinkAuronDynamicTableSource {

    /**
     * Stages a downstream Calc's logical {@code Project[Filter?]} sub-plan on this source so that the
     * source forwards it into its source function at build time. The function splices the sub-plan
     * onto its native scan and emits the projected rows directly.
     *
     * <p>Both arguments must be non-null, and a source may be staged at most once: a second
     * registration is rejected so one Calc cannot silently discard another's staged plan. The fusion
     * pass enforces this upstream by checking {@link #isMergedCalcPlanSet()} before staging.
     *
     * @param logicalCalcSubPlan the {@code Project[Filter?]} tree over the logical input columns
     * @param projectedOutputType the projected logical output row type emitted downstream
     * @throws IllegalStateException if a merged Calc plan is already staged on this source
     */
    void setMergedCalcPlan(PhysicalPlanNode logicalCalcSubPlan, RowType projectedOutputType);

    /**
     * Reports whether a merged Calc sub-plan has already been staged on this source, so the fusion
     * pass can skip a source that is already staged (multi-consumer or re-entry safety).
     *
     * @return {@code true} if {@link #setMergedCalcPlan} has already been called
     */
    boolean isMergedCalcPlanSet();

    /**
     * Reports whether this source emits an event-time watermark. The fusion pass uses this as the
     * planner-time gate so it never fuses over a watermarked source; the source function re-checks
     * the same condition at {@code open()} and throws {@link IllegalStateException} if a merged plan
     * and a watermark ever coexist, as the fail-fast correctness backstop for this gate.
     *
     * @return {@code true} if a watermark strategy is configured on this source
     */
    boolean hasWatermark();
}
