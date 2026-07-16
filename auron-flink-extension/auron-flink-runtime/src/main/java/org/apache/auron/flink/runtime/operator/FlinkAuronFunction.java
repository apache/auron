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
 * Flink function which supports Auron should implement this interface.
 *
 * <p>A native Auron source function also exposes the source-side fusion hand-off: a downstream Calc
 * that can fuse into this source registers its logical {@code Project[Filter?]} sub-plan here, and
 * the source splices it onto its native scan and emits the projected rows directly, so the chain
 * runs as one native plan with a single columnar-to-row conversion at the tail.
 */
public interface FlinkAuronFunction extends SupportsAuronNative {

    /**
     * Registers a downstream Calc's logical {@code Project[Filter?[FFIReader-placeholder]]} sub-plan
     * to be fused into this source's native scan. When set, the source runs the fused plan and emits
     * the projected rows in place of its original output.
     *
     * <p>Both arguments must be non-null, and a source may be registered at most once: a second
     * registration is rejected so a second downstream Calc cannot overwrite the first. Callers guard
     * this upstream by checking {@link #isMergedCalcPlanSet()} before registering.
     *
     * @param logicalCalcSubPlan the {@code Project[Filter?]} tree over the logical input columns
     * @param projectedOutputType the projected logical output row type emitted downstream
     * @throws IllegalStateException if a merged Calc plan is already registered
     */
    void setMergedCalcPlan(PhysicalPlanNode logicalCalcSubPlan, RowType projectedOutputType);

    /**
     * Reports whether a merged Calc sub-plan has already been registered, so a second downstream
     * Calc does not overwrite the first.
     *
     * @return {@code true} if {@link #setMergedCalcPlan} has already been called
     */
    boolean isMergedCalcPlanSet();

    /**
     * Reports whether this source emits an event-time watermark. Fusion is disabled for a
     * watermarked source because the downstream Calc must not strip the per-record timestamps it
     * still emits.
     *
     * @return {@code true} if a watermark strategy is configured on this source
     */
    boolean hasWatermark();
}
