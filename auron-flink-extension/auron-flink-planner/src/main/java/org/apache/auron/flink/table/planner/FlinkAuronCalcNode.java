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
package org.apache.auron.flink.table.planner;

import javax.annotation.Nullable;
import org.apache.calcite.rex.RexNode;

/**
 * Exposes a Calc {@code ExecNode}'s projection and condition to the graph-level fusion pass, which
 * lives in a different package than the shadowed {@code StreamExecCalc}.
 *
 * <p>The fusion processor depends on this Auron-package interface rather than the concrete shadowed
 * {@code StreamExecCalc}: when a source file references the shadowed class by name, {@code javac} may
 * bind to the stock {@code flink-table-planner} copy on the compile classpath (which lacks Auron's
 * accessors) instead of the in-module override. Because stock Flink classes never reference any Auron
 * type, depending on this interface keeps the cross-package access unambiguous.
 */
public interface FlinkAuronCalcNode {

    /**
     * The projection expressions this Calc evaluates per row.
     *
     * @return the projection {@link RexNode}s
     */
    java.util.List<RexNode> getProjection();

    /**
     * The filter expression this Calc evaluates per row, or {@code null} when there is no filter.
     *
     * @return the filter {@link RexNode}, or {@code null}
     */
    @Nullable
    RexNode getCondition();
}
