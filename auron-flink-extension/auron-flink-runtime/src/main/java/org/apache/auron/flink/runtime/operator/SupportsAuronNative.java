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

import java.util.List;
import org.apache.auron.metric.MetricNode;
import org.apache.auron.protobuf.PhysicalPlanNode;
import org.apache.flink.table.types.logical.RowType;

/**
 * Support native Auron interface.
 */
public interface SupportsAuronNative {

    /**
     * Get physical plan nodes.
     */
    List<PhysicalPlanNode> getPhysicalPlanNodes();

    /**
     * Get output type.
     */
    RowType getOutputType();

    /**
     * Get Auron operator id.
     */
    String getAuronOperatorId();

    /**
     * Get metric node.
     */
    MetricNode getMetricNode();
}
