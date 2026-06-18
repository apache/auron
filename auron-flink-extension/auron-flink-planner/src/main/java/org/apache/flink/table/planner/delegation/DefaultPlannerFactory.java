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
package org.apache.flink.table.planner.delegation;

import java.util.Collections;
import java.util.Set;
import org.apache.auron.flink.table.planner.delegation.AuronStreamPlanner;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.delegation.PlannerFactory;

/**
 * Shadows Flink's stock {@code DefaultPlannerFactory} via fully-qualified-class-name resolution so
 * Auron's planner is constructed for streaming mode. The planner SPI lookup is hard-coded to the
 * {@code "default"} factory identifier, so the only way to redirect planner instantiation is to ship
 * a class at this FQCN ahead of {@code flink-table-planner} on the classpath (the assembly excludes
 * the stock copy). The factory identifier and option sets match the stock factory exactly; only the
 * streaming-mode planner construction changes from {@code StreamPlanner} to {@link
 * AuronStreamPlanner}. Batch mode is unchanged.
 */
public final class DefaultPlannerFactory implements PlannerFactory {

    @Override
    public String factoryIdentifier() {
        return "default";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Planner create(Context context) {
        final RuntimeExecutionMode mode = context.getTableConfig().get(ExecutionOptions.RUNTIME_MODE);
        switch (mode) {
            case STREAMING:
                return new AuronStreamPlanner(
                        context.getExecutor(),
                        context.getTableConfig(),
                        context.getModuleManager(),
                        context.getFunctionCatalog(),
                        context.getCatalogManager(),
                        context.getClassLoader());
            case BATCH:
                return new BatchPlanner(
                        context.getExecutor(),
                        context.getTableConfig(),
                        context.getModuleManager(),
                        context.getFunctionCatalog(),
                        context.getCatalogManager(),
                        context.getClassLoader());
            default:
                throw new TableException(String.format(
                        "Unsupported mode '%s' for '%s'. Only an explicit BATCH or STREAMING mode is "
                                + "supported in Table API.",
                        mode, ExecutionOptions.RUNTIME_MODE.key()));
        }
    }
}
