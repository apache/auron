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
package org.apache.auron.flink.table.planner.delegation;

import java.util.Collections;
import org.apache.auron.flink.table.planner.processor.AuronSourceCalcFusionProcessor;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.nodes.exec.processor.ExecNodeGraphProcessor;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * Streaming {@link StreamPlanner} subclass that installs Auron's graph-level processors. Stock
 * streaming {@code StreamPlanner.getExecNodeGraphProcessors()} returns an empty {@code Seq}, so
 * appending {@link AuronSourceCalcFusionProcessor} displaces nothing. Auron's shadowed {@code
 * DefaultPlannerFactory} constructs this subclass for streaming mode so the processor runs over the
 * {@code ExecNodeGraph} before Transformation translation.
 */
public class AuronStreamPlanner extends StreamPlanner {

    /**
     * Forwards the stock {@link StreamPlanner} primary constructor unchanged.
     *
     * @param executor the executor
     * @param tableConfig the table configuration
     * @param moduleManager the module manager
     * @param functionCatalog the function catalog
     * @param catalogManager the catalog manager
     * @param userClassLoader the user code class loader
     */
    public AuronStreamPlanner(
            Executor executor,
            TableConfig tableConfig,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog,
            CatalogManager catalogManager,
            ClassLoader userClassLoader) {
        super(executor, tableConfig, moduleManager, functionCatalog, catalogManager, userClassLoader);
    }

    @Override
    public Seq<ExecNodeGraphProcessor> getExecNodeGraphProcessors() {
        // Stream mode ships no processors; install Auron's fusion pass as the sole processor.
        return JavaConverters.asScalaBufferConverter(
                        Collections.<ExecNodeGraphProcessor>singletonList(new AuronSourceCalcFusionProcessor()))
                .asScala()
                .toSeq();
    }
}
