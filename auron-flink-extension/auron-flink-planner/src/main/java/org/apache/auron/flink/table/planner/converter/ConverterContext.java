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

import java.util.Objects;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.types.logical.RowType;

/**
 * Provides shared state to {@link FlinkNodeConverter} implementations during conversion.
 *
 * <p>Carries the input schema, configuration, and classloader needed for type-aware conversion
 * of Flink expressions and aggregate calls.
 */
public class ConverterContext {

    private final ReadableConfig tableConfig;
    private final AuronConfiguration auronConfiguration;
    private final ClassLoader classLoader;
    private final RowType inputType;

    /**
     * Counts the UDF wrapper nodes emitted while converting one plan, so each can be given an
     * ordinal that distinguishes it from its siblings. One context covers one Calc conversion,
     * which is the scope the ordinals have to be unique over.
     */
    private int udfWrapperCount;

    /**
     * Creates a new converter context.
     *
     * @param tableConfig Flink table-level configuration
     * @param auronConfiguration Auron-specific configuration, may be {@code null}
     * @param classLoader classloader for the current Flink context
     * @param inputType input schema of the node being converted
     */
    public ConverterContext(
            ReadableConfig tableConfig,
            AuronConfiguration auronConfiguration,
            ClassLoader classLoader,
            RowType inputType) {
        this.tableConfig = Objects.requireNonNull(tableConfig, "tableConfig must not be null");
        this.auronConfiguration = auronConfiguration;
        this.classLoader = Objects.requireNonNull(classLoader, "classLoader must not be null");
        this.inputType = Objects.requireNonNull(inputType, "inputType must not be null");
    }

    /** Returns the Flink table-level configuration. */
    public ReadableConfig getTableConfig() {
        return tableConfig;
    }

    /** Returns the Auron-specific configuration, or {@code null} if not provided. */
    public AuronConfiguration getAuronConfiguration() {
        return auronConfiguration;
    }

    /** Returns the classloader for the current Flink context. */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Returns an ordinal no other wrapper node in this conversion will receive.
     *
     * <p>The value is written into the node's serialized payload at plan time and travels inside
     * the operator, so it is fixed for the life of the job rather than recomputed per drain cycle.
     *
     * @return the next unused wrapper ordinal
     */
    public int nextUdfWrapperOrdinal() {
        return udfWrapperCount++;
    }

    /**
     * Returns the input schema of the node being converted.
     *
     * <p>Converters use this to resolve {@code RexInputRef} column references to concrete types,
     * check type support, and determine if casts are needed.
     *
     * @return the input schema of the node being converted
     */
    public RowType getInputType() {
        return inputType;
    }
}
