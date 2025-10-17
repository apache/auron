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
package org.apache.auron.metric;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstract class representing a metric node in the Auron system.
 * This class provides functionality for hierarchical metrics tracking.
 */
public abstract class MetricNode implements Serializable {
    private final List<MetricNode> children = new ArrayList<>();

    public MetricNode(List<MetricNode> children) {
        this.children.addAll(children);
    }

    /**
     * Gets a child metric node with the specified ID.
     *
     * @param id The identifier for the child node
     * @return The child MetricNode associated with the given ID
     */
    public MetricNode getChild(int id) {
        return children.get(id);
    }

    /**
     * Adds a metric value with a specified name.
     *
     * @param name The name of the metric
     * @param value The value to add for the metric (as a long)
     */
    public abstract void add(String name, long value);
}
