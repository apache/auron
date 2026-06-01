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

/**
 * Marker interface every Auron override of a Flink {@code ExecNode} must implement.
 *
 * <p>An Auron override ships under Flink's own fully-qualified class name so that, with
 * {@code auron-flink-planner} ahead of {@code flink-table-planner} on the classpath, Flink's
 * planner constructs Auron's class instead of the stock one. Because stock Flink classes never
 * reference any Auron type, the presence of this interface on a class is an unambiguous signal that
 * Auron's copy — not Flink's — is the one in effect. The build-time structural tests rely on that
 * signal to verify every override is present and won the shade in the deployment jar.
 *
 * <p>When adding a new Auron override of a Flink {@code ExecNode}, follow all three steps:
 *
 * <ol>
 *   <li>Ship the override class under the stock Flink {@code ExecNode}'s fully-qualified class name
 *       and have it {@code implement} this interface.
 *   <li>List the override's fully-qualified class name in
 *       {@code META-INF/auron/shadowed-flink-execnodes.txt}, the override registry.
 *   <li>Add a matching {@code <exclude>} for the stock class to the shade {@code <filters>} in
 *       {@code auron-flink-assembly/pom.xml}, so the stock copy is dropped from the deployment jar
 *       and Auron's copy wins structurally.
 * </ol>
 */
public interface FlinkAuronExecNode {}
