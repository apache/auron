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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Enforces the override contract documented on {@link FlinkAuronExecNode}: every Auron override of a
 * Flink {@code ExecNode} compiled under {@code org.apache.flink.table.planner.plan.nodes.exec} must
 * implement the marker interface, and the override registry
 * {@code META-INF/auron/shadowed-flink-execnodes.txt} must list exactly those override classes.
 *
 * <p>Scanning compiled classes (rather than a hand-maintained list) means a developer who adds a new
 * override but forgets to implement the marker, or forgets to register it, fails the build here
 * rather than discovering a silently-stock {@code ExecNode} at deployment time.
 */
class ShadowedExecNodeRegistryTest {

    private static final String EXEC_PACKAGE = "org.apache.flink.table.planner.plan.nodes.exec";

    private static final String REGISTRY_RESOURCE = "/META-INF/auron/shadowed-flink-execnodes.txt";

    @Test
    void everyOverrideImplementsMarker() {
        for (Class<?> c : concreteTopLevelExecClasses()) {
            assertTrue(
                    FlinkAuronExecNode.class.isAssignableFrom(c),
                    "Override class " + c.getName() + " under " + EXEC_PACKAGE
                            + " does not implement " + FlinkAuronExecNode.class.getName()
                            + ". Every Auron override of a Flink ExecNode must implement this marker "
                            + "interface so the build can verify it won the shade.");
        }
    }

    @Test
    void registryMatchesMarkerSet() {
        Set<String> markerSet = new TreeSet<>();
        for (Class<?> c : concreteTopLevelExecClasses()) {
            if (FlinkAuronExecNode.class.isAssignableFrom(c)) {
                markerSet.add(c.getName());
            }
        }
        Set<String> registry = new TreeSet<>(readRegistry());

        Set<String> implementedButUnregistered = new TreeSet<>(markerSet);
        implementedButUnregistered.removeAll(registry);
        Set<String> registeredButNotImplemented = new TreeSet<>(registry);
        registeredButNotImplemented.removeAll(markerSet);

        assertEquals(
                markerSet,
                registry,
                "The override registry " + REGISTRY_RESOURCE + " is out of sync with the set of classes "
                        + "implementing " + FlinkAuronExecNode.class.getSimpleName()
                        + ". Classes implementing the marker but not registered: " + implementedButUnregistered
                        + "; registered but not found / not implementing the marker: " + registeredButNotImplemented
                        + ". Update META-INF/auron/shadowed-flink-execnodes.txt to list exactly the override classes.");
    }

    /**
     * Returns the concrete, top-level classes compiled under {@link #EXEC_PACKAGE}. Interfaces,
     * abstract classes, and inner classes (whose binary name contains {@code $}) are skipped, since
     * the override contract applies only to the concrete {@code ExecNode} classes Flink instantiates
     * by fully-qualified name.
     */
    private static Set<Class<?>> concreteTopLevelExecClasses() {
        Path classesDir = Paths.get(System.getProperty("user.dir"), "target", "classes");
        Path execDir = classesDir.resolve(EXEC_PACKAGE.replace('.', '/'));
        if (!Files.isDirectory(execDir)) {
            fail("Compiled exec-node package not found at " + execDir
                    + " — the module must be compiled before this test runs.");
        }
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Set<Class<?>> result = new TreeSet<>((a, b) -> a.getName().compareTo(b.getName()));
        try (Stream<Path> paths = Files.walk(execDir)) {
            for (Path classFile :
                    paths.filter(p -> p.toString().endsWith(".class")).collect(Collectors.toList())) {
                String fqcn = toFqcn(classesDir, classFile);
                if (fqcn.contains("$")) {
                    continue;
                }
                Class<?> c;
                try {
                    c = Class.forName(fqcn, false, loader);
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    throw new IllegalStateException("Failed to load compiled class " + fqcn, e);
                }
                if (c.isInterface() || Modifier.isAbstract(c.getModifiers())) {
                    continue;
                }
                result.add(c);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to walk compiled exec-node package at " + execDir, e);
        }
        return result;
    }

    private static String toFqcn(Path classesDir, Path classFile) {
        String relative = classesDir.relativize(classFile).toString();
        return relative.substring(0, relative.length() - ".class".length())
                .replace(classFile.getFileSystem().getSeparator(), ".");
    }

    private static Set<String> readRegistry() {
        Set<String> entries = new TreeSet<>();
        try (InputStream in = ShadowedExecNodeRegistryTest.class.getResourceAsStream(REGISTRY_RESOURCE)) {
            if (in == null) {
                fail("Override registry resource not found on the classpath: " + REGISTRY_RESOURCE);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                        continue;
                    }
                    entries.add(trimmed);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read override registry " + REGISTRY_RESOURCE, e);
        }
        return entries;
    }
}
