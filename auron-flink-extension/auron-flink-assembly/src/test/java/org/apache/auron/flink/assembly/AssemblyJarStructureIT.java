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
package org.apache.auron.flink.assembly;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;

/**
 * Structural smoke test for the shaded {@code auron-flink-assembly} uber jar.
 *
 * <p>The assembly bundles Flink's {@code flink-table-planner} content together with Auron's
 * override of {@code StreamExecCalc} (same fully-qualified class name). Because a jar holds at most
 * one entry per path, the assembly always contains exactly one {@code StreamExecCalc.class} — but
 * <em>which</em> copy survives the shade is what matters. The deployment guarantee A2 relies on is
 * that the surviving copy is <em>Auron's</em>, so that native Calc activation is structural rather
 * than dependent on shade artifact-processing order.
 *
 * <p>This test asserts the structural <em>outcome</em>: the surviving {@code StreamExecCalc} carries
 * Auron's bytecode marker, the full planner is bundled (not just Auron's one class), and the ASF
 * NOTICE names both products. The load-bearing-ness of the shade {@code <filters>} exclude that
 * produces this outcome is verified separately by the manual A/B procedure documented in the
 * reviewhelper — this test certifies the result that exclude must achieve.
 *
 * <p>Inspection is purely byte-level via {@link JarFile}: no classloading, and Flink is not on the
 * test classpath.
 */
class AssemblyJarStructureIT {

    private static final String STREAM_EXEC_CALC_ENTRY =
            "org/apache/flink/table/planner/plan/nodes/exec/stream/StreamExecCalc.class";

    /**
     * Internal-name marker that appears in the constant pool of Auron's {@code StreamExecCalc} (it
     * imports and constructs {@code FlinkAuronCalcOperator}). Flink's stock {@code StreamExecCalc}
     * has no reference to any Auron type, so this byte sequence distinguishes the two copies.
     */
    private static final byte[] AURON_MARKER =
            "org/apache/auron/flink/runtime/operator/FlinkAuronCalcOperator".getBytes(StandardCharsets.UTF_8);

    private static final String STREAM_EXEC_PREFIX = "org/apache/flink/table/planner/plan/nodes/exec/stream/StreamExec";

    @Test
    void exactlyOneStreamExecCalc() throws IOException {
        try (JarFile jar = openAssemblyJar()) {
            int count = 0;
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                if (STREAM_EXEC_CALC_ENTRY.equals(entries.nextElement().getName())) {
                    count++;
                }
            }
            assertEquals(1, count, "Expected exactly one " + STREAM_EXEC_CALC_ENTRY + " entry, found " + count);
        }
    }

    @Test
    void streamExecCalcIsAuron() throws IOException {
        try (JarFile jar = openAssemblyJar()) {
            JarEntry entry = jar.getJarEntry(STREAM_EXEC_CALC_ENTRY);
            assertNotNull(entry, "Assembly jar is missing " + STREAM_EXEC_CALC_ENTRY);
            byte[] classBytes = readEntry(jar, entry);
            assertTrue(
                    indexOf(classBytes, AURON_MARKER) >= 0,
                    "The bundled StreamExecCalc is not Auron's: its bytecode does not reference "
                            + "FlinkAuronCalcOperator. Flink's stock copy must have shadowed Auron's "
                            + "override — the determinism filter is not effective.");
        }
    }

    @Test
    void flinkPlannerContentBundled() throws IOException {
        try (JarFile jar = openAssemblyJar()) {
            boolean otherStreamExecPresent = false;
            Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                String name = entries.nextElement().getName();
                if (name.startsWith(STREAM_EXEC_PREFIX)
                        && name.endsWith(".class")
                        && !name.equals(STREAM_EXEC_CALC_ENTRY)) {
                    otherStreamExecPresent = true;
                    break;
                }
            }
            assertTrue(
                    otherStreamExecPresent,
                    "Assembly jar contains no StreamExec* class other than StreamExecCalc — the Flink "
                            + "planner content does not appear to be bundled.");
        }
    }

    @Test
    void noticeNamesAuronAndFlink() throws IOException {
        try (JarFile jar = openAssemblyJar()) {
            JarEntry notice = jar.getJarEntry("META-INF/NOTICE");
            assertNotNull(notice, "Assembly jar is missing META-INF/NOTICE");
            String text = new String(readEntry(jar, notice), StandardCharsets.UTF_8);
            assertTrue(text.contains("Apache Auron"), "META-INF/NOTICE does not name \"Apache Auron\"");
            assertTrue(
                    text.contains("Apache Flink"),
                    "META-INF/NOTICE does not name \"Apache Flink\" — the bundled, modified planner "
                            + "content is not attributed.");
        }
    }

    private static JarFile openAssemblyJar() throws IOException {
        Path targetDir = Paths.get(System.getProperty("user.dir"), "target");
        if (!Files.isDirectory(targetDir)) {
            fail("Module target/ directory not found at "
                    + targetDir
                    + " — the assembly jar must be built (package phase) before this test runs.");
        }
        List<Path> candidates = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(targetDir, "auron-flink-assembly-*.jar")) {
            for (Path path : stream) {
                String name = path.getFileName().toString();
                if (name.endsWith("-tests.jar") || name.endsWith("-sources.jar") || name.startsWith("original-")) {
                    continue;
                }
                candidates.add(path);
            }
        }
        if (candidates.size() != 1) {
            fail("Expected exactly one shaded auron-flink-assembly-*.jar in "
                    + targetDir
                    + ", found "
                    + candidates.size()
                    + ": "
                    + candidates
                    + ". A mis-ordered phase (test before package) or a stale target/ is the "
                    + "likely cause.");
        }
        return new JarFile(candidates.get(0).toFile());
    }

    private static byte[] readEntry(JarFile jar, JarEntry entry) throws IOException {
        try (InputStream in = jar.getInputStream(entry)) {
            return readAllBytes(in);
        }
    }

    private static byte[] readAllBytes(InputStream in) throws IOException {
        byte[] buffer = new byte[8192];
        int total = 0;
        int read;
        while ((read = in.read(buffer, total, buffer.length - total)) != -1) {
            total += read;
            if (total == buffer.length) {
                byte[] grown = new byte[buffer.length * 2];
                System.arraycopy(buffer, 0, grown, 0, total);
                buffer = grown;
            }
        }
        byte[] result = new byte[total];
        System.arraycopy(buffer, 0, result, 0, total);
        return result;
    }

    private static int indexOf(byte[] haystack, byte[] needle) {
        if (needle.length == 0 || haystack.length < needle.length) {
            return -1;
        }
        outer:
        for (int i = 0; i <= haystack.length - needle.length; i++) {
            for (int j = 0; j < needle.length; j++) {
                if (haystack[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }
}
