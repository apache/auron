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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** Tests for {@link FlinkDateTimeFormatConverter}. */
class FlinkDateTimeFormatConverterTest {

    /** The six supported letters translate at every accepted run length. */
    @Test
    void testAcceptsSupportedPatterns() {
        assertEquals(Optional.of("%Y-%m-%d %H:%M:%S"), translate("yyyy-MM-dd HH:mm:ss"));
        assertEquals(Optional.of("%Y-%m-%d %H:%M:%S"), translate("yyyy-M-d H:m:s"));
        assertEquals(Optional.of("%d-%m-%Y %H:%M:%S"), translate("dd-MM-yyyy HH:mm:ss"));
        assertEquals(Optional.of("%Y/%m/%d"), translate("yyyy/MM/dd"));
        assertEquals(Optional.of("%Y"), translate("yyy"));
        assertEquals(Optional.of("%Y"), translate("yyyy"));
    }

    /** Any letter outside the six-letter allowlist forces a fall back — Java reserves all letters,
     * so a denylist would silently mis-treat an unknown letter as a literal. */
    @Test
    void testRejectsUnsupportedLetters() {
        assertFalse(translate("yyyy-MM-dd HH:mm:ss.SSS").isPresent());
        assertFalse(translate("yyyy-MMM-dd").isPresent());
        assertFalse(translate("yyyy-MM-dd E").isPresent());
        assertFalse(translate("yyyy-MM-dd a").isPresent());
        assertFalse(translate("yyyy-MM-dd z").isPresent());
        assertFalse(translate("yyyy-MM-dd Z").isPresent());
        assertFalse(translate("yyyy-MM-dd X").isPresent());
    }

    /** Run lengths outside each letter's accepted set fall back: the 2-digit year pivot is
     * time-dependent, and over-long runs widen the lenient scan window past the native read. */
    @Test
    void testRejectsUnsupportedRunLengths() {
        assertFalse(translate("yy-MM-dd").isPresent());
        assertFalse(translate("yyyyy-MM-dd").isPresent());
        assertFalse(translate("yyyy-MM-dd HH:mm:sss").isPresent());
    }

    /** Single-quote escaping mirrors {@code SimpleDateFormat}, including the doubled {@code ''}
     * literal quote and a literal {@code %} that must be doubled for the native format string. */
    @Test
    void testQuoteEscaping() {
        assertEquals(Optional.of("%Y-%m-%dT%H:%M:%S"), translate("yyyy-MM-dd'T'HH:mm:ss"));
        assertEquals(Optional.of("%Y'%m"), translate("yyyy''MM"));
        assertEquals(Optional.of("%Y%%%m"), translate("yyyy'%'MM"));
        // An unterminated quote is not a valid pattern and falls back.
        assertFalse(translate("yyyy'T").isPresent());
    }

    /** When two numeric fields are adjacent with no literal separator, the left field's run length
     * must equal its native canonical width; otherwise Java's lenient window and the native
     * canonical-width read diverge silently, so the pattern falls back. */
    @Test
    void testAdjacencyRule() {
        // Left field M has run length 1 but native reads canonical width 2 → fall back.
        assertFalse(translate("yyyyMd").isPresent());
        // yyyyy is already rejected by run length, but is also an adjacency hazard (year 12020).
        assertFalse(translate("yyyyyMMdd").isPresent());
        // Every left field already equals its canonical width → accepted.
        assertEquals(Optional.of("%Y%m%d%H%M%S"), translate("yyyyMMddHHmmss"));
        // Literal separators remove the adjacency between fields → accepted at run length 1.
        assertEquals(Optional.of("%Y-%m-%d"), translate("yyyy-M-d"));
    }

    private static Optional<String> translate(String javaPattern) {
        return FlinkDateTimeFormatConverter.translate(javaPattern);
    }
}
