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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Translates a {@code java.text.SimpleDateFormat} pattern into the native parser's
 * {@code strftime}-style format string, or reports that the pattern cannot be translated.
 *
 * <p>The native {@code Flink_UnixTimestamp} function replicates {@code SimpleDateFormat} lenient
 * parsing only for a fixed set of numeric fields. This converter is the plan-time gate: it accepts
 * a pattern only when every token maps to a field the native parser handles identically, and
 * returns {@link Optional#empty()} otherwise so the whole {@code Calc} falls back to Flink's engine.
 *
 * <p>Accepted fields and their native specifiers:
 * <ul>
 *   <li>{@code yyy} / {@code yyyy} &rarr; {@code %Y} (year)
 *   <li>{@code M} / {@code MM} &rarr; {@code %m} (month)
 *   <li>{@code d} / {@code dd} &rarr; {@code %d} (day of month)
 *   <li>{@code H} / {@code HH} &rarr; {@code %H} (hour of day)
 *   <li>{@code m} / {@code mm} &rarr; {@code %M} (minute)
 *   <li>{@code s} / {@code ss} &rarr; {@code %S} (second)
 * </ul>
 *
 * <p>Non-alphabetic characters are literals (a literal {@code %} is emitted as {@code %%}).
 * {@code SimpleDateFormat} single-quote escaping applies, including the doubled {@code ''} that
 * denotes a literal quote. Every other ASCII letter and every unlisted run length forces a fall
 * back, because Java reserves all letters and the omitted forms either depend on a locale or on the
 * clock at parse time.
 *
 * <p>Adjacency rule: run length is erased by the translation ({@code M} and {@code MM} both become
 * {@code %m}), yet the native parser reads each numeric field at a canonical width (year 4; month,
 * day, hour, minute, second 2) while Java's lenient scan window equals the run length. When two
 * numeric fields are adjacent with no literal separator, those two widths must agree, so the left
 * field's run length must equal its canonical width; otherwise the pattern falls back to avoid a
 * silent divergence (e.g. {@code yyyyMd} on {@code 20201010} yields month 1 in Java but month 10
 * natively).
 */
public final class FlinkDateTimeFormatConverter {

    private FlinkDateTimeFormatConverter() {
        // utility class
    }

    /**
     * Translates the given Java {@code SimpleDateFormat} pattern to the native {@code strftime}-style
     * format string.
     *
     * @param javaPattern the Java date-time format pattern
     * @return the translated native format string, or {@link Optional#empty()} if any part of the
     *     pattern is outside the natively supported surface
     */
    public static Optional<String> translate(String javaPattern) {
        List<Token> tokens = scan(javaPattern);
        if (tokens == null) {
            return Optional.empty();
        }
        if (!adjacencyValid(tokens)) {
            return Optional.empty();
        }
        StringBuilder out = new StringBuilder();
        for (Token token : tokens) {
            out.append(token.rendered);
        }
        return Optional.of(out.toString());
    }

    /**
     * Walks the pattern into a token list, accumulating literal runs and emitting one token per
     * pattern-letter field. Returns {@code null} on any unsupported letter, unsupported run length,
     * or unterminated quote.
     */
    private static List<Token> scan(String pattern) {
        List<Token> tokens = new ArrayList<>();
        StringBuilder literal = new StringBuilder();
        int i = 0;
        int n = pattern.length();
        while (i < n) {
            char c = pattern.charAt(i);
            if (c == '\'') {
                if (i + 1 < n && pattern.charAt(i + 1) == '\'') {
                    literal.append('\'');
                    i += 2;
                    continue;
                }
                i++;
                boolean closed = false;
                while (i < n) {
                    char q = pattern.charAt(i);
                    if (q == '\'') {
                        if (i + 1 < n && pattern.charAt(i + 1) == '\'') {
                            literal.append('\'');
                            i += 2;
                            continue;
                        }
                        closed = true;
                        i++;
                        break;
                    }
                    literal.append(q);
                    i++;
                }
                if (!closed) {
                    return null;
                }
            } else if (isAsciiLetter(c)) {
                int j = i;
                while (j < n && pattern.charAt(j) == c) {
                    j++;
                }
                Field field = fieldFor(c, j - i);
                if (field == null) {
                    return null;
                }
                flushLiteral(tokens, literal);
                tokens.add(Token.field(field, j - i));
                i = j;
            } else {
                literal.append(c);
                i++;
            }
        }
        flushLiteral(tokens, literal);
        return tokens;
    }

    private static void flushLiteral(List<Token> tokens, StringBuilder literal) {
        if (literal.length() > 0) {
            tokens.add(Token.literal(literal.toString()));
            literal.setLength(0);
        }
    }

    /**
     * Checks the adjacency rule over the token list: for every field immediately followed by another
     * field (no intervening literal), the left field's run length must equal its canonical width.
     */
    private static boolean adjacencyValid(List<Token> tokens) {
        for (int i = 0; i + 1 < tokens.size(); i++) {
            Token left = tokens.get(i);
            Token right = tokens.get(i + 1);
            if (left.field != null && right.field != null && left.runLength != left.field.canonicalWidth) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAsciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static Field fieldFor(char letter, int runLength) {
        switch (letter) {
            case 'y':
                return runLength == 3 || runLength == 4 ? Field.YEAR : null;
            case 'M':
                return runLength == 1 || runLength == 2 ? Field.MONTH : null;
            case 'd':
                return runLength == 1 || runLength == 2 ? Field.DAY : null;
            case 'H':
                return runLength == 1 || runLength == 2 ? Field.HOUR : null;
            case 'm':
                return runLength == 1 || runLength == 2 ? Field.MINUTE : null;
            case 's':
                return runLength == 1 || runLength == 2 ? Field.SECOND : null;
            default:
                return null;
        }
    }

    /** A supported numeric field: its native specifier and the width the native parser reads. */
    private enum Field {
        YEAR("%Y", 4),
        MONTH("%m", 2),
        DAY("%d", 2),
        HOUR("%H", 2),
        MINUTE("%M", 2),
        SECOND("%S", 2);

        private final String specifier;
        private final int canonicalWidth;

        Field(String specifier, int canonicalWidth) {
            this.specifier = specifier;
            this.canonicalWidth = canonicalWidth;
        }
    }

    /** A scanned token: either a field (non-null {@link #field}) or a literal run. */
    private static final class Token {
        private final Field field;
        private final int runLength;
        private final String rendered;

        private Token(Field field, int runLength, String rendered) {
            this.field = field;
            this.runLength = runLength;
            this.rendered = rendered;
        }

        static Token field(Field field, int runLength) {
            return new Token(field, runLength, field.specifier);
        }

        static Token literal(String raw) {
            return new Token(null, 0, raw.replace("%", "%%"));
        }
    }
}
