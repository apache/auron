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
package org.apache.auron.util;

import javax.annotation.Nullable;

/**
 * A collection of static utility methods to validate input.
 *
 * <p>This class is modelled after Google Guava's Preconditions class, and partly takes code from
 * that class. We add this code to the Auron code base in order to reduce external dependencies.
 */
public class Preconditions {

    // ------------------------------------------------------------------------
    //  Null checks
    // ------------------------------------------------------------------------

    /**
     * Ensures that the given object reference is not null. Upon violation, a {@code
     * NullPointerException} with no message is thrown.
     *
     * @param reference The object reference
     * @return The object reference itself (generically typed).
     * @throws NullPointerException Thrown, if the passed reference was null.
     */
    public static <T> T checkNotNull(@Nullable T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }

    /**
     * Ensures that the given object reference is not null. Upon violation, a {@code
     * NullPointerException} with the given message is thrown.
     *
     * @param reference The object reference
     * @param errorMessage The message for the {@code NullPointerException} that is thrown if the
     *     check fails.
     * @return The object reference itself (generically typed).
     * @throws NullPointerException Thrown, if the passed reference was null.
     */
    public static <T> T checkNotNull(@Nullable T reference, @Nullable String errorMessage) {
        if (reference == null) {
            throw new NullPointerException(String.valueOf(errorMessage));
        }
        return reference;
    }
}
