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
package org.apache.auron.conf;

/**
 * Auron boxed configuration.
 * It is used to get the config from the real engine configuration, such as spark, flink etc.
 */
public interface AuronBoxedConfiguration {

    /**
     * Retrieves a string value associated with a given key, or returns a default value if the key is not found.
     * @param key the key whose associated value is to be returned
     * @param defaultValue the value to return if the key is not found
     * @return the value associated with the key, or the default value if the key is not found
     */
    String getString(String key, String defaultValue);

    /**
     * Retrieves an integer value associated with a given key, or returns a default value if the key is not found.
     * @param key the key whose associated value is to be returned
     * @param defaultValue the value to return if the key is not found
     * @return the value associated with the key, or the default value if the key is not found
     */
    int getInt(String key, int defaultValue);

    /**
     * Retrieves a boolean value associated with a given key, or returns a default value if the key is not found.
     * @param key the key whose associated value is to be returned
     * @param defaultValue the value to return if the key is not found
     * @return the value associated with the key, or the default value if the key is not found
     */
    boolean getBoolean(String key, boolean defaultValue);

    /**
     * Retrieves a long value associated with a given key, or returns a default value if the key is not found.
     * @param key the key whose associated value is to be returned
     * @param defaultValue the value to return if the key is not found
     * @return the value associated with the key, or the default value if the key is not found
     */
    long getLong(String key, long defaultValue);

    /**
     * Retrieves a double value associated with a given key, or returns a default value if the key is not found.
     * @param key the key whose associated value is to be returned
     * @param defaultValue the value to return if the key is not found
     * @return the value associated with the key, or the default value if the key is not found
     */
    double getDouble(String key, double defaultValue);
}
