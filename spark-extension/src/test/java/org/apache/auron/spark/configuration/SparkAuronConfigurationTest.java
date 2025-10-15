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
package org.apache.auron.spark.configuration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.spark.SparkConf;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class is used to test the {@link SparkAuronConfiguration) class.
 */
public class SparkAuronConfigurationTest {

    private SparkAuronConfiguration sparkAuronConfiguration;

    @BeforeEach
    public void setUp() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("spark.auron.ui.enabled", "false");
        sparkConf.set("spark.auron.process.vmrss.memoryFraction", "0.66");
        sparkConf.set("spark.auron.suggested.udaf.memUsedSize", "1024");
        sparkConf.set("spark.io.compression.codec", "gzip");
        sparkAuronConfiguration = new SparkAuronConfiguration(sparkConf);
    }

    @Test
    public void testGetSparkConfig() {
        assertEquals(sparkAuronConfiguration.get(SparkAuronConfiguration.UI_ENABLED), false);
        assertEquals(sparkAuronConfiguration.get(SparkAuronConfiguration.PROCESS_MEMORY_FRACTION), 0.66);
        assertEquals(sparkAuronConfiguration.get(SparkAuronConfiguration.SUGGESTED_UDAF_ROW_MEM_USAGE), 1024);
        assertEquals(sparkAuronConfiguration.get(SparkAuronConfiguration.SPARK_IO_COMPRESSION_CODEC), "gzip");

        assertEquals(
                sparkAuronConfiguration
                        .getOptional(SparkAuronConfiguration.UI_ENABLED.key())
                        .get(),
                false);
        assertEquals(
                sparkAuronConfiguration
                        .getOptional(SparkAuronConfiguration.PROCESS_MEMORY_FRACTION.key())
                        .get(),
                0.66);
        assertEquals(
                sparkAuronConfiguration
                        .getOptional(SparkAuronConfiguration.SUGGESTED_UDAF_ROW_MEM_USAGE.key())
                        .get(),
                1024);
        assertEquals(
                sparkAuronConfiguration
                        .getOptional(SparkAuronConfiguration.SPARK_IO_COMPRESSION_CODEC.key())
                        .get(),
                "gzip");

        // Test default value
        assertEquals(
                sparkAuronConfiguration
                        .getOptional(SparkAuronConfiguration.PARSE_JSON_ERROR_FALLBACK)
                        .get(),
                true);
    }
}
