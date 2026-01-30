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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.auron.configuration.ConfigOption;

public class SparkAuronConfigurationDocGenerator {

    // Generate documentation for SparkAuronConfiguration
    public static void main(String[] args) {
        // Categories array based on SparkAuronConfiguration categories
        String[] categories = {
            "Runtime Configuration",
            "Operator Supports",
            "Data Sources",
            "Expression/Function Supports",
            "UDAF Fallback",
            "Partial Aggregate Skipping"
        };
        Class<SparkAuronConfiguration> auronConfigurationClass = SparkAuronConfiguration.class;
        List<ConfigOption<?>> configOptions = new ArrayList<>();

        for (Field field : auronConfigurationClass.getFields()) {
            try {
                configOptions.add((ConfigOption<?>) field.get(null));
            } catch (IllegalAccessException | ClassCastException e) {
                // this is not a config option
            }
        }
        configOptions.sort(Comparator.comparing(option -> option.category() + option.key()));

        for (String category : categories) {
            System.out.println();
            System.out.println("### " + category);
            System.out.println("| Conf Key | Type | Default Value | Description |");
            System.out.println("| -------- | ---- | ------------- | ----------- |");
            for (ConfigOption<?> configOption : configOptions) {
                if (!configOption.category().equals(category)) {
                    continue;
                }
                String sparkConfKey =
                        configOption.key().startsWith("spark.") ? configOption.key() : "spark." + configOption.key();
                for (String altKey : configOption.altKeys()) {
                    String sparkConfAltKey = altKey.startsWith("spark.") ? altKey : "spark." + altKey;
                    sparkConfKey += "<br/>&nbsp;_alternative: " + sparkConfAltKey + "_";
                }
                String sparkConfDesc = configOption.description();
                Class<?> sparkConfValueClass = configOption.getValueClass();
                Object sparkConfDefaultValue = configOption.defaultValue() == null ? "-" : configOption.defaultValue();
                System.out.println("| " + sparkConfKey + " | " + sparkConfValueClass.getSimpleName() + " | "
                        + sparkConfDefaultValue + " | " + sparkConfDesc + " |");
            }
        }
    }
}
