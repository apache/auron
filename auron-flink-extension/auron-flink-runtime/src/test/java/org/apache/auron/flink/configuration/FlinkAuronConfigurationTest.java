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
package org.apache.auron.flink.configuration;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.auron.configuration.AuronConfiguration;
import org.apache.auron.flink.testutils.CommonTestUtils;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.junit.jupiter.api.Test;

/**
 * This class is used to test the FlinkAuronConfiguration class.
 */
public class FlinkAuronConfigurationTest {

    @Test
    public void testGetConfigFromFlinkConfig() {
        URL flinkConfigFileUrl =
                FlinkAuronConfigurationTest.class.getClassLoader().getResource(GlobalConfiguration.FLINK_CONF_FILENAME);
        Map<String, String> env = new HashMap<>(System.getenv());
        env.put(
                ConfigConstants.ENV_FLINK_CONF_DIR,
                new File(flinkConfigFileUrl.getFile()).getParentFile().getAbsolutePath());
        CommonTestUtils.setEnv(env);
        FlinkAuronConfiguration config = new FlinkAuronConfiguration();
        assertEquals(config.get(AuronConfiguration.BATCH_SIZE), 9999);
        assertEquals(config.get(AuronConfiguration.NATIVE_LOG_LEVEL), "DEBUG");
        assertEquals(config.get(AuronConfiguration.MEMORY_FRACTION), 0.6); // default value
    }
}
