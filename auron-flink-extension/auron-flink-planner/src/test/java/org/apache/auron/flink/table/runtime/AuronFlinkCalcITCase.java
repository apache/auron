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
package org.apache.auron.flink.table.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Test;

/**
 * IT case for Flink Calc Operator on Auron.
 */
public class AuronFlinkCalcITCase extends AuronFlinkTableTestBase {

    @Test
    public void testPlus() {
        List<Row> rows = CollectionUtil.iteratorToList(
                tableEnvironment.executeSql("select `int` + 1 from T1").collect());
        rows.sort(Comparator.comparingInt(o -> (int) o.getField(0)));
        assertThat(rows).isEqualTo(Arrays.asList(Row.of(2), Row.of(3), Row.of(3)));
    }
}
