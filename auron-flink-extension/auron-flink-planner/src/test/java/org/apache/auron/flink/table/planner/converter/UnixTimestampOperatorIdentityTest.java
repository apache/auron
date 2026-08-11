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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.auron.flink.table.AuronFlinkTableTestBase;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.junit.jupiter.api.Test;

/**
 * Pins the operator-identity invariant the {@link RexCallConverter} dispatch relies on: Flink must
 * resolve a SQL {@code UNIX_TIMESTAMP(...)} call to the singleton
 * {@code FlinkSqlOperatorTable.UNIX_TIMESTAMP}. If a future Flink release routes it to a different
 * operator (e.g. a bridging function), the reference-equality dispatch would silently miss and the
 * function would permanently fall back with no error — this test turns that into a red build.
 */
class UnixTimestampOperatorIdentityTest extends AuronFlinkTableTestBase {

    @Test
    void testSqlUnixTimestampResolvesToFlinkOperator() {
        assertResolvesToUnixTimestampOperator("SELECT UNIX_TIMESTAMP(`ts`) FROM T1");
        assertResolvesToUnixTimestampOperator("SELECT UNIX_TIMESTAMP(`ts`, 'yyyy-MM-dd HH:mm:ss') FROM T1");
    }

    private void assertResolvesToUnixTimestampOperator(String sql) {
        Table table = tableEnvironment.sqlQuery(sql);
        RelNode tree = ((PlannerQueryOperation) ((TableImpl) table).getQueryOperation()).getCalciteTree();

        SqlOperator operator = findUnixTimestampOperator(tree);
        assertNotNull(operator, "SQL " + sql + " did not produce a UNIX_TIMESTAMP call");
        assertSame(FlinkSqlOperatorTable.UNIX_TIMESTAMP, operator);
    }

    private static SqlOperator findUnixTimestampOperator(RelNode rel) {
        SqlOperator[] holder = new SqlOperator[1];
        RexShuttle shuttle = new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if ("UNIX_TIMESTAMP".equals(call.getOperator().getName())) {
                    holder[0] = call.getOperator();
                }
                return super.visitCall(call);
            }
        };
        walk(rel, shuttle);
        return holder[0];
    }

    private static void walk(RelNode rel, RexShuttle shuttle) {
        rel.accept(shuttle);
        for (RelNode input : rel.getInputs()) {
            walk(input, shuttle);
        }
    }
}
