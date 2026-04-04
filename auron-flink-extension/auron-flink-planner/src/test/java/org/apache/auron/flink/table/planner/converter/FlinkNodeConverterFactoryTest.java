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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link FlinkNodeConverterFactory}. */
class FlinkNodeConverterFactoryTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private FlinkNodeConverterFactory factory;
    private ConverterContext context;
    private RexLiteral testLiteral;
    private AggregateCall testAggCall;

    @BeforeEach
    void setUp() {
        factory = new FlinkNodeConverterFactory();
        context =
                new ConverterContext(new Configuration(), null, getClass().getClassLoader(), RowType.of(new IntType()));
        testLiteral = REX_BUILDER.makeExactLiteral(java.math.BigDecimal.valueOf(42));
        testAggCall = AggregateCall.create(
                SqlStdOperatorTable.COUNT,
                false,
                false,
                false,
                java.util.Collections.emptyList(),
                -1,
                null,
                org.apache.calcite.rel.RelCollations.EMPTY,
                TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
                "cnt");
    }

    @Test
    void testRexConverterDispatch() {
        PhysicalExprNode expected = PhysicalExprNode.newBuilder().build();
        factory.registerRexConverter(new StubRexNodeConverter(true, expected));

        Optional<PhysicalExprNode> result = factory.convertRexNode(testLiteral, context);
        assertTrue(result.isPresent());
        assertEquals(expected, result.get());
    }

    @Test
    void testAggConverterDispatch() {
        PhysicalExprNode expected = PhysicalExprNode.newBuilder().build();
        factory.registerAggConverter(new StubAggCallConverter(true, expected));

        Optional<PhysicalExprNode> result = factory.convertAggCall(testAggCall, context);
        assertTrue(result.isPresent());
        assertEquals(expected, result.get());
    }

    @Test
    void testUnsupportedRexPassthrough() {
        factory.registerRexConverter(new StubRexNodeConverter(false, null));

        Optional<PhysicalExprNode> result = factory.convertRexNode(testLiteral, context);
        assertFalse(result.isPresent());
    }

    @Test
    void testConversionFailureFallback() {
        factory.registerRexConverter(new StubRexNodeConverter(true, null) {
            @Override
            public PhysicalExprNode convert(RexNode node, ConverterContext context) {
                throw new RuntimeException("conversion error");
            }
        });

        Optional<PhysicalExprNode> result = factory.convertRexNode(testLiteral, context);
        assertFalse(result.isPresent());
    }

    @Test
    void testDuplicateRexConverterRejected() {
        factory.registerRexConverter(new StubRexNodeConverter(true, null));

        assertThrows(
                IllegalArgumentException.class,
                () -> factory.registerRexConverter(new StubRexNodeConverter(true, null)));
    }

    @Test
    void testGetConverterByClass() {
        StubRexNodeConverter converter = new StubRexNodeConverter(true, null);
        factory.registerRexConverter(converter);

        Optional<FlinkNodeConverter<?>> found = factory.getConverter(RexLiteral.class);
        assertTrue(found.isPresent());
        assertEquals(converter, found.get());
    }

    @Test
    void testGetConverterByClassAgg() {
        StubAggCallConverter converter = new StubAggCallConverter(true, null);
        factory.registerAggConverter(converter);

        Optional<FlinkNodeConverter<?>> found = factory.getConverter(AggregateCall.class);
        assertTrue(found.isPresent());
        assertEquals(converter, found.get());
    }

    @Test
    void testGetConverterAbsent() {
        Optional<FlinkNodeConverter<?>> found = factory.getConverter(RexLiteral.class);
        assertFalse(found.isPresent());
    }

    // ---- Test stubs ----

    /** Stub FlinkRexNodeConverter for testing. */
    private static class StubRexNodeConverter implements FlinkRexNodeConverter {
        private final boolean supported;
        private final PhysicalExprNode result;

        StubRexNodeConverter(boolean supported, PhysicalExprNode result) {
            this.supported = supported;
            this.result = result;
        }

        @Override
        public Class<? extends RexNode> getNodeClass() {
            return RexLiteral.class;
        }

        @Override
        public boolean isSupported(RexNode node, ConverterContext context) {
            return supported;
        }

        @Override
        public PhysicalExprNode convert(RexNode node, ConverterContext context) {
            return result;
        }
    }

    /** Stub FlinkAggCallConverter for testing. */
    private static class StubAggCallConverter implements FlinkAggCallConverter {
        private final boolean supported;
        private final PhysicalExprNode result;

        StubAggCallConverter(boolean supported, PhysicalExprNode result) {
            this.supported = supported;
            this.result = result;
        }

        @Override
        public Class<? extends AggregateCall> getNodeClass() {
            return AggregateCall.class;
        }

        @Override
        public boolean isSupported(AggregateCall node, ConverterContext context) {
            return supported;
        }

        @Override
        public PhysicalExprNode convert(AggregateCall node, ConverterContext context) {
            return result;
        }
    }
}
