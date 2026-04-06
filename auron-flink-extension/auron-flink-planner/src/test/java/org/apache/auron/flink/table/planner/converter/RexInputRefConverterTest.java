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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link RexInputRefConverter}. */
class RexInputRefConverterTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private RexInputRefConverter converter;
    private ConverterContext context;

    @BeforeEach
    void setUp() {
        converter = new RexInputRefConverter();
        RowType inputType = RowType.of(new LogicalType[] {new IntType(), new BigIntType()}, new String[] {"f0", "f1"});
        context = new ConverterContext(new Configuration(), null, getClass().getClassLoader(), inputType);
    }

    @Test
    void testGetNodeClass() {
        assertEquals(RexInputRef.class, converter.getNodeClass());
    }

    @Test
    void testIsSupportedValidIndex() {
        RexNode inputRef = REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 0);
        assertTrue(converter.isSupported(inputRef, context));
    }

    @Test
    void testIsNotSupportedOutOfRangeIndex() {
        // Schema has 2 fields (f0, f1) — index 5 is out of range
        RexNode inputRef = REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 5);
        assertFalse(converter.isSupported(inputRef, context));
    }

    @Test
    void testConvertFirstColumn() {
        RexNode inputRef = REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 0);

        PhysicalExprNode result = converter.convert(inputRef, context);

        assertTrue(result.hasColumn());
        assertEquals("f0", result.getColumn().getName());
        assertEquals(0, result.getColumn().getIndex());
    }

    @Test
    void testConvertSecondColumn() {
        RexNode inputRef = REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), 1);

        PhysicalExprNode result = converter.convert(inputRef, context);

        assertTrue(result.hasColumn());
        assertEquals("f1", result.getColumn().getName());
        assertEquals(1, result.getColumn().getIndex());
    }
}
