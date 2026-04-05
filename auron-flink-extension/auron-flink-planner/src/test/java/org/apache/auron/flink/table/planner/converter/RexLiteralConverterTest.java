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

import java.math.BigDecimal;
import org.apache.auron.protobuf.PhysicalExprNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link RexLiteralConverter}. */
class RexLiteralConverterTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private RexLiteralConverter converter;
    private ConverterContext context;

    @BeforeEach
    void setUp() {
        converter = new RexLiteralConverter();
        context =
                new ConverterContext(new Configuration(), null, getClass().getClassLoader(), RowType.of(new IntType()));
    }

    @Test
    void testGetNodeClass() {
        assertEquals(RexLiteral.class, converter.getNodeClass());
    }

    @Test
    void testConvertTinyIntLiteral() {
        RexLiteral lit = (RexLiteral)
                REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(7), TYPE_FACTORY.createSqlType(SqlTypeName.TINYINT));

        assertTrue(converter.isSupported(lit, context));
        PhysicalExprNode result = converter.convert(lit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertSmallIntLiteral() {
        RexLiteral lit = (RexLiteral)
                REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(256), TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT));

        assertTrue(converter.isSupported(lit, context));
        PhysicalExprNode result = converter.convert(lit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertIntLiteral() {
        RexLiteral intLit = (RexLiteral)
                REX_BUILDER.makeExactLiteral(BigDecimal.valueOf(42), TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

        assertTrue(converter.isSupported(intLit, context));
        PhysicalExprNode result = converter.convert(intLit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertLongLiteral() {
        RexLiteral longLit = (RexLiteral) REX_BUILDER.makeExactLiteral(
                BigDecimal.valueOf(123456789L), TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT));

        assertTrue(converter.isSupported(longLit, context));
        PhysicalExprNode result = converter.convert(longLit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertFloatLiteral() {
        RexLiteral lit = (RexLiteral)
                REX_BUILDER.makeApproxLiteral(BigDecimal.valueOf(2.5f), TYPE_FACTORY.createSqlType(SqlTypeName.FLOAT));

        assertTrue(converter.isSupported(lit, context));
        PhysicalExprNode result = converter.convert(lit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertDoubleLiteral() {
        RexLiteral doubleLit = (RexLiteral)
                REX_BUILDER.makeApproxLiteral(BigDecimal.valueOf(3.14), TYPE_FACTORY.createSqlType(SqlTypeName.DOUBLE));

        assertTrue(converter.isSupported(doubleLit, context));
        PhysicalExprNode result = converter.convert(doubleLit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertBooleanTrue() {
        RexLiteral boolLit = (RexLiteral) REX_BUILDER.makeLiteral(true);

        assertTrue(converter.isSupported(boolLit, context));
        PhysicalExprNode result = converter.convert(boolLit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertStringLiteral() {
        RexLiteral strLit = (RexLiteral) REX_BUILDER.makeLiteral("hello");

        assertTrue(converter.isSupported(strLit, context));
        PhysicalExprNode result = converter.convert(strLit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertDecimalLiteral() {
        RexLiteral decLit = (RexLiteral) REX_BUILDER.makeExactLiteral(
                new BigDecimal("123.45"), TYPE_FACTORY.createSqlType(SqlTypeName.DECIMAL, 10, 2));

        assertTrue(converter.isSupported(decLit, context));
        PhysicalExprNode result = converter.convert(decLit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testConvertNullLiteral() {
        RexLiteral nullLit = (RexLiteral) REX_BUILDER.makeNullLiteral(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER));

        assertTrue(converter.isSupported(nullLit, context));
        PhysicalExprNode result = converter.convert(nullLit, context);
        assertTrue(result.hasLiteral());
        assertTrue(result.getLiteral().getIpcBytes().size() > 0);
    }

    @Test
    void testUnsupportedTypeNotSupported() {
        RexNode tsLit = REX_BUILDER.makeNullLiteral(TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP));

        assertFalse(converter.isSupported(tsLit, context));
    }
}
