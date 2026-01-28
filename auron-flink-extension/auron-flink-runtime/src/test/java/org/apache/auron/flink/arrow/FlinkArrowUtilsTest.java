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
package org.apache.auron.flink.arrow;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

/** Unit tests for FlinkArrowUtils. */
public class FlinkArrowUtilsTest {

    @Test
    public void testBasicTypeConversion() {
        // Null
        assertEquals(ArrowType.Null.INSTANCE, FlinkArrowUtils.toArrowType(new NullType()));

        // Boolean
        assertEquals(ArrowType.Bool.INSTANCE, FlinkArrowUtils.toArrowType(new BooleanType()));

        // Integer types
        assertEquals(new ArrowType.Int(8, true), FlinkArrowUtils.toArrowType(new TinyIntType()));
        assertEquals(new ArrowType.Int(16, true), FlinkArrowUtils.toArrowType(new SmallIntType()));
        assertEquals(new ArrowType.Int(32, true), FlinkArrowUtils.toArrowType(new IntType()));
        assertEquals(new ArrowType.Int(64, true), FlinkArrowUtils.toArrowType(new BigIntType()));

        // Floating point types
        assertEquals(
                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE),
                FlinkArrowUtils.toArrowType(new FloatType()));
        assertEquals(
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE),
                FlinkArrowUtils.toArrowType(new DoubleType()));

        // String and binary types
        assertEquals(ArrowType.Utf8.INSTANCE, FlinkArrowUtils.toArrowType(new VarCharType(100)));
        assertEquals(ArrowType.Utf8.INSTANCE, FlinkArrowUtils.toArrowType(new CharType(10)));
        assertEquals(ArrowType.Binary.INSTANCE, FlinkArrowUtils.toArrowType(new VarBinaryType(100)));
        assertEquals(ArrowType.Binary.INSTANCE, FlinkArrowUtils.toArrowType(new BinaryType(10)));

        // Decimal type
        DecimalType decimalType = new DecimalType(10, 2);
        ArrowType arrowDecimal = FlinkArrowUtils.toArrowType(decimalType);
        assertTrue(arrowDecimal instanceof ArrowType.Decimal);
        assertEquals(10, ((ArrowType.Decimal) arrowDecimal).getPrecision());
        assertEquals(2, ((ArrowType.Decimal) arrowDecimal).getScale());
        assertEquals(128, ((ArrowType.Decimal) arrowDecimal).getBitWidth());

        // Date and timestamp types
        assertEquals(new ArrowType.Date(DateUnit.DAY), FlinkArrowUtils.toArrowType(new DateType()));
        assertEquals(
                new ArrowType.Timestamp(TimeUnit.MICROSECOND, null), FlinkArrowUtils.toArrowType(new TimestampType(3)));
    }

    @Test
    public void testArrayTypeConversion() {
        ArrayType arrayType = new ArrayType(new IntType());
        Field field = FlinkArrowUtils.toArrowField("test_array", arrayType, true);

        assertEquals("test_array", field.getName());
        assertTrue(field.isNullable());
        assertTrue(field.getType() instanceof ArrowType.List);
        assertEquals(1, field.getChildren().size());

        Field elementField = field.getChildren().get(0);
        assertEquals("element", elementField.getName());
        assertTrue(elementField.getType() instanceof ArrowType.Int);
    }

    @Test
    public void testRowTypeConversion() {
        RowType rowType =
                RowType.of(new LogicalType[] {new IntType(), new VarCharType(100)}, new String[] {"id", "name"});

        Field field = FlinkArrowUtils.toArrowField("test_row", rowType, false);

        assertEquals("test_row", field.getName());
        assertFalse(field.isNullable());
        assertTrue(field.getType() instanceof ArrowType.Struct);
        assertEquals(2, field.getChildren().size());

        Field idField = field.getChildren().get(0);
        assertEquals("id", idField.getName());
        assertTrue(idField.getType() instanceof ArrowType.Int);

        Field nameField = field.getChildren().get(1);
        assertEquals("name", nameField.getName());
        assertEquals(ArrowType.Utf8.INSTANCE, nameField.getType());
    }

    @Test
    public void testMapTypeConversion() {
        MapType mapType = new MapType(new VarCharType(100), new IntType());
        Field field = FlinkArrowUtils.toArrowField("test_map", mapType, true);

        assertEquals("test_map", field.getName());
        assertTrue(field.isNullable());
        assertTrue(field.getType() instanceof ArrowType.Map);
        assertEquals(1, field.getChildren().size());

        Field entriesField = field.getChildren().get(0);
        assertEquals("entries", entriesField.getName());
        assertTrue(entriesField.getType() instanceof ArrowType.Struct);
        assertEquals(2, entriesField.getChildren().size());

        Field keyField = entriesField.getChildren().get(0);
        assertEquals("key", keyField.getName());
        assertEquals(ArrowType.Utf8.INSTANCE, keyField.getType());

        Field valueField = entriesField.getChildren().get(1);
        assertEquals("value", valueField.getName());
        assertTrue(valueField.getType() instanceof ArrowType.Int);
    }

    @Test
    public void testSchemaConversion() {
        RowType rowType = RowType.of(
                new LogicalType[] {new IntType(), new VarCharType(100), new DoubleType()},
                new String[] {"id", "name", "score"});

        Schema schema = FlinkArrowUtils.toArrowSchema(rowType);

        assertEquals(3, schema.getFields().size());

        Field idField = schema.getFields().get(0);
        assertEquals("id", idField.getName());
        assertTrue(idField.getType() instanceof ArrowType.Int);

        Field nameField = schema.getFields().get(1);
        assertEquals("name", nameField.getName());
        assertEquals(ArrowType.Utf8.INSTANCE, nameField.getType());

        Field scoreField = schema.getFields().get(2);
        assertEquals("score", scoreField.getName());
        assertTrue(scoreField.getType() instanceof ArrowType.FloatingPoint);
    }

    @Test
    public void testTimeTypeConversion() {
        TimeType timeType = new TimeType(3);
        ArrowType arrowType = FlinkArrowUtils.toArrowType(timeType);
        assertTrue(arrowType instanceof ArrowType.Time);
        ArrowType.Time timeArrowType = (ArrowType.Time) arrowType;
        assertEquals(TimeUnit.MICROSECOND, timeArrowType.getUnit());
        assertEquals(64, timeArrowType.getBitWidth());
    }

    @Test
    public void testLocalZonedTimestampTypeConversion() {
        LocalZonedTimestampType lzType = new LocalZonedTimestampType(6);
        ArrowType arrowType = FlinkArrowUtils.toArrowType(lzType);
        assertTrue(arrowType instanceof ArrowType.Timestamp);
        ArrowType.Timestamp tsType = (ArrowType.Timestamp) arrowType;
        assertEquals(TimeUnit.MICROSECOND, tsType.getUnit());
        assertEquals("UTC", tsType.getTimezone());
    }

    @Test
    public void testUnsupportedTypeThrowsException() {
        // RawType is not supported
        assertThrows(
                UnsupportedOperationException.class,
                () -> FlinkArrowUtils.toArrowType(new RawType<>(String.class, StringSerializer.INSTANCE)));
    }
}
