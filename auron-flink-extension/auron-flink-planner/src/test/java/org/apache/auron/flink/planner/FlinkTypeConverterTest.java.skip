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
package org.apache.auron.flink.planner;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.auron.protobuf.ArrowType;
import org.apache.flink.table.types.logical.*;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for FlinkTypeConverter.
 * Tests conversion of Flink LogicalType to Arrow ArrowType protobuf.
 */
public class FlinkTypeConverterTest {

    @Test
    public void testBooleanTypeConversion() {
        BooleanType flinkType = new BooleanType();
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasBOOL());
        assertEquals("BOOL", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testTinyIntTypeConversion() {
        TinyIntType flinkType = new TinyIntType();
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasINT8());
        assertEquals("INT8", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testSmallIntTypeConversion() {
        SmallIntType flinkType = new SmallIntType();
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasINT16());
        assertEquals("INT16", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testIntTypeConversion() {
        IntType flinkType = new IntType();
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasINT32());
        assertEquals("INT32", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testBigIntTypeConversion() {
        BigIntType flinkType = new BigIntType();
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasINT64());
        assertEquals("INT64", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testFloatTypeConversion() {
        FloatType flinkType = new FloatType();
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasFLOAT32());
        assertEquals("FLOAT32", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testDoubleTypeConversion() {
        DoubleType flinkType = new DoubleType();
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasFLOAT64());
        assertEquals("FLOAT64", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testVarCharTypeConversion() {
        VarCharType flinkType = VarCharType.STRING_TYPE;
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasUTF8());
        assertEquals("UTF8", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testCharTypeConversion() {
        CharType flinkType = new CharType(10);
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasUTF8());
        assertEquals("UTF8", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testDateTypeConversion() {
        DateType flinkType = new DateType();
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasDATE32());
        assertEquals("DATE32", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testTimestampTypeConversion() {
        TimestampType flinkType = new TimestampType(6); // 6 precision
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasTIMESTAMP());
        assertEquals("TIMESTAMP(Microsecond)", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testLocalZonedTimestampTypeConversion() {
        LocalZonedTimestampType flinkType = new LocalZonedTimestampType(6);
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasTIMESTAMP());
        assertEquals("TIMESTAMP(Microsecond)", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testDecimalTypeConversion() {
        DecimalType flinkType = new DecimalType(10, 2);
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasDECIMAL());
        assertEquals(10, arrowType.getDECIMAL().getWhole());
        assertEquals(2, arrowType.getDECIMAL().getFractional());
        assertEquals("DECIMAL(10,2)", FlinkTypeConverter.getArrowTypeDescription(flinkType));
    }

    @Test
    public void testDecimalWithZeroPrecision() {
        // Edge case: precision = 0 should become 1
        DecimalType flinkType = new DecimalType(0, 0);
        ArrowType arrowType = FlinkTypeConverter.toArrowType(flinkType);

        assertTrue(arrowType.hasDECIMAL());
        assertEquals(1, arrowType.getDECIMAL().getWhole()); // Should be max(0, 1) = 1
    }

    @Test
    public void testIsTypeSupportedPositive() {
        // All supported types
        assertTrue(FlinkTypeConverter.isTypeSupported(new BooleanType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new TinyIntType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new SmallIntType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new IntType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new BigIntType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new FloatType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new DoubleType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(VarCharType.STRING_TYPE));
        assertTrue(FlinkTypeConverter.isTypeSupported(new CharType(10)));
        assertTrue(FlinkTypeConverter.isTypeSupported(new DateType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new TimestampType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new LocalZonedTimestampType()));
        assertTrue(FlinkTypeConverter.isTypeSupported(new DecimalType(10, 2)));
    }

    @Test
    public void testIsTypeSupportedNegative() {
        // Unsupported types (for MVP)
        assertFalse(FlinkTypeConverter.isTypeSupported(new BinaryType()));
        assertFalse(FlinkTypeConverter.isTypeSupported(new VarBinaryType()));
        assertFalse(FlinkTypeConverter.isTypeSupported(new TimeType()));
    }

    @Test
    public void testUnsupportedTypeThrowsException() {
        BinaryType unsupportedType = new BinaryType();

        assertThrows(UnsupportedOperationException.class, () -> {
            FlinkTypeConverter.toArrowType(unsupportedType);
        });
    }

    @Test
    public void testNullableTypes() {
        // Test that nullable flag is preserved (though not directly tested in conversion)
        IntType nullableInt = new IntType(true);
        IntType nonNullableInt = new IntType(false);

        ArrowType arrow1 = FlinkTypeConverter.toArrowType(nullableInt);
        ArrowType arrow2 = FlinkTypeConverter.toArrowType(nonNullableInt);

        // Both should convert to INT32 (nullability is handled at schema level)
        assertTrue(arrow1.hasINT32());
        assertTrue(arrow2.hasINT32());
    }

    @Test
    public void testAllPrimitiveTypes() {
        // Comprehensive test for all primitive types
        LogicalType[] types = {
            new BooleanType(),
            new TinyIntType(),
            new SmallIntType(),
            new IntType(),
            new BigIntType(),
            new FloatType(),
            new DoubleType()
        };

        for (LogicalType type : types) {
            ArrowType arrowType = FlinkTypeConverter.toArrowType(type);
            assertNotNull(arrowType);
            String description = FlinkTypeConverter.getArrowTypeDescription(type);
            assertNotNull(description);
            assertFalse(description.equals("UNSUPPORTED"));
        }
    }

    @Test
    public void testStringTypesVariations() {
        // Test different string type variations
        VarCharType varchar10 = new VarCharType(10);
        VarCharType varchar100 = new VarCharType(100);
        VarCharType varcharUnbounded = VarCharType.STRING_TYPE;
        CharType char10 = new CharType(10);

        // All should map to UTF8
        assertTrue(FlinkTypeConverter.toArrowType(varchar10).hasUTF8());
        assertTrue(FlinkTypeConverter.toArrowType(varchar100).hasUTF8());
        assertTrue(FlinkTypeConverter.toArrowType(varcharUnbounded).hasUTF8());
        assertTrue(FlinkTypeConverter.toArrowType(char10).hasUTF8());
    }

    @Test
    public void testDecimalPrecisionAndScale() {
        // Test various decimal precisions and scales
        DecimalType[] decimals = {
            new DecimalType(5, 2), // Small precision
            new DecimalType(10, 5), // Medium precision
            new DecimalType(38, 10), // Large precision
            new DecimalType(10, 0), // No fractional part
            new DecimalType(10, 10) // All fractional
        };

        for (DecimalType decimal : decimals) {
            ArrowType arrowType = FlinkTypeConverter.toArrowType(decimal);
            assertTrue(arrowType.hasDECIMAL());
            assertEquals(
                    Math.max(decimal.getPrecision(), 1), arrowType.getDECIMAL().getWhole());
            assertEquals(decimal.getScale(), arrowType.getDECIMAL().getFractional());
        }
    }

    @Test
    public void testTimestampPrecisions() {
        // Test different timestamp precisions
        TimestampType ts0 = new TimestampType(0); // Second precision
        TimestampType ts3 = new TimestampType(3); // Millisecond precision
        TimestampType ts6 = new TimestampType(6); // Microsecond precision
        TimestampType ts9 = new TimestampType(9); // Nanosecond precision

        // All should map to TIMESTAMP with Microsecond unit
        assertTrue(FlinkTypeConverter.toArrowType(ts0).hasTIMESTAMP());
        assertTrue(FlinkTypeConverter.toArrowType(ts3).hasTIMESTAMP());
        assertTrue(FlinkTypeConverter.toArrowType(ts6).hasTIMESTAMP());
        assertTrue(FlinkTypeConverter.toArrowType(ts9).hasTIMESTAMP());
    }
}
