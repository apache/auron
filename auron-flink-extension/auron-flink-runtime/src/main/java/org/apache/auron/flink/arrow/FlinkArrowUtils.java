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

import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
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
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Utility class for converting Flink {@link LogicalType} instances to Arrow types, fields and schemas.
 */
public final class FlinkArrowUtils {

    /**
     * Root allocator for Arrow memory management.
     */
    public static final RootAllocator ROOT_ALLOCATOR = new RootAllocator(Long.MAX_VALUE);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(ROOT_ALLOCATOR::close));
    }

    /**
     * Creates a child allocator from the root allocator.
     *
     * @param name Name for the child allocator
     * @return A new child allocator
     */
    public static BufferAllocator createChildAllocator(String name) {
        return ROOT_ALLOCATOR.newChildAllocator(name, 0, Long.MAX_VALUE);
    }

    /**
     * Converts a Flink LogicalType to Arrow ArrowType.
     *
     * @param logicalType The Flink logical type
     * @return The corresponding Arrow type
     * @throws UnsupportedOperationException if the type is not supported
     */
    public static ArrowType toArrowType(LogicalType logicalType) {
        if (logicalType == null) {
            throw new IllegalArgumentException("logicalType cannot be null");
        }
        if (logicalType instanceof NullType) {
            return ArrowType.Null.INSTANCE;
        } else if (logicalType instanceof BooleanType) {
            return ArrowType.Bool.INSTANCE;
        } else if (logicalType instanceof TinyIntType) {
            return new ArrowType.Int(8, true);
        } else if (logicalType instanceof SmallIntType) {
            return new ArrowType.Int(16, true);
        } else if (logicalType instanceof IntType) {
            return new ArrowType.Int(32, true);
        } else if (logicalType instanceof BigIntType) {
            return new ArrowType.Int(64, true);
        } else if (logicalType instanceof FloatType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (logicalType instanceof DoubleType) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (logicalType instanceof VarCharType || logicalType instanceof CharType) {
            return ArrowType.Utf8.INSTANCE;
        } else if (logicalType instanceof VarBinaryType || logicalType instanceof BinaryType) {
            return ArrowType.Binary.INSTANCE;
        } else if (logicalType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) logicalType;
            // Note: Arrow Java only has DecimalVector (128-bit) and Decimal256Vector (256-bit).
            // There's no Decimal64Vector, so we always use 128-bit to match the actual storage.
            // Setting bitWidth=64 would cause FFI export issues since the actual data is 128-bit.
            return new ArrowType.Decimal(decimalType.getPrecision(), decimalType.getScale(), 128);
        } else if (logicalType instanceof DateType) {
            return new ArrowType.Date(DateUnit.DAY);
        } else if (logicalType instanceof TimeType) {
            TimeType timeType = (TimeType) logicalType;
            int precision = timeType.getPrecision();
            if (precision == 0) {
                return new ArrowType.Time(TimeUnit.SECOND, 32);
            } else if (precision >= 1 && precision <= 3) {
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            } else if (precision >= 4 && precision <= 6) {
                return new ArrowType.Time(TimeUnit.MICROSECOND, 64);
            } else {
                return new ArrowType.Time(TimeUnit.NANOSECOND, 64);
            }
        } else if (logicalType instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) logicalType;
            int precision = timestampType.getPrecision();
            if (precision == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (precision >= 1 && precision <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (precision >= 4 && precision <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        } else if (logicalType instanceof LocalZonedTimestampType) {
            LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) logicalType;
            int precision = localZonedTimestampType.getPrecision();
            if (precision == 0) {
                return new ArrowType.Timestamp(TimeUnit.SECOND, null);
            } else if (precision >= 1 && precision <= 3) {
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            } else if (precision >= 4 && precision <= 6) {
                return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
            } else {
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
            }
        } else {
            throw new UnsupportedOperationException("Unsupported Flink type: " + logicalType.asSummaryString());
        }
    }

    /**
     * Converts a Flink LogicalType to an Arrow Field.
     *
     * @param name        The field name
     * @param logicalType The Flink logical type
     * @return The corresponding Arrow Field
     */
    public static Field toArrowField(String name, LogicalType logicalType) {
        boolean nullable = logicalType.isNullable();
        if (logicalType instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) logicalType;
            LogicalType elementType = arrayType.getElementType();
            FieldType fieldType = new FieldType(nullable, ArrowType.List.INSTANCE, null);
            Field elementField = toArrowField("element", elementType);
            List<Field> children = new ArrayList<>();
            children.add(elementField);
            return new Field(name, fieldType, children);
        } else if (logicalType instanceof RowType) {
            RowType rowType = (RowType) logicalType;
            FieldType fieldType = new FieldType(nullable, ArrowType.Struct.INSTANCE, null);
            List<Field> children = new ArrayList<>();
            for (RowType.RowField field : rowType.getFields()) {
                children.add(toArrowField(field.getName(), field.getType()));
            }
            return new Field(name, fieldType, children);
        } else if (logicalType instanceof MapType) {
            MapType mapType = (MapType) logicalType;
            LogicalType keyType = mapType.getKeyType();
            LogicalType valueType = mapType.getValueType();

            // Create entries field (struct<key, value>)
            FieldType entriesFieldType = new FieldType(false, ArrowType.Struct.INSTANCE, null);
            List<Field> entriesChildren = new ArrayList<>();
            entriesChildren.add(toArrowField(MapVector.KEY_NAME, keyType.copy(false)));
            entriesChildren.add(toArrowField(MapVector.VALUE_NAME, valueType));
            Field entriesField = new Field(MapVector.DATA_VECTOR_NAME, entriesFieldType, entriesChildren);

            // Create map field
            FieldType mapFieldType = new FieldType(nullable, new ArrowType.Map(false), null);
            List<Field> mapChildren = new ArrayList<>();
            mapChildren.add(entriesField);
            return new Field(name, mapFieldType, mapChildren);
        } else {
            ArrowType arrowType = toArrowType(logicalType);
            FieldType fieldType = new FieldType(nullable, arrowType, null);
            return new Field(name, fieldType, new ArrayList<>());
        }
    }

    /**
     * Converts a Flink RowType to an Arrow Schema.
     *
     * @param rowType The Flink row type
     * @return The corresponding Arrow Schema
     */
    public static Schema toArrowSchema(RowType rowType) {
        List<Field> fields = new ArrayList<>();
        for (RowType.RowField field : rowType.getFields()) {
            fields.add(toArrowField(field.getName(), field.getType()));
        }
        return new Schema(fields);
    }

    private FlinkArrowUtils() {
        // Utility class
    }
}
