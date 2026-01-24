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

import org.apache.auron.protobuf.ArrowType;
import org.apache.auron.protobuf.Decimal;
import org.apache.auron.protobuf.EmptyMessage;
import org.apache.auron.protobuf.Timestamp;
import org.apache.auron.protobuf.TimeUnit;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Converter for Flink LogicalType to Auron ArrowType protobuf.
 * Supports conversion of primitive types needed for MVP integration.
 */
public class FlinkTypeConverter {

    /**
     * Converts a Flink LogicalType to an Auron ArrowType protobuf message.
     *
     * @param flinkType The Flink LogicalType to convert.
     * @return The corresponding ArrowType protobuf.
     * @throws UnsupportedOperationException If the type is not supported.
     */
    public static ArrowType toArrowType(LogicalType flinkType) {
        ArrowType.Builder builder = ArrowType.newBuilder();

        if (flinkType instanceof BooleanType) {
            return builder.setBOOL(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof TinyIntType) {
            return builder.setINT8(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof SmallIntType) {
            return builder.setINT16(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof IntType) {
            return builder.setINT32(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof BigIntType) {
            return builder.setINT64(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof FloatType) {
            return builder.setFLOAT32(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof DoubleType) {
            return builder.setFLOAT64(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof VarCharType || flinkType instanceof CharType) {
            return builder.setUTF8(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof DateType) {
            return builder.setDATE32(EmptyMessage.getDefaultInstance()).build();
        } else if (flinkType instanceof TimestampType) {
            TimestampType timestampType = (TimestampType) flinkType;
            // Flink timestamps can have different precisions, but Auron uses microseconds
            Timestamp timestamp = Timestamp.newBuilder()
                    .setTimeUnit(TimeUnit.Microsecond)
                    .build();
            return builder.setTIMESTAMP(timestamp).build();
        } else if (flinkType instanceof LocalZonedTimestampType) {
            // LocalZonedTimestamp is similar to Timestamp
            Timestamp timestamp = Timestamp.newBuilder()
                    .setTimeUnit(TimeUnit.Microsecond)
                    .build();
            return builder.setTIMESTAMP(timestamp).build();
        } else if (flinkType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) flinkType;
            // Auron supports Decimal128 with specific precision and scale
            Decimal decimal = Decimal.newBuilder()
                    .setWhole(Math.max(decimalType.getPrecision(), 1))
                    .setFractional(decimalType.getScale())
                    .build();
            return builder.setDECIMAL(decimal).build();
        } else {
            throw new UnsupportedOperationException(
                    "Type conversion not supported for: " + flinkType.asSummaryString());
        }
    }

    /**
     * Checks if a Flink LogicalType is supported by Auron.
     *
     * @param flinkType The Flink LogicalType to check.
     * @return true if the type is supported, false otherwise.
     */
    public static boolean isTypeSupported(LogicalType flinkType) {
        return flinkType instanceof BooleanType
                || flinkType instanceof TinyIntType
                || flinkType instanceof SmallIntType
                || flinkType instanceof IntType
                || flinkType instanceof BigIntType
                || flinkType instanceof FloatType
                || flinkType instanceof DoubleType
                || flinkType instanceof VarCharType
                || flinkType instanceof CharType
                || flinkType instanceof DateType
                || flinkType instanceof TimestampType
                || flinkType instanceof LocalZonedTimestampType
                || flinkType instanceof DecimalType;
    }

    /**
     * Gets a human-readable description of the converted Arrow type.
     * Useful for debugging and error messages.
     *
     * @param flinkType The Flink LogicalType.
     * @return A string description of the Arrow type.
     */
    public static String getArrowTypeDescription(LogicalType flinkType) {
        if (flinkType instanceof BooleanType) {
            return "BOOL";
        } else if (flinkType instanceof TinyIntType) {
            return "INT8";
        } else if (flinkType instanceof SmallIntType) {
            return "INT16";
        } else if (flinkType instanceof IntType) {
            return "INT32";
        } else if (flinkType instanceof BigIntType) {
            return "INT64";
        } else if (flinkType instanceof FloatType) {
            return "FLOAT32";
        } else if (flinkType instanceof DoubleType) {
            return "FLOAT64";
        } else if (flinkType instanceof VarCharType || flinkType instanceof CharType) {
            return "UTF8";
        } else if (flinkType instanceof DateType) {
            return "DATE32";
        } else if (flinkType instanceof TimestampType || flinkType instanceof LocalZonedTimestampType) {
            return "TIMESTAMP(Microsecond)";
        } else if (flinkType instanceof DecimalType) {
            DecimalType dt = (DecimalType) flinkType;
            return "DECIMAL(" + dt.getPrecision() + "," + dt.getScale() + ")";
        } else {
            return "UNSUPPORTED";
        }
    }
}
