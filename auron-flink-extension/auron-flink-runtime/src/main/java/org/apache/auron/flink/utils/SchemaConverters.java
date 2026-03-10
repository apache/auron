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
package org.apache.auron.flink.utils;

import static org.apache.auron.flink.connector.kafka.KafkaConstants.*;

import java.util.stream.Collectors;
import org.apache.auron.protobuf.*;
import org.apache.flink.table.types.logical.*;

/**
 * Converts Flink's {@link RowType} to Auron's {@link Schema}.
 */
public class SchemaConverters {

    /**
     * Converts a {@link RowType} to a {@link Schema}.
     */
    public static Schema convertToAuronSchema(RowType rowType, boolean withKafkaMeta) {
        Schema.Builder schemaBuilder = Schema.newBuilder();
        if (withKafkaMeta) {
            schemaBuilder.addColumns(Field.newBuilder()
                    .setName(KAFKA_AURON_META_PARTITION_ID)
                    .setNullable(false)
                    .setArrowType(ArrowType.newBuilder()
                            .setINT32(EmptyMessage.getDefaultInstance())
                            .build()));
            schemaBuilder.addColumns(Field.newBuilder()
                    .setName(KAFKA_AURON_META_OFFSET)
                    .setNullable(false)
                    .setArrowType(ArrowType.newBuilder()
                            .setINT64(EmptyMessage.getDefaultInstance())
                            .build()));
            schemaBuilder.addColumns(Field.newBuilder()
                    .setName(KAFKA_AURON_META_TIMESTAMP)
                    .setNullable(false)
                    .setArrowType(ArrowType.newBuilder()
                            .setINT64(EmptyMessage.getDefaultInstance())
                            .build()));
        }
        for (int i = 0; i < rowType.getFields().size(); i++) {
            RowType.RowField rowField = rowType.getFields().get(i);
            if (rowField.getName().equalsIgnoreCase(FLINK_SQL_PROC_TIME_KEY_WORD)) {
                // proc time is nullable
                schemaBuilder.addColumns(convertField(rowField, true));
            } else {
                schemaBuilder.addColumns(convertField(rowField, false));
            }
        }
        return schemaBuilder.build();
    }

    public static Field convertField(RowType.RowField rowField, boolean isProctime) {
        return Field.newBuilder()
                .setName(rowField.getName())
                .setNullable(isProctime ? true : rowField.getType().isNullable())
                .setArrowType(convertToAuronArrowType(rowField.getType()))
                .build();
    }

    public static ArrowType convertToAuronArrowType(LogicalType flinkLogicalType) {
        ArrowType.Builder arrowTypeBuilder = ArrowType.newBuilder();
        switch (flinkLogicalType.getTypeRoot()) {
            case NULL:
                arrowTypeBuilder.setNONE(EmptyMessage.getDefaultInstance());
                break;
            case BOOLEAN:
                arrowTypeBuilder.setBOOL(EmptyMessage.getDefaultInstance());
                break;
            case TINYINT:
                arrowTypeBuilder.setINT8(EmptyMessage.getDefaultInstance());
                break;
            case SMALLINT:
                arrowTypeBuilder.setINT16(EmptyMessage.getDefaultInstance());
                break;
            case INTEGER:
                arrowTypeBuilder.setINT32(EmptyMessage.getDefaultInstance());
                break;
            case BIGINT:
                arrowTypeBuilder.setINT64(EmptyMessage.getDefaultInstance());
                break;
            case FLOAT:
                arrowTypeBuilder.setFLOAT32(EmptyMessage.getDefaultInstance());
                break;
            case DOUBLE:
                arrowTypeBuilder.setFLOAT64(EmptyMessage.getDefaultInstance());
                break;
            case CHAR:
            case VARCHAR:
                arrowTypeBuilder.setUTF8(EmptyMessage.getDefaultInstance());
                break;
            case BINARY:
            case VARBINARY:
                arrowTypeBuilder.setBINARY(EmptyMessage.getDefaultInstance());
                break;
            case DATE:
                arrowTypeBuilder.setDATE32(EmptyMessage.getDefaultInstance());
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                // timezone is never used in native side
                arrowTypeBuilder.setTIMESTAMP(Timestamp.newBuilder().setTimeUnit(TimeUnit.Millisecond));
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // timezone is never used in native side
                arrowTypeBuilder.setTIMESTAMP(Timestamp.newBuilder().setTimeUnit(TimeUnit.Millisecond));
                break;
            case DECIMAL:
                // decimal
                DecimalType t = (DecimalType) flinkLogicalType;
                arrowTypeBuilder.setDECIMAL(Decimal.newBuilder()
                        .setWhole(Math.max(t.getPrecision(), 1))
                        .setFractional(t.getScale())
                        .build());
                break;
            case ARRAY:
                // array/list
                ArrayType a = (ArrayType) flinkLogicalType;
                arrowTypeBuilder.setLIST(List.newBuilder()
                        .setFieldType(Field.newBuilder()
                                .setName("item")
                                .setArrowType(convertToAuronArrowType(a.getElementType()))
                                .setNullable(a.isNullable()))
                        .build());
                break;
            case MAP:
                MapType m = (MapType) flinkLogicalType;
                arrowTypeBuilder.setMAP(Map.newBuilder()
                        .setKeyType(Field.newBuilder()
                                .setName("key")
                                .setArrowType(convertToAuronArrowType(m.getKeyType()))
                                .setNullable(false))
                        .setValueType(Field.newBuilder()
                                .setName("value")
                                .setArrowType(convertToAuronArrowType(m.getValueType()))
                                .setNullable(m.getValueType().isNullable()))
                        .build());
                break;
            case ROW:
                // StructType
                RowType r = (RowType) flinkLogicalType;
                arrowTypeBuilder.setSTRUCT(Struct.newBuilder()
                        .addAllSubFieldTypes(r.getFields().stream()
                                .map(e -> Field.newBuilder()
                                        .setArrowType(convertToAuronArrowType(e.getType()))
                                        .setName(e.getName())
                                        .setNullable(e.getType().isNullable())
                                        .build())
                                .collect(Collectors.toList()))
                        .build());
                break;
            default:
                throw new UnsupportedOperationException(
                        "Data type conversion not implemented " + flinkLogicalType.asSummaryString());
        }
        return arrowTypeBuilder.build();
    }
}
