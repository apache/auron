// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    any::Any,
    cell::UnsafeCell,
    collections::{HashMap, HashSet},
    io::Cursor,
    sync::Arc,
};

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryArray, BinaryBuilder, BooleanBuilder, Float32Builder,
    Float64Builder, Int32Array, Int32Builder, Int64Array, Int64Builder, RecordBatch,
    RecordBatchOptions, StringBuilder, StructArray, TimestampMillisecondBuilder, UInt32Builder,
    UInt64Builder, new_null_array,
};
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use bytes::Buf;
use datafusion::error::DataFusionError;
use datafusion_ext_commons::{df_execution_err, downcast_any};
use prost::encoding::{DecodeContext, WireType};
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor, UnknownField};

use crate::flink::serde::{
    flink_deserializer::FlinkDeserializer, shared_array_builder::SharedArrayBuilder,
    shared_list_array_builder::SharedListArrayBuilder,
    shared_map_array_builder::SharedMapArrayBuilder,
    shared_struct_array_builder::SharedStructArrayBuilder,
};

type ValueHandler =
    Box<dyn Fn(&mut Cursor<&[u8]>, u32, WireType) -> datafusion::error::Result<()> + Send>;
type ValueHandlerMap = hashbrown::HashMap<u32, ValueHandler, foldhash::fast::RandomState>;

pub struct PbDeserializer {
    output_schema: SchemaRef,
    output_schema_without_meta: SchemaRef,
    pb_schema: SchemaRef,
    output_array_builders: Vec<SharedArrayBuilder>,
    ensure_size: Box<dyn FnMut(usize) + Send>,
    value_handlers: ValueHandlerMap,
    msg_mapping: Vec<Vec<usize>>,
}

impl FlinkDeserializer for PbDeserializer {
    fn parse_messages_with_kafka_meta(
        &mut self,
        messages: &BinaryArray,
        kafka_partition: &Int32Array,
        kafka_offset: &Int64Array,
        kafka_timestamp: &Int64Array,
    ) -> datafusion::common::Result<RecordBatch> {
        let mut msg_cursors = messages
            .iter()
            .map(|v| {
                let s = v.expect("message bytes must not be null");
                Cursor::new(s)
            })
            .collect::<Vec<_>>();
        for (row_idx, msg_cursor) in msg_cursors.iter_mut().enumerate() {
            while msg_cursor.has_remaining() {
                let (tag, wired_type) = prost::encoding::decode_key(msg_cursor).map_err(|e| {
                    DataFusionError::Execution(format!("Failed to parse protobuf key: {e}"))
                })?;
                if let Some(value_handler) = self.value_handlers.get_mut(&tag) {
                    value_handler(msg_cursor, tag, wired_type)?;
                }
            }
            let ensure_size = &mut self.ensure_size;
            ensure_size(row_idx + 1);
        }

        let root_struct = StructArray::from({
            RecordBatch::try_new_with_options(
                self.pb_schema.clone(),
                self.output_array_builders
                    .iter()
                    .map(|builder| builder.get_dyn_mut().finish())
                    .collect(),
                &RecordBatchOptions::new().with_row_count(Some(messages.len())),
            )?
        });
        let mut output_arrays: Vec<ArrayRef> = Vec::new();
        output_arrays.push(Arc::new(kafka_partition.clone()));
        output_arrays.push(Arc::new(kafka_offset.clone()));
        output_arrays.push(Arc::new(kafka_timestamp.clone()));
        for (field_idx, field) in self.output_schema_without_meta.fields().iter().enumerate() {
            let array_ref: ArrayRef = get_output_array(&root_struct, &self.msg_mapping[field_idx])?;
            if array_ref.null_count() == array_ref.len() {
                output_arrays.push(new_null_array(field.data_type(), array_ref.len()));
            } else {
                output_arrays.push(
                    datafusion_ext_commons::arrow::cast::cast(&array_ref, field.data_type())
                        .expect("Failed to cast array"),
                );
            }
        }
        let batch = RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            output_arrays,
            &RecordBatchOptions::new().with_row_count(Some(messages.len())),
        )?;
        Ok(batch)
    }
}

impl PbDeserializer {
    pub fn new(
        proto_desc_data: impl AsRef<[u8]>,
        message_name: &str,
        output_schema: SchemaRef,
        // Protobuf data may contain deeply nested hierarchies, supporting the extraction of
        // certain fields to the topmost layer of the Flink output. {"flink_output_col1":
        // "pb_field1.pb_sub_field2", "flink_output_col2":
        // "pb_field1.pb_sub_field3.pb_sub_sub_field1"}
        nested_msg_mapping: &HashMap<String, String>,
        skip_fields: &[String],
    ) -> datafusion::error::Result<Self> {
        let pool: DescriptorPool =
            DescriptorPool::decode(proto_desc_data.as_ref()).map_err(|e| {
                DataFusionError::Execution(format!("Failed to parse descriptor file: {e}"))
            })?;

        for message in pool.all_messages() {
            if message.name() == message_name {
                return Self::try_new(message, output_schema, nested_msg_mapping, skip_fields);
            }
        }
        Err(DataFusionError::Execution(format!(
            "Message '{message_name}' not found"
        )))
    }

    pub fn try_new(
        message_descriptor: MessageDescriptor,
        output_schema: SchemaRef,
        nested_msg_mapping: &HashMap<String, String>,
        skip_fields: &[String],
    ) -> datafusion::error::Result<Self> {
        // The output schema includes Kafka's meta fields, but these are absent in the
        // PB data, so they must be filtered out.
        let output_schema_without_meta = Arc::new(Schema::new(
            output_schema
                .fields()
                .iter()
                .filter(|f| {
                    f.name() != "serialized_kafka_records_partition"
                        && f.name() != "serialized_kafka_records_offset"
                        && f.name() != "serialized_kafka_records_timestamp"
                })
                .cloned()
                .collect::<Fields>(),
        ));
        // Schema inferred from the PB descriptor.
        let pb_schema = transfer_output_schema_to_pb_schema(
            message_descriptor.clone(),
            &output_schema_without_meta,
            nested_msg_mapping.clone(),
            &skip_fields,
        )
        .expect("Failed to transfer output scheam to pb scheam");

        let tag_to_output_mapping =
            create_tag_to_output_mapping(message_descriptor.clone(), &pb_schema);

        let output_array_builders =
            create_output_array_builders(&pb_schema, message_descriptor.clone())?;
        let ensure_size = ensure_output_array_builders_size(&output_array_builders)?;

        let value_handlers = message_descriptor
            .fields()
            .map(|field| {
                Ok((
                    field.number(),
                    create_value_handler(
                        &message_descriptor,
                        field.number(),
                        &tag_to_output_mapping,
                        &pb_schema,
                        &output_array_builders,
                    )?,
                ))
            })
            .collect::<datafusion::error::Result<hashbrown::HashMap<_, _, foldhash::fast::RandomState>>>()?;

        // precompute message mappings
        let msg_mapping = output_schema_without_meta
            .fields()
            .iter()
            .map(|field| {
                let mut mapped_field_indices = vec![];
                let mut cur_fields = pb_schema.fields();
                if let Some(nested) = nested_msg_mapping.get(field.name()) {
                    let nested_fields = nested.split(".").collect::<Vec<_>>();
                    for nested_field in &nested_fields[..nested_fields.len() - 1] {
                        match cur_fields.find(nested_field) {
                            Some((idx, f)) => {
                                if let DataType::Struct(fields) = f.data_type() {
                                    mapped_field_indices.push(idx);
                                    cur_fields = fields;
                                } else {
                                    return df_execution_err!("nested field must be struct");
                                }
                            }
                            _ => return df_execution_err!("nested field not found in pb schema"),
                        };
                    }
                    if let Some((idx, _)) = cur_fields.find(nested_fields[nested_fields.len() - 1])
                    {
                        mapped_field_indices.push(idx);
                    } else {
                        return df_execution_err!("field not found in pb schema");
                    }
                } else if let Ok(idx) = pb_schema.index_of(field.name()) {
                    mapped_field_indices.push(idx);
                } else {
                    return df_execution_err!("field not found in pb schema");
                }
                Ok(mapped_field_indices)
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;

        Ok(Self {
            output_schema,
            output_schema_without_meta,
            pb_schema,
            output_array_builders,
            ensure_size,
            value_handlers,
            msg_mapping,
        })
    }
}

fn transfer_output_schema_to_pb_schema(
    message_descriptor: MessageDescriptor,
    output_schema: &SchemaRef,
    nested_msg_mapping: HashMap<String, String>,
    skip_fields: &[String],
) -> datafusion::error::Result<SchemaRef> {
    let mut pb_schema_fields: Vec<Field> = vec![];
    let mut sub_pb_nested_msg_mapping: HashMap<String, String> = HashMap::new();
    let mut sub_pb_schema_mapping: HashMap<String, Vec<Field>> = HashMap::new();
    // To ensure sequential processing, the output schema is used to traverse the
    // data.
    for field in output_schema.fields().iter() {
        if let Some(pb_nested_msg_name) = nested_msg_mapping.get(field.name()) {
            let index_start = pb_nested_msg_name.find(".");
            if let Some(index) = index_start {
                sub_pb_nested_msg_mapping.insert(
                    field.name().to_string(),
                    pb_nested_msg_name[(index + 1)..].to_string(),
                );
                sub_pb_schema_mapping
                    .entry(pb_nested_msg_name[..index].to_string())
                    .and_modify(|v| {
                        v.push(field.as_ref().clone());
                    })
                    .or_insert(vec![field.as_ref().clone()]);
            }
        }
    }
    let mut msg_set: HashSet<String> = HashSet::new();
    for field in output_schema.fields().iter() {
        if let Some(field_name) = nested_msg_mapping.get(field.name()) {
            let index_start = field_name.find(".");
            if let Some(index) = index_start {
                let msg_field_name = &field_name[..index];
                let msg_field_desc =
                    message_descriptor
                        .get_field_by_name(msg_field_name)
                        .expect(&format!(
                            "nested field {msg_field_name} not exits in message_descriptor"
                        ));
                if let Kind::Message(sub_message_desc) = msg_field_desc.kind() {
                    if !msg_set.contains(msg_field_name) {
                        let sub_fields = sub_pb_schema_mapping
                            .get(msg_field_name)
                            .ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "Field {msg_field_name} not found in sub_pb_schema_mapping"
                                ))
                            })?
                            .clone();
                        let sub_pb_schema = transfer_output_schema_to_pb_schema(
                            sub_message_desc.clone(),
                            &Arc::new(Schema::new(sub_fields)),
                            sub_pb_nested_msg_mapping.clone(),
                            skip_fields,
                        )
                        .expect("transfer_output_schema_to_pb_schema failed");
                        pb_schema_fields.push(Field::new(
                            msg_field_name,
                            DataType::Struct(sub_pb_schema.fields.clone()),
                            true,
                        ));
                        msg_set.insert(msg_field_name.to_string());
                    }
                } else {
                    return df_execution_err!("not message field");
                }
            } else {
                let msg_field_desc =
                    message_descriptor
                        .get_field_by_name(field_name)
                        .expect(&format!(
                            "nested innermost field {field_name} not exits in message_descriptor"
                        ));
                pb_schema_fields.push(create_arrow_field(msg_field_desc.clone(), skip_fields));
            }
        } else {
            let msg_field_desc = message_descriptor
                .get_field_by_name(field.name())
                .expect(&format!("{} not exits in message_descriptor", field.name()));
            pb_schema_fields.push(create_arrow_field(msg_field_desc.clone(), skip_fields));
        }
    }
    Ok(Arc::new(Schema::new(pb_schema_fields)))
}

fn create_arrow_field(field_desc: FieldDescriptor, skip_fields: &[String]) -> Field {
    Field::new(
        field_desc.name(),
        convert_pb_type_to_arrow(
            field_desc.kind(),
            field_desc.is_list(),
            field_desc.is_map(),
            field_desc.name(),
            skip_fields,
        )
        .expect("convert_pb_type_to_arrow failed"),
        true, // TODO: is_nullable
    )
}

fn convert_pb_type_to_arrow(
    field_kind: Kind,
    is_list: bool,
    is_map: bool,
    field_name: &str,
    skip_fields: &[String],
) -> datafusion::error::Result<DataType> {
    match field_kind {
        Kind::Bool => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Boolean,
                    true,
                )))
            } else {
                Ok(DataType::Boolean)
            }
        }
        Kind::String => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Utf8,
                    true,
                )))
            } else {
                Ok(DataType::Utf8)
            }
        }
        Kind::Bytes => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Binary,
                    true,
                )))
            } else {
                Ok(DataType::Binary)
            }
        }
        Kind::Float => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Float32,
                    true,
                )))
            } else {
                Ok(DataType::Float32)
            }
        }
        Kind::Double => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Float64,
                    true,
                )))
            } else {
                Ok(DataType::Float64)
            }
        }
        Kind::Int32 => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Int32,
                    true,
                )))
            } else {
                Ok(DataType::Int32)
            }
        }
        Kind::Int64 => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Int64,
                    true,
                )))
            } else {
                Ok(DataType::Int64)
            }
        }
        Kind::Uint32 => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::UInt32,
                    true,
                )))
            } else {
                Ok(DataType::UInt32)
            }
        }
        Kind::Uint64 => {
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::UInt64,
                    true,
                )))
            } else {
                Ok(DataType::UInt64)
            }
        }
        Kind::Enum(_enum_descriptor) => {
            // Enum to get the Name, so use String.
            if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Utf8,
                    true,
                )))
            } else {
                Ok(DataType::Utf8)
            }
        }
        Kind::Message(message_descriptor) => {
            if is_map {
                Ok(DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(
                            message_descriptor
                                .fields()
                                .filter(|field| {
                                    !skip_fields.contains(&field.full_name().to_string())
                                })
                                .map(|field| create_arrow_field(field, skip_fields))
                                .collect::<Vec<Field>>(),
                        )),
                        false,
                    )),
                    false,
                ))
            } else if is_list {
                Ok(DataType::List(create_arrow_field_ref(
                    field_name,
                    DataType::Struct(Fields::from(
                        message_descriptor
                            .fields()
                            .filter(|field| !skip_fields.contains(&field.full_name().to_string()))
                            .map(|field| create_arrow_field(field, skip_fields))
                            .collect::<Vec<Field>>(),
                    )),
                    true,
                )))
            } else {
                Ok(DataType::Struct(Fields::from(
                    message_descriptor
                        .fields()
                        .filter(|field| !skip_fields.contains(&field.full_name().to_string()))
                        .map(|field| create_arrow_field(field, skip_fields))
                        .collect::<Vec<Field>>(),
                )))
            }
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported data type for Arrow conversion: {other:?}"
            )));
        }
    }
}

fn create_arrow_field_ref(field_name: &str, data_type: DataType, is_nullable: bool) -> FieldRef {
    Arc::new(Field::new(field_name, data_type, is_nullable))
}

fn create_tag_to_output_mapping(
    message_descriptor: MessageDescriptor,
    output_schema: &SchemaRef,
) -> HashMap<u32, usize> {
    let mut tag_to_output_index = HashMap::new();

    for field in message_descriptor.fields() {
        if let Some(output_index) = output_schema
            .fields()
            .iter()
            .position(|f| f.name() == field.name())
        {
            tag_to_output_index.insert(field.number(), output_index);
        }
    }
    tag_to_output_index
}

fn create_output_array_builders(
    schema: &SchemaRef,
    message_descriptor: MessageDescriptor,
) -> datafusion::error::Result<Vec<SharedArrayBuilder>> {
    let mut array_builders: Vec<SharedArrayBuilder> = vec![];
    for field in schema.fields() {
        let field_name = field.name();
        let field_desc = message_descriptor
            .get_field_by_name(field_name)
            .expect(&format!(
                "Field {field_name} not exits in message_descriptor",
            ));
        match field.data_type() {
            DataType::Boolean => {
                array_builders.push(SharedArrayBuilder::new(BooleanBuilder::new()));
            }
            DataType::Int32 => {
                array_builders.push(SharedArrayBuilder::new(Int32Builder::new()));
            }
            DataType::Int64 => {
                array_builders.push(SharedArrayBuilder::new(Int64Builder::new()));
            }
            DataType::Utf8 => {
                array_builders.push(SharedArrayBuilder::new(StringBuilder::new()));
            }
            DataType::Float32 => {
                array_builders.push(SharedArrayBuilder::new(Float32Builder::new()));
            }
            DataType::Float64 => {
                array_builders.push(SharedArrayBuilder::new(Float64Builder::new()));
            }
            DataType::UInt32 => {
                array_builders.push(SharedArrayBuilder::new(UInt32Builder::new()));
            }
            DataType::UInt64 => {
                array_builders.push(SharedArrayBuilder::new(UInt64Builder::new()));
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                array_builders.push(SharedArrayBuilder::new(TimestampMillisecondBuilder::new()));
            }
            DataType::Binary => {
                array_builders.push(SharedArrayBuilder::new(BinaryBuilder::new()));
            }
            DataType::Struct(fields) => {
                let field_kind = field_desc.kind();
                let sub_msg_desc = field_kind.as_message().expect("as_message failed");
                let struct_builder = create_output_array_builders(
                    &Arc::new(Schema::new(fields.clone())),
                    sub_msg_desc.clone(),
                )
                .expect("struct create_output_array_builders failed");
                array_builders.push(SharedArrayBuilder::new(SharedStructArrayBuilder::new(
                    fields.clone(),
                    struct_builder,
                )));
            }
            DataType::Map(field_ref, _boolean) => {
                let field_kind = field_desc.kind();
                let sub_msg_desc = field_kind.as_message().expect("map as_message failed");
                if let DataType::Struct(fields) = field_ref.data_type() {
                    array_builders.push(SharedArrayBuilder::new(SharedMapArrayBuilder::new(
                        None,
                        create_shared_array_builder_by_data_type(
                            fields.get(0).expect("get 0 failed").data_type().clone(),
                            sub_msg_desc.get_field(1).expect("get map key failed"),
                        )
                        .expect("map create_shared_array_builder_by_data_type failed"),
                        create_shared_array_builder_by_data_type(
                            fields.get(1).expect("get 1 failed").data_type().clone(),
                            sub_msg_desc.get_field(2).expect("get map key failed"),
                        )
                        .expect("map create_shared_array_builder_by_data_type failed"),
                    )));
                } else {
                    return Err(DataFusionError::NotImplemented(format!(
                        "Unsupported Map data type for Arrow conversion: {field_ref:?}"
                    )));
                }
            }
            DataType::List(field_ref) => {
                array_builders.push(SharedArrayBuilder::new(SharedListArrayBuilder::new(
                    create_shared_array_builder_by_data_type(
                        field_ref.data_type().clone(),
                        field_desc,
                    )
                    .expect("List create_shared_array_builder_by_data_type failed"),
                )));
            }
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "Unsupported data type for Arrow conversion: {other:?}"
                )));
            }
        }
    }
    Ok(array_builders)
}

fn create_shared_array_builder_by_data_type(
    data_type: DataType,
    field_desc: FieldDescriptor,
) -> datafusion::error::Result<SharedArrayBuilder> {
    match data_type {
        DataType::Boolean => {
            return Ok(SharedArrayBuilder::new(BooleanBuilder::new()));
        }
        DataType::Int32 => {
            return Ok(SharedArrayBuilder::new(Int32Builder::new()));
        }
        DataType::Int64 => {
            return Ok(SharedArrayBuilder::new(Int64Builder::new()));
        }
        DataType::Utf8 => {
            return Ok(SharedArrayBuilder::new(StringBuilder::new()));
        }
        DataType::Float32 => {
            return Ok(SharedArrayBuilder::new(Float32Builder::new()));
        }
        DataType::Float64 => {
            return Ok(SharedArrayBuilder::new(Float64Builder::new()));
        }
        DataType::UInt32 => {
            return Ok(SharedArrayBuilder::new(UInt32Builder::new()));
        }
        DataType::UInt64 => {
            return Ok(SharedArrayBuilder::new(UInt64Builder::new()));
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            return Ok(SharedArrayBuilder::new(TimestampMillisecondBuilder::new()));
        }
        DataType::Binary => {
            return Ok(SharedArrayBuilder::new(BinaryBuilder::new()));
        }
        DataType::Struct(fields) => {
            let field_kind = field_desc.kind();
            let sub_msg_desc = field_kind.as_message().expect("as_message failed");
            let struct_builder = create_output_array_builders(
                &Arc::new(Schema::new(fields.clone())),
                sub_msg_desc.clone(),
            )
            .expect("struct create_output_array_builders failed");
            return Ok(SharedArrayBuilder::new(SharedStructArrayBuilder::new(
                fields.clone(),
                struct_builder,
            )));
        }
        DataType::Map(field_ref, _boolean) => {
            let field_kind = field_desc.kind();
            let sub_msg_desc = field_kind.as_message().expect("map as_message failed");
            if let DataType::Struct(fields) = field_ref.data_type() {
                return Ok(SharedArrayBuilder::new(SharedMapArrayBuilder::new(
                    None,
                    create_shared_array_builder_by_data_type(
                        fields.get(0).expect("get 0 failed").data_type().clone(),
                        sub_msg_desc.get_field(1).expect("get map key failed"),
                    )
                    .expect("map create_shared_array_builder_by_data_type failed"),
                    create_shared_array_builder_by_data_type(
                        fields.get(1).expect("get 1 failed").data_type().clone(),
                        sub_msg_desc.get_field(2).expect("get map key failed"),
                    )
                    .expect("map create_shared_array_builder_by_data_type failed"),
                )));
            } else {
                return df_execution_err!(
                    "Map DataType Unsupported non-struct data type for Arrow conversion"
                );
            }
        }
        DataType::List(field_ref) => {
            return Ok(SharedArrayBuilder::new(SharedListArrayBuilder::new(
                create_shared_array_builder_by_data_type(field_ref.data_type().clone(), field_desc)
                    .expect("List create_shared_array_builder_by_data_type failed"),
            )));
        }
        other => return df_execution_err!("Unsupported data type for Arrow conversion: {other:?}"),
    }
}

pub(crate) fn ensure_output_array_builders_size(
    builders: &[SharedArrayBuilder],
) -> datafusion::error::Result<Box<dyn FnMut(usize) + Send + Sync>> {
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    enum BuilderType {
        Boolean,
        Int32,
        Int64,
        UInt32,
        UInt64,
        String,
        Float32,
        Float64,
        TimestampMillisecond,
        Binary,
        SharedArrayStruct,
        SharedArrayList,
        SharedArrayMap,
    }
    let mut classified_builders = HashMap::<BuilderType, Vec<SharedArrayBuilder>>::new();
    let mut processing_builders = builders.to_vec();
    while let Some(builder) = processing_builders.pop() {
        if let Ok(_) = builder.get_mut::<BooleanBuilder>() {
            classified_builders
                .entry(BuilderType::Boolean)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<Int32Builder>() {
            classified_builders
                .entry(BuilderType::Int32)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<Int64Builder>() {
            classified_builders
                .entry(BuilderType::Int64)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<UInt32Builder>() {
            classified_builders
                .entry(BuilderType::UInt32)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<UInt64Builder>() {
            classified_builders
                .entry(BuilderType::UInt64)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<StringBuilder>() {
            classified_builders
                .entry(BuilderType::String)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<Float32Builder>() {
            classified_builders
                .entry(BuilderType::Float32)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<Float64Builder>() {
            classified_builders
                .entry(BuilderType::Float64)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<TimestampMillisecondBuilder>() {
            classified_builders
                .entry(BuilderType::TimestampMillisecond)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<BinaryBuilder>() {
            classified_builders
                .entry(BuilderType::Binary)
                .or_default()
                .push(builder.clone());
        } else if let Ok(struct_builder) = builder.get_mut::<SharedStructArrayBuilder>() {
            classified_builders
                .entry(BuilderType::SharedArrayStruct)
                .or_default()
                .push(builder.clone());
            processing_builders.extend(struct_builder.get_mut().get_field_builders().clone());
        } else if let Ok(_) = builder.get_mut::<SharedListArrayBuilder>() {
            classified_builders
                .entry(BuilderType::SharedArrayList)
                .or_default()
                .push(builder.clone());
        } else if let Ok(_) = builder.get_mut::<SharedMapArrayBuilder>() {
            classified_builders
                .entry(BuilderType::SharedArrayMap)
                .or_default()
                .push(builder.clone());
        } else {
            return Err(DataFusionError::NotImplemented(format!(
                "Unsupported data type for Arrow conversion in ensure_size: {:?}",
                builder.type_id()
            )));
        }
    }

    macro_rules! impl_for_builders {
        ($builder_type:ty, $builders:expr, $append_fn:expr) => {{
            let builders = $builders
                .into_iter()
                .map(|builder| builder.get_mut::<$builder_type>())
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            Box::new(move |size| {
                for builder in &builders {
                    let builder = builder.get_mut();
                    if builder.len() < size {
                        fn wrap(append_fn: impl Fn(&mut $builder_type), b: &mut $builder_type) {
                            append_fn(b);
                        }
                        wrap($append_fn, builder);
                    }
                }
            }) as Box<dyn FnMut(usize) + Send + Sync>
        }};
    }

    let mut adaptive_append_nulls = classified_builders
        .into_iter()
        .map(|(builder_type, builders)| {
            Ok(match builder_type {
                BuilderType::Boolean => {
                    impl_for_builders!(BooleanBuilder, builders, |b| b.append_null())
                }
                BuilderType::Int32 => {
                    impl_for_builders!(Int32Builder, builders, |b| b.append_value(0))
                }
                BuilderType::Int64 => {
                    impl_for_builders!(Int64Builder, builders, |b| b.append_value(0))
                }
                BuilderType::UInt32 => {
                    impl_for_builders!(UInt32Builder, builders, |b| b.append_value(0))
                }
                BuilderType::UInt64 => {
                    impl_for_builders!(UInt64Builder, builders, |b| b.append_value(0))
                }
                BuilderType::String => {
                    impl_for_builders!(StringBuilder, builders, |b| b.append_value(""))
                }
                BuilderType::Float32 => {
                    impl_for_builders!(Float32Builder, builders, |b| b.append_value(0.0))
                }
                BuilderType::Float64 => {
                    impl_for_builders!(Float64Builder, builders, |b| b.append_value(0.0))
                }
                BuilderType::TimestampMillisecond => {
                    impl_for_builders!(TimestampMillisecondBuilder, builders, |b| b.append_null())
                }
                BuilderType::Binary => {
                    impl_for_builders!(BinaryBuilder, builders, |b| b.append_value(b""))
                }
                BuilderType::SharedArrayStruct => {
                    impl_for_builders!(SharedStructArrayBuilder, builders, |b| b.append(false))
                }
                BuilderType::SharedArrayList => {
                    impl_for_builders!(SharedListArrayBuilder, builders, |b| b.append(true))
                }
                BuilderType::SharedArrayMap => {
                    impl_for_builders!(SharedMapArrayBuilder, builders, |b| b.append(true))
                }
            })
        })
        .collect::<datafusion::error::Result<Vec<_>>>()?;

    Ok(Box::new(move |size| {
        adaptive_append_nulls.iter_mut().for_each(|imp| {
            imp(size);
        })
    }))
}

fn get_output_array(
    struct_array: &StructArray,
    nested_field_name: &[usize],
) -> datafusion::error::Result<ArrayRef> {
    let column = struct_array.column(nested_field_name[0]);
    if nested_field_name.len() > 1 {
        return get_output_array(downcast_any!(column, StructArray)?, &nested_field_name[1..]);
    }
    Ok(column.clone())
}

fn create_value_handler(
    message_descriptor: &MessageDescriptor,
    tag_id: u32,
    tag_to_output_index: &HashMap<u32, usize>,
    pb_schema: &SchemaRef,
    output_array_builders: &[SharedArrayBuilder],
) -> datafusion::error::Result<ValueHandler> {
    let output_index = tag_to_output_index.get(&tag_id);
    let field = message_descriptor.get_field(tag_id);

    if let Some((field, &output_index)) = field.clone().zip(output_index) {
        let output_array_builder = output_array_builders[output_index].clone();
        let output_field = pb_schema.field(output_index);

        macro_rules! impl_for_builder {
            ($encoding_tyname:ident, $handle_fn:expr) => {{
                Box::new(move |cursor, tag, wire_type| {
                    let merge_method = prost::encoding::$encoding_tyname::merge;
                    let mut value = Default::default();
                    merge_method(wire_type, &mut value, cursor, DecodeContext::default()).map_err(
                        |e| {
                            DataFusionError::Execution(format!(
                                "Failed to decode {:?} [{}] and {} field: {}",
                                wire_type,
                                tag,
                                stringify!($encoding_tyname),
                                e
                            ))
                        },
                    )?;
                    $handle_fn(&value);
                    Ok(())
                })
            }};
        }

        macro_rules! impl_for_bytes_builder {
            ($encoding_tyname:ident, $handle_fn:expr) => {{
                Box::new(move |cursor: &mut Cursor<&[u8]>, _tag, wire_type| {
                    prost::encoding::check_wire_type(WireType::LengthDelimited, wire_type)
                        .or_else(|err| df_execution_err!("{err}"))?;
                    let len = prost::encoding::decode_varint(cursor)
                        .or_else(|err| df_execution_err!("{err}"))?;
                    if len > cursor.remaining() as u64 {
                        return df_execution_err!("buffer underflow");
                    }
                    let value = &cursor.get_mut()[cursor.position() as usize..][..len as usize];
                    $handle_fn(value);
                    cursor.advance(len as usize);
                    Ok(())
                })
            }};
        }

        macro_rules! impl_for_repeated_builder {
            ($encoding_tyname:ident, $handle_fn:expr) => {{
                Box::new(move |cursor, tag, wire_type| {
                    let merge_method = prost::encoding::$encoding_tyname::merge_repeated;
                    let value = UnsafeCell::new(Default::default());
                    merge_method(
                        wire_type,
                        unsafe { &mut *value.get() },
                        cursor,
                        DecodeContext::default(),
                    )
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to decode repeated {:?} [{}] and {} field: {}",
                            wire_type,
                            tag,
                            stringify!($encoding_tyname),
                            e
                        ))
                    })?;
                    $handle_fn(unsafe { &*value.get() });
                    unsafe { &mut *value.get() }.clear();
                    Ok(())
                })
            }};
        }

        macro_rules! impl_for_message_builder {
            ($handle_fn:expr) => {{
                Box::new(move |cursor: &mut Cursor<&[u8]>, _tag, wire_type| {
                    prost::encoding::check_wire_type(WireType::LengthDelimited, wire_type)
                        .or_else(|err| df_execution_err!("{err}"))?;
                    let len = prost::encoding::decode_varint(cursor)
                        .or_else(|err| df_execution_err!("{err}"))?;
                    if len > cursor.remaining() as u64 {
                        return df_execution_err!("buffer underflow");
                    }

                    $handle_fn(&cursor.get_mut()[cursor.position() as usize..][..len as usize]);
                    cursor.advance(len as usize);
                    Ok(())
                })
            }};
        }

        match field.kind() {
            Kind::Bool => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<BooleanBuilder>()?;
                    if field.is_packed() {
                        return Ok(impl_for_repeated_builder!(bool, |values: &Vec<bool>| {
                            for value in values {
                                array_builder.get_mut().append_value(*value);
                            }
                        }));
                    } else {
                        return Ok(impl_for_builder!(bool, |value: &bool| {
                            array_builder.get_mut().append_value(*value);
                        }));
                    }
                } else {
                    let array_builder = output_array_builder.get_mut::<BooleanBuilder>()?;
                    return Ok(impl_for_builder!(bool, |value: &bool| {
                        array_builder.get_mut().append_value(*value);
                    }));
                }
            }
            Kind::Int32 => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<Int32Builder>()?;
                    if field.is_packed() {
                        return Ok(impl_for_repeated_builder!(int32, |values: &Vec<i32>| {
                            for value in values {
                                array_builder.get_mut().append_value(*value);
                            }
                        }));
                    } else {
                        return Ok(impl_for_builder!(int32, |value: &i32| {
                            array_builder.get_mut().append_value(*value);
                        }));
                    }
                } else {
                    let array_builder = output_array_builder.get_mut::<Int32Builder>()?;
                    return Ok(impl_for_builder!(int32, |value: &i32| {
                        array_builder.get_mut().append_value(*value);
                    }));
                }
            }
            Kind::Int64 => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<Int64Builder>()?;
                    if field.is_packed() {
                        return Ok(impl_for_repeated_builder!(int64, |values: &Vec<i64>| {
                            for value in values {
                                array_builder.get_mut().append_value(*value);
                            }
                        }));
                    } else {
                        return Ok(impl_for_builder!(int64, |value: &i64| {
                            array_builder.get_mut().append_value(*value);
                        }));
                    }
                } else {
                    let array_builder = output_array_builder.get_mut::<Int64Builder>()?;
                    return Ok(impl_for_builder!(int64, |value: &i64| {
                        array_builder.get_mut().append_value(*value);
                    }));
                }
            }
            Kind::String => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<StringBuilder>()?;
                    return Ok(impl_for_bytes_builder!(string, |value: &[u8]| {
                        let s = unsafe { str::from_utf8_unchecked(value) };
                        array_builder.get_mut().append_value(s);
                    }));
                } else {
                    let array_builder = output_array_builder.get_mut::<StringBuilder>()?;
                    return Ok(impl_for_bytes_builder!(string, |value: &[u8]| {
                        let s = unsafe { str::from_utf8_unchecked(value) };
                        array_builder.get_mut().append_value(s);
                    }));
                }
            }
            Kind::Float => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<Float32Builder>()?;
                    if field.is_packed() {
                        return Ok(impl_for_repeated_builder!(float, |values: &Vec<f32>| {
                            for value in values {
                                array_builder.get_mut().append_value(*value);
                            }
                        }));
                    } else {
                        return Ok(impl_for_builder!(float, |value: &f32| {
                            array_builder.get_mut().append_value(*value);
                        }));
                    }
                } else {
                    let array_builder = output_array_builder.get_mut::<Float32Builder>()?;
                    return Ok(impl_for_builder!(float, |value: &f32| {
                        array_builder.get_mut().append_value(*value);
                    }));
                }
            }
            Kind::Double => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<Float64Builder>()?;
                    if field.is_packed() {
                        return Ok(impl_for_repeated_builder!(double, |values: &Vec<f64>| {
                            for value in values {
                                array_builder.get_mut().append_value(*value);
                            }
                        }));
                    } else {
                        return Ok(impl_for_builder!(double, |value: &f64| {
                            array_builder.get_mut().append_value(*value);
                        }));
                    }
                } else {
                    let array_builder = output_array_builder.get_mut::<Float64Builder>()?;
                    return Ok(impl_for_builder!(double, |value: &f64| {
                        array_builder.get_mut().append_value(*value);
                    }));
                }
            }
            Kind::Uint32 => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<UInt32Builder>()?;
                    if field.is_packed() {
                        return Ok(impl_for_repeated_builder!(uint32, |values: &Vec<u32>| {
                            for value in values {
                                array_builder.get_mut().append_value(*value);
                            }
                        }));
                    } else {
                        return Ok(impl_for_builder!(uint32, |value: &u32| {
                            array_builder.get_mut().append_value(*value);
                        }));
                    }
                } else {
                    let array_builder = output_array_builder.get_mut::<UInt32Builder>()?;
                    return Ok(impl_for_builder!(uint32, |value: &u32| {
                        array_builder.get_mut().append_value(*value);
                    }));
                }
            }
            Kind::Uint64 => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<UInt64Builder>()?;
                    if field.is_packed() {
                        return Ok(impl_for_repeated_builder!(uint64, |values: &Vec<u64>| {
                            for value in values {
                                array_builder.get_mut().append_value(*value);
                            }
                        }));
                    } else {
                        return Ok(impl_for_builder!(uint64, |value: &u64| {
                            array_builder.get_mut().append_value(*value);
                        }));
                    }
                } else {
                    let array_builder = output_array_builder.get_mut::<UInt64Builder>()?;
                    return Ok(impl_for_builder!(uint64, |value: &u64| {
                        array_builder.get_mut().append_value(*value);
                    }));
                }
            }
            Kind::Enum(enum_descriptor) => {
                let mut enum_string_mapping = HashMap::new();
                for enum_value_descriptor in enum_descriptor.values() {
                    enum_string_mapping.insert(
                        enum_value_descriptor.number(),
                        get_content_after_last_dot(enum_value_descriptor.name()).to_string(),
                    );
                }
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<StringBuilder>()?;
                    if field.is_packed() {
                        return Ok(impl_for_repeated_builder!(int32, |values: &Vec<i32>| {
                            for value in values {
                                array_builder.get_mut().append_value(
                                    enum_string_mapping
                                        .get(value)
                                        .map_or("Unknown", |v| v.as_str()),
                                );
                            }
                        }));
                    } else {
                        return Ok(impl_for_builder!(int32, |value: &i32| {
                            array_builder.get_mut().append_value(
                                enum_string_mapping
                                    .get(value)
                                    .map_or("Unknown", |v| v.as_str()),
                            );
                        }));
                    }
                } else {
                    let array_builder = output_array_builder.get_mut::<StringBuilder>()?;
                    return Ok(impl_for_builder!(int32, |value: &i32| {
                        array_builder.get_mut().append_value(
                            enum_string_mapping
                                .get(value)
                                .map_or("Unknown", |v| v.as_str()),
                        );
                    }));
                }
            }
            Kind::Message(sub_message_descriptor) => {
                if let DataType::Struct(sub_fields) = output_field.data_type() {
                    let sub_pb_schema = Arc::new(Schema::new(sub_fields.clone()));
                    let sub_tag_to_output_mapping = create_tag_to_output_mapping(
                        sub_message_descriptor.clone(),
                        &sub_pb_schema,
                    );
                    let sub_output_array_builders = output_array_builder
                        .get_mut::<SharedStructArrayBuilder>()
                        .expect("SharedStructArrayBuilder is null")
                        .get_mut()
                        .get_field_builders();
                    let mut sub_value_handlers: ValueHandlerMap = Default::default();
                    for field in sub_message_descriptor.fields() {
                        if let Ok(handler) = create_value_handler(
                            &sub_message_descriptor,
                            field.number(),
                            &sub_tag_to_output_mapping,
                            &sub_pb_schema,
                            &sub_output_array_builders,
                        ) {
                            sub_value_handlers.insert(field.number(), handler);
                        } else {
                            return df_execution_err!(
                                "Failed to create value handler for sub field: {:?}, {}",
                                field.kind(),
                                output_field.data_type()
                            );
                        }
                    }

                    let struct_builder = output_array_builder
                        .get_mut::<SharedStructArrayBuilder>()
                        .expect("SharedStructArrayBuilder is null");

                    return Ok(impl_for_message_builder!(|buf: &[u8]| {
                        if buf.is_empty() {
                            struct_builder.get_mut().append(false);
                        } else {
                            let mut sub_cursor = Cursor::new(buf);
                            while sub_cursor.has_remaining() {
                                if let Ok((sub_tag, sub_wire_type)) =
                                    prost::encoding::decode_key(&mut sub_cursor)
                                {
                                    if let Some(sub_value_handler) =
                                        sub_value_handlers.get(&sub_tag)
                                    {
                                        (*sub_value_handler)(
                                            &mut sub_cursor,
                                            sub_tag,
                                            sub_wire_type,
                                        )
                                        .expect("Failed to process sub field");
                                    }
                                }
                            }
                            struct_builder.get_mut().append(true);
                        }
                    }));
                } else if let DataType::List(struct_fields) = output_field.data_type() {
                    if let DataType::Struct(sub_fields) = struct_fields.data_type() {
                        let sub_pb_schema = Arc::new(Schema::new(sub_fields.clone()));
                        let sub_tag_to_output_mapping = create_tag_to_output_mapping(
                            sub_message_descriptor.clone(),
                            &sub_pb_schema,
                        );

                        let sub_output_array_builders = output_array_builder
                            .get_mut::<SharedListArrayBuilder>()
                            .expect("SharedListArrayBuilder is null")
                            .get_mut()
                            .values()
                            .get_mut::<SharedStructArrayBuilder>()
                            .expect("SharedStructArrayBuilder is null")
                            .get_mut()
                            .get_field_builders();
                        let mut sub_value_handlers: ValueHandlerMap = Default::default();
                        for field in sub_message_descriptor.fields() {
                            if let Ok(handler) = create_value_handler(
                                &sub_message_descriptor,
                                field.number(),
                                &sub_tag_to_output_mapping,
                                &sub_pb_schema,
                                &sub_output_array_builders,
                            ) {
                                sub_value_handlers.insert(field.number(), handler);
                            } else {
                                return df_execution_err!(
                                    "For List Struct Failed to create value handler for sub field: {:?}, {}",
                                    field.kind(),
                                    output_field.data_type()
                                );
                            }
                        }
                        return Ok(impl_for_message_builder!(|buf: &[u8]| {
                            let struct_builder = output_array_builder
                                .get_mut::<SharedListArrayBuilder>()
                                .expect("SharedListArrayBuilder is null")
                                .get_mut()
                                .values()
                                .get_mut::<SharedStructArrayBuilder>()
                                .expect("SharedStructArrayBuilder is null");
                            if buf.is_empty() {
                                struct_builder.get_mut().append(false);
                            } else {
                                // 解析嵌套的 message
                                let mut sub_cursor = Cursor::new(buf);
                                while sub_cursor.has_remaining() {
                                    if let Ok((sub_tag, sub_wire_type)) =
                                        prost::encoding::decode_key(&mut sub_cursor)
                                    {
                                        if let Some(sub_value_handler) =
                                            sub_value_handlers.get(&sub_tag)
                                        {
                                            (*sub_value_handler)(
                                                &mut sub_cursor,
                                                sub_tag,
                                                sub_wire_type,
                                            )
                                            .expect("Failed to process sub field");
                                        }
                                    }
                                }
                                struct_builder.get_mut().append(true);
                            }
                        }));
                    } else {
                        return Err(DataFusionError::Execution(format!(
                            "For List Struct Failed to create value handler field is not struct: {:?}, {}",
                            field.kind(),
                            output_field.data_type()
                        )));
                    }
                } else if let DataType::Map(struct_fields, _boolean) = output_field.data_type() {
                    if let DataType::Struct(sub_fields) = struct_fields.data_type() {
                        let sub_pb_schema = Arc::new(Schema::new(sub_fields.clone()));
                        let sub_tag_to_output_mapping = create_tag_to_output_mapping(
                            sub_message_descriptor.clone(),
                            &sub_pb_schema,
                        );
                        let mut sub_value_handlers: ValueHandlerMap = Default::default();
                        let map_builder = output_array_builder
                            .get_mut::<SharedMapArrayBuilder>()
                            .expect("SharedMapArrayBuilder is null");
                        let map_key_value_builder = map_builder.get_mut().entries();
                        let sub_output_array_builders = vec![
                            map_key_value_builder.0.clone(),
                            map_key_value_builder.1.clone(),
                        ];
                        for field in sub_message_descriptor.fields() {
                            if let Ok(handler) = create_value_handler(
                                &sub_message_descriptor,
                                field.number(),
                                &sub_tag_to_output_mapping,
                                &sub_pb_schema,
                                &sub_output_array_builders,
                            ) {
                                sub_value_handlers.insert(field.number(), handler);
                            } else {
                                return df_execution_err!(
                                    "Failed to create value handler for sub field: {:?}, {}",
                                    field.kind(),
                                    output_field.data_type()
                                );
                            }
                        }
                        let map_builder = output_array_builder
                            .get_mut::<SharedMapArrayBuilder>()
                            .expect("SharedMapArrayBuilder is null");

                        return Ok(impl_for_message_builder!(|buf: &[u8]| {
                            if buf.is_empty() {
                                map_builder.get_mut().append(true);
                            } else {
                                let mut sub_cursor = Cursor::new(buf);
                                while sub_cursor.has_remaining() {
                                    if let Ok((sub_tag, sub_wire_type)) =
                                        prost::encoding::decode_key(&mut sub_cursor)
                                    {
                                        if let Some(sub_value_handler) =
                                            sub_value_handlers.get(&sub_tag)
                                        {
                                            (*sub_value_handler)(
                                                &mut sub_cursor,
                                                sub_tag,
                                                sub_wire_type,
                                            )
                                            .expect("Failed to process sub field");
                                        }
                                    }
                                }
                            }
                        }));
                    } else {
                        return Err(DataFusionError::Execution(format!(
                            "For Map Failed to create value handler field is not struct: {:?}, {}",
                            field.kind(),
                            output_field.data_type()
                        )));
                    }
                } else {
                    return Err(DataFusionError::Execution(format!(
                        "Failed to create value handler field is not struct: {:?}, {}",
                        field.kind(),
                        output_field.data_type()
                    )));
                }
            }
            Kind::Bytes => {
                if field.is_list() {
                    let array_builder = output_array_builder
                        .get_mut::<SharedListArrayBuilder>()
                        .expect("SharedListArrayBuilder is null")
                        .get_mut()
                        .values()
                        .get_mut::<BinaryBuilder>()?;
                    return Ok(impl_for_builder!(bytes, |value: &Vec<u8>| {
                        array_builder.get_mut().append_value(value);
                    }));
                } else {
                    let array_builder = output_array_builder.get_mut::<BinaryBuilder>()?;
                    return Ok(impl_for_builder!(bytes, |value: &Vec<u8>| {
                        array_builder.get_mut().append_value(value);
                    }));
                }
            }
            _other => {
                return Err(DataFusionError::Execution(format!(
                    "Failed to create value handler field: {:?}, {}",
                    field.kind(),
                    output_field.data_type()
                )));
            }
        }
    }

    Ok(Box::new(|cursor, tag, wire_type| {
        let mut skip_value = move || {
            match wire_type {
                WireType::Varint => {
                    prost::encoding::decode_varint(cursor)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                }
                WireType::ThirtyTwoBit => {
                    if cursor.remaining() < 4 {
                        return df_execution_err!("buffer underflow");
                    }
                    cursor.advance(4);
                }
                WireType::SixtyFourBit => {
                    if cursor.remaining() < 8 {
                        return df_execution_err!("buffer underflow");
                    }
                    cursor.advance(8);
                }
                WireType::LengthDelimited => {
                    let len = prost::encoding::decode_varint(cursor)
                        .map_err(|e| DataFusionError::Execution(e.to_string()))?
                        as usize;
                    if cursor.remaining() < len {
                        return df_execution_err!("buffer underflow");
                    }
                    cursor.advance(len);
                }
                _ => {
                    UnknownField::decode_value(tag, wire_type, cursor, DecodeContext::default())
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to decode unknown value: {e}"
                            ))
                        })?;
                }
            }
            Ok(())
        };

        skip_value()
            .map_err(|e| DataFusionError::Execution(format!("Failed to decode unknown value: {e}")))
    }))
}

fn get_content_after_last_dot(s: &str) -> &str {
    match s.rfind('.') {
        Some(index) => &s[index + 1..],
        None => s,
    }
}

pub(crate) fn adaptive_append_children(
    builder: &SharedArrayBuilder,
) -> Option<Box<dyn FnMut(usize) + Send + Sync>> {
    let mut appender = None;
    if let Ok(builder) = builder.get_mut::<SharedStructArrayBuilder>() {
        let ensure_size =
            ensure_output_array_builders_size(&builder.get_mut().get_field_builders())
                .expect("ensure_output_array_builders_size failed");
        appender = Some(ensure_size);
    } else if let Ok(builder) = builder.get_mut::<SharedListArrayBuilder>() {
        let f: Box<dyn FnMut(usize) + Send + Sync> =
            Box::new(move |_| builder.get_mut().adaptive_append());
        appender = Some(f);
    } else if let Ok(builder) = builder.get_mut::<SharedMapArrayBuilder>() {
        let f: Box<dyn FnMut(usize) + Send + Sync> =
            Box::new(move |_| builder.get_mut().adaptive_append());
        appender = Some(f);
    }
    appender
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::*,
        datatypes::{DataType, Field, Schema},
    };
    use prost::Message as ProstMessage;
    use prost_reflect::prost_types::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
    use prost_types::{
        FieldDescriptorProto,
        field_descriptor_proto::{Label, Type},
    };

    use super::*;

    fn create_test_descriptor() -> Vec<u8> {
        let field_descriptors = vec![
            // int32 id = 1;
            FieldDescriptorProto {
                name: Some("id".to_string()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::Int32 as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("id".to_string()),
                options: None,
                proto3_optional: None,
            },
            // string name = 2;
            FieldDescriptorProto {
                name: Some("name".to_string()),
                number: Some(2),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("name".to_string()),
                options: None,
                proto3_optional: None,
            },
            // double score = 3;
            FieldDescriptorProto {
                name: Some("score".to_string()),
                number: Some(3),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::Double as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("score".to_string()),
                options: None,
                proto3_optional: None,
            },
            // bool active = 4;
            FieldDescriptorProto {
                name: Some("active".to_string()),
                number: Some(4),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::Bool as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("active".to_string()),
                options: None,
                proto3_optional: None,
            },
        ];

        let message_descriptor = DescriptorProto {
            name: Some("TestMessage".to_string()),
            field: field_descriptors,
            extension: vec![],
            nested_type: vec![],
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        };

        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            dependency: vec![],
            public_dependency: vec![],
            weak_dependency: vec![],
            message_type: vec![message_descriptor],
            enum_type: vec![],
            service: vec![],
            extension: vec![],
            options: None,
            source_code_info: None,
            syntax: Some("proto3".to_string()),
        };

        let descriptor_set = FileDescriptorSet {
            file: vec![file_descriptor],
        };

        let mut buf = Vec::new();
        descriptor_set
            .encode(&mut buf)
            .expect("Failed to encode FileDescriptorSet");
        buf
    }

    fn create_nested_test_descriptor() -> Vec<u8> {
        let address_fields = vec![
            // string street = 1;
            FieldDescriptorProto {
                name: Some("street".to_string()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("street".to_string()),
                options: None,
                proto3_optional: None,
            },
            // string city = 2;
            FieldDescriptorProto {
                name: Some("city".to_string()),
                number: Some(2),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("city".to_string()),
                options: None,
                proto3_optional: None,
            },
        ];

        let address_descriptor = DescriptorProto {
            name: Some("Address".to_string()),
            field: address_fields,
            extension: vec![],
            nested_type: vec![],
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        };

        let person_fields = vec![
            // string name = 1;
            FieldDescriptorProto {
                name: Some("name".to_string()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("name".to_string()),
                options: None,
                proto3_optional: None,
            },
            // Address address = 2;
            FieldDescriptorProto {
                name: Some("address".to_string()),
                number: Some(2),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::Message as i32),
                type_name: Some(".test.Address".to_string()),
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("address".to_string()),
                options: None,
                proto3_optional: None,
            },
        ];

        let person_descriptor = DescriptorProto {
            name: Some("Person".to_string()),
            field: person_fields,
            extension: vec![],
            nested_type: vec![],
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        };

        let file_descriptor = FileDescriptorProto {
            name: Some("nested_test.proto".to_string()),
            package: Some("test".to_string()),
            dependency: vec![],
            public_dependency: vec![],
            weak_dependency: vec![],
            message_type: vec![address_descriptor, person_descriptor],
            enum_type: vec![],
            service: vec![],
            extension: vec![],
            options: None,
            source_code_info: None,
            syntax: Some("proto3".to_string()),
        };

        let descriptor_set = FileDescriptorSet {
            file: vec![file_descriptor],
        };

        let mut buf = Vec::new();
        descriptor_set
            .encode(&mut buf)
            .expect("Failed to encode FileDescriptorSet");
        buf
    }

    fn create_test_message(id: i32, name: &str, score: f64, active: bool) -> Vec<u8> {
        use prost::encoding::*;

        let mut buf = Vec::new();

        // id (field 1, int32)
        encode_key(1, WireType::Varint, &mut buf);
        encode_varint(id as u64, &mut buf);

        // name (field 2, string)
        encode_key(2, WireType::LengthDelimited, &mut buf);
        encode_varint(name.len() as u64, &mut buf);
        buf.extend_from_slice(name.as_bytes());

        // score (field 3, double)
        encode_key(3, WireType::SixtyFourBit, &mut buf);
        buf.extend_from_slice(&score.to_le_bytes());

        // active (field 4, bool)
        encode_key(4, WireType::Varint, &mut buf);
        encode_varint(active as u64, &mut buf);

        buf
    }

    fn create_nested_test_message(name: &str, street: &str, city: &str) -> Vec<u8> {
        use prost::encoding::*;

        let mut buf = Vec::new();

        // name (field 1, string)
        encode_key(1, WireType::LengthDelimited, &mut buf);
        encode_varint(name.len() as u64, &mut buf);
        buf.extend_from_slice(name.as_bytes());

        // address (field 2, message)
        let mut address_buf = Vec::new();

        // address.street (field 1, string)
        encode_key(1, WireType::LengthDelimited, &mut address_buf);
        encode_varint(street.len() as u64, &mut address_buf);
        address_buf.extend_from_slice(street.as_bytes());

        encode_key(2, WireType::LengthDelimited, &mut address_buf);
        encode_varint(city.len() as u64, &mut address_buf);
        address_buf.extend_from_slice(city.as_bytes());

        encode_key(2, WireType::LengthDelimited, &mut buf);
        encode_varint(address_buf.len() as u64, &mut buf);
        buf.extend_from_slice(&address_buf);

        buf
    }

    fn create_binary_array(messages: Vec<Vec<u8>>) -> BinaryArray {
        let mut builder = BinaryBuilder::new();
        for msg in messages {
            builder.append_value(&msg);
        }
        builder.finish()
    }

    fn create_partition_array(partitions: Vec<i32>) -> Int32Array {
        Int32Array::from(partitions)
    }

    fn create_offset_array(offsets: Vec<i64>) -> Int64Array {
        Int64Array::from(offsets)
    }

    fn create_timestamp_array(timestamps: Vec<i64>) -> Int64Array {
        Int64Array::from(timestamps)
    }

    #[test]
    fn test_parse_messages_with_kafka_meta_basic() {
        let descriptor_data = create_test_descriptor();
        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        let mut deserializer = PbDeserializer::new(
            descriptor_data,
            "TestMessage", // 使用简短名称
            schema.clone(),
            &HashMap::new(),
            &[],
        )
        .expect("Failed to create deserializer");

        let messages = create_binary_array(vec![
            create_test_message(1, "Alice", 95.5, true),
            create_test_message(2, "Bob", 87.3, false),
            create_test_message(3, "Charlie", 92.1, true),
        ]);

        let partitions = create_partition_array(vec![0, 0, 1]);
        let offsets = create_offset_array(vec![100, 101, 50]);
        let timestamps = create_timestamp_array(vec![1234567890000, 1234567891000, 1234567892000]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to deserialize");

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 7);

        let partition_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast partition array to Int32Array");
        assert_eq!(partition_array.value(0), 0);
        assert_eq!(partition_array.value(1), 0);
        assert_eq!(partition_array.value(2), 1);

        let offset_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast offset array to Int64Array");
        assert_eq!(offset_array.value(0), 100);
        assert_eq!(offset_array.value(1), 101);
        assert_eq!(offset_array.value(2), 50);

        let timestamp_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast timestamp array to Int64Array");
        assert_eq!(timestamp_array.value(0), 1234567890000);
        assert_eq!(timestamp_array.value(1), 1234567891000);
        assert_eq!(timestamp_array.value(2), 1234567892000);

        let id_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast id array to Int32Array");
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);

        let name_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast name array to StringArray");
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");
        assert_eq!(name_array.value(2), "Charlie");

        let score_array = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast score array to Float64Array");
        assert_eq!(score_array.value(0), 95.5);
        assert_eq!(score_array.value(1), 87.3);
        assert_eq!(score_array.value(2), 92.1);

        let active_array = batch
            .column(6)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Failed to downcast active array to BooleanArray");
        assert!(active_array.value(0));
        assert!(!active_array.value(1));
        assert!(active_array.value(2));
    }

    #[test]
    fn test_parse_messages_with_kafka_meta_nested() {
        let descriptor_data = create_nested_test_descriptor();

        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("street", DataType::Utf8, true),
            Field::new("city", DataType::Utf8, true),
        ]));

        let mut nested_mapping = HashMap::new();
        nested_mapping.insert("street".to_string(), "address.street".to_string());
        nested_mapping.insert("city".to_string(), "address.city".to_string());

        let mut deserializer = PbDeserializer::new(
            descriptor_data,
            "Person",
            schema.clone(),
            &nested_mapping,
            &[],
        )
        .expect("Failed to create deserializer");

        let messages = create_binary_array(vec![
            create_nested_test_message("Alice", "123 Main St", "New York"),
            create_nested_test_message("Bob", "456 Oak Ave", "Los Angeles"),
        ]);

        let partitions = create_partition_array(vec![0, 1]);
        let offsets = create_offset_array(vec![200, 150]);
        let timestamps = create_timestamp_array(vec![1234567893000, 1234567894000]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to deserialize");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6);

        let partition_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast partition array to Int32Array");
        assert_eq!(partition_array.value(0), 0);
        assert_eq!(partition_array.value(1), 1);

        let name_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast name array to StringArray");
        assert_eq!(name_array.value(0), "Alice");
        assert_eq!(name_array.value(1), "Bob");

        let street_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast street array to StringArray");
        assert_eq!(street_array.value(0), "123 Main St");
        assert_eq!(street_array.value(1), "456 Oak Ave");

        let city_array = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast city array to StringArray");
        assert_eq!(city_array.value(0), "New York");
        assert_eq!(city_array.value(1), "Los Angeles");
    }

    #[test]
    fn test_parse_messages_with_kafka_meta_empty() {
        let descriptor_data = create_test_descriptor();

        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let mut deserializer = PbDeserializer::new(
            descriptor_data,
            "TestMessage",
            schema.clone(),
            &HashMap::new(),
            &[],
        )
        .expect("Failed to create deserializer");

        let messages = create_binary_array(vec![]);
        let partitions = create_partition_array(vec![]);
        let offsets = create_offset_array(vec![]);
        let timestamps = create_timestamp_array(vec![]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to deserialize");

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_parse_messages_with_kafka_meta_different_partitions() {
        let descriptor_data = create_test_descriptor();

        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let mut deserializer = PbDeserializer::new(
            descriptor_data,
            "TestMessage",
            schema.clone(),
            &HashMap::new(),
            &[],
        )
        .expect("Failed to create deserializer");

        let messages = create_binary_array(vec![
            create_test_message(1, "Alice", 95.5, true),
            create_test_message(2, "Bob", 87.3, false),
            create_test_message(3, "Charlie", 92.1, true),
            create_test_message(4, "David", 88.0, false),
        ]);

        let partitions = create_partition_array(vec![0, 1, 0, 2]);
        let offsets = create_offset_array(vec![100, 50, 200, 75]);
        let timestamps = create_timestamp_array(vec![1000, 2000, 3000, 4000]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to deserialize");

        assert_eq!(batch.num_rows(), 4);
        assert_eq!(batch.num_columns(), 5);

        // check partition
        let partition_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast partition array to Int32Array");
        assert_eq!(partition_array.value(0), 0);
        assert_eq!(partition_array.value(1), 1);
        assert_eq!(partition_array.value(2), 0);
        assert_eq!(partition_array.value(3), 2);

        // check offset
        let offset_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast offset array to Int64Array");
        assert_eq!(offset_array.value(0), 100);
        assert_eq!(offset_array.value(1), 50);
        assert_eq!(offset_array.value(2), 200);
        assert_eq!(offset_array.value(3), 75);

        // check timestamp
        let timestamp_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast timestamp array to Int64Array");
        assert_eq!(timestamp_array.value(0), 1000);
        assert_eq!(timestamp_array.value(1), 2000);
        assert_eq!(timestamp_array.value(2), 3000);
        assert_eq!(timestamp_array.value(3), 4000);

        // check id
        let id_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast id array to Int32Array");
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);
        assert_eq!(id_array.value(2), 3);
        assert_eq!(id_array.value(3), 4);
    }
}
