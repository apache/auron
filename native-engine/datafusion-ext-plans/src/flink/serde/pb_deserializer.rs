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
use datafusion::error::{DataFusionError, Result};
use datafusion_ext_commons::{df_execution_err, downcast_any};
use prost::encoding::{DecodeContext, WireType};
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor, UnknownField};

use crate::flink::serde::{
    flink_deserializer::FlinkDeserializer, shared_array_builder::SharedArrayBuilder,
    shared_list_array_builder::SharedListArrayBuilder,
    shared_map_array_builder::SharedMapArrayBuilder,
    shared_struct_array_builder::SharedStructArrayBuilder,
};

type ValueHandler = Box<dyn Fn(&mut Cursor<&[u8]>, u32, WireType) -> Result<()> + Send>;
type ValueHandlerMap = hashbrown::HashMap<u32, ValueHandler, foldhash::fast::RandomState>;

/// Adaptive dispatch table for protobuf field handlers keyed by tag.
///
/// O2 optimization: when the tag space is dense (max_tag is small relative to
/// the number of fields), use a `Vec<Option<_>>` for O(1) array indexing,
/// avoiding the HashMap hashing/probing overhead on the hot path. When tags
/// are sparse (e.g. extensions or large field numbers), fall back to a
/// `HashMap` to avoid wasting memory.
///
/// The threshold `max_tag <= 64 && max_tag <= 4 * field_count` keeps the Vec
/// path activated for the overwhelmingly common case where producers use
/// small contiguous tags (typically 1..N).
enum ValueHandlers {
    Vec(Vec<Option<ValueHandler>>),
    Map(ValueHandlerMap),
}

impl ValueHandlers {
    fn from_map(map: ValueHandlerMap) -> Self {
        let max_tag = map.keys().copied().max().unwrap_or(0);
        let field_count = map.len();
        // Heuristic: dense enough and within 64-tag bitmap range. We cap at
        // 64 so it composes nicely with O3's seen_tags bitmap, but the cap
        // is independent — the fallback HashMap remains correct.
        if field_count > 0 && max_tag <= 64 && (max_tag as usize) <= field_count.saturating_mul(4) {
            let mut vec: Vec<Option<ValueHandler>> = (0..=max_tag).map(|_| None).collect();
            for (tag, handler) in map.into_iter() {
                vec[tag as usize] = Some(handler);
            }
            ValueHandlers::Vec(vec)
        } else {
            ValueHandlers::Map(map)
        }
    }

    #[inline(always)]
    fn get(&self, tag: u32) -> Option<&ValueHandler> {
        match self {
            ValueHandlers::Vec(v) => v.get(tag as usize).and_then(|h| h.as_ref()),
            ValueHandlers::Map(m) => m.get(&tag),
        }
    }

    fn len(&self) -> usize {
        match self {
            ValueHandlers::Vec(v) => v.iter().filter(|h| h.is_some()).count(),
            ValueHandlers::Map(m) => m.len(),
        }
    }
}

pub struct PbDeserializer {
    output_schema: SchemaRef,
    output_schema_without_meta: SchemaRef,
    pb_schema: SchemaRef,
    output_array_builders: Vec<SharedArrayBuilder>,
    ensure_size: Box<dyn FnMut(usize) + Send>,
    value_handlers: ValueHandlers,
    /// O(n)/O(1)-read cache of `value_handlers.len()`, computed once in
    /// `try_new`. The `Vec` variant of `len()` is an O(max_tag) scan, so
    /// recomputing it per batch (as `total_handlers`) is wasteful — it is
    /// constant for the deserializer's lifetime.
    handler_count: u32,
    msg_mapping: Vec<Vec<usize>>,
    /// C1 fix: whether any top-level pb_schema column is a List or Map. The O3
    /// ensure_size skip is only sound for scalar/struct columns, which finalize
    /// their own per-row slot. List/Map builders rely on ensure_size to append
    /// their per-row offset/null entries (the per-value handlers only push to
    /// the child values builder, never the parent). When this is true,
    /// ensure_size must run every row regardless of how many tags were seen.
    top_level_has_list_or_map: bool,
}

impl FlinkDeserializer for PbDeserializer {
    fn parse_messages_with_kafka_meta(
        &mut self,
        messages: &BinaryArray,
        kafka_partition: &Int32Array,
        kafka_offset: &Int64Array,
        kafka_timestamp: &Int64Array,
    ) -> datafusion::common::Result<RecordBatch> {
        // O5: inline cursor creation (avoid Vec<Cursor<&[u8]>> preallocation)
        // O7/C3 fix: replace `expect("message bytes must not be null")` with `?`
        //            so that JNI callers don't crash the JVM via process abort.
        // O3: track which tags appear via a u64 bitmap (tag 0..63). When all
        //     schema tags were observed in a row, scalar/struct builders are
        //     already aligned and ensure_size can be skipped for that row.
        // C1 fix: the O3 skip is UNSOUND for top-level List/Map columns. Their
        //     per-row offset/null slot is finalized only inside ensure_size —
        //     the per-value handlers append to the child values builder, never
        //     to the parent SharedListArrayBuilder/SharedMapArrayBuilder. So
        //     when the schema has any top-level List/Map, ensure_size must run
        //     every row (see `ensure_size_every_row` below).
        // NOTE on builder row-alignment invariant: every row, all builders must
        // be padded to length `row_idx + 1`. We therefore must NOT defer
        // ensure_size to after the loop — that would let later rows write
        // values into the wrong positions.
        // NOTE: we cannot use a simple counter because protobuf repeated
        // fields (non-packed) emit multiple tag-value pairs for the same tag,
        // which would over-count. The bitmap correctly records unique tags.
        let total_handlers = self.handler_count;
        let ensure_size_every_row = self.top_level_has_list_or_map;
        for (row_idx, opt_bytes) in messages.iter().enumerate() {
            let bytes = opt_bytes.ok_or_else(|| {
                DataFusionError::Execution("message bytes must not be null".to_string())
            })?;
            let mut msg_cursor = Cursor::new(bytes);
            let mut seen_tags: u64 = 0;
            while msg_cursor.has_remaining() {
                let (tag, wired_type) =
                    prost::encoding::decode_key(&mut msg_cursor).map_err(|e| {
                        DataFusionError::Execution(format!("Failed to parse protobuf key: {e}"))
                    })?;
                if let Some(value_handler) = self.value_handlers.get(tag) {
                    value_handler(&mut msg_cursor, tag, wired_type)?;
                    // Tags >= 64 fall through to ensure_size (always safe).
                    if tag < 64 {
                        seen_tags |= 1u64 << tag;
                    }
                } else {
                    // O1/C1 fix: skip unknown tags so the cursor stays in sync.
                    skip_pb_value(&mut msg_cursor, tag, wired_type)?;
                }
            }
            if ensure_size_every_row || seen_tags.count_ones() < total_handlers {
                (self.ensure_size)(row_idx + 1);
            }
        }

        // O4 optimization: avoid building an intermediate `RecordBatch` and
        // converting it to `StructArray`. We finish builders directly into a
        // `Vec<ArrayRef>` and walk the per-output `msg_mapping` path to
        // extract the target column from any nested StructArray.
        let pb_top_arrays: Vec<ArrayRef> = self
            .output_array_builders
            .iter()
            .map(|builder| builder.get_dyn_mut().finish())
            .collect();
        let mut output_arrays: Vec<ArrayRef> = Vec::new();
        output_arrays.push(Arc::new(kafka_partition.clone()));
        output_arrays.push(Arc::new(kafka_offset.clone()));
        output_arrays.push(Arc::new(kafka_timestamp.clone()));
        for (field_idx, field) in self.output_schema_without_meta.fields().iter().enumerate() {
            let mapping = &self.msg_mapping[field_idx];
            let array_ref: ArrayRef = get_output_array_from_top(&pb_top_arrays, mapping)?;
            if array_ref.null_count() == array_ref.len() {
                output_arrays.push(new_null_array(field.data_type(), array_ref.len()));
            } else {
                // O7/C3 fix: replace `.expect("Failed to cast array")` with
                // error propagation so JNI callers don't get a process abort.
                output_arrays.push(
                    datafusion_ext_commons::arrow::cast::cast(&array_ref, field.data_type())
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to cast array for field {}: {e}",
                                field.name()
                            ))
                        })?,
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
    ) -> Result<Self> {
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
    ) -> Result<Self> {
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
        // O9: pass nested_msg_mapping by reference to avoid a HashMap clone
        // on every initialization (and on every recursive nested call).
        let pb_schema = transfer_output_schema_to_pb_schema(
            message_descriptor.clone(),
            &output_schema_without_meta,
            nested_msg_mapping,
            &skip_fields,
        )?;

        let tag_to_output_mapping =
            create_tag_to_output_mapping(message_descriptor.clone(), &pb_schema);

        let output_array_builders =
            create_output_array_builders(&pb_schema, message_descriptor.clone())?;
        let ensure_size = ensure_output_array_builders_size(&output_array_builders)?;

        let value_handlers_map = message_descriptor
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
            .collect::<Result<hashbrown::HashMap<_, _, foldhash::fast::RandomState>>>()?;
        // O2 optimization: switch to Vec<Option<_>> when tags are dense.
        let value_handlers = ValueHandlers::from_map(value_handlers_map);
        // Precompute the handler count once (the Vec variant's `len()` is an
        // O(max_tag) scan); the per-batch hot path reads `handler_count` instead.
        let handler_count = value_handlers.len() as u32;

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
            .collect::<Result<Vec<_>>>()?;

        // C1 fix: detect top-level List/Map columns that require ensure_size
        // every row (their per-row slots are finalized only inside ensure_size).
        let top_level_has_list_or_map = pb_schema
            .fields()
            .iter()
            .any(|f| matches!(f.data_type(), DataType::List(_) | DataType::Map(_, _)));

        Ok(Self {
            output_schema,
            output_schema_without_meta,
            pb_schema,
            output_array_builders,
            ensure_size,
            value_handlers,
            handler_count,
            msg_mapping,
            top_level_has_list_or_map,
        })
    }
}

fn transfer_output_schema_to_pb_schema(
    message_descriptor: MessageDescriptor,
    output_schema: &SchemaRef,
    nested_msg_mapping: &HashMap<String, String>,
    skip_fields: &[String],
) -> Result<SchemaRef> {
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
                let msg_field_desc = message_descriptor
                    .get_field_by_name(msg_field_name)
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "nested field {msg_field_name} does not exist in message_descriptor"
                        ))
                    })?;
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
                            // O9 optimization: pass by reference instead of
                            // cloning the entire HashMap on every recursive
                            // call.
                            &sub_pb_nested_msg_mapping,
                            skip_fields,
                        )?;
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
                let msg_field_desc = message_descriptor
                    .get_field_by_name(field_name)
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "nested innermost field {field_name} does not exist in message_descriptor"
                        ))
                    })?;
                pb_schema_fields.push(create_arrow_field(msg_field_desc.clone(), skip_fields));
            }
        } else {
            let msg_field_desc = message_descriptor
                .get_field_by_name(field.name())
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{} does not exist in message_descriptor",
                        field.name()
                    ))
                })?;
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
) -> Result<DataType> {
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
) -> Result<Vec<SharedArrayBuilder>> {
    let mut array_builders: Vec<SharedArrayBuilder> = vec![];
    for field in schema.fields() {
        let field_name = field.name();
        let field_desc = message_descriptor
            .get_field_by_name(field_name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Field {field_name} does not exist in message_descriptor"
                ))
            })?;
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
                    Some(field_ref.clone()),
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
) -> Result<SharedArrayBuilder> {
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
                Some(field_ref.clone()),
            )));
        }
        other => return df_execution_err!("Unsupported data type for Arrow conversion: {other:?}"),
    }
}

pub(crate) fn ensure_output_array_builders_size(
    builders: &[SharedArrayBuilder],
) -> Result<Box<dyn FnMut(usize) + Send + Sync>> {
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
                .collect::<Result<Vec<_>>>()?;
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
                    impl_for_builders!(BooleanBuilder, builders, |b| b.append_value(false))
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
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(move |size| {
        adaptive_append_nulls.iter_mut().for_each(|imp| {
            imp(size);
        })
    }))
}

fn get_output_array(struct_array: &StructArray, nested_field_name: &[usize]) -> Result<ArrayRef> {
    let column = struct_array.column(nested_field_name[0]);
    if nested_field_name.len() > 1 {
        return get_output_array(downcast_any!(column, StructArray)?, &nested_field_name[1..]);
    }
    Ok(column.clone())
}

/// O4 optimization helper: extract a (possibly nested) column from the list
/// of top-level finished arrays without first building a wrapping
/// `StructArray` for the root level. The first index selects from the top
/// `Vec<ArrayRef>`; remaining indices descend into nested `StructArray`s.
fn get_output_array_from_top(
    top_arrays: &[ArrayRef],
    nested_field_indices: &[usize],
) -> Result<ArrayRef> {
    let column = top_arrays[nested_field_indices[0]].clone();
    if nested_field_indices.len() > 1 {
        return get_output_array(
            downcast_any!(&column, StructArray)?,
            &nested_field_indices[1..],
        );
    }
    Ok(column)
}

fn create_value_handler(
    message_descriptor: &MessageDescriptor,
    tag_id: u32,
    tag_to_output_index: &HashMap<u32, usize>,
    pb_schema: &SchemaRef,
    output_array_builders: &[SharedArrayBuilder],
) -> Result<ValueHandler> {
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
                    // O7/C3 fix: propagate handle_fn errors instead of
                    // discarding them, so an invalid UTF-8 string (or any other
                    // handle_fn failure) surfaces to the caller rather than
                    // aborting the JVM via JNI.
                    let res: Result<()> = $handle_fn(value);
                    res?;
                    cursor.advance(len as usize);
                    Ok(())
                })
            }};
        }

        macro_rules! impl_for_repeated_builder {
            ($encoding_tyname:ident, $handle_fn:expr) => {{
                // O6 optimization: hoist the buffer out of the per-call body so
                // its capacity is reused across calls instead of alloc/dealloc
                // per repeated field decode. We use `RefCell` because the outer
                // ValueHandler is `Box<dyn Fn>` (immutable closure); the buffer
                // is borrowed mut for the duration of decoding/handle_fn, and
                // each handler is single-threaded.
                let value_buf: std::cell::RefCell<Vec<_>> =
                    std::cell::RefCell::new(Default::default());
                Box::new(move |cursor, tag, wire_type| {
                    let merge_method = prost::encoding::$encoding_tyname::merge_repeated;
                    let mut value = value_buf.borrow_mut();
                    value.clear();
                    merge_method(wire_type, &mut *value, cursor, DecodeContext::default())
                        .map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Failed to decode repeated {:?} [{}] and {} field: {}",
                                wire_type,
                                tag,
                                stringify!($encoding_tyname),
                                e
                            ))
                        })?;
                    $handle_fn(&*value);
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

                    // O7/C3 fix: handle_fn is now expected to return Result<()> so
                    // sub-handler errors propagate up through `?` instead of using
                    // .expect()` which would abort the JVM via JNI.
                    let res: Result<()> =
                        $handle_fn(&cursor.get_mut()[cursor.position() as usize..][..len as usize]);
                    res?;
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
                        // SAFETY: validate on the release path. protobuf 3 says
                        // `string` fields are UTF-8, but Kafka payloads may come
                        // from non-conformant producers; an unchecked decode
                        // would construct an invalid `&str` and violate Arrow's
                        // UTF-8 invariant (UB). Surface the error instead.
                        let s = std::str::from_utf8(value).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "protobuf string field contains invalid UTF-8: {e}"
                            ))
                        })?;
                        array_builder.get_mut().append_value(s);
                        Ok(())
                    }));
                } else {
                    let array_builder = output_array_builder.get_mut::<StringBuilder>()?;
                    return Ok(impl_for_bytes_builder!(string, |value: &[u8]| {
                        // SAFETY: see above — validate UTF-8 on the release path.
                        let s = std::str::from_utf8(value).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "protobuf string field contains invalid UTF-8: {e}"
                            ))
                        })?;
                        array_builder.get_mut().append_value(s);
                        Ok(())
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
                // Build the enum number→name map once for this field and move it
                // into the value-handler closure. It is per-field (not shared
                // across handlers), so a plain owned HashMap is enough — no Arc
                // refcount overhead.
                let mut enum_string_mapping: HashMap<i32, String> = HashMap::new();
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
                    let sub_ensure_size = std::cell::RefCell::new(
                        ensure_output_array_builders_size(&sub_output_array_builders)?,
                    );

                    return Ok(impl_for_message_builder!(|buf: &[u8]| -> Result<()> {
                        if buf.is_empty() {
                            // C2 fix: pad the struct's child builders before
                            // advancing the struct null buffer, so children
                            // length stays aligned with the struct length.
                            (sub_ensure_size.borrow_mut())(struct_builder.get_mut().len() + 1);
                            struct_builder.get_mut().append(false);
                        } else {
                            decode_sub_message(buf, &sub_value_handlers)?;
                            (sub_ensure_size.borrow_mut())(struct_builder.get_mut().len() + 1);
                            struct_builder.get_mut().append(true);
                        }
                        Ok(())
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
                        let sub_ensure_size = std::cell::RefCell::new(
                            ensure_output_array_builders_size(&sub_output_array_builders)?,
                        );
                        return Ok(impl_for_message_builder!(|buf: &[u8]| -> Result<()> {
                            let struct_builder = output_array_builder
                                .get_mut::<SharedListArrayBuilder>()
                                .expect("SharedListArrayBuilder is null")
                                .get_mut()
                                .values()
                                .get_mut::<SharedStructArrayBuilder>()
                                .expect("SharedStructArrayBuilder is null");
                            if buf.is_empty() {
                                // C2 fix: pad child builders before append(false)
                                // to keep struct children aligned with the
                                // struct length (symmetric with the non-empty
                                // branch below).
                                (sub_ensure_size.borrow_mut())(struct_builder.get_mut().len() + 1);
                                struct_builder.get_mut().append(false);
                            } else {
                                // 解析嵌套的 message
                                decode_sub_message(buf, &sub_value_handlers)?;
                                (sub_ensure_size.borrow_mut())(struct_builder.get_mut().len() + 1);
                                struct_builder.get_mut().append(true);
                            }
                            Ok(())
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

                        return Ok(impl_for_message_builder!(|buf: &[u8]| -> Result<()> {
                            if buf.is_empty() {
                                map_builder.get_mut().append(true);
                            } else {
                                decode_sub_message(buf, &sub_value_handlers)?;
                            }
                            Ok(())
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
        skip_pb_value(cursor, tag, wire_type)
            .map_err(|e| DataFusionError::Execution(format!("Failed to decode unknown value: {e}")))
    }))
}

fn get_content_after_last_dot(s: &str) -> &str {
    match s.rfind('.') {
        Some(index) => &s[index + 1..],
        None => s,
    }
}

/// Skip an unknown protobuf field's value, advancing the cursor past it so the
/// outer parsing loop stays in sync. Used by both the top-level main loop and
/// the fallback handler returned by `create_value_handler` when the field has
/// no associated builder. Without this, an unknown tag (e.g., a new field
/// added by an upstream producer) would leave the cursor positioned at the
/// value bytes and the next `decode_key` would interpret garbage.
fn skip_pb_value(cursor: &mut Cursor<&[u8]>, tag: u32, wire_type: WireType) -> Result<()> {
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
            UnknownField::decode_value(tag, wire_type, cursor, DecodeContext::default()).map_err(
                |e| DataFusionError::Execution(format!("Failed to decode unknown value: {e}")),
            )?;
        }
    }
    Ok(())
}

/// Decode a length-delimited sub-message body, dispatching each known tag to
/// its value handler and skipping unknown tags (C1) with error propagation
/// (O7). Shared by the Struct / List-of-Struct / Map sub-message handlers,
/// which were previously three near-verbatim copies of this loop.
fn decode_sub_message(buf: &[u8], handlers: &ValueHandlerMap) -> Result<()> {
    let mut sub_cursor = Cursor::new(buf);
    while sub_cursor.has_remaining() {
        let (sub_tag, sub_wire_type) = prost::encoding::decode_key(&mut sub_cursor)
            .map_err(|e| DataFusionError::Execution(format!("Failed to decode sub key: {e}")))?;
        if let Some(sub_value_handler) = handlers.get(&sub_tag) {
            (*sub_value_handler)(&mut sub_cursor, sub_tag, sub_wire_type)?;
        } else {
            // C1 fix: skip unknown sub-tags so the cursor stays in sync.
            skip_pb_value(&mut sub_cursor, sub_tag, sub_wire_type)?;
        }
    }
    Ok(())
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
        FieldDescriptorProto, MessageOptions,
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

    fn create_repeated_test_descriptor() -> Vec<u8> {
        let field_descriptors = vec![
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
            FieldDescriptorProto {
                name: Some("scores".to_string()),
                number: Some(2),
                label: Some(Label::Repeated as i32),
                r#type: Some(Type::Int32 as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("scores".to_string()),
                options: None,
                proto3_optional: None,
            },
        ];

        let message_descriptor = DescriptorProto {
            name: Some("RepeatedMessage".to_string()),
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
            name: Some("repeated_test.proto".to_string()),
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

    /// Descriptor for a message with a single top-level `map<string, int32>`
    /// field `kv` (number 1). The map entry is a nested message `KvEntry`
    /// with the `[map_entry = true]` option, which is what `prost_reflect`
    /// keys on to report `field.is_map()` == true (and thus produce an
    /// Arrow `DataType::Map` rather than a `DataType::List` of struct).
    fn create_map_test_descriptor() -> Vec<u8> {
        let kv_entry_fields = vec![
            // string key = 1;
            FieldDescriptorProto {
                name: Some("key".to_string()),
                number: Some(1),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::String as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("key".to_string()),
                options: None,
                proto3_optional: None,
            },
            // int32 value = 2;
            FieldDescriptorProto {
                name: Some("value".to_string()),
                number: Some(2),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::Int32 as i32),
                type_name: None,
                extendee: None,
                default_value: None,
                oneof_index: None,
                json_name: Some("value".to_string()),
                options: None,
                proto3_optional: None,
            },
        ];

        let kv_entry_descriptor = DescriptorProto {
            name: Some("KvEntry".to_string()),
            field: kv_entry_fields,
            extension: vec![],
            nested_type: vec![],
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            // The marker that makes prost_reflect treat `kv` as a map.
            options: Some(MessageOptions {
                map_entry: Some(true),
                ..Default::default()
            }),
            reserved_range: vec![],
            reserved_name: vec![],
        };

        // map<string, int32> kv = 1; — desugars to `repeated KvEntry kv = 1`.
        let kv_field = FieldDescriptorProto {
            name: Some("kv".to_string()),
            number: Some(1),
            label: Some(Label::Repeated as i32),
            r#type: Some(Type::Message as i32),
            type_name: Some(".test.MapMessage.KvEntry".to_string()),
            extendee: None,
            default_value: None,
            oneof_index: None,
            json_name: Some("kv".to_string()),
            options: None,
            proto3_optional: None,
        };

        let map_message_descriptor = DescriptorProto {
            name: Some("MapMessage".to_string()),
            field: vec![kv_field],
            extension: vec![],
            nested_type: vec![kv_entry_descriptor],
            enum_type: vec![],
            extension_range: vec![],
            oneof_decl: vec![],
            options: None,
            reserved_range: vec![],
            reserved_name: vec![],
        };

        let file_descriptor = FileDescriptorProto {
            name: Some("map_test.proto".to_string()),
            package: Some("test".to_string()),
            dependency: vec![],
            public_dependency: vec![],
            weak_dependency: vec![],
            message_type: vec![map_message_descriptor],
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

    /// Encode a `MapMessage` with the given `kv` entries. An empty `entries`
    /// slice yields a message with the `kv` field entirely absent (no tag-1
    /// pairs), exercising the absent-map path.
    fn create_map_test_message(entries: &[(&str, i32)]) -> Vec<u8> {
        use prost::encoding::*;

        let mut buf = Vec::new();
        for (k, v) in entries {
            let mut entry = Vec::new();
            // key (entry field 1, string)
            encode_key(1, WireType::LengthDelimited, &mut entry);
            encode_varint(k.len() as u64, &mut entry);
            entry.extend_from_slice(k.as_bytes());
            // value (entry field 2, int32)
            encode_key(2, WireType::Varint, &mut entry);
            encode_varint(*v as u64, &mut entry);
            // map field 1 (length-delimited entry)
            encode_key(1, WireType::LengthDelimited, &mut buf);
            encode_varint(entry.len() as u64, &mut buf);
            buf.extend_from_slice(&entry);
        }
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

    fn create_repeated_test_message(id: i32, scores: &[i32]) -> Vec<u8> {
        use prost::encoding::*;

        let mut buf = Vec::new();

        encode_key(1, WireType::Varint, &mut buf);
        encode_varint(id as u64, &mut buf);

        for score in scores {
            encode_key(2, WireType::Varint, &mut buf);
            encode_varint(*score as u64, &mut buf);
        }

        buf
    }

    fn create_empty_nested_test_message(name: &str) -> Vec<u8> {
        use prost::encoding::*;

        let mut buf = Vec::new();

        // name (field 1, string) —— present
        encode_key(1, WireType::LengthDelimited, &mut buf);
        encode_varint(name.len() as u64, &mut buf);
        buf.extend_from_slice(name.as_bytes());

        // address (field 2, message) —— present but length 0（空 sub-message）
        encode_key(2, WireType::LengthDelimited, &mut buf);
        encode_varint(0, &mut buf);

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
    fn test_parse_messages_with_repeated_field_all_tags_present() {
        let descriptor_data = create_repeated_test_descriptor();
        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("id", DataType::Int32, true),
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("scores", DataType::Int32, true))),
                true,
            ),
        ]));

        let mut deserializer = PbDeserializer::new(
            descriptor_data,
            "RepeatedMessage",
            schema,
            &HashMap::new(),
            &[],
        )
        .expect("Failed to create deserializer");

        // Key: fill every row with id + scores, so that seen_tags.count_ones() ==
        // total_handlers, triggering O3 to skip the ensure_size path (under the
        // current bug, list row slots are not finalized).
        let messages = create_binary_array(vec![
            create_repeated_test_message(1, &[10, 11]),
            create_repeated_test_message(2, &[20, 21, 22]),
        ]);
        let partitions = create_partition_array(vec![0, 0]);
        let offsets = create_offset_array(vec![100, 101]);
        let timestamps = create_timestamp_array(vec![1000, 1001]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to deserialize repeated message");

        assert_eq!(batch.num_rows(), 2);
        let scores = batch
            .column(4)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("Failed to downcast scores array to ListArray");
        assert_eq!(scores.len(), 2);

        let row0 = scores.value(0);
        let row0_values = row0
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast row0 scores to Int32Array");
        assert_eq!(row0_values.values(), &[10, 11]);

        let row1 = scores.value(1);
        let row1_values = row1
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast row1 scores to Int32Array");
        assert_eq!(row1_values.values(), &[20, 21, 22]);
    }

    #[test]
    fn test_parse_messages_with_empty_struct_message_all_tags_present() {
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

        let mut deserializer =
            PbDeserializer::new(descriptor_data, "Person", schema, &nested_mapping, &[])
                .expect("Failed to create deserializer");

        // Both name and address tags are present (address being an empty sub-message),
        // triggering the empty struct branch + the O3 all-fields-hit path.
        let messages = create_binary_array(vec![
            create_empty_nested_test_message("Alice"),
            create_empty_nested_test_message("Bob"),
        ]);
        let partitions = create_partition_array(vec![0, 0]);
        let offsets = create_offset_array(vec![200, 201]);
        let timestamps = create_timestamp_array(vec![2000, 2001]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to deserialize empty nested message");

        assert_eq!(batch.num_rows(), 2);

        let street = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast street array to StringArray");
        assert_eq!(street.len(), 2);
        // C2: the empty sub-message pads children to align with the struct
        // length. `ensure_output_array_builders_size` pads String children
        // with a non-null default (""), consistent with how absent fields are
        // already handled everywhere else — so street is non-null empty.
        assert_eq!(street.null_count(), 0);
        assert_eq!(street.value(0), "");
        assert_eq!(street.value(1), "");

        let city = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast city array to StringArray");
        assert_eq!(city.len(), 2);
        assert_eq!(city.null_count(), 0);
        assert_eq!(city.value(0), "");
        assert_eq!(city.value(1), "");
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

    /// Pin the row-alignment invariant for a top-level `DataType::Map` column.
    ///
    /// A top-level Map is structurally different from List: the per-row
    /// offset/null slot is finalized only inside `ensure_size` (which runs
    /// every row because `top_level_has_list_or_map` is true), while the
    /// per-entry key/value pushes go to the child builders via
    /// `decode_sub_message`. This test covers both the present (≥1 entry)
    /// and absent (no `kv` tag) cases and asserts `map.len() == num_rows`
    /// plus the per-row entry counts, so a regression where the map column
    /// ever desyncs from the batch row count
    /// (e.g. if `top_level_has_list_or_map` stopped matching `DataType::Map`)
    /// fails loudly instead of silently corrupting offsets.
    #[test]
    fn test_parse_messages_with_top_level_map() {
        let descriptor_data = create_map_test_descriptor();
        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new(
                "kv",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, true),
                            Field::new("value", DataType::Int32, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]));

        let mut deserializer =
            PbDeserializer::new(descriptor_data, "MapMessage", schema, &HashMap::new(), &[])
                .expect("Failed to create deserializer");

        // row0: two entries; row1: map absent (no kv tag at all).
        let messages = create_binary_array(vec![
            create_map_test_message(&[("a", 1), ("b", 2)]),
            create_map_test_message(&[]),
        ]);
        let partitions = create_partition_array(vec![0, 1]);
        let offsets = create_offset_array(vec![10, 20]);
        let timestamps = create_timestamp_array(vec![100, 200]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to deserialize map message");

        assert_eq!(batch.num_rows(), 2);
        let map_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("Failed to downcast kv column to MapArray");

        // Row-alignment invariant: the map column has exactly one slot per row.
        assert_eq!(map_array.len(), 2);

        // row0: present with 2 entries → non-null, offsets span 2 entries.
        assert!(!map_array.is_null(0));
        let row0 = map_array.value(0);
        let row0_entries = row0
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("map entries are a StructArray");
        assert_eq!(row0_entries.len(), 2);
        let row0_keys = row0_entries
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map keys are StringArray");
        let row0_values = row0_entries
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("map values are Int32Array");
        assert_eq!(row0_keys.value(0), "a");
        assert_eq!(row0_values.value(0), 1);
        assert_eq!(row0_keys.value(1), "b");
        assert_eq!(row0_values.value(1), 2);

        // row1: absent map → ensure_size finalizes one non-null slot with
        // 0 entries (current behavior). Pin it so an absent-vs-null change is
        // conscious.
        assert!(!map_array.is_null(1));
        let row1 = map_array.value(1);
        let row1_entries = row1
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("map entries are a StructArray");
        assert_eq!(row1_entries.len(), 0);
    }

    /// Regression test for the #2320 boolean bug, in the specific
    /// "top-level field absent from EVERY row of the batch" shape that the
    /// dropped O10 short-circuit used to mis-handle.
    ///
    /// With O10 present, a column never touched in any row short-circuited to
    /// `new_null_array`, emitting all-NULL — re-introducing #2320 for the
    /// all-absent case (and similarly for int/string/float/binary). With O10
    /// removed, the column falls through to the cast path on a builder that
    /// `ensure_size` filled with the proto3 default `false`, so it is
    /// non-null all-false. This test pins that: if O10 (or an equivalent
    /// all-null short-circuit) is ever re-introduced for a top-level field,
    /// `null_count()` would be 2 and the test would fail.
    #[test]
    fn test_parse_messages_top_level_boolean_absent_in_all_rows() {
        let descriptor_data = create_test_descriptor();
        // Schema exposes only `active` (field 4, bool). The messages below
        // carry only `id` (field 1), so `active` (tag 4) is never decoded.
        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("active", DataType::Boolean, true),
        ]));

        let mut deserializer =
            PbDeserializer::new(descriptor_data, "TestMessage", schema, &HashMap::new(), &[])
                .expect("Failed to create deserializer");

        // Each message encodes ONLY `id` (tag 1); `active` (tag 4) is absent
        // in every row of the batch — the exact shape O10 mishandled.
        let only_id_message = |id: i32| {
            use prost::encoding::*;
            let mut buf = Vec::new();
            encode_key(1, WireType::Varint, &mut buf);
            encode_varint(id as u64, &mut buf);
            buf
        };
        let messages = create_binary_array(vec![only_id_message(1), only_id_message(2)]);
        let partitions = create_partition_array(vec![0, 0]);
        let offsets = create_offset_array(vec![100, 101]);
        let timestamps = create_timestamp_array(vec![1000, 1001]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to deserialize");

        assert_eq!(batch.num_rows(), 2);
        let active_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Failed to downcast active array to BooleanArray");

        // All-absent boolean must be the proto3 default `false`, NON-null —
        // NOT all-NULL (which is the #2320 regression this PR fixes).
        assert_eq!(active_array.null_count(), 0);
        assert!(!active_array.is_null(0));
        assert!(!active_array.is_null(1));
        assert!(!active_array.value(0));
        assert!(!active_array.value(1));
    }
}
