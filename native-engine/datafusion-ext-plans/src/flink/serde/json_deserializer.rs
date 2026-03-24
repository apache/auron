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

use std::{collections::HashMap, sync::Arc};

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanBuilder, Float32Builder, Float64Builder,
    Int32Array, Int32Builder, Int64Array, Int64Builder, RecordBatch, RecordBatchOptions,
    StringBuilder, StructArray, TimestampMillisecondBuilder, UInt32Builder, UInt64Builder,
    new_null_array,
};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::error::{DataFusionError, Result};
use datafusion_ext_commons::{df_execution_err, downcast_any};
use sonic_rs::{JsonContainerTrait, JsonValueTrait, Value};

use crate::flink::serde::{
    flink_deserializer::FlinkDeserializer, pb_deserializer::ensure_output_array_builders_size,
    shared_array_builder::SharedArrayBuilder, shared_list_array_builder::SharedListArrayBuilder,
    shared_map_array_builder::SharedMapArrayBuilder,
    shared_struct_array_builder::SharedStructArrayBuilder,
};

type ValueHandler = Box<dyn Fn(&Value) -> Result<()> + Send>;

pub struct JsonDeserializer {
    output_schema: SchemaRef,
    output_schema_without_meta: SchemaRef,
    json_schema: SchemaRef,
    output_array_builders: Vec<SharedArrayBuilder>,
    ensure_size: Box<dyn FnMut(usize) + Send>,
    value_handlers: Vec<(String, ValueHandler)>,
    msg_mapping: Vec<Vec<usize>>,
}

impl FlinkDeserializer for JsonDeserializer {
    fn parse_messages_with_kafka_meta(
        &mut self,
        messages: &BinaryArray,
        kafka_partition: &Int32Array,
        kafka_offset: &Int64Array,
        kafka_timestamp: &Int64Array,
    ) -> datafusion::common::Result<RecordBatch> {
        for (row_idx, msg_bytes) in messages.iter().enumerate() {
            let msg = msg_bytes.expect("message bytes must not be null");
            let json_value: Value = sonic_rs::from_slice(msg).map_err(|e| {
                DataFusionError::Execution(format!("Failed to parse JSON message: {e}"))
            })?;

            if let Some(obj) = json_value.as_object() {
                for (field_name, handler) in &self.value_handlers {
                    if let Some(value) = obj.get(field_name) {
                        handler(value)?;
                    }
                }
            }

            let ensure_size = &mut self.ensure_size;
            ensure_size(row_idx + 1);
        }

        let root_struct = StructArray::from({
            RecordBatch::try_new_with_options(
                self.json_schema.clone(),
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
                output_arrays.push(datafusion_ext_commons::arrow::cast::cast(
                    &array_ref,
                    field.data_type(),
                )?);
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

impl JsonDeserializer {
    pub fn new(
        output_schema: SchemaRef,
        nested_msg_mapping: &HashMap<String, String>,
    ) -> Result<Self> {
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

        let json_schema =
            transfer_output_schema_to_json_schema(&output_schema_without_meta, nested_msg_mapping)?;

        let output_array_builders = create_output_array_builders(&json_schema)?;
        let ensure_size = ensure_output_array_builders_size(&output_array_builders)?;

        let value_handlers = create_value_handlers(&json_schema, &output_array_builders)?;

        let msg_mapping = output_schema_without_meta
            .fields()
            .iter()
            .map(|field| {
                let mut mapped_field_indices = vec![];
                let mut cur_fields = json_schema.fields();
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
                            _ => return df_execution_err!("nested field not found in json schema"),
                        };
                    }
                    if let Some((idx, _)) = cur_fields.find(nested_fields[nested_fields.len() - 1])
                    {
                        mapped_field_indices.push(idx);
                    } else {
                        return df_execution_err!("field not found in json schema");
                    }
                } else if let Ok(idx) = json_schema.index_of(field.name()) {
                    mapped_field_indices.push(idx);
                } else {
                    return df_execution_err!("field not found in json schema");
                }
                Ok(mapped_field_indices)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            output_schema,
            output_schema_without_meta,
            json_schema,
            output_array_builders,
            ensure_size,
            value_handlers,
            msg_mapping,
        })
    }
}

/// Build the internal json_schema from the output schema and
/// nested_msg_mapping. For non-nested fields, the field is copied as-is.
/// For nested fields (e.g. "address.street"), we reconstruct the struct
/// hierarchy.
fn transfer_output_schema_to_json_schema(
    output_schema: &SchemaRef,
    nested_msg_mapping: &HashMap<String, String>,
) -> Result<SchemaRef> {
    let mut json_schema_fields: Vec<Field> = vec![];
    let mut sub_nested_mapping: HashMap<String, String> = HashMap::new();
    let mut sub_schema_mapping: HashMap<String, Vec<Field>> = HashMap::new();

    for field in output_schema.fields().iter() {
        if let Some(json_path) = nested_msg_mapping.get(field.name()) {
            if let Some(index) = json_path.find(".") {
                sub_nested_mapping.insert(
                    field.name().to_string(),
                    json_path[(index + 1)..].to_string(),
                );
                sub_schema_mapping
                    .entry(json_path[..index].to_string())
                    .and_modify(|v| {
                        v.push(field.as_ref().clone());
                    })
                    .or_insert(vec![field.as_ref().clone()]);
            }
        }
    }

    let mut seen_parents: std::collections::HashSet<String> = std::collections::HashSet::new();
    for field in output_schema.fields().iter() {
        if let Some(json_path) = nested_msg_mapping.get(field.name()) {
            if let Some(index) = json_path.find(".") {
                let parent_field_name = &json_path[..index];
                if !seen_parents.contains(parent_field_name) {
                    let sub_fields = sub_schema_mapping
                        .get(parent_field_name)
                        .ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "Field {parent_field_name} not found in sub_schema_mapping"
                            ))
                        })?
                        .clone();
                    let sub_schema = transfer_output_schema_to_json_schema(
                        &Arc::new(Schema::new(sub_fields)),
                        &sub_nested_mapping,
                    )?;
                    json_schema_fields.push(Field::new(
                        parent_field_name,
                        DataType::Struct(sub_schema.fields.clone()),
                        true,
                    ));
                    seen_parents.insert(parent_field_name.to_string());
                }
            } else {
                // innermost field mapped directly
                json_schema_fields.push(field.as_ref().clone());
            }
        } else {
            json_schema_fields.push(field.as_ref().clone());
        }
    }
    Ok(Arc::new(Schema::new(json_schema_fields)))
}

fn create_output_array_builders(schema: &SchemaRef) -> Result<Vec<SharedArrayBuilder>> {
    let mut array_builders: Vec<SharedArrayBuilder> = vec![];
    for field in schema.fields() {
        array_builders.push(create_shared_array_builder_by_data_type(field.data_type())?);
    }
    Ok(array_builders)
}

fn create_shared_array_builder_by_data_type(data_type: &DataType) -> Result<SharedArrayBuilder> {
    match data_type {
        DataType::Boolean => Ok(SharedArrayBuilder::new(BooleanBuilder::new())),
        DataType::Int32 => Ok(SharedArrayBuilder::new(Int32Builder::new())),
        DataType::Int64 => Ok(SharedArrayBuilder::new(Int64Builder::new())),
        DataType::UInt32 => Ok(SharedArrayBuilder::new(UInt32Builder::new())),
        DataType::UInt64 => Ok(SharedArrayBuilder::new(UInt64Builder::new())),
        DataType::Float32 => Ok(SharedArrayBuilder::new(Float32Builder::new())),
        DataType::Float64 => Ok(SharedArrayBuilder::new(Float64Builder::new())),
        DataType::Utf8 => Ok(SharedArrayBuilder::new(StringBuilder::new())),
        DataType::Binary => Ok(SharedArrayBuilder::new(BinaryBuilder::new())),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(SharedArrayBuilder::new(TimestampMillisecondBuilder::new()))
        }
        DataType::Struct(fields) => {
            let sub_schema = Arc::new(Schema::new(fields.clone()));
            let struct_builders = create_output_array_builders(&sub_schema)?;
            Ok(SharedArrayBuilder::new(SharedStructArrayBuilder::new(
                fields.clone(),
                struct_builders,
            )))
        }
        DataType::List(field_ref) => {
            let values_builder = create_shared_array_builder_by_data_type(field_ref.data_type())?;
            Ok(SharedArrayBuilder::new(SharedListArrayBuilder::new(
                values_builder,
            )))
        }
        DataType::Map(field_ref, _) => {
            if let DataType::Struct(fields) = field_ref.data_type() {
                let key_builder = create_shared_array_builder_by_data_type(
                    fields.get(0).expect("map must have key field").data_type(),
                )?;
                let value_builder = create_shared_array_builder_by_data_type(
                    fields
                        .get(1)
                        .expect("map must have value field")
                        .data_type(),
                )?;
                Ok(SharedArrayBuilder::new(SharedMapArrayBuilder::new(
                    None,
                    key_builder,
                    value_builder,
                )))
            } else {
                df_execution_err!("Map DataType: unsupported non-struct data type: {field_ref:?}")
            }
        }
        other => df_execution_err!("Unsupported data type for JSON conversion: {other:?}"),
    }
}

/// Create value handlers for each top-level field in the json_schema.
/// Each handler knows how to write a sonic_rs::Value into the corresponding
/// array builder.
fn create_value_handlers(
    json_schema: &SchemaRef,
    output_array_builders: &[SharedArrayBuilder],
) -> Result<Vec<(String, ValueHandler)>> {
    let mut handlers = Vec::new();
    for (idx, field) in json_schema.fields().iter().enumerate() {
        let handler = create_value_handler_for_field(field, &output_array_builders[idx])?;
        handlers.push((field.name().clone(), handler));
    }
    Ok(handlers)
}

fn create_value_handler_for_field(
    field: &Field,
    output_array_builder: &SharedArrayBuilder,
) -> Result<ValueHandler> {
    match field.data_type() {
        DataType::Boolean => {
            let builder = output_array_builder.get_mut::<BooleanBuilder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_null();
                } else if let Some(b) = value.as_bool() {
                    builder.get_mut().append_value(b);
                } else {
                    builder.get_mut().append_null();
                }
                Ok(())
            }))
        }
        DataType::Int32 => {
            let builder = output_array_builder.get_mut::<Int32Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_value(0);
                } else if let Some(n) = value.as_i64() {
                    builder.get_mut().append_value(n as i32);
                } else {
                    builder.get_mut().append_value(0);
                }
                Ok(())
            }))
        }
        DataType::Int64 => {
            let builder = output_array_builder.get_mut::<Int64Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_value(0);
                } else if let Some(n) = value.as_i64() {
                    builder.get_mut().append_value(n);
                } else {
                    builder.get_mut().append_value(0);
                }
                Ok(())
            }))
        }
        DataType::UInt32 => {
            let builder = output_array_builder.get_mut::<UInt32Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_value(0);
                } else if let Some(n) = value.as_u64() {
                    builder.get_mut().append_value(n as u32);
                } else {
                    builder.get_mut().append_value(0);
                }
                Ok(())
            }))
        }
        DataType::UInt64 => {
            let builder = output_array_builder.get_mut::<UInt64Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_value(0);
                } else if let Some(n) = value.as_u64() {
                    builder.get_mut().append_value(n);
                } else {
                    builder.get_mut().append_value(0);
                }
                Ok(())
            }))
        }
        DataType::Float32 => {
            let builder = output_array_builder.get_mut::<Float32Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_value(0.0);
                } else if let Some(n) = value.as_f64() {
                    builder.get_mut().append_value(n as f32);
                } else {
                    builder.get_mut().append_value(0.0);
                }
                Ok(())
            }))
        }
        DataType::Float64 => {
            let builder = output_array_builder.get_mut::<Float64Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_value(0.0);
                } else if let Some(n) = value.as_f64() {
                    builder.get_mut().append_value(n);
                } else {
                    builder.get_mut().append_value(0.0);
                }
                Ok(())
            }))
        }
        DataType::Utf8 => {
            let builder = output_array_builder.get_mut::<StringBuilder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_value("");
                } else if let Some(s) = value.as_str() {
                    builder.get_mut().append_value(s);
                } else {
                    // For non-string JSON values, serialize them as string
                    let s = sonic_rs::to_string(value).unwrap_or_default();
                    builder.get_mut().append_value(&s);
                }
                Ok(())
            }))
        }
        DataType::Binary => {
            let builder = output_array_builder.get_mut::<BinaryBuilder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_value(b"");
                } else if let Some(s) = value.as_str() {
                    builder.get_mut().append_value(s.as_bytes());
                } else {
                    builder.get_mut().append_value(b"");
                }
                Ok(())
            }))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let builder = output_array_builder.get_mut::<TimestampMillisecondBuilder>()?;
            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    builder.get_mut().append_null();
                } else if let Some(n) = value.as_i64() {
                    builder.get_mut().append_value(n);
                } else {
                    builder.get_mut().append_null();
                }
                Ok(())
            }))
        }
        DataType::Struct(sub_fields) => {
            let sub_schema = Arc::new(Schema::new(sub_fields.clone()));
            let sub_builders = output_array_builder
                .get_mut::<SharedStructArrayBuilder>()
                .expect("SharedStructArrayBuilder is null")
                .get_mut()
                .get_field_builders();
            let mut sub_handlers = Vec::new();
            for (idx, sub_field) in sub_schema.fields().iter().enumerate() {
                let handler = create_value_handler_for_field(sub_field, &sub_builders[idx])?;
                sub_handlers.push((sub_field.name().clone(), handler));
            }
            let struct_builder = output_array_builder
                .get_mut::<SharedStructArrayBuilder>()
                .expect("SharedStructArrayBuilder is null");

            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    struct_builder.get_mut().append(false);
                } else if let Some(obj) = value.as_object() {
                    for (field_name, handler) in &sub_handlers {
                        if let Some(v) = obj.get(field_name) {
                            handler(v)?;
                        }
                    }
                    struct_builder.get_mut().append(true);
                } else {
                    struct_builder.get_mut().append(false);
                }
                Ok(())
            }))
        }
        DataType::List(item_field) => {
            let list_builder = output_array_builder
                .get_mut::<SharedListArrayBuilder>()
                .expect("SharedListArrayBuilder is null");
            let values_builder = list_builder.get_mut().values().clone();
            let item_handler =
                create_value_handler_for_item(item_field.data_type(), &values_builder)?;

            Ok(Box::new(move |value: &Value| {
                if value.is_null() {
                    list_builder.get_mut().append(true);
                } else if let Some(arr) = value.as_array() {
                    for item in arr.iter() {
                        item_handler(item)?;
                    }
                    list_builder.get_mut().append(true);
                } else {
                    list_builder.get_mut().append(true);
                }
                Ok(())
            }))
        }
        DataType::Map(entries_field, _) => {
            if let DataType::Struct(kv_fields) = entries_field.data_type() {
                let map_builder = output_array_builder
                    .get_mut::<SharedMapArrayBuilder>()
                    .expect("SharedMapArrayBuilder is null");
                let (key_builder, value_builder) = map_builder.get_mut().entries();
                let key_builder = key_builder.clone();
                let value_builder = value_builder.clone();
                let key_handler = create_value_handler_for_item(
                    kv_fields.get(0).expect("map must have key").data_type(),
                    &key_builder,
                )?;
                let value_handler = create_value_handler_for_item(
                    kv_fields.get(1).expect("map must have value").data_type(),
                    &value_builder,
                )?;

                Ok(Box::new(move |value: &Value| {
                    if value.is_null() {
                        map_builder.get_mut().append(true);
                    } else if let Some(obj) = value.as_object() {
                        for (k, v) in obj.iter() {
                            // Map keys in JSON are always strings
                            let key_value: Value =
                                sonic_rs::from_str(&format!("\"{k}\"")).unwrap_or_default();
                            key_handler(&key_value)?;
                            value_handler(v)?;
                        }
                        map_builder.get_mut().append(true);
                    } else {
                        map_builder.get_mut().append(true);
                    }
                    Ok(())
                }))
            } else {
                df_execution_err!("Map DataType: unsupported non-struct entry type")
            }
        }
        other => df_execution_err!("Unsupported data type for JSON value handler: {other:?}"),
    }
}

/// Create a handler for writing a single JSON value to a SharedArrayBuilder,
/// used for list items and map key/value entries.
fn create_value_handler_for_item(
    data_type: &DataType,
    builder: &SharedArrayBuilder,
) -> Result<ValueHandler> {
    match data_type {
        DataType::Boolean => {
            let b = builder.get_mut::<BooleanBuilder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(v) = value.as_bool() {
                    b.get_mut().append_value(v);
                } else {
                    b.get_mut().append_null();
                }
                Ok(())
            }))
        }
        DataType::Int32 => {
            let b = builder.get_mut::<Int32Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(v) = value.as_i64() {
                    b.get_mut().append_value(v as i32);
                } else {
                    b.get_mut().append_value(0);
                }
                Ok(())
            }))
        }
        DataType::Int64 => {
            let b = builder.get_mut::<Int64Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(v) = value.as_i64() {
                    b.get_mut().append_value(v);
                } else {
                    b.get_mut().append_value(0);
                }
                Ok(())
            }))
        }
        DataType::UInt32 => {
            let b = builder.get_mut::<UInt32Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(v) = value.as_u64() {
                    b.get_mut().append_value(v as u32);
                } else {
                    b.get_mut().append_value(0);
                }
                Ok(())
            }))
        }
        DataType::UInt64 => {
            let b = builder.get_mut::<UInt64Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(v) = value.as_u64() {
                    b.get_mut().append_value(v);
                } else {
                    b.get_mut().append_value(0);
                }
                Ok(())
            }))
        }
        DataType::Float32 => {
            let b = builder.get_mut::<Float32Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(v) = value.as_f64() {
                    b.get_mut().append_value(v as f32);
                } else {
                    b.get_mut().append_value(0.0);
                }
                Ok(())
            }))
        }
        DataType::Float64 => {
            let b = builder.get_mut::<Float64Builder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(v) = value.as_f64() {
                    b.get_mut().append_value(v);
                } else {
                    b.get_mut().append_value(0.0);
                }
                Ok(())
            }))
        }
        DataType::Utf8 => {
            let b = builder.get_mut::<StringBuilder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(s) = value.as_str() {
                    b.get_mut().append_value(s);
                } else if value.is_null() {
                    b.get_mut().append_value("");
                } else {
                    let s = sonic_rs::to_string(value).unwrap_or_default();
                    b.get_mut().append_value(&s);
                }
                Ok(())
            }))
        }
        DataType::Binary => {
            let b = builder.get_mut::<BinaryBuilder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(s) = value.as_str() {
                    b.get_mut().append_value(s.as_bytes());
                } else {
                    b.get_mut().append_value(b"");
                }
                Ok(())
            }))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let b = builder.get_mut::<TimestampMillisecondBuilder>()?;
            Ok(Box::new(move |value: &Value| {
                if let Some(v) = value.as_i64() {
                    b.get_mut().append_value(v);
                } else {
                    b.get_mut().append_null();
                }
                Ok(())
            }))
        }
        DataType::Struct(sub_fields) => {
            let sub_schema = Arc::new(Schema::new(sub_fields.clone()));
            let sub_builders = builder
                .get_mut::<SharedStructArrayBuilder>()
                .expect("SharedStructArrayBuilder is null")
                .get_mut()
                .get_field_builders();
            let mut sub_handlers = Vec::new();
            for (idx, sub_field) in sub_schema.fields().iter().enumerate() {
                let handler =
                    create_value_handler_for_item(sub_field.data_type(), &sub_builders[idx])?;
                sub_handlers.push((sub_field.name().clone(), handler));
            }
            let struct_builder = builder
                .get_mut::<SharedStructArrayBuilder>()
                .expect("SharedStructArrayBuilder is null");

            Ok(Box::new(move |value: &Value| {
                if let Some(obj) = value.as_object() {
                    for (field_name, handler) in &sub_handlers {
                        if let Some(v) = obj.get(field_name) {
                            handler(v)?;
                        }
                    }
                    struct_builder.get_mut().append(true);
                } else {
                    struct_builder.get_mut().append(false);
                }
                Ok(())
            }))
        }
        DataType::List(item_field) => {
            let list_builder = builder
                .get_mut::<SharedListArrayBuilder>()
                .expect("SharedListArrayBuilder is null");
            let values_builder = list_builder.get_mut().values().clone();
            let item_handler =
                create_value_handler_for_item(item_field.data_type(), &values_builder)?;

            Ok(Box::new(move |value: &Value| {
                if let Some(arr) = value.as_array() {
                    for item in arr.iter() {
                        item_handler(item)?;
                    }
                    list_builder.get_mut().append(true);
                } else {
                    list_builder.get_mut().append(true);
                }
                Ok(())
            }))
        }
        DataType::Map(entries_field, _) => {
            if let DataType::Struct(kv_fields) = entries_field.data_type() {
                let map_builder = builder
                    .get_mut::<SharedMapArrayBuilder>()
                    .expect("SharedMapArrayBuilder is null");
                let (key_b, value_b) = map_builder.get_mut().entries();
                let key_b = key_b.clone();
                let value_b = value_b.clone();
                let key_handler = create_value_handler_for_item(
                    kv_fields.get(0).expect("map must have key").data_type(),
                    &key_b,
                )?;
                let value_handler = create_value_handler_for_item(
                    kv_fields.get(1).expect("map must have value").data_type(),
                    &value_b,
                )?;

                Ok(Box::new(move |value: &Value| {
                    if let Some(obj) = value.as_object() {
                        for (k, v) in obj.iter() {
                            let key_value: Value =
                                sonic_rs::from_str(&format!("\"{k}\"")).unwrap_or_default();
                            key_handler(&key_value)?;
                            value_handler(v)?;
                        }
                        map_builder.get_mut().append(true);
                    } else {
                        map_builder.get_mut().append(true);
                    }
                    Ok(())
                }))
            } else {
                df_execution_err!("Map DataType: unsupported non-struct entry type")
            }
        }
        other => df_execution_err!("Unsupported data type for JSON item handler: {other:?}"),
    }
}

fn get_output_array(struct_array: &StructArray, nested_field_name: &[usize]) -> Result<ArrayRef> {
    let column = struct_array.column(nested_field_name[0]);
    if nested_field_name.len() > 1 {
        return get_output_array(downcast_any!(column, StructArray)?, &nested_field_name[1..]);
    }
    Ok(column.clone())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::{
        array::*,
        datatypes::{DataType, Field, Schema},
    };

    use super::*;

    fn create_binary_array(messages: Vec<&[u8]>) -> BinaryArray {
        let mut builder = BinaryBuilder::new();
        for msg in messages {
            builder.append_value(msg);
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
    fn test_parse_basic_json_messages() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
        ]));

        let nested_mapping = HashMap::new();
        let mut deserializer = JsonDeserializer::new(schema.clone(), &nested_mapping)
            .expect("Failed to create JsonDeserializer");

        let msg1 = br#"{"id": 1, "name": "Alice", "score": 95.5, "active": true}"#;
        let msg2 = br#"{"id": 2, "name": "Bob", "score": 87.3, "active": false}"#;

        let messages = create_binary_array(vec![msg1.as_ref(), msg2.as_ref()]);
        let partitions = create_partition_array(vec![0, 0]);
        let offsets = create_offset_array(vec![100, 101]);
        let timestamps = create_timestamp_array(vec![1000, 1001]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to parse messages");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7);

        let id_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);

        let name_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");

        let score_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("score column");
        assert_eq!(score_col.value(0), 95.5);
        assert_eq!(score_col.value(1), 87.3);

        let active_col = batch
            .column(6)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("active column");
        assert!(active_col.value(0));
        assert!(!active_col.value(1));
    }

    #[test]
    fn test_parse_nested_json_messages() {
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

        let mut deserializer = JsonDeserializer::new(schema.clone(), &nested_mapping)
            .expect("Failed to create JsonDeserializer");

        let msg1 =
            br#"{"name": "Alice", "address": {"street": "123 Main St", "city": "Springfield"}}"#;
        let msg2 =
            br#"{"name": "Bob", "address": {"street": "456 Oak Ave", "city": "Shelbyville"}}"#;

        let messages = create_binary_array(vec![msg1.as_ref(), msg2.as_ref()]);
        let partitions = create_partition_array(vec![0, 0]);
        let offsets = create_offset_array(vec![100, 101]);
        let timestamps = create_timestamp_array(vec![1000, 1001]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to parse messages");

        assert_eq!(batch.num_rows(), 2);

        let name_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");

        let street_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("street column");
        assert_eq!(street_col.value(0), "123 Main St");
        assert_eq!(street_col.value(1), "456 Oak Ave");

        let city_col = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("city column");
        assert_eq!(city_col.value(0), "Springfield");
        assert_eq!(city_col.value(1), "Shelbyville");
    }

    #[test]
    fn test_parse_json_with_list() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new(
                "scores",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ]));

        let nested_mapping = HashMap::new();
        let mut deserializer = JsonDeserializer::new(schema.clone(), &nested_mapping)
            .expect("Failed to create JsonDeserializer");

        let msg1 = br#"{"name": "Alice", "scores": [90, 85, 95]}"#;
        let msg2 = br#"{"name": "Bob", "scores": [70, 80]}"#;

        let messages = create_binary_array(vec![msg1.as_ref(), msg2.as_ref()]);
        let partitions = create_partition_array(vec![0, 0]);
        let offsets = create_offset_array(vec![100, 101]);
        let timestamps = create_timestamp_array(vec![1000, 1001]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to parse messages");

        assert_eq!(batch.num_rows(), 2);

        let scores_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("scores column");

        let values = scores_col
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 values")
            .clone();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 90);
        assert_eq!(values.value(1), 85);
        assert_eq!(values.value(2), 95);

        let values = scores_col
            .value(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("int32 values")
            .clone();
        assert_eq!(values.len(), 2);
        assert_eq!(values.value(0), 70);
        assert_eq!(values.value(1), 80);
    }

    #[test]
    fn test_parse_json_with_missing_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("serialized_kafka_records_partition", DataType::Int32, false),
            Field::new("serialized_kafka_records_offset", DataType::Int64, false),
            Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ]));

        let nested_mapping = HashMap::new();
        let mut deserializer = JsonDeserializer::new(schema.clone(), &nested_mapping)
            .expect("Failed to create JsonDeserializer");

        // msg1 has both fields, msg2 only has id
        let msg1 = br#"{"id": 1, "name": "Alice"}"#;
        let msg2 = br#"{"id": 2}"#;

        let messages = create_binary_array(vec![msg1.as_ref(), msg2.as_ref()]);
        let partitions = create_partition_array(vec![0, 0]);
        let offsets = create_offset_array(vec![100, 101]);
        let timestamps = create_timestamp_array(vec![1000, 1001]);

        let batch = deserializer
            .parse_messages_with_kafka_meta(&messages, &partitions, &offsets, &timestamps)
            .expect("Failed to parse messages");

        assert_eq!(batch.num_rows(), 2);

        let id_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id column");
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);

        let name_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        assert_eq!(name_col.value(0), "Alice");
        // msg2 missing "name" field, should get default empty string from ensure_size
        assert_eq!(name_col.value(1), "");
    }
}
