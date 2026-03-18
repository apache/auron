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

use std::{any::Any, sync::Arc};

use arrow::{
    array::{
        Array, ArrayBuilder, ArrayData, ArrayRef, BufferBuilder, MapArray, NullBufferBuilder,
        StructArray,
    },
    buffer::{Buffer, NullBuffer},
};
use arrow_schema::{DataType, Field, FieldRef};

use crate::flink::serde::{
    pb_deserializer::adaptive_append_children, shared_array_builder::SharedArrayBuilder,
};

pub struct SharedMapArrayBuilder {
    offsets_builder: BufferBuilder<i32>,
    null_buffer_builder: NullBufferBuilder,
    field_names: MapFieldNames,
    key_builder: SharedArrayBuilder,
    value_builder: SharedArrayBuilder,
    value_field: Option<FieldRef>,
    current_offset: i32,
    adaptive_append_children: Option<Box<dyn FnMut(usize) + Send + Sync>>,
}

/// The [`Field`] names for a [`MapArray`]
#[derive(Debug, Clone)]
pub struct MapFieldNames {
    /// [`Field`] name for map entries
    pub entry: String,
    /// [`Field`] name for map key
    pub key: String,
    /// [`Field`] name for map value
    pub value: String,
}

impl Default for MapFieldNames {
    fn default() -> Self {
        Self {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }
    }
}

impl SharedMapArrayBuilder {
    /// Creates a new `MapBuilder`
    pub(crate) fn new(
        field_names: Option<MapFieldNames>,
        key_builder: SharedArrayBuilder,
        value_builder: SharedArrayBuilder,
    ) -> Self {
        let capacity = key_builder.len();

        let mut appenders = vec![];
        appenders.extend(adaptive_append_children(&key_builder));
        appenders.extend(adaptive_append_children(&value_builder));
        let appender = match appenders.len() {
            1 => {
                let mut appender = appenders
                    .pop()
                    .expect("appenders length is 1, pop should succeed");
                Some(Box::new(move |size| appender(size)) as Box<dyn FnMut(usize) + Send + Sync>)
            }
            2 => {
                let mut appender2 = appenders
                    .pop()
                    .expect("appenders length is 2, first pop should succeed");
                let mut appender1 = appenders
                    .pop()
                    .expect("appenders length is 2, second pop should succeed");
                Some(Box::new(move |size| {
                    appender1(size);
                    appender2(size);
                }) as Box<dyn FnMut(usize) + Send + Sync>)
            }
            _ => None,
        };
        Self::with_capacity(field_names, key_builder, value_builder, capacity, appender)
    }

    /// Creates a new `MapBuilder` with specified capacity
    pub(crate) fn with_capacity(
        field_names: Option<MapFieldNames>,
        key_builder: SharedArrayBuilder,
        value_builder: SharedArrayBuilder,
        capacity: usize,
        adaptive_append_children: Option<Box<dyn FnMut(usize) + Send + Sync>>,
    ) -> Self {
        let mut offsets_builder = BufferBuilder::<i32>::new(capacity + 1);
        offsets_builder.append(0);
        Self {
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
            field_names: field_names.unwrap_or_default(),
            key_builder,
            value_builder,
            value_field: None,
            current_offset: 0,
            adaptive_append_children,
        }
    }

    /// Returns the key array builder of the map
    #[allow(dead_code)]
    pub(crate) fn keys(&mut self) -> &mut SharedArrayBuilder {
        &mut self.key_builder
    }

    /// Returns the value array builder of the map
    #[allow(dead_code)]
    pub(crate) fn values(&mut self) -> &mut SharedArrayBuilder {
        &mut self.value_builder
    }

    /// Returns both the key and value array builders of the map
    pub(crate) fn entries(&mut self) -> (&mut SharedArrayBuilder, &mut SharedArrayBuilder) {
        (&mut self.key_builder, &mut self.value_builder)
    }

    /// Finish the current map array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) {
        if let Some(adaptive_append_children) = self.adaptive_append_children.as_mut() {
            adaptive_append_children(self.key_builder.len());
        }
        self.offsets_builder.append(self.key_builder.len() as i32);
        self.null_buffer_builder.append(is_valid);
    }

    pub fn adaptive_append(&mut self) {
        let next_offset = self.key_builder.len() as i32;
        if next_offset > self.current_offset {
            self.append(true);
            self.current_offset = next_offset;
        }
    }

    /// Builds the [`MapArray`]
    pub fn finish(&mut self) -> MapArray {
        let len = self.len();
        let keys_arr = self.key_builder.get_dyn_mut().finish();
        let values_arr = self.value_builder.get_dyn_mut().finish();
        let offset_buffer = self.offsets_builder.finish();
        self.offsets_builder.append(0);
        self.current_offset = 0;
        let null_bit_buffer = self.null_buffer_builder.finish();

        self.finish_helper(keys_arr, values_arr, offset_buffer, null_bit_buffer, len)
    }

    /// Builds the [`MapArray`] without resetting the builder
    pub fn finish_cloned(&self) -> MapArray {
        let len = self.len();
        let keys_arr = self.key_builder.get_dyn_mut().finish_cloned();
        let values_arr = self.value_builder.get_dyn_mut().finish_cloned();
        let offset_buffer = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        let nulls = self.null_buffer_builder.finish_cloned();
        self.finish_helper(keys_arr, values_arr, offset_buffer, nulls, len)
    }

    fn finish_helper(
        &self,
        keys_arr: Arc<dyn Array>,
        values_arr: Arc<dyn Array>,
        offset_buffer: Buffer,
        nulls: Option<NullBuffer>,
        len: usize,
    ) -> MapArray {
        let keys_field = Arc::new(Field::new(
            self.field_names.key.as_str(),
            keys_arr.data_type().clone(),
            true,
        ));
        let values_field = match &self.value_field {
            Some(f) => f.clone(),
            None => Arc::new(Field::new(
                self.field_names.value.as_str(),
                values_arr.data_type().clone(),
                true,
            )),
        };

        let struct_array =
            StructArray::from(vec![(keys_field, keys_arr), (values_field, values_arr)]);

        let map_field = Arc::new(Field::new(
            self.field_names.entry.as_str(),
            struct_array.data_type().clone(),
            false,
        ));
        let array_data = ArrayData::builder(DataType::Map(map_field, false))
            .len(len)
            .add_buffer(offset_buffer)
            .add_child_data(struct_array.into_data())
            .nulls(nulls);

        let array_data = unsafe { array_data.build_unchecked() };

        MapArray::from(array_data)
    }
}

impl ArrayBuilder for SharedMapArrayBuilder {
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
