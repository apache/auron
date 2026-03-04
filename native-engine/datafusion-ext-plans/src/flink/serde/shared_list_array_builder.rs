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
    array::{ArrayBuilder, ArrayRef, BufferBuilder, GenericListArray, NullBufferBuilder},
    buffer::{Buffer, OffsetBuffer},
};
use arrow_schema::{Field, FieldRef};

use crate::flink::serde::{
    pb_deserializer::adaptive_append_children, shared_array_builder::SharedArrayBuilder,
};

pub struct SharedListArrayBuilder {
    offsets_builder: BufferBuilder<i32>,
    null_buffer_builder: NullBufferBuilder,
    values_builder: SharedArrayBuilder,
    field: Option<FieldRef>,
    current_offset: i32,
    adaptive_append_children: Option<Box<dyn FnMut(usize) + Send + Sync>>,
}

impl ArrayBuilder for SharedListArrayBuilder {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }
}

impl SharedListArrayBuilder {
    /// Creates a new [`SharedArrayListBuilder`] from a given values array
    /// builder
    pub(crate) fn new(values_builder: SharedArrayBuilder) -> Self {
        let capacity = values_builder.len();
        let appender = adaptive_append_children(&values_builder);
        Self::with_capacity(values_builder, capacity, appender)
    }

    /// Creates a new [`SharedArrayListBuilder`] with specified capacity
    pub(crate) fn with_capacity(
        values_builder: SharedArrayBuilder,
        capacity: usize,
        adaptive_append_children: Option<Box<dyn FnMut(usize) + Send + Sync>>,
    ) -> Self {
        let mut offsets_builder = BufferBuilder::<i32>::new(capacity + 1);
        offsets_builder.append(0);
        Self {
            offsets_builder,
            null_buffer_builder: NullBufferBuilder::new(capacity),
            values_builder,
            field: None,
            current_offset: 0,
            adaptive_append_children,
        }
    }

    /// Returns the child array builder as a reference
    pub(crate) fn values(&mut self) -> &SharedArrayBuilder {
        &self.values_builder
    }

    /// Finish the current variable-length list array slot
    #[inline]
    pub fn append(&mut self, is_valid: bool) {
        if let Some(adaptive_append_children) = self.adaptive_append_children.as_mut() {
            adaptive_append_children(self.values_builder.len());
        }
        self.offsets_builder
            .append(self.values_builder.len() as i32);
        self.null_buffer_builder.append(is_valid);
    }

    pub fn adaptive_append(&mut self) {
        let next_offset = self.values_builder.len() as i32;
        if next_offset > self.current_offset {
            self.append(true);
            self.current_offset = next_offset;
        }
    }

    /// Builds the [`GenericListArray`] and reset this builder
    pub fn finish(&mut self) -> GenericListArray<i32> {
        let values = self.values_builder.get_dyn_mut().finish();
        let nulls = self.null_buffer_builder.finish();

        let offsets = self.offsets_builder.finish();
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };
        self.offsets_builder.append(0);
        self.current_offset = 0;

        let field = match &self.field {
            Some(f) => f.clone(),
            None => Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        };

        GenericListArray::new(field, offsets, values, nulls)
    }

    /// Builds the [`GenericListArray`] without resetting the builder
    pub fn finish_cloned(&self) -> GenericListArray<i32> {
        let values = self.values_builder.get_dyn_mut().finish_cloned();
        let nulls = self.null_buffer_builder.finish_cloned();

        let offsets = Buffer::from_slice_ref(self.offsets_builder.as_slice());
        let offsets = unsafe { OffsetBuffer::new_unchecked(offsets.into()) };

        let field = match &self.field {
            Some(f) => f.clone(),
            None => Arc::new(Field::new_list_field(values.data_type().clone(), true)),
        };

        GenericListArray::new(field, offsets, values, nulls)
    }
}
