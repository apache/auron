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

use arrow::array::{ArrayBuilder, ArrayRef, NullBufferBuilder, StructArray};
use arrow_schema::Fields;

use crate::flink::serde::shared_array_builder::SharedArrayBuilder;

pub struct SharedStructArrayBuilder {
    fields: Fields,
    field_builders: Vec<SharedArrayBuilder>,
    null_buffer_builder: NullBufferBuilder,
}

impl std::fmt::Debug for SharedStructArrayBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedStructArrayBuilder")
            .field("fields", &self.fields)
            .field("bitmap_builder", &self.null_buffer_builder)
            .field("len", &self.len())
            .finish()
    }
}

impl ArrayBuilder for SharedStructArrayBuilder {
    /// Returns the number of array slots in the builder.
    ///
    /// Note that this always return the first child field builder's length, and
    /// it is the caller's responsibility to maintain the consistency that
    /// all the child field builder should have the equal number of
    /// elements.
    fn len(&self) -> usize {
        self.null_buffer_builder.len()
    }

    /// Builds the array.
    fn finish(&mut self) -> ArrayRef {
        Arc::new(self.finish())
    }

    /// Builds the array without resetting the builder.
    fn finish_cloned(&self) -> ArrayRef {
        Arc::new(self.finish_cloned())
    }

    /// Returns the builder as a non-mutable `Any` reference.
    ///
    /// This is most useful when one wants to call non-mutable APIs on a
    /// specific builder type. In this case, one can first cast this into a
    /// `Any`, and then use `downcast_ref` to get a reference on the
    /// specific builder.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the builder as a mutable `Any` reference.
    ///
    /// This is most useful when one wants to call mutable APIs on a specific
    /// builder type. In this case, one can first cast this into a `Any`,
    /// and then use `downcast_mut` to get a reference on the specific
    /// builder.
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    /// Returns the boxed builder as a box of `Any`.
    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

impl SharedStructArrayBuilder {
    /// Creates a new `SharedStructArrayBuilder`
    pub(crate) fn new(fields: impl Into<Fields>, field_builders: Vec<SharedArrayBuilder>) -> Self {
        let fields: Fields = fields.into();
        Self {
            field_builders,
            fields,
            null_buffer_builder: NullBufferBuilder::new(0),
        }
    }

    pub(crate) fn get_field_builders(&self) -> Vec<SharedArrayBuilder> {
        self.field_builders.clone()
    }

    /// Appends an element (either null or non-null) to the struct. The actual
    /// elements should be appended for each child sub-array in a consistent
    /// way.
    #[inline]
    pub fn append(&mut self, is_valid: bool) {
        self.null_buffer_builder.append(is_valid);
    }

    /// Builds the `StructArray` and reset this builder.
    pub fn finish(&mut self) -> StructArray {
        if self.fields.is_empty() {
            return StructArray::new_empty_fields(self.len(), self.null_buffer_builder.finish());
        }

        let arrays = self
            .field_builders
            .iter_mut()
            .map(|f| f.get_dyn_mut().finish())
            .collect();
        let nulls = self.null_buffer_builder.finish();
        StructArray::new(self.fields.clone(), arrays, nulls)
    }

    /// Builds the `StructArray` without resetting the builder.
    pub fn finish_cloned(&self) -> StructArray {
        if self.fields.is_empty() {
            return StructArray::new_empty_fields(
                self.len(),
                self.null_buffer_builder.finish_cloned(),
            );
        }

        let arrays = self
            .field_builders
            .iter()
            .map(|f| f.get_dyn_mut().finish_cloned())
            .collect();

        let nulls = self.null_buffer_builder.finish_cloned();

        StructArray::new(self.fields.clone(), arrays, nulls)
    }
}
