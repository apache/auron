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

use std::ops::Range;

use arrow::{
    array::{Array, ArrayData, AsArray, StructArray},
    buffer::Buffer,
    datatypes::{ArrowNativeType, DataType},
    record_batch::RecordBatch,
};

// NOTE:
// the official Array::get_array_memory_size() use buffer.capacity()
// which does not work on ffi arrays. we would like to use .len()
// instead for more precise memory statistics.
pub trait ArraySize {
    fn get_array_mem_size(&self) -> usize;
}

impl<T: ?Sized + Array> ArraySize for T {
    fn get_array_mem_size(&self) -> usize {
        get_array_data_mem_size(&self.to_data())
    }
}

pub trait BatchSize {
    fn get_batch_mem_size(&self) -> usize;
}

impl BatchSize for RecordBatch {
    fn get_batch_mem_size(&self) -> usize {
        let as_struct = StructArray::from(self.clone());
        let as_dyn_array: &dyn Array = &as_struct;
        as_dyn_array.get_array_mem_size()
    }
}

fn get_array_data_mem_size(array_data: &ArrayData) -> usize {
    let mut mem_size = 0;

    for buffer in array_data.buffers() {
        mem_size += size_of::<Buffer>() + buffer.len().max(buffer.capacity());
    }

    mem_size += size_of::<Option<Buffer>>();
    mem_size += array_data
        .nulls()
        .map(|nb| nb.buffer().len().max(nb.buffer().capacity()))
        .unwrap_or_default();

    // summing child data size
    for child in array_data.child_data() {
        mem_size += size_of::<ArrayData>() + get_array_data_mem_size(child);
    }
    mem_size
}

/// Largest advance of any i32 offset in these rows. String/binary offsets
/// count bytes, while list/map offsets count entries, even for zero-width
/// children. Only the two endpoints of each offset buffer are needed.
pub fn max_i32_offset_span(array: &dyn Array, rows: Range<usize>) -> usize {
    fn span<T: ArrowNativeType>(offsets: &[T], rows: Range<usize>) -> Range<usize> {
        offsets[rows.start].as_usize()..offsets[rows.end].as_usize()
    }

    match array.data_type() {
        DataType::Utf8 => span(array.as_string::<i32>().value_offsets(), rows).len(),
        DataType::Binary => span(array.as_binary::<i32>().value_offsets(), rows).len(),
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let children = span(list.value_offsets(), rows);
            children
                .len()
                .max(max_i32_offset_span(list.values().as_ref(), children))
        }
        DataType::Map(_, _) => {
            let map = array.as_map();
            let entries = span(map.value_offsets(), rows);
            entries
                .len()
                .max(max_i32_offset_span(map.entries(), entries))
        }
        DataType::Struct(_) => array
            .as_struct()
            .columns()
            .iter()
            .map(|col| max_i32_offset_span(col.as_ref(), rows.clone()))
            .max()
            .unwrap_or(0),
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            max_i32_offset_span(list.values().as_ref(), span(list.value_offsets(), rows))
        }
        DataType::FixedSizeList(_, size) => {
            let size = *size as usize;
            max_i32_offset_span(
                array.as_fixed_size_list().values().as_ref(),
                rows.start * size..rows.end * size,
            )
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, ListArray, StringArray},
        buffer::OffsetBuffer,
        datatypes::Field,
    };

    use super::*;

    #[test]
    fn nested_string_offsets_use_the_selected_slice() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["unused", "é", "三", "tail"]));
        let lists = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Utf8, true)),
            OffsetBuffer::from_lengths([1, 2, 1]),
            strings,
            None,
        );
        assert_eq!(max_i32_offset_span(&lists.slice(1, 2), 0..1), 5);
        assert_eq!(max_i32_offset_span(&lists, 1..3), 9);
    }
}
