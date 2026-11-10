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

use std::{collections::VecDeque, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, ArrowPrimitiveType, AsArray, BooleanBufferBuilder, BufferBuilder,
        Capacities, GenericByteArray, MutableArrayData, PrimitiveArray, RecordBatch,
        RecordBatchOptions, downcast_primitive, make_array, new_empty_array,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{
        ArrowNativeType, BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type,
    },
};
use arrow_schema::{DataType, SchemaRef};
use datafusion::common::{DataFusionError, Result};

use crate::{arrow::array_size::BatchSize, batch_size, suggested_batch_mem_size};

pub struct BatchCoalescer {
    schema: SchemaRef,
    batch_size: usize,
    batch_mem_size: usize,
    staging_batches: Vec<RecordBatch>,
    staging_num_rows: usize,
    staging_mem_size: usize,
    completed_batches: VecDeque<RecordBatch>,
}

impl BatchCoalescer {
    pub fn new(schema: SchemaRef, batch_size: usize, batch_mem_size: usize) -> Self {
        Self {
            schema,
            batch_size,
            batch_mem_size,
            staging_batches: Vec::new(),
            staging_num_rows: 0,
            staging_mem_size: 0,
            completed_batches: VecDeque::new(),
        }
    }

    pub fn new_with_default_batch_size(schema: SchemaRef) -> Self {
        Self::new(schema, batch_size(), suggested_batch_mem_size())
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.staging_num_rows += batch.num_rows();
        self.staging_mem_size += batch.get_batch_mem_size();
        self.staging_batches.push(batch);

        if self.staging_num_rows >= self.batch_size || self.staging_mem_size > self.batch_mem_size {
            self.finish_buffered_batch()?;
        }
        Ok(())
    }

    pub fn finish_buffered_batch(&mut self) -> Result<()> {
        if self.staging_batches.is_empty() {
            return Ok(());
        }
        let batch = if self.staging_batches.len() == 1 {
            self.staging_batches
                .pop()
                .ok_or_else(|| DataFusionError::Internal("missing buffered batch".to_string()))?
        } else {
            let batch = arrow::compute::concat_batches(&self.schema, &self.staging_batches)
                .map_err(DataFusionError::from)?;
            self.staging_batches.clear();
            batch
        };
        self.completed_batches.push_back(batch);
        self.staging_num_rows = 0;
        self.staging_mem_size = 0;
        Ok(())
    }

    pub fn has_completed_batch(&self) -> bool {
        !self.completed_batches.is_empty()
    }

    pub fn has_buffered_batches(&self) -> bool {
        !self.staging_batches.is_empty()
    }

    pub fn next_completed_batch(&mut self) -> Option<RecordBatch> {
        self.completed_batches.pop_front()
    }
}

/// coalesce batches without checking there schemas, invokers must make
/// sure all arrays have the same schema
pub fn coalesce_batches_unchecked(schema: SchemaRef, batches: &[RecordBatch]) -> RecordBatch {
    match batches.len() {
        0 => return RecordBatch::new_empty(schema),
        1 => return batches[0].clone(),
        _ => {}
    }
    let num_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let num_fields = schema.fields().len();
    let mut coalesced_cols = vec![];

    for i in 0..num_fields {
        let data_type = schema.field(i).data_type();
        let mut cols = Vec::with_capacity(batches.len());
        for j in 0..batches.len() {
            cols.push(batches[j].column(i).clone());
        }
        coalesced_cols.push(coalesce_arrays_unchecked(data_type, &cols));
    }

    RecordBatch::try_new_with_options(
        schema,
        coalesced_cols,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )
    .expect("error coalescing record batch")
}

/// coalesce arrays without checking there data types, invokers must make
/// sure all arrays have the same data type
pub fn coalesce_arrays_unchecked(data_type: &DataType, arrays: &[ArrayRef]) -> ArrayRef {
    if arrays.is_empty() {
        return new_empty_array(data_type);
    }
    if arrays.len() == 1 {
        return arrays[0].clone();
    }

    macro_rules! primitive_helper {
        ($t:ty, $dt:ident) => {{
            return coalesce_primitive_arrays_unchecked::<$t>(arrays, $dt);
        }};
    }

    downcast_primitive! {
        data_type => (primitive_helper, data_type),
        DataType::Utf8 => {
            return coalesce_bytes_arrays_unchecked::<Utf8Type>(arrays);
        }
        DataType::LargeUtf8 => {
            return coalesce_bytes_arrays_unchecked::<LargeUtf8Type>(arrays);
        }
        DataType::Binary => {
            return coalesce_bytes_arrays_unchecked::<BinaryType>(arrays);
        }
        DataType::LargeBinary => {
            return coalesce_bytes_arrays_unchecked::<LargeBinaryType>(arrays);
        }
        _ => {},
    }

    // fallback implementation with MutableArrayData
    let capacity = Capacities::Array(arrays.iter().map(|a| a.len()).sum());
    let array_data: Vec<_> = arrays.iter().map(|a| a.to_data()).collect::<Vec<_>>();
    let array_data_refs = array_data.iter().collect();
    let mut mutable = MutableArrayData::with_capacities(array_data_refs, false, capacity);

    for (i, a) in arrays.iter().enumerate() {
        mutable.extend(i, 0, a.len())
    }
    make_array(mutable.freeze())
}

fn coalesce_primitive_arrays_unchecked<T>(arrays: &[ArrayRef], dt: &DataType) -> ArrayRef
where
    T: ArrowPrimitiveType,
{
    let items_len = arrays.iter().map(|a| a.len()).sum();
    let mut buffer: BufferBuilder<T::Native> = BufferBuilder::new(items_len);

    // extend buffer
    for array in arrays {
        buffer.append_slice(array.as_primitive::<T>().values());
    }

    // build coalesced byte array
    let values_buffer = ScalarBuffer::<T::Native>::from(buffer.finish());
    let nulls = coalesce_null_buffer(items_len, arrays);
    let array = PrimitiveArray::<T>::new(values_buffer, nulls);
    Arc::new(array.with_data_type(dt.clone()))
}

fn coalesce_bytes_arrays_unchecked<T>(arrays: &[ArrayRef]) -> ArrayRef
where
    T: ByteArrayType,
{
    let items_len = arrays.iter().map(|a| a.len()).sum();
    let data_len = arrays
        .iter()
        .map(|a| {
            let array: &GenericByteArray<T> = a.as_bytes();
            let value_offsets = array.value_offsets();
            value_offsets[value_offsets.len() - 1].as_usize() - value_offsets[0].as_usize()
        })
        .sum();

    let mut buffer: BufferBuilder<u8> = BufferBuilder::new(data_len);
    let mut offsets: BufferBuilder<T::Offset> = BufferBuilder::new(items_len + 1);
    let mut cur_offset = 0;
    offsets.append(T::Offset::usize_as(0));

    for array in arrays {
        let array = array.as_bytes::<T>();
        let value_offsets = array.value_offsets();
        let array_begin_offset = value_offsets[0].as_usize();
        let array_end_offset = value_offsets[array.len()].as_usize();
        let array_bytes = &array.value_data()[array_begin_offset..array_end_offset];

        // extend offsets
        let relative_offset = T::Offset::usize_as(cur_offset - array_begin_offset);
        for offset in value_offsets[1..].iter() {
            offsets.append(*offset + relative_offset);
        }
        cur_offset += array_bytes.len();

        // extend data
        buffer.append_slice(array_bytes);
    }

    // build coalesced byte array
    let offset_buffer = OffsetBuffer::<T::Offset>::new(ScalarBuffer::from(offsets.finish()));
    let data_buffer = buffer.finish();
    let nulls = coalesce_null_buffer(items_len, arrays);
    Arc::new(GenericByteArray::<T>::new(
        offset_buffer,
        data_buffer,
        nulls,
    ))
}

fn coalesce_null_buffer(items_len: usize, arrays: &[ArrayRef]) -> Option<NullBuffer> {
    arrays.iter().any(|array| array.nulls().is_some()).then(|| {
        let mut valids = BooleanBufferBuilder::new(items_len);
        for array in arrays {
            if let Some(nb) = array.nulls() {
                valids.append_buffer(nb.inner());
            } else {
                valids.append_n(array.len(), true);
            }
        }
        NullBuffer::from(valids.finish())
    })
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{BinaryArray, Int32Array, StringArray},
        datatypes::{Field, Schema},
    };
    use datafusion::common::Result;

    use super::*;

    fn binary_batch(num_rows: usize) -> Result<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "buffer",
            DataType::Binary,
            true,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(BinaryArray::from_iter_values(
                (0..num_rows).map(|_| b"buffer".as_slice()),
            ))],
        )
        .map_err(DataFusionError::from)
    }

    #[test]
    fn test_batch_coalescer_row_threshold_does_not_split_binary() -> Result<()> {
        let first = binary_batch(1)?;
        let second = binary_batch(8)?;
        let mut coalescer = BatchCoalescer::new(first.schema(), 8, usize::MAX);
        coalescer.push_batch(first)?;
        assert!(!coalescer.has_completed_batch());
        coalescer.push_batch(second)?;
        let output = coalescer.next_completed_batch().expect("expected batch");
        assert_eq!(output.num_rows(), 9);
        let binary = output.column(0).as_binary::<i32>();
        assert_eq!(binary.value_offsets()[0], 0);
        assert_eq!(
            binary.value_offsets()[9] as usize,
            binary.value_data().len()
        );
        assert!(
            binary
                .iter()
                .all(|value| value == Some(b"buffer".as_slice()))
        );
        coalescer.finish_buffered_batch()?;
        assert!(!coalescer.has_completed_batch());
        Ok(())
    }

    #[test]
    fn test_batch_coalescer_preserves_oversized_batch() -> Result<()> {
        let input = binary_batch(17)?;
        let column = input.column(0).clone();
        let mut coalescer = BatchCoalescer::new(input.schema(), 8, usize::MAX);
        coalescer.push_batch(input)?;
        let output = coalescer.next_completed_batch().expect("expected batch");
        assert_eq!(output.num_rows(), 17);
        assert!(Arc::ptr_eq(output.column(0), &column));
        assert!(!coalescer.has_completed_batch());
        Ok(())
    }

    #[test]
    fn test_batch_coalescer_memory_threshold_and_reset() -> Result<()> {
        let input = binary_batch(2)?;
        let batch_mem_size = input.get_batch_mem_size();
        let mut coalescer = BatchCoalescer::new(input.schema(), 100, batch_mem_size);
        coalescer.push_batch(input.clone())?;
        assert!(!coalescer.has_completed_batch());
        coalescer.push_batch(input.clone())?;
        assert_eq!(
            coalescer
                .next_completed_batch()
                .expect("expected batch")
                .num_rows(),
            4
        );
        coalescer.push_batch(input)?;
        assert!(!coalescer.has_completed_batch());
        coalescer.finish_buffered_batch()?;
        assert_eq!(
            coalescer
                .next_completed_batch()
                .expect("expected batch")
                .num_rows(),
            2
        );
        coalescer.finish_buffered_batch()?;
        assert!(!coalescer.has_completed_batch());
        Ok(())
    }

    #[test]
    fn test_batch_coalescer_row_threshold_and_completed_queue() -> Result<()> {
        let input = binary_batch(4)?;
        let mut coalescer = BatchCoalescer::new(input.schema(), 4, usize::MAX);
        coalescer.push_batch(RecordBatch::new_empty(input.schema()))?;
        coalescer.finish_buffered_batch()?;
        assert!(!coalescer.has_completed_batch());
        coalescer.push_batch(input.clone())?;
        coalescer.push_batch(binary_batch(5)?)?;
        coalescer.push_batch(binary_batch(1)?)?;
        assert_eq!(
            coalescer
                .next_completed_batch()
                .expect("expected batch")
                .num_rows(),
            4
        );
        assert_eq!(
            coalescer
                .next_completed_batch()
                .expect("expected batch")
                .num_rows(),
            5
        );
        assert!(!coalescer.has_completed_batch());
        coalescer.finish_buffered_batch()?;
        assert_eq!(
            coalescer
                .next_completed_batch()
                .expect("expected batch")
                .num_rows(),
            1
        );
        assert!(!coalescer.has_completed_batch());
        Ok(())
    }

    #[test]
    fn test_coalesce_primitive_with_offsets() -> Result<()> {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(0),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
        ]));

        let test = vec![array.slice(0, 6), array.slice(2, 6), array.slice(4, 6)];
        let coalesced = coalesce_arrays_unchecked(&DataType::Int32, &test);
        let coalesced_std =
            arrow::compute::concat(&test.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
        assert_eq!(&coalesced, &coalesced_std);
        Ok(())
    }

    #[test]
    fn test_coalesce_string_with_offsets() -> Result<()> {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Some("000".to_string()),
            Some("111".to_string()),
            Some("222".to_string()),
            Some("333".to_string()),
            Some("444".to_string()),
            None,
            Some("666666".to_string()),
            Some("777".to_string()),
            Some("888".to_string()),
            Some("999".to_string()),
            Some("101010".to_string()),
        ]));
        let test = vec![array.slice(0, 6), array.slice(2, 6), array.slice(4, 6)];
        let coalesced = coalesce_arrays_unchecked(&DataType::Utf8, &test);
        let coalesced_std =
            arrow::compute::concat(&test.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
        assert_eq!(&coalesced, &coalesced_std);
        Ok(())
    }

    #[test]
    fn test_coalesce_string_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(StringArray::from(vec![
            Option::<String>::None,
            None,
            None,
            None,
        ]));
        let test = vec![array.slice(0, 3), array.slice(1, 3)];
        let coalesced = coalesce_arrays_unchecked(&DataType::Utf8, &test);
        let coalesced_std =
            arrow::compute::concat(&test.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?;
        assert_eq!(&coalesced, &coalesced_std);
        Ok(())
    }
}
