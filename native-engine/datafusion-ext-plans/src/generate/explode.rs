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

use std::{any::Any, ops::Range, sync::Arc};

use arrow::{array::*, record_batch::RecordBatch};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{
    arrow::coalesce::coalesce_arrays_unchecked, batch_size, downcast_any,
};

use crate::generate::{GenerateState, GeneratedRows, Generator};

fn append_range(ranges: &mut Vec<Range<usize>>, range: Range<usize>) {
    if range.is_empty() {
        return;
    }
    if let Some(last) = ranges.last_mut()
        && last.end == range.start
    {
        last.end = range.end;
        return;
    }
    ranges.push(range);
}

fn slices_for_ranges(values: &ArrayRef, ranges: &[Range<usize>]) -> Vec<ArrayRef> {
    ranges
        .iter()
        .map(|range| values.slice(range.start, range.len()))
        .collect()
}

#[derive(Debug)]
pub struct ExplodeArray {
    child: PhysicalExprRef,
    position: bool,
}

impl ExplodeArray {
    pub fn new(child: PhysicalExprRef, position: bool) -> Self {
        Self { child, position }
    }
}

impl Generator for ExplodeArray {
    fn exprs(&self) -> Vec<PhysicalExprRef> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self {
            child: exprs[0].clone(),
            position: self.position,
        }))
    }

    fn eval_start(&self, batch: &RecordBatch) -> Result<Box<dyn GenerateState>> {
        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(Box::new(ExplodeArrayGenerateState {
            input_array: input_array.as_list().clone(),
            cur_row_id: 0,
        }))
    }

    fn eval_loop(&self, state: &mut Box<dyn GenerateState>) -> Result<Option<GeneratedRows>> {
        let state = downcast_any!(state, mut ExplodeArrayGenerateState)?;
        let batch_size = batch_size();

        let mut row_idx = state.cur_row_id;
        let mut row_ids = vec![];
        let mut pos_ids = vec![];
        let mut sub_list_ranges = vec![];
        let value_offsets = state.input_array.value_offsets();

        while row_idx < state.input_array.len() && row_ids.len() < batch_size {
            if state.input_array.is_null(row_idx) {
                row_idx += 1;
                continue;
            }

            let start = value_offsets[row_idx] as usize;
            let end = value_offsets[row_idx + 1] as usize;
            let len = end - start;
            row_ids.resize(row_ids.len() + len, row_idx as i32);
            if self.position {
                pos_ids.extend(0..len as i32);
            }
            append_range(&mut sub_list_ranges, start..end);
            row_idx += 1;
        }
        state.cur_row_id = row_idx;

        if row_ids.is_empty() {
            return Ok(None);
        }

        let sub_lists = slices_for_ranges(state.input_array.values(), &sub_list_ranges);
        let values = coalesce_arrays_unchecked(&state.input_array.value_type(), &sub_lists);
        let cols = if self.position {
            vec![Arc::new(Int32Array::from(pos_ids)), values]
        } else {
            vec![values]
        };

        Ok(Some(GeneratedRows {
            row_ids: Int32Array::from(row_ids),
            cols,
        }))
    }
}

struct ExplodeArrayGenerateState {
    pub input_array: ListArray,
    pub cur_row_id: usize,
}

impl GenerateState for ExplodeArrayGenerateState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cur_row_id(&self) -> usize {
        self.cur_row_id
    }
}

#[derive(Debug)]
pub struct ExplodeMap {
    child: PhysicalExprRef,
    position: bool,
}

impl ExplodeMap {
    pub fn new(child: PhysicalExprRef, position: bool) -> Self {
        Self { child, position }
    }
}

impl Generator for ExplodeMap {
    fn exprs(&self) -> Vec<PhysicalExprRef> {
        vec![self.child.clone()]
    }

    fn with_new_exprs(&self, exprs: Vec<PhysicalExprRef>) -> Result<Arc<dyn Generator>> {
        Ok(Arc::new(Self {
            child: exprs[0].clone(),
            position: self.position,
        }))
    }

    fn eval_start(&self, batch: &RecordBatch) -> Result<Box<dyn GenerateState>> {
        let input_array = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        Ok(Box::new(ExplodeMapGenerateState {
            input_array: input_array.as_map().clone(),
            cur_row_id: 0,
        }))
    }

    fn eval_loop(&self, state: &mut Box<dyn GenerateState>) -> Result<Option<GeneratedRows>> {
        let state = downcast_any!(state, mut ExplodeMapGenerateState)?;
        let batch_size = batch_size();

        let mut row_idx = state.cur_row_id;
        let mut row_ids = vec![];
        let mut pos_ids = vec![];
        let mut entry_ranges = vec![];
        let value_offsets = state.input_array.value_offsets();

        while row_idx < state.input_array.len() && row_ids.len() < batch_size {
            if state.input_array.is_null(row_idx) {
                row_idx += 1;
                continue;
            }

            let start = value_offsets[row_idx] as usize;
            let end = value_offsets[row_idx + 1] as usize;
            let len = end - start;
            row_ids.resize(row_ids.len() + len, row_idx as i32);
            if self.position {
                pos_ids.extend(0..len as i32);
            }
            append_range(&mut entry_ranges, start..end);
            row_idx += 1;
        }
        state.cur_row_id = row_idx;

        if row_ids.is_empty() {
            return Ok(None);
        }

        let sub_keys = slices_for_ranges(state.input_array.keys(), &entry_ranges);
        let sub_vals = slices_for_ranges(state.input_array.values(), &entry_ranges);
        let keys = coalesce_arrays_unchecked(&state.input_array.key_type(), &sub_keys);
        let vals = coalesce_arrays_unchecked(&state.input_array.value_type(), &sub_vals);
        let cols = if self.position {
            vec![Arc::new(Int32Array::from(pos_ids)), keys, vals]
        } else {
            vec![keys, vals]
        };

        Ok(Some(GeneratedRows {
            row_ids: Int32Array::from(row_ids),
            cols,
        }))
    }
}

struct ExplodeMapGenerateState {
    pub input_array: MapArray,
    pub cur_row_id: usize,
}

impl GenerateState for ExplodeMapGenerateState {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cur_row_id(&self) -> usize {
        self.cur_row_id
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, ArrayRef, Int32Array, ListArray, MapArray, StringArray},
        buffer::{NullBuffer, OffsetBuffer},
        datatypes::{DataType, Field, Int32Type},
        record_batch::RecordBatch,
    };
    use datafusion::physical_expr::expressions::Column;

    use super::*;

    #[test]
    fn dense_map_reuses_input_key_and_value_buffers() -> Result<()> {
        let map = MapArray::new_from_strings(
            ["a", "b", "c", "d"].into_iter(),
            &Int32Array::from_iter_values([1, 2, 3, 4]),
            &[0, 2, 4],
        )?;
        let input_key_data = map.keys().as_string::<i32>().value_data().as_ptr();
        let input_values = map.values().as_primitive::<Int32Type>().values().clone();
        let batch = RecordBatch::try_from_iter([("map", Arc::new(map) as ArrayRef)])?;
        let generator = ExplodeMap::new(Arc::new(Column::new("map", 0)), false);

        let mut state = generator.eval_start(&batch)?;
        let output = generator
            .eval_loop(&mut state)?
            .expect("dense map should generate rows");
        assert_eq!(output.cols.len(), 2);
        let output_key_data = output.cols[0].as_string::<i32>().value_data().as_ptr();
        let output_values = output.cols[1].as_primitive::<Int32Type>().values();

        assert_eq!(input_key_data, output_key_data);
        assert!(input_values.ptr_eq(output_values));
        Ok(())
    }

    #[test]
    fn posexplode_list_outputs_positions() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
        ]);
        let batch = RecordBatch::try_from_iter([("list", Arc::new(list) as ArrayRef)])?;
        let generator = ExplodeArray::new(Arc::new(Column::new("list", 0)), true);

        let mut state = generator.eval_start(&batch)?;
        let output = generator
            .eval_loop(&mut state)?
            .expect("non-empty list should generate rows");

        assert_eq!(output.cols.len(), 2);
        assert_eq!(
            output.cols[0].as_primitive::<Int32Type>(),
            &Int32Array::from_iter_values([0, 1, 0])
        );
        assert_eq!(
            output.cols[1].as_primitive::<Int32Type>(),
            &Int32Array::from_iter_values([1, 2, 3])
        );
        Ok(())
    }

    #[test]
    fn list_offsets_skip_hidden_null_values() -> Result<()> {
        let field = Arc::new(Field::new_list_field(DataType::Int32, true));
        let offsets = OffsetBuffer::new(vec![0_i32, 1, 3, 4, 6].into());
        let values = Arc::new(Int32Array::from_iter_values([0, 10, 11, 20, 30, 31]));
        let nulls = NullBuffer::from(vec![true, false, true, true]);
        let list = ListArray::try_new(field, offsets, values, Some(nulls))?.slice(0, 4);
        let batch = RecordBatch::try_from_iter([("list", Arc::new(list) as ArrayRef)])?;
        let generator = ExplodeArray::new(Arc::new(Column::new("list", 0)), false);

        let mut state = generator.eval_start(&batch)?;
        let output = generator
            .eval_loop(&mut state)?
            .expect("valid list rows should generate rows");

        assert_eq!(
            output.cols[0].as_primitive::<Int32Type>(),
            &Int32Array::from_iter_values([0, 20, 30, 31])
        );
        Ok(())
    }

    #[test]
    fn map_offsets_skip_hidden_null_entries() -> Result<()> {
        let map = MapArray::new_from_strings(
            ["a", "hidden1", "hidden2", "d"].into_iter(),
            &Int32Array::from_iter_values([1, 10, 11, 4]),
            &[0, 1, 3, 4],
        )?;
        let (field, offsets, entries, _, ordered) = map.into_parts();
        let map = MapArray::try_new(
            field,
            offsets,
            entries,
            Some(NullBuffer::from(vec![true, false, true])),
            ordered,
        )?;
        let batch = RecordBatch::try_from_iter([("map", Arc::new(map) as ArrayRef)])?;
        let generator = ExplodeMap::new(Arc::new(Column::new("map", 0)), false);

        let mut state = generator.eval_start(&batch)?;
        let output = generator
            .eval_loop(&mut state)?
            .expect("valid map rows should generate rows");

        assert_eq!(
            output.cols[0].as_string::<i32>(),
            &StringArray::from(vec!["a", "d"])
        );
        assert_eq!(
            output.cols[1].as_primitive::<Int32Type>(),
            &Int32Array::from_iter_values([1, 4])
        );
        Ok(())
    }

    #[test]
    fn sliced_list_parent_reuses_input_value_buffer() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(0), Some(1)]),
            Some(vec![Some(2)]),
            Some(vec![Some(3), Some(4)]),
        ])
        .slice(1, 2);
        let input_values = list.values().as_primitive::<Int32Type>().values().clone();
        let batch = RecordBatch::try_from_iter([("list", Arc::new(list) as ArrayRef)])?;
        let generator = ExplodeArray::new(Arc::new(Column::new("list", 0)), false);

        let mut state = generator.eval_start(&batch)?;
        let output = generator
            .eval_loop(&mut state)?
            .expect("sliced list should generate rows");
        assert_eq!(output.cols.len(), 1);
        let output_values = output.cols[0].as_primitive::<Int32Type>();

        assert_eq!(output_values, &Int32Array::from_iter_values([2, 3, 4]));
        assert_eq!(
            input_values.inner().data_ptr(),
            output_values.values().inner().data_ptr()
        );
        Ok(())
    }
}
