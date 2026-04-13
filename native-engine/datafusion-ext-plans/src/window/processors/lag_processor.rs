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

use std::sync::Arc;

use arrow::{array::ArrayRef, datatypes::DataType, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    physical_expr::PhysicalExprRef,
};
use datafusion_ext_commons::arrow::cast::cast;

use crate::window::{WindowFunctionProcessor, window_context::WindowContext};

pub struct LagProcessor {
    children: Vec<PhysicalExprRef>,
}

impl LagProcessor {
    pub fn new(children: Vec<PhysicalExprRef>) -> Self {
        Self { children }
    }
}

impl WindowFunctionProcessor for LagProcessor {
    fn process_batch(&mut self, context: &WindowContext, batch: &RecordBatch) -> Result<ArrayRef> {
        assert_eq!(
            self.children.len(),
            3,
            "lag expects input/offset/default children",
        );

        let input_values = self.children[0]
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))?;

        let offset_values = self.children[1]
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))?;
        let offset_values = if offset_values.data_type() == &DataType::Int32 {
            offset_values
        } else {
            cast(&offset_values, &DataType::Int32)?
        };
        let offset = match ScalarValue::try_from_array(&offset_values, 0)? {
            ScalarValue::Int32(Some(offset)) => offset as i64,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "lag offset must be a non-null foldable integer, got {other:?}",
                )));
            }
        };

        let default_values = self.children[2]
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))?;
        let default_values = if default_values.data_type() == input_values.data_type() {
            default_values
        } else {
            cast(&default_values, input_values.data_type())?
        };

        let mut partition_starts = vec![0usize; batch.num_rows()];
        let mut partition_ends = vec![batch.num_rows(); batch.num_rows()];
        if context.has_partition() && batch.num_rows() > 0 {
            let partition_rows = context.get_partition_rows(batch)?;
            let mut partition_start = 0usize;
            for row_idx in 1..=batch.num_rows() {
                let is_boundary = row_idx == batch.num_rows()
                    || partition_rows.row(row_idx).as_ref()
                        != partition_rows.row(partition_start).as_ref();
                if is_boundary {
                    for idx in partition_start..row_idx {
                        partition_starts[idx] = partition_start;
                        partition_ends[idx] = row_idx;
                    }
                    partition_start = row_idx;
                }
            }
        }

        let mut output = Vec::with_capacity(batch.num_rows());
        for row_idx in 0..batch.num_rows() {
            // lag looks backward: target is offset rows before current row
            let target_idx = row_idx as i64 - offset;
            let partition_start = partition_starts[row_idx] as i64;
            let partition_end = partition_ends[row_idx] as i64;
            let value = if target_idx >= partition_start && target_idx < partition_end {
                ScalarValue::try_from_array(&input_values, target_idx as usize)?
            } else {
                ScalarValue::try_from_array(&default_values, row_idx)?
            };
            output.push(value);
        }

        ScalarValue::iter_to_array(output)
    }
}
