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

use arrow::{array::ArrayRef, record_batch::RecordBatch};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    physical_expr::{PhysicalExprRef, expressions::Literal},
};

use crate::window::{WindowFunctionProcessor, window_context::WindowContext};

pub struct NthValueProcessor {
    input: PhysicalExprRef,
    offset: usize,
    ignore_nulls: bool,
    cur_partition: Box<[u8]>,
    observed_rows: usize,
    nth_value: Option<ScalarValue>,
}

impl NthValueProcessor {
    pub fn try_new(children: Vec<PhysicalExprRef>, ignore_nulls: bool) -> Result<Self> {
        if children.len() != 2 {
            return Err(DataFusionError::Execution(format!(
                "nth_value expects input/offset children, got {}",
                children.len(),
            )));
        }

        let offset_literal = children[1]
            .as_any()
            .downcast_ref::<Literal>()
            .ok_or_else(|| {
                DataFusionError::Execution(
                    "nth_value offset must be a literal physical expression; \
                     foldable offsets should be constant-folded before building the native plan"
                        .to_string(),
                )
            })?;

        let offset = match offset_literal.value() {
            ScalarValue::Int32(Some(value)) if *value > 0 => *value as usize,
            ScalarValue::Int64(Some(value)) if *value > 0 => *value as usize,
            ScalarValue::UInt32(Some(value)) if *value > 0 => *value as usize,
            ScalarValue::UInt64(Some(value)) if *value > 0 => *value as usize,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "nth_value offset must be a positive non-null integer literal, got {other:?}",
                )));
            }
        };

        Ok(Self {
            input: children[0].clone(),
            offset,
            ignore_nulls,
            cur_partition: Box::default(),
            observed_rows: 0,
            nth_value: None,
        })
    }
}

impl WindowFunctionProcessor for NthValueProcessor {
    fn process_batch(&mut self, context: &WindowContext, batch: &RecordBatch) -> Result<ArrayRef> {
        let partition_rows = context.get_partition_rows(batch)?;
        let input_values = self
            .input
            .evaluate(batch)
            .and_then(|value| value.into_array(batch.num_rows()))?;
        let null_value = ScalarValue::try_from(input_values.data_type().clone())?;
        let mut output = Vec::with_capacity(batch.num_rows());

        for row_idx in 0..batch.num_rows() {
            let same_partition = !context.has_partition() || {
                let partition_row = partition_rows.row(row_idx);
                if partition_row.as_ref() != self.cur_partition.as_ref() {
                    self.cur_partition = partition_row.as_ref().into();
                    false
                } else {
                    true
                }
            };

            if !same_partition {
                self.observed_rows = 0;
                self.nth_value = None;
            }

            if self.nth_value.is_none() {
                let value = ScalarValue::try_from_array(&input_values, row_idx)?;
                let counts_for_offset = !self.ignore_nulls || !value.is_null();
                if counts_for_offset {
                    self.observed_rows += 1;
                    if self.observed_rows == self.offset {
                        self.nth_value = Some(value);
                    }
                }
            }

            output.push(self.nth_value.clone().unwrap_or_else(|| null_value.clone()));
        }

        ScalarValue::iter_to_array(output)
    }
}
