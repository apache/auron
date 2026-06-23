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

use arrow::{
    array::{ArrayRef, Float64Builder},
    record_batch::RecordBatch,
};
use datafusion::common::Result;

use crate::window::{WindowFunctionProcessor, window_context::WindowContext};

pub struct PercentRankProcessor;

impl PercentRankProcessor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for PercentRankProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowFunctionProcessor for PercentRankProcessor {
    fn process_batch(&mut self, context: &WindowContext, batch: &RecordBatch) -> Result<ArrayRef> {
        let partition_rows = context.get_partition_rows(batch)?;
        let order_rows = context.get_order_rows(batch)?;
        let mut builder = Float64Builder::with_capacity(batch.num_rows());

        let mut row_idx = 0usize;
        while row_idx < batch.num_rows() {
            let partition_start = row_idx;
            row_idx += 1;
            while row_idx < batch.num_rows()
                && (!context.has_partition()
                    || partition_rows.row(row_idx).as_ref()
                        == partition_rows.row(partition_start).as_ref())
            {
                row_idx += 1;
            }

            let partition_end = row_idx;
            let partition_size = partition_end - partition_start;
            let denominator = (partition_size.saturating_sub(1)) as f64;

            let mut rank = 1usize;
            let mut peer_group_size = 1usize;
            for current_idx in partition_start..partition_end {
                if current_idx > partition_start {
                    let prev_idx = current_idx - 1;
                    if order_rows.row(current_idx).as_ref() == order_rows.row(prev_idx).as_ref() {
                        peer_group_size += 1;
                    } else {
                        rank += peer_group_size;
                        peer_group_size = 1;
                    }
                }

                let percent_rank = if partition_size <= 1 {
                    0.0
                } else {
                    (rank - 1) as f64 / denominator
                };
                builder.append_value(percent_rank);
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}
