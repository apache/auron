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

pub struct CumeDistProcessor;

impl CumeDistProcessor {
    pub fn new() -> Self {
        Self
    }
}

impl Default for CumeDistProcessor {
    fn default() -> Self {
        Self::new()
    }
}

impl WindowFunctionProcessor for CumeDistProcessor {
    fn process_batch(&mut self, context: &WindowContext, batch: &RecordBatch) -> Result<ArrayRef> {
        let partition_rows = context.get_partition_rows(batch)?;
        let order_rows = context.get_order_rows(batch)?;
        let mut builder = Float64Builder::with_capacity(batch.num_rows());

        let mut partition_start = 0usize;
        while partition_start < batch.num_rows() {
            let partition_start_row = partition_rows.row(partition_start);
            let mut partition_end = partition_start + 1;
            while partition_end < batch.num_rows()
                && (!context.has_partition()
                    || partition_rows.row(partition_end).as_ref() == partition_start_row.as_ref())
            {
                partition_end += 1;
            }

            let partition_size = (partition_end - partition_start) as f64;
            let mut peer_start = partition_start;
            while peer_start < partition_end {
                let peer_start_row = order_rows.row(peer_start);
                let mut peer_end = peer_start + 1;
                while peer_end < partition_end
                    && order_rows.row(peer_end).as_ref() == peer_start_row.as_ref()
                {
                    peer_end += 1;
                }

                let cume_dist = (peer_end - partition_start) as f64 / partition_size;
                for _ in peer_start..peer_end {
                    builder.append_value(cume_dist);
                }

                peer_start = peer_end;
            }

            partition_start = partition_end;
        }

        Ok(Arc::new(builder.finish()))
    }
}
