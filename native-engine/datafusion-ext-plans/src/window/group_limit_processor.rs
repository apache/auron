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

use arrow::record_batch::RecordBatch;
use datafusion::common::Result;

use crate::window::{
    WindowRankType,
    processors::{rank_processor::RankProcessor, row_number_processor::RowNumberProcessor},
    window_context::WindowContext,
};

pub(crate) struct WindowGroupLimitProcessor {
    processor: RankingProcessor,
    limit: i32,
}

// RankProcessor handles both rank and dense_rank; row_number has its own
// processor.
enum RankingProcessor {
    RowNumber(RowNumberProcessor),
    Rank(RankProcessor),
}

impl WindowGroupLimitProcessor {
    pub(crate) fn new(rank_type: WindowRankType, limit: usize) -> Self {
        let processor = match rank_type {
            WindowRankType::RowNumber => RankingProcessor::RowNumber(RowNumberProcessor::new()),
            WindowRankType::Rank => RankingProcessor::Rank(RankProcessor::new(false)),
            WindowRankType::DenseRank => RankingProcessor::Rank(RankProcessor::new(true)),
        };
        Self {
            processor,
            limit: i32::try_from(limit).unwrap_or(i32::MAX),
        }
    }

    pub(crate) fn process_batch(
        &mut self,
        context: &WindowContext,
        batch: &RecordBatch,
    ) -> Result<Vec<Range<usize>>> {
        let mut selected_ranges = vec![];
        let mut selected_start = None;
        let mut collect_range = |row_idx, rank| {
            if rank <= self.limit {
                selected_start.get_or_insert(row_idx);
            } else if let Some(start) = selected_start.take() {
                selected_ranges.push(start..row_idx);
            }
        };
        match &mut self.processor {
            RankingProcessor::RowNumber(processor) => {
                processor.process_batch_with(context, batch, &mut collect_range)?;
            }
            RankingProcessor::Rank(processor) => {
                processor.process_batch_with(context, batch, &mut collect_range)?;
            }
        }
        if let Some(start) = selected_start {
            selected_ranges.push(start..batch.num_rows());
        }
        Ok(selected_ranges)
    }
}
