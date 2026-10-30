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

use arrow::array::{ArrayRef, RecordBatch, UInt32Array};
use datafusion::common::Result;
use datafusion_ext_commons::arrow::selection::take_cols;

use crate::joins::{
    bhj::ProbeSide::{L, R},
    join_filter::JoinFilter,
    join_hash_map::JoinHashMap,
};

pub mod full_join;
pub mod semi_join;

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub enum ProbeSide {
    L,
    R,
}

/// Gather only condition columns, left then right, with one row per candidate
/// pair. `built_map` is the build side (may internally hold multiple
/// un-coalesced data batches - see `JoinHashMap::gather_columns`); the
/// probed side is always a plain single `RecordBatch`.
pub fn filter_cols(
    filter: &JoinFilter,
    probe_side: ProbeSide,
    probed_batch: &RecordBatch,
    built_map: &JoinHashMap,
    probe_indices: &[u32],
    build_indices: &[u32],
) -> Result<Vec<ArrayRef>> {
    let (probe_read, build_read) = match probe_side {
        L => (filter.left_read(), filter.right_read()),
        R => (filter.right_read(), filter.left_read()),
    };
    let probe_cols: Vec<ArrayRef> = probe_read
        .iter()
        .map(|&i| probed_batch.column(i).clone())
        .collect();
    let probe_taken = take_cols(&probe_cols, UInt32Array::from(probe_indices.to_vec()))?;
    let build_taken =
        built_map.gather_columns(build_read, UInt32Array::from(build_indices.to_vec()))?;

    Ok(match probe_side {
        L => [probe_taken, build_taken].concat(),
        R => [build_taken, probe_taken].concat(),
    })
}

#[cfg(test)]
mod tests;
