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
};

pub mod full_join;
pub mod semi_join;

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub enum ProbeSide {
    L,
    R,
}

/// Gather only condition columns, left then right, with one row per candidate
/// pair.
pub fn filter_cols(
    filter: &JoinFilter,
    probe_side: ProbeSide,
    probed_batch: &RecordBatch,
    built_batch: &RecordBatch,
    probe_indices: &[u32],
    build_indices: &[u32],
) -> Result<Vec<ArrayRef>> {
    let (pcols, bcols) = (probed_batch.columns(), built_batch.columns());
    let (lcols, lindices, rcols, rindices) = match probe_side {
        L => (pcols, probe_indices, bcols, build_indices),
        R => (bcols, build_indices, pcols, probe_indices),
    };
    let pick = |cols: &[ArrayRef], read: &[usize]| -> Vec<ArrayRef> {
        read.iter().map(|&i| cols[i].clone()).collect()
    };
    Ok([
        take_cols(
            &pick(lcols, filter.left_read()),
            UInt32Array::from(lindices.to_vec()),
        )?,
        take_cols(
            &pick(rcols, filter.right_read()),
            UInt32Array::from(rindices.to_vec()),
        )?,
    ]
    .concat())
}

#[cfg(test)]
mod tests;
