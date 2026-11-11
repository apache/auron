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

use std::collections::HashMap;

use arrow::array::{ArrayRef, Int32Array, UInt32Array};
use datafusion::common::Result;

use crate::{
    common::lazy::{LazyBatch, LazyColumn, LazyColumnRef, column, take_columns},
    joins::{
        bhj::ProbeSide::{L, R},
        join_filter::JoinFilter,
        join_hash_map::JoinHashMap,
    },
};

pub mod full_join;
pub mod semi_join;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ProbeSide {
    L,
    R,
}

/// Candidate-level takes of a join filter's read columns, keyed by their
/// source column index (probed batch for `probe`, build batch for `build`),
/// so output projections reading the same columns can reuse them instead of
/// gathering the source a second time.
pub struct FilterTaken {
    pub probe: Vec<(usize, ArrayRef)>,
    pub build: Vec<(usize, ArrayRef)>,
}

/// Gather only condition columns, left then right, with one row per candidate
/// pair. Returns the filter-evaluation columns together with the per-source
/// takes they were gathered from.
pub fn filter_cols(
    filter: &JoinFilter,
    probe_side: ProbeSide,
    probed_batch: &dyn LazyBatch,
    built_map: &JoinHashMap,
    probe_indices: &[u32],
    build_indices: &[u32],
) -> Result<(Vec<ArrayRef>, FilterTaken)> {
    let (probe_read, build_read) = match probe_side {
        L => (filter.left_read(), filter.right_read()),
        R => (filter.right_read(), filter.left_read()),
    };
    let probe_cols: Vec<LazyColumnRef> = probe_read
        .iter()
        .map(|&i| probed_batch.get_column(i))
        .collect::<Result<_>>()?;
    let probe_taken = take_columns(
        &probe_cols,
        &probe_indices
            .iter()
            .map(|&index| index as i32)
            .collect::<Int32Array>(),
    )?;
    let build_taken = built_map.gather_columns(
        build_read,
        build_indices.iter().copied().collect::<UInt32Array>(),
    )?;
    let taken = FilterTaken {
        probe: probe_read
            .iter()
            .copied()
            .zip(probe_taken.iter().cloned())
            .collect(),
        build: build_read
            .iter()
            .copied()
            .zip(build_taken.iter().cloned())
            .collect(),
    };
    let eval_cols = match probe_side {
        L => [probe_taken, build_taken].concat(),
        R => [build_taken, probe_taken].concat(),
    };
    Ok((eval_cols, taken))
}

/// Replace output columns whose source column was already gathered at
/// candidate level by [`filter_cols`] with takes over the gathered arrays, so
/// the source is not gathered a second time. `positions` selects the surviving
/// candidate rows; duplicated projections share one output column.
pub fn reuse_candidate_columns(
    cols: &mut [LazyColumnRef],
    projected: &[usize],
    taken: &[(usize, ArrayRef)],
    positions: &Int32Array,
) -> Result<()> {
    let mut shared = HashMap::<usize, LazyColumnRef>::new();
    for (col, &index) in cols.iter_mut().zip(projected) {
        if let Some((_, array)) = taken.iter().find(|(i, _)| *i == index) {
            if let std::collections::hash_map::Entry::Vacant(entry) = shared.entry(index) {
                entry.insert(column(array.clone()).take_lazy(positions)?);
            }
            *col = shared[&index].clone();
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
