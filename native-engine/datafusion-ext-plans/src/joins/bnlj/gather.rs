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

use arrow::array::{Array, ArrayRef, RecordBatch, UInt32Array};
use datafusion::common::Result;
use datafusion_ext_commons::{
    arrow::{array_size::max_i32_offset_span, selection::take_cols},
    suggested_batch_mem_size,
};
use smallvec::{SmallVec, smallvec};

use crate::joins::join_hash_map::JoinBuildRows;

/// Row indices are local when the candidate block comes from one physical batch.
#[derive(Clone, Copy)]
pub(super) enum BuildChunk<'a> {
    Whole(&'a JoinBuildRows),
    Batch(&'a RecordBatch),
}

impl BuildChunk<'_> {
    pub(super) fn gather_columns(
        self,
        fields: &[usize],
        indices: impl Into<UInt32Array>,
    ) -> Result<Vec<ArrayRef>> {
        let indices = indices.into();
        let batch = match self {
            Self::Whole(rows) => return rows.gather_columns(fields, indices),
            Self::Batch(batch) => batch,
        };
        // A local block contains one probe, so surviving build IDs are strictly
        // increasing. Copy tiny selections to avoid pinning a large source batch.
        let contiguous = indices.len() >= batch.num_rows().div_ceil(2)
            && indices.null_count() == 0
            && indices.value(indices.len() - 1) - indices.value(0) == indices.len() as u32 - 1;
        if contiguous {
            return Ok(fields
                .iter()
                .map(|&c| {
                    batch
                        .column(c)
                        .slice(indices.value(0) as usize, indices.len())
                })
                .collect());
        }
        let cols = fields
            .iter()
            .map(|&c| batch.column(c).clone())
            .collect::<Vec<_>>();
        take_cols(&cols, indices)
    }
}

/// Partition keyless join pairs before materializing repeated values.
/// Probe indices are in order and each probe/build pair occurs at most once.
pub(super) fn gather_ranges<'a>(
    build: BuildChunk<'_>,
    probe_cols: impl IntoIterator<Item = &'a ArrayRef>,
    probe_indices: &[u32],
    build_cols: &[usize],
    build_index: impl Fn(usize) -> Option<u32>,
) -> SmallVec<Range<usize>, 1> {
    let len = probe_indices.len();
    let mut ranges = smallvec![0..len];
    if len < 2 {
        return ranges;
    }
    let limit = i32::MAX as usize;
    let (rows, single) = match build {
        BuildChunk::Whole(rows) => (rows.num_rows(), rows.data_batches().len() == 1),
        BuildChunk::Batch(batch) => (batch.num_rows(), true),
    };
    let probe_repeats = rows.clamp(1, len);
    if probe_repeats > 1 {
        for col in probe_cols {
            let span = max_i32_offset_span(col.as_ref(), 0..col.len());
            if span > limit / probe_repeats {
                split_offset_ranges(&mut ranges, |i| {
                    let row = probe_indices[i] as usize;
                    max_i32_offset_span(col.as_ref(), row..row + 1)
                });
            }
        }
    }
    let probes = (probe_indices[len - 1] - probe_indices[0]) as usize + 1;
    let repeats = probes.min(len);
    if repeats == 1 && single {
        return ranges;
    }
    for &c in build_cols {
        let span = match build {
            BuildChunk::Whole(rows) => rows.offset_span(c),
            BuildChunk::Batch(batch) => {
                max_i32_offset_span(batch.column(c).as_ref(), 0..batch.num_rows())
            }
        };
        if span > limit / repeats {
            split_offset_ranges(&mut ranges, |i| {
                let Some(index) = build_index(i) else {
                    return 0;
                };
                let (batch, row) = match build {
                    BuildChunk::Whole(rows) => {
                        let (b, row) = rows.resolve_row(index);
                        (&rows.data_batches()[b], row)
                    }
                    BuildChunk::Batch(batch) => (batch, index as usize),
                };
                max_i32_offset_span(batch.column(c).as_ref(), row..row + 1)
            });
        }
    }
    ranges
}

fn split_offset_ranges(ranges: &mut SmallVec<Range<usize>, 1>, row_span: impl Fn(usize) -> usize) {
    for range in std::mem::take(ranges) {
        // A wide source column alone should not force a smaller gather.
        // Check the selected rows before using the smaller memory target.
        let mut total = 0usize;
        if range.clone().all(|i| {
            total += row_span(i);
            total <= i32::MAX as usize
        }) {
            ranges.push(range);
            continue;
        }
        // For an oversized gather, avoid allocating a nearly 2 GiB temporary.
        // Use the output memory target in offset units (bytes or child entries).
        let target = suggested_batch_mem_size().clamp(1, i32::MAX as usize);
        let mut start = range.start;
        let mut used = 0;
        for i in range.clone() {
            let span = row_span(i);
            if i > start && used + span > target {
                ranges.push(start..i);
                start = i;
                used = 0;
            }
            used += span;
        }
        ranges.push(start..range.end);
    }
}
