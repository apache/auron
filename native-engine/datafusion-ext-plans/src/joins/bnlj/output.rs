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

use arrow::array::{
    Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, UInt32Array, new_null_array,
};
use bitvec::slice::BitSlice;
use datafusion::{common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::{
    arrow::{array_size::max_i32_offset_span, selection::take_cols},
    suggested_batch_mem_size,
};

use super::gather::{BuildChunk, gather_ranges};
use crate::{
    common::{execution_context::WrappedLazyBatchSender, timer_helper::TimerHelper},
    joins::{
        JoinParams, join_filter::JoinFilter, join_hash_map::JoinBuildRows, join_utils::JoinType,
    },
};

fn left_then_right(
    first_is_left: bool,
    first: Vec<ArrayRef>,
    second: Vec<ArrayRef>,
) -> Vec<ArrayRef> {
    let (mut left, right) = if first_is_left {
        (first, second)
    } else {
        (second, first)
    };
    left.extend(right);
    left
}

/// The predicate can repeat a large column even if that column is absent from the output.
pub(super) fn filter_candidates(
    filter: &JoinFilter,
    probe_left: bool,
    build_rows: &JoinBuildRows,
    probe: &RecordBatch,
    probe_indices: &[u32],
    build_indices: &[u32],
) -> Result<Vec<u32>> {
    let (probe_read, build_read) = if probe_left {
        (filter.left_read(), filter.right_read())
    } else {
        (filter.right_read(), filter.left_read())
    };
    let probe_cols = probe_read
        .iter()
        .map(|&i| probe.column(i).clone())
        .collect::<Vec<_>>();
    let ranges = gather_ranges(
        BuildChunk::Whole(build_rows),
        &probe_cols,
        probe_indices,
        build_read,
        |i| Some(build_indices[i]),
    );
    let mut matches = Vec::new();
    for range in ranges {
        let probes = take_cols(&probe_cols, probe_indices[range.clone()].to_vec())?;
        let builds =
            build_rows.gather_columns(build_read, build_indices[range.clone()].to_vec())?;
        let selected =
            filter.apply_rows(left_then_right(probe_left, probes, builds), range.len())?;
        if range.len() == probe_indices.len() {
            return Ok(selected);
        }
        matches.extend(selected.into_iter().map(|i| range.start as u32 + i));
    }
    Ok(matches)
}

/// One probe row compared to a consecutive interval of one build batch.
pub(super) fn filter_one_probe(
    filter: &JoinFilter,
    probe_left: bool,
    probe: &RecordBatch,
    probe_row: usize,
    build: &RecordBatch,
    first_build: usize,
    count: usize,
) -> Result<Vec<u32>> {
    let (probe_read, build_read) = if probe_left {
        (filter.left_read(), filter.right_read())
    } else {
        (filter.right_read(), filter.left_read())
    };
    let probe_cols = probe_read
        .iter()
        .map(|&i| probe.column(i).clone())
        .collect::<Vec<_>>();
    let span = probe_cols
        .iter()
        .map(|col| max_i32_offset_span(col.as_ref(), probe_row..probe_row + 1))
        .max()
        .unwrap_or(0);
    let step = repeat_limit(span, count);
    let mut matches = vec![];
    for start in (0..count).step_by(step) {
        let len = step.min(count - start);
        let probes = take_cols(&probe_cols, UInt32Array::from_value(probe_row as u32, len))?;
        let builds = build_read
            .iter()
            .map(|&i| build.column(i).slice(first_build + start, len))
            .collect();
        let selected = filter.apply_rows(left_then_right(probe_left, probes, builds), len)?;
        if len == count {
            return Ok(selected);
        }
        matches.extend(selected.into_iter().map(|i| start as u32 + i));
    }
    Ok(matches)
}

fn repeat_limit(span: usize, rows: usize) -> usize {
    if span > i32::MAX as usize / rows {
        (suggested_batch_mem_size().min(i32::MAX as usize) / span).max(1)
    } else {
        rows
    }
}

async fn send(
    params: &JoinParams,
    sender: &WrappedLazyBatchSender,
    cols: Vec<ArrayRef>,
    rows: usize,
    build_output_time: &Time,
) -> Result<()> {
    let batch = RecordBatch::try_new_with_options(
        params.projection.schema.clone(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?;
    build_output_time
        .exclude_timer_async(sender.send(Arc::new(batch)))
        .await;
    Ok(())
}

/// An unconditional singleton join repeats one build row and forwards contiguous probe columns.
pub(super) async fn emit_singleton(
    params: &JoinParams,
    probe_left: bool,
    build_rows: &JoinBuildRows,
    sender: &WrappedLazyBatchSender,
    probe: &RecordBatch,
    timer: &Time,
) -> Result<()> {
    let (batch, row) = build_rows.resolve_row(0);
    let build = &build_rows.data_batches()[batch];
    let projection = &params.projection;
    let (probe_cols, build_cols) = if probe_left {
        (
            projection.project_left(probe.columns()),
            projection.project_right(build.columns()),
        )
    } else {
        (
            projection.project_right(probe.columns()),
            projection.project_left(build.columns()),
        )
    };
    let span = build_cols
        .iter()
        .map(|col| max_i32_offset_span(col.as_ref(), row..row + 1))
        .max()
        .unwrap_or(0);
    let limit = params.batch_size.max(1);
    for batch_start in (0..probe.num_rows()).step_by(limit) {
        let batch_len = limit.min(probe.num_rows() - batch_start);
        let step = repeat_limit(span, batch_len);
        for offset in (0..batch_len).step_by(step) {
            let len = step.min(batch_len - offset);
            let probes = if len == probe.num_rows() {
                probe_cols.clone()
            } else {
                probe_cols
                    .iter()
                    .map(|col| col.slice(batch_start + offset, len))
                    .collect()
            };
            let builds = take_cols(&build_cols, UInt32Array::from_value(row as u32, len))?;
            send(
                params,
                sender,
                left_then_right(probe_left, probes, builds),
                len,
                timer,
            )
            .await?;
        }
    }
    Ok(())
}

/// Gather matching pairs and null-extended rows in offset-safe batches.
pub(super) async fn emit_pairs(
    params: &JoinParams,
    probe_left: bool,
    build_rows: BuildChunk<'_>,
    sender: &WrappedLazyBatchSender,
    probe: &RecordBatch,
    probe_indices: Vec<u32>,
    build_indices: UInt32Array,
    build_output_time: &Time,
) -> Result<()> {
    let projection = &params.projection;
    let (mut probe_cols, build_cols) = if probe_left {
        (projection.project_left(probe.columns()), &projection.right)
    } else {
        (projection.project_right(probe.columns()), &projection.left)
    };
    let rows = probe_indices.len();
    let ranges = gather_ranges(build_rows, &probe_cols, &probe_indices, build_cols, |i| {
        build_indices.is_valid(i).then(|| build_indices.value(i))
    });
    let identity = rows == probe.num_rows() && probe_indices.iter().copied().eq(0..rows as u32);
    let take_indices = (!identity).then(|| UInt32Array::from(probe_indices));
    for range in ranges {
        let len = range.len();
        let probes = match &take_indices {
            Some(indices) => take_cols(&probe_cols, indices.slice(range.start, len))?,
            None if len == rows => std::mem::take(&mut probe_cols),
            None => probe_cols
                .iter()
                .map(|col| col.slice(range.start, len))
                .collect(),
        };
        let builds =
            build_rows.gather_columns(build_cols, build_indices.slice(range.start, len))?;
        send(
            params,
            sender,
            left_then_right(probe_left, probes, builds),
            len,
            build_output_time,
        )
        .await?;
    }
    Ok(())
}

/// Emit rows from one input after its matches are known. Each input row occurs at most once.
pub(super) async fn emit_selected(
    params: &JoinParams,
    side_left: bool,
    data: &RecordBatch,
    matched: &BitSlice,
    sender: &WrappedLazyBatchSender,
    build_output_time: &Time,
) -> Result<usize> {
    let limit = params.batch_size.max(1);
    let mut total = 0;
    for start in (0..data.num_rows()).step_by(limit) {
        let len = limit.min(data.num_rows() - start);
        let slice = (len < data.num_rows()).then(|| data.slice(start, len));
        let (cols, rows) = selected_columns(
            params,
            side_left,
            slice.as_ref().unwrap_or(data),
            &matched[start..start + len],
        )?;
        if rows > 0 {
            send(params, sender, cols, rows, build_output_time).await?;
            total += rows;
        }
    }
    Ok(total)
}

fn selected_columns(
    params: &JoinParams,
    side_left: bool,
    data: &RecordBatch,
    matched: &BitSlice,
) -> Result<(Vec<ArrayRef>, usize)> {
    use JoinType::*;

    let projection = &params.projection;
    let mut cols = if side_left {
        projection.project_left(data.columns())
    } else {
        projection.project_right(data.columns())
    };
    let rows = if params.join_type == Existence {
        if let Some(pos) = projection.existence_output {
            cols.insert(
                pos,
                Arc::new(BooleanArray::from_iter(matched.iter().by_vals().map(Some))),
            );
        }
        data.num_rows()
    } else {
        let take_matched = matches!(params.join_type, LeftSemi | RightSemi);
        let rows = if take_matched {
            matched.count_ones()
        } else {
            matched.count_zeros()
        };
        if rows == 0 {
            return Ok((Vec::new(), 0));
        }
        if rows < data.num_rows() {
            let indices: UInt32Array = if take_matched {
                matched.iter_ones().map(|i| i as u32).collect()
            } else {
                matched.iter_zeros().map(|i| i as u32).collect()
            };
            cols = take_cols(&cols, indices)?;
        }
        if matches!(params.join_type, Left | Right | Full) {
            let (opposite, fields) = if side_left {
                (&params.right_schema, &projection.right)
            } else {
                (&params.left_schema, &projection.left)
            };
            let null_cols = fields
                .iter()
                .map(|&i| new_null_array(opposite.field(i).data_type(), rows))
                .collect();
            cols = left_then_right(side_left, cols, null_cols);
        }
        rows
    };
    Ok((cols, rows))
}
