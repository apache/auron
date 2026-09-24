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

use std::{pin::Pin, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion_ext_commons::arrow::selection::create_batch_interleaver;

use crate::{
    common::execution_context::WrappedRecordBatchSender,
    joins::{
        Idx, IdxRange, JoinParams,
        join_filter::RunBatch,
        product_blocks, ranges_iter,
        smj::{
            group_merge::{GroupAction, GroupMerger},
            semi_join::SemiJoinSide::{L, R},
        },
        stream_cursor::StreamCursor,
    },
    sort_merge_join_exec::Joiner,
};

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub enum SemiJoinSide {
    L,
    R,
}

#[derive(std::marker::ConstParamTy, Clone, Copy, PartialEq, Eq)]
pub struct JoinerParams {
    join_side: SemiJoinSide,
    semi: bool,
}

impl JoinerParams {
    const fn new(join_side: SemiJoinSide, semi: bool) -> Self {
        Self { join_side, semi }
    }
}

/// Left/right semi and anti sort-merge join.
///
/// Only one side is emitted, and each row at most once, so the merge result
/// boils down to one question: Did this row match? Semi join keeps matched
/// rows, anti join keeps unmatched rows.
pub struct SemiJoiner<const P: JoinerParams> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    /// Rows already decided for output by this join
    indices: Vec<Idx>,
    /// Equality groups whose product has not yet been evaluated
    pending: RunBatch,
    /// Rows contributed by this join's side to pending groups, and whether each
    /// has matched so far
    ///
    /// A row's answer is only final once all its groups have been evaluated,
    /// so both must be retained until then before finalizing, rather than
    /// emitted immediately.
    rows: Vec<Idx>,
    matched: Vec<bool>,
    output_rows: usize,
}

const LEFT_SEMI: JoinerParams = JoinerParams::new(L, true);
const LEFT_ANTI: JoinerParams = JoinerParams::new(L, false);
const RIGHT_SEMI: JoinerParams = JoinerParams::new(R, true);
const RIGHT_ANTI: JoinerParams = JoinerParams::new(R, false);

pub type LeftSemiJoiner = SemiJoiner<LEFT_SEMI>;
pub type LeftAntiJoiner = SemiJoiner<LEFT_ANTI>;
pub type RightSemiJoiner = SemiJoiner<RIGHT_SEMI>;
pub type RightAntiJoiner = SemiJoiner<RIGHT_ANTI>;

impl<const P: JoinerParams> SemiJoiner<P> {
    pub fn new(join_params: JoinParams, output_sender: Arc<WrappedRecordBatchSender>) -> Self {
        Self {
            join_params,
            output_sender,
            indices: vec![],
            pending: RunBatch::default(),
            rows: vec![],
            matched: vec![],
            output_rows: 0,
        }
    }

    #[inline]
    fn should_flush(&self) -> bool {
        self.indices.len() >= self.join_params.batch_size
    }

    /// Evaluate all accumulated groups, marking matched rows on this side.
    fn evaluate_pending(
        self: Pin<&mut Self>,
        cur1: &StreamCursor,
        cur2: &StreamCursor,
    ) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let this = self.get_mut();
        let join_filter = this
            .join_params
            .residual_filter
            .clone()
            .expect("pending groups without a condition");
        let mut pending = std::mem::take(&mut this.pending);
        let matched = &mut this.matched;

        // Only this join's own side is queried, so the opposite side's position array
        // is never read
        let result = pending.evaluate(&join_filter, cur1.batches(), cur2.batches(), |run| {
            let (positions, shift) = match P.join_side {
                L => (run.lpos, run.lbase),
                R => (run.rpos, run.rbase),
            };
            for &pos in positions {
                matched[pos.wrapping_add(shift) as usize] = true;
            }
        });
        // Even if evaluation fails, this memory is worth retaining for reuse
        this.pending = pending;
        result
    }

    /// Finalize pending rows: semi join keeps matched ones, anti join keeps
    /// unmatched ones.
    ///
    /// This is only correct once all pending groups have been evaluated,
    /// because a row's last chance to match may be in a not-yet-evaluated run.
    fn drain_pending_rows(self: Pin<&mut Self>) {
        debug_assert!(self.pending.is_empty());
        let this = self.get_mut();
        for (&idx, &matched) in this.rows.iter().zip(&this.matched) {
            if matched == P.semi {
                this.indices.push(idx);
            }
        }
        this.rows.clear();
        this.matched.clear();
    }

    /// Materialize the rows finalized so far.
    async fn flush_output(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        let indices = std::mem::take(&mut self.indices);
        let num_rows = indices.len();
        if num_rows == 0 {
            return Ok(());
        }

        let output_time = self.join_params.output_time.clone();
        let output_timer = output_time.timer();

        let projection = &self.join_params.projection;
        let cols = match P.join_side {
            L => {
                let batch_interleaver = create_batch_interleaver(cur1.batches(), false)?;
                let cols = batch_interleaver(&indices)?;
                projection.output_left(cols.columns())
            }
            R => {
                let batch_interleaver = create_batch_interleaver(cur2.batches(), false)?;
                let cols = batch_interleaver(&indices)?;
                projection.output_right(cols.columns())
            }
        };
        let output_batch = RecordBatch::try_new_with_options(
            projection.schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        drop(output_timer);

        if output_batch.num_rows() > 0 {
            self.output_rows += output_batch.num_rows();
            self.output_sender.send(output_batch).await;
        }
        Ok(())
    }

    /// Finalize everything pending. Only correct between two equality groups,
    /// which is also the only time the caller can release the batches
    /// addressed here.
    async fn flush(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        self.as_mut().evaluate_pending(cur1, cur2)?;
        self.as_mut().drain_pending_rows();
        self.flush_output(cur1, cur2).await
    }

    /// Emit one equality group.
    ///
    /// Without a condition, every row in the group matches by definition. With
    /// a condition, the group's product is chunked and accumulated,
    /// so groups too small to justify a standalone evaluation can be answered
    /// together with later groups.
    ///
    /// Here we only ask "has it matched at least once",
    /// so blocks whose rows on this side all already have answers can be
    /// skipped entirely — this is what lets groups that amplify the input
    /// avoid being fully evaluated.
    async fn emit_matched(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        lranges: &[IdxRange],
        rranges: &[IdxRange],
    ) -> Result<()> {
        let own_ranges = match P.join_side {
            L => lranges,
            R => rranges,
        };
        if self.join_params.residual_filter.is_none() {
            return match P.semi {
                true => self.push(cur1, cur2, own_ranges).await,
                false => Ok(()),
            };
        }
        let chunk_size = self.join_params.batch_size.max(1);

        let base = self.rows.len();
        self.rows.extend(ranges_iter(own_ranges));
        let num_rows = self.rows.len();
        self.matched.resize(num_rows, false);

        for block in product_blocks(lranges, rranges, chunk_size) {
            let (offset, len) = match P.join_side {
                L => (block.loffset, block.lrange.len()),
                R => (block.roffset, block.rrange.len()),
            };
            // All rows answerable by this block already have answers
            if self.matched[base + offset..base + offset + len]
                .iter()
                .all(|&matched| matched)
            {
                continue;
            }

            // A single evaluation reads only one pair of batches
            if !self
                .pending
                .accepts(block.lrange.batch_idx, block.rrange.batch_idx)
            {
                self.as_mut().evaluate_pending(cur1, cur2)?;
            }
            self.pending.push(&block, base, base);

            if self.pending.num_pairs() >= chunk_size {
                self.as_mut().evaluate_pending(cur1, cur2)?;
            }
        }

        // The entire group is now accumulated into pending, so this is the earliest
        // moment its rows can be finalized — but only worth actually evaluating
        // once enough rows have been accumulated
        if self.rows.len() >= chunk_size {
            self.as_mut().flush(cur1, cur2).await?;
        }
        Ok(())
    }

    /// Emit rows that no opposite-side row could possibly match: the anti join
    /// on that side keeps them; all other cases discard them.
    async fn emit_unmatched(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        side: SemiJoinSide,
        ranges: &[IdxRange],
    ) -> Result<()> {
        if side != P.join_side || P.semi {
            return Ok(());
        }
        if self.join_params.residual_filter.is_none() {
            return self.push(cur1, cur2, ranges).await;
        }

        // Keep these rows behind earlier groups whose condition is still
        // pending. Appending directly to `indices` would let them overtake
        // earlier unmatched rows. They need no condition evaluation.
        self.rows.extend(ranges_iter(ranges));
        let num_rows = self.rows.len();
        self.matched.resize(num_rows, false);
        if num_rows >= self.join_params.batch_size.max(1) {
            self.as_mut().flush(cur1, cur2).await?;
        }
        Ok(())
    }

    async fn push(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        ranges: &[IdxRange],
    ) -> Result<()> {
        for range in ranges {
            for row in range.start..range.end {
                self.indices.push((range.batch_idx, row));
            }
            if self.should_flush() {
                self.as_mut().flush_output(cur1, cur2).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<const P: JoinerParams> Joiner for SemiJoiner<P> {
    async fn join(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        let mut merger = GroupMerger::new();

        while let Some(action) = merger.next(cur1, cur2).await? {
            // Note: the group slices below live inside the merger and are invalidated by
            // the next `next` / `release_batches` call
            match action {
                GroupAction::LeftOnly => {
                    self.as_mut()
                        .emit_unmatched(cur1, cur2, L, merger.lgroup())
                        .await?
                }
                GroupAction::RightOnly => {
                    self.as_mut()
                        .emit_unmatched(cur1, cur2, R, merger.rgroup())
                        .await?
                }
                GroupAction::Both => {
                    self.as_mut()
                        .emit_matched(cur1, cur2, merger.lgroup(), merger.rgroup())
                        .await?
                }
            }

            // Batches addressed by accumulated data may be released now, and group
            // boundaries are the only place to finalize it
            if self.should_flush()
                || cur1.num_buffered_batches() > 2
                || cur2.num_buffered_batches() > 2
            {
                self.as_mut().flush(cur1, cur2).await?;
                merger.release_batches(cur1, cur2);
            }
        }
        self.flush(cur1, cur2).await?;
        Ok(())
    }

    fn num_output_rows(&self) -> usize {
        self.output_rows
    }
}
