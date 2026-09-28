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

use std::{collections::VecDeque, pin::Pin, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion_ext_commons::arrow::selection::create_batch_interleaver;

use crate::{
    common::execution_context::WrappedRecordBatchSender,
    joins::{
        Idx, IdxRange, JoinParams,
        join_filter::RunBatch,
        product_blocks, ranges_iter, ranges_num_rows,
        smj::group_merge::{GroupAction, GroupMerger},
        stream_cursor::StreamCursor,
    },
    sort_merge_join_exec::Joiner,
};

/// Inner / left outer / right outer / full outer sort-merge join.
///
/// The merge itself lives in [`GroupMerger`]; this type only decides what "one
/// equality group" and "a run of unmatched rows" mean for its join type, and
/// collects the resulting `(left row, right row)` pairs until enough are
/// gathered for a batch. Rows filled with nulls are represented by
/// [`Idx::default()`], pointing to the all-null sentinel batch placed at the
/// front of every [`StreamCursor`].
pub struct FullJoiner<const L_OUTER: bool, const R_OUTER: bool> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    /// Pairs accepted by the condition (if any).
    lindices: Vec<Idx>,
    rindices: Vec<Idx>,
    /// Equality groups whose cartesian product has not yet been evaluated.
    pending: RunBatch,
    /// Rows contributed by the outer side to pending groups, and whether each
    /// row has found a match so far.
    ///
    /// A row that never finds a match must be emitted with nulls filled in;
    /// but this cannot be determined until all groups it belongs to are
    /// evaluated, so it must be deferred. Only the outer side needs tracking;
    /// for inner join both are empty.
    lrows: Vec<Idx>,
    lmatched: Vec<bool>,
    rrows: Vec<Idx>,
    rmatched: Vec<bool>,
    /// Ends of equality groups in `lrows` / `rrows`, in merge order. They let
    /// evaluation emit unmatched rows before survivors of a later group,
    /// while still evaluating several small groups together.
    groups: VecDeque<(usize, usize)>,
    drained: (usize, usize),
    output_rows: usize,
}

pub type InnerJoiner = FullJoiner<false, false>;
pub type LeftOuterJoiner = FullJoiner<true, false>;
pub type RightOuterJoiner = FullJoiner<false, true>;
pub type FullOuterJoiner = FullJoiner<true, true>;

impl<const L_OUTER: bool, const R_OUTER: bool> FullJoiner<L_OUTER, R_OUTER> {
    pub fn new(join_params: JoinParams, output_sender: Arc<WrappedRecordBatchSender>) -> Self {
        Self {
            join_params,
            output_sender,
            lindices: vec![],
            rindices: vec![],
            pending: RunBatch::default(),
            lrows: vec![],
            lmatched: vec![],
            rrows: vec![],
            rmatched: vec![],
            groups: VecDeque::new(),
            drained: (0, 0),
            output_rows: 0,
        }
    }

    #[inline]
    fn should_flush(&self) -> bool {
        self.lindices.len() >= self.join_params.batch_size
    }

    /// Evaluate all accumulated groups and collect surviving pairs.
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
        let result = pending.evaluate(&join_filter, cur1.batches(), cur2.batches(), |run| {
            // Runs are reported in merge order. Once a survivor belongs to a
            // later group, all preceding groups have had their final chance
            // to match and must emit their unmatched rows first.
            let lpos = run.lpos[0].wrapping_add(run.lbase) as usize;
            let rpos = run.rpos[0].wrapping_add(run.rbase) as usize;
            while let Some(&(lend, rend)) = this.groups.front() {
                if (L_OUTER && lpos < lend) || (!L_OUTER && R_OUTER && rpos < rend) {
                    break;
                }
                this.groups.pop_front();
                this.drain_group(lend, rend);
            }
            let (lindices, rindices) = (&mut this.lindices, &mut this.rindices);
            let (lmatched, rmatched) = (&mut this.lmatched, &mut this.rmatched);
            lindices.reserve(run.lpos.len());
            rindices.reserve(run.rpos.len());
            for (&l, &r) in run.lpos.iter().zip(run.rpos) {
                lindices.push((run.lbatch, l.wrapping_add(run.lrow) as usize));
                rindices.push((run.rbatch, r.wrapping_add(run.rrow) as usize));
            }
            if L_OUTER {
                for &l in run.lpos {
                    lmatched[l.wrapping_add(run.lbase) as usize] = true;
                }
            }
            if R_OUTER {
                for &r in run.rpos {
                    rmatched[r.wrapping_add(run.rbase) as usize] = true;
                }
            }
        });
        // Keep the allocation for reuse even if evaluation failed.
        this.pending = pending;
        result
    }

    /// Append unmatched rows from one completed group before moving on to the
    /// next group. This never materializes a batch on its own.
    fn drain_group(&mut self, lend: usize, rend: usize) {
        if L_OUTER {
            for pos in self.drained.0..lend {
                if !self.lmatched[pos] {
                    self.lindices.push(self.lrows[pos]);
                    self.rindices.push(Idx::default());
                }
            }
        }
        if R_OUTER {
            for pos in self.drained.1..rend {
                if !self.rmatched[pos] {
                    self.lindices.push(Idx::default());
                    self.rindices.push(self.rrows[pos]);
                }
            }
        }
        self.drained = (lend, rend);
    }

    /// Emit remaining unmatched rows after all pending groups are complete.
    fn drain_unmatched(self: Pin<&mut Self>) {
        debug_assert!(self.pending.is_empty());
        let this = self.get_mut();
        while let Some((lend, rend)) = this.groups.pop_front() {
            this.drain_group(lend, rend);
        }
        this.lrows.clear();
        this.lmatched.clear();
        this.rrows.clear();
        this.rmatched.clear();
        this.drained = (0, 0);
    }

    /// Materialize the pairs collected so far. Safe to call at any time,
    /// since collected pairs are already final.
    async fn flush_output(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        let lindices = std::mem::take(&mut self.lindices);
        let rindices = std::mem::take(&mut self.rindices);
        assert_eq!(lindices.len(), rindices.len());
        if lindices.is_empty() {
            return Ok(());
        }
        let num_rows = lindices.len();

        let output_time = self.join_params.output_time.clone();
        let output_timer = output_time.timer();

        let lbatch_interleaver = create_batch_interleaver(cur1.batches(), false)?;
        let rbatch_interleaver = create_batch_interleaver(cur2.batches(), false)?;
        let lcols = lbatch_interleaver(&lindices)?;
        let rcols = rbatch_interleaver(&rindices)?;

        let projection = &self.join_params.projection;
        let output_batch = RecordBatch::try_new_with_options(
            projection.schema.clone(),
            [
                projection.output_left(lcols.columns()),
                projection.output_right(rcols.columns()),
            ]
            .concat(),
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        drop(output_timer);

        if output_batch.num_rows() > 0 {
            self.output_rows += output_batch.num_rows();
            self.output_sender.send(output_batch).await;
        }
        Ok(())
    }

    /// Finalize all pending work: evaluate pending groups, fill nulls for the
    /// unmatched rows they leave behind, then materialize the result.
    ///
    /// Only correct at a boundary between equality groups, which is also the
    /// only point where the caller may release the batches addressed here.
    async fn flush(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        self.as_mut().evaluate_pending(cur1, cur2)?;
        self.as_mut().drain_unmatched();
        self.flush_output(cur1, cur2).await
    }

    async fn emit_left_only(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        lranges: &[IdxRange],
    ) -> Result<()> {
        if L_OUTER {
            self.as_mut().evaluate_pending(cur1, cur2)?;
            self.as_mut().drain_unmatched();
            for range in lranges {
                for row in range.start..range.end {
                    self.lindices.push((range.batch_idx, row));
                    self.rindices.push(Idx::default());
                }
                if self.should_flush() {
                    self.as_mut().flush_output(cur1, cur2).await?;
                }
            }
        }
        Ok(())
    }

    async fn emit_right_only(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        rranges: &[IdxRange],
    ) -> Result<()> {
        if R_OUTER {
            self.as_mut().evaluate_pending(cur1, cur2)?;
            self.as_mut().drain_unmatched();
            for range in rranges {
                for row in range.start..range.end {
                    self.lindices.push(Idx::default());
                    self.rindices.push((range.batch_idx, row));
                }
                if self.should_flush() {
                    self.as_mut().flush_output(cur1, cur2).await?;
                }
            }
        }
        Ok(())
    }

    /// Emit an equality group.
    ///
    /// Without a condition, every pair in the cartesian product is collected
    /// directly. With a condition, the group's product is split into chunks
    /// and accumulated, so the product is never materialized, and groups too
    /// small to warrant a separate evaluation can be answered together with
    /// later groups.
    async fn emit_matched(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        lranges: &[IdxRange],
        rranges: &[IdxRange],
    ) -> Result<()> {
        if self.join_params.residual_filter.is_none() {
            return self.emit_product(cur1, cur2, lranges, rranges).await;
        }
        let chunk_size = self.join_params.batch_size.max(1);

        // This group's rows enter the pending indexing scheme; condition results are
        // back-filled using these indices.
        let lbase = self.lrows.len();
        let rbase = self.rrows.len();
        if L_OUTER {
            self.lrows.extend(ranges_iter(lranges));
            let num_rows = lbase + ranges_num_rows(lranges);
            self.lmatched.resize(num_rows, false);
        }
        if R_OUTER {
            self.rrows.extend(ranges_iter(rranges));
            let num_rows = rbase + ranges_num_rows(rranges);
            self.rmatched.resize(num_rows, false);
        }

        if L_OUTER || R_OUTER {
            let ends = (self.lrows.len(), self.rrows.len());
            self.groups.push_back(ends);
        }

        for block in product_blocks(lranges, rranges, chunk_size) {
            // Each evaluation reads only a single pair of batches.
            if !self
                .pending
                .accepts(block.lrange.batch_idx, block.rrange.batch_idx)
            {
                self.as_mut().evaluate_pending(cur1, cur2)?;
            }
            self.pending.push(&block, lbase, rbase);

            if self.pending.num_pairs() >= chunk_size {
                self.as_mut().evaluate_pending(cur1, cur2)?;
                if self.should_flush() {
                    self.as_mut().flush_output(cur1, cur2).await?;
                }
            }
        }

        // The whole group is now in pending, so this is the earliest point at
        // which unmatched rows can be identified — but only worth evaluating
        // once enough rows have accumulated.
        if self.lrows.len() >= chunk_size || self.rrows.len() >= chunk_size {
            self.as_mut().flush(cur1, cur2).await?;
        }
        Ok(())
    }

    async fn emit_product(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        lranges: &[IdxRange],
        rranges: &[IdxRange],
    ) -> Result<()> {
        // Fast path for one-to-one join.
        if let ([lrange], [rrange]) = (lranges, rranges) {
            if lrange.len() == 1 && rrange.len() == 1 {
                self.lindices.push((lrange.batch_idx, lrange.start));
                self.rindices.push((rrange.batch_idx, rrange.start));
                return Ok(());
            }
        }

        let rnum = ranges_num_rows(rranges);
        for lidx in ranges_iter(lranges) {
            let len = self.lindices.len();
            self.lindices.resize(len + rnum, lidx);
            self.rindices.extend(ranges_iter(rranges));
            if self.should_flush() {
                self.as_mut().flush_output(cur1, cur2).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl<const L_OUTER: bool, const R_OUTER: bool> Joiner for FullJoiner<L_OUTER, R_OUTER> {
    async fn join(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        let mut merger = GroupMerger::new();

        while let Some(action) = merger.next(cur1, cur2).await? {
            // Note: the group slices below live inside the merger and are
            // invalidated by the next `next` / `release_batches` call.
            match action {
                GroupAction::LeftOnly => {
                    self.as_mut()
                        .emit_left_only(cur1, cur2, merger.lgroup())
                        .await?
                }
                GroupAction::RightOnly => {
                    self.as_mut()
                        .emit_right_only(cur1, cur2, merger.rgroup())
                        .await?
                }
                GroupAction::Both => {
                    self.as_mut()
                        .emit_matched(cur1, cur2, merger.lgroup(), merger.rgroup())
                        .await?
                }
            }

            // Batches addressed by accumulated state may be released now; group boundaries
            // are the only place to finalize it.
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
