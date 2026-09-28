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

use arrow::array::{ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion_ext_commons::arrow::selection::create_batch_interleaver;

use crate::{
    common::execution_context::WrappedRecordBatchSender,
    joins::{
        Idx, IdxRange, JoinParams,
        join_filter::RunBatch,
        product_blocks,
        smj::group_merge::{GroupAction, GroupMerger},
        stream_cursor::StreamCursor,
    },
    sort_merge_join_exec::Joiner,
};

/// Existence sort-merge join: every left row is output with a boolean
/// indicating whether it matched.
///
/// Because every left row must be output regardless, the rows "waiting for
/// the condition answer" are simply the output buffer itself: a row is
/// marked "unmatched" on entry and flipped by the first surviving pair.
pub struct ExistenceJoiner {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    indices: Vec<Idx>,
    exists: Vec<bool>,
    /// Equality groups whose products have not yet been evaluated; the
    /// `exists` values of rows they cover are not final yet
    pending: RunBatch,
    output_rows: usize,
}

impl ExistenceJoiner {
    pub fn new(join_params: JoinParams, output_sender: Arc<WrappedRecordBatchSender>) -> Self {
        Self {
            join_params,
            output_sender,
            indices: vec![],
            exists: vec![],
            pending: RunBatch::default(),
            output_rows: 0,
        }
    }

    #[inline]
    fn should_flush(&self) -> bool {
        self.indices.len() >= self.join_params.batch_size
    }

    /// Evaluate all currently accumulated groups and flip matched rows.
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
        let exists = &mut this.exists;

        // Only the left side is queried, so the right-side position arrays are never
        // read
        let result = pending.evaluate(&join_filter, cur1.batches(), cur2.batches(), |run| {
            for &l in run.lpos {
                exists[l.wrapping_add(run.lbase) as usize] = true;
            }
        });
        // Even if evaluation fails, this memory is worth retaining for reuse
        this.pending = pending;
        result
    }

    /// Materialize the currently collected rows once their answers are final.
    ///
    /// This is only correct between two equality groups, which is also the
    /// only point where the caller may release the batches addressed here.
    async fn flush(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        _cur2: &mut StreamCursor,
    ) -> Result<()> {
        self.as_mut().evaluate_pending(cur1, _cur2)?;

        let indices = std::mem::take(&mut self.indices);
        let exists = std::mem::take(&mut self.exists);
        let num_rows = indices.len();
        if num_rows == 0 {
            return Ok(());
        }

        let output_time = self.join_params.output_time.clone();
        let output_timer = output_time.timer();

        let batch_interleaver = create_batch_interleaver(cur1.batches(), false)?;
        let cols = batch_interleaver(&indices)?;

        let projection = &self.join_params.projection;
        let mut cols = projection.output_left(cols.columns());
        // The `exists` column is synthesized here rather than read from either
        // side, so it goes back to the position the projection assigned it —
        // which is also why it might not exist at all
        if let Some(pos) = projection.existence_output {
            cols.insert(pos, Arc::new(BooleanArray::from(exists)) as ArrayRef);
        }

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

    /// Collect left rows whose answers are already determined.
    async fn push(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        lranges: &[IdxRange],
        exists: bool,
    ) -> Result<()> {
        for range in lranges {
            for row in range.start..range.end {
                self.indices.push((range.batch_idx, row));
                self.exists.push(exists);
            }
            if self.should_flush() {
                self.as_mut().flush(cur1, cur2).await?;
            }
        }
        Ok(())
    }

    /// Emit an equality group: a left row exists if it satisfies the condition
    /// with at least one right row in the group; if there is no condition, it
    /// unconditionally exists.
    ///
    /// Since we only ask "has it matched at least once", blocks whose left rows
    /// are all already existing can be skipped — this is what lets groups that
    /// amplify the input avoid full evaluation.
    async fn emit_matched(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
        lranges: &[IdxRange],
        rranges: &[IdxRange],
    ) -> Result<()> {
        if self.join_params.residual_filter.is_none() {
            return self.push(cur1, cur2, lranges, true).await;
        }
        let chunk_size = self.join_params.batch_size.max(1);

        // This group's left rows enter the output buffer, and the condition
        // answers are filled back into that same buffer
        let base = self.indices.len();
        for range in lranges {
            for row in range.start..range.end {
                self.indices.push((range.batch_idx, row));
                self.exists.push(false);
            }
        }

        for block in product_blocks(lranges, rranges, chunk_size) {
            let (offset, len) = (block.loffset, block.lrange.len());
            // All rows answerable by this block already have their answer
            if self.exists[base + offset..base + offset + len]
                .iter()
                .all(|&exists| exists)
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
            self.pending.push(&block, base, 0);

            if self.pending.num_pairs() >= chunk_size {
                self.as_mut().evaluate_pending(cur1, cur2)?;
            }
        }

        if self.should_flush() {
            self.as_mut().flush(cur1, cur2).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Joiner for ExistenceJoiner {
    async fn join(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        let mut merger = GroupMerger::new();

        while let Some(action) = merger.next(cur1, cur2).await? {
            // Note: the slices of the groups below live inside the merger and
            // will be invalidated by the next `next` / `release_batches` call
            match action {
                GroupAction::LeftOnly => {
                    self.as_mut()
                        .push(cur1, cur2, merger.lgroup(), false)
                        .await?
                }
                // The right side is never output
                GroupAction::RightOnly => {}
                GroupAction::Both => {
                    self.as_mut()
                        .emit_matched(cur1, cur2, merger.lgroup(), merger.rgroup())
                        .await?
                }
            }

            // The batches addressed by accumulated data may be released now,
            // and group boundaries are the only place to finalize it
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
