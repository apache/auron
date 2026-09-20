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

//! Merges two sorted join inputs at the granularity of *equality groups*.
//!
//! Sort-merge join used to advance through both inputs row by row, deciding at
//! each step which side to advance. Switching to *group*-by-group processing —
//! a group being a contiguous run of rows sharing the same key — lets the
//! joiner hand the entire `(left group, right group)` to [`JoinFilter`] at
//! once, and turns the three merge outcomes into three explicit actions:
//! [`GroupAction::LeftOnly`], [`GroupAction::RightOnly`] and
//! [`GroupAction::Both`].
//!
//! # No row-level materialization
//!
//! Buffered rows and dispatched groups are both [`IdxRange`], so buffering a
//! whole batch is just pushing one range, and a group is usually one range too.
//! Reading a batch is therefore O(1), and the merge only touches the rows it
//! must compare.
//!
//! # Deciding what to emit
//!
//! Groups are found lazily, first comparing the heads of both sides:
//!
//! * Unequal heads — all rows on the smaller side below the other side's head
//!   can never match, because the other input only grows upward from its head.
//!   Those rows are all emitted at once, regardless of group boundaries, and
//!   neither side's group needs to be complete.
//! * Equal heads — both runs must be confirmed finished before pairing,
//!   otherwise missing rows would be dropped. The merge keeps reading input
//!   until then, which is exactly the mechanism that carries a group across
//!   batch boundaries.
//!
//! Both "run of equal keys" and "run below the other side's head" are located
//! with exponential search ([`GroupedSide::count_while`]), so a one-to-one join
//! pays a single comparison, and a large group pays logarithmic comparisons,
//! rather than one per row.
//!
//! An exhausted side is treated as "unknown, keep reading input", unless its
//! stream has ended — in which case everything remaining on the other side is
//! unmatched and emitted at once.
//!
//! # Null keys
//!
//! Null keys join with nothing, not even another null key, so a run of null
//! keys is emitted as unmatched as soon as it reaches the head, without
//! comparing to the other side at all.
//!
//! # Buffer lifetime
//!
//! Rows reference batches buffered by [`StreamCursor`], so a batch can only be
//! dropped when no pending row references it. [`GroupMerger::release_batches`]
//! computes this bound and rebases the ranges it holds, rather than relying on
//! the caller to figure out which batches are still alive.
//!
//! [`JoinFilter`]: crate::joins::join_filter::JoinFilter

use std::{cmp::Ordering, collections::VecDeque};

use datafusion::common::Result;

use crate::{
    cur_forward,
    joins::{Idx, IdxRange, stream_cursor::StreamCursor},
};

/// Buffered, not-yet-emitted rows on one side.
#[derive(Default)]
struct GroupedSide {
    /// Pending rows in stream order; `segs[0].start` is the first unconsumed
    /// row. No segment is ever empty.
    segs: VecDeque<IdxRange>,
    num_rows: usize,
}

impl GroupedSide {
    #[inline]
    fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    #[inline]
    fn head(&self) -> Idx {
        let seg = self.segs[0];
        (seg.batch_idx, seg.start)
    }

    #[inline]
    fn idx_at(&self, n: usize) -> Idx {
        // Fast path taken whenever no group spans a batch boundary.
        let front = self.segs[0];
        if n < front.len() {
            return (front.batch_idx, front.start + n);
        }
        let mut n = n - front.len();
        for seg in self.segs.iter().skip(1) {
            if n < seg.len() {
                return (seg.batch_idx, seg.start + n);
            }
            n -= seg.len();
        }
        unreachable!("index out of range")
    }

    /// Number of leading rows satisfying a predicate that is "false forever
    /// once false". `pred` must hold for the head row.
    ///
    /// Uses exponential search rather than binary search, so a length-one run —
    /// the common case — costs a single probe instead of a logarithmic scan
    /// over the whole buffer.
    fn count_while(&self, mut pred: impl FnMut(Idx) -> bool) -> usize {
        debug_assert!(!self.is_empty());
        let num_rows = self.num_rows;
        let mut lo = 0; // `pred` holds here
        let mut step = 1;
        while lo + step < num_rows && pred(self.idx_at(lo + step)) {
            lo += step;
            step *= 2;
        }
        // Either `hi == num_rows`, or `pred` is false at `hi`.
        let mut hi = (lo + step).min(num_rows);
        while lo + 1 < hi {
            let mid = lo + (hi - lo) / 2;
            match pred(self.idx_at(mid)) {
                true => lo = mid,
                false => hi = mid,
            }
        }
        hi
    }

    /// Count consecutive null keys without assuming that nullness is monotone.
    /// For composite keys, a null in a later column can occur again after a
    /// non-null key when an earlier column changes.
    fn count_null_prefix(&self, cur: &StreamCursor) -> usize {
        self.segs
            .iter()
            .flat_map(IdxRange::iter)
            .take_while(|&idx| cur.is_null_key(idx))
            .count()
    }

    /// Buffers the cursor's current whole batch and fetches the next, so that
    /// the caller can later tell — via [`StreamCursor::finished`] — whether
    /// a run that reached the end of the buffer might still continue.
    async fn fill_one_batch(&mut self, cur: &mut StreamCursor) -> Result<()> {
        debug_assert!(!cur.finished());
        let (batch_idx, start) = cur.cur_idx();
        let end = cur.cur_batch_num_rows();
        self.segs.push_back(IdxRange {
            batch_idx,
            start,
            end,
        });
        self.num_rows += end - start;
        cur.seek_to_current_batch_end();
        cur_forward!(cur);
        Ok(())
    }

    /// Moves the first `n` rows out into `out`.
    fn take(&mut self, n: usize, out: &mut Vec<IdxRange>) {
        debug_assert!(n <= self.num_rows);
        out.clear();
        self.num_rows -= n;

        let mut left = n;
        while left > 0 {
            let seg = self.segs.front_mut().expect("no segment left");
            let taken = left.min(seg.len());
            out.push(IdxRange {
                batch_idx: seg.batch_idx,
                start: seg.start,
                end: seg.start + taken,
            });
            seg.start += taken;
            if seg.is_empty() {
                self.segs.pop_front();
            }
            left -= taken;
        }
    }

    /// Smallest batch index still referenced.
    fn min_referenced_batch(&self, cur: &StreamCursor) -> usize {
        self.segs
            .front()
            .map(|seg| seg.batch_idx)
            .unwrap_or(cur.cur_idx().0)
    }

    fn rebase(&mut self, shift: usize) {
        if shift > 0 {
            for seg in &mut self.segs {
                seg.batch_idx -= shift;
            }
        }
    }
}

/// One step of the merge. The rows it covers are in
/// [`GroupMerger::lgroup`] and [`GroupMerger::rgroup`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupAction {
    /// Two runs of equal, non-null keys.
    Both,
    /// Left rows that no right row can match; not necessarily a single group.
    LeftOnly,
    /// Right rows that no left row can match; not necessarily a single group.
    RightOnly,
}

#[derive(Default)]
pub struct GroupMerger {
    l: GroupedSide,
    r: GroupedSide,
    lgroup: Vec<IdxRange>,
    rgroup: Vec<IdxRange>,
}

impl GroupMerger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Rows covered by the last [`GroupAction::LeftOnly`] or
    /// [`GroupAction::Both`].
    #[inline]
    pub fn lgroup(&self) -> &[IdxRange] {
        &self.lgroup
    }

    /// Rows covered by the last [`GroupAction::RightOnly`] or
    /// [`GroupAction::Both`].
    #[inline]
    pub fn rgroup(&self) -> &[IdxRange] {
        &self.rgroup
    }

    /// Reads enough input to decide the next action, or reports both inputs
    /// exhausted.
    pub async fn next(
        &mut self,
        lcur: &mut StreamCursor,
        rcur: &mut StreamCursor,
    ) -> Result<Option<GroupAction>> {
        loop {
            // Null keys match nothing, so their run needn't be complete or
            // compared to the other side.
            if !self.l.is_empty() && lcur.is_null_key(self.l.head()) {
                let n = self.l.count_null_prefix(lcur);
                self.l.take(n, &mut self.lgroup);
                return Ok(Some(GroupAction::LeftOnly));
            }
            if !self.r.is_empty() && rcur.is_null_key(self.r.head()) {
                let n = self.r.count_null_prefix(rcur);
                self.r.take(n, &mut self.rgroup);
                return Ok(Some(GroupAction::RightOnly));
            }

            match (self.l.is_empty(), self.r.is_empty()) {
                (true, true) => match (lcur.finished(), rcur.finished()) {
                    (true, true) => return Ok(None),
                    (false, _) => self.l.fill_one_batch(lcur).await?,
                    (true, false) => self.r.fill_one_batch(rcur).await?,
                },
                // An exhausted side knows nothing about not-yet-arriving keys,
                // unless its stream has ended — then everything on the other
                // side is unmatched.
                (false, true) => match rcur.finished() {
                    true => {
                        self.l.take(self.l.num_rows, &mut self.lgroup);
                        return Ok(Some(GroupAction::LeftOnly));
                    }
                    false => self.r.fill_one_batch(rcur).await?,
                },
                (true, false) => match lcur.finished() {
                    true => {
                        self.r.take(self.r.num_rows, &mut self.rgroup);
                        return Ok(Some(GroupAction::RightOnly));
                    }
                    false => self.l.fill_one_batch(lcur).await?,
                },
                (false, false) => {
                    let lkey = lcur.key(self.l.head());
                    let rkey = rcur.key(self.r.head());
                    match lkey.cmp(&rkey) {
                        Ordering::Less => {
                            let n = self.l.count_while(|idx| lcur.key(idx) < rkey);
                            self.l.take(n, &mut self.lgroup);
                            return Ok(Some(GroupAction::LeftOnly));
                        }
                        Ordering::Greater => {
                            let n = self.r.count_while(|idx| rcur.key(idx) < lkey);
                            self.r.take(n, &mut self.rgroup);
                            return Ok(Some(GroupAction::RightOnly));
                        }
                        Ordering::Equal => {
                            // A run that reached the end of the buffer may
                            // still continue in a not-yet-read batch.
                            let ln = self.l.count_while(|idx| lcur.key(idx) == lkey);
                            if ln == self.l.num_rows && !lcur.finished() {
                                self.l.fill_one_batch(lcur).await?;
                                continue;
                            }
                            let rn = self.r.count_while(|idx| rcur.key(idx) == rkey);
                            if rn == self.r.num_rows && !rcur.finished() {
                                self.r.fill_one_batch(rcur).await?;
                                continue;
                            }
                            self.l.take(ln, &mut self.lgroup);
                            self.r.take(rn, &mut self.rgroup);
                            return Ok(Some(GroupAction::Both));
                        }
                    }
                }
            }
        }
    }

    /// Releases batches no longer referenced by either side.
    ///
    /// Precondition: the caller has already materialized the output indices it
    /// collected, because those indices will not be rebased by this method (nor
    /// by any other). This method invalidates [`Self::lgroup`] and
    /// [`Self::rgroup`].
    pub fn release_batches(&mut self, lcur: &mut StreamCursor, rcur: &mut StreamCursor) {
        self.lgroup.clear();
        self.rgroup.clear();
        let shift = lcur.clean_batches_before(self.l.min_referenced_batch(lcur));
        self.l.rebase(shift);
        let shift = rcur.clean_batches_before(self.r.min_referenced_batch(rcur));
        self.r.rebase(shift);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, Int32Array, RecordBatch},
        compute::SortOptions,
        datatypes::{DataType, Field, Schema},
        row::RowConverter,
    };
    use datafusion::{
        physical_expr::{PhysicalSortExpr, expressions::Column},
        physical_plan::metrics::Time,
    };

    use super::*;
    use crate::{
        common::key_rows_output::{RecordBatchWithKeyRows, RecordBatchWithKeyRowsStreamAdapter},
        joins::ranges_iter,
    };

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]))
    }

    /// Builds a cursor from a single Int32 key column; each batch is one Vec.
    fn cursor(batches: Vec<Vec<Option<i32>>>) -> StreamCursor {
        let schema = schema();
        let sort_options = SortOptions::default();
        let converter = RowConverter::new(vec![arrow::row::SortField::new_with_options(
            DataType::Int32,
            sort_options,
        )])
        .unwrap();

        let converter = Arc::new(parking_lot::Mutex::new(converter));
        let batches = batches
            .into_iter()
            .map(|values| {
                let col = Arc::new(Int32Array::from(values)) as ArrayRef;
                let key_rows = Arc::new(converter.lock().convert_columns(&[col.clone()]).unwrap());
                let batch = RecordBatch::try_new(schema.clone(), vec![col]).unwrap();
                Ok(RecordBatchWithKeyRows::new(
                    batch,
                    key_rows,
                    converter.clone(),
                ))
            })
            .collect::<Vec<_>>();

        let stream = RecordBatchWithKeyRowsStreamAdapter::new(
            futures::stream::iter(batches),
            schema,
            vec![PhysicalSortExpr::new(
                Arc::new(Column::new("k", 0)),
                sort_options,
            )],
        );
        StreamCursor::try_new(Box::pin(stream), Time::default(), &[DataType::Int32]).unwrap()
    }

    /// Runs the merge to completion, describing each action by the key values
    /// it covers.
    async fn actions(
        lbatches: Vec<Vec<Option<i32>>>,
        rbatches: Vec<Vec<Option<i32>>>,
    ) -> Vec<String> {
        actions_impl(lbatches, rbatches).await.unwrap()
    }

    async fn actions_impl(
        lbatches: Vec<Vec<Option<i32>>>,
        rbatches: Vec<Vec<Option<i32>>>,
    ) -> Result<Vec<String>> {
        let (mut lcur, mut rcur) = (cursor(lbatches), cursor(rbatches));
        cur_forward!(lcur);
        cur_forward!(rcur);

        let describe = |cur: &StreamCursor, ranges: &[IdxRange]| {
            ranges_iter(ranges)
                .map(|(b, r)| {
                    let col = cur.batches()[b].column(0);
                    let col = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    match col.is_valid(r) {
                        true => col.value(r).to_string(),
                        false => "N".to_owned(),
                    }
                })
                .collect::<Vec<_>>()
                .join(",")
        };

        let mut merger = GroupMerger::new();
        let mut out = vec![];
        while let Some(action) = merger.next(&mut lcur, &mut rcur).await? {
            out.push(match action {
                GroupAction::LeftOnly => format!("L[{}]", describe(&lcur, merger.lgroup())),
                GroupAction::RightOnly => format!("R[{}]", describe(&rcur, merger.rgroup())),
                GroupAction::Both => format!(
                    "B[{}|{}]",
                    describe(&lcur, merger.lgroup()),
                    describe(&rcur, merger.rgroup()),
                ),
            });
            merger.release_batches(&mut lcur, &mut rcur);
        }
        Ok(out)
    }

    fn v(values: &[i32]) -> Vec<Option<i32>> {
        values.iter().map(|&x| Some(x)).collect()
    }

    #[tokio::test]
    async fn test_single_batch() {
        assert_eq!(
            actions(vec![v(&[1, 2, 2, 4])], vec![v(&[2, 3])]).await,
            vec!["L[1]", "B[2,2|2]", "R[3]", "L[4]"]
        );
    }

    #[tokio::test]
    async fn test_group_spanning_batches() {
        // The group for key 2 is split across 3 left batches and 2 right batches.
        assert_eq!(
            actions(
                vec![v(&[1, 2]), v(&[2, 2]), v(&[2, 5])],
                vec![v(&[2]), v(&[2, 6])],
            )
            .await,
            vec!["L[1]", "B[2,2,2,2|2,2]", "L[5]", "R[6]"]
        );
    }

    #[tokio::test]
    async fn test_group_ends_exactly_at_batch_boundary() {
        assert_eq!(
            actions(vec![v(&[1, 1]), v(&[2, 2])], vec![v(&[1]), v(&[1, 2])]).await,
            vec!["B[1,1|1,1]", "B[2,2|2]"]
        );
    }

    #[tokio::test]
    async fn test_unmatched_open_group_is_emitted_in_pieces() {
        // The left run of key 1 has no counterpart, so it never needs to be
        // buffered as a whole: each batch is emitted as it arrives.
        assert_eq!(
            actions(vec![v(&[1, 1]), v(&[1, 1]), v(&[1, 3])], vec![v(&[2])]).await,
            vec!["L[1,1]", "L[1,1]", "L[1]", "R[2]", "L[3]"]
        );
    }

    #[tokio::test]
    async fn test_one_side_finished_with_equal_keys_remaining() {
        // The left stream ends on key 3 while the right group for key 3 is still
        // open across three batches.
        assert_eq!(
            actions(vec![v(&[1, 2, 3])], vec![v(&[3]), v(&[3]), v(&[3, 4])]).await,
            vec!["L[1,2]", "B[3|3,3,3]", "R[4]"]
        );
    }

    #[tokio::test]
    async fn test_null_keys_never_match() {
        assert_eq!(
            actions(vec![vec![None, None, Some(1)]], vec![vec![None, Some(1)]]).await,
            vec!["L[N,N]", "R[N]", "B[1|1]"]
        );
    }

    #[tokio::test]
    async fn test_composite_null_keys_do_not_skip_non_null_rows() -> Result<()> {
        fn composite_cursor(values: Vec<(Option<i32>, Option<i32>)>) -> StreamCursor {
            let schema = Arc::new(Schema::new(vec![
                Field::new("k1", DataType::Int32, true),
                Field::new("k2", DataType::Int32, true),
            ]));
            let columns = vec![
                Arc::new(Int32Array::from_iter(values.iter().map(|row| row.0))) as ArrayRef,
                Arc::new(Int32Array::from_iter(values.iter().map(|row| row.1))) as ArrayRef,
            ];
            let converter = RowConverter::new(vec![
                arrow::row::SortField::new(DataType::Int32),
                arrow::row::SortField::new(DataType::Int32),
            ])
            .unwrap();
            let converter = Arc::new(parking_lot::Mutex::new(converter));
            let rows = Arc::new(converter.lock().convert_columns(&columns).unwrap());
            let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
            let stream = RecordBatchWithKeyRowsStreamAdapter::new(
                futures::stream::iter(vec![Ok(RecordBatchWithKeyRows::new(
                    batch,
                    rows,
                    converter.clone(),
                ))]),
                schema,
                vec![
                    PhysicalSortExpr::new(Arc::new(Column::new("k1", 0)), SortOptions::default()),
                    PhysicalSortExpr::new(Arc::new(Column::new("k2", 1)), SortOptions::default()),
                ],
            );
            StreamCursor::try_new(
                Box::pin(stream),
                Time::default(),
                &[DataType::Int32, DataType::Int32],
            )
            .unwrap()
        }

        // Nullness is true, true, false, true despite lexicographically sorted keys.
        // Exponential search previously skipped the valid (3, 3) key.
        for swap in [false, true] {
            let many = vec![
                (Some(1), None),
                (Some(2), None),
                (Some(3), Some(3)),
                (Some(4), None),
            ];
            let one = vec![(Some(3), Some(3))];
            let (mut lcur, mut rcur) = match swap {
                false => (composite_cursor(many), composite_cursor(one)),
                true => (composite_cursor(one), composite_cursor(many)),
            };
            cur_forward!(lcur);
            cur_forward!(rcur);
            let mut merger = GroupMerger::new();
            let mut matched = 0;
            while let Some(action) = merger.next(&mut lcur, &mut rcur).await.unwrap() {
                if action == GroupAction::Both {
                    matched +=
                        ranges_iter(merger.lgroup()).count() * ranges_iter(merger.rgroup()).count();
                }
                merger.release_batches(&mut lcur, &mut rcur);
            }
            assert_eq!(matched, 1, "swap={swap}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_empty_streams() {
        assert_eq!(actions(vec![], vec![]).await, Vec::<String>::new());
        assert_eq!(actions(vec![v(&[1, 2])], vec![]).await, vec!["L[1,2]"]);
        assert_eq!(actions(vec![], vec![v(&[1, 2])]).await, vec!["R[1,2]"]);
    }

    #[tokio::test]
    async fn test_disjoint_keys() {
        assert_eq!(
            actions(vec![v(&[1, 3, 5])], vec![v(&[2, 4, 6])]).await,
            vec!["L[1]", "R[2]", "L[3]", "R[4]", "L[5]", "R[6]"]
        );
    }

    /// A long stretch of unmatched rows is emitted in one action, not one per
    /// group.
    #[tokio::test]
    async fn test_unmatched_stretch_is_emitted_at_once() {
        assert_eq!(
            actions(vec![v(&[1, 2, 3, 4, 5, 9])], vec![v(&[5, 9])]).await,
            vec!["L[1,2,3,4]", "B[5|5]", "B[9|9]"]
        );
    }
}
