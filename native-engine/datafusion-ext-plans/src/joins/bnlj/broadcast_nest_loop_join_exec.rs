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

use std::{borrow::Borrow, pin::Pin, sync::Arc};

use arrow::array::{RecordBatch, UInt32Array, UInt32Builder};
use async_trait::async_trait;
use bitvec::vec::BitVec;
use datafusion::{
    common::{JoinSide, Result, ScalarValue},
    physical_expr::expressions::Literal,
    physical_plan::metrics::Time,
};

use super::gather::BuildChunk;
use super::output::{
    emit_pairs, emit_selected, emit_singleton, filter_candidates, filter_one_probe,
};
use crate::{
    broadcast_join_exec::Joiner,
    common::execution_context::WrappedRecordBatchSender,
    joins::{JoinParams, join_hash_map::JoinBuildRows, join_utils::JoinType},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Output {
    Pairs {
        probe_outer: bool,
        build_outer: bool,
    },
    ProbeSelection,
    BuildSelection,
}

impl Output {
    fn new(kind: JoinType, probe_left: bool) -> Self {
        use JoinType::*;
        match kind {
            Inner | Left | Right | Full => Self::Pairs {
                probe_outer: kind == Full || kind == if probe_left { Left } else { Right },
                build_outer: kind == Full || kind == if probe_left { Right } else { Left },
            },
            LeftSemi | LeftAnti | Existence if probe_left => Self::ProbeSelection,
            RightSemi | RightAnti if !probe_left => Self::ProbeSelection,
            _ => Self::BuildSelection,
        }
    }
}

enum PairIndices {
    OneProbe { row: u32, first_build: u32 },
    Explicit { probes: Vec<u32>, builds: Vec<u32> },
}

impl PairIndices {
    fn get(&self, pos: u32) -> (u32, u32) {
        match self {
            Self::OneProbe { row, first_build } => (*row, *first_build + pos),
            Self::Explicit { probes, builds } => (probes[pos as usize], builds[pos as usize]),
        }
    }
}

pub(crate) struct BnljJoiner {
    params: JoinParams,
    build: Arc<dyn Borrow<JoinBuildRows> + Send + Sync>,
    sender: Arc<WrappedRecordBatchSender>,
    probe_left: bool,
    output: Output,
    can_match: bool,
    build_matched: BitVec,
    output_rows: usize,
}

impl BnljJoiner {
    pub(crate) fn new(
        broadcast_side: JoinSide,
        mut params: JoinParams,
        build: Arc<dyn Borrow<JoinBuildRows> + Send + Sync>,
        sender: Arc<WrappedRecordBatchSender>,
    ) -> Self {
        let rows = build.as_ref().borrow();
        let probe_left = broadcast_side == JoinSide::Right;
        let output = Output::new(params.join_type, probe_left);
        let build_matched = match output {
            Output::BuildSelection
            | Output::Pairs {
                build_outer: true, ..
            } => BitVec::repeat(false, rows.num_rows()),
            _ => BitVec::new(),
        };
        let mut can_match = !rows.is_empty();
        if let Some(filter) = &params.residual_filter
            && let Some(literal) = filter.filter_expr().downcast_ref::<Literal>()
            && let ScalarValue::Boolean(value) = literal.value()
        {
            can_match &= value.unwrap_or(false);
            params.residual_filter = None;
        }
        Self {
            params,
            build,
            sender,
            probe_left,
            output,
            can_match,
            build_matched,
            output_rows: 0,
        }
    }

    fn rows(&self) -> &JoinBuildRows {
        self.build.as_ref().borrow()
    }

    async fn send_pairs(
        &mut self,
        probe: &RecordBatch,
        chunk: BuildChunk<'_>,
        probes: Vec<u32>,
        builds: UInt32Array,
        timer: &Time,
    ) -> Result<()> {
        let count = probes.len();
        if count > 0 {
            emit_pairs(
                &self.params,
                self.probe_left,
                chunk,
                &self.sender,
                probe,
                probes,
                builds,
                timer,
            )
            .await?;
            self.output_rows += count;
        }
        Ok(())
    }

    async fn join_candidates(
        &mut self,
        probe: &RecordBatch,
        probe_outer: bool,
        timer: &Time,
    ) -> Result<()> {
        let owner = self.build.clone();
        let rows: &JoinBuildRows = owner.as_ref().borrow();
        let build_rows = rows.num_rows();
        let probe_rows = probe.num_rows();
        let limit = self.params.batch_size.max(1);
        let mut probe_matched = BitVec::new();
        if self.params.residual_filter.is_some()
            && (probe_outer || self.output == Output::ProbeSelection)
        {
            probe_matched.resize(probe_rows, false);
        }

        // Keep probe order. If one build range needs several evaluations, finish
        // that probe row before starting the next one.
        let rows_per_block = (limit / build_rows).max(1);
        for first_probe in (0..probe_rows).step_by(rows_per_block) {
            let probe_range = first_probe..probe_rows.min(first_probe + rows_per_block);
            let mut next_build = 0;
            while next_build < build_rows {
                let mut count = limit.min(build_rows - next_build);
                let mut local_start = next_build;
                let mut chunk = BuildChunk::Whole(rows);
                // For a single probe, emit large physical batches in their original
                // order. Keep small adjacent fragments in a common candidate block.
                if rows_per_block == 1 && rows.data_batches().len() > 1 {
                    let (i, local) = rows.resolve_row(next_build as u32);
                    let batch = &rows.data_batches()[i];
                    if batch.num_rows() >= limit.div_ceil(2) {
                        chunk = BuildChunk::Batch(batch);
                        local_start = local;
                        count = count.min(batch.num_rows() - local);
                    }
                }
                let global_base = next_build - local_start;
                next_build += count;
                let build_range = local_start as u32..(local_start + count) as u32;
                let (selected, indices) = if let (Some(filter), BuildChunk::Batch(batch)) =
                    (&self.params.residual_filter, chunk)
                {
                    let selected = filter_one_probe(
                        filter,
                        self.probe_left,
                        probe,
                        first_probe,
                        batch,
                        local_start,
                        count,
                    )?;
                    (
                        selected,
                        PairIndices::OneProbe {
                            row: first_probe as u32,
                            first_build: local_start as u32,
                        },
                    )
                } else {
                    let pairs = probe_range.len() * build_range.len();
                    let mut probes = Vec::with_capacity(pairs);
                    let mut builds = Vec::with_capacity(pairs);
                    for row in probe_range.clone() {
                        probes.resize(probes.len() + build_range.len(), row as u32);
                        builds.extend(build_range.clone());
                    }
                    let Some(filter) = &self.params.residual_filter else {
                        self.send_pairs(probe, chunk, probes, builds.into(), timer)
                            .await?;
                        continue;
                    };
                    let selected =
                        filter_candidates(filter, self.probe_left, rows, probe, &probes, &builds)?;
                    (selected, PairIndices::Explicit { probes, builds })
                };
                if !probe_matched.is_empty() {
                    for &pos in &selected {
                        probe_matched.set(indices.get(pos).0 as usize, true);
                    }
                }
                if !self.build_matched.is_empty() {
                    for &pos in &selected {
                        self.build_matched
                            .set(global_base + indices.get(pos).1 as usize, true);
                    }
                }
                if matches!(self.output, Output::Pairs { .. }) {
                    let mut out_probes = Vec::with_capacity(selected.len());
                    let mut out_builds = UInt32Builder::with_capacity(selected.len());
                    let mut matches = selected.into_iter().map(|pos| indices.get(pos)).peekable();
                    for row in probe_range.clone() {
                        while let Some(&(probe_row, build_row)) = matches.peek()
                            && probe_row as usize == row
                        {
                            out_probes.push(row as u32);
                            out_builds.append_value(build_row);
                            matches.next();
                        }
                        // A later block may still match this probe row.
                        if probe_outer && next_build == build_rows && !probe_matched[row] {
                            out_probes.push(row as u32);
                            out_builds.append_null();
                        }
                    }
                    self.send_pairs(probe, chunk, out_probes, out_builds.finish(), timer)
                        .await?;
                } else if self.output == Output::ProbeSelection
                    && next_build < build_rows
                    && probe_matched[first_probe]
                {
                    // A split build range has exactly one probe. Its existence is known.
                    break;
                }
            }
        }
        if self.output == Output::ProbeSelection {
            self.output_rows += emit_selected(
                &self.params,
                self.probe_left,
                probe,
                &probe_matched,
                &self.sender,
                timer,
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Joiner for BnljJoiner {
    async fn join(
        self: Pin<&mut Self>,
        probe: RecordBatch,
        _: &Time,
        _: &Time,
        _: &Time,
        timer: &Time,
    ) -> Result<()> {
        let this = self.get_mut();
        let _output_timer = timer.timer();
        let probe_rows = probe.num_rows();
        if probe_rows == 0 {
            return Ok(());
        }
        let probe_outer = matches!(
            this.output,
            Output::Pairs {
                probe_outer: true,
                ..
            }
        );
        if !this.can_match || this.params.residual_filter.is_none() {
            let matched = this.can_match;
            if matched && this.build_matched.first().is_some_and(|bit| !*bit) {
                this.build_matched.fill(true);
            }
            if this.output == Output::ProbeSelection || (probe_outer && !matched) {
                let probe_matched = BitVec::repeat(matched, probe_rows);
                this.output_rows += emit_selected(
                    &this.params,
                    this.probe_left,
                    &probe,
                    &probe_matched,
                    &this.sender,
                    timer,
                )
                .await?;
            }
            if !matched || !matches!(this.output, Output::Pairs { .. }) {
                return Ok(());
            }
            if this.rows().num_rows() == 1 {
                emit_singleton(
                    &this.params,
                    this.probe_left,
                    this.rows(),
                    &this.sender,
                    &probe,
                    timer,
                )
                .await?;
                this.output_rows += probe_rows;
                return Ok(());
            }
        }
        this.join_candidates(&probe, probe_outer, timer).await
    }

    async fn finish(self: Pin<&mut Self>, timer: &Time) -> Result<()> {
        let this = self.get_mut();
        if this.build_matched.is_empty() {
            return Ok(());
        }
        let _output_timer = timer.timer();
        let mut offset = 0;
        for batch in this.build.as_ref().borrow().data_batches() {
            let end = offset + batch.num_rows();
            this.output_rows += emit_selected(
                &this.params,
                !this.probe_left,
                batch,
                &this.build_matched[offset..end],
                &this.sender,
                timer,
            )
            .await?;
            offset = end;
        }
        Ok(())
    }

    fn can_early_stop(&self) -> bool {
        match self.output {
            Output::Pairs {
                probe_outer: false, ..
            } => self.rows().is_empty(),
            Output::BuildSelection => self.build_matched.all(),
            _ => false,
        }
    }

    fn num_output_rows(&self) -> usize {
        self.output_rows
    }
}
