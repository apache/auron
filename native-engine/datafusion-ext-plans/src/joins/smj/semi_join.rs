// Copyright 2022 The Auron Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{cmp::Ordering, pin::Pin, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchOptions};
use async_trait::async_trait;
use datafusion::common::Result;
use datafusion_ext_commons::arrow::selection::create_batch_interleaver;

use crate::{
    common::execution_context::WrappedRecordBatchSender,
    compare_cursor, cur_forward,
    joins::{
        Idx, JoinParams,
        smj::semi_join::SemiJoinSide::{L, R},
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
pub struct SemiJoiner<const P: JoinerParams> {
    join_params: JoinParams,
    output_sender: Arc<WrappedRecordBatchSender>,
    indices: Vec<Idx>,
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
            output_rows: 0,
        }
    }

    fn should_flush(&self) -> bool {
        self.indices.len() >= self.join_params.batch_size
    }

    async fn flush(
        mut self: Pin<&mut Self>,
        cur1: &mut StreamCursor,
        cur2: &mut StreamCursor,
    ) -> Result<()> {
        let indices = std::mem::take(&mut self.indices);
        let num_rows = indices.len();

        let cols = match P.join_side {
            L => {
                let batch_interleaver = create_batch_interleaver(cur1.batches(), false)?;
                batch_interleaver(&indices)?
            }
            R => {
                let batch_interleaver = create_batch_interleaver(cur2.batches(), false)?;
                batch_interleaver(&indices)?
            }
        };
        let output_batch = RecordBatch::try_new_with_options(
            self.join_params.projection.schema.clone(),
            cols.columns().to_vec(),
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;

        if output_batch.num_rows() > 0 {
            self.output_rows += output_batch.num_rows();
            self.output_sender.send(output_batch).await;
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
        while !cur1.finished() && !cur2.finished() {
            if self.should_flush()
                || cur1.num_buffered_batches() > 1
                || cur2.num_buffered_batches() > 1
            {
                self.as_mut().flush(cur1, cur2).await?;
                cur1.clean_out_dated_batches();
                cur2.clean_out_dated_batches();
            }

            match compare_cursor!(cur1, cur2) {
                Ordering::Less => {
                    if P.join_side == L && !P.semi {
                        self.indices.push(cur1.cur_idx());
                    }
                    cur_forward!(cur1);
                }
                Ordering::Greater => {
                    if P.join_side == R && !P.semi {
                        self.indices.push(cur2.cur_idx());
                    }
                    cur_forward!(cur2);
                }
                Ordering::Equal => {
                    let l_key_idx = cur1.cur_idx();
                    let r_key_idx = cur2.cur_idx();

                    if P.join_side == L && P.semi {
                        self.indices.push(l_key_idx);
                    }
                    cur_forward!(cur1);

                    if P.join_side == R && P.semi {
                        self.indices.push(r_key_idx);
                    }
                    cur_forward!(cur2);

                    // iterate both stream, find smaller one, use it for probing
                    let mut l_equal = true;
                    let mut r_equal = true;
                    while l_equal && r_equal {
                        if l_equal {
                            l_equal = !cur1.finished() && cur1.cur_key() == cur1.key(l_key_idx);
                            if l_equal {
                                if P.join_side == L && P.semi {
                                    self.indices.push(cur1.cur_idx());
                                }
                                cur_forward!(cur1);
                            }
                        }
                        if r_equal {
                            r_equal = !cur2.finished() && cur2.cur_key() == cur2.key(r_key_idx);
                            if r_equal {
                                if P.join_side == R && P.semi {
                                    self.indices.push(cur2.cur_idx());
                                }
                                cur_forward!(cur2);
                            }
                        }
                    }

                    if l_equal {
                        // stream left side
                        while !cur1.finished() && cur1.cur_key() == cur2.key(r_key_idx) {
                            if P.join_side == L && P.semi {
                                self.indices.push(cur1.cur_idx());
                            }
                            cur_forward!(cur1);
                            if self.should_flush() || cur1.num_buffered_batches() > 1 {
                                self.as_mut().flush(cur1, cur2).await?;
                                cur1.clean_out_dated_batches();
                            }
                        }
                    }

                    if r_equal {
                        // stream right side
                        while !cur2.finished() && cur2.cur_key() == cur1.key(l_key_idx) {
                            if P.join_side == R && P.semi {
                                self.indices.push(cur2.cur_idx());
                            }
                            cur_forward!(cur2);
                            if self.should_flush() || cur2.num_buffered_batches() > 1 {
                                self.as_mut().flush(cur1, cur2).await?;
                                cur2.clean_out_dated_batches();
                            }
                        }
                    }
                }
            }
        }

        // at least one side is finished, consume the other side if it is an anti side
        if !P.semi {
            while P.join_side == L && !P.semi && !cur1.finished() {
                let lidx = cur1.cur_idx();
                self.indices.push(lidx);
                cur_forward!(cur1);
                if self.should_flush() {
                    self.as_mut().flush(cur1, cur2).await?;
                    cur1.clean_out_dated_batches();
                }
            }
            while P.join_side == R && !P.semi && !cur2.finished() {
                let ridx = cur2.cur_idx();
                self.indices.push(ridx);
                cur_forward!(cur2);
                if self.should_flush() {
                    self.as_mut().flush(cur1, cur2).await?;
                    cur2.clean_out_dated_batches();
                }
            }
        }
        if !self.indices.is_empty() {
            self.flush(cur1, cur2).await?;
        }
        Ok(())
    }

    fn num_output_rows(&self) -> usize {
        self.output_rows
    }
}
