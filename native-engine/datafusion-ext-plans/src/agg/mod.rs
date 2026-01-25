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

pub mod acc;
pub mod agg;
pub mod agg_ctx;
pub mod agg_hash_map;
pub mod agg_table;
pub mod avg;
pub mod bloom_filter;
pub mod brickhouse;
pub mod collect;
pub mod count;
pub mod first;
pub mod first_ignores_null;
pub mod maxmin;
#[cfg(not(feature = "flink"))]
pub mod spark_udaf_wrapper;
#[cfg(feature = "flink")]
pub mod spark_udaf_wrapper {
    // Stub module for Flink builds - UDAFs not supported in Flink MVP
    use arrow::array::ArrayRef;
    use datafusion::common::Result;
    use once_cell::sync::OnceCell;

    use super::*;

    #[derive(Debug)]
    pub struct SparkUDAFWrapper;

    impl SparkUDAFWrapper {
        pub fn partial_update_with_indices_cache(
            &self,
            _acc_col: &mut acc::AccColumnRef,
            _acc_idx: agg::IdxSelection<'_>,
            _partial_args: &[ArrayRef],
            _partial_arg_idx: agg::IdxSelection<'_>,
            _indices_cache: &OnceCell<Vec<usize>>,
        ) -> Result<()> {
            unreachable!("UDAF not supported in Flink builds")
        }

        pub fn partial_merge_with_indices_cache(
            &self,
            _accs: &mut acc::AccColumnRef,
            _acc_idx: agg::IdxSelection<'_>,
            _merging_accs: &mut acc::AccColumnRef,
            _merging_acc_idx: agg::IdxSelection<'_>,
            _cache: &OnceCell<auron_jni_bridge::jni_bridge::LocalRef>,
        ) -> Result<()> {
            unreachable!("UDAF not supported in Flink builds")
        }

        pub fn final_merge_with_indices_cache(
            &self,
            _acc_col: &mut acc::AccColumnRef,
            _idx: agg::IdxSelection<'_>,
            _indices_cache: &OnceCell<Vec<usize>>,
        ) -> Result<ArrayRef> {
            unreachable!("UDAF not supported in Flink builds")
        }
    }

    #[derive(Debug)]
    pub struct AccUDAFBufferRowsColumn;

    impl AccUDAFBufferRowsColumn {
        pub fn spill_with_indices_cache(
            &self,
            _idx: agg::IdxSelection<'_>,
            _buf: &mut auron_memmgr::spill::SpillCompressedWriter,
            _spill_idx: usize,
            _mem_tracker: &SparkUDAFMemTracker,
            _cache: &OnceCell<auron_jni_bridge::jni_bridge::LocalRef>,
        ) -> Result<()> {
            unreachable!("UDAF not supported in Flink builds")
        }

        pub fn freeze_to_rows_with_indices_cache(
            &self,
            _idx: agg::IdxSelection<'_>,
            _array: &mut [Vec<u8>],
            _cache: &OnceCell<auron_jni_bridge::jni_bridge::LocalRef>,
        ) -> Result<()> {
            unreachable!("UDAF not supported in Flink builds")
        }

        pub fn unspill_with_key(
            &mut self,
            _num_rows: usize,
            _r: &mut auron_memmgr::spill::SpillCompressedReader,
            _mem_tracker: &SparkUDAFMemTracker,
            _spill_idx: usize,
        ) -> Result<()> {
            unreachable!("UDAF not supported in Flink builds")
        }
    }

    #[derive(Debug)]
    pub struct SparkUDAFMemTracker;

    impl SparkUDAFMemTracker {
        pub fn try_new() -> Result<Self> {
            unreachable!("UDAF not supported in Flink builds")
        }

        pub fn as_obj(&self) -> jni::objects::JObject {
            unreachable!("UDAF not supported in Flink builds")
        }

        pub fn update_used(&self) -> Result<bool> {
            unreachable!("UDAF not supported in Flink builds")
        }

        pub fn reset(&self) -> Result<()> {
            unreachable!("UDAF not supported in Flink builds")
        }

        pub fn add_column(&self, _column: &AccUDAFBufferRowsColumn) -> Result<()> {
            unreachable!("UDAF not supported in Flink builds")
        }
    }
}
pub mod sum;

use std::{fmt::Debug, sync::Arc};

use agg::Agg;
use datafusion::physical_expr::PhysicalExprRef;

pub const AGG_BUF_COLUMN_NAME: &str = "#9223372036854775807";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggExecMode {
    HashAgg,
    SortAgg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggMode {
    Partial,
    PartialMerge,
    Final,
}
impl AggMode {
    pub fn is_partial(&self) -> bool {
        matches!(self, AggMode::Partial)
    }

    pub fn is_partial_merge(&self) -> bool {
        matches!(self, AggMode::PartialMerge)
    }

    pub fn is_final(&self) -> bool {
        matches!(self, AggMode::Final)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunction {
    Count,
    Sum,
    Avg,
    Max,
    Min,
    First,
    FirstIgnoresNull,
    CollectList,
    CollectSet,
    BloomFilter,
    BrickhouseCollect,
    BrickhouseCombineUnique,
    Udaf,
}

#[derive(Debug, Clone)]
pub struct GroupingExpr {
    pub field_name: String,
    pub expr: PhysicalExprRef,
}

#[derive(Debug, Clone)]
pub struct AggExpr {
    pub field_name: String,
    pub mode: AggMode,
    pub agg: Arc<dyn Agg>,
}
