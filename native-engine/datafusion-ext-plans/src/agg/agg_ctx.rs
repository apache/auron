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

use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, RecordBatchOptions},
    datatypes::{Field, Fields, Schema, SchemaRef},
    record_batch::RecordBatch,
    row::{RowConverter, Rows, SortField},
};
use auron_jni_bridge::{
    conf,
    conf::{BooleanConf, DoubleConf, IntConf},
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{downcast_any, suggested_batch_mem_size};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    agg::{
        AggExecMode, AggExpr, AggMode, GroupingExpr,
        acc::AccTable,
        agg::{Agg, IdxSelection},
        spark_udaf_wrapper::{AccUDAFBufferRowsColumn, SparkUDAFMemTracker, SparkUDAFWrapper},
    },
    common::{
        cached_exprs_evaluator::CachedExprsEvaluator,
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
    },
};

pub struct AggContext {
    pub exec_mode: AggExecMode,
    pub need_partial_update: bool,
    pub need_partial_merge: bool,
    pub need_final_merge: bool,
    pub need_partial_update_aggs: Vec<(usize, Arc<dyn Agg>)>,
    pub need_partial_merge_aggs: Vec<(usize, Arc<dyn Agg>)>,

    pub output_schema: SchemaRef,
    pub grouping_row_converter: Arc<Mutex<RowConverter>>,
    pub groupings: Vec<GroupingExpr>,
    pub aggs: Vec<AggExpr>,
    pub input_acc_arrays_len: usize,
    pub output_acc_arrays_len: usize,
    pub supports_partial_skipping: bool,
    pub partial_skipping_ratio: f64,
    pub partial_skipping_min_rows: usize,
    pub partial_skipping_skip_spill: bool,
    pub is_expand_agg: bool,
    pub agg_expr_evaluator: CachedExprsEvaluator,
    pub num_spill_buckets: OnceCell<usize>,
    pub udaf_mem_tracker: OnceCell<SparkUDAFMemTracker>,
}

impl Debug for AggContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[groupings={:?}, aggs={:?}]", self.groupings, self.aggs,)
    }
}

impl AggContext {
    pub fn try_new(
        exec_mode: AggExecMode,
        input_schema: SchemaRef,
        groupings: Vec<GroupingExpr>,
        aggs: Vec<AggExpr>,
        supports_partial_skipping: bool,
        is_expand_agg: bool,
    ) -> Result<Self> {
        let grouping_schema = Arc::new(Schema::new(
            groupings
                .iter()
                .map(|grouping: &GroupingExpr| {
                    Ok(Field::new(
                        grouping.field_name.as_str(),
                        grouping.expr.data_type(&input_schema)?,
                        grouping.expr.nullable(&input_schema)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ));
        let grouping_row_converter = Arc::new(Mutex::new(RowConverter::new(
            grouping_schema
                .fields()
                .iter()
                .map(|field| SortField::new(field.data_type().clone()))
                .collect(),
        )?));

        // final aggregates may not exist along with partial/partial-merge
        let need_partial_update = aggs.iter().any(|agg| agg.mode == AggMode::Partial);
        let need_partial_merge = aggs.iter().any(|agg| agg.mode != AggMode::Partial);
        let need_final_merge = aggs.iter().any(|agg| agg.mode == AggMode::Final);
        assert!(!(need_final_merge && aggs.iter().any(|agg| agg.mode != AggMode::Final)));

        // FILTER predicates are only meaningful in Partial mode; the Spark planner must
        // not attach a filter to PartialMerge or Final aggregates (they operate
        // on already-filtered partial buffers, not on raw input rows).
        assert!(
            aggs.iter()
                .all(|agg| agg.filter.is_none() || agg.mode == AggMode::Partial),
            "aggregate FILTER is only valid in Partial mode"
        );

        let need_partial_update_aggs: Vec<(usize, Arc<dyn Agg>)> = aggs
            .iter()
            .enumerate()
            .filter(|(_idx, agg)| agg.mode.is_partial())
            .map(|(idx, agg)| (idx, agg.agg.clone()))
            .collect();
        let need_partial_merge_aggs: Vec<(usize, Arc<dyn Agg>)> = aggs
            .iter()
            .enumerate()
            .filter(|(_idx, agg)| !agg.mode.is_partial())
            .map(|(idx, agg)| (idx, agg.agg.clone()))
            .collect();

        let mut agg_fields = vec![];
        if need_final_merge {
            for agg in &aggs {
                agg_fields.push(Field::new(
                    &agg.field_name,
                    agg.agg.data_type().clone(),
                    agg.agg.nullable(),
                ));
            }
        } else {
            for agg in &aggs {
                for dt in agg.agg.acc_array_data_types() {
                    agg_fields.push(Field::new("", dt.clone(), true));
                }
            }
        }
        let agg_schema = Arc::new(Schema::new(agg_fields));
        let output_schema = Arc::new(Schema::new(
            [
                grouping_schema.fields().to_vec(),
                agg_schema.fields().to_vec(),
            ]
            .concat(),
        ));

        let input_acc_arrays_len = aggs
            .iter()
            .filter(|agg| agg.mode.is_partial_merge() || agg.mode.is_final())
            .map(|agg| agg.agg.acc_array_data_types().len())
            .sum();
        let output_acc_arrays_len = aggs
            .iter()
            .filter(|agg| agg.mode.is_partial() || agg.mode.is_partial_merge())
            .map(|agg| agg.agg.acc_array_data_types().len())
            .sum();

        let agg_exprs_flatten: Vec<PhysicalExprRef> = aggs
            .iter()
            .filter(|agg| agg.mode.is_partial())
            .flat_map(|agg| agg.agg.exprs())
            .collect();
        let agg_expr_evaluator_output_schema = Arc::new(Schema::new(
            agg_exprs_flatten
                .iter()
                .map(|e| {
                    Ok(Field::new(
                        "",
                        e.data_type(&input_schema)?,
                        e.nullable(&input_schema)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ));
        let agg_expr_evaluator = CachedExprsEvaluator::try_new(
            vec![],
            agg_exprs_flatten,
            agg_expr_evaluator_output_schema,
        )?;

        let (partial_skipping_ratio, partial_skipping_min_rows, partial_skipping_skip_spill) =
            if supports_partial_skipping {
                (
                    conf::PARTIAL_AGG_SKIPPING_RATIO.value().unwrap_or(0.999),
                    conf::PARTIAL_AGG_SKIPPING_MIN_ROWS.value().unwrap_or(20000) as usize,
                    conf::PARTIAL_AGG_SKIPPING_SKIP_SPILL
                        .value()
                        .unwrap_or(false),
                )
            } else {
                Default::default()
            };

        Ok(Self {
            exec_mode,
            need_partial_update,
            need_partial_merge,
            need_final_merge,
            need_partial_update_aggs,
            need_partial_merge_aggs,
            output_schema,
            grouping_row_converter,
            groupings,
            aggs,
            input_acc_arrays_len,
            output_acc_arrays_len,
            agg_expr_evaluator,
            supports_partial_skipping,
            partial_skipping_ratio,
            partial_skipping_min_rows,
            partial_skipping_skip_spill,
            is_expand_agg,
            num_spill_buckets: Default::default(),
            udaf_mem_tracker: Default::default(),
        })
    }

    pub fn create_acc_table(&self, num_rows: usize) -> AccTable {
        AccTable::new(
            self.aggs
                .iter()
                .map(|agg| agg.agg.create_acc_column(num_rows))
                .collect(),
            num_rows,
        )
    }

    pub fn create_grouping_rows(&self, input_batch: &RecordBatch) -> Result<Rows> {
        let grouping_arrays: Vec<ArrayRef> = self
            .groupings
            .iter()
            .map(|grouping| grouping.expr.evaluate(&input_batch))
            .map(|r| r.and_then(|columnar| columnar.into_array(input_batch.num_rows())))
            .collect::<Result<_>>()
            .map_err(|err| err.context("agg: evaluating grouping arrays error"))?;
        Ok(self
            .grouping_row_converter
            .lock()
            .convert_columns(&grouping_arrays)?)
    }

    pub fn update_batch_to_acc_table(
        &self,
        batch: &RecordBatch,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
    ) -> Result<()> {
        self.update_batch_slice_to_acc_table(
            batch,
            0,
            batch.num_rows(),
            acc_table,
            acc_idx,
            &mut None,
        )
    }

    pub fn create_merging_acc_table(&self, batch: &RecordBatch) -> Result<AccTable> {
        let mut merging_acc_table = self.create_acc_table(0);
        let mut acc_arrays_start = batch.num_columns() - self.input_acc_arrays_len;

        for (agg_idx, agg) in &self.need_partial_merge_aggs {
            let num_acc_arrays = agg.acc_array_data_types().len();
            let acc_arrays = &batch.columns()[acc_arrays_start..][..num_acc_arrays];
            acc_arrays_start += num_acc_arrays;
            merging_acc_table.cols_mut()[*agg_idx].unfreeze_from_arrays(acc_arrays)?;
        }
        Ok(merging_acc_table)
    }

    pub fn update_batch_slice_to_acc_table(
        &self,
        batch: &RecordBatch,
        batch_start_idx: usize,
        batch_end_idx: usize,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
        merging_acc_table: &mut Option<AccTable>,
    ) -> Result<()> {
        // NOTE:
        // arrow-ffi with sliced batch is buggy in older arrow-java, so we use unsliced
        // batch with explicit offsets

        // Every group needs an accumulator slot even when FILTER excludes all of its
        // rows.
        let acc_idx = acc_idx.with_cached_max();
        acc_table.ensure_size(acc_idx);

        // partial update
        if self.need_partial_update {
            let agg_exprs_batch = self.agg_expr_evaluator.filter_project(&batch)?;
            let mut input_arrays = Vec::with_capacity(self.aggs.len());
            let mut offset = 0;
            for agg in &self.aggs {
                if agg.mode.is_partial() {
                    let num_agg_exprs = agg.agg.exprs().len();
                    let prepared = agg.agg.prepare_partial_args(
                        &agg_exprs_batch.columns()[offset..][..num_agg_exprs],
                    )?;
                    input_arrays.push(prepared);
                    offset += num_agg_exprs;
                } else {
                    input_arrays.push(vec![]);
                }
            }

            // Evaluate per-aggregate FILTER predicates against the input batch.
            // Each aggregate may have an optional filter expression; we produce
            // a boolean array for each so that only rows evaluating to true
            // contribute to that aggregate.
            let filter_arrays: Vec<Option<BooleanArray>> = self
                .aggs
                .iter()
                .map(|agg| {
                    agg.filter
                        .as_ref()
                        .map(|filter_expr| {
                            let result = filter_expr.evaluate(batch)?;
                            let array = result.into_array(batch.num_rows())?;
                            let bool_array =
                                datafusion::common::cast::as_boolean_array(&array)?.clone();
                            Ok(bool_array)
                        })
                        .transpose()
                })
                .collect::<Result<Vec<_>>>()?;

            let udaf_indices_cache = OnceCell::new();
            for (agg_idx, agg) in &self.need_partial_update_aggs {
                let acc_col = &mut acc_table.cols_mut()[*agg_idx];
                if let Some(filter_array) = &filter_arrays[*agg_idx] {
                    // Build filtered index vectors: only rows where the filter
                    // is true are passed to partial_update, preserving the
                    // correct accumulator-to-input-row mapping.
                    let (filtered_acc, filtered_input) = Self::build_filtered_indices(
                        acc_idx,
                        batch_start_idx,
                        batch_end_idx,
                        filter_array,
                    );
                    if !filtered_acc.is_empty() {
                        let filtered_acc_idx =
                            IdxSelection::Indices(&filtered_acc).with_cached_max();
                        if let Ok(udaf_agg) = downcast_any!(agg, SparkUDAFWrapper) {
                            udaf_agg.partial_update_with_indices_cache(
                                acc_col,
                                filtered_acc_idx,
                                &input_arrays[*agg_idx],
                                IdxSelection::Indices(&filtered_input),
                                &udaf_indices_cache,
                            )?;
                        } else {
                            agg.partial_update(
                                acc_col,
                                filtered_acc_idx,
                                &input_arrays[*agg_idx],
                                IdxSelection::Indices(&filtered_input),
                            )?;
                        }
                    }
                } else {
                    let batch_selection = IdxSelection::Range(batch_start_idx, batch_end_idx);
                    if let Ok(udaf_agg) = downcast_any!(agg, SparkUDAFWrapper) {
                        udaf_agg.partial_update_with_indices_cache(
                            acc_col,
                            acc_idx,
                            &input_arrays[*agg_idx],
                            batch_selection,
                            &udaf_indices_cache,
                        )?;
                    } else {
                        agg.partial_update(
                            acc_col,
                            acc_idx,
                            &input_arrays[*agg_idx],
                            batch_selection,
                        )?;
                    }
                }
            }
        }

        // partial merge
        if self.need_partial_merge {
            let merging_acc_table = match merging_acc_table {
                Some(merging_acc_table) => merging_acc_table,
                merging_acc_table => {
                    merging_acc_table.insert(self.create_merging_acc_table(batch)?)
                }
            };
            let batch_selection = IdxSelection::Range(batch_start_idx, batch_end_idx);
            self.partial_merge(acc_table, acc_idx, merging_acc_table, batch_selection)?;
        }
        Ok(())
    }

    pub fn build_agg_columns(
        &self,
        acc_table: &mut AccTable,
        idx: IdxSelection,
    ) -> Result<Vec<ArrayRef>> {
        if self.need_final_merge {
            // output final merged value
            let udaf_indices_cache = OnceCell::new();
            let mut agg_columns = vec![];
            for (agg, acc_col) in self.aggs.iter().zip(acc_table.cols_mut()) {
                let values = if let Ok(udaf_agg) = downcast_any!(agg.agg, SparkUDAFWrapper) {
                    udaf_agg.final_merge_with_indices_cache(acc_col, idx, &udaf_indices_cache)?
                } else {
                    agg.agg.final_merge(acc_col, idx)?
                };
                agg_columns.push(values);
            }
            Ok(agg_columns)
        } else {
            self.freeze_acc_table(acc_table, idx)
        }
    }

    pub fn convert_records_to_batch(
        &self,
        keys: &[impl AsRef<[u8]>],
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
    ) -> Result<RecordBatch> {
        let grouping_row_converter = self.grouping_row_converter.lock();
        let grouping_row_parser = grouping_row_converter.parser();
        let grouping_columns = grouping_row_converter.convert_rows(
            keys.iter()
                .map(|key| grouping_row_parser.parse(key.as_ref())),
        )?;
        let agg_columns = self.build_agg_columns(acc_table, acc_idx)?;

        // at least one column exists
        Ok(RecordBatch::try_new(
            self.output_schema.clone(),
            [grouping_columns, agg_columns].concat(),
        )?)
    }

    /// Given an `acc_idx` mapping and a filter boolean array, produce two
    /// aligned index vectors:
    /// - `filtered_acc`: accumulator indices to update
    /// - `filtered_input`: input batch row indices to read from
    ///
    /// Only rows in `[batch_start_idx, batch_end_idx)` where the filter is
    /// non-null and true are included. The mapping logic handles all
    /// `IdxSelection` variants.
    fn build_filtered_indices(
        acc_idx: IdxSelection,
        batch_start_idx: usize,
        batch_end_idx: usize,
        filter_array: &BooleanArray,
    ) -> (Vec<usize>, Vec<usize>) {
        let mut filtered_acc = vec![];
        let mut filtered_input = vec![];
        match acc_idx {
            // Single accumulator for all rows in this group.
            IdxSelection::Single(idx) => {
                for i in batch_start_idx..batch_end_idx {
                    if filter_array.is_valid(i) && filter_array.value(i) {
                        filtered_acc.push(idx);
                        filtered_input.push(i);
                    }
                }
            }
            // Per-row accumulator indices (used by sort aggregation).
            IdxSelection::Indices(indices) => {
                for i in batch_start_idx..batch_end_idx {
                    if filter_array.is_valid(i) && filter_array.value(i) {
                        filtered_acc.push(indices[i - batch_start_idx]);
                        filtered_input.push(i);
                    }
                }
            }
            // Per-row accumulator indices as u32 (used by hash aggregation).
            IdxSelection::IndicesU32(indices) => {
                for i in batch_start_idx..batch_end_idx {
                    if filter_array.is_valid(i) && filter_array.value(i) {
                        filtered_acc.push(indices[i - batch_start_idx] as usize);
                        filtered_input.push(i);
                    }
                }
            }
            IdxSelection::IndicesWithMax(cached) => {
                return Self::build_filtered_indices(
                    IdxSelection::Indices(cached.indices()),
                    batch_start_idx,
                    batch_end_idx,
                    filter_array,
                );
            }
            IdxSelection::IndicesU32WithMax(cached) => {
                return Self::build_filtered_indices(
                    IdxSelection::IndicesU32(cached.indices()),
                    batch_start_idx,
                    batch_end_idx,
                    filter_array,
                );
            }
            // Contiguous accumulator range (used by merge / partial-skip paths).
            IdxSelection::Range(start, _end) => {
                for i in batch_start_idx..batch_end_idx {
                    if filter_array.is_valid(i) && filter_array.value(i) {
                        filtered_acc.push(start + (i - batch_start_idx));
                        filtered_input.push(i);
                    }
                }
            }
        }
        (filtered_acc, filtered_input)
    }

    pub fn partial_update(
        &self,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
        input_arrays: &[Vec<ArrayRef>],
        input_idx: IdxSelection,
    ) -> Result<()> {
        if self.need_partial_update {
            let acc_idx = acc_idx.with_cached_max();
            let udaf_indices_cache = OnceCell::new();
            for (agg_idx, agg) in &self.need_partial_update_aggs {
                let acc_col = &mut acc_table.cols_mut()[*agg_idx];
                // use indices cached version for UDAFs
                if let Ok(udaf_agg) = downcast_any!(agg, SparkUDAFWrapper) {
                    udaf_agg.partial_update_with_indices_cache(
                        acc_col,
                        acc_idx,
                        &input_arrays[*agg_idx],
                        input_idx,
                        &udaf_indices_cache,
                    )?;
                } else {
                    agg.partial_update(acc_col, acc_idx, &input_arrays[*agg_idx], input_idx)?;
                }
            }
        }
        Ok(())
    }

    pub fn partial_merge(
        &self,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
        merging_acc_table: &mut AccTable,
        merging_acc_idx: IdxSelection,
    ) -> Result<()> {
        if self.need_partial_merge {
            let acc_idx = acc_idx.with_cached_max();
            let udaf_indices_cache = OnceCell::new();
            for (agg_idx, agg) in &self.need_partial_merge_aggs {
                let acc_col = &mut acc_table.cols_mut()[*agg_idx];
                let merging_acc_col = &mut merging_acc_table.cols_mut()[*agg_idx];

                // use indices cached version for UDAFs
                if let Ok(udaf_agg) = downcast_any!(agg, SparkUDAFWrapper) {
                    udaf_agg.partial_merge_with_indices_cache(
                        acc_col,
                        acc_idx,
                        merging_acc_col,
                        merging_acc_idx,
                        &udaf_indices_cache,
                    )?;
                } else {
                    agg.partial_merge(acc_col, acc_idx, merging_acc_col, merging_acc_idx)?;
                }
            }
        }
        Ok(())
    }

    pub fn freeze_acc_table(
        &self,
        acc_table: &mut AccTable,
        acc_idx: IdxSelection,
    ) -> Result<Vec<ArrayRef>> {
        let udaf_indices_cache = OnceCell::new();
        let mut arrays = vec![];

        for acc_col in acc_table.cols_mut() {
            if let Ok(udaf_acc_col) = downcast_any!(acc_col, AccUDAFBufferRowsColumn) {
                arrays.push(
                    udaf_acc_col
                        .freeze_to_array_with_indices_cache(acc_idx, &udaf_indices_cache)?,
                );
            } else {
                arrays.extend(acc_col.freeze_to_arrays(acc_idx)?);
            }
        }
        Ok(arrays)
    }

    pub async fn process_partial_skipped(
        &self,
        batch: RecordBatch,
        exec_ctx: Arc<ExecutionContext>,
        sender: Arc<WrappedRecordBatchSender>,
    ) -> Result<()> {
        let batch_num_rows = batch.num_rows();
        let mut acc_table = self.create_acc_table(batch_num_rows);
        self.update_batch_to_acc_table(
            &batch,
            &mut acc_table,
            IdxSelection::Range(0, batch_num_rows),
        )?;

        // create output batch
        let grouping_columns = self
            .groupings
            .iter()
            .map(|grouping| grouping.expr.evaluate(&batch))
            .map(|r| r.and_then(|columnar| columnar.into_array(batch_num_rows)))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let agg_columns =
            self.build_agg_columns(&mut acc_table, IdxSelection::Range(0, batch_num_rows))?;
        let output_batch = RecordBatch::try_new_with_options(
            self.output_schema.clone(),
            [grouping_columns, agg_columns].concat(),
            &RecordBatchOptions::new().with_row_count(Some(batch_num_rows)),
        )?;

        exec_ctx
            .baseline_metrics()
            .record_output(output_batch.num_rows());
        sender.send(output_batch).await;
        return Ok(());
    }

    pub fn num_spill_buckets(&self, mem_size: usize) -> usize {
        *self
            .num_spill_buckets
            .get_or_init(|| (mem_size / suggested_batch_mem_size() / 2).max(16))
    }

    pub fn get_udaf_mem_tracker(&self) -> Option<&SparkUDAFMemTracker> {
        self.udaf_mem_tracker.get()
    }

    pub fn get_or_try_init_udaf_mem_tracker(&self) -> Result<&SparkUDAFMemTracker> {
        self.udaf_mem_tracker
            .get_or_try_init(|| SparkUDAFMemTracker::try_new())
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{AsArray, Float64Array, Int64Array},
        datatypes::{DataType, Float64Type, Int64Type},
    };
    use datafusion::physical_expr::expressions::Column;

    use super::*;
    use crate::agg::{AggFunction, agg::create_agg, count::AggCount, sum::AggSum};

    #[test]
    fn filtered_cached_indices_preserve_slice_mapping() {
        let filter = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            None,
            Some(true),
            Some(true),
        ]);
        for (selection, expected) in [
            (IdxSelection::Indices(&[2, 19, 2, 7]), vec![2, 7]),
            (IdxSelection::IndicesU32(&[2, 19, 2, 7]), vec![2, 7]),
            (IdxSelection::Single(7), vec![7, 7]),
            (IdxSelection::Range(3, 7), vec![3, 6]),
        ] {
            for selection in [selection, selection.with_cached_max()] {
                assert_eq!(
                    AggContext::build_filtered_indices(selection, 1, 5, &filter),
                    (expected.clone(), vec![1, 4])
                );
                assert_eq!(
                    AggContext::build_filtered_indices(selection, 2, 2, &filter),
                    (vec![], vec![])
                );
                let excluded = BooleanArray::from(vec![false; 6]);
                assert_eq!(
                    AggContext::build_filtered_indices(selection, 1, 5, &excluded),
                    (vec![], vec![])
                );
            }
        }
    }

    fn context(
        exec_mode: AggExecMode,
        input_schema: SchemaRef,
        mode: AggMode,
        aggs: &[AggExpr],
    ) -> Result<AggContext> {
        AggContext::try_new(
            exec_mode,
            input_schema,
            vec![],
            aggs.iter()
                .cloned()
                .map(|mut agg| {
                    agg.mode = mode;
                    if mode != AggMode::Partial {
                        agg.filter = None;
                    }
                    agg
                })
                .collect(),
            false,
            false,
        )
    }

    #[test]
    fn filtered_aggregates_through_sliced_partial_merge_and_final() -> Result<()> {
        for exec_mode in [AggExecMode::HashAgg, AggExecMode::SortAgg] {
            for multiple_batches in [false, true] {
                check_filtered_aggregates(exec_mode, multiple_batches)?;
            }
        }
        Ok(())
    }

    fn check_filtered_aggregates(exec_mode: AggExecMode, multiple_batches: bool) -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Float64, true),
            Field::new("filter", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![
                    999., 10., 20., 30., 40., 50., 999.,
                ])),
                Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(true),
                    None,
                    Some(true),
                    Some(true),
                ])),
            ],
        )?;
        let value: PhysicalExprRef = Arc::new(Column::new("value", 0));
        let filter: PhysicalExprRef = Arc::new(Column::new("filter", 1));
        let aggs = [AggFunction::Count, AggFunction::Sum, AggFunction::Avg]
            .into_iter()
            .map(|function| {
                Ok(AggExpr {
                    field_name: format!("{function:?}"),
                    mode: AggMode::Partial,
                    agg: create_agg(function, &[value.clone()], &schema, DataType::Float64)?,
                    filter: Some(filter.clone()),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let partial = context(exec_mode, schema, AggMode::Partial, &aggs)?;
        let mut table = partial.create_acc_table(0);
        partial.update_batch_slice_to_acc_table(
            &batch,
            1,
            6,
            &mut table,
            IdxSelection::IndicesU32(&[2, 7, 2, 19, 7]).with_cached_max(),
            &mut None,
        )?;
        assert!(table.cols().iter().all(|col| col.num_records() >= 20));
        if multiple_batches {
            let next_batch = RecordBatch::try_new(
                batch.schema(),
                vec![
                    Arc::new(Float64Array::from(vec![
                        Some(999.),
                        None,
                        Some(70.),
                        Some(80.),
                        Some(90.),
                        Some(999.),
                    ])),
                    Arc::new(BooleanArray::from(vec![
                        Some(true),
                        Some(true),
                        Some(true),
                        Some(false),
                        None,
                        Some(true),
                    ])),
                ],
            )?;
            partial.update_batch_slice_to_acc_table(
                &next_batch,
                1,
                5,
                &mut table,
                IdxSelection::IndicesU32(&[2, 2, 7, 31]).with_cached_max(),
                &mut None,
            )?;
            assert!(table.cols().iter().all(|col| col.num_records() >= 32));
        }
        let partial_batch = RecordBatch::try_new(
            partial.output_schema.clone(),
            partial.build_agg_columns(
                &mut table,
                IdxSelection::Indices(&[19, 2, 7, 2, 19, 19]).with_cached_max(),
            )?,
        )?;

        let merging = context(
            exec_mode,
            partial_batch.schema(),
            AggMode::PartialMerge,
            &aggs,
        )?;
        let mut table = merging.create_acc_table(0);
        let (expected_count, expected_sum) = if multiple_batches {
            (7, 270.)
        } else {
            (5, 130.)
        };
        let expected_avg = expected_sum / expected_count as f64;
        merging.update_batch_slice_to_acc_table(
            &partial_batch,
            1,
            5,
            &mut table,
            IdxSelection::Indices(&[4, 4, 4, 9]).with_cached_max(),
            &mut None,
        )?;
        let merged_batch = RecordBatch::try_new(
            merging.output_schema.clone(),
            merging.build_agg_columns(
                &mut table,
                IdxSelection::IndicesU32(&[9, 4, 9]).with_cached_max(),
            )?,
        )?;

        let final_ctx = context(exec_mode, merged_batch.schema(), AggMode::Final, &aggs)?;
        let mut table = final_ctx.create_acc_table(0);
        final_ctx.update_batch_slice_to_acc_table(
            &merged_batch,
            1,
            3,
            &mut table,
            IdxSelection::IndicesU32(&[8, 15]).with_cached_max(),
            &mut None,
        )?;
        let results = final_ctx.build_agg_columns(
            &mut table,
            IdxSelection::Indices(&[8, 15]).with_cached_max(),
        )?;
        assert_eq!(
            results[0].as_ref(),
            &Int64Array::from(vec![expected_count, 0])
        );
        assert_eq!(
            results[1].as_ref(),
            &Float64Array::from(vec![Some(expected_sum), None])
        );
        assert_eq!(
            results[2].as_ref(),
            &Float64Array::from(vec![Some(expected_avg), None])
        );

        let mut table = final_ctx.create_acc_table(0);
        final_ctx.update_batch_slice_to_acc_table(
            &merged_batch,
            1,
            3,
            &mut table,
            IdxSelection::Single(6),
            &mut None,
        )?;
        let results = final_ctx.build_agg_columns(&mut table, IdxSelection::Single(6))?;
        assert_eq!(results[0].as_ref(), &Int64Array::from(vec![expected_count]));
        assert_eq!(results[2].as_ref(), &Float64Array::from(vec![expected_avg]));
        Ok(())
    }

    fn partial_output_batch(num_rows: usize) -> RecordBatch {
        let keys = Int64Array::from((0..num_rows as i64).map(|i| i / 3).collect::<Vec<_>>());
        let sums = Float64Array::from((0..num_rows).map(|i| i as f64 * 1.5).collect::<Vec<_>>());
        let counts = Int64Array::from((0..num_rows as i64).map(|i| i % 5).collect::<Vec<_>>());
        RecordBatch::try_from_iter_with_nullable(vec![
            ("key", Arc::new(keys) as ArrayRef, false),
            ("sum", Arc::new(sums) as ArrayRef, true),
            ("cnt", Arc::new(counts) as ArrayRef, false),
        ])
        .expect("failed to create test batch")
    }

    fn merging_agg_ctx(schema: SchemaRef) -> Result<AggContext> {
        AggContext::try_new(
            AggExecMode::SortAgg,
            schema,
            vec![GroupingExpr {
                field_name: "key".to_string(),
                expr: Arc::new(Column::new("key", 0)),
            }],
            vec![
                AggExpr {
                    field_name: "sum".to_string(),
                    mode: AggMode::Final,
                    agg: Arc::new(AggSum::try_new(
                        Arc::new(Column::new("sum", 1)),
                        DataType::Float64,
                    )?),
                    filter: None,
                },
                AggExpr {
                    field_name: "cnt".to_string(),
                    mode: AggMode::Final,
                    agg: Arc::new(AggCount::try_new(
                        vec![Arc::new(Column::new("cnt", 2))],
                        DataType::Int64,
                    )?),
                    filter: None,
                },
            ],
            false,
            false,
        )
    }

    fn merge_in_ranges(
        agg_ctx: &AggContext,
        batch: &RecordBatch,
        split_points: &[usize],
    ) -> Result<Vec<ArrayRef>> {
        let num_rows = batch.num_rows();
        let mut acc_table = agg_ctx.create_acc_table(num_rows);
        let mut merging_acc_table = None;
        let mut start = 0;

        for &end in split_points.iter().chain(std::iter::once(&num_rows)) {
            let acc_indices = (start..end).collect::<Vec<_>>();
            agg_ctx.update_batch_slice_to_acc_table(
                batch,
                start,
                end,
                &mut acc_table,
                IdxSelection::Indices(&acc_indices),
                &mut merging_acc_table,
            )?;
            start = end;
        }
        agg_ctx.build_agg_columns(&mut acc_table, IdxSelection::Range(0, num_rows))
    }

    fn assert_columns_eq(expected: &[ArrayRef], actual: &[ArrayRef]) {
        let expected_sums = expected[0].as_primitive::<Float64Type>();
        let actual_sums = actual[0].as_primitive::<Float64Type>();
        let expected_counts = expected[1].as_primitive::<Int64Type>();
        let actual_counts = actual[1].as_primitive::<Int64Type>();
        assert_eq!(expected_sums, actual_sums);
        assert_eq!(expected_counts, actual_counts);
    }

    #[test]
    fn test_partial_merge_row_ranges_match_whole_batch() -> Result<()> {
        let batch = partial_output_batch(97);
        let agg_ctx = merging_agg_ctx(batch.schema())?;

        let whole = merge_in_ranges(&agg_ctx, &batch, &[])?;
        let splitted = merge_in_ranges(&agg_ctx, &batch, &[13, 40, 40, 96])?;
        assert_columns_eq(&whole, &splitted);
        Ok(())
    }

    #[test]
    fn test_partial_merge_accepts_sliced_input_batch() -> Result<()> {
        let batch = partial_output_batch(97);
        let sliced = batch.slice(11, 63);
        let agg_ctx = merging_agg_ctx(sliced.schema())?;

        let expected = merge_in_ranges(
            &merging_agg_ctx(batch.schema())?,
            &RecordBatch::try_new(
                sliced.schema(),
                sliced
                    .columns()
                    .iter()
                    .map(|c| arrow::compute::concat(&[c.as_ref()]).expect("concat failed"))
                    .collect(),
            )?,
            &[],
        )?;
        let actual = merge_in_ranges(&agg_ctx, &sliced, &[7, 30])?;
        assert_columns_eq(&expected, &actual);
        Ok(())
    }
}
