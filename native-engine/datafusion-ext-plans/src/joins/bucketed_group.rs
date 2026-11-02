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

//! Building blocks for "hybrid join" (bucketed hash join): partitioning both
//! build/probe sides of a broadcast/shuffled hash join by
//! `hash(key) % NUM_BUCKETS` so that an otherwise too-large build side can be
//! processed group by group instead of as a single huge in-memory
//! [`crate::joins::join_hash_map::JoinHashMap`].

use std::sync::Arc;

use arrow::{
    array::{Array, ArrayRef, AsArray, Int32Array, RecordBatch},
    datatypes::{DataType, Field, FieldRef, Int32Type, Schema, SchemaRef},
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::arrow::{array_size::BatchSize, coalesce::coalesce_batches_unchecked};
use once_cell::sync::OnceCell;

use crate::joins::join_hash_map::{JoinHashMap, join_create_hashes};

/// Number of buckets used to partition hybrid join build/probe sides.
/// Fixed at compile time for now; could be made configurable later.
pub const NUM_BUCKETS: u32 = 4096;

const BUCKET_COLUMN_NAME: &str = "~BUCKET";

#[inline]
pub fn bucket_field() -> FieldRef {
    static FIELD: OnceCell<FieldRef> = OnceCell::new();
    FIELD
        .get_or_init(|| Arc::new(Field::new(BUCKET_COLUMN_NAME, DataType::Int32, false)))
        .clone()
}

/// `data_schema` -> `data_schema + ~BUCKET`
#[inline]
pub fn bucket_schema(data_schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        data_schema
            .fields()
            .iter()
            .cloned()
            .chain(std::iter::once(bucket_field()))
            .collect::<Vec<_>>(),
    ))
}

/// Compute `hash(key_columns) % NUM_BUCKETS` for every row of `batch` and
/// append it as a trailing `~BUCKET` `Int32` column.
pub fn append_bucket_id_column(
    batch: RecordBatch,
    key_exprs: &[PhysicalExprRef],
) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let key_columns: Vec<ArrayRef> = key_exprs
        .iter()
        .map(|expr| Ok(expr.evaluate(&batch)?.into_array(num_rows)?))
        .collect::<Result<_>>()?;
    let hashes = join_create_hashes(num_rows, &key_columns);
    let buckets: Int32Array = hashes
        .into_iter()
        .map(|h| (h % NUM_BUCKETS) as i32)
        .collect();

    let schema = bucket_schema(&batch.schema());
    Ok(RecordBatch::try_new(
        schema,
        batch
            .columns()
            .iter()
            .cloned()
            .chain(std::iter::once(Arc::new(buckets) as ArrayRef))
            .collect(),
    )?)
}

/// Remove the trailing `~BUCKET` column of a batch.
pub fn remove_last_column(mut batch: RecordBatch) -> RecordBatch {
    batch.remove_column(batch.num_columns() - 1);
    batch
}

/// Split `batch` (already sorted ascending by the `~BUCKET` column at
/// `bucket_col_idx`) into `(rows with bucket <= threshold, rows with bucket >
/// threshold)`.
pub fn split_batch_by_bucket_threshold(
    batch: &RecordBatch,
    bucket_col_idx: usize,
    threshold: i32,
) -> Result<(Option<RecordBatch>, Option<RecordBatch>)> {
    let buckets = batch.column(bucket_col_idx).as_primitive::<Int32Type>();
    let values = buckets.values();
    let total = batch.num_rows();
    let cut = values.partition_point(|&b| b <= threshold);

    let lo = if cut > 0 {
        Some(batch.slice(0, cut))
    } else {
        None
    };
    let hi = if cut < total {
        Some(batch.slice(cut, total - cut))
    } else {
        None
    };
    Ok((lo, hi))
}

/// Given a sequence of per-row bucket ids (ascending) and the total memory
/// size of those rows, find a "safe" cut point at or after the row where the
/// accumulated size is expected to reach `threshold_mem_size`, such that the
/// cut always falls on a bucket boundary (`buckets[cut - 1] !=
/// buckets[cut]`).
///
/// Returns `None` when no such boundary exists at or after the estimated
/// position (i.e. everything from there to the end belongs to the same
/// bucket - extreme data skew), in which case the caller should keep
/// accumulating more input instead of force-splitting a bucket.
fn find_safe_cut_point(
    buckets: &[i32],
    total_mem_size: usize,
    threshold_mem_size: usize,
) -> Option<usize> {
    let total_rows = buckets.len();
    if total_rows == 0 {
        return None;
    }
    let avg_row_size = (total_mem_size as f64 / total_rows as f64).max(1.0);
    let mut cut = ((threshold_mem_size as f64 / avg_row_size).ceil() as usize).clamp(1, total_rows);
    while cut < total_rows && buckets[cut] == buckets[cut - 1] {
        cut += 1;
    }
    if cut >= total_rows { None } else { Some(cut) }
}

/// Incrementally consumes bucket-sorted build-side batches (`data columns +
/// ~BUCKET`) and emits bucketed-group hash-map-batches (`data columns +
/// ~TABLE`, with the bucket group id packed into each batch's `~TABLE`
/// metadata - see [`JoinHashMap::into_hash_map_batches`]), each bounded to
/// roughly `threshold_mem_size` bytes (except under extreme single-bucket
/// data skew, where correctness takes priority over the memory bound).
pub struct BucketGroupBuilder {
    threshold_mem_size: usize,
    keys: Vec<PhysicalExprRef>,
    data_schema: SchemaRef,
    staging: Vec<RecordBatch>,
    staging_mem_size: usize,
    oversized_count: usize,
}

impl BucketGroupBuilder {
    pub fn new(
        threshold_mem_size: usize,
        keys: Vec<PhysicalExprRef>,
        data_schema: SchemaRef,
    ) -> Self {
        Self {
            threshold_mem_size,
            keys,
            data_schema,
            staging: vec![],
            staging_mem_size: 0,
            oversized_count: 0,
        }
    }

    /// Number of emitted groups that exceeded `threshold_mem_size` due to a
    /// single bucket being larger than the threshold on its own.
    pub fn oversized_count(&self) -> usize {
        self.oversized_count
    }

    /// Feed in a new batch (already sorted ascending by `~BUCKET`, bucket
    /// column being the last column). May return zero or more fully-formed
    /// bucketed-group batches.
    pub fn push(&mut self, batch_with_bucket: RecordBatch) -> Result<Vec<RecordBatch>> {
        self.staging_mem_size += batch_with_bucket.get_batch_mem_size();
        self.staging.push(batch_with_bucket);

        let mut outputs = vec![];
        while self.staging_mem_size > self.threshold_mem_size {
            let bucket_col_idx = self.staging[0].num_columns() - 1;
            let buckets: Vec<i32> = self
                .staging
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(bucket_col_idx)
                        .as_primitive::<Int32Type>()
                        .values()
                        .iter()
                        .copied()
                })
                .collect();

            match find_safe_cut_point(&buckets, self.staging_mem_size, self.threshold_mem_size) {
                Some(cut) => {
                    let (group, remainder) =
                        Self::split_staging_at(std::mem::take(&mut self.staging), cut);
                    self.staging = remainder;
                    self.staging_mem_size =
                        self.staging.iter().map(|b| b.get_batch_mem_size()).sum();
                    outputs.extend(self.build_group_batches(group)?);
                }
                None => {
                    // whole staging belongs to a single bucket (or no
                    // boundary found yet) - cannot safely cut; keep
                    // accumulating without coalescing (avoids repeated
                    // O(n^2) merges and avoids prematurely overflowing a
                    // bucket that may still be split by a later batch).
                    break;
                }
            }
        }
        Ok(outputs)
    }

    /// Split a list of (row-count-prefix-summed) staging batches at global
    /// row index `cut`, slicing at most one batch (the one straddling the
    /// cut point).
    fn split_staging_at(
        staging: Vec<RecordBatch>,
        cut: usize,
    ) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        let mut group = Vec::with_capacity(staging.len());
        let mut remainder = Vec::with_capacity(staging.len());
        let mut consumed = 0usize;
        for batch in staging {
            let rows = batch.num_rows();
            if consumed >= cut {
                remainder.push(batch);
            } else if consumed + rows <= cut {
                group.push(batch);
            } else {
                let local_cut = cut - consumed;
                group.push(batch.slice(0, local_cut));
                remainder.push(batch.slice(local_cut, rows - local_cut));
            }
            consumed += rows;
        }
        (group, remainder)
    }

    /// Flush the last (possibly undersized) group once the input is
    /// exhausted.
    pub fn finish(&mut self) -> Result<Vec<RecordBatch>> {
        if self.staging.is_empty() {
            return Ok(vec![]);
        }
        let group = std::mem::take(&mut self.staging);
        self.staging_mem_size = 0;
        self.build_group_batches(group)
    }

    /// Build the final bucketed-group hash-map-batches out of the (bucket
    /// column still attached) staging batches belonging to one logical
    /// group. Normally coalesces them into a single physical `RecordBatch`
    /// (today's behavior); under extreme single-bucket data skew (total
    /// size > `threshold_mem_size`), keeps them as multiple un-coalesced
    /// physical batches instead, to avoid an i32 offset overflow (>2GB)
    /// when coalescing variable-length columns. Every output batch carries
    /// the same `group_max_bucket`, packed into its `~TABLE` metadata (see
    /// [`JoinHashMap::into_hash_map_batches`]) rather than a separate
    /// column, so the declared hash-map-batch schema never changes
    /// depending on whether hybrid join is in effect.
    fn build_group_batches(
        &mut self,
        group_with_bucket: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        let total_rows: usize = group_with_bucket.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return Ok(vec![]);
        }
        let total_mem_size: usize = group_with_bucket
            .iter()
            .map(|b| b.get_batch_mem_size())
            .sum();

        let group_max_bucket = {
            let last = group_with_bucket.last().expect("non-empty group");
            let bucket_col_idx = last.num_columns() - 1;
            last.column(bucket_col_idx)
                .as_primitive::<Int32Type>()
                .value(last.num_rows() - 1)
        };

        let data_batches: Vec<RecordBatch> = if total_mem_size <= self.threshold_mem_size {
            // common case: still coalesce into a single physical batch to
            // keep today's "1 group = 1 physical batch" output format and
            // performance.
            let coalesced =
                coalesce_batches_unchecked(bucket_schema(&self.data_schema), &group_with_bucket);
            vec![remove_last_column(coalesced)]
        } else {
            self.oversized_count += 1;
            log::warn!(
                "hybrid join: bucket group size ({total_mem_size}) exceeds group mem size \
                 threshold ({}) due to data skew on a single bucket; kept as {} un-coalesced \
                 batches to avoid 2GB overflow",
                self.threshold_mem_size,
                group_with_bucket.len(),
            );
            group_with_bucket
                .into_iter()
                .map(remove_last_column)
                .filter(|b| b.num_rows() > 0)
                .collect()
        };

        let hash_map = JoinHashMap::create_from_data_batches(data_batches, &self.keys)?;
        Ok(hash_map
            .into_hash_map_batches(Some(group_max_bucket))?
            .into_iter()
            .filter(|b| b.num_rows() > 0)
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::physical_expr::expressions::Column;

    use super::*;
    use crate::joins::join_hash_map::peek_group_max_bucket;

    fn int32_batch(schema: SchemaRef, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values)) as ArrayRef])
            .expect("valid Int32 test batch")
    }

    fn data_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]))
    }

    #[test]
    fn same_key_maps_to_same_bucket() -> Result<()> {
        let schema = data_schema();
        let batch = int32_batch(schema.clone(), vec![1, 2, 1, 3, 2]);
        let key: PhysicalExprRef = Arc::new(Column::new("k", 0));
        let bucketed = append_bucket_id_column(batch, &[key])?;
        let buckets = bucketed
            .column(bucketed.num_columns() - 1)
            .as_primitive::<Int32Type>()
            .values();
        assert_eq!(buckets[0], buckets[2]); // both key=1
        assert_eq!(buckets[1], buckets[4]); // both key=2
        for &b in buckets {
            assert!(b >= 0 && (b as u32) < NUM_BUCKETS);
        }
        Ok(())
    }

    #[test]
    fn group_builder_splits_on_bucket_boundaries() -> Result<()> {
        // 8 rows, each row ~ a few bytes; use a tiny threshold to force
        // multiple splits, and craft bucket ids so that boundaries exist.
        let schema = data_schema();
        let keys: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("k", 0))];
        let mut builder = BucketGroupBuilder::new(1, keys, schema.clone());

        // bucket ids ascending, with duplicates to verify no-split-within-bucket
        let batch_with_bucket = {
            let data = int32_batch(schema.clone(), vec![10, 20, 20, 30, 30, 30, 40, 50]);
            let bucket_schema = bucket_schema(&schema);
            let buckets = Int32Array::from(vec![0, 0, 0, 1, 1, 1, 2, 3]);
            RecordBatch::try_new(
                bucket_schema,
                vec![data.column(0).clone(), Arc::new(buckets)],
            )?
        };

        let mut all_groups = builder.push(batch_with_bucket)?;
        all_groups.extend(builder.finish()?);

        // every group must correspond to a contiguous run of a single
        // group-max-bucket id, and the groups' total row count must equal
        // the input row count. Also: declared schema must stay identical
        // (data columns + ~TABLE) for every emitted batch, independent of
        // hybrid join being in effect - this is the whole point of packing
        // the bucket id into ~TABLE's metadata instead of a separate
        // column.
        let mut total_rows = 0;
        let mut seen_max_buckets = vec![];
        for g in &all_groups {
            assert_eq!(g.num_columns(), 2); // k + ~TABLE, never a 3rd column
            total_rows += g.num_rows();
            seen_max_buckets.push(peek_group_max_bucket(g)?.expect("hybrid join group batch"));
        }
        assert_eq!(total_rows, 8);
        // max buckets must be non-decreasing across emitted groups
        for i in 1..seen_max_buckets.len() {
            assert!(seen_max_buckets[i] >= seen_max_buckets[i - 1]);
        }
        Ok(())
    }

    #[test]
    fn group_builder_keeps_skewed_single_bucket_as_multiple_batches() -> Result<()> {
        // All rows map to the same bucket (single distinct key) and the
        // memory threshold is tiny, so every batch pushed in should stay
        // un-coalesced (no safe cut point exists) and the final group must
        // be emitted as multiple physical hash-map-batches that all share
        // the same group-max-bucket id.
        let schema = data_schema();
        let keys: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("k", 0))];
        let mut builder = BucketGroupBuilder::new(1, keys.clone(), schema.clone());

        let mut total_pushed_rows = 0;
        let mut all_groups = vec![];
        for chunk in [vec![1, 1, 1], vec![1, 1], vec![1, 1, 1, 1]] {
            total_pushed_rows += chunk.len();
            let data = int32_batch(schema.clone(), chunk);
            let bucket_schema = bucket_schema(&schema);
            let n = data.num_rows();
            let batch_with_bucket = RecordBatch::try_new(
                bucket_schema,
                vec![
                    data.column(0).clone(),
                    Arc::new(Int32Array::from(vec![0; n])),
                ],
            )?;
            all_groups.extend(builder.push(batch_with_bucket)?);
        }
        all_groups.extend(builder.finish()?);

        // single bucket, never safely splittable -> never flushed early;
        // everything comes out as one oversized logical group.
        assert!(builder.oversized_count() >= 1);
        assert!(
            all_groups.len() > 1,
            "expected the skewed group to be emitted as multiple physical batches, got {}",
            all_groups.len()
        );

        let total_rows: usize = all_groups.iter().map(|g| g.num_rows()).sum();
        assert_eq!(total_rows, total_pushed_rows);
        for g in &all_groups {
            assert_eq!(g.num_columns(), 2); // k + ~TABLE, never a 3rd column
        }
        // Only the last batch of a multi-batch group carries the bucket sentinel.
        assert_eq!(
            peek_group_max_bucket(all_groups.last().expect("expected a nonempty bucket group"))?,
            Some(0)
        );

        // round-trip through JoinHashMap to confirm the multi-batch group
        // is a valid, loadable hash map spanning all physical batches.
        let map = JoinHashMap::load_from_hash_map_batches(all_groups, &keys)?;
        assert_eq!(map.num_rows(), total_pushed_rows);
        Ok(())
    }

    #[test]
    fn split_batch_by_bucket_threshold_works() -> Result<()> {
        let schema = bucket_schema(&data_schema());
        let data = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let buckets = Int32Array::from(vec![0, 0, 1, 1, 2]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(data), Arc::new(buckets)])?;

        let (lo, hi) = split_batch_by_bucket_threshold(&batch, 1, 1)?;
        assert_eq!(lo.expect("expected lower bucket group").num_rows(), 4);
        assert_eq!(hi.expect("expected upper bucket group").num_rows(), 1);

        let (lo, hi) = split_batch_by_bucket_threshold(&batch, 1, 10)?;
        assert_eq!(lo.expect("expected lower bucket group").num_rows(), 5);
        assert!(hi.is_none());

        let (lo, hi) = split_batch_by_bucket_threshold(&batch, 1, -1)?;
        assert!(lo.is_none());
        assert_eq!(hi.expect("expected upper bucket group").num_rows(), 5);
        Ok(())
    }
}
