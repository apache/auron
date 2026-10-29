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
    hash::{BuildHasher, Hasher},
    io::{Cursor, Read, Write},
    simd::{Simd, cmp::SimdPartialEq},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryBuilder, BooleanArray, RecordBatch, UInt32Array},
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{
    SliceAsRawBytes, UninitializedInit,
    arrow::{
        coalesce::coalesce_arrays_unchecked,
        selection::{create_batch_interleaver, take_cols},
    },
    io::{read_len, write_len},
    prefetch_read_data,
    spark_hash::create_hashes,
    unchecked,
};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use unchecked_index::UncheckedIndex;

// empty:  lead=0, value=0
// range:  lead=0, value=start, mapped_indices[start-1]=len
// single: lead=1, value=idx
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub struct MapValue(u32);

impl MapValue {
    pub const EMPTY: MapValue = MapValue(0);

    pub fn new_single(idx: u32) -> Self {
        Self(1 << 31 | idx)
    }

    pub fn new_range(start: u32) -> Self {
        Self(start)
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn is_single(&self) -> bool {
        self.0 >> 31 == 1
    }

    pub fn is_range(&self) -> bool {
        self.0 >> 31 == 0 && !self.is_empty()
    }

    pub fn get_single(&self) -> u32 {
        self.0 & 0x7fffffff
    }

    pub fn get_range<'a>(&self, map: &'a JoinHashMap) -> &'a [u32] {
        let start = self.0 as usize;
        let len = map.table.mapped_indices[start - 1] as usize;
        let end = start + len;
        &map.table.mapped_indices[start..end]
    }
}

const MAP_VALUE_GROUP_SIZE: usize = 8;

#[derive(Clone, Copy, Default)]
#[repr(align(64))] // ensure one group can be cached into a cache line
struct MapValueGroup {
    hashes: Simd<u32, MAP_VALUE_GROUP_SIZE>,
    values: [MapValue; MAP_VALUE_GROUP_SIZE],
}
const _MAP_VALUE_GROUP_SIZE_CHECKER: [(); 64] = [(); size_of::<MapValueGroup>()];

struct Table {
    num_valid_items: usize,
    map_mod_bits: u32,
    map: UncheckedIndex<Vec<MapValueGroup>>,
    mapped_indices: UncheckedIndex<Vec<u32>>,
}

impl Table {
    fn create_from_key_columns(num_rows: usize, key_columns: &[ArrayRef]) -> Result<Self> {
        assert!(
            num_rows < 1073741824,
            "join hash table: number of rows exceeded 2^30: {num_rows}"
        );
        let hashes = join_create_hashes(num_rows, key_columns);
        Self::create_from_key_columns_and_hashes(num_rows, key_columns, hashes)
    }

    fn create_from_key_columns_and_hashes(
        num_rows: usize,
        key_columns: &[ArrayRef],
        hashes: Vec<u32>,
    ) -> Result<Self> {
        assert!(
            num_rows < 1073741824,
            "join hash table: number of rows exceeded 2^30: {num_rows}"
        );

        let key_is_valid = |row_idx| key_columns.iter().all(|col| col.is_valid(row_idx));
        let mut mapped_indices = unchecked!(vec![]);
        let mut num_valid_items = 0;

        // collect map items
        let mut map_items = unchecked!(vec![]);
        for (hash, chunk) in hashes
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| key_is_valid(*idx))
            .map(|(idx, hash)| {
                num_valid_items += 1;
                (idx as u32, hash)
            })
            .sorted_unstable_by_key(|&(idx, hash)| (hash, idx))
            .chunk_by(|(_, hash)| *hash)
            .into_iter()
        {
            let pos = mapped_indices.len() as u32;
            mapped_indices.push(0);
            mapped_indices.extend(chunk.map(|(idx, _hash)| idx));

            let start = pos + 1;
            let len = mapped_indices.len() as u32 - start;
            mapped_indices[pos as usize] = len;

            map_items.push((
                hash,
                match len {
                    0 => unreachable!(),
                    1 => {
                        let single = mapped_indices.pop().expect("missing index");
                        let _len = mapped_indices.pop().expect("missing length");
                        MapValue::new_single(single)
                    }
                    _ => MapValue::new_range(start),
                },
            ));
        }

        // build map
        let map_mod_bits = (map_items.len().max(128) * 2 / MAP_VALUE_GROUP_SIZE)
            .next_power_of_two()
            .trailing_zeros();
        let mut map = unchecked!(vec![MapValueGroup::default(); 1usize << map_mod_bits]);

        macro_rules! entries {
            [$i:expr] => (map_items[$i].0 % (1 << map_mod_bits))
        }

        const PREFETCH_AHEAD: usize = 4;
        for i in 0..map_items.len() {
            if i + PREFETCH_AHEAD < map_items.len() {
                prefetch_read_data!(&map[entries![i + PREFETCH_AHEAD] as usize]);
            }

            let mut e = entries![i] as usize;
            loop {
                let empty = map[e].hashes.simd_eq(Simd::splat(0));
                if let Some(empty_pos) = empty.first_set() {
                    map[e].hashes.as_mut_array()[empty_pos] = map_items[i].0;
                    map[e].values[empty_pos] = map_items[i].1;
                    break;
                }
                e += 1;
                e %= 1 << map_mod_bits;
            }
        }

        Ok(Table {
            num_valid_items,
            map_mod_bits,
            map,
            mapped_indices,
        })
    }

    pub fn read_from(mut r: impl Read) -> Result<Self> {
        // read map
        let num_valid_items = read_len(&mut r)?;
        let map_mod_bits = read_len(&mut r)? as u32;
        let mut map = Vec::uninitialized_init(1usize << map_mod_bits);
        r.read_exact(map.as_raw_bytes_mut())?;

        // read mapped indices
        let mapped_indices_len = read_len(&mut r)?;
        let mut mapped_indices = Vec::with_capacity(mapped_indices_len);
        for _ in 0..mapped_indices_len {
            mapped_indices.push(read_len(&mut r)? as u32);
        }

        Ok(Self {
            num_valid_items,
            map_mod_bits,
            map: unchecked!(map),
            mapped_indices: unchecked!(mapped_indices),
        })
    }

    pub fn write_to(self, mut w: impl Write) -> Result<()> {
        // write map
        write_len(self.num_valid_items, &mut w)?;
        write_len(self.map_mod_bits as usize, &mut w)?;
        w.write_all(self.map.as_raw_bytes())?;

        // write mapped indices
        write_len(self.mapped_indices.len(), &mut w)?;
        for &v in self.mapped_indices.as_slice() {
            write_len(v as usize, &mut w)?;
        }
        Ok(())
    }

    pub fn lookup_many(&self, hashes: Vec<u32>) -> Vec<MapValue> {
        let mut hashes = unchecked!(hashes);
        const PREFETCH_AHEAD: usize = 4;

        macro_rules! entries {
            [$i:expr] => (hashes[$i] % (1 << self.map_mod_bits))
        }

        macro_rules! prefetch_at {
            ($i:expr) => {{
                if $i < hashes.len() {
                    prefetch_read_data!(&self.map[entries!($i) as usize]);
                }
            }};
        }

        for i in 0..PREFETCH_AHEAD {
            prefetch_at!(i);
        }

        for i in 0..hashes.len() {
            prefetch_at!(i + PREFETCH_AHEAD);
            let mut e = entries![i] as usize;
            loop {
                let hash_matched = self.map[e].hashes.simd_eq(Simd::splat(hashes[i]));
                let empty = self.map[e].hashes.simd_eq(Simd::splat(0));

                if let Some(pos) = (hash_matched | empty).first_set() {
                    hashes[i] = unsafe {
                        // safety: transmute MapValue(u32) to u32
                        std::mem::transmute(self.map[e].values[pos])
                    };
                    break;
                }
                e += 1;
                e %= 1 << self.map_mod_bits;
            }
        }

        unsafe {
            // safety: transmute Vec<u32> to Vec<MapValue(u32)>
            std::mem::transmute(hashes)
        }
    }
}

pub struct JoinHashMap {
    // normally a single batch; may hold multiple un-coalesced batches when a
    // hybrid-join bucket group is too skewed to safely coalesce into one
    // `RecordBatch` without risking a >2GB offset overflow on variable-length
    // columns (see `crate::joins::bucketed_group`).
    data_batches: Vec<RecordBatch>,
    // row-count prefix sums over `data_batches`, length == data_batches.len()
    // + 1, used to resolve a global (virtual) row id into (batch_idx,
    // local_row_idx).
    row_offsets: Vec<u32>,
    key_columns: Vec<ArrayRef>,
    table: Table,
}

// safety: JoinHashMap is Send + Sync
unsafe impl Send for JoinHashMap {}
unsafe impl Sync for JoinHashMap {}

impl Debug for JoinHashMap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "JoinHashMap(..)")
    }
}

fn compute_row_offsets(data_batches: &[RecordBatch]) -> Vec<u32> {
    let mut offsets = Vec::with_capacity(data_batches.len() + 1);
    let mut acc = 0u32;
    offsets.push(acc);
    for batch in data_batches {
        acc += batch.num_rows() as u32;
        offsets.push(acc);
    }
    offsets
}

/// Evaluate `key_exprs` against every batch in `data_batches` and merge each
/// key column into a single flat `ArrayRef` spanning all batches (using the
/// same global row-id numbering as `row_offsets`).
///
/// NOTE: this still goes through `coalesce_arrays_unchecked`, so if a key
/// column is itself Utf8/Binary and its total size across one skewed bucket
/// exceeds 2GB, the i32 offset overflow described in `bucketed_group.rs`
/// still applies here. This is treated as an out-of-scope, extremely rare
/// edge case (real-world join keys are not multi-GB blobs).
fn evaluate_merged_key_columns(
    data_batches: &[RecordBatch],
    key_exprs: &[PhysicalExprRef],
) -> Result<Vec<ArrayRef>> {
    if data_batches.len() == 1 {
        return key_exprs
            .iter()
            .map(|expr| {
                Ok(expr
                    .evaluate(&data_batches[0])?
                    .into_array(data_batches[0].num_rows())?)
            })
            .collect();
    }
    key_exprs
        .iter()
        .map(|expr| {
            let per_batch: Vec<ArrayRef> = data_batches
                .iter()
                .map(|batch| Ok(expr.evaluate(batch)?.into_array(batch.num_rows())?))
                .collect::<Result<_>>()?;
            let data_type = per_batch[0].data_type().clone();
            Ok(coalesce_arrays_unchecked(&data_type, &per_batch))
        })
        .collect()
}

impl JoinHashMap {
    pub fn create_from_data_batch(
        data_batch: RecordBatch,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        Self::create_from_data_batches(vec![data_batch], key_exprs)
    }

    /// Like [`Self::create_from_data_batch`], but builds a single logical
    /// hash map spanning multiple (not coalesced) data batches. Used by
    /// hybrid join when a bucket group is too skewed to safely coalesce
    /// into one `RecordBatch`.
    pub fn create_from_data_batches(
        data_batches: Vec<RecordBatch>,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        let key_columns = evaluate_merged_key_columns(&data_batches, key_exprs)?;
        let num_rows = data_batches.iter().map(|b| b.num_rows()).sum();
        let table = Table::create_from_key_columns(num_rows, &key_columns)?;
        let row_offsets = compute_row_offsets(&data_batches);

        Ok(Self {
            data_batches,
            row_offsets,
            key_columns,
            table,
        })
    }

    pub fn create_from_data_batch_and_hashes(
        data_batch: RecordBatch,
        key_columns: Vec<ArrayRef>,
        hashes: Vec<u32>,
    ) -> Result<Self> {
        let table =
            Table::create_from_key_columns_and_hashes(data_batch.num_rows(), &key_columns, hashes)?;
        let row_offsets = compute_row_offsets(std::slice::from_ref(&data_batch));

        Ok(Self {
            data_batches: vec![data_batch],
            row_offsets,
            key_columns,
            table,
        })
    }

    pub fn create_empty(hash_map_schema: SchemaRef, key_exprs: &[PhysicalExprRef]) -> Result<Self> {
        let data_batch = RecordBatch::new_empty(hash_map_schema);
        Self::create_from_data_batch(data_batch, key_exprs)
    }

    pub fn load_from_hash_map_batch(
        hash_map_batch: RecordBatch,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        Self::load_from_hash_map_batches(vec![hash_map_batch], key_exprs)
    }

    /// Like [`Self::load_from_hash_map_batch`], but accepts multiple
    /// consecutive hash-map-batches belonging to the same logical bucket
    /// group (see [`Self::into_hash_map_batches`]/`bucketed_group.rs`), or
    /// multiple physical batches a single plain (non hybrid-join) hash map
    /// happened to be split into during its IPC round trip. Either way,
    /// exactly one row across all batches carries the table bytes (the
    /// sentinel row for hybrid-join groups, row 0 for plain hash maps);
    /// this is located by scanning every batch for its last non-null
    /// metadata row (see [`JoinHashMap::into_hash_map_batches`]).
    pub fn load_from_hash_map_batches(
        hash_map_batches: Vec<RecordBatch>,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        let mut data_batches = hash_map_batches;

        // Strip the metadata column from every batch and find the last non-null
        // metadata value, which for both plain and hybrid-join groups always
        // carries the table bytes (and the bucket id for hybrid groups).
        let mut last_metadata: Option<Vec<u8>> = None;
        for batch in &mut data_batches {
            let col_idx = batch.num_columns() - 1;
            let col = batch.remove_column(col_idx);
            let col = col.as_binary::<i32>();
            for i in (0..col.len()).rev() {
                if !col.is_null(i) {
                    last_metadata = Some(col.value(i).to_vec());
                    break;
                }
            }
        }

        let last = last_metadata.ok_or_else(|| {
            datafusion::common::DataFusionError::Execution(
                "first hash-map-batch must carry table bytes".to_string(),
            )
        })?;
        let (_, table_bytes) = decode_metadata(&last)?;
        let table_bytes = table_bytes.expect("first hash-map-batch must carry table bytes");
        let table = Table::read_from(&mut Cursor::new(table_bytes))?;

        let key_columns = evaluate_merged_key_columns(&data_batches, key_exprs)?;
        let row_offsets = compute_row_offsets(&data_batches);
        Ok(Self {
            data_batches,
            row_offsets,
            key_columns,
            table,
        })
    }

    /// Serialize this hash map back into one or more hash-map-batches. When
    /// `data_batches` holds a single batch (the overwhelming common case),
    /// behaves identically to the previous single-batch `into_hash_map_batch`.
    /// Otherwise emits one output batch per input data batch.
    ///
    /// `group_max_bucket` should be `None` for a plain (non hybrid-join)
    /// hash map, or `Some(bucket)` when this hash map represents one
    /// bucketed-group of a hybrid (bucketed hash) join. It is encoded into
    /// `~TABLE`'s own metadata (see [`encode_metadata`]) so the declared
    /// schema - `data columns + ~TABLE` - is identical in both cases.
    ///
    /// For hybrid-join batches (`group_max_bucket = Some(..)`): the **very
    /// last row of the last output batch** carries both the bucket sentinel
    /// and the table bytes (`encode_metadata(Some(bucket),
    /// Some(table_bytes))`). Every other row carries a non-null but
    /// payload-less marker (`encode_metadata(None, None)`) rather than a
    /// real null - this lets a consumer tell "hybrid join, no sentinel in
    /// this row" apart from "plain hash map, no data in this row" by
    /// looking at **any single row** (typically row 0), without needing to
    /// see the sentinel row at all. This matters because peeking only the
    /// stream's first physical batch to detect hybrid join must work
    /// correctly even when the first bucket group itself spans multiple
    /// physical batches (so its first batch has no sentinel row yet) - see
    /// [`peek_is_hybrid_join`]/`broadcast_join_exec.rs`.
    ///
    /// The `IpcReaderExec` on the read side only ever _coalesces_
    /// consecutive input batches (never splits one), so the sentinel row
    /// always remains the last non-null row of whatever coalesced batch the
    /// reader emits. The consumer scans backward from the end of each
    /// received batch to find sentinels.
    ///
    /// For non-hybrid batches the cheaper "row 0 only" sparse encoding
    /// (real SQL null elsewhere) is kept, since there is only one group and
    /// no boundary detection is needed.
    pub fn into_hash_map_batches(self, group_max_bucket: Option<i32>) -> Result<Vec<RecordBatch>> {
        let schema = join_hash_map_schema(&self.data_batches[0].schema());
        if self.num_rows() == 0 {
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }

        let mut table_data = vec![];
        self.table.write_to(&mut table_data)?;

        let total_batches = self.data_batches.len();
        let mut output = Vec::with_capacity(total_batches);
        for (i, data_batch) in self.data_batches.into_iter().enumerate() {
            let is_last_batch = i == total_batches - 1;
            let mut metadata_col_builder = BinaryBuilder::new();
            for j in 0..data_batch.num_rows() {
                let is_last_row = j == data_batch.num_rows() - 1;
                match group_max_bucket {
                    None => {
                        // plain hash map: only row 0 gets the table bytes.
                        if i == 0 && j == 0 {
                            metadata_col_builder
                                .append_value(encode_metadata(None, Some(table_data.as_slice())));
                        } else {
                            metadata_col_builder.append_null();
                        }
                    }
                    Some(bucket) => {
                        // hybrid-join: only the last row of the last batch
                        // carries the sentinel - bucket id + table bytes.
                        // Every other row carries a non-null, payload-less
                        // marker (see doc comment above) rather than a real
                        // null, so a consumer can detect "hybrid join" from
                        // any single row without needing the sentinel.
                        if is_last_batch && is_last_row {
                            metadata_col_builder.append_value(encode_metadata(
                                Some(bucket),
                                Some(table_data.as_slice()),
                            ));
                        } else {
                            metadata_col_builder.append_value(encode_metadata(None, None));
                        }
                    }
                }
            }
            let metadata_col: ArrayRef = Arc::new(metadata_col_builder.finish());
            output.push(RecordBatch::try_new(
                schema.clone(),
                vec![data_batch.columns().to_vec(), vec![metadata_col]].concat(),
            )?);
        }
        Ok(output)
    }

    pub fn data_schema(&self) -> SchemaRef {
        self.data_batches[0].schema()
    }

    pub fn num_rows(&self) -> usize {
        *self.row_offsets.last().expect("row_offsets non-empty") as usize
    }

    pub fn key_columns(&self) -> &[ArrayRef] {
        &self.key_columns
    }

    pub fn is_all_nulls(&self) -> bool {
        self.table.num_valid_items == 0
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }

    pub fn lookup_many(&self, hashes: Vec<u32>) -> Vec<MapValue> {
        self.table.lookup_many(hashes)
    }

    pub fn get_range(&self, map_value: MapValue) -> &[u32] {
        map_value.get_range(self)
    }

    /// Resolve a global (virtual) row id into `(batch_idx, local_row_idx)`.
    fn resolve_row(&self, global_row_id: u32) -> (usize, usize) {
        if self.data_batches.len() == 1 {
            return (0, global_row_id as usize);
        }
        // row_offsets[0] == 0 always, so partition_point - 1 is safe and in
        // range for any global_row_id < num_rows().
        let batch_idx = self.row_offsets.partition_point(|&o| o <= global_row_id) - 1;
        (
            batch_idx,
            (global_row_id - self.row_offsets[batch_idx]) as usize,
        )
    }

    /// Gather `col_indices` columns at `row_indices` (global row ids, may
    /// contain nulls - e.g. unmatched outer-join rows - which produce null
    /// output rows, same as `take_cols`) from the build-side data. Replaces
    /// the previous `project_left/right(map.data_batch().columns())` +
    /// `take_cols(..)` two-step for callers that only needed to read
    /// build-side output columns at a given set of row indices.
    pub fn gather_columns(
        &self,
        col_indices: &[usize],
        row_indices: impl Into<UInt32Array>,
    ) -> Result<Vec<ArrayRef>> {
        let row_indices: UInt32Array = row_indices.into();

        if self.data_batches.len() == 1 {
            let cols: Vec<ArrayRef> = col_indices
                .iter()
                .map(|&i| self.data_batches[0].column(i).clone())
                .collect();
            return take_cols(&cols, row_indices);
        }

        let projected_batches: Vec<RecordBatch> = self
            .data_batches
            .iter()
            .map(|batch| {
                let schema = Arc::new(Schema::new(
                    col_indices
                        .iter()
                        .map(|&i| batch.schema().fields()[i].clone())
                        .collect::<Vec<_>>(),
                ));
                let columns = col_indices
                    .iter()
                    .map(|&i| batch.column(i).clone())
                    .collect::<Vec<_>>();
                Ok(RecordBatch::try_new(schema, columns)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let interleaver = create_batch_interleaver(&projected_batches, false)?;

        // nulls in `row_indices` (unmatched outer rows) are mapped to a
        // dummy (0, 0) pair for the interleaver, then masked back to null
        // below.
        let pairs: Vec<(usize, usize)> = row_indices
            .iter()
            .map(|gid| match gid {
                Some(gid) => self.resolve_row(gid),
                None => (0, 0),
            })
            .collect();
        let batch = interleaver(&pairs)?;

        if row_indices.null_count() == 0 {
            return Ok(batch.columns().to_vec());
        }
        let is_null = BooleanArray::from_iter(row_indices.iter().map(|v| Some(v.is_none())));
        batch
            .columns()
            .iter()
            .map(|col| Ok(arrow::compute::nullif(col, &is_null)?))
            .collect()
    }
}

#[inline]
pub fn join_data_schema(hash_map_schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        hash_map_schema
            .fields()
            .iter()
            .take(hash_map_schema.fields().len() - 1) // exclude hash map column
            .cloned()
            .collect::<Vec<_>>(),
    ))
}

#[inline]
pub fn join_hash_map_schema(data_schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        data_schema
            .fields()
            .iter()
            .map(|field| Arc::new(field.as_ref().clone().with_nullable(true)))
            .chain(std::iter::once(join_table_field()))
            .collect::<Vec<_>>(),
    ))
}

#[inline]
pub fn join_create_hashes(num_rows: usize, key_columns: &[ArrayRef]) -> Vec<u32> {
    const JOIN_HASH_RANDOM_SEED: u32 = 0x1E39FA04;
    const HASHER: foldhash::fast::FixedState =
        foldhash::fast::FixedState::with_seed(JOIN_HASH_RANDOM_SEED as u64);
    let mut hashes = create_hashes(num_rows, key_columns, JOIN_HASH_RANDOM_SEED, |v, h| {
        let mut hasher = HASHER.build_hasher();
        hasher.write_u32(h);
        hasher.write(v);
        hasher.finish() as u32
    });

    // use 31-bit non-zero hash
    for h in &mut hashes {
        *h |= 0x80000000;
    }
    hashes
}

#[inline]
pub fn join_table_field() -> FieldRef {
    static BHJ_KEY_FIELD: OnceCell<FieldRef> = OnceCell::new();
    BHJ_KEY_FIELD
        .get_or_init(|| Arc::new(Field::new("~TABLE", DataType::Binary, true)))
        .clone()
}

/// Encode a hash-map-batch row-0 metadata value (the `~TABLE` column).
///
/// Historically this column held nothing but the serialized hash table
/// bytes (or null for "no table here"). Hybrid (bucketed hash) join needs
/// an extra per-batch piece of information - which bucket group a batch
/// belongs to - but the broadcast exchange's declared schema on the Scala
/// side is fixed to `data columns + ~TABLE` (one single binary column,
/// see `NativeBroadcastExchangeBase.hashMapOutput`), so a separate
/// `~BUCKET_END` column is not an option: whenever a batch produced by the
/// hybrid-join fallback carried that extra column, reading it back through
/// the broadcast exchange's IPC round trip (which decodes using the fixed
/// declared schema) silently decoded the wrong number of columns and
/// corrupted the whole stream.
///
/// Instead, both pieces of information are packed into this single binary
/// value: `[group_max_bucket: varint (0 = None, else value+1)][has_table:
/// varint][table_len: varint][table_bytes]` (the last two fields only
/// present when `has_table != 0`). This keeps the declared schema
/// identical regardless of whether hybrid join is in effect.
fn encode_metadata(group_max_bucket: Option<i32>, table_bytes: Option<&[u8]>) -> Vec<u8> {
    let mut buf = vec![];
    write_len(
        group_max_bucket.map(|b| b as usize + 1).unwrap_or(0),
        &mut buf,
    )
    .expect("write to Vec<u8> never fails");
    match table_bytes {
        Some(bytes) => {
            write_len(1, &mut buf).expect("write to Vec<u8> never fails");
            write_len(bytes.len(), &mut buf).expect("write to Vec<u8> never fails");
            buf.extend_from_slice(bytes);
        }
        None => {
            write_len(0, &mut buf).expect("write to Vec<u8> never fails");
        }
    }
    buf
}

/// Decode a metadata value produced by [`encode_metadata`]. Returns
/// `(group_max_bucket, table_bytes)`, where `table_bytes` borrows directly
/// from `bytes`.
fn decode_metadata(bytes: &[u8]) -> Result<(Option<i32>, Option<&[u8]>)> {
    let mut cursor = Cursor::new(bytes);
    let encoded_bucket = read_len(&mut cursor)?;
    let group_max_bucket = if encoded_bucket == 0 {
        None
    } else {
        Some((encoded_bucket - 1) as i32)
    };
    let has_table = read_len(&mut cursor)? != 0;
    let table_bytes = if has_table {
        let len = read_len(&mut cursor)?;
        let start = cursor.position() as usize;
        Some(&bytes[start..start + len])
    } else {
        None
    };
    Ok((group_max_bucket, table_bytes))
}

/// Peek whether a hash-map-batch is part of a hybrid (bucketed hash) join,
/// by decoding **row 0** of its `~TABLE` metadata column. This is reliable
/// even when row 0 isn't the sentinel row of its group (i.e. the batch is
/// a non-final fragment of a multi-batch bucket group), because
/// [`JoinHashMap::into_hash_map_batches`] always writes a non-null,
/// payload-less marker (`encode_metadata(None, None)`) into hybrid-join
/// rows that aren't the sentinel, whereas a plain (non hybrid-join) batch
/// always carries the table bytes at row 0 (`encode_metadata(None,
/// Some(table_bytes))`) and never writes this marker. A 0-row batch is
/// reported as non-hybrid (there is nothing to peek; the caller should
/// keep looking at the next batch).
pub fn peek_is_hybrid_join(hash_map_batch: &RecordBatch) -> Result<bool> {
    if hash_map_batch.num_rows() == 0 {
        return Ok(false);
    }
    let col = hash_map_batch
        .column(hash_map_batch.num_columns() - 1)
        .as_binary::<i32>();
    if col.is_null(0) {
        // Only plain (non hybrid-join) batches ever have an empty/null row
        // 0; see doc comment above.
        return Ok(false);
    }
    let (bucket, table_bytes) = decode_metadata(col.value(0))?;
    Ok(bucket.is_some() || table_bytes.is_none())
}

/// Peek the bucket group id (if any) a hash-map-batch belongs to, by
/// scanning its `~TABLE` metadata column backward from the last row to
/// find the bucket sentinel (see [`JoinHashMap::into_hash_map_batches`]).
/// Returns `None` for a plain (non hybrid-join) batch, or for a
/// hybrid-join batch that doesn't contain its group's sentinel row (e.g. a
/// non-final fragment of a multi-batch bucket group) - use
/// [`peek_is_hybrid_join`] to detect hybrid join in that case instead.
/// Returns `Some(bucket)` when the sentinel row is found.
pub fn peek_group_max_bucket(hash_map_batch: &RecordBatch) -> Result<Option<i32>> {
    let num_rows = hash_map_batch.num_rows();
    if num_rows == 0 {
        return Ok(None);
    }
    let col = hash_map_batch
        .column(hash_map_batch.num_columns() - 1)
        .as_binary::<i32>();
    for i in (0..num_rows).rev() {
        if !col.is_null(i) {
            let (bucket, _) = decode_metadata(col.value(i))?;
            if bucket.is_some() {
                return Ok(bucket);
            }
        }
    }
    Ok(None)
}

/// Split a hybrid-join hash-map-batch into maximal runs of consecutive rows
/// belonging to the same logical bucket group, returned as `(bucket, slice)`
/// pairs in original row order. A group ends at the row whose `~TABLE`
/// metadata carries a bucket sentinel (the non-marker, non-null value
/// written by [`JoinHashMap::into_hash_map_batches`] as the last row of
/// every group). All rows up to and including that sentinel belong to the
/// same group; rows after it start the next group. Payload-less marker
/// rows (non-null but no bucket - see [`peek_is_hybrid_join`]) are treated
/// the same as ordinary data rows here (not a boundary).
///
/// After an `IpcReaderExec` coalesce pass, a single incoming physical batch
/// may contain multiple complete groups (sentinel rows interleaved with
/// data rows), or just a fragment of a group with no sentinel yet. Callers
/// must accumulate fragments across batches until a sentinel is seen.
pub fn split_hash_map_batch_by_group(batch: RecordBatch) -> Result<Vec<(i32, RecordBatch)>> {
    if batch.num_rows() == 0 {
        return Ok(vec![]);
    }
    let col = batch.column(batch.num_columns() - 1).as_binary::<i32>();

    let mut result = vec![];
    let mut start = 0usize;
    for i in 0..batch.num_rows() {
        if !col.is_null(i) {
            let (bucket, _) = decode_metadata(col.value(i))?;
            if let Some(bucket) = bucket {
                result.push((bucket, batch.slice(start, i - start + 1)));
                start = i + 1;
            }
            // else: payload-less hybrid marker row - not a group boundary.
        }
    }
    if start < batch.num_rows() {
        result.push((-1, batch.slice(start, batch.num_rows() - start)));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::physical_expr::expressions::Column;

    use super::*;

    fn int32_data_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values)) as ArrayRef])
            .expect("valid Int32 test batch")
    }

    /// Normal (non hybrid-join) and hybrid-join hash-map-batches must share
    /// *exactly* the same declared schema (`data columns + ~TABLE`) -
    /// otherwise reading them back through the broadcast exchange's fixed
    /// Scala-declared schema (`NativeBroadcastExchangeBase.hashMapOutput`)
    /// silently decodes the wrong number of columns and corrupts the whole
    /// IPC stream. See the module-level docs on `encode_metadata`.
    #[test]
    fn normal_and_hybrid_batches_share_identical_schema() -> Result<()> {
        let key: PhysicalExprRef = Arc::new(Column::new("k", 0));

        let normal_batches =
            JoinHashMap::create_from_data_batch(int32_data_batch(vec![1, 2, 3]), &[key.clone()])?
                .into_hash_map_batches(None)?;

        let hybrid_batches = JoinHashMap::create_from_data_batches(
            vec![int32_data_batch(vec![1, 2]), int32_data_batch(vec![3])],
            &[key],
        )?
        .into_hash_map_batches(Some(7))?;

        assert_eq!(normal_batches[0].schema(), hybrid_batches[0].schema());
        for b in normal_batches.iter().chain(hybrid_batches.iter()) {
            assert_eq!(b.num_columns(), 2); // k + ~TABLE, never a 3rd column
        }

        assert_eq!(peek_group_max_bucket(&normal_batches[0])?, None);
        // Multi-batch hybrid group: only the last batch has the sentinel.
        assert_eq!(
            peek_group_max_bucket(
                hybrid_batches
                    .last()
                    .expect("expected hybrid hash-map batches")
            )?,
            Some(7)
        );
        Ok(())
    }

    /// Regression test: `collect_join_hash_map_without_caching` in
    /// `broadcast_join_exec.rs` decides whether a build-side stream is a
    /// hybrid (bucketed hash) join by peeking only its *first* physical
    /// batch. If the very first bucket group itself spans multiple
    /// physical batches (oversized group), that first batch carries no
    /// bucket sentinel at all - every row is a payload-less marker. Using
    /// [`peek_group_max_bucket`] (which only finds `Some(bucket)` at an
    /// actual sentinel row) for that detection would wrongly report "not
    /// hybrid", causing the caller to collect the *entire* build stream
    /// into memory via `try_collect` instead of streaming group by group -
    /// defeating hybrid join's whole memory-bounding purpose.
    /// [`peek_is_hybrid_join`] must correctly say "yes" from row 0 alone.
    #[test]
    fn peek_is_hybrid_join_detects_first_batch_of_oversized_first_group() -> Result<()> {
        let key: PhysicalExprRef = Arc::new(Column::new("k", 0));

        // multi-batch hybrid group: into_hash_map_batches(Some(_)) emits one
        // batch per input data batch; take the FIRST one, which (being an
        // oversized group spanning >1 batches) carries no sentinel.
        let hybrid_batches = JoinHashMap::create_from_data_batches(
            vec![int32_data_batch(vec![1, 2]), int32_data_batch(vec![3])],
            &[key.clone()],
        )?
        .into_hash_map_batches(Some(7))?;
        assert_eq!(hybrid_batches.len(), 2, "expected one batch per data batch");
        let first_batch_of_oversized_group = &hybrid_batches[0];

        // this batch has no sentinel row - peek_group_max_bucket can't see it.
        assert_eq!(peek_group_max_bucket(first_batch_of_oversized_group)?, None);
        // but peek_is_hybrid_join must still correctly identify it as hybrid.
        assert!(peek_is_hybrid_join(first_batch_of_oversized_group)?);

        // sanity check: a plain (non hybrid-join) batch must still be
        // correctly identified as NOT hybrid.
        let normal_batches =
            JoinHashMap::create_from_data_batch(int32_data_batch(vec![1, 2, 3]), &[key])?
                .into_hash_map_batches(None)?;
        assert!(!peek_is_hybrid_join(&normal_batches[0])?);
        Ok(())
    }

    #[test]
    fn metadata_round_trips_with_and_without_table_bytes() -> Result<()> {
        let encoded = encode_metadata(None, Some(b"hello"));
        assert_eq!(decode_metadata(&encoded)?, (None, Some(b"hello".as_ref())));

        let encoded = encode_metadata(Some(42), Some(b"world"));
        assert_eq!(
            decode_metadata(&encoded)?,
            (Some(42), Some(b"world".as_ref()))
        );

        let encoded = encode_metadata(Some(0), None);
        assert_eq!(decode_metadata(&encoded)?, (Some(0), None));

        let encoded = encode_metadata(None, None);
        assert_eq!(decode_metadata(&encoded)?, (None, None));
        Ok(())
    }

    #[test]
    fn load_from_hash_map_batches_round_trips_multi_batch_group() -> Result<()> {
        let key: PhysicalExprRef = Arc::new(Column::new("k", 0));
        let batches = JoinHashMap::create_from_data_batches(
            vec![int32_data_batch(vec![1, 2]), int32_data_batch(vec![3, 1])],
            &[key.clone()],
        )?
        .into_hash_map_batches(Some(3))?;

        let loaded = JoinHashMap::load_from_hash_map_batches(batches, &[key])?;
        assert_eq!(loaded.num_rows(), 4);
        Ok(())
    }
}
