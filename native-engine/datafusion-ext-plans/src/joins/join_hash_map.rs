// Copyright 2022 The Blaze Authors
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

use std::{
    fmt::{Debug, Formatter},
    io::Cursor,
    mem::MaybeUninit,
    simd::{cmp::SimdPartialEq, Simd},
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayRef, AsArray, BinaryBuilder, RecordBatch},
    datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
};
use datafusion::{common::Result, physical_expr::PhysicalExprRef};
use datafusion_ext_commons::{
    io::{read_len, read_raw_slice, write_len, write_raw_slice},
    rdxsort::RadixSortIterExt,
    spark_hash::create_hashes,
};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use unchecked_index::UncheckedIndex;

use crate::{prefetch_read_data, unchecked};

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
            .radix_sorted_unstable_by_key(|&(_idx, hash)| hash)
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
                        let single = mapped_indices.pop().unwrap();
                        let _len = mapped_indices.pop().unwrap();
                        MapValue::new_single(single)
                    }
                    _ => MapValue::new_range(start),
                },
            ));
        }

        // build map
        let map_mod_bits = (map_items.len() * 2 + 1)
            .next_power_of_two()
            .trailing_zeros();
        let mut map = unchecked!(Vec::with_capacity((1usize << map_mod_bits) + 16));
        map.resize(1 << map_mod_bits, MapValueGroup::default());

        macro_rules! entries {
            [$i:expr] => (map_items[$i].0 % (1 << map_mod_bits))
        }

        const PREFETCH_AHEAD: usize = 4;
        if map_items.len() >= PREFETCH_AHEAD {
            for i in 1..PREFETCH_AHEAD - 1 {
                prefetch_read_data!(&map[entries![i] as usize]);
            }
        }
        for i in 0..map_items.len() {
            if i + PREFETCH_AHEAD < map_items.len() {
                prefetch_read_data!(&map[entries![i + PREFETCH_AHEAD] as usize]);
            }

            let mut e = entries![i] as usize;
            loop {
                let empty = map[e].hashes.simd_eq(Simd::splat(0));
                if let Some(j) = empty.first_set() {
                    map[e].hashes.as_mut_array()[j] = map_items[i].0;
                    map[e].values[j] = map_items[i].1;
                    break;
                }

                e += 1;
                if e == map.len() {
                    map.push(MapValueGroup::default());
                }
            }
        }
        map.push(MapValueGroup::default()); // sentinel at the end

        Ok(Table {
            num_valid_items,
            map_mod_bits,
            map,
            mapped_indices,
        })
    }

    pub fn load_from_raw_bytes(raw_bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(raw_bytes);

        // read map
        let num_valid_items = read_len(&mut cursor)?;
        let map_mod_bits = read_len(&mut cursor)? as u32;
        let map_len = read_len(&mut cursor)?;
        let mut map = vec![
            unsafe {
                // safety: no need to init to zeros
                #[allow(invalid_value)]
                MaybeUninit::<MapValueGroup>::uninit().assume_init()
            };
            map_len
        ];
        read_raw_slice(&mut map, &mut cursor)?;

        // read mapped indices
        let mapped_indices_len = read_len(&mut cursor)?;
        let mut mapped_indices = Vec::with_capacity(mapped_indices_len);
        for _ in 0..mapped_indices_len {
            mapped_indices.push(read_len(&mut cursor)? as u32);
        }

        Ok(Self {
            num_valid_items,
            map_mod_bits,
            map: unchecked!(map),
            mapped_indices: unchecked!(mapped_indices),
        })
    }

    pub fn try_into_raw_bytes(self) -> Result<Vec<u8>> {
        let mut raw_bytes = Vec::with_capacity(
            (8 + self.mapped_indices.len() + size_of::<u32>())
                + (24 + self.map.len() * size_of::<MapValueGroup>()),
        );

        // write map
        write_len(self.num_valid_items, &mut raw_bytes)?;
        write_len(self.map_mod_bits as usize, &mut raw_bytes)?;
        write_len(self.map.len(), &mut raw_bytes)?;
        write_raw_slice(&self.map, &mut raw_bytes)?;

        // write mapped indices
        write_len(self.mapped_indices.len(), &mut raw_bytes)?;
        for &v in self.mapped_indices.as_slice() {
            write_len(v as usize, &mut raw_bytes)?;
        }

        raw_bytes.shrink_to_fit();
        Ok(raw_bytes)
    }

    pub fn lookup(&self, hash: u32) -> MapValue {
        let mut i = (hash % (1 << self.map_mod_bits)) as usize;
        loop {
            let hash_matched = self.map[i].hashes.simd_eq(Simd::splat(hash));
            let empty = self.map[i].hashes.simd_eq(Simd::splat(0));

            if let Some(pos) = (hash_matched | empty).first_set() {
                return self.map[i].values[pos];
            }
            i += 1;
        }
    }

    pub fn lookup_many(&self, hashes: Vec<u32>) -> Vec<MapValue> {
        const PREFETCH_AHEAD: usize = 4;
        let mut hashes = unchecked!(hashes);

        macro_rules! entries {
            [$i:expr] => (hashes[$i] % (1 << self.map_mod_bits))
        }

        if hashes.len() >= PREFETCH_AHEAD {
            for i in 1..PREFETCH_AHEAD - 1 {
                prefetch_read_data!(&self.map[entries![i] as usize]);
            }
        }

        for i in 0..hashes.len() {
            if i + PREFETCH_AHEAD < hashes.len() {
                prefetch_read_data!(&self.map[entries![i + PREFETCH_AHEAD] as usize]);
            }

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
            }
        }

        unsafe {
            // safety: transmute Vec<u32> to Vec<MapValue(u32)>
            std::mem::transmute(hashes)
        }
    }
}

pub struct JoinHashMap {
    data_batch: RecordBatch,
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

impl JoinHashMap {
    pub fn create_from_data_batch(
        data_batch: RecordBatch,
        key_exprs: &[PhysicalExprRef],
    ) -> Result<Self> {
        let key_columns: Vec<ArrayRef> = key_exprs
            .iter()
            .map(|expr| {
                Ok(expr
                    .evaluate(&data_batch)?
                    .into_array(data_batch.num_rows())?)
            })
            .collect::<Result<_>>()?;

        let table = Table::create_from_key_columns(data_batch.num_rows(), &key_columns)?;

        Ok(Self {
            data_batch,
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
        let mut data_batch = hash_map_batch.clone();
        let table = Table::load_from_raw_bytes(
            data_batch
                .remove_column(data_batch.num_columns() - 1)
                .as_binary::<i32>()
                .value(0),
        )?;
        let key_columns: Vec<ArrayRef> = key_exprs
            .iter()
            .map(|expr| {
                Ok(expr
                    .evaluate(&data_batch)?
                    .into_array(data_batch.num_rows())?)
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            data_batch,
            key_columns,
            table,
        })
    }

    pub fn into_hash_map_batch(self) -> Result<RecordBatch> {
        let schema = join_hash_map_schema(&self.data_batch.schema());
        if self.data_batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(schema));
        }
        let mut table_col_builder = BinaryBuilder::new();
        table_col_builder.append_value(&self.table.try_into_raw_bytes()?);
        for _ in 1..self.data_batch.num_rows() {
            table_col_builder.append_null();
        }
        let table_col: ArrayRef = Arc::new(table_col_builder.finish());
        Ok(RecordBatch::try_new(
            schema,
            vec![self.data_batch.columns().to_vec(), vec![table_col]].concat(),
        )?)
    }

    pub fn data_schema(&self) -> SchemaRef {
        self.data_batch().schema()
    }

    pub fn data_batch(&self) -> &RecordBatch {
        &self.data_batch
    }

    pub fn key_columns(&self) -> &[ArrayRef] {
        &self.key_columns
    }

    pub fn is_all_nulls(&self) -> bool {
        self.table.num_valid_items == 0
    }

    pub fn is_empty(&self) -> bool {
        self.data_batch.num_rows() == 0
    }

    pub fn lookup(&self, hash: u32) -> MapValue {
        self.table.lookup(hash)
    }

    pub fn lookup_many(&self, hashes: Vec<u32>) -> Vec<MapValue> {
        self.table.lookup_many(hashes)
    }

    pub fn get_range(&self, map_value: MapValue) -> &[u32] {
        map_value.get_range(self)
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
    let mut hashes = create_hashes(num_rows, key_columns, JOIN_HASH_RANDOM_SEED, |v, h| {
        gxhash::gxhash32(v, h as i64)
    });

    // use 31-bit non-zero hash
    for h in &mut hashes {
        *h |= 0x80000000;
    }
    hashes
}

#[inline]
fn join_table_field() -> FieldRef {
    static BHJ_KEY_FIELD: OnceCell<FieldRef> = OnceCell::new();
    BHJ_KEY_FIELD
        .get_or_init(|| Arc::new(Field::new("~TABLE", DataType::Binary, true)))
        .clone()
}
