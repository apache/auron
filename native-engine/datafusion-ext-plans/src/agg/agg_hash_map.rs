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

use std::hash::BuildHasher;

use datafusion_ext_commons::{likely, prefetch_write_data, unchecked};
use fearless_simd::{Level, Simd, dispatch, prelude::*, u32x8};
use unchecked_index::UncheckedIndex;

use crate::agg::agg_table::OwnedKey;

const MAP_VALUE_GROUP_SIZE: usize = 8;

#[derive(Clone, Copy, Default)]
#[repr(align(64))] // ensure one group can be cached into a cache line
struct MapValueGroup {
    hashes: [u32; MAP_VALUE_GROUP_SIZE],
    values: [u32; MAP_VALUE_GROUP_SIZE],
}
const _MAP_VALUE_GROUP_SIZE_CHECKER: [(); 64] = [(); size_of::<MapValueGroup>()];

struct Table {
    pub map: UncheckedIndex<Vec<MapValueGroup>>,
    pub map_mod_bits: u32,
    pub key_heap_mem_size: usize,
    pub keys: UncheckedIndex<Vec<OwnedKey>>,
}

impl Default for Table {
    fn default() -> Self {
        Self {
            map: unchecked!(vec![]),
            map_mod_bits: 0,
            key_heap_mem_size: 0,
            keys: unchecked!(vec![]),
        }
    }
}

impl Table {
    fn len(&self) -> usize {
        self.keys.len()
    }

    fn mem_size(&self) -> usize {
        size_of_val(self)
            + self.map.capacity() * 2 * size_of::<MapValueGroup>()
            + self.keys.capacity() * 2 * size_of::<OwnedKey>()
            + self.key_heap_mem_size
    }

    fn reserve(&mut self, num_new_items: usize) {
        let num_reserved_items = self.len() + num_new_items;
        let new_map_mod_bits = (num_reserved_items.max(128) * 2 / MAP_VALUE_GROUP_SIZE)
            .next_power_of_two()
            .trailing_zeros();

        if new_map_mod_bits > self.map_mod_bits {
            self.rehash(new_map_mod_bits);
        }
    }

    fn upsert_many(&mut self, keys: Vec<impl AggHashMapKey>) -> Vec<u32> {
        dispatch!(Level::new(), simd => self.upsert_many_impl(simd, keys))
    }

    #[inline(always)]
    fn upsert_many_impl<S: Simd>(&mut self, simd: S, keys: Vec<impl AggHashMapKey>) -> Vec<u32> {
        let mut hashes = unchecked!(keys.iter().map(agg_hash).collect::<Vec<_>>());
        const PREFETCH_AHEAD: usize = 4;

        macro_rules! entries {
            ($i:expr) => {{ (hashes[$i] % (1 << self.map_mod_bits)) }};
        }

        macro_rules! prefetch_at {
            ($i:expr) => {{
                if $i < hashes.len() {
                    prefetch_write_data!(&self.map[entries!($i) as usize]);
                }
            }};
        }

        for i in 0..PREFETCH_AHEAD {
            prefetch_at!(i);
        }

        for (i, key) in keys.into_iter().enumerate() {
            prefetch_at!(i + PREFETCH_AHEAD);
            hashes[i] = self.upsert_one_impl(simd, key, hashes[i], entries!(i) as usize);
        }

        // safety: transmute to Vec<u32>
        unsafe { std::mem::transmute(hashes) }
    }

    #[inline(always)]
    fn upsert_one_impl<S: Simd>(
        &mut self,
        simd: S,
        key: impl AggHashMapKey,
        hash: u32,
        mut entries: usize,
    ) -> u32 {
        let hashes = u32x8::splat(simd, hash);
        let zeros = u32x8::splat(simd, 0);
        loop {
            let group_hashes = u32x8::from_slice(simd, &self.map[entries].hashes);
            let hash_matched = group_hashes.simd_eq(hashes);
            let mut bits = hash_matched.to_bitmask();
            while bits != 0 {
                let i = bits.trailing_zeros() as usize;
                bits &= bits - 1;
                let record_idx = self.map[entries].values[i] as usize;
                if likely!(self.keys[record_idx].as_ref() == key.as_bytes()) {
                    return record_idx as u32;
                }
            }

            let empty = group_hashes.simd_eq(zeros);
            let empty_bits = empty.to_bitmask();
            if empty_bits != 0 {
                let empty_pos = empty_bits.trailing_zeros() as usize;
                let record_idx = self.len();
                self.map[entries].hashes[empty_pos] = hash;
                self.map[entries].values[empty_pos] = record_idx as u32;

                let key = key.into_owned();
                if key.spilled() {
                    self.key_heap_mem_size += key.len();
                }
                self.keys.push(key);
                return record_idx as u32;
            }
            entries += 1;
            entries %= 1 << self.map_mod_bits;
        }
    }

    #[inline]
    fn rehash(&mut self, map_mod_bits: u32) {
        dispatch!(Level::new(), simd => self.rehash_impl(simd, map_mod_bits))
    }

    #[inline(always)]
    fn rehash_impl<S: Simd>(&mut self, simd: S, map_mod_bits: u32) {
        let mut rehashed_map = unchecked!(vec![MapValueGroup::default(); 1 << map_mod_bits]);
        let zeros = u32x8::splat(simd, 0);
        let new_mods_mask = (1u32 << map_mod_bits) - 1;

        for group in self.map.drain(..) {
            let group_hashes = u32x8::from_slice(simd, &group.hashes);
            let new_entries: [u32; MAP_VALUE_GROUP_SIZE] =
                (group_hashes & new_mods_mask).to_array();
            for &e in new_entries.iter().rev() {
                prefetch_write_data!(&rehashed_map[e as usize]);
            }

            let non_empty = group_hashes.simd_ne(zeros);
            for j in 0..MAP_VALUE_GROUP_SIZE {
                if non_empty.test(j) {
                    let mut e = new_entries[j] as usize;
                    loop {
                        let empty_bits = u32x8::from_slice(simd, &rehashed_map[e].hashes)
                            .simd_eq(zeros)
                            .to_bitmask();
                        if empty_bits != 0 {
                            let empty_pos = empty_bits.trailing_zeros() as usize;
                            rehashed_map[e].hashes[empty_pos] = group.hashes[j];
                            rehashed_map[e].values[empty_pos] = group.values[j];
                            break;
                        }
                        e += 1;
                        e %= 1 << map_mod_bits;
                    }
                }
            }
        }
        self.map_mod_bits = map_mod_bits;
        self.map = rehashed_map;
    }
}

pub trait AggHashMapKey {
    fn as_bytes(&self) -> &[u8];
    fn into_owned(self) -> OwnedKey;
}

impl AggHashMapKey for &[u8] {
    fn as_bytes(&self) -> &[u8] {
        self
    }

    fn into_owned(self) -> OwnedKey {
        OwnedKey::from_slice(self)
    }
}

impl AggHashMapKey for OwnedKey {
    fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
    fn into_owned(self) -> OwnedKey {
        self
    }
}

// map<key: bytes, value: u32> where value is the index of accumulators
#[derive(Default)]
pub struct AggHashMap {
    map: Table,
}

impl AggHashMap {
    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn mem_size(&self) -> usize {
        self.map.mem_size()
    }

    pub fn upsert_records(&mut self, keys: Vec<impl AggHashMapKey>) -> Vec<u32> {
        self.map.reserve(keys.len());
        self.map.upsert_many(keys)
    }

    pub fn take_keys(&mut self) -> Vec<OwnedKey> {
        self.map.map.fill(MapValueGroup::default());
        self.map.key_heap_mem_size = 0;
        std::mem::take(&mut *self.map.keys)
    }

    pub fn into_keys(self) -> Vec<OwnedKey> {
        let mut keys = self.map.keys;
        std::mem::take(&mut keys)
    }
}

#[inline]
pub fn agg_hash(value: &impl AggHashMapKey) -> u32 {
    // 32-bits non-zero hash
    const AGG_HASH_SEED_HASHING: i64 = 0x3F6F1B93;
    const HASHER: foldhash::fast::FixedState =
        foldhash::fast::FixedState::with_seed(AGG_HASH_SEED_HASHING as u64);
    HASHER.hash_one(value.as_bytes()) as u32 | 0x80000000
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;

    /// Inserts the same set of keys across multiple batches (forcing several
    /// `rehash` calls along the way) and checks that:
    /// - the same key always maps to the same record index across batches
    /// - distinct keys always map to distinct record indices
    /// This exercises the fearless_simd based probing/rehashing logic
    /// (in particular the `& mask` replacement for the old `% new_mods`
    /// SIMD modulo).
    #[test]
    fn test_upsert_many_rehash_consistency() {
        let mut map = AggHashMap::default();
        let n = 2000u32;
        let mut ids_by_key: HashMap<u32, u32> = HashMap::default();

        for _round in 0..3 {
            let batch: Vec<Vec<u8>> = (0..n).map(|i| i.to_le_bytes().to_vec()).collect();
            let batch_refs: Vec<&[u8]> = batch.iter().map(|v| v.as_slice()).collect();
            let ids = map.upsert_records(batch_refs);
            assert_eq!(ids.len(), n as usize);
            for (i, &id) in ids.iter().enumerate() {
                match ids_by_key.get(&(i as u32)) {
                    Some(&prev) => assert_eq!(prev, id, "key {i} got different id across rounds"),
                    None => {
                        ids_by_key.insert(i as u32, id);
                    }
                }
            }
        }

        assert_eq!(map.len(), n as usize);
        let mut seen = HashSet::new();
        for &id in ids_by_key.values() {
            assert!(seen.insert(id), "duplicate id assigned to different keys");
        }
    }
}
