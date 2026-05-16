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

//! Microbenchmarks for JoinHashMap::lookup_many.
//!
//! Sweeps three build-side map sizes to cover different cache regimes:
//!   - 4K keys  (~2 MB map)  → fits in L2, mostly cache-hot
//!   - 64K keys (~32 MB map) → spills into L3
//!   - 1M keys  (~512 MB map)→ well beyond L3, cache-cold
//!
//! For each map size, three probe hit rates are measured:
//!   - 0%   (all misses)
//!   - 50%  (half hit, half miss)
//!   - 100% (all hits — typical inner-join scenario)
//!
//! Probe batch size is fixed at 4096 to match a typical Spark batch.

use std::sync::Arc;

use arrow::{
    array::Int32Array,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use datafusion_ext_plans::joins::join_hash_map::JoinHashMap;

/// Probe batch size — matches the default Spark vectorized batch size.
const PROBE_SIZE: usize = 4096;

/// Build-side hash base: top bit always set so hashes are non-zero
/// (matches join_create_hashes convention).
const BUILD_HASH_BASE: u32 = 0x8000_0001;

fn make_map(build_size: usize) -> (JoinHashMap, Vec<u32>) {
    let values: Vec<i32> = (0..build_size as i32).collect();
    let array = Arc::new(Int32Array::from(values));
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![array.clone() as _]).expect("build batch");

    let build_hashes: Vec<u32> = (0..build_size as u32)
        .map(|i| BUILD_HASH_BASE.wrapping_add(i))
        .collect();

    let map = JoinHashMap::create_from_data_batch_and_hashes(
        batch,
        vec![array as _],
        build_hashes.clone(),
    )
    .expect("build map");

    (map, build_hashes)
}

/// Generate PROBE_SIZE hashes at the given hit rate.
/// Hits and misses are interleaved evenly (Bresenham) to avoid cache grouping
/// bias.
fn make_probe_hashes(build_hashes: &[u32], build_size: usize, hit_rate: f64) -> Vec<u32> {
    // Miss hashes start well above the build range to avoid accidental collision.
    let miss_base: u32 = BUILD_HASH_BASE.wrapping_add(build_size as u32 + 0x0010_0000);
    let build_len = build_hashes.len();
    (0..PROBE_SIZE)
        .map(|i| {
            let cumulative = ((i + 1) as f64 * hit_rate) as usize;
            let prev = (i as f64 * hit_rate) as usize;
            if cumulative > prev {
                build_hashes[i % build_len]
            } else {
                miss_base.wrapping_add(i as u32)
            }
        })
        .collect()
}

fn bench_lookup_many(c: &mut Criterion) {
    // (label, build_size)
    // map memory ≈ (build_size * 2 / 8).next_power_of_two() * 64 bytes
    //   5M  →  ~128 MB  (realistic BHJ, ~50 MB serialized data)
    //   10M →  ~256 MB  (realistic BHJ, ~100–200 MB serialized data)
    //   20M →  ~512 MB  (realistic BHJ, ~1 GB serialized data)
    let build_sizes: &[(&str, usize)] = &[
        ("build=5M", 5_000_000),
        ("build=10M", 10_000_000),
        ("build=20M", 20_000_000),
    ];
    let hit_rates: &[(&str, f64)] = &[("hit=0%", 0.0), ("hit=50%", 0.5), ("hit=100%", 1.0)];

    let mut group = c.benchmark_group("JoinHashMap::lookup_many");

    for &(size_label, build_size) in build_sizes {
        let (map, build_hashes) = make_map(build_size);
        for &(rate_label, hit_rate) in hit_rates {
            let probe = make_probe_hashes(&build_hashes, build_size, hit_rate);
            let label = format!("{size_label}/{rate_label}");
            group.bench_with_input(BenchmarkId::from_parameter(&label), &label, |b, _| {
                b.iter(|| {
                    let result = map.lookup_many(black_box(probe.clone()));
                    black_box(result)
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_lookup_many);
criterion_main!(benches);
