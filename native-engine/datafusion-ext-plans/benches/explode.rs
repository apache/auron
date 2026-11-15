// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(test)]

extern crate test;

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int32Array, ListArray, MapArray},
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::{DataType, Field, Int32Type, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    execution::TaskContext,
    physical_expr::expressions::Column,
    physical_plan::{ExecutionPlan, common, test::TestMemoryExec},
    prelude::SessionContext,
};
use datafusion_ext_plans::{
    generate::{GenerateFunc, create_generator},
    generate_exec::GenerateExec,
};
use test::{Bencher, black_box};

const NUM_ROWS: usize = 10_000;
const VALUES_PER_ROW: usize = 8;
const NULL_EVERY: usize = 4;

fn list_batch() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..NUM_ROWS as i32));
    let rows = (0..NUM_ROWS).map(|row| {
        Some(
            (0..VALUES_PER_ROW)
                .map(|value| Some((row * VALUES_PER_ROW + value) as i32))
                .collect::<Vec<_>>(),
        )
    });
    let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(rows));
    RecordBatch::try_from_iter([("id", ids), ("items", lists)])
        .expect("list benchmark batch should be valid")
}

fn map_batch() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..NUM_ROWS as i32));
    let num_values = NUM_ROWS * VALUES_PER_ROW;
    let keys = (0..num_values)
        .map(|index| format!("key_{}", index % VALUES_PER_ROW))
        .collect::<Vec<_>>();
    let values = Int32Array::from_iter_values(0..num_values as i32);
    let offsets = (0..=NUM_ROWS)
        .map(|row| (row * VALUES_PER_ROW) as u32)
        .collect::<Vec<_>>();
    let maps: ArrayRef = Arc::new(
        MapArray::new_from_strings(keys.iter().map(String::as_str), &values, &offsets)
            .expect("map benchmark array should be valid"),
    );
    RecordBatch::try_from_iter([("id", ids), ("items", maps)])
        .expect("map benchmark batch should be valid")
}

fn list_with_parent_null_gaps_batch() -> RecordBatch {
    let ids: ArrayRef = Arc::new(Int32Array::from_iter_values(0..NUM_ROWS as i32));
    let offsets = OffsetBuffer::new(
        (0..=NUM_ROWS)
            .map(|row| (row * VALUES_PER_ROW) as i32)
            .collect::<Vec<_>>()
            .into(),
    );
    let values: ArrayRef = Arc::new(Int32Array::from_iter_values(
        0..(NUM_ROWS * VALUES_PER_ROW) as i32,
    ));
    let nulls = NullBuffer::from(
        (0..NUM_ROWS)
            .map(|row| row % NULL_EVERY != 0)
            .collect::<Vec<_>>(),
    );
    let lists: ArrayRef = Arc::new(
        ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            offsets,
            values,
            Some(nulls),
        )
        .expect("list benchmark array with parent nulls should be valid"),
    );
    RecordBatch::try_from_iter([("id", ids), ("items", lists)])
        .expect("list benchmark batch with parent nulls should be valid")
}

fn generate_exec(
    batch: RecordBatch,
    generate_func: GenerateFunc,
    output_fields: Vec<Field>,
) -> Arc<dyn ExecutionPlan> {
    let input = Arc::new(
        TestMemoryExec::try_new(&[vec![batch.clone()]], batch.schema(), None)
            .expect("benchmark input plan should be valid"),
    );
    let generator = create_generator(
        &input.schema(),
        generate_func,
        vec![Arc::new(Column::new("items", 1))],
    )
    .expect("benchmark generator should be valid");
    Arc::new(
        GenerateExec::try_new(
            input,
            generator,
            vec![Column::new("id", 0)],
            Arc::new(Schema::new(output_fields)),
            false,
        )
        .expect("benchmark generate plan should be valid"),
    )
}

fn execute(
    runtime: &tokio::runtime::Runtime,
    task_ctx: &Arc<TaskContext>,
    exec: &Arc<dyn ExecutionPlan>,
) {
    black_box(runtime.block_on(async {
        let stream = exec
            .execute(0, task_ctx.clone())
            .expect("benchmark execution should start");
        common::collect(stream)
            .await
            .expect("benchmark output should be collected")
    }));
}

fn bench_generate(
    b: &mut Bencher,
    batch: RecordBatch,
    generate_func: GenerateFunc,
    output_fields: Vec<Field>,
) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should be created");
    let task_ctx = SessionContext::new().task_ctx();
    let exec = generate_exec(batch, generate_func, output_fields);
    b.iter(|| execute(&runtime, &task_ctx, &exec));
}

#[bench]
fn explode_array_i32(b: &mut Bencher) {
    bench_generate(
        b,
        list_batch(),
        GenerateFunc::Explode,
        vec![Field::new("item", DataType::Int32, true)],
    );
}

#[bench]
fn posexplode_array_i32(b: &mut Bencher) {
    bench_generate(
        b,
        list_batch(),
        GenerateFunc::PosExplode,
        vec![
            Field::new("position", DataType::Int32, false),
            Field::new("item", DataType::Int32, true),
        ],
    );
}

#[bench]
fn explode_map_utf8_i32(b: &mut Bencher) {
    bench_generate(
        b,
        map_batch(),
        GenerateFunc::Explode,
        vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ],
    );
}

#[bench]
fn explode_array_i32_parent_null_gaps(b: &mut Bencher) {
    bench_generate(
        b,
        list_with_parent_null_gaps_batch(),
        GenerateFunc::Explode,
        vec![Field::new("item", DataType::Int32, true)],
    );
}
