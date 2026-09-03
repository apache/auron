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
    array::{ArrayRef, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    execution::TaskContext,
    physical_expr::{PhysicalSortExpr, expressions::Column},
    physical_plan::{ExecutionPlan, common, test::TestMemoryExec},
    prelude::SessionContext,
};
use datafusion_ext_plans::{
    window::{WindowExpr, WindowFunction, WindowRankType},
    window_exec::WindowExec,
};
use test::{Bencher, black_box};

fn create_batch(
    num_partitions: usize,
    rows_per_partition: usize,
    peer_group_size: usize,
) -> RecordBatch {
    let num_rows = num_partitions * rows_per_partition;
    let partitions = (0..num_rows)
        .map(|row| (row / rows_per_partition) as i32)
        .collect::<Vec<_>>();
    let order_values = (0..num_rows)
        .map(|row| ((row % rows_per_partition) / peer_group_size) as i32)
        .collect::<Vec<_>>();
    let payloads = (0..num_rows as i32).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("partition", DataType::Int32, false),
        Field::new("order", DataType::Int32, false),
        Field::new("payload", DataType::Int32, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(partitions)),
        Arc::new(Int32Array::from(order_values)),
        Arc::new(Int32Array::from(payloads)),
    ];
    RecordBatch::try_new(schema, columns).expect("benchmark batch should be valid")
}

fn create_window_group_limit(
    rank_type: WindowRankType,
    num_partitions: usize,
    rows_per_partition: usize,
    peer_group_size: usize,
    group_limit: usize,
) -> Arc<dyn ExecutionPlan> {
    let batch = create_batch(num_partitions, rows_per_partition, peer_group_size);
    let input = Arc::new(
        TestMemoryExec::try_new(&[vec![batch.clone()]], batch.schema(), None)
            .expect("benchmark input should be valid"),
    );
    let window_exprs = vec![WindowExpr::new(
        WindowFunction::RankLike(rank_type),
        vec![],
        Arc::new(Field::new("rank", DataType::Int32, false)),
        DataType::Int32,
    )];
    Arc::new(
        WindowExec::try_new(
            input,
            window_exprs,
            vec![Arc::new(Column::new("partition", 0))],
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("order", 1)),
                options: Default::default(),
            }],
            Some(group_limit),
            false,
        )
        .expect("benchmark WindowGroupLimit should be valid"),
    )
}

fn execute(
    runtime: &tokio::runtime::Runtime,
    task_ctx: &Arc<TaskContext>,
    exec: &Arc<dyn ExecutionPlan>,
) -> Vec<RecordBatch> {
    runtime
        .block_on(async {
            let stream = exec
                .execute(0, task_ctx.clone())
                .expect("benchmark execution should start");
            common::collect(stream).await
        })
        .expect("benchmark output should be collected")
}

fn expected_rows(
    rank_type: WindowRankType,
    num_partitions: usize,
    rows_per_partition: usize,
    peer_group_size: usize,
    group_limit: usize,
) -> usize {
    let rows_per_partition = match rank_type {
        WindowRankType::RowNumber => group_limit.min(rows_per_partition),
        WindowRankType::Rank => group_limit
            .div_ceil(peer_group_size)
            .saturating_mul(peer_group_size)
            .min(rows_per_partition),
        WindowRankType::DenseRank => group_limit
            .saturating_mul(peer_group_size)
            .min(rows_per_partition),
    };
    num_partitions * rows_per_partition
}

fn bench_window_group_limit(
    b: &mut Bencher,
    rank_type: WindowRankType,
    num_partitions: usize,
    rows_per_partition: usize,
    peer_group_size: usize,
    group_limit: usize,
) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should be created");
    let task_ctx = SessionContext::new().task_ctx();
    let exec = create_window_group_limit(
        rank_type,
        num_partitions,
        rows_per_partition,
        peer_group_size,
        group_limit,
    );
    let output = execute(&runtime, &task_ctx, &exec);
    assert_eq!(
        output.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected_rows(
            rank_type,
            num_partitions,
            rows_per_partition,
            peer_group_size,
            group_limit,
        )
    );

    b.iter(|| black_box(execute(&runtime, &task_ctx, &exec)));
}

#[bench]
fn window_group_limit_row_number_slice(b: &mut Bencher) {
    bench_window_group_limit(b, WindowRankType::RowNumber, 1, 100_000, 10, 10);
}

#[bench]
fn window_group_limit_rank_slice(b: &mut Bencher) {
    bench_window_group_limit(b, WindowRankType::Rank, 1, 100_000, 10, 10);
}

#[bench]
fn window_group_limit_dense_rank_slice(b: &mut Bencher) {
    bench_window_group_limit(b, WindowRankType::DenseRank, 1, 100_000, 10, 10);
}

#[bench]
fn window_group_limit_row/_number(b: &mut Bencher) {
    bench_window_group_limit(b, WindowRankType::RowNumber, 100, 1_000, 10, 10);
}

#[bench]
fn window_group_limit_rank(b: &mut Bencher) {
    bench_window_group_limit(b, WindowRankType::Rank, 100, 1_000, 10, 10);
}

#[bench]
fn window_group_limit_dense_rank(b: &mut Bencher) {
    bench_window_group_limit(b, WindowRankType::DenseRank, 100, 1_000, 10, 10);
}
