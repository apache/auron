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

const NUM_PARTITIONS: usize = 100;
const ROWS_PER_PARTITION: usize = 1_000;
const PEER_GROUP_SIZE: usize = 10;
const GROUP_LIMIT: usize = 10;

fn create_batch() -> RecordBatch {
    let num_rows = NUM_PARTITIONS * ROWS_PER_PARTITION;
    let partitions = (0..num_rows)
        .map(|row| (row / ROWS_PER_PARTITION) as i32)
        .collect::<Vec<_>>();
    let order_values = (0..num_rows)
        .map(|row| ((row % ROWS_PER_PARTITION) / PEER_GROUP_SIZE) as i32)
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

fn create_window_group_limit(rank_type: WindowRankType) -> Arc<dyn ExecutionPlan> {
    let batch = create_batch();
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
            Some(GROUP_LIMIT),
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

fn bench_window_group_limit(b: &mut Bencher, rank_type: WindowRankType) {
    let runtime = tokio::runtime::Runtime::new().expect("benchmark runtime should be created");
    let task_ctx = SessionContext::new().task_ctx();
    let exec = create_window_group_limit(rank_type);
    let expected_rows = match rank_type {
        WindowRankType::RowNumber | WindowRankType::Rank => NUM_PARTITIONS * GROUP_LIMIT,
        WindowRankType::DenseRank => NUM_PARTITIONS * GROUP_LIMIT * PEER_GROUP_SIZE,
    };
    let output = execute(&runtime, &task_ctx, &exec);
    assert_eq!(
        output.iter().map(RecordBatch::num_rows).sum::<usize>(),
        expected_rows
    );

    b.iter(|| black_box(execute(&runtime, &task_ctx, &exec)));
}

macro_rules! benchmark {
    ($name:ident, $rank_type:expr) => {
        #[bench]
        fn $name(b: &mut Bencher) {
            bench_window_group_limit(b, $rank_type);
        }
    };
}

benchmark!(window_group_limit_row_number, WindowRankType::RowNumber);
benchmark!(window_group_limit_rank, WindowRankType::Rank);
benchmark!(window_group_limit_dense_rank, WindowRankType::DenseRank);
