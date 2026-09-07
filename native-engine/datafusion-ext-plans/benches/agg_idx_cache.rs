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
    array::{Array, ArrayRef, Int64Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use auron_memmgr::MemManager;
use datafusion::{
    common::ScalarValue,
    execution::TaskContext,
    physical_expr::expressions::{Column, Literal},
    physical_plan::{ExecutionPlan, common, test::TestMemoryExec},
    prelude::SessionContext,
};
use datafusion_ext_plans::{
    agg::{AggExecMode, AggExpr, AggFunction, AggMode, GroupingExpr, agg::create_agg},
    agg_exec::AggExec,
};
use test::{Bencher, black_box};

const NUM_BATCHES: usize = 32;
const BATCH_SIZE: usize = 8192;
const NUM_GROUPS: usize = 1024;

fn aggregate_plan(num_sums: usize) -> Arc<dyn ExecutionPlan> {
    let batches = (0..NUM_BATCHES)
        .map(|batch_idx| {
            let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(
                (0..BATCH_SIZE).map(|row| ((row * 31 + 17) % NUM_GROUPS) as i64),
            ));
            let mut columns = vec![("key".to_owned(), keys)];
            for col in 0..num_sums {
                let values: ArrayRef =
                    Arc::new(Int64Array::from_iter_values((0..BATCH_SIZE).map(|row| {
                        let key = (row * 31 + 17) % NUM_GROUPS;
                        (key + col + batch_idx + 1) as i64
                    })));
                columns.push((format!("value_{col}"), values));
            }
            RecordBatch::try_from_iter(columns).expect("benchmark batch should be valid")
        })
        .collect::<Vec<_>>();
    let schema = batches[0].schema();
    let input = Arc::new(
        TestMemoryExec::try_new(&[batches], schema.clone(), None)
            .expect("benchmark input should be valid"),
    );
    let aggs = (0..num_sums)
        .map(|col| AggExpr {
            field_name: format!("sum_{col}"),
            mode: AggMode::Partial,
            filter: None,
            agg: create_agg(
                AggFunction::Sum,
                &[Arc::new(Column::new(&format!("value_{col}"), col + 1))],
                &schema,
                DataType::Int64,
            )
            .expect("SUM expression should be valid"),
        })
        .collect::<Vec<_>>();
    let grouping = vec![GroupingExpr {
        field_name: "key".to_owned(),
        expr: Arc::new(Column::new("key", 0)),
    }];
    let partial = Arc::new(
        AggExec::try_new(
            AggExecMode::HashAgg,
            grouping.clone(),
            aggs.clone(),
            false,
            input,
        )
        .expect("partial aggregate should be valid"),
    );
    let final_aggs = aggs
        .into_iter()
        .map(|mut agg| {
            agg.mode = AggMode::Final;
            agg.agg = agg
                .agg
                .with_new_exprs(vec![Arc::new(Literal::new(ScalarValue::Null))])
                .expect("final SUM expression should be valid");
            agg
        })
        .collect();
    Arc::new(
        AggExec::try_new(AggExecMode::HashAgg, grouping, final_aggs, false, partial)
            .expect("final aggregate should be valid"),
    )
}

fn execute(
    runtime: &tokio::runtime::Runtime,
    task_ctx: &Arc<TaskContext>,
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<RecordBatch> {
    runtime.block_on(async {
        let stream = plan
            .execute(0, task_ctx.clone())
            .expect("execution should start");
        common::collect(stream)
            .await
            .expect("output should be collected")
    })
}

fn check_output(batches: &[RecordBatch], num_sums: usize) {
    let mut seen = vec![false; NUM_GROUPS];
    for batch in batches {
        assert_eq!(batch.num_columns(), num_sums + 1);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("group keys should be i64");
        assert_eq!(keys.null_count(), 0);
        for row in 0..batch.num_rows() {
            let key = keys.value(row) as usize;
            assert!(key < NUM_GROUPS && !seen[key]);
            seen[key] = true;
            for col in 0..num_sums {
                let sums = batch
                    .column(col + 1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("SUM output should be i64");
                assert!(!sums.is_null(row));
                let expected = (BATCH_SIZE / NUM_GROUPS)
                    * (NUM_BATCHES * (key + col + 1) + NUM_BATCHES * (NUM_BATCHES - 1) / 2);
                assert_eq!(sums.value(row), expected as i64);
            }
        }
    }
    assert!(seen.into_iter().all(|present| present));
}

// Time the full partial/final HashAgg pipeline, including allocation, grouping,
// accumulator updates and collecting output. Build input/plan and check results
// outside timing. Run this same benchmark on both revisions for before/after
// data.
fn bench_hash_agg(b: &mut Bencher, num_sums: usize) {
    MemManager::init(1024 * 1024 * 1024);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("benchmark runtime should be created");
    let task_ctx = SessionContext::new().task_ctx();
    let plan = aggregate_plan(num_sums);
    check_output(&execute(&runtime, &task_ctx, &plan), num_sums);
    b.iter(|| black_box(execute(&runtime, &task_ctx, &plan)));
}

#[bench]
fn hash_agg_1_sum(b: &mut Bencher) {
    bench_hash_agg(b, 1);
}

#[bench]
fn hash_agg_8_sums(b: &mut Bencher) {
    bench_hash_agg(b, 8);
}

#[bench]
fn hash_agg_4_sums(b: &mut Bencher) {
    bench_hash_agg(b, 4);
}
