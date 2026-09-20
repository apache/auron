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

use std::{pin::Pin, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, Int32Array, RecordBatch},
    compute::SortOptions,
    datatypes::{DataType, Field, Schema},
    row::{RowConverter, SortField},
};
use datafusion::{
    common::Result,
    execution::TaskContext,
    logical_expr::Operator,
    physical_expr::{
        PhysicalExprRef, PhysicalSortExpr,
        expressions::{BinaryExpr, Column},
    },
    physical_plan::metrics::{ExecutionPlanMetricsSet, Time},
};

use crate::{
    common::{
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
        key_rows_output::{RecordBatchWithKeyRows, RecordBatchWithKeyRowsStreamAdapter},
    },
    cur_forward,
    joins::{
        JoinParams, JoinProjection,
        join_filter::JoinFilter,
        join_utils::JoinType,
        smj::{
            full_join::{LeftOuterJoiner, RightOuterJoiner},
            semi_join::{LeftAntiJoiner, RightAntiJoiner},
        },
        stream_cursor::StreamCursor,
    },
    sort_merge_join_exec::Joiner,
};

fn batch(prefix: &str, keys: &[i32], values: &[i32]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}k"), DataType::Int32, true),
            Field::new(format!("{prefix}v"), DataType::Int32, true),
        ])),
        vec![
            Arc::new(Int32Array::from(keys.to_vec())) as ArrayRef,
            Arc::new(Int32Array::from(values.to_vec())) as ArrayRef,
        ],
    )
    .unwrap()
}

fn cursor(batch: RecordBatch, input_batch_size: usize, read: &[usize]) -> StreamCursor {
    let schema = batch.schema();
    let converter = Arc::new(parking_lot::Mutex::new(
        RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap(),
    ));
    let batches = (0..batch.num_rows())
        .step_by(input_batch_size)
        .map(|start| {
            let batch = batch.slice(start, input_batch_size.min(batch.num_rows() - start));
            let rows = Arc::new(
                converter
                    .lock()
                    .convert_columns(&[batch.column(0).clone()])
                    .unwrap(),
            );
            Ok(RecordBatchWithKeyRows::new(
                batch.project(read).unwrap(),
                rows,
                converter.clone(),
            ))
        })
        .collect::<Vec<_>>();
    let stream = RecordBatchWithKeyRowsStreamAdapter::new(
        futures::stream::iter(batches),
        Arc::new(schema.project(read).unwrap()),
        vec![PhysicalSortExpr::new(
            Arc::new(Column::new(schema.field(0).name(), 0)),
            SortOptions::default(),
        )],
    );
    StreamCursor::try_new(Box::pin(stream), Time::default(), &[DataType::Int32]).unwrap()
}

async fn run(
    own: RecordBatch,
    other: RecordBatch,
    join_type: JoinType,
    batch_size: usize,
    input_batch_size: usize,
) -> Result<Vec<RecordBatch>> {
    let is_right = matches!(join_type, JoinType::Right | JoinType::RightAnti);
    let is_anti = matches!(join_type, JoinType::LeftAnti | JoinType::RightAnti);
    let (left, right) = if is_right { (other, own) } else { (own, other) };
    let (lschema, rschema) = (left.schema(), right.schema());
    let fields = match (is_anti, is_right) {
        (true, false) => lschema.fields().to_vec(),
        (true, true) => rschema.fields().to_vec(),
        (false, _) => [lschema.fields().to_vec(), rschema.fields().to_vec()].concat(),
    };
    let output_schema = Arc::new(Schema::new(fields));
    let (own_value, other_value) = if is_right { (3, 1) } else { (1, 3) };
    let expr = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("own_value", own_value)),
        Operator::Gt,
        Arc::new(Column::new("other_value", other_value)),
    )) as PhysicalExprRef;
    let projection = JoinProjection::try_new_with_filter(
        join_type,
        &output_schema,
        &lschema,
        &rschema,
        &(0..output_schema.fields().len()).collect::<Vec<_>>(),
        Some(&expr),
    )?;
    let filter = JoinFilter::try_new_remapped(
        expr,
        &lschema,
        &rschema,
        &projection.left_read,
        &projection.right_read,
        Time::default(),
    )?;
    let lread = projection.left_read.clone();
    let rread = projection.right_read.clone();
    let params = JoinParams {
        join_type,
        left_schema: lschema.clone(),
        right_schema: rschema.clone(),
        output_schema: output_schema.clone(),
        left_keys: vec![Arc::new(Column::new(lschema.field(0).name(), 0))],
        right_keys: vec![Arc::new(Column::new(rschema.field(0).name(), 0))],
        key_data_types: vec![DataType::Int32],
        sort_options: vec![SortOptions::default()],
        projection,
        join_filter: None,
        residual_filter: Some(Arc::new(filter)),
        batch_size,
        is_null_aware_anti_join: false,
        output_time: Time::default(),
    };
    let context = ExecutionContext::new(
        Arc::new(TaskContext::default()),
        0,
        output_schema,
        &ExecutionPlanMetricsSet::new(),
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let sender = WrappedRecordBatchSender::new(context, tx);
    let collector = tokio::spawn(async move {
        let mut batches = vec![];
        while let Some(batch) = rx.recv().await {
            batches.push(batch.unwrap());
        }
        batches
    });
    let mut joiner: Pin<Box<dyn Joiner>> = match join_type {
        JoinType::Left => Box::pin(LeftOuterJoiner::new(params, sender)),
        JoinType::Right => Box::pin(RightOuterJoiner::new(params, sender)),
        JoinType::LeftAnti => Box::pin(LeftAntiJoiner::new(params, sender)),
        JoinType::RightAnti => Box::pin(RightAntiJoiner::new(params, sender)),
        _ => unreachable!(),
    };
    let (mut lcur, mut rcur) = (
        cursor(left, input_batch_size, &lread),
        cursor(right, input_batch_size, &rread),
    );
    cur_forward!(lcur);
    cur_forward!(rcur);
    joiner.as_mut().join(&mut lcur, &mut rcur).await?;
    drop(joiner);
    Ok(collector.await.unwrap())
}

#[tokio::test]
async fn test_filtered_outer_and_anti_preserve_key_order() -> Result<()> {
    for join_type in [
        JoinType::Left,
        JoinType::Right,
        JoinType::LeftAnti,
        JoinType::RightAnti,
    ] {
        for batch_size in [1, 2, 3, 1024] {
            for input_batch_size in [1, 2, 1024] {
                let batches = run(
                    batch("own", &[1, 2, 3, 4, 5], &[0, 20, 0, 0, 20]),
                    batch("other", &[1, 2, 4, 5], &[10, 10, 10, 10]),
                    join_type,
                    batch_size,
                    input_batch_size,
                )
                .await?;
                let key_column = if join_type == JoinType::Right { 2 } else { 0 };
                let keys = batches
                    .iter()
                    .flat_map(|batch| {
                        batch
                            .column(key_column)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap()
                            .values()
                            .to_vec()
                    })
                    .collect::<Vec<_>>();
                let expected = if matches!(join_type, JoinType::LeftAnti | JoinType::RightAnti) {
                    vec![1, 3, 4]
                } else {
                    vec![1, 2, 3, 4, 5]
                };
                assert_eq!(
                    keys, expected,
                    "{join_type:?}, batch_size={batch_size}, input_batch_size={input_batch_size}"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_outer_order_survives_mid_group_output_flush() -> Result<()> {
    for join_type in [JoinType::Left, JoinType::Right] {
        let mut own_keys = vec![1];
        own_keys.extend(vec![2; 20]);
        own_keys.push(3);
        let mut own_values = vec![0];
        own_values.extend(vec![20; 20]);
        own_values.push(0);
        let mut other_keys = vec![1];
        other_keys.extend(vec![2; 7]);
        let other_values = vec![10; other_keys.len()];
        for input_batch_size in [3, 1024] {
            let batches = run(
                batch("own", &own_keys, &own_values),
                batch("other", &other_keys, &other_values),
                join_type,
                4,
                input_batch_size,
            )
            .await?;
            let key_column = if join_type == JoinType::Right { 2 } else { 0 };
            let other_column = if join_type == JoinType::Right { 0 } else { 2 };
            let keys = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(key_column)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect::<Vec<_>>();
            let mut expected = vec![1];
            expected.extend(vec![2; 140]);
            expected.push(3);
            assert_eq!(keys, expected);
            assert_eq!(
                batches
                    .iter()
                    .map(|batch| batch.column(other_column).null_count())
                    .sum::<usize>(),
                2
            );
            assert!(batches.len() > 1);
        }
    }
    Ok(())
}

#[test]
fn test_product_blocks_are_lazy_for_large_groups() {
    use crate::joins::{IdxRange, product_blocks};

    // Eager generation would attempt to allocate almost a quadrillion block
    // descriptors before producing the first candidate batch.
    let ranges = [IdxRange {
        batch_idx: 1,
        start: 0,
        end: 1_000_000_000,
    }];
    let blocks = product_blocks(&ranges, &ranges, 1024)
        .take(3)
        .collect::<Vec<_>>();
    assert_eq!(blocks.len(), 3);
    for (i, block) in blocks.iter().enumerate() {
        assert_eq!(block.loffset, 0);
        assert_eq!(block.lrange.len(), 1);
        assert_eq!(block.roffset, i * 1024);
        assert_eq!(block.rrange.len(), 1024);
    }
    assert_eq!(product_blocks(&[], &ranges, 0).count(), 0);
    assert_eq!(product_blocks(&ranges[..1], &[], 0).count(), 0);
}

#[tokio::test]
async fn test_compact_residual_filter_all_join_types_and_projections() -> Result<()> {
    use arrow::array::BooleanArray;
    use datafusion::{
        common::JoinSide,
        physical_plan::{
            ExecutionPlan, common::collect, joins::utils::build_join_schema, test::TestMemoryExec,
        },
        prelude::SessionContext,
    };

    use crate::{
        common::column_pruning::ExecuteWithColumnPruning,
        joins::{ColumnIndex, JoinFilter as CompactJoinFilter},
        sort_merge_join_exec::SortMergeJoinExec,
    };

    auron_memmgr::MemManager::init(1_000_000);
    let lkeys = vec![None, Some(1), Some(1), Some(2), Some(3)];
    let lvalues = vec![Some(5), Some(1), Some(20), None, Some(30)];
    let rkeys = vec![None, Some(1), Some(1), Some(2), Some(4)];
    let rvalues = vec![Some(5), Some(10), None, Some(0), Some(40)];
    let make_batch = |prefix: &str, keys: Vec<Option<i32>>, values: Vec<Option<i32>>| {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new(format!("{prefix}k"), DataType::Int32, true),
                Field::new(format!("{prefix}v"), DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(keys)) as ArrayRef,
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ],
        )
        .unwrap()
    };
    let left_batch = make_batch("l", lkeys.clone(), lvalues.clone());
    let right_batch = make_batch("r", rkeys.clone(), rvalues.clone());
    let mut matches = vec![];
    let mut lmatched = vec![false; lkeys.len()];
    let mut rmatched = vec![false; rkeys.len()];
    for l in 0..lkeys.len() {
        for r in 0..rkeys.len() {
            if lkeys[l].is_some()
                && lkeys[l] == rkeys[r]
                && lvalues[l]
                    .zip(rvalues[r])
                    .is_some_and(|(lvalue, rvalue)| lvalue > rvalue)
            {
                matches.push((l, r));
                lmatched[l] = true;
                rmatched[r] = true;
            }
        }
    }
    let key =
        |value: Option<i32>| value.map_or_else(|| "null".to_owned(), |value| value.to_string());
    for join_type in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::RightSemi,
        JoinType::LeftAnti,
        JoinType::RightAnti,
        JoinType::Existence,
    ] {
        let mut expected = vec![];
        match join_type {
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                expected.extend(
                    matches
                        .iter()
                        .map(|&(l, r)| vec![key(lkeys[l]), key(rkeys[r])]),
                );
                if matches!(join_type, JoinType::Left | JoinType::Full) {
                    expected.extend(
                        lkeys
                            .iter()
                            .zip(&lmatched)
                            .filter(|(_, matched)| !**matched)
                            .map(|(&value, _)| vec![key(value), key(None)]),
                    );
                }
                if matches!(join_type, JoinType::Right | JoinType::Full) {
                    expected.extend(
                        rkeys
                            .iter()
                            .zip(&rmatched)
                            .filter(|(_, matched)| !**matched)
                            .map(|(&value, _)| vec![key(None), key(value)]),
                    );
                }
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                expected.extend(
                    lkeys
                        .iter()
                        .zip(&lmatched)
                        .filter(|(_, matched)| **matched == (join_type == JoinType::LeftSemi))
                        .map(|(&value, _)| vec![key(value)]),
                );
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                expected.extend(
                    rkeys
                        .iter()
                        .zip(&rmatched)
                        .filter(|(_, matched)| **matched == (join_type == JoinType::RightSemi))
                        .map(|(&value, _)| vec![key(value)]),
                );
            }
            JoinType::Existence => {
                expected.extend(
                    lkeys
                        .iter()
                        .zip(&lmatched)
                        .map(|(&value, matched)| vec![key(value), matched.to_string()]),
                );
            }
        }
        for empty_projection in [false, true] {
            for input_batch_size in [2, 5] {
                let input = |batch: &RecordBatch| -> Arc<dyn ExecutionPlan> {
                    let batches = (0..batch.num_rows())
                        .step_by(input_batch_size)
                        .map(|start| {
                            batch.slice(start, input_batch_size.min(batch.num_rows() - start))
                        })
                        .collect::<Vec<_>>();
                    Arc::new(TestMemoryExec::try_new(&[batches], batch.schema(), None).unwrap())
                };
                let (left, right) = (input(&left_batch), input(&right_batch));
                let schema = if join_type == JoinType::Existence {
                    Arc::new(Schema::new(
                        [
                            left.schema().fields().to_vec(),
                            vec![Arc::new(Field::new("exists", DataType::Boolean, false))],
                        ]
                        .concat(),
                    ))
                } else {
                    Arc::new(
                        build_join_schema(&left.schema(), &right.schema(), &join_type.try_into()?)
                            .0,
                    )
                };
                let projection = if empty_projection {
                    vec![]
                } else if schema.fields().len() > 2 {
                    vec![0, 2]
                } else {
                    vec![0]
                };
                // The compact payload deliberately lists right before left and
                // contains only values; neither value is in the output projection.
                let filter = CompactJoinFilter {
                    expression: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("lv", 1)),
                        Operator::Gt,
                        Arc::new(Column::new("rv", 0)),
                    )),
                    column_indices: vec![
                        ColumnIndex {
                            side: JoinSide::Right,
                            index: 1,
                        },
                        ColumnIndex {
                            side: JoinSide::Left,
                            index: 1,
                        },
                    ],
                    schema: Arc::new(Schema::new(vec![
                        Field::new("rv", DataType::Int32, true),
                        Field::new("lv", DataType::Int32, true),
                    ])),
                };
                let join = SortMergeJoinExec::try_new(
                    schema,
                    left,
                    right,
                    vec![(
                        Arc::new(Column::new("lk", 0)),
                        Arc::new(Column::new("rk", 0)),
                    )],
                    join_type,
                    Some(filter),
                    vec![SortOptions::default()],
                )?;
                let batches = collect(join.execute_projected(
                    0,
                    SessionContext::new().task_ctx(),
                    &projection,
                )?)
                .await?;
                let mut actual = batches
                    .iter()
                    .flat_map(|batch| {
                        (0..batch.num_rows()).map(|row| {
                            batch
                                .columns()
                                .iter()
                                .map(|column| {
                                    if column.is_null(row) {
                                        "null".to_owned()
                                    } else if let Some(column) =
                                        column.as_any().downcast_ref::<Int32Array>()
                                    {
                                        column.value(row).to_string()
                                    } else {
                                        column
                                            .as_any()
                                            .downcast_ref::<BooleanArray>()
                                            .unwrap()
                                            .value(row)
                                            .to_string()
                                    }
                                })
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect::<Vec<_>>();
                let mut expected = if empty_projection {
                    vec![vec![]; expected.len()]
                } else {
                    expected.clone()
                };
                actual.sort();
                expected.sort();
                assert_eq!(
                    actual, expected,
                    "join_type={join_type:?}, empty_projection={empty_projection}, input_batch_size={input_batch_size}"
                );
            }
        }
    }
    Ok(())
}
