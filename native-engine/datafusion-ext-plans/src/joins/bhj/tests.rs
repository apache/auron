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
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    common::Result,
    logical_expr::{Volatility, create_udf},
    physical_expr::{PhysicalExprRef, ScalarFunctionExpr, expressions::Column},
    physical_plan::{
        ColumnarValue,
        metrics::{ExecutionPlanMetricsSet, Time},
    },
    prelude::SessionContext,
};

use crate::{
    broadcast_join_exec::Joiner,
    common::execution_context::{ExecutionContext, WrappedRecordBatchSender},
    joins::{
        JoinParams, JoinProjection,
        bhj::{full_join::*, semi_join::*},
        join_filter::JoinFilter,
        join_hash_map::JoinHashMap,
        join_utils::JoinType,
    },
};

fn batch(prefix: &str, keys: Vec<i32>, values: Vec<i32>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(format!("{prefix}k"), DataType::Int32, true),
        Field::new(format!("{prefix}v"), DataType::Int32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(values)),
        ],
    )
    .expect("valid test batch")
}

async fn run_bounded(left_probed: bool, kind: usize, limit: usize, threshold: i32) -> Result<()> {
    let built = batch(
        if left_probed { "r" } else { "l" },
        vec![1; 17],
        (0..17).collect(),
    );
    let probed = batch(
        if left_probed { "l" } else { "r" },
        vec![1, 2],
        vec![threshold, 100],
    );
    let (ls, rs) = if left_probed {
        (probed.schema(), built.schema())
    } else {
        (built.schema(), probed.schema())
    };
    let jt = match (left_probed, kind) {
        (_, 0) => JoinType::Inner,
        (true, 1) => JoinType::Left,
        (false, 1) => JoinType::Right,
        (_, 2) => JoinType::Full,
        (true, 3) => JoinType::LeftSemi,
        (false, 3) => JoinType::RightSemi,
        (true, 4) => JoinType::LeftAnti,
        (false, 4) => JoinType::RightAnti,
        (_, 5 | 8) => JoinType::Existence,
        (true, 6) => JoinType::RightSemi,
        (false, 6) => JoinType::LeftSemi,
        (true, 7) => JoinType::RightAnti,
        (false, 7) => JoinType::LeftAnti,
        _ => unreachable!(),
    };
    let schema = Arc::new(Schema::new(match kind {
        0..=2 => [ls.fields().to_vec(), rs.fields().to_vec()].concat(),
        3 | 4 => probed.schema().fields().to_vec(),
        6 | 7 => built.schema().fields().to_vec(),
        5 | 8 => [
            ls.fields().to_vec(),
            vec![Arc::new(Field::new("exists", DataType::Boolean, false))],
        ]
        .concat(),
        _ => unreachable!(),
    }));
    let eval_rows = Arc::new(AtomicUsize::new(0));
    let observed = eval_rows.clone();
    let udf = create_udf(
        "bounded_condition",
        vec![DataType::Int32, DataType::Int32],
        DataType::Boolean,
        Volatility::Volatile,
        Arc::new(move |args| {
            let cols = ColumnarValue::values_to_arrays(args)?;
            observed.fetch_max(cols[0].len(), Ordering::Relaxed);
            assert!(
                cols[0].len() <= limit,
                "candidate evaluation exceeded batch size"
            );
            let p = cols[0]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("expected test array type");
            let b = cols[1]
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("expected test array type");
            Ok(ColumnarValue::Array(Arc::new(BooleanArray::from_iter(
                p.iter()
                    .zip(b.iter())
                    .map(|(p, b)| p.zip(b).map(|(p, b)| p < b)),
            ))))
        }),
    );
    let filter_schema = Arc::new(Schema::new(
        [ls.fields().to_vec(), rs.fields().to_vec()].concat(),
    ));
    let (pi, bi) = if left_probed { (1, 3) } else { (3, 1) };
    let expr = Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(udf),
        vec![
            Arc::new(Column::new(filter_schema.field(pi).name(), pi)),
            Arc::new(Column::new(filter_schema.field(bi).name(), bi)),
        ],
        &filter_schema,
    )?);
    let params = JoinParams {
        join_type: jt,
        left_schema: ls.clone(),
        right_schema: rs.clone(),
        output_schema: schema.clone(),
        left_keys: vec![Arc::new(Column::new("lk", 0))],
        right_keys: vec![Arc::new(Column::new("rk", 0))],
        key_data_types: vec![DataType::Int32],
        sort_options: vec![Default::default()],
        projection: JoinProjection::try_new(
            jt,
            &schema,
            &ls,
            &rs,
            &(0..schema.fields().len()).collect::<Vec<_>>(),
        )?,
        join_filter: None,
        residual_filter: Some(Arc::new(JoinFilter::try_new(
            expr,
            filter_schema,
            2,
            Time::new(),
        )?)),
        batch_size: limit,
        is_null_aware_anti_join: false,
        output_time: Time::new(),
    };
    let key = Arc::new(Column::new(built.schema().field(0).name(), 0)) as PhysicalExprRef;
    let map = Arc::new(JoinHashMap::create_from_data_batch(built, &[key])?);
    let ctx = ExecutionContext::new(
        SessionContext::new().task_ctx(),
        0,
        schema,
        &ExecutionPlanMetricsSet::new(),
    );
    let (tx, mut rx) = tokio::sync::mpsc::channel(128);
    let sender = WrappedRecordBatchSender::new(ctx, tx);
    let mut joiner: Pin<Box<dyn Joiner>> = match (left_probed, kind) {
        (true, 0) => Box::pin(LProbedInnerJoiner::new(params, map, sender)),
        (false, 0) => Box::pin(RProbedInnerJoiner::new(params, map, sender)),
        (true, 1) => Box::pin(LProbedLeftJoiner::new(params, map, sender)),
        (false, 1) => Box::pin(RProbedRightJoiner::new(params, map, sender)),
        (true, 2) => Box::pin(LProbedFullOuterJoiner::new(params, map, sender)),
        (false, 2) => Box::pin(RProbedFullOuterJoiner::new(params, map, sender)),
        (true, 3) => Box::pin(LProbedLeftSemiJoiner::new(params, map, sender)),
        (false, 3) => Box::pin(RProbedRightSemiJoiner::new(params, map, sender)),
        (true, 4) => Box::pin(LProbedLeftAntiJoiner::new(params, map, sender)),
        (false, 4) => Box::pin(RProbedRightAntiJoiner::new(params, map, sender)),
        (true, 5) => Box::pin(LProbedExistenceJoiner::new(params, map, sender)),
        (true, 6) => Box::pin(LProbedRightSemiJoiner::new(params, map, sender)),
        (false, 6) => Box::pin(RProbedLeftSemiJoiner::new(params, map, sender)),
        (true, 7) => Box::pin(LProbedRightAntiJoiner::new(params, map, sender)),
        (false, 7) => Box::pin(RProbedLeftAntiJoiner::new(params, map, sender)),
        (false, 8) => Box::pin(RProbedExistenceJoiner::new(params, map, sender)),
        _ => unreachable!(),
    };
    let t = Time::new();
    joiner.as_mut().join(probed, &t, &t, &t, &t).await?;
    joiner.as_mut().finish(&t).await?;
    drop(joiner);
    let mut rows = 0;
    let mut exists = vec![];
    while let Some(b) = rx.recv().await {
        let b = b?;
        rows += b.num_rows();
        if kind == 5 {
            exists.extend(
                b.column(2)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("expected test array type")
                    .iter(),
            );
        }
    }
    let matches = (0..17).filter(|&b| threshold < b).count();
    let expected = match kind {
        0 => matches,
        1 => matches.max(1) + 1,
        2 => matches.max(1) + 1 + 17 - matches,
        3 => usize::from(matches > 0),
        4 => 1 + usize::from(matches == 0),
        5 => 2,
        6 => matches,
        7 => 17 - matches,
        8 => 17,
        _ => unreachable!(),
    };
    assert_eq!(
        rows, expected,
        "left_probed={left_probed} kind={kind} limit={limit} threshold={threshold}"
    );
    if kind == 5 {
        assert_eq!(exists, vec![Some(matches > 0), Some(false)]);
    }
    assert!(eval_rows.load(Ordering::Relaxed) > 0);
    Ok(())
}

#[tokio::test]
async fn duplicate_key_candidates_stay_bounded() -> Result<()> {
    for left_probed in [true, false] {
        for kind in [0, 1, 2, 3, 4, 6, 7] {
            for limit in [1, 4, 16] {
                for threshold in [-1, 8, 15, 100] {
                    run_bounded(left_probed, kind, limit, threshold).await?;
                }
            }
        }
    }
    for threshold in [-1, 15, 100] {
        run_bounded(true, 5, 4, threshold).await?;
        run_bounded(false, 8, 4, threshold).await?;
    }
    Ok(())
}

#[tokio::test]
async fn residual_hash_join_operator_and_spill_parity() -> Result<()> {
    use arrow::array::{ArrayRef, new_null_array};
    use auron_memmgr::MemManager;
    use datafusion::{
        common::{JoinSide, ScalarValue},
        logical_expr::Operator,
        physical_expr::expressions::BinaryExpr,
        physical_plan::{
            ExecutionPlan, common::collect, joins::utils::build_join_schema, test::TestMemoryExec,
        },
    };

    use crate::{
        broadcast_join_build_hash_map_exec::BroadcastJoinBuildHashMapExec,
        broadcast_join_exec::BroadcastJoinExec,
        common::column_pruning::ExecuteWithColumnPruning,
        joins::{ColumnIndex, JoinFilter as CompactFilter, join_hash_map::join_hash_map_schema},
    };
    MemManager::init(1000000);
    let left_rows = [
        (None, Some(10)),
        (Some(1), None),
        (Some(1), Some(20)),
        (Some(2), Some(5)),
        (Some(4), Some(40)),
    ];
    let right_rows = [
        (None, Some(9)),
        (Some(1), Some(15)),
        (Some(1), Some(25)),
        (Some(3), Some(30)),
        (Some(4), Some(0)),
    ];
    let make_batch = |prefix: &str, rows: &[(Option<i32>, Option<i32>)]| {
        let schema = Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}k"), DataType::Int32, true),
            Field::new(format!("{prefix}v"), DataType::Int32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.0))) as ArrayRef,
                Arc::new(Int32Array::from_iter(rows.iter().map(|r| r.1))),
            ],
        )
        .expect("valid test batch")
    };
    let left_batch = make_batch("l", &left_rows);
    let right_batch = make_batch("r", &right_rows);
    let memory = |batch: RecordBatch| -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TestMemoryExec::try_new(
            &[vec![batch.clone()]],
            batch.schema(),
            None,
        )?))
    };
    let scalar = |v: Option<i32>| ScalarValue::Int32(v).to_string();
    let null_row = vec![scalar(None); 2];
    let row_values = |r: &(Option<i32>, Option<i32>)| vec![scalar(r.0), scalar(r.1)];
    for jt in [
        JoinType::Inner,
        JoinType::Left,
        JoinType::Right,
        JoinType::Full,
        JoinType::LeftSemi,
        JoinType::LeftAnti,
        JoinType::RightSemi,
        JoinType::RightAnti,
        JoinType::Existence,
    ] {
        let schema = if jt == JoinType::Existence {
            Arc::new(Schema::new(
                [
                    left_batch.schema().fields().to_vec(),
                    vec![Arc::new(Field::new("exists", DataType::Boolean, false))],
                ]
                .concat(),
            ))
        } else {
            Arc::new(
                build_join_schema(&left_batch.schema(), &right_batch.schema(), &jt.try_into()?).0,
            )
        };
        let mut expected = vec![];
        let mut right_joined = vec![false; right_rows.len()];
        for l in &left_rows {
            let mut matched = false;
            for (ri, r) in right_rows.iter().enumerate() {
                if l.0.is_some() && l.0 == r.0 && l.1.zip(r.1).is_some_and(|(l, r)| l < r) {
                    matched = true;
                    right_joined[ri] = true;
                    if matches!(
                        jt,
                        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                    ) {
                        expected.push([row_values(l), row_values(r)].concat());
                    }
                }
            }
            match jt {
                JoinType::Left | JoinType::Full if !matched => {
                    expected.push([row_values(l), null_row.clone()].concat())
                }
                JoinType::LeftSemi if matched => expected.push(row_values(l)),
                JoinType::LeftAnti if !matched => expected.push(row_values(l)),
                JoinType::Existence => expected.push(
                    [
                        row_values(l),
                        vec![ScalarValue::Boolean(Some(matched)).to_string()],
                    ]
                    .concat(),
                ),
                _ => {}
            }
        }
        for (r, matched) in right_rows.iter().zip(right_joined) {
            match jt {
                JoinType::Right | JoinType::Full if !matched => {
                    expected.push([null_row.clone(), row_values(r)].concat())
                }
                JoinType::RightSemi if matched => expected.push(row_values(r)),
                JoinType::RightAnti if !matched => expected.push(row_values(r)),
                _ => {}
            }
        }
        for build_left in [true, false] {
            for mode in 0..3 {
                let key = Arc::new(Column::new(if build_left { "lk" } else { "rk" }, 0))
                    as PhysicalExprRef;
                let build_batch = if build_left {
                    left_batch.clone()
                } else {
                    right_batch.clone()
                };
                let built: Arc<dyn ExecutionPlan> = match mode {
                    0 => memory(build_batch)?,
                    1 => Arc::new(BroadcastJoinBuildHashMapExec::new(
                        memory(build_batch)?,
                        vec![key],
                    )),
                    2 => {
                        let schema = join_hash_map_schema(&build_batch.schema());
                        let cols = [
                            build_batch.columns().to_vec(),
                            vec![new_null_array(&DataType::Binary, build_batch.num_rows())],
                        ]
                        .concat();
                        memory(RecordBatch::try_new(schema, cols)?)?
                    }
                    _ => unreachable!(),
                };
                let (left, right) = if build_left {
                    (built, memory(right_batch.clone())?)
                } else {
                    (memory(left_batch.clone())?, built)
                };
                let filter = CompactFilter {
                    expression: Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("lv", 1)),
                        Operator::Lt,
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
                        right_batch.schema().field(1).clone(),
                        left_batch.schema().field(1).clone(),
                    ])),
                };
                let join = BroadcastJoinExec::try_new(
                    schema.clone(),
                    left,
                    right,
                    vec![(
                        Arc::new(Column::new("lk", 0)),
                        Arc::new(Column::new("rk", 0)),
                    )],
                    jt,
                    if build_left {
                        JoinSide::Left
                    } else {
                        JoinSide::Right
                    },
                    mode != 0,
                    None,
                    false,
                    Some(filter),
                )?;
                for projection in [
                    (0..schema.fields().len()).collect::<Vec<_>>(),
                    vec![schema.fields().len() - 1],
                    vec![],
                ] {
                    let output = collect(join.execute_projected(
                        0,
                        SessionContext::new().task_ctx(),
                        &projection,
                    )?)
                    .await?;
                    let mut actual = vec![];
                    for b in output {
                        assert_eq!(b.schema().as_ref(), &schema.project(&projection)?);
                        for i in 0..b.num_rows() {
                            actual.push(
                                b.columns()
                                    .iter()
                                    .map(|c| {
                                        ScalarValue::try_from_array(c, i)
                                            .expect("valid test scalar")
                                            .to_string()
                                    })
                                    .collect::<Vec<_>>(),
                            );
                        }
                    }
                    let mut expected = expected
                        .iter()
                        .map(|r| projection.iter().map(|&i| r[i].clone()).collect::<Vec<_>>())
                        .collect::<Vec<_>>();
                    actual.sort();
                    expected.sort();
                    assert_eq!(
                        actual, expected,
                        "type={jt:?} build_left={build_left} mode={mode} projection={projection:?}"
                    );
                }
            }
        }
    }
    Ok(())
}
