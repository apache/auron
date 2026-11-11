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

use std::sync::Arc;

use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    common::Result,
    physical_expr::{PhysicalExprRef, expressions::Column},
    prelude::SessionContext,
};

use crate::joins::join_utils::JoinType;

#[tokio::test]
async fn residual_hash_join_operator_and_spill_parity() -> Result<()> {
    use arrow::array::ArrayRef;
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
        joins::{
            ColumnIndex, JoinFilter as CompactFilter,
            bucketed_group::{BucketGroupBuilder, append_bucket_id_column},
            join_hash_map::join_hash_map_schema,
        },
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
                        // simulate a hybrid-join (bucketed hash join)
                        // fallback build output: bucketed-group
                        // hash-map-batches (data + ~TABLE, with the bucket
                        // group id packed into ~TABLE's own metadata),
                        // possibly split into multiple groups. Build groups
                        // require rows to be pre-sorted ascending by
                        // ~BUCKET (mirrors the real build-exec pipeline's
                        // SortExec stage).
                        let bucketed = append_bucket_id_column(
                            build_batch.clone(),
                            std::slice::from_ref(&key),
                        )?;
                        let bucket_col_idx = bucketed.num_columns() - 1;
                        let sort_indices = arrow::compute::sort_to_indices(
                            bucketed.column(bucket_col_idx),
                            None,
                            None,
                        )?;
                        let bucketed = arrow::compute::take_record_batch(&bucketed, &sort_indices)?;
                        let mut group_builder =
                            BucketGroupBuilder::new(0, vec![key], build_batch.schema());
                        let mut group_batches = group_builder.push(bucketed)?;
                        group_batches.extend(group_builder.finish()?);
                        let hash_map_schema = join_hash_map_schema(&build_batch.schema());
                        Arc::new(TestMemoryExec::try_new(
                            &[group_batches],
                            hash_map_schema,
                            None,
                        )?)
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
                let join: Arc<dyn ExecutionPlan> = Arc::new(BroadcastJoinExec::try_new(
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
                )?);
                for projection in [
                    (0..schema.fields().len()).collect::<Vec<_>>(),
                    vec![schema.fields().len() - 1],
                    vec![],
                ] {
                    let output = collect(crate::joins::test::execute_projected(
                        &join,
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

#[test]
fn reuse_candidate_columns_takes_survivors_and_dedups() -> Result<()> {
    use arrow::array::ArrayRef;

    use crate::{
        common::lazy::{LazyColumn, column},
        joins::bhj::reuse_candidate_columns,
    };

    // candidate-level takes use distinct values so outputs prove they come
    // from the gathered arrays, not from the source columns
    let taken: ArrayRef = Arc::new(Int32Array::from(vec![110, 120, 130]));
    let source: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
    let mut cols = vec![
        column(source.clone()),
        column(source.clone()),
        column(source.clone()),
    ];
    let positions = Int32Array::from(vec![Some(2), None, Some(0)]);
    reuse_candidate_columns(&mut cols, &[7, 7, 8], &[(7, taken.clone())], &positions)?;
    // duplicated projections share one output column
    assert!(Arc::ptr_eq(&cols[0], &cols[1]));
    assert_eq!(
        cols[0].materialize()?.as_ref(),
        &Int32Array::from(vec![Some(130), None, Some(110)])
    );
    // non-shared projections keep their source-based columns
    assert_eq!(
        cols[2].materialize()?.as_ref(),
        &Int32Array::from(vec![10, 20, 30, 40])
    );
    Ok(())
}

#[test]
fn reuse_candidate_columns_identity_positions_share_payload() -> Result<()> {
    use arrow::array::ArrayRef;

    use crate::{
        common::lazy::{LazyColumn, column},
        joins::bhj::reuse_candidate_columns,
    };

    let taken: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
    let mut cols = vec![column(taken.clone())];
    reuse_candidate_columns(
        &mut cols,
        &[0],
        &[(0, taken.clone())],
        &Int32Array::from(vec![0, 1]),
    )?;
    // identity survivor positions reuse the gathered array without copying
    assert!(Arc::ptr_eq(&cols[0].materialize()?, &taken));
    Ok(())
}
