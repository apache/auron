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
    array::{Array, ArrayRef, Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use datafusion::{
    common::{JoinSide, Result},
    physical_expr::expressions::Column,
    physical_plan::{
        ExecutionPlan, common::collect, joins::utils::build_join_schema, test::TestMemoryExec,
    },
    prelude::SessionContext,
};

use crate::{
    broadcast_join_exec::BroadcastJoinExec,
    joins::{JoinFilter as CompactFilter, join_utils::JoinType},
};

fn keyless_broadcast_join(
    left: &RecordBatch,
    right: &RecordBatch,
    kind: JoinType,
    build_side: JoinSide,
    filter: Option<CompactFilter>,
) -> Result<BroadcastJoinExec> {
    let input = |batch: &RecordBatch| -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TestMemoryExec::try_new(
            &[vec![batch.clone()]],
            batch.schema(),
            None,
        )?))
    };
    raw_join(input(left)?, input(right)?, kind, build_side, filter, None)
}

fn raw_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    kind: JoinType,
    build_side: JoinSide,
    filter: Option<CompactFilter>,
    cache_id: Option<String>,
) -> Result<BroadcastJoinExec> {
    BroadcastJoinExec::try_new(
        Arc::new(build_join_schema(&left.schema(), &right.schema(), &kind.try_into()?).0),
        left,
        right,
        vec![],
        kind,
        build_side,
        true,
        cache_id,
        false,
        filter,
    )?
    .with_broadcast_rows()
}

#[tokio::test]
async fn singleton_keyless_join_skips_hashing_and_preserves_probe_batches() -> Result<()> {
    use arrow::{array::AsArray, datatypes::Int32Type};
    use datafusion::{common::ScalarValue, physical_expr::expressions::Literal};
    use datafusion_ext_commons::batch_size;

    let limit = batch_size();
    let ids = Int32Array::from_iter((0..=limit).map(|i| (i != 1).then_some(i as i32)));
    let probe = RecordBatch::try_from_iter([("id", Arc::new(ids) as ArrayRef)])?;
    let lookup =
        RecordBatch::try_from_iter([("lookup", Arc::new(Int32Array::from(vec![42])) as ArrayRef)])?;
    for (side, outer) in [
        (JoinSide::Left, JoinType::Right),
        (JoinSide::Right, JoinType::Left),
    ] {
        let build_left = side == JoinSide::Left;
        let empty = lookup.slice(0, 0);
        for (kind, literal, build) in [
            (JoinType::Inner, None, &lookup),
            (outer, Some(Some(true)), &lookup),
            (outer, Some(Some(false)), &lookup),
            (outer, Some(None), &lookup),
            (outer, None, &empty),
        ] {
            let (left, right) = if build_left {
                (build, &probe)
            } else {
                (&probe, build)
            };
            let filter = literal.map(|value| CompactFilter {
                expression: Arc::new(Literal::new(ScalarValue::Boolean(value))),
                column_indices: vec![],
                schema: Arc::new(Schema::empty()),
            });
            let join = keyless_broadcast_join(left, right, kind, side, filter)?;
            let output = collect(join.execute(0, SessionContext::new().task_ctx())?).await?;
            let matched = build.num_rows() > 0 && literal.unwrap_or(Some(true)) == Some(true);
            assert_eq!(
                output.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
                [limit, 1]
            );
            let metrics = join.metrics().expect("broadcast join metrics");
            for name in [
                "build_hash_map_time",
                "probed_side_hash_time",
                "probed_side_search_time",
                "probed_side_compare_time",
            ] {
                let timer = metrics
                    .iter()
                    .find(|m| m.value().name() == name)
                    .expect("join timer");
                assert_eq!(timer.value().as_usize(), 0, "{name}");
            }
            let mut seen = 0;
            for batch in output {
                let len = batch.num_rows();
                assert_eq!(
                    batch.column(usize::from(build_left)),
                    &probe.column(0).slice(seen, len)
                );
                let values = batch
                    .column(usize::from(!build_left))
                    .as_primitive::<Int32Type>();
                assert!(values.iter().all(|v| v == matched.then_some(42)));
                seen += len;
            }
            assert_eq!(seen, probe.num_rows());
        }
    }
    Ok(())
}

#[tokio::test]
async fn keyless_left_join_handles_empty_and_multirow_builds() -> Result<()> {
    use arrow::{array::AsArray, datatypes::Int32Type};

    let input = |name, ids: Vec<i32>| {
        RecordBatch::try_from_iter([(name, Arc::new(Int32Array::from(ids)) as ArrayRef)])
    };
    let probe = input("probe", vec![7])?;
    for (values, expected) in [(vec![], vec![None]), (vec![3, 5], vec![Some(3), Some(5)])] {
        let build = input("lookup", values)?;
        let join = keyless_broadcast_join(&probe, &build, JoinType::Left, JoinSide::Right, None)?;
        let output = collect(join.execute(0, SessionContext::new().task_ctx())?).await?;
        assert_eq!(output.len(), 1);
        let batch = &output[0];
        assert_eq!(batch.num_rows(), expected.len());
        assert!(
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .iter()
                .all(|v| v == Some(7))
        );
        assert!(
            batch
                .column(1)
                .as_primitive::<Int32Type>()
                .iter()
                .eq(expected)
        );
    }
    Ok(())
}

#[tokio::test]
async fn legacy_keyless_hash_broadcast_matches_raw_rows() -> Result<()> {
    use datafusion::physical_expr::PhysicalExprRef;

    use crate::broadcast_join_build_hash_map_exec::BroadcastJoinBuildHashMapExec;

    let memory = |batch: RecordBatch| -> Result<Arc<dyn ExecutionPlan>> {
        let schema = batch.schema();
        Ok(Arc::new(TestMemoryExec::try_new(
            &[vec![batch]],
            schema,
            None,
        )?))
    };
    let probe = memory(RecordBatch::try_from_iter([(
        "probe",
        Arc::new(Int32Array::from(vec![5])) as ArrayRef,
    )])?)?;
    let build = memory(RecordBatch::try_from_iter([(
        "build",
        Arc::new(Int32Array::from(vec![3, 6])) as ArrayRef,
    )])?)?;
    let schema = Arc::new(
        build_join_schema(
            &probe.schema(),
            &build.schema(),
            &JoinType::Inner.try_into()?,
        )
        .0,
    );
    let plan = |right, keys, is_built| {
        BroadcastJoinExec::try_new(
            schema.clone(),
            probe.clone(),
            right,
            keys,
            JoinType::Inner,
            JoinSide::Right,
            is_built,
            None,
            false,
            None,
        )
    };
    let map = Arc::new(BroadcastJoinBuildHashMapExec::new(build.clone(), vec![]));
    let legacy = plan(map, vec![], true)?;
    let raw = plan(build.clone(), vec![], true)?.with_broadcast_rows()?;
    let ctx = SessionContext::new().task_ctx();
    let expected = collect(legacy.execute(0, ctx.clone())?).await?;
    assert_eq!(expected.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert_eq!(collect(raw.execute(0, ctx)?).await?, expected);

    let key = Arc::new(Column::new("probe", 0)) as PhysicalExprRef;
    let build_key = Arc::new(Column::new("build", 0)) as PhysicalExprRef;
    assert!(
        plan(build.clone(), vec![(key, build_key)], true)?
            .with_broadcast_rows()
            .is_err()
    );
    assert!(plan(build, vec![], false)?.with_broadcast_rows().is_err());
    Ok(())
}

#[tokio::test]
async fn raw_broadcast_preserves_zero_column_and_empty_inputs() -> Result<()> {
    use arrow::array::RecordBatchOptions;

    let probe =
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![7, 9])) as ArrayRef)])?;
    let empty_schema = Arc::new(Schema::empty());
    let build_batch = |rows| {
        RecordBatch::try_new_with_options(
            empty_schema.clone(),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
    };
    for (batches, expected) in [
        (vec![], vec![7, 9]),
        (
            vec![
                build_batch(0)?,
                build_batch(2)?,
                build_batch(0)?,
                build_batch(1)?,
            ],
            vec![7, 7, 7, 9, 9, 9],
        ),
    ] {
        let left = Arc::new(TestMemoryExec::try_new(
            &[vec![probe.clone()]],
            probe.schema(),
            None,
        )?);
        let right = Arc::new(TestMemoryExec::try_new(
            &[batches],
            empty_schema.clone(),
            None,
        )?);
        let join = raw_join(left, right, JoinType::Left, JoinSide::Right, None, None)?;
        let output = collect(join.execute(0, SessionContext::new().task_ctx())?).await?;
        assert_eq!(
            output,
            [RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int32Array::from(expected)) as ArrayRef
            )])?]
        );
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_joins_share_the_same_raw_broadcast() -> Result<()> {
    use crate::common::stream_exec::create_record_batch_stream_exec;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::stream;
    use tokio::sync::oneshot;

    let int_batch = |name, values: Vec<i32>| {
        RecordBatch::try_from_iter([(name, Arc::new(Int32Array::from(values)) as ArrayRef)])
    };
    // A raw input can legitimately contain a data column called ~TABLE.
    let build = int_batch("~TABLE", vec![7, 9])?;
    let build_stream = RecordBatchStreamAdapter::new(
        build.schema(),
        stream::iter([
            Ok(build.slice(0, 0)),
            Ok(build.slice(0, 1)),
            Ok(build.slice(1, 1)),
        ]),
    );
    // This plan can provide its build stream once. A second uncached read is empty.
    let build_plan = create_record_batch_stream_exec(Box::pin(build_stream), 0)?;

    let probe = int_batch("id", vec![1])?;
    let (entered, ready) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    let probe_stream = RecordBatchStreamAdapter::new(
        probe.schema(),
        stream::once(async move {
            entered.send(()).expect("second probe is waiting");
            gate.await.expect("second probe completed");
            Ok(probe)
        }),
    );
    let first = Arc::new(raw_join(
        create_record_batch_stream_exec(Box::pin(probe_stream), 0)?,
        build_plan.clone(),
        JoinType::Inner,
        JoinSide::Right,
        None,
        Some("concurrent_joins_share_the_same_raw_broadcast".to_owned()),
    )?);
    let probe = int_batch("id", vec![2])?;
    let second = first.clone().with_new_children(vec![
        Arc::new(TestMemoryExec::try_new(
            &[vec![probe.clone()]],
            probe.schema(),
            None,
        )?),
        build_plan,
    ])?;
    let task_ctx = SessionContext::new().task_ctx();
    let (first_rows, second_rows) =
        tokio::try_join!(collect(first.execute(0, task_ctx.clone())?), async {
            ready.await.expect("first task holds the cached build");
            let rows = collect(second.execute(0, task_ctx)?).await?;
            release.send(()).expect("first probe is waiting");
            Ok(rows)
        },)?;
    for (output, id) in [(first_rows, 1), (second_rows, 2)] {
        let expected = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![id; 2])) as ArrayRef),
            ("~TABLE", build.column(0).clone()),
        ])?;
        assert_eq!(output, [expected]);
    }
    Ok(())
}

/// Two probe rows repeat one nested list. NullArray stores only its length,
/// so the inner list can cross the real i32 offset limit without a 2 GiB fixture.
#[tokio::test]
async fn keyless_join_respects_nested_offsets() -> Result<()> {
    use arrow::{
        array::{AsArray, ListArray, NullArray},
        buffer::OffsetBuffer,
    };
    use datafusion::{
        common::ScalarValue,
        logical_expr::Operator,
        physical_expr::expressions::{BinaryExpr, Literal, is_not_null},
    };

    use crate::joins::ColumnIndex;

    let probe =
        RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![7, 9])) as ArrayRef)])?;
    for (child_len, filtered, expected_batches) in [
        (3, false, 1),
        (i32::MAX as usize / 2, false, 1),
        (1 << 30, false, 2),
        (1 << 30, true, 1),
    ] {
        let list = |child: ArrayRef| -> ArrayRef {
            Arc::new(ListArray::new(
                Arc::new(Field::new_list_field(child.data_type().clone(), true)),
                OffsetBuffer::from_lengths([child.len()]),
                child,
                None,
            ))
        };
        let lookup = list(list(Arc::new(NullArray::new(child_len))));
        let build = RecordBatch::try_from_iter([("lookup", lookup)])?;
        if child_len == 1 << 30 && !filtered {
            // One probe matching two separate build batches also crosses the offset limit.
            let left = TestMemoryExec::try_new(&[vec![probe.slice(0, 1)]], probe.schema(), None)?;
            let right = TestMemoryExec::try_new(
                &[vec![build.clone(), build.clone()]],
                build.schema(),
                None,
            )?;
            let join = raw_join(
                Arc::new(left),
                Arc::new(right),
                JoinType::Inner,
                JoinSide::Right,
                None,
                None,
            )?;
            let output = collect(join.execute(0, SessionContext::new().task_ctx())?).await?;
            assert_eq!(output.len(), 2);
            for batch in output {
                assert_eq!(batch.num_rows(), 1);
                let list = batch.column(1).as_list::<i32>();
                assert_eq!(list.value_length(0), 1);
                assert_eq!(
                    list.values().as_list::<i32>().value_length(0) as usize,
                    child_len
                );
            }
        }
        // The residual also gathers the list. Its first pair is rejected,
        // so the left join must emit exactly one null-extended row for id 7.
        let condition = if filtered {
            Some(CompactFilter {
                expression: Arc::new(BinaryExpr::new(
                    is_not_null(Arc::new(Column::new("lookup", 1)))?,
                    Operator::And,
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("id", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Int32(Some(9)))),
                    )),
                )),
                column_indices: vec![
                    ColumnIndex {
                        side: JoinSide::Left,
                        index: 0,
                    },
                    ColumnIndex {
                        side: JoinSide::Right,
                        index: 0,
                    },
                ],
                schema: Arc::new(Schema::new(vec![
                    probe.schema().field(0).clone(),
                    build.schema().field(0).clone(),
                ])),
            })
        } else {
            None
        };
        if filtered {
            // Reverse the build side: one probe list is repeated for two build IDs.
            // The list is only read by the predicate, so output splitting cannot help.
            let predicate = condition.as_ref().expect("filtered case");
            let prepared = crate::joins::join_filter::JoinFilter::try_new(
                predicate.expression.clone(),
                predicate.schema.clone(),
                1,
                datafusion::physical_plan::metrics::Time::new(),
            )?;
            // Physical-batch-aligned candidate ranges use this evaluator directly.
            assert_eq!(
                super::output::filter_one_probe(&prepared, false, &build, 0, &probe, 0, 2)?,
                [1]
            );
            let join: Arc<dyn ExecutionPlan> = Arc::new(keyless_broadcast_join(
                &probe,
                &build,
                JoinType::Right,
                JoinSide::Left,
                condition.clone(),
            )?);
            let output = collect(crate::joins::test::execute_projected(
                &join,
                0,
                SessionContext::new().task_ctx(),
                &[0],
            )?)
            .await?;
            assert_eq!(output.len(), 1);
            assert_eq!(output[0].num_columns(), 1);
            assert_eq!(output[0].column(0), &probe.column(0).slice(1, 1));
        }
        let join =
            keyless_broadcast_join(&probe, &build, JoinType::Left, JoinSide::Right, condition)?;
        let out = collect(join.execute(0, SessionContext::new().task_ctx())?).await?;
        assert_eq!(out.len(), expected_batches);
        let mut ids = vec![];
        for batch in out {
            let batch_ids = batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int32Type>();
            ids.extend_from_slice(batch_ids.values());
            let outer = batch.column(1).as_list::<i32>();
            for (i, len) in outer.offsets().lengths().enumerate() {
                let matched = !filtered || batch_ids.value(i) == 9;
                assert_eq!(outer.is_valid(i), matched);
                assert_eq!(len, usize::from(matched));
            }
            let inner = outer.values().as_list::<i32>();
            assert!(inner.offsets().lengths().all(|len| len == child_len));
        }
        assert_eq!(ids, [7, 9]);
    }
    Ok(())
}

/// Each probe row spans two candidate chunks. Both inputs also span physical
/// batches, so build-side matches must survive multiple calls to `join`.
#[tokio::test]
async fn conditional_join_preserves_matches_and_order_across_chunks() -> Result<()> {
    use std::{borrow::Borrow, pin::pin};

    use arrow::util::display::array_value_to_string;
    use datafusion::{
        logical_expr::Operator,
        physical_expr::expressions::BinaryExpr,
        physical_plan::metrics::{ExecutionPlanMetricsSet, Time},
    };

    use super::broadcast_nest_loop_join_exec::BnljJoiner;
    use crate::{
        broadcast_join_exec::Joiner,
        common::execution_context::{ExecutionContext, WrappedLazyBatchSender},
        joins::{
            JoinParams, JoinProjection,
            join_filter::JoinFilter,
            join_hash_map::{JoinBuildRows, JoinHashMap},
        },
    };

    let int_batch = |name, values| {
        RecordBatch::try_from_iter([(name, Arc::new(Int32Array::from(values)) as ArrayRef)])
    };
    let left = int_batch("l", vec![Some(1), Some(11), Some(7)])?;
    let right = int_batch("r", vec![Some(9), None, Some(6)])?;
    let ls = left.schema();
    let rs = right.schema();
    let residual = Arc::new(JoinFilter::try_new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("l", 0)),
            Operator::Lt,
            Arc::new(Column::new("r", 1)),
        )),
        Arc::new(Schema::new(vec![ls.field(0).clone(), rs.field(0).clone()])),
        1,
        Time::new(),
    )?);
    for (kind, right_built, left_built) in [
        (JoinType::Inner, "1,9 1,6 7,9", "1,9 7,9 1,6"),
        (JoinType::Left, "1,9 1,6 11,_ 7,9", "1,9 7,9 1,6 11,_"),
        (JoinType::Right, "1,9 1,6 7,9 _,_", "1,9 7,9 _,_ 1,6"),
        (
            JoinType::Full,
            "1,9 1,6 11,_ 7,9 _,_",
            "1,9 7,9 _,_ 1,6 11,_",
        ),
        (JoinType::LeftSemi, "1 7", "1 7"),
        (JoinType::RightSemi, "9 6", "9 6"),
        (JoinType::LeftAnti, "11", "11"),
        (JoinType::RightAnti, "_", "_"),
        (
            JoinType::Existence,
            "1,true 11,false 7,true",
            "1,true 11,false 7,true",
        ),
    ] {
        let schema = Arc::new(if kind == JoinType::Existence {
            Schema::new(vec![
                ls.field(0).clone(),
                Field::new("exists", DataType::Boolean, false),
            ])
        } else {
            build_join_schema(&ls, &rs, &kind.try_into()?).0
        });
        for (side, build, probe, expected) in [
            (JoinSide::Right, &right, &left, right_built),
            (JoinSide::Left, &left, &right, left_built),
        ] {
            let params = JoinParams {
                join_type: kind,
                left_schema: ls.clone(),
                right_schema: rs.clone(),
                output_schema: schema.clone(),
                left_keys: vec![],
                right_keys: vec![],
                key_data_types: vec![],
                sort_options: vec![],
                projection: JoinProjection::try_new(
                    kind,
                    &schema,
                    &ls,
                    &rs,
                    &(0..schema.fields().len()).collect::<Vec<_>>(),
                )?,
                join_filter: None,
                residual_filter: Some(residual.clone()),
                batch_size: 2,
                is_null_aware_anti_join: false,
                output_time: Time::new(),
            };
            let map = Arc::new(JoinHashMap::create_from_data_batches(
                vec![build.slice(0, 1), build.slice(1, 2)],
                &[],
            )?);
            let rows = map.materialized_build_rows()?;
            assert_eq!(rows.data_batches().len(), 2);
            let rows = Arc::new(rows);
            let ctx = ExecutionContext::new(
                SessionContext::new().task_ctx(),
                0,
                schema.clone(),
                &ExecutionPlanMetricsSet::new(),
            );
            let (tx, mut rx) = tokio::sync::mpsc::channel(32);
            {
                let sender = WrappedLazyBatchSender::new(ctx, tx);
                let mut joiner = pin!(BnljJoiner::new(side, params, rows, sender));
                let time = Time::new();
                for batch in [probe.slice(0, 2), probe.slice(2, 1)] {
                    joiner
                        .as_mut()
                        .join(Arc::new(batch), &time, &time, &time, &time)
                        .await?;
                }
                joiner.as_mut().finish(&time).await?;
            }
            let mut rows = vec![];
            while let Some(batch) = rx.recv().await {
                let batch = batch?.materialize()?;
                for row in 0..batch.num_rows() {
                    let values = batch
                        .columns()
                        .iter()
                        .map(|col| {
                            if col.is_null(row) {
                                Ok("_".to_string())
                            } else {
                                array_value_to_string(col, row)
                            }
                        })
                        .collect::<arrow::error::Result<Vec<_>>>()?;
                    rows.push(values.join(","));
                }
            }
            assert_eq!(rows.join(" "), expected, "{kind:?} with {side:?} build");
        }
    }
    Ok(())
}
