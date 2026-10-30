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
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow::{array::RecordBatch, datatypes::SchemaRef};
use auron_jni_bridge::{
    conf,
    conf::{BooleanConf, IntConf},
};
use auron_memmgr::MemManager;
use datafusion::{
    common::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{EquivalenceProperties, Partitioning, PhysicalExprRef, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet, Time},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_ext_commons::arrow::{array_size::BatchSize, coalesce::coalesce_batches_unchecked};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::{
    common::{execution_context::ExecutionContext, timer_helper::TimerHelper},
    joins::{
        bucket_repartitioner::BucketRepartitioner,
        bucketed_group::{BucketGroupBuilder, NUM_BUCKETS, append_bucket_id_column, bucket_schema},
        join_hash_map::{JoinHashMap, join_hash_map_schema},
    },
};

pub struct BroadcastJoinBuildHashMapExec {
    input: Arc<dyn ExecutionPlan>,
    keys: Vec<PhysicalExprRef>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl BroadcastJoinBuildHashMapExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, keys: Vec<PhysicalExprRef>) -> Self {
        Self {
            input,
            keys,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl Debug for BroadcastJoinBuildHashMapExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BroadcastJoinBuildHashMap [{:?}]", self.keys)
    }
}

impl DisplayAs for BroadcastJoinBuildHashMapExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "BroadcastJoinBuildHashMapExec [{:?}]", self.keys)
    }
}

impl ExecutionPlan for BroadcastJoinBuildHashMapExec {
    fn name(&self) -> &str {
        "BroadcastJoinBuildHashMapExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        join_hash_map_schema(&self.input.schema())
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(
                    self.input.output_partitioning().partition_count(),
                ),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone(), self.keys.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute(&self.input)?;
        let build_time = exec_ctx.register_timer_metric("build_time");
        execute_build_hash_map(input, self.keys.clone(), exec_ctx, build_time)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

pub fn execute_build_hash_map(
    mut input: SendableRecordBatchStream,
    keys: Vec<PhysicalExprRef>,
    exec_ctx: Arc<ExecutionContext>,
    build_time: Time,
) -> Result<SendableRecordBatchStream> {
    // output hash map batches as stream
    Ok(exec_ctx
        .clone()
        .output_with_sender("BuildHashMap", move |sender| async move {
            sender.exclude_time(&build_time);
            let _timer = build_time.timer();

            let hybrid_join_enabled = conf::HYBRID_JOIN_ENABLE.value().unwrap_or(false);
            let hybrid_join_rows_threshold =
                conf::HYBRID_JOIN_ROWS_THRESHOLD.value().unwrap_or(i32::MAX) as usize;
            let hybrid_join_mem_threshold = conf::HYBRID_JOIN_MEM_SIZE_THRESHOLD
                .value()
                .unwrap_or(i32::MAX) as usize;

            let data_schema = input.schema();
            let mut staging_batches: Vec<RecordBatch> = vec![];
            let mut staging_num_rows = 0;
            let mut stating_mem_size = 0;
            let mut fallback_to_hybrid_join = false;

            while let Some(batch) = build_time
                .exclude_timer_async(input.next())
                .await
                .transpose()?
            {
                staging_batches.push(batch.clone());
                if hybrid_join_enabled {
                    staging_num_rows += batch.num_rows();
                    stating_mem_size += batch.get_batch_mem_size();

                    // fallback if staging data is too large
                    if staging_num_rows > hybrid_join_rows_threshold
                        || stating_mem_size > hybrid_join_mem_threshold
                    {
                        fallback_to_hybrid_join = true;
                        break;
                    }
                }
            }

            // no fallbacks - generate one hashmap batch
            if !fallback_to_hybrid_join {
                let data_batch =
                    coalesce_batches_unchecked(data_schema, &std::mem::take(&mut staging_batches));
                let hash_map = JoinHashMap::create_from_data_batch(data_batch, &keys)?;
                for hash_map_batch in hash_map.into_hash_map_batches(None)? {
                    sender.send(hash_map_batch).await;
                }
                exec_ctx
                    .baseline_metrics()
                    .elapsed_compute()
                    .add_duration(build_time.duration());
                return Ok(());
            }

            // fallback to hybrid join (bucketed hash join):
            // 1. append a ~BUCKET column (hash(keys) % NUM_BUCKETS) to every batch
            // 2. group all input data by ~BUCKET using a spill-capable radix-sort based
            //    repartitioner (reusing the same grouping/spill machinery as shuffle writes
            //    - see `BucketRepartitioner`), rather than a full comparison sort: the join
            //    only needs rows grouped by bucket, not globally ordered.
            // 3. greedily cut the bucket-grouped stream into bucket groups, each one built
            //    into its own JoinHashMap and serialized as a bucketed-group hash-map-batch
            let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                data_schema.clone(),
                futures::stream::iter(staging_batches.into_iter().map(Ok)).chain(input),
            ));
            let bucket_keys = keys.clone();
            let bucket_col_idx = data_schema.fields().len();
            let mut bucketed_input = input.map(move |batch| {
                batch.and_then(|batch| append_bucket_id_column(batch, &bucket_keys))
            });

            let bucket_repartitioner = Arc::new(BucketRepartitioner::new(
                exec_ctx.clone(),
                bucket_schema(&data_schema),
                bucket_col_idx,
                NUM_BUCKETS as usize,
            ));
            MemManager::register_consumer(bucket_repartitioner.clone(), true);
            while let Some(batch) = bucketed_input.next().await.transpose()? {
                bucket_repartitioner.insert_batch(batch).await?;
            }
            let mut sorted_stream = bucket_repartitioner.into_stream().await?;

            let group_mem_size_threshold = conf::HYBRID_JOIN_GROUP_MEM_SIZE_THRESHOLD
                .value()
                .unwrap_or(16 * 1024 * 1024) as usize;
            let mut group_builder = BucketGroupBuilder::new(
                group_mem_size_threshold,
                keys.clone(),
                data_schema.clone(),
            );

            while let Some(batch) = sorted_stream.next().await.transpose()? {
                for group_batch in group_builder.push(batch)? {
                    sender.send(group_batch).await;
                }
            }
            for group_batch in group_builder.finish()? {
                sender.send(group_batch).await;
            }
            exec_ctx
                .baseline_metrics()
                .elapsed_compute()
                .add_duration(build_time.duration());
            Ok(())
        }))
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use auron_memmgr::MemManager;
    use datafusion::{
        common::{JoinSide, ScalarValue},
        logical_expr::Operator,
        physical_expr::expressions::{BinaryExpr, Column},
        physical_plan::{common::collect, joins::utils::build_join_schema, test::TestMemoryExec},
        prelude::SessionContext,
    };

    use super::*;
    use crate::{
        broadcast_join_exec::BroadcastJoinExec,
        common::column_pruning::ExecuteWithColumnPruning,
        joins::{
            ColumnIndex, JoinFilter,
            bucketed_group::{BucketGroupBuilder, append_bucket_id_column},
            join_utils::JoinType,
        },
    };

    fn memory(batch: RecordBatch) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TestMemoryExec::try_new(
            &[vec![batch.clone()]],
            batch.schema(),
            None,
        )?))
    }

    #[tokio::test]
    async fn nested_loop_bucketed_group_fallback_preserves_condition_and_projection() -> Result<()>
    {
        MemManager::init(1000000);
        let values = [
            vec![None, Some(1), Some(3), Some(8)],
            vec![None, Some(2), Some(4)],
        ];
        let batches = values
            .iter()
            .enumerate()
            .map(|(side, values)| {
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        if side == 0 { "l" } else { "r" },
                        DataType::Int32,
                        true,
                    )])),
                    vec![Arc::new(Int32Array::from(values.clone()))],
                )
                .expect("valid test batch")
            })
            .collect::<Vec<_>>();
        for (jt, build_left) in [
            (JoinType::Inner, false),
            (JoinType::Inner, true),
            (JoinType::Left, false),
            (JoinType::Right, true),
            (JoinType::LeftSemi, false),
            (JoinType::LeftAnti, false),
            (JoinType::Existence, false),
        ] {
            let schema = if jt == JoinType::Existence {
                Arc::new(Schema::new(vec![
                    batches[0].schema().field(0).clone(),
                    Field::new("exists", DataType::Boolean, false),
                ]))
            } else {
                Arc::new(
                    build_join_schema(&batches[0].schema(), &batches[1].schema(), &jt.try_into()?)
                        .0,
                )
            };
            let mut expected = vec![];
            let mut right_matched = vec![false; values[1].len()];
            for &l in &values[0] {
                let mut matched = false;
                for (ri, &r) in values[1].iter().enumerate() {
                    if l.zip(r).is_some_and(|(l, r)| l < r) {
                        matched = true;
                        right_matched[ri] = true;
                        if matches!(jt, JoinType::Inner | JoinType::Left | JoinType::Right) {
                            expected.push(vec![ScalarValue::Int32(l), ScalarValue::Int32(r)]);
                        }
                    }
                }
                match jt {
                    JoinType::Left if !matched => {
                        expected.push(vec![ScalarValue::Int32(l), ScalarValue::Int32(None)])
                    }
                    JoinType::LeftSemi if matched => expected.push(vec![ScalarValue::Int32(l)]),
                    JoinType::LeftAnti if !matched => expected.push(vec![ScalarValue::Int32(l)]),
                    JoinType::Existence => expected.push(vec![
                        ScalarValue::Int32(l),
                        ScalarValue::Boolean(Some(matched)),
                    ]),
                    _ => {}
                }
            }
            if jt == JoinType::Right {
                for (&r, matched) in values[1].iter().zip(right_matched) {
                    if !matched {
                        expected.push(vec![ScalarValue::Int32(None), ScalarValue::Int32(r)]);
                    }
                }
            }
            let ctx = SessionContext::new().task_ctx();
            let build_batch = batches[usize::from(!build_left)].clone();
            // Simulate a hybrid-join (bucketed hash join) fallback: the build
            // side exceeded the configured threshold, so it got partitioned
            // into bucket groups (here: a single oversized group, since the
            // join has no equality keys). The build exec's declared schema
            // is always `join_hash_map_schema` (data + ~TABLE); the bucket
            // group id is packed into ~TABLE's own metadata rather than a
            // separate column, so this matches BroadcastJoinBuildHashMapExec's
            // real output exactly, hybrid or not.
            let bucketed_batch = append_bucket_id_column(build_batch.clone(), &[])?;
            let mut group_builder = BucketGroupBuilder::new(0, vec![], build_batch.schema());
            let mut group_batches = group_builder.push(bucketed_batch)?;
            group_batches.extend(group_builder.finish()?);
            let hash_map_schema = join_hash_map_schema(&build_batch.schema());
            let built = Arc::new(TestMemoryExec::try_new(
                &[group_batches],
                hash_map_schema,
                None,
            )?) as Arc<dyn ExecutionPlan>;
            let (left, right) = if build_left {
                (built, memory(batches[1].clone())?)
            } else {
                (memory(batches[0].clone())?, built)
            };
            let filter = JoinFilter {
                expression: Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("l", 0)),
                    Operator::Lt,
                    Arc::new(Column::new("r", 1)),
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
                    batches[0].schema().field(0).clone(),
                    batches[1].schema().field(0).clone(),
                ])),
            };
            let join = BroadcastJoinExec::try_new(
                schema.clone(),
                left,
                right,
                vec![],
                jt,
                if build_left {
                    JoinSide::Left
                } else {
                    JoinSide::Right
                },
                true,
                None,
                false,
                Some(filter),
            )?;
            for projection in [
                (0..schema.fields().len()).collect::<Vec<_>>(),
                vec![schema.fields().len() - 1],
                vec![],
            ] {
                let output = collect(join.execute_projected(0, ctx.clone(), &projection)?).await?;
                let mut actual = vec![];
                for batch in output {
                    for row in 0..batch.num_rows() {
                        actual.push(
                            batch
                                .columns()
                                .iter()
                                .map(|col| {
                                    ScalarValue::try_from_array(col, row).map(|v| v.to_string())
                                })
                                .collect::<Result<Vec<_>>>()?,
                        );
                    }
                }
                let mut wanted = expected
                    .iter()
                    .map(|row| {
                        projection
                            .iter()
                            .map(|&i| row[i].to_string())
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                actual.sort();
                wanted.sort();
                assert_eq!(
                    actual, wanted,
                    "join={jt:?} build_left={build_left} projection={projection:?}"
                );
            }
        }
        Ok(())
    }
}
