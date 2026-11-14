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
    collections::VecDeque,
    fmt::{Debug, Formatter},
    future::Future,
    pin::Pin,
    sync::{Arc, Weak},
};

use arrow::{
    array::{RecordBatch, UInt64Array},
    compute::SortOptions,
    datatypes::{DataType, Field, FieldRef, Schema},
};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use auron_memmgr::MemManager;
use datafusion::{
    common::{JoinSide, Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        joins::utils::JoinOn,
        metrics::{ExecutionPlanMetricsSet, MetricsSet, Time},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_ext_commons::{batch_size, df_execution_err};
use futures::{StreamExt, TryStreamExt};
use futures_util::stream::Peekable;
use hashbrown::HashMap;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    broadcast_join_build_hash_map_exec::execute_build_hash_map,
    common::{
        column_pruning::ExecuteWithColumnPruning,
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
        stream_exec::create_record_batch_stream_exec,
        timer_helper::TimerHelper,
    },
    joins::{
        JoinFilter, JoinParams, JoinProjection,
        bhj::{
            full_join::{
                LProbedFullOuterJoiner, LProbedInnerJoiner, LProbedLeftJoiner, LProbedRightJoiner,
                RProbedFullOuterJoiner, RProbedInnerJoiner, RProbedLeftJoiner, RProbedRightJoiner,
            },
            semi_join::{
                LProbedExistenceJoiner, LProbedLeftAntiJoiner, LProbedLeftSemiJoiner,
                LProbedRightAntiJoiner, LProbedRightSemiJoiner, RProbedExistenceJoiner,
                RProbedLeftAntiJoiner, RProbedLeftSemiJoiner, RProbedRightAntiJoiner,
                RProbedRightSemiJoiner,
            },
        },
        bucket_repartitioner::BucketRepartitioner,
        bucketed_group::{
            NUM_BUCKETS, append_bucket_id_column, bucket_schema, remove_last_column,
            split_batch_by_bucket_threshold,
        },
        filter_read_columns,
        join_filter::JoinFilter as PreparedJoinFilter,
        join_hash_map::{
            JoinHashMap, join_data_schema, join_hash_map_schema, peek_is_hybrid_join,
            split_hash_map_batch_by_group,
        },
        join_utils::{JoinType, JoinType::*},
    },
    sort_exec::create_default_ascending_sort_exec,
};

#[derive(Debug)]
pub struct BroadcastJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: JoinOn,
    join_type: JoinType,
    broadcast_side: JoinSide,
    schema: SchemaRef,
    is_built: bool, // true for BroadcastHashJoin, false for ShuffledHashJoin
    cached_build_hash_map_id: Option<String>,
    is_null_aware_anti_join: bool,
    join_filter: Option<JoinFilter>,
    // preserve probe-side row order even when falling back to a hybrid join
    required_reserved_order: bool,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<Arc<PlanProperties>>,
}

impl BroadcastJoinExec {
    pub fn try_new(
        schema: SchemaRef,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        broadcast_side: JoinSide,
        is_built: bool,
        cached_build_hash_map_id: Option<String>,
        is_null_aware_anti_join: bool,
        join_filter: Option<JoinFilter>,
    ) -> Result<Self> {
        Ok(Self {
            left,
            right,
            on,
            join_type,
            broadcast_side,
            schema,
            is_built,
            cached_build_hash_map_id,
            is_null_aware_anti_join,
            join_filter,
            required_reserved_order: false,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }

    /// Keep the probe side's row order in the output, even if this join falls
    /// back to a hybrid (bucketed hash) join.
    pub fn with_required_reserved_order(mut self, required_reserved_order: bool) -> Self {
        self.required_reserved_order = required_reserved_order;
        self
    }

    pub fn on(&self) -> &JoinOn {
        &self.on
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    pub fn broadcast_side(&self) -> JoinSide {
        self.broadcast_side
    }

    // Built hash maps append a private table column, absent from join input rows.
    fn data_schemas(&self) -> (SchemaRef, SchemaRef) {
        let left = self.left.schema();
        let right = self.right.schema();
        (
            if self.is_built && self.broadcast_side == JoinSide::Left {
                join_data_schema(&left)
            } else {
                left
            },
            if self.is_built && self.broadcast_side == JoinSide::Right {
                join_data_schema(&right)
            } else {
                right
            },
        )
    }

    fn create_join_params(&self, projection: &[usize]) -> Result<JoinParams> {
        let left_schema = self.left.schema();
        let right_schema = self.right.schema();
        let (left_keys, right_keys): (Vec<PhysicalExprRef>, Vec<PhysicalExprRef>) =
            self.on.iter().cloned().unzip();
        let key_data_types: Vec<DataType> = self
            .on
            .iter()
            .map(|(left_key, right_key)| {
                Ok({
                    let left_dt = left_key.data_type(&left_schema)?;
                    let right_dt = right_key.data_type(&right_schema)?;
                    if left_dt != right_dt {
                        df_execution_err!(
                            "join key data type differs {left_dt:?} <-> {right_dt:?}"
                        )?;
                    }
                    left_dt
                })
            })
            .collect::<Result<_>>()?;

        let projection = JoinProjection::try_new(
            self.join_type,
            &self.schema,
            &match self.broadcast_side {
                JoinSide::Left if self.is_built => join_data_schema(&left_schema),
                _ => left_schema.clone(),
            },
            &match self.broadcast_side {
                JoinSide::Right if self.is_built => join_data_schema(&right_schema),
                _ => right_schema.clone(),
            },
            projection,
        )?;

        Ok(JoinParams {
            join_type: self.join_type,
            left_schema,
            right_schema,
            output_schema: self.schema(),
            left_keys,
            right_keys,
            batch_size: batch_size(),
            sort_options: vec![SortOptions::default(); self.on.len()],
            projection,
            join_filter: self.join_filter.clone(),
            residual_filter: None,
            output_time: Time::new(),
            key_data_types,
            is_null_aware_anti_join: self.is_null_aware_anti_join,
        })
    }

    fn execute_with_projection(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: Vec<usize>,
    ) -> Result<SendableRecordBatchStream> {
        let mut join_params = self.create_join_params(&projection)?;
        let exec_ctx = ExecutionContext::new(
            context,
            partition,
            join_params.projection.schema.clone(),
            &self.metrics,
        );
        if let Some(filter) = &self.join_filter {
            let (left_schema, right_schema) = self.data_schemas();
            let expr = filter.physical_expr(left_schema.fields().len())?;
            let (left_read, right_read) = filter_read_columns(&expr, left_schema.fields().len());
            join_params.residual_filter = Some(Arc::new(PreparedJoinFilter::try_new_remapped(
                expr,
                &left_schema,
                &right_schema,
                &left_read,
                &right_read,
                exec_ctx.register_timer_metric("post_filter_time"),
            )?));
        }
        let left = self.left.clone();
        let right = self.right.clone();
        let broadcast_side = self.broadcast_side;
        let is_built = self.is_built;
        let cached_build_hash_map_id = self.cached_build_hash_map_id.clone();
        let required_reserved_order = self.required_reserved_order;

        let exec_ctx_cloned = exec_ctx.clone();
        let output_stream = exec_ctx_cloned.clone().output_with_sender(
            if is_built {
                "BroadcastJoin"
            } else {
                "HashJoin"
            },
            move |sender| {
                sender.exclude_time(exec_ctx_cloned.baseline_metrics().elapsed_compute());
                execute_join(
                    left,
                    right,
                    join_params,
                    broadcast_side,
                    cached_build_hash_map_id,
                    is_built,
                    required_reserved_order,
                    exec_ctx_cloned,
                    sender,
                )
            },
        );
        Ok(exec_ctx.coalesce_with_default_batch_size(output_stream))
    }
}

impl ExecuteWithColumnPruning for BroadcastJoinExec {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        self.execute_with_projection(partition, context, projection.to_vec())
    }
}

impl ExecutionPlan for BroadcastJoinExec {
    fn name(&self) -> &str {
        "BroadcastJoin"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.props.get_or_init(|| {
            Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                match self.broadcast_side {
                    JoinSide::Left => self.right.output_partitioning().clone(),
                    JoinSide::Right => self.left.output_partitioning().clone(),
                    JoinSide::None => unreachable!(),
                },
                EmissionType::Both,
                Boundedness::Bounded,
            ))
        })
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> Result<datafusion::common::tree_node::TreeNodeRecursion>,
    ) -> Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            Self::try_new(
                self.schema.clone(),
                children[0].clone(),
                children[1].clone(),
                self.on.iter().cloned().collect(),
                self.join_type,
                self.broadcast_side,
                self.is_built,
                None,
                self.is_null_aware_anti_join,
                self.join_filter.clone(),
            )?
            .with_required_reserved_order(self.required_reserved_order),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let projection = (0..self.schema.fields().len()).collect();
        self.execute_with_projection(partition, context, projection)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for BroadcastJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}",
            if self.is_built {
                "BroadcastJoin"
            } else {
                "HashJoin"
            }
        )
    }
}

fn create_joiner(
    broadcast_side: JoinSide,
    join_params: JoinParams,
    map: Arc<JoinHashMap>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Pin<Box<dyn Joiner + Send>> {
    match broadcast_side {
        JoinSide::Left => match join_params.join_type {
            Inner => Box::pin(RProbedInnerJoiner::new(join_params, map, sender)),
            Left => Box::pin(RProbedLeftJoiner::new(join_params, map, sender)),
            Right => Box::pin(RProbedRightJoiner::new(join_params, map, sender)),
            Full => Box::pin(RProbedFullOuterJoiner::new(join_params, map, sender)),
            LeftSemi => Box::pin(RProbedLeftSemiJoiner::new(join_params, map, sender)),
            LeftAnti => Box::pin(RProbedLeftAntiJoiner::new(join_params, map, sender)),
            RightSemi => Box::pin(RProbedRightSemiJoiner::new(join_params, map, sender)),
            RightAnti => Box::pin(RProbedRightAntiJoiner::new(join_params, map, sender)),
            Existence => Box::pin(RProbedExistenceJoiner::new(join_params, map, sender)),
        },
        JoinSide::Right => match join_params.join_type {
            Inner => Box::pin(LProbedInnerJoiner::new(join_params, map, sender)),
            Left => Box::pin(LProbedLeftJoiner::new(join_params, map, sender)),
            Right => Box::pin(LProbedRightJoiner::new(join_params, map, sender)),
            Full => Box::pin(LProbedFullOuterJoiner::new(join_params, map, sender)),
            LeftSemi => Box::pin(LProbedLeftSemiJoiner::new(join_params, map, sender)),
            LeftAnti => Box::pin(LProbedLeftAntiJoiner::new(join_params, map, sender)),
            RightSemi => Box::pin(LProbedRightSemiJoiner::new(join_params, map, sender)),
            RightAnti => Box::pin(LProbedRightAntiJoiner::new(join_params, map, sender)),
            Existence => Box::pin(LProbedExistenceJoiner::new(join_params, map, sender)),
        },
        JoinSide::None => unreachable!(),
    }
}

async fn execute_join_with_map(
    probed_plan: Arc<dyn ExecutionPlan>,
    map: Arc<JoinHashMap>,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    exec_ctx: Arc<ExecutionContext>,
    probed_side_hash_time: Time,
    probed_side_search_time: Time,
    probed_side_compare_time: Time,
    build_output_time: Time,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
    let _timer = elapsed_compute.timer();

    let mut joiner = create_joiner(broadcast_side, join_params, map, sender);

    if !joiner.can_early_stop() {
        let mut probed = exec_ctx.stat_input(exec_ctx.execute(&probed_plan)?);
        while !joiner.can_early_stop()
            && let Some(batch) = exec_ctx
                .baseline_metrics()
                .elapsed_compute()
                .exclude_timer_async(probed.next())
                .await
                .transpose()?
        {
            joiner
                .as_mut()
                .join(
                    batch,
                    &probed_side_hash_time,
                    &probed_side_search_time,
                    &probed_side_compare_time,
                    &build_output_time,
                )
                .await?;
        }
    }
    joiner.as_mut().finish(&build_output_time).await?;
    exec_ctx
        .baseline_metrics()
        .record_output(joiner.num_output_rows());
    Ok(())
}

/// Executes a hybrid join (bucketed hash join): the build side arrives as
/// bucketed-group hash-map-batches and is probed one group at a time
/// (`run_hybrid_join`), bounding memory to one group instead of the whole
/// build/probe side. That loses the probe side's row order, so with
/// `required_reserved_order` a `~ROW_NUM` column rides through the join and
/// the output is sorted back into probe order before the column is dropped.
async fn execute_join_with_hybrid_join(
    probed_plan: Arc<dyn ExecutionPlan>,
    built: SendableRecordBatchStream,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    map_keys: Vec<PhysicalExprRef>,
    probe_keys: Vec<PhysicalExprRef>,
    required_reserved_order: bool,
    exec_ctx: Arc<ExecutionContext>,
    probed_side_hash_time: Time,
    probed_side_search_time: Time,
    probed_side_compare_time: Time,
    build_output_time: Time,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
    let _timer = elapsed_compute.timer();

    // Register up front (mirroring `reserve_order_sort_time` below) so the
    // metric is present in Spark's SQL metrics UI even if the output stream is
    // dropped early (e.g. a downstream LIMIT); registering at the end would
    // leave it entirely absent rather than zero. The guard measures the whole
    // function body - the actual cost of falling back to hybrid join.
    let hybrid_join_time = exec_ctx.register_timer_metric("hybrid_join_time");
    let _hybrid_join_timer = hybrid_join_time.timer();

    let output_schema = join_params.projection.schema.clone();
    let probe_side = match broadcast_side {
        JoinSide::Left => JoinSide::Right,
        JoinSide::Right => JoinSide::Left,
        JoinSide::None => unreachable!(),
    };
    let mut row_num_col_idx = 0usize;
    let join_params = if required_reserved_order {
        log::info!(
            "broadcast join fell back to hybrid (bucketed hash) join with \
             required_reserved_order=true, appending ~ROW_NUM column to preserve \
             probe side's original row order"
        );
        let (augmented, idx) = augment_join_params_with_row_num(&join_params, probe_side);
        row_num_col_idx = idx;
        augmented
    } else {
        join_params
    };

    if !required_reserved_order {
        return run_hybrid_join(
            probed_plan,
            built,
            join_params,
            broadcast_side,
            map_keys,
            probe_keys,
            false,
            exec_ctx,
            probed_side_hash_time,
            probed_side_search_time,
            probed_side_compare_time,
            build_output_time,
            sender,
        )
        .await;
    }

    // Sort the ~ROW_NUM-carrying output back into probe order with a
    // spill-capable external sort (same `ExternalSorter` as the bucket sorts).
    // Don't buffer it in memory first: it's the whole probe side, unbounded by
    // the per-group memory limit, and `concat_batches` over variable-length
    // columns silently corrupts past arrow's 2GB offset limit.
    let augmented_schema = join_params.projection.schema.clone();
    let exec_ctx_join = exec_ctx.clone();
    let unsorted_stream = exec_ctx
        .with_new_output_schema(augmented_schema)
        .output_with_sender("HybridJoin", move |collector| {
            run_hybrid_join(
                probed_plan,
                built,
                join_params,
                broadcast_side,
                map_keys,
                probe_keys,
                true,
                exec_ctx_join,
                probed_side_hash_time,
                probed_side_search_time,
                probed_side_compare_time,
                build_output_time,
                collector,
            )
        });

    let unsorted_exec = create_record_batch_stream_exec(unsorted_stream, exec_ctx.partition_id())?;
    let row_num_col: PhysicalExprRef = Arc::new(Column::new(ROW_NUM_COLUMN_NAME, row_num_col_idx));
    let sort_exec = create_default_ascending_sort_exec(
        unsorted_exec,
        &[row_num_col],
        Some(exec_ctx.execution_plan_metrics().clone()),
        false, // we record our own reserve_order_sort_time metric below
    );

    // sort by ~ROW_NUM, drop that column, and forward to `sender`.
    let reserve_order_sort_time = exec_ctx.register_timer_metric("reserve_order_sort_time");
    let exec_ctx_sort = exec_ctx.clone();
    reserve_order_sort_time
        .with_timer_async(async move {
            let mut sorted_stream =
                sort_exec.execute(exec_ctx_sort.partition_id(), exec_ctx_sort.task_ctx())?;
            while let Some(sorted_batch) = sorted_stream.next().await.transpose()? {
                let data_columns: Vec<_> = sorted_batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| *idx != row_num_col_idx)
                    .map(|(_, col)| col.clone())
                    .collect();
                sender
                    .send(RecordBatch::try_new(output_schema.clone(), data_columns)?)
                    .await;
            }
            Result::<()>::Ok(())
        })
        .await
}

/// Probe half of a hybrid join: buckets the probe side, then joins each
/// build-side bucket group against only the probe rows in its bucket range.
/// Batches carry the `~ROW_NUM` column when `required_reserved_order` is set.
async fn run_hybrid_join(
    probed_plan: Arc<dyn ExecutionPlan>,
    mut built: SendableRecordBatchStream,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    map_keys: Vec<PhysicalExprRef>,
    probe_keys: Vec<PhysicalExprRef>,
    required_reserved_order: bool,
    exec_ctx: Arc<ExecutionContext>,
    probed_side_hash_time: Time,
    probed_side_search_time: Time,
    probed_side_compare_time: Time,
    build_output_time: Time,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    // hash-map-batch schema (data columns + ~TABLE) for the empty
    // JoinHashMap used by the probe-only tail group.
    let hash_map_schema = built.schema();

    // Append ~ROW_NUM (when preserving order) and ~BUCKET to the probe side,
    // then group by ~BUCKET with a spill-capable radix repartitioner
    // (`BucketRepartitioner`): the join needs rows grouped by bucket, not
    // globally ordered.
    let probed_schema = probed_plan.schema();
    let probe_bucket_col_idx =
        probed_schema.fields().len() + if required_reserved_order { 1 } else { 0 };
    let bucket_schema_with_row_num = if required_reserved_order {
        let mut fields = probed_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.push(row_num_field());
        bucket_schema(&Arc::new(Schema::new(fields)))
    } else {
        bucket_schema(&probed_schema)
    };
    let mut next_row_num = 0u64;
    let mut bucketed_probe_input = exec_ctx.execute(&probed_plan)?.map(move |batch| {
        batch.and_then(|batch| {
            let batch = if required_reserved_order {
                append_row_num_column(batch, &mut next_row_num)?
            } else {
                batch
            };
            append_bucket_id_column(batch, &probe_keys)
        })
    });
    let probe_bucket_repartitioner = Arc::new(BucketRepartitioner::new(
        exec_ctx.clone(),
        bucket_schema_with_row_num,
        probe_bucket_col_idx,
        NUM_BUCKETS as usize,
    ));
    MemManager::register_consumer(probe_bucket_repartitioner.clone(), true);
    while let Some(batch) = bucketed_probe_input.next().await.transpose()? {
        probe_bucket_repartitioner.insert_batch(batch).await?;
    }
    let mut probe_sorted = probe_bucket_repartitioner.into_stream().await?;

    let mut pending_probe: Option<RecordBatch> = None;
    let mut probe_exhausted = false;
    // Accumulate hash-map-batch slices for the current group. Slices come back
    // as `(bucket, slice)` pairs from `split_hash_map_batch_by_group`: -1 means
    // mid-group, >= 0 closes the group with that bucket id (a sentinel row,
    // always the last row of some coalesced batch).
    let mut current_group_batches: Vec<RecordBatch> = vec![];
    let mut current_group_bucket: Option<i32> = None;
    let mut pending_runs: VecDeque<(i32, RecordBatch)> = VecDeque::new();
    let mut built_exhausted = false;

    loop {
        // pull built batches until a complete group (a run with bucket >= 0)
        // is available or the stream is exhausted
        while !built_exhausted && !pending_runs.iter().any(|(b, _)| *b >= 0) {
            match built.next().await.transpose()? {
                Some(batch) => pending_runs.extend(split_hash_map_batch_by_group(batch)?),
                None => built_exhausted = true,
            }
        }

        // drain runs into the current group until a sentinel
        while let Some((bucket, slice)) = pending_runs.pop_front() {
            if bucket < 0 {
                current_group_batches.push(slice);
            } else {
                current_group_batches.push(slice);
                current_group_bucket = Some(bucket);
                break;
            }
        }

        let Some(group_max_bucket) = current_group_bucket.take() else {
            break;
        };

        let group_batches = std::mem::take(&mut current_group_batches);

        let total_rows: usize = group_batches.iter().map(|b| b.num_rows()).sum();
        let map = Arc::new(if total_rows == 0 {
            JoinHashMap::create_empty(hash_map_schema.clone(), &map_keys)?
        } else {
            JoinHashMap::load_from_hash_map_batches(group_batches, &map_keys)?
        });
        // NULL-aware anti joins have one key. Its NULLs are in bucket 0,
        // so they are encountered before any probe rows can be emitted.
        if join_params.is_null_aware_anti_join && map.has_null_keys() {
            return Ok(());
        }
        let mut joiner = create_joiner(broadcast_side, join_params.clone(), map, sender.clone());

        // probe data left over from the previous group, if it belongs here
        if let Some(pending) = pending_probe.take() {
            let (lo, hi) =
                split_batch_by_bucket_threshold(&pending, probe_bucket_col_idx, group_max_bucket)?;
            if let Some(lo) = lo {
                joiner
                    .as_mut()
                    .join(
                        remove_last_column(lo),
                        &probed_side_hash_time,
                        &probed_side_search_time,
                        &probed_side_compare_time,
                        &build_output_time,
                    )
                    .await?;
            }
            pending_probe = hi;
        }

        // pull probe batches until we cross into the next group's range
        while pending_probe.is_none() && !probe_exhausted && !joiner.can_early_stop() {
            match probe_sorted.next().await.transpose()? {
                Some(batch) => {
                    let (lo, hi) = split_batch_by_bucket_threshold(
                        &batch,
                        probe_bucket_col_idx,
                        group_max_bucket,
                    )?;
                    if let Some(lo) = lo {
                        joiner
                            .as_mut()
                            .join(
                                remove_last_column(lo),
                                &probed_side_hash_time,
                                &probed_side_search_time,
                                &probed_side_compare_time,
                                &build_output_time,
                            )
                            .await?;
                    }
                    if let Some(hi) = hi {
                        pending_probe = Some(hi);
                    }
                }
                None => {
                    probe_exhausted = true;
                }
            }
        }

        joiner.as_mut().finish(&build_output_time).await?;
        exec_ctx
            .baseline_metrics()
            .record_output(joiner.num_output_rows());
    }

    // virtual tail group: probe rows with no matching build-side group must
    // still be visited once, to preserve outer/anti/existence semantics.
    if pending_probe.is_some() || !probe_exhausted {
        let empty_map = Arc::new(JoinHashMap::create_empty(hash_map_schema, &map_keys)?);
        let mut joiner = create_joiner(broadcast_side, join_params, empty_map, sender);

        if let Some(pending) = pending_probe.take() {
            joiner
                .as_mut()
                .join(
                    remove_last_column(pending),
                    &probed_side_hash_time,
                    &probed_side_search_time,
                    &probed_side_compare_time,
                    &build_output_time,
                )
                .await?;
        }
        while !probe_exhausted && !joiner.can_early_stop() {
            match probe_sorted.next().await.transpose()? {
                Some(batch) => {
                    joiner
                        .as_mut()
                        .join(
                            remove_last_column(batch),
                            &probed_side_hash_time,
                            &probed_side_search_time,
                            &probed_side_compare_time,
                            &build_output_time,
                        )
                        .await?;
                }
                None => probe_exhausted = true,
            }
        }
        joiner.as_mut().finish(&build_output_time).await?;
        exec_ctx
            .baseline_metrics()
            .record_output(joiner.num_output_rows());
    }
    Ok(())
}

const ROW_NUM_COLUMN_NAME: &str = "~ROW_NUM";

fn row_num_field() -> FieldRef {
    Arc::new(Field::new(ROW_NUM_COLUMN_NAME, DataType::UInt64, true))
}

/// Append a monotonically increasing `~ROW_NUM` column, used to recover the
/// probe side's original row order after a hybrid join.
fn append_row_num_column(batch: RecordBatch, next_row_num: &mut u64) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let row_nums: UInt64Array = (*next_row_num..*next_row_num + num_rows as u64).collect();
    *next_row_num += num_rows as u64;

    let mut fields = batch.schema().fields().iter().cloned().collect::<Vec<_>>();
    fields.push(row_num_field());
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        batch
            .columns()
            .iter()
            .cloned()
            .chain(std::iter::once(Arc::new(row_nums) as _))
            .collect(),
    )?)
}

/// Add a `~ROW_NUM` passthrough column to the probe side of `join_params`,
/// returning the augmented params and the column's index in the output schema.
///
/// Placement depends on the output layout. For dual-side joins
/// (Inner/Left/Right/Full) columns are always `[left][right]` regardless of
/// probe side (see `full_join.rs`'s `flush()`), so a left probe must insert
/// `~ROW_NUM` after the left block rather than at the end. For single-side
/// outputs (semi/anti/existence) appending at the end is always right.
fn augment_join_params_with_row_num(
    join_params: &JoinParams,
    probe_side: JoinSide,
) -> (JoinParams, usize) {
    let mut augmented = join_params.clone();
    let is_dual_block =
        !augmented.projection.left.is_empty() && !augmented.projection.right.is_empty();

    match probe_side {
        JoinSide::Left => {
            let probe_col_idx = augmented.left_schema.fields().len();
            let mut fields = augmented
                .left_schema
                .fields()
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            fields.push(row_num_field());
            augmented.left_schema = Arc::new(Schema::new(fields));
            augmented.projection.left.push(probe_col_idx);
            augmented
                .projection
                .left_output
                .push(augmented.projection.left_output.len());
        }
        JoinSide::Right => {
            let probe_col_idx = augmented.right_schema.fields().len();
            let mut fields = augmented
                .right_schema
                .fields()
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            fields.push(row_num_field());
            augmented.right_schema = Arc::new(Schema::new(fields));
            augmented.projection.right.push(probe_col_idx);
            augmented
                .projection
                .right_output
                .push(augmented.projection.right_output.len());
        }
        JoinSide::None => unreachable!(),
    }

    let mut out_fields = augmented
        .projection
        .schema
        .fields()
        .iter()
        .cloned()
        .collect::<Vec<_>>();
    let row_num_col_idx = if is_dual_block && probe_side == JoinSide::Left {
        // left block's original width: projection.left just gained one entry.
        augmented.projection.left.len() - 1
    } else {
        out_fields.len()
    };
    out_fields.insert(row_num_col_idx, row_num_field());
    let augmented_schema = Arc::new(Schema::new(out_fields));
    augmented.projection.schema = augmented_schema.clone();
    augmented.output_schema = augmented_schema;
    (augmented, row_num_col_idx)
}

async fn execute_join(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_params: JoinParams,
    broadcast_side: JoinSide,
    cached_build_hash_map_id: Option<String>,
    is_built: bool,
    required_reserved_order: bool,
    exec_ctx: Arc<ExecutionContext>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let build_time = exec_ctx.register_timer_metric("build_hash_map_time");
    let probed_side_hash_time = exec_ctx.register_timer_metric("probed_side_hash_time");
    let probed_side_search_time = exec_ctx.register_timer_metric("probed_side_search_time");
    let probed_side_compare_time = exec_ctx.register_timer_metric("probed_side_compare_time");
    let build_output_time = exec_ctx.register_timer_metric("build_output_time");

    let (probed_plan, built_plan) = match broadcast_side {
        JoinSide::Left => (right, left),
        JoinSide::Right => (left, right),
        JoinSide::None => unreachable!(),
    };
    let map_keys = match broadcast_side {
        JoinSide::Left => join_params.left_keys.clone(),
        JoinSide::Right => join_params.right_keys.clone(),
        JoinSide::None => unreachable!(),
    };
    let probe_keys = match broadcast_side {
        JoinSide::Left => join_params.right_keys.clone(),
        JoinSide::Right => join_params.left_keys.clone(),
        JoinSide::None => unreachable!(),
    };

    let built_input = if is_built {
        exec_ctx.stat_input(exec_ctx.execute(&built_plan)?)
    } else {
        let data_input = exec_ctx.stat_input(exec_ctx.execute(&built_plan)?);
        let built_schema = join_hash_map_schema(&data_input.schema());
        execute_build_hash_map(
            data_input,
            map_keys.clone(),
            exec_ctx.with_new_output_schema(built_schema),
            build_time.clone(),
        )?
    };
    let built_collected = collect_join_hash_map(
        Box::pin(built_input.peekable()),
        cached_build_hash_map_id.filter(|_| is_built),
        &map_keys,
        build_time,
    )
    .await?;

    match built_collected {
        CollectJoinHashMapResult::Map(map) => {
            let join_with_map = execute_join_with_map(
                probed_plan,
                map,
                join_params,
                broadcast_side,
                exec_ctx,
                probed_side_hash_time,
                probed_side_search_time,
                probed_side_compare_time,
                build_output_time,
                sender,
            );
            join_with_map.await?;
        }
        CollectJoinHashMapResult::BucketedGroups(stream) => {
            let built_input = Box::pin(RecordBatchStreamAdapter::new(
                stream.get_ref().schema(),
                stream,
            ));
            let join_with_hybrid_join = execute_join_with_hybrid_join(
                probed_plan,
                built_input,
                join_params,
                broadcast_side,
                map_keys,
                probe_keys,
                required_reserved_order,
                exec_ctx,
                probed_side_hash_time,
                probed_side_search_time,
                probed_side_compare_time,
                build_output_time,
                sender,
            );
            join_with_hybrid_join.await?;
        }
    }
    Ok(())
}

enum CollectJoinHashMapResult {
    Map(Arc<JoinHashMap>),
    BucketedGroups(Pin<Box<Peekable<SendableRecordBatchStream>>>),
}

async fn collect_join_hash_map(
    input: Pin<Box<Peekable<SendableRecordBatchStream>>>,
    cached_build_hash_map_id: Option<String>,
    key_exprs: &[PhysicalExprRef],
    build_time: Time,
) -> Result<CollectJoinHashMapResult> {
    Ok(match cached_build_hash_map_id {
        Some(cached_id) => {
            get_cached_join_hash_map(&cached_id, || async {
                collect_join_hash_map_without_caching(input, key_exprs, build_time).await
            })
            .await?
        }
        None => collect_join_hash_map_without_caching(input, key_exprs, build_time).await?,
    })
}

async fn collect_join_hash_map_without_caching(
    mut input: Pin<Box<Peekable<SendableRecordBatchStream>>>,
    key_exprs: &[PhysicalExprRef],
    build_time: Time,
) -> Result<CollectJoinHashMapResult> {
    let hash_map_schema = input.get_ref().schema();
    // The build exec's schema is always `data columns + ~TABLE` whether or not
    // it fell back to hybrid join, so detect the fallback by decoding the
    // bucket-group id that `JoinHashMap::into_hash_map_batches` packs into
    // `~TABLE`'s metadata. `peek_is_hybrid_join` answers from just row 0 of one
    // batch, so it works even when the first group spans multiple batches.
    let is_bucketed_group_join = if let Some(Ok(batch)) = input.as_mut().peek().await {
        peek_is_hybrid_join(batch)?
    } else {
        false
    };
    if is_bucketed_group_join {
        return Ok(CollectJoinHashMapResult::BucketedGroups(input));
    }

    let hash_map_batches: Vec<RecordBatch> = input.try_collect().await?;
    build_time.with_timer(|| {
        let total_rows: usize = hash_map_batches.iter().map(|b| b.num_rows()).sum();
        let join_hash_map = if total_rows == 0 {
            JoinHashMap::create_empty(hash_map_schema, key_exprs)?
        } else {
            // Logically one group, but IpcReaderExec may still emit several
            // physical batches (it coalesces to a threshold, never to one).
            JoinHashMap::load_from_hash_map_batches(hash_map_batches, key_exprs)?
        };
        Ok(CollectJoinHashMapResult::Map(Arc::new(join_hash_map)))
    })
}

#[async_trait]
#[allow(
    clippy::double_must_use,
    reason = "async_trait adds #[must_use] to methods returning futures"
)]
pub trait Joiner {
    async fn join(
        self: Pin<&mut Self>,
        probed_batch: RecordBatch,
        probed_side_hash_time: &Time,
        probed_side_search_time: &Time,
        probed_side_compare_time: &Time,
        build_output_time: &Time,
    ) -> Result<()>;

    async fn finish(self: Pin<&mut Self>, build_output_time: &Time) -> Result<()>;

    fn can_early_stop(&self) -> bool {
        false
    }

    fn num_output_rows(&self) -> usize;
}

async fn get_cached_join_hash_map<Fut: Future<Output = Result<CollectJoinHashMapResult>> + Send>(
    cached_id: &str,
    init: impl FnOnce() -> Fut,
) -> Result<CollectJoinHashMapResult> {
    type Slot = Arc<tokio::sync::Mutex<Weak<JoinHashMap>>>;
    static CACHED_JOIN_HASH_MAP: OnceCell<Arc<Mutex<HashMap<String, Slot>>>> = OnceCell::new();

    // remove expire keys and insert new key
    let slot = {
        let cached_join_hash_map = CACHED_JOIN_HASH_MAP.get_or_init(|| Arc::default());
        let mut cached_join_hash_map = cached_join_hash_map.lock();

        cached_join_hash_map.retain(|_, v| Arc::strong_count(v) > 0);
        cached_join_hash_map
            .entry(cached_id.to_string())
            .or_default()
            .clone()
    };

    let mut slot = slot.lock().await;
    if let Some(cached) = slot.upgrade() {
        log::info!("got cached broadcast join hash map: ${cached_id}");
        Ok(CollectJoinHashMapResult::Map(cached))
    } else {
        log::info!("collecting broadcast join hash map: ${cached_id}");
        match init().await? {
            CollectJoinHashMapResult::Map(map) => {
                *slot = Arc::downgrade(&map);
                Ok(CollectJoinHashMapResult::Map(map))
            }
            CollectJoinHashMapResult::BucketedGroups(stream) => {
                Ok(CollectJoinHashMapResult::BucketedGroups(stream))
            }
        }
    }
}

/// Fuzz test comparing hybrid join (forced into many tiny groups via a 1-byte
/// group threshold) against a plain in-memory hash join over the same random
/// inputs, across all join types and key distributions (duplicates, nulls,
/// one-sided keys).
#[cfg(test)]
mod fuzztest {
    use arrow::{
        array::{Array, ArrayRef, Int32Array},
        datatypes::{Field, Schema},
    };
    use auron_memmgr::MemManager;
    use datafusion::{
        common::ScalarValue,
        physical_plan::{common::collect, joins::utils::build_join_schema, test::TestMemoryExec},
        prelude::SessionContext,
    };
    use datafusion_ext_commons::arrow::coalesce::coalesce_batches_unchecked;
    use rand::{Rng, RngExt, SeedableRng};

    use super::*;
    use crate::joins::bucketed_group::{BucketGroupBuilder, append_bucket_id_column};

    fn gen_batch(prefix: &str, rng: &mut impl Rng, n: usize, key_range: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}k"), DataType::Int32, true),
            Field::new(format!("{prefix}v"), DataType::Int32, true),
        ]));
        let keys: Vec<Option<i32>> = (0..n)
            .map(|_| {
                if rng.random_bool(0.1) {
                    None
                } else {
                    Some(rng.random_range(0..key_range))
                }
            })
            .collect();
        let values: Vec<Option<i32>> = (0..n).map(|_| Some(rng.random_range(0..1000))).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(keys)) as ArrayRef,
                Arc::new(Int32Array::from(values)),
            ],
        )
        .expect("valid fuzz batch")
    }

    fn memory(batch: RecordBatch) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            TestMemoryExec::try_new(&[vec![batch.clone()]], batch.schema(), None)
                .expect("valid memory exec"),
        )
    }

    /// Partition `build_batch` into bucketed-group hash-map-batches with a tiny
    /// memory threshold, forcing many small (and, since the threshold is tiny
    /// relative to any real row, almost always multi-batch) groups to stress
    /// the per-group probe-splitting logic.
    fn build_bucketed_group_batches(
        build_batch: RecordBatch,
        key: PhysicalExprRef,
        threshold: usize,
    ) -> Result<Vec<RecordBatch>> {
        let bucketed = append_bucket_id_column(build_batch.clone(), std::slice::from_ref(&key))?;
        let bucket_col_idx = bucketed.num_columns() - 1;
        let sort_indices =
            arrow::compute::sort_to_indices(bucketed.column(bucket_col_idx), None, None)?;
        let bucketed = arrow::compute::take_record_batch(&bucketed, &sort_indices)?;

        let mut builder = BucketGroupBuilder::new(threshold, vec![key], build_batch.schema());
        let mut groups = builder.push(bucketed)?;
        groups.extend(builder.finish()?);
        Ok(groups)
    }

    fn build_bucketed_groups(
        build_batch: RecordBatch,
        key: PhysicalExprRef,
        threshold: usize,
        simulate_reader_coalesce: bool,
        rng: &mut impl Rng,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let hash_map_schema = join_hash_map_schema(&build_batch.schema());
        let mut groups = build_bucketed_group_batches(build_batch.clone(), key, threshold)?;
        if simulate_reader_coalesce {
            groups = simulate_ipc_reader_coalesce(groups, hash_map_schema.clone(), rng);
        }
        Ok(Arc::new(TestMemoryExec::try_new(
            &[groups],
            hash_map_schema,
            None,
        )?))
    }

    /// Re-chunk hash-map-batches with different, row-count-based boundaries that
    /// freely merge rows from adjacent logical groups into one physical batch -
    /// what `IpcReaderExec` does to a broadcast exchange's stream on the read
    /// side. This is the scenario the sentinel-row encoding exists to survive
    /// and that `build_bucketed_groups`'s original batch boundaries miss.
    fn simulate_ipc_reader_coalesce(
        groups: Vec<RecordBatch>,
        schema: SchemaRef,
        rng: &mut impl Rng,
    ) -> Vec<RecordBatch> {
        if groups.is_empty() {
            return groups;
        }
        let total_rows: usize = groups.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return groups;
        }
        let mut flat: Vec<RecordBatch> = groups;
        let mut out = vec![];
        let mut staging: Vec<RecordBatch> = vec![];
        let mut staging_rows = 0usize;
        // random coalesce threshold, deliberately small enough to force
        // multiple merge points across many groups' boundaries.
        let batch_size_threshold = rng.random_range(1..=total_rows.max(1));
        for batch in flat.drain(..) {
            if batch.num_rows() == 0 {
                continue;
            }
            staging_rows += batch.num_rows();
            staging.push(batch);
            if staging_rows >= batch_size_threshold {
                out.push(coalesce_batches_unchecked(schema.clone(), &staging));
                staging.clear();
                staging_rows = 0;
            }
        }
        if !staging.is_empty() {
            out.push(coalesce_batches_unchecked(schema.clone(), &staging));
        }
        out
    }

    fn to_sorted_rows(batches: Vec<RecordBatch>) -> Vec<Vec<String>> {
        let mut rows = vec![];
        for batch in batches {
            for i in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|col| {
                            ScalarValue::try_from_array(col, i)
                                .expect("valid scalar")
                                .to_string()
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        rows.sort();
        rows
    }

    async fn run_one(seed: u64, simulate_reader_coalesce: bool) -> Result<()> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let left_n = rng.random_range(0..40);
        let right_n = rng.random_range(0..40);
        let key_range = rng.random_range(1..6);
        let left_batch = gen_batch("l", &mut rng, left_n, key_range);
        let right_batch = gen_batch("r", &mut rng, right_n, key_range);

        let join_types = [
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::LeftSemi,
            JoinType::LeftAnti,
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::Existence,
        ];
        let jt = join_types[rng.random_range(0..join_types.len())];
        let build_left = rng.random_bool(0.5);
        let broadcast_side = if build_left {
            JoinSide::Left
        } else {
            JoinSide::Right
        };

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
        let on: JoinOn = vec![(
            Arc::new(Column::new("lk", 0)) as PhysicalExprRef,
            Arc::new(Column::new("rk", 0)) as PhysicalExprRef,
        )];

        // reference result: no fallback, a single in-memory JoinHashMap.
        let join0 = BroadcastJoinExec::try_new(
            schema.clone(),
            memory(left_batch.clone()),
            memory(right_batch.clone()),
            on.clone(),
            jt,
            broadcast_side,
            false,
            None,
            false,
            None,
        )?;
        let out0 = collect(join0.execute(0, SessionContext::new().task_ctx())?).await?;

        // hybrid join result: build side pre-partitioned into many tiny
        // bucketed groups.
        let key = if build_left {
            Arc::new(Column::new("lk", 0)) as PhysicalExprRef
        } else {
            Arc::new(Column::new("rk", 0)) as PhysicalExprRef
        };
        let build_batch = if build_left {
            left_batch.clone()
        } else {
            right_batch.clone()
        };
        let built = build_bucketed_groups(
            build_batch.clone(),
            key.clone(),
            1,
            simulate_reader_coalesce,
            &mut rng,
        )?;
        let (left1, right1) = if build_left {
            (built, memory(right_batch.clone()))
        } else {
            (memory(left_batch.clone()), built)
        };
        let join1 = BroadcastJoinExec::try_new(
            schema,
            left1,
            right1,
            on,
            jt,
            broadcast_side,
            true,
            None,
            false,
            None,
        )?;
        let out1 = collect(join1.execute(0, SessionContext::new().task_ctx())?).await?;

        assert_eq!(
            to_sorted_rows(out0),
            to_sorted_rows(out1),
            "seed={seed} jt={jt:?} build_left={build_left} left_n={left_n} right_n={right_n} \
             key_range={key_range}",
        );
        Ok(())
    }

    #[tokio::test]
    async fn null_aware_anti_join_build_nulls() -> Result<()> {
        MemManager::init(10_000_000);
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, true)]));
        let key = Arc::new(Column::new("k", 0)) as PhysicalExprRef;
        let batch = |values: Vec<Option<i32>>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])
        };
        let probe = batch(vec![Some(1), None, Some(2), Some(3)])?;
        for (values, expected) in [
            (vec![Some(1), None], vec![]),
            (vec![None], vec![]),
            (vec![Some(1)], vec![Some(2), Some(3)]),
            (vec![], vec![Some(1), None, Some(2), Some(3)]),
        ] {
            let build = batch(values.clone())?;
            for path in ["direct", "hash map", "hybrid"] {
                let built = if path == "direct" {
                    memory(build.clone())
                } else {
                    let groups = if path == "hybrid" {
                        build_bucketed_group_batches(build.clone(), key.clone(), 1)?
                    } else {
                        JoinHashMap::create_from_data_batch(
                            build.clone(),
                            std::slice::from_ref(&key),
                        )?
                        .into_hash_map_batches(None)?
                    };
                    Arc::new(TestMemoryExec::try_new(
                        &[groups],
                        join_hash_map_schema(&schema),
                        None,
                    )?) as Arc<dyn ExecutionPlan>
                };
                let join = BroadcastJoinExec::try_new(
                    schema.clone(),
                    memory(probe.clone()),
                    built,
                    vec![(key.clone(), key.clone())],
                    JoinType::LeftAnti,
                    JoinSide::Right,
                    path != "direct",
                    None,
                    true,
                    None,
                )?;
                let output = collect(join.execute(0, SessionContext::new().task_ctx())?).await?;
                assert_eq!(
                    to_sorted_rows(output),
                    to_sorted_rows(vec![batch(expected.clone())?]),
                    "build={values:?}, path={path}",
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn fuzztest_hybrid_join_matches_direct_hash_join() -> Result<()> {
        MemManager::init(10_000_000);
        for seed in 0..300u64 {
            run_one(seed, false).await?;
        }
        Ok(())
    }

    /// Like `fuzztest_hybrid_join_matches_direct_hash_join`, but re-chunks the
    /// build batches across their original group boundaries first (see
    /// `simulate_ipc_reader_coalesce`), stressing `split_hash_map_batch_by_group`
    /// / `peek_is_hybrid_join` / the sentinel-row encoding.
    #[tokio::test]
    async fn fuzztest_hybrid_join_with_coalesced_build_batches_matches_direct_hash_join()
    -> Result<()> {
        MemManager::init(10_000_000);
        for seed in 0..300u64 {
            run_one(seed, true).await?;
        }
        Ok(())
    }

    /// Unlike `to_sorted_rows`, keeps the batches' original row order, so
    /// callers can assert on exact output ordering (not just set equality).
    fn to_rows(batches: Vec<RecordBatch>) -> Vec<Vec<String>> {
        let mut rows = vec![];
        for batch in batches {
            for i in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|col| {
                            ScalarValue::try_from_array(col, i)
                                .expect("valid scalar")
                                .to_string()
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        rows
    }

    /// Like `gen_batch`, but with an extra leading `{prefix}id` column of
    /// sequential row ids, so `run_one_reserved_order` can tell which probe
    /// row each output row came from even when keys/values duplicate.
    fn gen_batch_with_id(
        prefix: &str,
        rng: &mut impl Rng,
        n: usize,
        key_range: i32,
    ) -> RecordBatch {
        let batch = gen_batch(prefix, rng, n, key_range);
        let ids: ArrayRef = Arc::new(Int32Array::from((0..n as i32).collect::<Vec<_>>()));
        let mut fields = vec![Arc::new(Field::new(
            format!("{prefix}id"),
            DataType::Int32,
            false,
        ))];
        fields.extend(batch.schema().fields().iter().cloned());
        let mut columns = vec![ids];
        columns.extend(batch.columns().iter().cloned());
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("valid id batch")
    }

    /// Flatten column `name` across all batches into `Vec<Option<i32>>`
    /// (`None` for SQL NULL), in batch/row order.
    fn extract_i32_col(batches: &[RecordBatch], name: &str) -> Vec<Option<i32>> {
        let mut out = vec![];
        for batch in batches {
            let idx = batch.schema().index_of(name).expect("column must exist");
            let col = batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id column must be Int32Array");
            for i in 0..col.len() {
                out.push(if col.is_null(i) {
                    None
                } else {
                    Some(col.value(i))
                });
            }
        }
        out
    }

    /// Verify that a forced hybrid join with `required_reserved_order` preserves
    /// the probe side's row order, against a plain in-memory hash join as the
    /// reference. Only join-type/broadcast-side combos where the probe side
    /// drives output order are exercised.
    ///
    /// Checked invariant: grouping consecutive output rows by probe row id, the
    /// id sequence must match the reference and each group must hold the same
    /// multiset of rows. Ties among multiple build matches for one probe row are
    /// unordered (hash map iteration order differs between the reference's
    /// single map and the hybrid join's per-group maps); unmatched build-only
    /// rows are compared as an unordered tail.
    async fn run_one_reserved_order(seed: u64, simulate_reader_coalesce: bool) -> Result<()> {
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        let left_n = rng.random_range(1..40);
        let right_n = rng.random_range(1..40);
        let key_range = rng.random_range(1..6);
        let left_batch = gen_batch_with_id("l", &mut rng, left_n, key_range);
        let right_batch = gen_batch_with_id("r", &mut rng, right_n, key_range);

        // (join_type, build_left) combinations where the probe side (the
        // side opposite of `build_left`) actually drives the output row
        // order, i.e. where "preserving probe order" is a well-defined
        // property to assert on.
        let candidates: [(JoinType, bool); 13] = [
            (JoinType::Inner, true),
            (JoinType::Inner, false),
            (JoinType::Left, true),
            (JoinType::Left, false),
            (JoinType::Right, true),
            (JoinType::Right, false),
            (JoinType::Full, true),
            (JoinType::Full, false),
            (JoinType::LeftSemi, false),
            (JoinType::LeftAnti, false),
            (JoinType::RightSemi, true),
            (JoinType::RightAnti, true),
            (JoinType::Existence, false),
        ];
        let (jt, build_left) = candidates[rng.random_range(0..candidates.len())];
        let broadcast_side = if build_left {
            JoinSide::Left
        } else {
            JoinSide::Right
        };
        let probe_id_col = if build_left { "rid" } else { "lid" };

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
        let on: JoinOn = vec![(
            Arc::new(Column::new("lk", 1)) as PhysicalExprRef,
            Arc::new(Column::new("rk", 1)) as PhysicalExprRef,
        )];

        // reference result: no fallback, output already follows the probe
        // side's original row order for the selected combinations above.
        let join0 = BroadcastJoinExec::try_new(
            schema.clone(),
            memory(left_batch.clone()),
            memory(right_batch.clone()),
            on.clone(),
            jt,
            broadcast_side,
            false,
            None,
            false,
            None,
        )?;
        let out0 = collect(join0.execute(0, SessionContext::new().task_ctx())?).await?;

        // forced hybrid join result, with required_reserved_order set.
        let key = if build_left {
            Arc::new(Column::new("lk", 1)) as PhysicalExprRef
        } else {
            Arc::new(Column::new("rk", 1)) as PhysicalExprRef
        };
        let build_batch = if build_left {
            left_batch.clone()
        } else {
            right_batch.clone()
        };
        let built = build_bucketed_groups(build_batch, key, 1, simulate_reader_coalesce, &mut rng)?;
        let (left1, right1) = if build_left {
            (built, memory(right_batch.clone()))
        } else {
            (memory(left_batch.clone()), built)
        };
        let join1 = BroadcastJoinExec::try_new(
            schema,
            left1,
            right1,
            on,
            jt,
            broadcast_side,
            true,
            None,
            false,
            None,
        )?
        .with_required_reserved_order(true);
        let out1 = collect(join1.execute(0, SessionContext::new().task_ctx())?).await?;

        let msg = || {
            format!(
                "seed={seed} jt={jt:?} build_left={build_left} left_n={left_n} right_n={right_n} \
                 key_range={key_range}"
            )
        };

        let probe_ids0 = extract_i32_col(&out0, probe_id_col);
        let probe_ids1 = extract_i32_col(&out1, probe_id_col);
        let rows0 = to_rows(out0);
        let rows1 = to_rows(out1);
        assert_eq!(rows0.len(), rows1.len(), "{}", msg());

        // split each output into (probe-driven groups in order, unordered
        // build-only tail), keyed by consecutive runs of `probe_id_col`.
        type ProbeGroup = (i32, Vec<Vec<String>>);
        fn split_groups(
            probe_ids: &[Option<i32>],
            rows: &[Vec<String>],
        ) -> (Vec<ProbeGroup>, Vec<Vec<String>>) {
            let mut groups: Vec<ProbeGroup> = vec![];
            let mut tail: Vec<Vec<String>> = vec![];
            for (pid, row) in probe_ids.iter().zip(rows.iter()) {
                match pid {
                    Some(pid) => match groups.last_mut() {
                        Some((last_pid, group)) if last_pid == pid => group.push(row.clone()),
                        _ => groups.push((*pid, vec![row.clone()])),
                    },
                    None => tail.push(row.clone()),
                }
            }
            (groups, tail)
        }

        let (groups0, mut tail0) = split_groups(&probe_ids0, &rows0);
        let (groups1, mut tail1) = split_groups(&probe_ids1, &rows1);

        assert_eq!(
            groups0.iter().map(|(pid, _)| *pid).collect::<Vec<_>>(),
            groups1.iter().map(|(pid, _)| *pid).collect::<Vec<_>>(),
            "probe row id sequence mismatch: {}",
            msg()
        );
        for ((pid0, mut g0), (pid1, mut g1)) in groups0.into_iter().zip(groups1) {
            assert_eq!(pid0, pid1);
            g0.sort();
            g1.sort();
            assert_eq!(g0, g1, "probe row id={pid0}: {}", msg());
        }
        tail0.sort();
        tail1.sort();
        assert_eq!(tail0, tail1, "build-only tail: {}", msg());
        Ok(())
    }

    #[tokio::test]
    async fn fuzztest_hybrid_join_reserved_order_matches_probe_order() -> Result<()> {
        MemManager::init(10_000_000);
        for seed in 0..300u64 {
            run_one_reserved_order(seed, false).await?;
        }
        Ok(())
    }

    /// Like `fuzztest_hybrid_join_reserved_order_matches_probe_order`, but with
    /// the build batches re-chunked across their original group boundaries (see
    /// `simulate_ipc_reader_coalesce`) - the hybrid + reserved-order +
    /// reader-coalesced combination seen to misbehave in production.
    #[tokio::test]
    async fn fuzztest_hybrid_join_reserved_order_with_coalesced_build_batches_matches_probe_order()
    -> Result<()> {
        MemManager::init(10_000_000);
        for seed in 0..300u64 {
            run_one_reserved_order(seed, true).await?;
        }
        Ok(())
    }
}
