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
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Weak},
    task::{Context, Poll, ready},
    time::Instant,
};

use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    row::{RowConverter, SortField},
};
use arrow_schema::Schema;
use auron_jni_bridge::{conf, conf::BooleanConf, is_task_running};
use auron_memmgr::metrics::SpillMetrics;
use datafusion::{
    common::{DataFusionError, Result},
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        ExecutionPlan,
        metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, Time},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_ext_commons::{
    arrow::{array_size::BatchSize, coalesce::BatchCoalescer},
    batch_size, df_execution_err, downcast_any, downcast_any_dyn, suggested_batch_mem_size,
};
use futures::{Stream, StreamExt};
use futures_util::{FutureExt, stream::BoxStream};
use log::debug;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use tokio::{
    sync::mpsc::{Receiver, Sender},
    task::JoinSet,
};

use crate::{
    broadcast_join_exec::BroadcastJoinExec,
    common::{
        column_pruning::extend_projection_by_expr,
        lazy::{
            LazyBatchRef, LazyBatchStreamAdapter, SendableLazyBatchStream, evaluate_columns,
            wrap_stream,
        },
        optimized_batch::{
            ExecuteOptimizedProps, KeyRows, OptimizedBatch, OptimizedBatchStreamAdapter,
            SendableOptimizedBatchStream,
        },
        timer_helper::TimerHelper,
    },
    filter_exec::FilterExec,
    flink::kafka_scan_exec::KafkaScanExec,
    project_exec::ProjectExec,
    sort_exec::SortExec,
    sort_merge_join_exec::SortMergeJoinExec,
};

pub struct ExecutionContext {
    task_ctx: Arc<TaskContext>,
    partition_id: usize,
    output_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    baseline_metrics: BaselineMetrics,
    spill_metrics: Arc<OnceCell<SpillMetrics>>,
    input_stat_metrics: Arc<OnceCell<Option<InputBatchStatistics>>>,
}

#[allow(clippy::panic)] // Temporarily allow panic to refactor to Result later
impl ExecutionContext {
    pub fn new(
        task_ctx: Arc<TaskContext>,
        partition_id: usize,
        output_schema: SchemaRef,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Arc<Self> {
        Arc::new(Self {
            task_ctx,
            partition_id,
            output_schema,
            baseline_metrics: BaselineMetrics::new(&metrics, partition_id),
            metrics: metrics.clone(),
            spill_metrics: Arc::default(),
            input_stat_metrics: Arc::default(),
        })
    }

    pub fn with_new_output_schema(&self, output_schema: SchemaRef) -> Arc<Self> {
        Arc::new(Self {
            task_ctx: self.task_ctx.clone(),
            partition_id: self.partition_id,
            output_schema,
            metrics: self.metrics.clone(),
            baseline_metrics: self.baseline_metrics.clone(),
            spill_metrics: self.spill_metrics.clone(),
            input_stat_metrics: self.input_stat_metrics.clone(),
        })
    }

    pub fn task_ctx(&self) -> Arc<TaskContext> {
        self.task_ctx.clone()
    }

    pub fn partition_id(&self) -> usize {
        self.partition_id
    }

    pub fn output_schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    pub fn execution_plan_metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    pub fn baseline_metrics(&self) -> &BaselineMetrics {
        &self.baseline_metrics
    }

    pub fn spill_metrics(&self) -> &SpillMetrics {
        self.spill_metrics
            .get_or_init(|| SpillMetrics::new(&self.metrics, self.partition_id))
    }

    pub fn register_timer_metric(&self, name: &str) -> Time {
        MetricBuilder::new(self.execution_plan_metrics())
            .subset_time(name.to_owned(), self.partition_id)
    }

    pub fn register_counter_metric(&self, name: &str) -> Count {
        MetricBuilder::new(self.execution_plan_metrics())
            .counter(name.to_owned(), self.partition_id)
    }

    pub fn coalesce_with_default_batch_size(
        self: &Arc<Self>,
        input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        struct CoalesceStream {
            input: SendableRecordBatchStream,
            coalescer: BatchCoalescer,
            input_exhausted: bool,
            elapsed_compute: Time,
        }

        impl RecordBatchStream for CoalesceStream {
            fn schema(&self) -> SchemaRef {
                self.coalescer.schema()
            }
        }

        impl Stream for CoalesceStream {
            type Item = Result<RecordBatch>;

            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
                loop {
                    if let Some(batch) = self.coalescer.next_completed_batch() {
                        let elapsed = self.elapsed_compute.clone();
                        let _timer = elapsed.timer();
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    if self.input_exhausted {
                        return Poll::Ready(None);
                    }
                    match ready!(self.input.as_mut().poll_next_unpin(cx)).transpose()? {
                        Some(batch) => {
                            let elapsed = self.elapsed_compute.clone();
                            let _timer = elapsed.timer();
                            if batch.num_rows() == 0 {
                                continue;
                            }
                            // Avoid copying sufficiently large batches, preserving input order.
                            if !self.coalescer.has_buffered_batches()
                                && (batch.num_rows() > batch_size() / 4
                                    || batch.get_batch_mem_size() >= suggested_batch_mem_size() / 4)
                            {
                                return Poll::Ready(Some(Ok(batch)));
                            }
                            self.coalescer.push_batch(batch)?;
                        }
                        None => {
                            let elapsed = self.elapsed_compute.clone();
                            let _timer = elapsed.timer();
                            self.input_exhausted = true;
                            self.coalescer.finish_buffered_batch()?;
                        }
                    }
                }
            }
        }

        let schema = input.schema();
        Box::pin(CoalesceStream {
            input,
            coalescer: BatchCoalescer::new_with_default_batch_size(schema),
            input_exhausted: false,
            elapsed_compute: self.baseline_metrics().elapsed_compute().clone(),
        })
    }

    pub fn execute_optimized(
        self: &Arc<Self>,
        input: &Arc<dyn ExecutionPlan>,
        opts: &ExecuteOptimizedProps,
    ) -> Result<SendableOptimizedBatchStream> {
        let stream = if let Some(keys) = opts.key_rows()
            && let Ok(sort) = downcast_any_dyn!(input, SortExec)
            && keys == sort.sort_exprs()
        {
            match opts.projection() {
                Some(projection) => sort.execute_projected_with_key_rows(
                    self.partition_id,
                    self.task_ctx.clone(),
                    projection,
                )?,
                None => sort.execute_with_key_rows(self.partition_id, self.task_ctx.clone())?,
            }
        } else {
            let input_schema = input.schema();
            let output_schema = match opts.projection() {
                Some(projection) => Arc::new(input_schema.project(projection)?),
                None => input_schema.clone(),
            };
            let mut projection = opts.projection().map(<[usize]>::to_vec);
            let mut key_exprs = vec![];
            let converter = opts
                .key_rows()
                .map(|keys| {
                    key_exprs = keys
                        .iter()
                        .map(|key| match &mut projection {
                            Some(projection) => extend_projection_by_expr(projection, &key.expr),
                            None => key.expr.clone(),
                        })
                        .collect();
                    let fields = keys
                        .iter()
                        .map(|key| {
                            Ok(SortField::new_with_options(
                                key.expr.data_type(&input_schema)?,
                                key.options,
                            ))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok::<_, DataFusionError>(Arc::new(RowConverter::new(fields)?))
                })
                .transpose()?;
            let lazy = self.execute_lazy(input, projection.as_deref())?;
            let output_projection = opts
                .projection()
                .map(|projection| (0..projection.len()).collect::<Vec<_>>());
            let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
            Box::pin(OptimizedBatchStreamAdapter::new(
                lazy.map(move |batch| {
                    batch.and_then(|batch| {
                        let _timer = elapsed_compute.timer();
                        let key_rows = converter
                            .as_ref()
                            .map(|converter| {
                                let columns = evaluate_columns(batch.as_ref(), &key_exprs)?;
                                let rows = if key_exprs.is_empty() {
                                    let parser = converter.parser();
                                    let mut rows = converter.empty_rows(batch.num_rows(), 0);
                                    for _ in 0..batch.num_rows() {
                                        rows.push(parser.parse(&[]));
                                    }
                                    rows
                                } else {
                                    converter.convert_columns(&columns)?
                                };
                                Ok::<_, DataFusionError>(KeyRows {
                                    rows: Arc::new(rows),
                                    rows_converter: converter.clone(),
                                })
                            })
                            .transpose()?;
                        let batch = match &output_projection {
                            Some(projection) => batch.project(projection)?,
                            None => batch,
                        };
                        Ok(OptimizedBatch::new(batch, key_rows))
                    })
                }),
                output_schema,
                opts.key_rows().map(<[PhysicalSortExpr]>::to_vec),
            )) as SendableOptimizedBatchStream
        };
        Ok(if opts.input_stats() {
            self.stat_input_optimized(stream)
        } else {
            stream
        })
    }

    fn execute_lazy(
        self: &Arc<Self>,
        input: &Arc<dyn ExecutionPlan>,
        projection: Option<&[usize]>,
    ) -> Result<SendableLazyBatchStream> {
        if let Ok(project) = downcast_any_dyn!(input, ProjectExec) {
            return project.execute_optimized(
                self.partition_id,
                self.task_ctx.clone(),
                projection.map(<[usize]>::to_vec),
            );
        }
        if let Ok(filter) = downcast_any_dyn!(input, FilterExec) {
            return filter.execute_optimized(
                self.partition_id,
                self.task_ctx.clone(),
                projection.map(<[usize]>::to_vec),
            );
        }
        if let Ok(join) = downcast_any_dyn!(input, BroadcastJoinExec) {
            return join.execute_optimized(
                self.partition_id,
                self.task_ctx.clone(),
                projection.map(<[usize]>::to_vec),
            );
        }
        if let Ok(join) = downcast_any_dyn!(input, SortMergeJoinExec) {
            return Ok(wrap_stream(join.execute_optimized(
                self.partition_id,
                self.task_ctx.clone(),
                projection.map(<[usize]>::to_vec),
            )?));
        }
        if let Ok(scan) = downcast_any_dyn!(input, KafkaScanExec) {
            return Ok(wrap_stream(scan.execute_optimized(
                self.partition_id,
                self.task_ctx.clone(),
                projection.map(<[usize]>::to_vec),
            )?));
        }
        let lazy = wrap_stream(input.execute(self.partition_id, self.task_ctx.clone())?);
        Ok(match projection {
            Some(projection) => {
                let schema = Arc::new(input.schema().project(projection)?);
                let projection = projection.to_vec();
                Box::pin(LazyBatchStreamAdapter::new(
                    schema,
                    lazy.map(move |batch| batch.and_then(|batch| batch.project(&projection))),
                ))
            }
            None => lazy,
        })
    }

    fn stat_input_optimized(
        self: &Arc<Self>,
        input: SendableOptimizedBatchStream,
    ) -> SendableOptimizedBatchStream {
        let statistics = self.input_stat_metrics.get_or_init(|| {
            InputBatchStatistics::from_metrics_set_and_auron_conf(
                self.execution_plan_metrics(),
                self.partition_id,
            )
            .expect("error creating input batch statistics")
        });
        if let Some(statistics) = statistics.clone() {
            let schema = input.schema();
            let keys = input.keys().map(<[PhysicalSortExpr]>::to_vec);
            return Box::pin(OptimizedBatchStreamAdapter::new(
                input.inspect(move |result| {
                    if let Ok(batch) = result {
                        let batch = batch.as_lazy_batch();
                        statistics.input_batch_count.add(1);
                        statistics
                            .input_batch_mem_size
                            .add(batch.get_estimated_materialized_mem_size());
                        statistics.input_row_count.add(batch.num_rows());
                    }
                }),
                schema,
                keys,
            ));
        }
        input
    }

    pub fn stat_input_lazy(
        self: &Arc<Self>,
        input: SendableLazyBatchStream,
    ) -> SendableLazyBatchStream {
        let statistics = self.input_stat_metrics.get_or_init(|| {
            InputBatchStatistics::from_metrics_set_and_auron_conf(
                self.execution_plan_metrics(),
                self.partition_id,
            )
            .expect("error creating input batch statistics")
        });
        if let Some(statistics) = statistics.clone() {
            return Box::pin(LazyBatchStreamAdapter::new(
                input.schema(),
                input.inspect(move |result| {
                    if let Ok(batch) = result {
                        statistics.input_batch_count.add(1);
                        statistics
                            .input_batch_mem_size
                            .add(batch.get_estimated_materialized_mem_size());
                        statistics.input_row_count.add(batch.num_rows());
                    }
                }),
            ));
        }
        input
    }

    pub fn stat_input(
        self: &Arc<Self>,
        input: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        let input_batch_statistics = self.input_stat_metrics.get_or_init(|| {
            InputBatchStatistics::from_metrics_set_and_auron_conf(
                self.execution_plan_metrics(),
                self.partition_id,
            )
            .expect("error creating input batch statistics")
        });

        if let Some(input_batch_statistics) = input_batch_statistics.clone() {
            let stat_input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
                input.schema(),
                input.inspect(move |batch_result| {
                    if let Ok(batch) = &batch_result {
                        input_batch_statistics.record_input_batch(batch);
                    }
                }),
            ));
            return stat_input;
        }
        input
    }

    pub fn stream_on_completion(
        self: &Arc<Self>,
        input: SendableRecordBatchStream,
        on_completion: Box<dyn FnOnce() -> Result<()> + Send + 'static>,
    ) -> SendableRecordBatchStream {
        struct CompletionStream {
            input: SendableRecordBatchStream,
            on_completion: Option<Box<dyn FnOnce() -> Result<()> + Send + 'static>>,
        }

        impl RecordBatchStream for CompletionStream {
            fn schema(&self) -> SchemaRef {
                self.input.schema()
            }
        }

        impl Stream for CompletionStream {
            type Item = Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                match ready!(self.as_mut().input.poll_next_unpin(cx)) {
                    Some(r) => Poll::Ready(Some(r)),
                    None => {
                        if let Some(on_completion) = self.as_mut().on_completion.take()
                            && let Err(e) = on_completion()
                        {
                            return Poll::Ready(Some(Err(e)));
                        }
                        Poll::Ready(None)
                    }
                }
            }
        }

        Box::pin(CompletionStream {
            input,
            on_completion: Some(on_completion),
        })
    }

    pub fn output_lazy_with_sender<Fut: Future<Output = Result<()>> + Send>(
        self: &Arc<Self>,
        desc: &'static str,
        output: impl FnOnce(Arc<WrappedLazyBatchSender>) -> Fut + Send + 'static,
    ) -> SendableLazyBatchStream {
        Box::pin(LazyBatchStreamAdapter::new(
            self.output_schema.clone(),
            self.output_with_sender_impl::<LazyBatchRef, _>(desc, output),
        ))
    }

    pub fn output_with_sender<Fut: Future<Output = Result<()>> + Send>(
        self: &Arc<Self>,
        desc: &'static str,
        output: impl FnOnce(Arc<WrappedRecordBatchSender>) -> Fut + Send + 'static,
    ) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            self.output_with_sender_impl::<RecordBatch, _>(desc, output),
        ))
    }

    pub fn output_optimized_with_sender<Fut: Future<Output = Result<()>> + Send>(
        self: &Arc<Self>,
        desc: &'static str,
        keys: &[PhysicalSortExpr],
        output: impl FnOnce(Arc<WrappedOptimizedBatchSender>) -> Fut + Send + 'static,
    ) -> SendableOptimizedBatchStream {
        Box::pin(OptimizedBatchStreamAdapter::new(
            self.output_with_sender_impl::<OptimizedBatch, _>(desc, output),
            self.output_schema.clone(),
            Some(keys.to_vec()),
        ))
    }

    fn output_with_sender_impl<
        T: RecordBatchWithPayload,
        Fut: Future<Output = Result<()>> + Send,
    >(
        self: &Arc<Self>,
        desc: &'static str,
        output: impl FnOnce(Arc<WrappedSender<T>>) -> Fut + Send + 'static,
    ) -> impl Stream<Item = Result<T>> + Send + 'static {
        // ReceiverStreamBuilder is copied from datafusion_physical_plan
        struct ReceiverStreamBuilder<O> {
            tx: Sender<Result<O>>,
            rx: Receiver<Result<O>>,
            join_set: JoinSet<Result<()>>,
        }
        impl<O: Send + 'static> ReceiverStreamBuilder<O> {
            pub fn new(capacity: usize) -> Self {
                let (tx, rx) = tokio::sync::mpsc::channel(capacity);
                Self {
                    tx,
                    rx,
                    join_set: JoinSet::new(),
                }
            }

            pub fn build(self) -> BoxStream<'static, Result<O>> {
                let Self {
                    tx,
                    rx,
                    mut join_set,
                } = self;
                drop(tx);

                let check = async move {
                    while let Some(result) = join_set.join_next().await {
                        match result {
                            Ok(task_result) => match task_result {
                                Ok(_) => continue,
                                Err(error) => return Some(Err(error)),
                            },
                            Err(e) => {
                                if e.is_panic() {
                                    std::panic::resume_unwind(e.into_panic());
                                } else {
                                    return Some(df_execution_err!("Non Panic Task error: {e}"));
                                }
                            }
                        }
                    }
                    None
                };

                let check_stream =
                    futures::stream::once(check).filter_map(|item| async move { item });
                let rx_stream = futures::stream::unfold(rx, |mut rx| async move {
                    let next_item = rx.recv().await;
                    next_item.map(|next_item| (next_item, rx))
                });
                futures::stream::select(rx_stream, check_stream).boxed()
            }
        }

        let mut stream_builder = ReceiverStreamBuilder::<T>::new(1);
        let wrapped_sender = WrappedSender::<T>::new(self.clone(), stream_builder.tx.clone());
        let err_sender = stream_builder.tx.clone();

        stream_builder.join_set.spawn(async move {
            let result = AssertUnwindSafe(async move {
                if let Err(err) = output(wrapped_sender).await {
                    panic!("output_with_sender[{desc}]: output() returns error: {err}");
                }
            })
            .catch_unwind()
            .await
            .map(|_| Ok(()))
            .unwrap_or_else(|err| {
                let panic_message =
                    panic_message::get_panic_message(&err).unwrap_or("unknown error");
                df_execution_err!("{panic_message}")
            });

            if let Err(err) = result {
                err_sender
                    .send(df_execution_err!("{err}"))
                    .await
                    .unwrap_or_default();

                // panic current spawn
                let task_running = is_task_running();
                if !task_running {
                    panic!("output_with_sender[{desc}] canceled due to task finished/killed");
                } else {
                    panic!("output_with_sender[{desc}] error: {err}");
                }
            }
            Ok::<_, DataFusionError>(())
        });
        stream_builder.build()
    }
}

#[derive(Clone)]
pub struct InputBatchStatistics {
    input_batch_count: Count,
    input_batch_mem_size: Count,
    input_row_count: Count,
}

impl InputBatchStatistics {
    pub fn from_metrics_set_and_auron_conf(
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Option<Self>> {
        let enabled = conf::INPUT_BATCH_STATISTICS_ENABLE.value().unwrap_or(false);
        Ok(enabled.then_some(Self::from_metrics_set(metrics_set, partition)))
    }

    pub fn from_metrics_set(metrics_set: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            input_batch_count: MetricBuilder::new(metrics_set)
                .counter("input_batch_count", partition),
            input_batch_mem_size: MetricBuilder::new(metrics_set)
                .counter("input_batch_mem_size", partition),
            input_row_count: MetricBuilder::new(metrics_set).counter("input_row_count", partition),
        }
    }

    pub fn record_input_batch(&self, input_batch: &RecordBatch) {
        let mem_size = input_batch.get_batch_mem_size();
        let num_rows = input_batch.num_rows();
        self.input_batch_count.add(1);
        self.input_batch_mem_size.add(mem_size);
        self.input_row_count.add(num_rows);
    }
}

fn working_senders() -> &'static Mutex<Vec<Weak<dyn WrappedSenderTrait>>> {
    static WORKING_SENDERS: OnceCell<Mutex<Vec<Weak<dyn WrappedSenderTrait>>>> = OnceCell::new();
    WORKING_SENDERS.get_or_init(|| Mutex::default())
}

pub trait RecordBatchWithPayload: Unpin + Send + 'static {
    fn is_empty(&self) -> bool;
}

impl RecordBatchWithPayload for RecordBatch {
    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}

impl RecordBatchWithPayload for OptimizedBatch {
    fn is_empty(&self) -> bool {
        self.as_lazy_batch().num_rows() == 0
    }
}

impl RecordBatchWithPayload for LazyBatchRef {
    fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}

pub type WrappedLazyBatchSender = WrappedSender<LazyBatchRef>;
pub type WrappedRecordBatchSender = WrappedSender<RecordBatch>;
pub type WrappedOptimizedBatchSender = WrappedSender<OptimizedBatch>;

pub trait WrappedSenderTrait: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn exec_ctx(&self) -> &Arc<ExecutionContext>;
}

impl<T: RecordBatchWithPayload> WrappedSenderTrait for WrappedSender<T> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn exec_ctx(&self) -> &Arc<ExecutionContext> {
        &self.exec_ctx
    }
}

pub struct WrappedSender<T: RecordBatchWithPayload> {
    exec_ctx: Arc<ExecutionContext>,
    sender: Sender<Result<T>>,
    exclude_time: OnceCell<Time>,
}

#[allow(clippy::panic)] // Temporarily allow panic to refactor to Result later
impl<T: RecordBatchWithPayload> WrappedSender<T> {
    pub fn new(exec_ctx: Arc<ExecutionContext>, sender: Sender<Result<T>>) -> Arc<Self> {
        let wrapped = Arc::new(Self {
            exec_ctx,
            sender,
            exclude_time: OnceCell::new(),
        });
        let mut working_senders = working_senders().lock();
        working_senders.push(Arc::downgrade(&wrapped) as Weak<dyn WrappedSenderTrait>);
        wrapped
    }

    pub fn exclude_time(&self, exclude_time: &Time) {
        assert!(
            self.exclude_time.get().is_none(),
            "already used a exclude_time"
        );
        self.exclude_time.get_or_init(|| exclude_time.clone());
    }

    pub async fn send(&self, batch: T) {
        if batch.is_empty() {
            return;
        }
        let exclude_time = self.exclude_time.get().cloned();
        let send_time = exclude_time.as_ref().map(|_| Instant::now());
        if self.sender.send(Ok(batch)).await.is_err() {
            let task_ctx = self.exec_ctx.task_ctx();
            debug!(
                "output_with_sender: channel closed, skipping batch send; partition_id={}, task_id={:?}, session_id={}",
                self.exec_ctx.partition_id(),
                task_ctx.task_id(),
                task_ctx.session_id()
            );
            return;
        }

        send_time.inspect(|send_time| {
            exclude_time
                .as_ref()
                .expect("exclude_time must be set")
                .sub_duration(send_time.elapsed());
        });
    }
}

pub fn cancel_all_tasks(task_ctx: &Arc<TaskContext>) {
    let mut working_senders = working_senders().lock();
    *working_senders = std::mem::take(&mut *working_senders)
        .into_iter()
        .filter(|wrapped| match wrapped.upgrade() {
            Some(wrapped) if Arc::ptr_eq(&wrapped.exec_ctx().task_ctx, task_ctx) => {
                if let Ok(wrapped) = downcast_any!(wrapped, WrappedLazyBatchSender) {
                    wrapped
                        .sender
                        .try_send(df_execution_err!("task completed/cancelled"))
                        .unwrap_or_default();
                    return false;
                }
                if let Ok(wrapped) = downcast_any!(wrapped, WrappedRecordBatchSender) {
                    wrapped
                        .sender
                        .try_send(df_execution_err!("task completed/cancelled"))
                        .unwrap_or_default();
                    return false;
                }
                if let Ok(wrapped) = downcast_any!(wrapped, WrappedOptimizedBatchSender) {
                    wrapped
                        .sender
                        .try_send(df_execution_err!("task completed/cancelled"))
                        .unwrap_or_default();
                    return false;
                }
                true
            }
            Some(_) => true, // do not modify senders from other tasks
            None => false,   // already released
        })
        .collect();
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray},
        datatypes::{DataType, Field},
    };
    use datafusion::prelude::SessionContext;
    use futures::TryStreamExt;

    use super::*;
    use crate::common::{
        lazy::{ColumnBatch, LazyColumn, column},
        optimized_batch::materialize_optimized_stream,
        stream_exec::create_record_batch_stream_exec,
    };

    fn optimized_test_batch() -> Result<RecordBatch> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(
                vec![
                    Field::new("key", DataType::Int32, true),
                    Field::new("value", DataType::Utf8, false),
                ],
                [("source".to_owned(), "optimized-test".to_owned())].into(),
            )),
            vec![
                Arc::new(Int32Array::from(vec![Some(3), None, Some(1)])),
                Arc::new(StringArray::from(vec!["c", "b", "a"])),
            ],
        )?)
    }

    fn optimized_test_plan(batch: RecordBatch) -> Result<Arc<dyn ExecutionPlan>> {
        create_record_batch_stream_exec(
            Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter(vec![Ok(batch)]),
            )),
            0,
        )
    }

    fn optimized_test_keys() -> Vec<PhysicalSortExpr> {
        vec![PhysicalSortExpr {
            expr: Arc::new(datafusion::physical_expr::expressions::Column::new(
                "key", 0,
            )),
            options: arrow::compute::SortOptions {
                descending: true,
                nulls_first: false,
            },
        }]
    }

    #[tokio::test]
    async fn optimized_projection_keys_stats_combinations() -> Result<()> {
        let batch = optimized_test_batch()?;
        for projection in [None, Some(vec![1]), Some(vec![1, 1]), Some(vec![])] {
            for keys in [None, Some(vec![]), Some(optimized_test_keys())] {
                for with_input_stats in [false, true] {
                    let plan = optimized_test_plan(batch.clone())?;
                    let expected = match &projection {
                        Some(projection) => batch.project(projection)?,
                        None => batch.clone(),
                    };
                    let ctx = ExecutionContext::new(
                        SessionContext::new().task_ctx(),
                        0,
                        Arc::new(Schema::empty()),
                        &ExecutionPlanMetricsSet::new(),
                    );
                    let statistics =
                        InputBatchStatistics::from_metrics_set(ctx.execution_plan_metrics(), 0);
                    assert!(ctx.input_stat_metrics.set(Some(statistics.clone())).is_ok());
                    let stream = ctx.execute_optimized(
                        &plan,
                        &ExecuteOptimizedProps::default()
                            .with_optional_projection(projection.clone())
                            .with_input_stats(with_input_stats)
                            .with_optional_key_rows(keys.clone()),
                    )?;
                    assert_eq!(stream.schema(), expected.schema());
                    assert_eq!(stream.keys(), keys.as_deref());
                    let output: Vec<_> = stream.try_collect().await?;
                    assert_eq!(output.len(), 1);
                    let output = &output[0];
                    assert_eq!(output.as_lazy_batch().materialize()?, expected);
                    assert_eq!(output.key_rows().is_some(), keys.is_some());
                    if let Some(key_rows) = output.key_rows() {
                        assert_eq!(key_rows.rows.num_rows(), batch.num_rows());
                        if keys.as_ref().is_some_and(|keys| !keys.is_empty()) {
                            let expected_rows = key_rows
                                .rows_converter
                                .convert_columns(&[batch.column(0).clone()])?;
                            for row in 0..batch.num_rows() {
                                assert_eq!(key_rows.rows.row(row), expected_rows.row(row));
                            }
                        } else {
                            assert!(key_rows.rows.iter().all(|row| row.as_ref().is_empty()));
                        }
                    }
                    assert_eq!(
                        statistics.input_batch_count.value(),
                        usize::from(with_input_stats)
                    );
                    assert_eq!(
                        statistics.input_row_count.value(),
                        if with_input_stats {
                            batch.num_rows()
                        } else {
                            0
                        }
                    );
                    assert_eq!(
                        statistics.input_batch_mem_size.value(),
                        if with_input_stats {
                            output.as_lazy_batch().get_estimated_materialized_mem_size()
                        } else {
                            0
                        }
                    );
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn optimized_sort_shortcut_preserves_keys_and_final_stats() -> Result<()> {
        auron_memmgr::MemManager::init(100);
        let batch = optimized_test_batch()?;
        for projection in [None, Some(vec![1]), Some(vec![])] {
            let keys = optimized_test_keys();
            let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(
                optimized_test_plan(batch.clone())?,
                keys.clone(),
                None,
                0,
            ));
            let ctx = ExecutionContext::new(
                SessionContext::new().task_ctx(),
                0,
                sort.schema(),
                &ExecutionPlanMetricsSet::new(),
            );
            let statistics =
                InputBatchStatistics::from_metrics_set(ctx.execution_plan_metrics(), 0);
            assert!(ctx.input_stat_metrics.set(Some(statistics.clone())).is_ok());
            let stream = ctx.execute_optimized(
                &sort,
                &ExecuteOptimizedProps::default()
                    .with_optional_projection(projection.clone())
                    .with_input_stats(true)
                    .with_key_rows(keys.clone()),
            )?;
            let expected_schema = match &projection {
                Some(projection) => Arc::new(batch.schema().project(projection)?),
                None => batch.schema(),
            };
            assert_eq!(stream.schema(), expected_schema);
            assert_eq!(stream.keys(), Some(keys.as_slice()));
            let output: Vec<_> = stream.try_collect().await?;
            let mut rows = 0;
            let mut bytes = 0;
            for output in &output {
                let batch = output.as_lazy_batch();
                assert_eq!(batch.schema(), expected_schema);
                rows += batch.num_rows();
                bytes += batch.get_estimated_materialized_mem_size();
                let keys = output.key_rows().expect("sort must return key rows");
                assert_eq!(keys.rows.num_rows(), batch.num_rows());
                assert!(
                    keys.rows
                        .iter()
                        .zip(keys.rows.iter().skip(1))
                        .all(|(a, b)| a <= b)
                );
            }
            assert_eq!(rows, batch.num_rows());
            assert_eq!(statistics.input_row_count.value(), rows);
            assert_eq!(statistics.input_batch_count.value(), output.len());
            assert_eq!(statistics.input_batch_mem_size.value(), bytes);
        }
        Ok(())
    }

    #[tokio::test]
    async fn optimized_smj_supports_repeated_projections_and_key_rows() -> Result<()> {
        use datafusion::physical_plan::test::TestMemoryExec;

        auron_memmgr::MemManager::init(100);
        let left_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("left_key", DataType::Int32, false),
                Field::new("left_value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )?;
        let right_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("right_key", DataType::Int32, false),
                Field::new("right_value", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["c", "d"])),
            ],
        )?;
        let left: Arc<dyn ExecutionPlan> = Arc::new(TestMemoryExec::try_new(
            &[vec![left_batch.clone()]],
            left_batch.schema(),
            None,
        )?);
        let right: Arc<dyn ExecutionPlan> = Arc::new(TestMemoryExec::try_new(
            &[vec![right_batch.clone()]],
            right_batch.schema(),
            None,
        )?);
        let schema = Arc::new(Schema::new_with_metadata(
            left_batch
                .schema()
                .fields()
                .iter()
                .chain(right_batch.schema().fields().iter())
                .cloned()
                .collect::<Vec<_>>(),
            [("source".to_owned(), "smj-projection".to_owned())].into(),
        ));
        let plan: Arc<dyn ExecutionPlan> = Arc::new(SortMergeJoinExec::try_new(
            schema.clone(),
            left,
            right,
            vec![(
                Arc::new(datafusion::physical_expr::expressions::Column::new(
                    "left_key", 0,
                )),
                Arc::new(datafusion::physical_expr::expressions::Column::new(
                    "right_key",
                    0,
                )),
            )],
            crate::joins::join_utils::JoinType::Inner,
            None,
            vec![arrow::compute::SortOptions::default()],
        )?);
        let expected = RecordBatch::try_new(
            schema,
            left_batch
                .columns()
                .iter()
                .chain(right_batch.columns())
                .cloned()
                .collect(),
        )?;
        let ctx = ExecutionContext::new(
            SessionContext::new().task_ctx(),
            0,
            plan.schema(),
            &ExecutionPlanMetricsSet::new(),
        );
        for projection in [
            vec![1],
            vec![3],
            vec![1, 1],
            vec![3, 1, 3, 0],
            vec![],
            vec![0, 1, 2, 3],
        ] {
            for with_keys in [false, true] {
                let mut props =
                    ExecuteOptimizedProps::default().with_projection(projection.clone());
                if with_keys {
                    props = props.with_key_rows(vec![PhysicalSortExpr::new(
                        Arc::new(datafusion::physical_expr::expressions::Column::new(
                            "left_key", 0,
                        )),
                        arrow::compute::SortOptions::default(),
                    )]);
                }
                let stream = ctx.execute_optimized(&plan, &props)?;
                assert_eq!(stream.schema(), expected.project(&projection)?.schema());
                let batches: Vec<_> = stream.try_collect().await?;
                assert_eq!(batches.len(), 1);
                assert_eq!(
                    batches[0].as_lazy_batch().materialize()?,
                    expected.project(&projection)?
                );
                assert_eq!(batches[0].key_rows().is_some(), with_keys);
                if let Some(keys) = batches[0].key_rows() {
                    let expected_rows = keys
                        .rows_converter
                        .convert_columns(&[left_batch.column(0).clone()])?;
                    assert_eq!(keys.rows.row(0), expected_rows.row(0));
                    assert_eq!(keys.rows.row(1), expected_rows.row(1));
                }
            }
        }
        assert!(
            ctx.execute_optimized(
                &plan,
                &ExecuteOptimizedProps::default().with_projection(vec![4])
            )
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn optimized_materialization_preserves_empty_projection_rows() -> Result<()> {
        let batch = optimized_test_batch()?;
        let plan = optimized_test_plan(batch.clone())?;
        let ctx = ExecutionContext::new(
            SessionContext::new().task_ctx(),
            0,
            batch.schema(),
            &ExecutionPlanMetricsSet::new(),
        );
        let output: Vec<_> = materialize_optimized_stream(
            ctx.execute_optimized(
                &plan,
                &ExecuteOptimizedProps::default()
                    .with_projection(vec![])
                    .with_key_rows(optimized_test_keys()),
            )?,
        )
        .try_collect()
        .await?;
        assert_eq!(output, vec![batch.project(&[])?]);
        Ok(())
    }

    async fn coalesce(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let schema = batches[0].schema();
        let exec_ctx = ExecutionContext::new(
            Arc::new(TaskContext::default()),
            0,
            schema.clone(),
            &ExecutionPlanMetricsSet::new(),
        );
        let input = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(batches.into_iter().map(Ok)),
        ));
        exec_ctx
            .coalesce_with_default_batch_size(input)
            .try_collect()
            .await
    }

    fn integer_batch(start: i64, len: usize) -> Result<RecordBatch> {
        Ok(RecordBatch::try_from_iter([(
            "id",
            Arc::new(Int64Array::from_iter_values(start..start + len as i64))
                as arrow::array::ArrayRef,
        )])?)
    }

    #[tokio::test]
    async fn coalesce_preserves_large_row_batches_without_copying() -> Result<()> {
        for len in [batch_size() / 4, batch_size() / 4 + 1] {
            let first = integer_batch(0, len)?;
            let second = integer_batch(len as i64, len)?;
            let input = vec![first, second];
            let output = coalesce(input.clone()).await?;
            if len > batch_size() / 4 {
                assert_eq!(output.len(), 2);
                for (input, output) in input.iter().zip(&output) {
                    assert!(Arc::ptr_eq(input.column(0), output.column(0)));
                }
            } else {
                assert_eq!(output.len(), 1);
                assert_eq!(output[0].num_rows(), 2 * len);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn coalesce_preserves_large_orc_batches_without_copying() -> Result<()> {
        let value_size = suggested_batch_mem_size() * 3 / 8;
        let text = "x".repeat(value_size);
        let bytes = vec![b'y'; value_size];
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
            Field::new("b", DataType::Binary, true),
        ]));
        let mut input = vec![];
        for id in 0..7i64 {
            let string = match id {
                0 => None,
                1 => Some(""),
                _ => Some(text.as_str()),
            };
            let binary = match id {
                2 => None,
                3 => Some(b"".as_slice()),
                _ => Some(bytes.as_slice()),
            };
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![id])),
                    Arc::new(StringArray::from(vec![string])),
                    Arc::new(BinaryArray::from(vec![binary])),
                ],
            )?;
            assert!(batch.get_batch_mem_size() >= suggested_batch_mem_size() / 4);
            input.push(batch);
        }
        let mut batches = vec![input[0].slice(0, 0)];
        batches.extend(input.clone());
        let output = coalesce(batches).await?;
        assert_eq!(output.len(), 7);
        for (input, output) in input.iter().zip(&output) {
            for (input, output) in input.columns().iter().zip(output.columns()) {
                assert!(Arc::ptr_eq(input, output));
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn coalesce_large_batch_does_not_overtake_buffered_rows() -> Result<()> {
        let first = integer_batch(0, 1)?;
        let second = integer_batch(1, batch_size() / 4 + 1)?;
        let input = vec![
            first.clone(),
            RecordBatch::new_empty(first.schema()),
            second,
        ];
        let expected = arrow::compute::concat_batches(&first.schema(), &input)?;
        let output = coalesce(input).await?;
        assert_eq!(output, vec![expected]);
        Ok(())
    }

    fn sampled_batch() -> Result<LazyBatchRef> {
        let value = "x".repeat(datafusion_ext_commons::suggested_batch_mem_size() / 1024 + 1);
        let source: ArrayRef = Arc::new(StringArray::from(vec![value.as_str(); 1024]));
        let selected = column(source).take_lazy(&Int32Array::from(vec![0]))?;
        ColumnBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", arrow::datatypes::DataType::Utf8, false),
                Field::new("duplicate", arrow::datatypes::DataType::Utf8, false),
            ])),
            vec![selected.clone(), selected],
            1,
        )
    }

    #[tokio::test]
    async fn optimized_sender_keeps_nonempty_zero_column_batches() -> Result<()> {
        let source = optimized_test_batch()?;
        let batch: LazyBatchRef = Arc::new(source.project(&[])?);
        let ctx = ExecutionContext::new(
            SessionContext::new().task_ctx(),
            0,
            batch.schema(),
            &ExecutionPlanMetricsSet::new(),
        );
        let expected = batch.clone();
        let stream =
            ctx.output_optimized_with_sender("OptimizedTest", &[], move |sender| async move {
                sender
                    .send(OptimizedBatch::new(
                        Arc::new(RecordBatch::new_empty(batch.schema())),
                        None,
                    ))
                    .await;
                sender.send(OptimizedBatch::new(batch, None)).await;
                Ok(())
            });
        assert_eq!(stream.keys(), Some([].as_slice()));
        let output: Vec<_> = stream.try_collect().await?;
        assert_eq!(output.len(), 1);
        assert!(Arc::ptr_eq(output[0].as_lazy_batch(), &expected));
        assert_eq!(output[0].as_lazy_batch().num_rows(), source.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn optimized_stats_preserve_lazy_estimates_without_materialization() -> Result<()> {
        let batch = sampled_batch()?;
        assert!(!batch.get_column(0)?.is_materialized());
        let expected = batch.get_estimated_materialized_mem_size();
        let ctx = ExecutionContext::new(
            SessionContext::new().task_ctx(),
            0,
            batch.schema(),
            &ExecutionPlanMetricsSet::new(),
        );
        let statistics = InputBatchStatistics::from_metrics_set(ctx.execution_plan_metrics(), 0);
        assert!(ctx.input_stat_metrics.set(Some(statistics.clone())).is_ok());
        let stream = Box::pin(OptimizedBatchStreamAdapter::new(
            futures::stream::iter(vec![Ok(OptimizedBatch::new(batch.clone(), None))]),
            batch.schema(),
            None,
        ));
        let output: Vec<_> = ctx.stat_input_optimized(stream).try_collect().await?;
        assert_eq!(output.len(), 1);
        assert!(Arc::ptr_eq(output[0].as_lazy_batch(), &batch));
        assert!(!batch.get_column(0)?.is_materialized());
        assert_eq!(statistics.input_batch_mem_size.value(), expected);
        assert_eq!(statistics.input_batch_count.value(), 1);
        assert_eq!(statistics.input_row_count.value(), batch.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn lazy_input_metrics_record_materialized_estimates() -> Result<()> {
        let batch = sampled_batch()?;
        let ctx = ExecutionContext::new(
            SessionContext::new().task_ctx(),
            0,
            batch.schema(),
            &ExecutionPlanMetricsSet::new(),
        );
        let statistics = InputBatchStatistics::from_metrics_set(ctx.execution_plan_metrics(), 0);
        assert!(ctx.input_stat_metrics.set(Some(statistics.clone())).is_ok());
        let expected = batch.get_estimated_materialized_mem_size();
        let stream = Box::pin(LazyBatchStreamAdapter::new(
            batch.schema(),
            futures::stream::iter(vec![Ok(batch)]),
        ));
        let output: Vec<_> = ctx.stat_input_lazy(stream).try_collect().await?;
        assert_eq!(output.len(), 1);
        assert_eq!(statistics.input_batch_mem_size.value(), expected);
        assert_eq!(statistics.input_batch_count.value(), 1);
        assert_eq!(statistics.input_row_count.value(), 1);
        Ok(())
    }
}
