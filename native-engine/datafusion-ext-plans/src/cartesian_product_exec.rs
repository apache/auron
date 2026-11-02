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
    fmt::Formatter,
    io::Write,
    sync::{Arc, Weak},
};

use arrow::{
    array::{RecordBatch, RecordBatchOptions, UInt32Array},
    compute::{filter_record_batch, take},
    datatypes::SchemaRef,
};
use async_trait::async_trait;
use auron_memmgr::{
    MemConsumer, MemConsumerInfo, MemManager,
    spill::{Spill, try_new_file_spill},
};
use datafusion::{
    common::{DataFusionError, Result, Statistics},
    execution::TaskContext,
    physical_expr::{EquivalenceProperties, Partitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_ext_commons::{
    arrow::array_size::BatchSize,
    batch_size,
    io::{read_one_batch, recover_named_batch, write_one_batch},
};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use parking_lot::Mutex;

use crate::{
    common::{
        execution_context::{ExecutionContext, WrappedRecordBatchSender},
        timer_helper::TimerHelper,
    },
    joins::{IdxRange, JoinFilter, product_blocks},
};

#[derive(Debug)]
pub struct CartesianProductExec {
    schema: SchemaRef,
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    left_partition: usize,
    right_partition: usize,
    num_partitions: usize,
    filter: Option<JoinFilter>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl CartesianProductExec {
    pub fn new(
        schema: SchemaRef,
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_partition: usize,
        right_partition: usize,
        num_partitions: usize,
        filter: Option<JoinFilter>,
    ) -> Self {
        Self {
            schema,
            left,
            right,
            left_partition,
            right_partition,
            num_partitions,
            filter,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for CartesianProductExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CartesianProductExec")
    }
}

impl ExecutionPlan for CartesianProductExec {
    fn name(&self) -> &str {
        "CartesianProductExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(self.num_partitions),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.schema(),
            children[0].clone(),
            children[1].clone(),
            self.left_partition,
            self.right_partition,
            self.num_partitions,
            self.filter.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx =
            ExecutionContext::new(context.clone(), partition, self.schema(), &self.metrics);
        let mut left =
            exec_ctx.stat_input(self.left.execute(self.left_partition, context.clone())?);
        let mut right = exec_ctx.stat_input(self.right.execute(self.right_partition, context)?);
        let filter = self.filter.clone();
        let output_ctx = exec_ctx.clone();
        let output = exec_ctx.output_with_sender("CartesianProduct", move |sender| async move {
            let elapsed = output_ctx.baseline_metrics().elapsed_compute().clone();
            let _timer = elapsed.timer();
            sender.exclude_time(&elapsed);

            let Some(first_left) = elapsed.exclude_timer_async(left.next()).await.transpose()?
            else {
                return Ok(());
            };
            let right_schema = right.schema();
            let buffer = RightBuffer::new(output_ctx.clone());
            let mut right_rows = 0;
            while let Some(batch) = elapsed
                .exclude_timer_async(right.next())
                .await
                .transpose()?
            {
                right_rows += batch.num_rows();
                buffer.insert(batch).await?;
            }
            if right_rows == 0 {
                return Ok(());
            }
            let buffered = buffer.finish().await;
            let left_schema = left.schema();
            let left = Box::pin(RecordBatchStreamAdapter::new(
                left_schema,
                futures::stream::iter([Ok(first_left)]).chain(left),
            ));
            execute_product(left, right_schema, buffered, filter, output_ctx, sender).await
        });
        Ok(exec_ctx.coalesce_with_default_batch_size(output))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

async fn execute_product(
    mut left: SendableRecordBatchStream,
    right_schema: SchemaRef,
    buffered: BufferedRight,
    filter: Option<JoinFilter>,
    exec_ctx: Arc<ExecutionContext>,
    sender: Arc<WrappedRecordBatchSender>,
) -> Result<()> {
    let elapsed = exec_ctx.baseline_metrics().elapsed_compute();
    while let Some(batch) = elapsed.exclude_timer_async(left.next()).await.transpose()? {
        if let Some(spill) = &buffered.spill {
            let mut reader = spill.get_buf_reader();
            while let Some((rows, columns)) = read_one_batch(&mut reader, &right_schema)? {
                let right = recover_named_batch(rows, &columns, right_schema.clone())?;
                emit_product(
                    &batch,
                    &right,
                    filter.as_ref(),
                    &exec_ctx,
                    &sender,
                    batch_size(),
                )
                .await?;
            }
        }
        for right in &buffered.batches {
            emit_product(
                &batch,
                right,
                filter.as_ref(),
                &exec_ctx,
                &sender,
                batch_size(),
            )
            .await?;
        }
    }
    Ok(())
}

async fn emit_product(
    left: &RecordBatch,
    right: &RecordBatch,
    filter: Option<&JoinFilter>,
    exec_ctx: &Arc<ExecutionContext>,
    sender: &WrappedRecordBatchSender,
    batch_size: usize,
) -> Result<()> {
    let left_range = [IdxRange {
        batch_idx: 0,
        start: 0,
        end: left.num_rows(),
    }];
    let right_range = [IdxRange {
        batch_idx: 0,
        start: 0,
        end: right.num_rows(),
    }];
    for block in product_blocks(&left_range, &right_range, batch_size) {
        let mut lindices = vec![];
        let mut rindices = vec![];
        for l in block.lrange.start..block.lrange.end {
            for r in block.rrange.start..block.rrange.end {
                lindices.push(l as u32);
                rindices.push(r as u32);
            }
        }
        let rows = lindices.len();
        let lindices = UInt32Array::from(lindices);
        let rindices = UInt32Array::from(rindices);
        let left_cols = left
            .columns()
            .iter()
            .map(|col| take(col, &lindices, None))
            .collect::<Result<Vec<_>, _>>()?;
        let right_cols = right
            .columns()
            .iter()
            .map(|col| take(col, &rindices, None))
            .collect::<Result<Vec<_>, _>>()?;
        let mask = filter
            .map(|filter| filter.evaluate(&left_cols, &right_cols, rows))
            .transpose()?;
        let mut batch = RecordBatch::try_new_with_options(
            exec_ctx.output_schema(),
            [left_cols, right_cols].concat(),
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?;
        if let Some(mask) = mask {
            batch = filter_record_batch(&batch, &mask)?;
        }
        if batch.num_rows() > 0 {
            exec_ctx.baseline_metrics().record_output(batch.num_rows());
            sender.send(batch).await;
        }
    }
    Ok(())
}

#[derive(Default)]
struct BufferedRight {
    batches: Vec<RecordBatch>,
    spill: Option<Box<dyn Spill>>,
    mem_used: usize,
}

struct RightBuffer {
    exec_ctx: Arc<ExecutionContext>,
    consumer_info: Option<Weak<MemConsumerInfo>>,
    buffered: Mutex<BufferedRight>,
    spill_lock: tokio::sync::Mutex<()>,
}

impl RightBuffer {
    fn new(exec_ctx: Arc<ExecutionContext>) -> Arc<Self> {
        let buffer = Arc::new(Self {
            exec_ctx,
            consumer_info: None,
            buffered: Mutex::new(BufferedRight::default()),
            spill_lock: tokio::sync::Mutex::new(()),
        });
        MemManager::register_consumer(buffer.clone(), true);
        buffer
    }

    fn buffered_mem(&self) -> usize {
        self.buffered.lock().mem_used
    }

    async fn insert(&self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() > 0 {
            {
                let mut buffered = self.buffered.lock();
                buffered.mem_used += batch.get_batch_mem_size();
                buffered.batches.push(batch);
            }
            self.update_mem_used(self.buffered_mem()).await?;
        }
        Ok(())
    }

    async fn finish(&self) -> BufferedRight {
        let _guard = self.spill_lock.lock().await;
        self.set_spillable(false);
        std::mem::take(&mut *self.buffered.lock())
    }
}

#[async_trait]
impl MemConsumer for RightBuffer {
    fn name(&self) -> &str {
        "CartesianProduct"
    }

    fn set_consumer_info(&mut self, consumer_info: Weak<MemConsumerInfo>) {
        self.consumer_info = Some(consumer_info);
    }

    fn get_consumer_info(&self) -> &Weak<MemConsumerInfo> {
        self.consumer_info.as_ref().expect("consumer info not set")
    }

    async fn spill(&self) -> Result<()> {
        let _guard = self.spill_lock.lock().await;
        let (batches, spill) = {
            let mut buffered = self.buffered.lock();
            if buffered.batches.is_empty() {
                return Ok(());
            }
            buffered.mem_used = 0;
            (std::mem::take(&mut buffered.batches), buffered.spill.take())
        };
        let spill_metrics = self.exec_ctx.spill_metrics().clone();
        let spill = tokio::task::spawn_blocking(move || {
            let mut spill = match spill {
                Some(spill) => spill,
                None => try_new_file_spill(&spill_metrics)?,
            };
            {
                let mut writer = spill.get_buf_writer();
                for batch in batches {
                    spill_metrics.mem_spill_size.add(batch.get_batch_mem_size());
                    write_one_batch(batch.num_rows(), batch.columns(), &mut writer)?;
                }
                writer.flush()?;
            }
            spill_metrics.mem_spill_count.add(1);
            Ok::<_, DataFusionError>(spill)
        })
        .await
        .map_err(|err| {
            DataFusionError::Execution(format!("Cartesian spill task failed: {err}"))
        })??;
        self.buffered.lock().spill = Some(spill);
        self.update_mem_used(self.buffered_mem()).await
    }
}

impl Drop for RightBuffer {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

#[cfg(test)]
mod tests {
    #![allow(deprecated)]

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        assert_batches_sorted_eq,
        common::JoinSide,
        logical_expr::Operator,
        physical_expr::expressions::{BinaryExpr, Column},
        physical_plan::{common::collect, test::TestMemoryExec},
        prelude::SessionContext,
    };

    use super::*;
    use crate::joins::ColumnIndex;

    fn batch(name: &str, values: Vec<Option<i32>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .expect("valid test batch")
    }

    fn less_than_filter(schema: SchemaRef) -> JoinFilter {
        JoinFilter {
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
            schema,
        }
    }

    #[tokio::test]
    async fn cartesian_partition_mapping_and_filter() -> Result<()> {
        MemManager::init(1024 * 1024);
        let left = batch("l", vec![Some(1), None, Some(3)]);
        let right = batch("r", vec![Some(2), None, Some(4)]);
        let schema = Arc::new(Schema::new(vec![
            left.schema().field(0).clone(),
            right.schema().field(0).clone(),
        ]));
        let filter = less_than_filter(schema.clone());
        let left_plan = Arc::new(TestMemoryExec::try_new(
            &[vec![], vec![left.clone()]],
            left.schema(),
            None,
        )?);
        let right_plan = Arc::new(TestMemoryExec::try_new(
            &[vec![], vec![], vec![right.clone()]],
            right.schema(),
            None,
        )?);
        let exec =
            CartesianProductExec::new(schema.clone(), left_plan, right_plan, 1, 2, 6, Some(filter));
        let output = collect(exec.execute(5, SessionContext::new().task_ctx())?).await?;
        assert_batches_sorted_eq!(
            [
                "+---+---+",
                "| l | r |",
                "+---+---+",
                "| 1 | 2 |",
                "| 1 | 4 |",
                "| 3 | 4 |",
                "+---+---+",
            ],
            &output
        );
        assert_eq!(
            exec.metrics().expect("Cartesian metrics").output_rows(),
            Some(3)
        );
        Ok(())
    }

    #[tokio::test]
    async fn cartesian_spill_replays_multiple_writes_and_zero_columns() -> Result<()> {
        MemManager::init(1024 * 1024);
        for input in [
            batch("r", vec![Some(2), None, Some(4)]),
            RecordBatch::try_new_with_options(
                Arc::new(Schema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(3)),
            )?,
        ] {
            let metrics = ExecutionPlanMetricsSet::new();
            let ctx = ExecutionContext::new(
                SessionContext::new().task_ctx(),
                0,
                input.schema(),
                &metrics,
            );
            let buffer = RightBuffer::new(ctx.clone());
            for _ in 0..2 {
                buffer.insert(input.clone()).await?;
                buffer.force_spill().await?;
                assert_eq!(buffer.mem_used(), 0);
            }
            let buffered = buffer.finish().await;
            assert!(buffered.batches.is_empty());
            let spill = buffered.spill.as_ref().expect("right input spilled");
            for _ in 0..2 {
                let mut reader = spill.get_buf_reader();
                let mut rows = 0;
                while let Some((num_rows, cols)) = read_one_batch(&mut reader, &input.schema())? {
                    let batch = recover_named_batch(num_rows, &cols, input.schema())?;
                    assert_eq!(batch, input);
                    rows += num_rows;
                }
                assert_eq!(rows, 6);
            }
            assert_eq!(ctx.spill_metrics().mem_spill_count.value(), 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn cartesian_product_emits_bounded_batches() -> Result<()> {
        let left = batch("l", (0..11).map(Some).collect());
        let right = batch("r", (0..13).map(Some).collect());
        let schema = Arc::new(Schema::new(vec![
            left.schema().field(0).clone(),
            right.schema().field(0).clone(),
        ]));
        let metrics = ExecutionPlanMetricsSet::new();
        let ctx = ExecutionContext::new(SessionContext::new().task_ctx(), 0, schema, &metrics);
        let output_ctx = ctx.clone();
        let stream = ctx.output_with_sender("CartesianProductTest", move |sender| async move {
            emit_product(&left, &right, None, &output_ctx, &sender, 3).await
        });
        let output = collect(stream).await?;
        assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 143);
        assert!(output.iter().all(|batch| batch.num_rows() <= 3));
        Ok(())
    }

    #[tokio::test]
    async fn cartesian_product_replays_spilled_right_input() -> Result<()> {
        MemManager::init(1024 * 1024);
        let left = batch("l", vec![Some(1), None]);
        let right = batch("r", vec![Some(2), None, Some(4)]);
        let schema = Arc::new(Schema::new(vec![
            left.schema().field(0).clone(),
            right.schema().field(0).clone(),
        ]));
        let metrics = ExecutionPlanMetricsSet::new();
        let ctx = ExecutionContext::new(
            SessionContext::new().task_ctx(),
            0,
            schema.clone(),
            &metrics,
        );
        let buffer = RightBuffer::new(ctx.clone());
        for _ in 0..2 {
            buffer.insert(right.clone()).await?;
            buffer.force_spill().await?;
        }
        buffer.insert(batch("r", vec![Some(7)])).await?;
        let buffered = buffer.finish().await;
        let left_schema = left.schema();
        let left = Box::pin(RecordBatchStreamAdapter::new(
            left_schema,
            futures::stream::iter([Ok(left), Ok(batch("l", vec![Some(3)]))]),
        ));
        let filter = less_than_filter(schema);
        let output_ctx = ctx.clone();
        let stream =
            ctx.output_with_sender("CartesianProductSpillTest", move |sender| async move {
                execute_product(
                    left,
                    right.schema(),
                    buffered,
                    Some(filter),
                    output_ctx,
                    sender,
                )
                .await
            });
        let output = collect(stream).await?;
        assert_batches_sorted_eq!(
            [
                "+---+---+",
                "| l | r |",
                "+---+---+",
                "| 1 | 2 |",
                "| 1 | 2 |",
                "| 1 | 4 |",
                "| 1 | 4 |",
                "| 1 | 7 |",
                "| 3 | 4 |",
                "| 3 | 4 |",
                "| 3 | 7 |",
                "+---+---+",
            ],
            &output
        );
        assert!(ctx.spill_metrics().mem_spill_count.value() >= 2);
        assert_eq!(metrics.clone_inner().output_rows(), Some(8));
        Ok(())
    }
}
