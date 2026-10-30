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

//! A spill-capable "repartition by precomputed bucket id" operator, used by
//! hybrid (bucketed hash) join to group build/probe side rows by their
//! `~BUCKET` column.
//!
//! This intentionally reuses the same radix-sort-based grouping and spill
//! machinery as
//! [`crate::shuffle::sort_repartitioner::SortShuffleRepartitioner`]
//! (both are backed by
//! [`BufferedData`](crate::shuffle::buffered_data::BufferedData)), rather than
//! a comparison-sort based [`crate::sort_exec::SortExec`]: the join only needs
//! rows grouped by bucket id (not globally ordered), so an O(n) radix
//! partition is both simpler and cheaper than a full external sort. Unlike
//! `SortShuffleRepartitioner` (which writes the final result to Spark shuffle
//! data/index files), [`BucketRepartitioner::into_stream`] merges any spilled
//! runs plus the remaining in-memory data directly back into a
//! [`SendableRecordBatchStream`], since the result is consumed in-process by
//! the join operator rather than being read back by a downstream shuffle
//! reader.

use std::{
    io::Read,
    sync::{Arc, Weak},
};

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use async_trait::async_trait;
use auron_memmgr::{
    MemConsumer, MemConsumerInfo, MemManager,
    spill::{OwnedSpillBufReader, Spill, try_new_spill},
};
use bytesize::ByteSize;
use datafusion::{
    common::{DataFusionError, Result},
    physical_plan::{SendableRecordBatchStream, metrics::Time},
};
use datafusion_ext_commons::{
    arrow::array_size::BatchSize, df_execution_err, io::ipc_compression::IpcCompressionReader,
};
use futures::lock::Mutex;
use once_cell::sync::OnceCell;

use crate::{
    common::{
        execution_context::ExecutionContext,
        offsetted::{Offsetted, OffsettedMergeIterator},
    },
    shuffle::{Partitioning, buffered_data::BufferedData},
};

pub struct BucketRepartitioner {
    exec_ctx: Arc<ExecutionContext>,
    mem_consumer_info: OnceCell<Weak<MemConsumerInfo>>,
    schema: SchemaRef,
    num_buckets: usize,
    data: Mutex<BufferedData>,
    spills: Mutex<Vec<Offsetted<u64, Box<dyn Spill>>>>,
}

impl BucketRepartitioner {
    /// `schema` is the schema of the batches that will be fed into
    /// [`Self::insert_batch`] (data columns + the trailing `~BUCKET: Int32`
    /// column at `bucket_col_idx`, as produced by
    /// [`super::bucketed_group::append_bucket_id_column`]).
    pub fn new(
        exec_ctx: Arc<ExecutionContext>,
        schema: SchemaRef,
        bucket_col_idx: usize,
        num_buckets: usize,
    ) -> Self {
        let partition_id = exec_ctx.partition_id();
        Self {
            exec_ctx,
            mem_consumer_info: OnceCell::new(),
            schema,
            num_buckets,
            data: Mutex::new(BufferedData::new(
                Partitioning::BucketPartitioning(bucket_col_idx, num_buckets),
                partition_id,
                Time::new(),
            )),
            spills: Mutex::default(),
        }
    }

    pub async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        // update memory usage before adding to buffered data
        let mem_used = self.data.lock().await.mem_used() + input.get_batch_mem_size() * 2;
        self.update_mem_used(mem_used).await?;

        // add batch to buffered data
        let mem_used = {
            let mut data = self.data.lock().await;
            data.add_batch(input)?;
            data.mem_used()
        };
        self.update_mem_used(mem_used).await?;

        // we are likely to spill more frequently because the cost of spilling a
        // bucket repartition is lower than other consumers.
        let mem_used_percent = self.mem_used_percent();
        if mem_used_percent > 0.8 {
            log::info!(
                "{} memory usage: {}, percent: {:.3}, spilling...",
                self.name(),
                ByteSize(mem_used as u64),
                mem_used_percent,
            );
            self.spill().await?;
        }
        Ok(())
    }

    /// Consume all previously inserted batches and return a stream whose
    /// batches are grouped by non-decreasing bucket id (groups are not
    /// further sub-ordered - the join only cares which bucket a row belongs
    /// to). Must be called exactly once, after all [`Self::insert_batch`]
    /// calls have completed.
    pub async fn into_stream(self: Arc<Self>) -> Result<SendableRecordBatchStream> {
        self.set_spillable(false);
        let mut spills = std::mem::take(&mut *self.spills.lock().await);
        let data = self.data.lock().await.drain();
        let schema = self.schema.clone();
        let exec_ctx = self.exec_ctx.clone();
        let num_buckets = self.num_buckets;

        // fast path: nothing ever spilled, so the in-memory grouped batches can be
        // streamed out directly without an IPC-encode/decode round trip.
        if spills.is_empty() {
            return Ok(exec_ctx.output_with_sender(
                "BucketRepartitioner",
                move |sender| async move {
                    if !data.is_empty() {
                        let mut iter = data.into_sorted_batches()?;
                        while let Some((_bucket_id, batch_iter)) = iter.next_partition_chunk() {
                            for batch in batch_iter {
                                sender.send(batch).await;
                            }
                        }
                    }
                    Ok::<(), DataFusionError>(())
                },
            ));
        }

        // slow path: one or more spills happened; also spill the remaining
        // in-memory data (mirrors
        // `SortShuffleRepartitioner::shuffle_write`'s handling of leftover data) so
        // every run can be merged uniformly below.
        if !data.is_empty() {
            if self.mem_used_percent() < 0.5 {
                let mut spill = Box::new(vec![]);
                let offsets = data.write(spill.get_buf_writer())?;
                self.update_mem_used(spill.len()).await?;
                spills.push(Offsetted::new(offsets, spill));
            } else {
                let spill_metrics = self.exec_ctx.spill_metrics().clone();
                let spill = tokio::task::spawn_blocking(move || {
                    let mut spill = try_new_spill(&spill_metrics)?;
                    let offsets = data.write(spill.get_buf_writer())?;
                    Ok::<_, DataFusionError>(Offsetted::new(offsets, spill))
                })
                .await
                .expect("tokio spawn_blocking error")?;
                self.update_mem_used(0).await?;
                spills.push(spill);
            }
        }

        Ok(
            exec_ctx.output_with_sender("BucketRepartitioner", move |sender| async move {
                let mut merge_iter = OffsettedMergeIterator::new(
                    num_buckets,
                    spills
                        .into_iter()
                        .map(|spill| spill.map_data(|s| OwnedSpillBufReader::from(s)))
                        .collect(),
                );

                while let Some((_bucket_id, chunk)) = merge_iter.next_partition_chunk() {
                    for (reader, range) in chunk {
                        let mut raw = Vec::with_capacity((range.end - range.start) as usize);
                        reader
                            .buf_reader()
                            .take(range.end - range.start)
                            .read_to_end(&mut raw)
                            .or_else(|e| df_execution_err!("bucket repartition read error: {e}"))?;
                        let mut ipc_reader = IpcCompressionReader::new(std::io::Cursor::new(raw));
                        while let Some((num_rows, cols)) = ipc_reader.read_batch(&schema)? {
                            let batch = RecordBatch::try_new_with_options(
                                schema.clone(),
                                cols,
                                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
                            )?;
                            sender.send(batch).await;
                        }
                    }
                }
                Ok::<(), DataFusionError>(())
            }),
        )
    }
}

#[async_trait]
impl MemConsumer for BucketRepartitioner {
    fn name(&self) -> &str {
        "BucketRepartitioner"
    }

    fn set_consumer_info(&mut self, consumer_info: Weak<MemConsumerInfo>) {
        self.mem_consumer_info
            .set(consumer_info)
            .expect("consumer info already set");
    }

    fn get_consumer_info(&self) -> &Weak<MemConsumerInfo> {
        self.mem_consumer_info.get().expect("consumer info not set")
    }

    async fn spill(&self) -> Result<()> {
        let data = self.data.lock().await.drain();
        let spill_metrics = self.exec_ctx.spill_metrics().clone();
        let mut spills = self.spills.lock().await;
        let spill = tokio::task::spawn_blocking(move || {
            let mut spill = try_new_spill(&spill_metrics)?;
            let offsets = data.write(spill.get_buf_writer())?;
            Ok::<_, DataFusionError>(Offsetted::new(offsets, spill))
        })
        .await
        .expect("tokio spawn_blocking error")?;
        spills.push(spill);
        drop(spills);
        self.update_mem_used(0).await?;
        Ok(())
    }
}

impl Drop for BucketRepartitioner {
    fn drop(&mut self) {
        MemManager::deregister_consumer(self);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int32Array},
        datatypes::{DataType, Field, Schema},
    };
    use auron_memmgr::MemManager;
    use datafusion::{
        common::Result,
        physical_expr::{PhysicalExprRef, expressions::Column},
        physical_plan::SendableRecordBatchStream,
        prelude::SessionContext,
    };
    use futures::StreamExt;

    use super::*;
    use crate::{
        common::execution_context::ExecutionContext,
        joins::bucketed_group::{append_bucket_id_column, bucket_schema},
    };

    fn init_mem_mgr() {
        MemManager::init(10000000);
    }

    fn int32_batch(schema: SchemaRef, values: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values)) as ArrayRef])
            .expect("valid Int32 test batch")
    }

    fn data_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]))
    }

    fn exec_ctx() -> Arc<ExecutionContext> {
        let session_ctx = SessionContext::default();
        let task_ctx = session_ctx.task_ctx();
        ExecutionContext::new(
            task_ctx,
            0,
            bucket_schema(&data_schema()),
            &Default::default(),
        )
    }

    async fn collect_buckets(stream: SendableRecordBatchStream) -> Result<Vec<i32>> {
        let mut stream = stream;
        let mut buckets = vec![];
        while let Some(batch) = stream.next().await.transpose()? {
            let bucket_col = batch.column(batch.num_columns() - 1);
            let bucket_col = bucket_col
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("~BUCKET must be Int32");
            buckets.extend(bucket_col.values().iter().copied());
        }
        Ok(buckets)
    }

    fn assert_non_decreasing_and_same_multiset(output: &[i32], mut expected: Vec<i32>) {
        for i in 1..output.len() {
            assert!(
                output[i] >= output[i - 1],
                "output not grouped by bucket: {output:?}"
            );
        }
        let mut sorted_output = output.to_vec();
        sorted_output.sort();
        expected.sort();
        assert_eq!(sorted_output, expected);
    }

    #[tokio::test]
    async fn empty_input_produces_no_batches() -> Result<()> {
        init_mem_mgr();
        let repartitioner = Arc::new(BucketRepartitioner::new(
            exec_ctx(),
            bucket_schema(&data_schema()),
            1,
            4,
        ));
        MemManager::register_consumer(repartitioner.clone(), true);
        let stream = repartitioner.into_stream().await?;
        let buckets = collect_buckets(stream).await?;
        assert!(buckets.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn groups_rows_by_bucket_without_spilling() -> Result<()> {
        init_mem_mgr();
        let schema = data_schema();
        let key: PhysicalExprRef = Arc::new(Column::new("k", 0));
        let repartitioner = Arc::new(BucketRepartitioner::new(
            exec_ctx(),
            bucket_schema(&schema),
            1,
            4096,
        ));
        MemManager::register_consumer(repartitioner.clone(), true);

        let values = vec![5, 1, 5, 2, 1, 3, 2, 5];
        let batch = int32_batch(schema.clone(), values.clone());
        let bucketed = append_bucket_id_column(batch, &[key])?;
        let expected_buckets: Vec<i32> = {
            let col = bucketed.column(bucketed.num_columns() - 1);
            col.as_any()
                .downcast_ref::<Int32Array>()
                .expect("bucket column must contain Int32 values")
                .values()
                .to_vec()
        };
        repartitioner.insert_batch(bucketed).await?;

        let stream = repartitioner.into_stream().await?;
        let buckets = collect_buckets(stream).await?;
        assert_eq!(buckets.len(), values.len());
        assert_non_decreasing_and_same_multiset(&buckets, expected_buckets);
        Ok(())
    }

    #[tokio::test]
    async fn merges_correctly_after_forced_spill() -> Result<()> {
        init_mem_mgr();
        let schema = data_schema();
        let key: PhysicalExprRef = Arc::new(Column::new("k", 0));
        let repartitioner = Arc::new(BucketRepartitioner::new(
            exec_ctx(),
            bucket_schema(&schema),
            1,
            4096,
        ));
        MemManager::register_consumer(repartitioner.clone(), true);

        let mut all_expected = vec![];
        for chunk in [vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]] {
            let batch = int32_batch(schema.clone(), chunk);
            let bucketed = append_bucket_id_column(batch, &[key.clone()])?;
            let bucket_values: Vec<i32> = {
                let col = bucketed.column(bucketed.num_columns() - 1);
                col.as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("bucket column must contain Int32 values")
                    .values()
                    .to_vec()
            };
            all_expected.extend(bucket_values);
            repartitioner.insert_batch(bucketed).await?;
            // force a spill after every batch to exercise the merge path
            repartitioner.spill().await?;
        }

        let stream = repartitioner.into_stream().await?;
        let buckets = collect_buckets(stream).await?;
        assert_eq!(buckets.len(), all_expected.len());
        assert_non_decreasing_and_same_multiset(&buckets, all_expected);
        Ok(())
    }
}
