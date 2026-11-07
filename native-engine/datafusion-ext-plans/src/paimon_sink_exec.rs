// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Native writer for Paimon append-only (bucket-unaware) tables.
//!
//! The JVM side owns everything that is Paimon specific: data file naming,
//! column statistics and the final commit. This operator only encodes the input
//! batches into parquet files:
//!
//! * input columns are the table columns in table order and are all written,
//!   since Paimon data files also contain the partition columns;
//! * rows are routed by the `partition_col_indices` columns to one open file
//!   per partition, so the input does not need to be clustered by partition;
//! * a file is closed when it reaches `target_file_size`, when more than
//!   `max_open_files` files are open (the largest is closed), or at the end of
//!   the input;
//! * the JVM side hands out output paths and is told the row count and size of
//!   every closed file; it derives the partition of a file from the parquet
//!   footer statistics of the partition columns (min == max), so no partition
//!   values cross the JNI boundary.

use std::{any::Any, collections::HashMap, fmt::Formatter, io::Write, sync::Arc};

use arrow::{
    array::{ArrayRef, UInt32Array},
    compute::take_record_batch,
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
    row::{OwnedRow, RowConverter, SortField},
};
use auron_jni_bridge::{jni_call_static, jni_get_string, jni_new_global_ref, jni_new_string};
use datafusion::{
    common::{Result, Statistics},
    execution::context::TaskContext,
    parquet::{arrow::ArrowWriter, file::properties::WriterProperties},
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{Count, ExecutionPlanMetricsSet, MetricsSet, Time},
    },
};
use datafusion_ext_commons::{
    arrow::cast::cast,
    df_execution_err,
    hadoop_fs::{FsDataOutputWrapper, FsProvider},
};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::{common::execution_context::ExecutionContext, parquet_sink_exec::parse_writer_props};

#[derive(Debug)]
pub struct PaimonSinkExec {
    fs_resource_id: String,
    input: Arc<dyn ExecutionPlan>,
    partition_col_indices: Vec<usize>,
    data_schema: SchemaRef,
    target_file_size: usize,
    max_open_files: usize,
    memory_budget: usize,
    props: Vec<(String, String)>,
    metrics: ExecutionPlanMetricsSet,
    plan_props: OnceCell<PlanProperties>,
}

impl PaimonSinkExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        fs_resource_id: String,
        partition_col_indices: Vec<usize>,
        data_schema: SchemaRef,
        target_file_size: usize,
        max_open_files: usize,
        memory_budget: usize,
        props: Vec<(String, String)>,
    ) -> Self {
        Self {
            input,
            fs_resource_id,
            partition_col_indices,
            data_schema,
            target_file_size,
            max_open_files,
            memory_budget,
            props,
            metrics: ExecutionPlanMetricsSet::new(),
            plan_props: OnceCell::new(),
        }
    }
}

impl DisplayAs for PaimonSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PaimonSink")
    }
}

impl ExecutionPlan for PaimonSinkExec {
    fn name(&self) -> &str {
        "PaimonSinkExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.plan_props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
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
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.fs_resource_id.clone(),
            self.partition_col_indices.clone(),
            self.data_schema.clone(),
            self.target_file_size,
            self.max_open_files,
            self.memory_budget,
            self.props.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let io_time = exec_ctx.register_timer_metric("io_time");
        let sink_ctx = Arc::new(PaimonSinkContext::try_new(self, &io_time)?);
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        execute_paimon_sink(sink_ctx, input, exec_ctx)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

struct PaimonSinkContext {
    fs_provider: FsProvider,
    partition_col_indices: Vec<usize>,
    data_schema: SchemaRef,
    target_file_size: usize,
    max_open_files: usize,
    memory_budget: usize,
    row_group_block_size: usize,
    props: WriterProperties,
}

impl PaimonSinkContext {
    fn try_new(exec: &PaimonSinkExec, io_time: &Time) -> Result<Self> {
        let fs_provider = {
            let resource_id = jni_new_string!(&exec.fs_resource_id)?;
            let fs = jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
            FsProvider::new(jni_new_global_ref!(fs.as_obj())?, io_time)
        };
        let row_group_block_size = exec
            .props
            .iter()
            .find(|(key, _)| key == "parquet.block.size")
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .unwrap_or(128 * 1024 * 1024);
        Ok(Self {
            fs_provider,
            partition_col_indices: exec.partition_col_indices.clone(),
            data_schema: exec.data_schema.clone(),
            target_file_size: exec.target_file_size.max(1),
            max_open_files: exec.max_open_files.max(1),
            memory_budget: exec.memory_budget.max(1),
            row_group_block_size,
            props: parse_writer_props(&exec.props),
        })
    }
}

fn execute_paimon_sink(
    sink_ctx: Arc<PaimonSinkContext>,
    mut input: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let bytes_written = exec_ctx.register_counter_metric("bytes_written");
    let files_written = exec_ctx.register_counter_metric("files_written");

    Ok(exec_ctx
        .clone()
        .output_with_sender("PaimonSink", move |_sender| async move {
            let mut writers = PartitionedWriters::new(sink_ctx.clone());
            let finish = |stats: Vec<FileStat>| -> Result<()> {
                for stat in stats {
                    jni_call_static!(
                        AuronNativePaimonSinkUtils.completeFile(
                            jni_new_string!(&stat.path)?.as_obj(),
                            stat.num_rows as i64,
                            stat.num_bytes as i64,
                        ) -> ()
                    )?;
                    exec_ctx.baseline_metrics().output_rows().add(stat.num_rows);
                    bytes_written.add(stat.num_bytes);
                    files_written.add(1);
                }
                Ok(())
            };
            while let Some(batch) = input.next().await.transpose()? {
                let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                if batch.num_rows() == 0 {
                    continue;
                }
                let batch = cast_to_data_schema(&batch, &sink_ctx.data_schema)?;
                let closed = tokio::task::block_in_place(|| writers.write(&batch))?;
                finish(closed)?;
            }
            let closed = tokio::task::block_in_place(|| writers.close_all())?;
            finish(closed)?;
            Ok(())
        }))
}

/// Casts input columns to the data file schema, which carries Paimon field ids
/// as parquet field ids and the exact parquet-compatible types expected by
/// Paimon.
fn cast_to_data_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let mut cols = Vec::with_capacity(schema.fields().len());
    for (idx, field) in schema.fields().iter().enumerate() {
        cols.push(cast(batch.column(idx), field.data_type())?);
    }
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

/// One open parquet file per partition. Rows are routed by the partition
/// columns, so the input does not need to be clustered (Paimon's own writer
/// behaves the same way).
struct PartitionedWriters {
    sink_ctx: Arc<PaimonSinkContext>,
    row_converter: Option<RowConverter>,
    writers: HashMap<OwnedRow, FileWriter>,
    // used when the table is not partitioned
    single: Option<FileWriter>,
}

impl PartitionedWriters {
    fn new(sink_ctx: Arc<PaimonSinkContext>) -> Self {
        let row_converter = if sink_ctx.partition_col_indices.is_empty() {
            None
        } else {
            let fields = sink_ctx
                .partition_col_indices
                .iter()
                .map(|&i| SortField::new(sink_ctx.data_schema.field(i).data_type().clone()))
                .collect();
            Some(RowConverter::new(fields).expect("unsupported partition column type"))
        };
        Self {
            sink_ctx,
            row_converter,
            writers: HashMap::new(),
            single: None,
        }
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<Vec<FileStat>> {
        let mut closed = vec![];
        let Some(converter) = &self.row_converter else {
            if self.single.is_none() {
                self.single = Some(FileWriter::try_new(self.sink_ctx.clone())?);
            }
            let w = self.single.as_mut().expect("single writer");
            w.write(batch)?;
            if w.estimated_size() >= self.sink_ctx.target_file_size {
                closed.push(self.single.take().expect("single writer").close()?);
            }
            return Ok(closed);
        };

        // group row indices by partition, keeping first-seen order
        let part_cols: Vec<ArrayRef> = self
            .sink_ctx
            .partition_col_indices
            .iter()
            .map(|&i| batch.column(i).clone())
            .collect();
        let rows = converter.convert_columns(&part_cols)?;
        let mut groups: Vec<(OwnedRow, Vec<u32>)> = vec![];
        let mut group_idx: HashMap<OwnedRow, usize> = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let key = rows.row(row_idx).owned();
            match group_idx.get(&key) {
                Some(&g) => groups[g].1.push(row_idx as u32),
                None => {
                    group_idx.insert(key.clone(), groups.len());
                    groups.push((key, vec![row_idx as u32]));
                }
            }
        }

        for (key, indices) in groups {
            let part_batch = if indices.len() == batch.num_rows() {
                batch.clone()
            } else {
                take_record_batch(batch, &UInt32Array::from(indices))?
            };
            if !self.writers.contains_key(&key) {
                if self.writers.len() >= self.sink_ctx.max_open_files {
                    closed.push(self.close_largest()?);
                }
                self.writers
                    .insert(key.clone(), FileWriter::try_new(self.sink_ctx.clone())?);
            }
            let w = self.writers.get_mut(&key).expect("partition writer");
            w.write(&part_batch)?;
            if w.estimated_size() >= self.sink_ctx.target_file_size {
                let w = self.writers.remove(&key).expect("partition writer");
                closed.push(w.close()?);
            }
        }

        // bound memory held by open row groups across partitions
        while self.writers.len() > 1 && self.buffered_size() > self.sink_ctx.memory_budget {
            let (_, w) = self
                .writers
                .iter_mut()
                .max_by_key(|(_, w)| w.buffered_size())
                .expect("non-empty writers");
            w.flush_row_group()?;
            if w.buffered_size() > 0 {
                break;
            }
        }
        Ok(closed)
    }

    fn buffered_size(&self) -> usize {
        self.writers.values().map(|w| w.buffered_size()).sum()
    }

    fn close_largest(&mut self) -> Result<FileStat> {
        let key = self
            .writers
            .iter()
            .max_by_key(|(_, w)| w.estimated_size())
            .map(|(k, _)| k.clone())
            .expect("non-empty writers");
        self.writers.remove(&key).expect("partition writer").close()
    }

    fn close_all(&mut self) -> Result<Vec<FileStat>> {
        let mut closed = vec![];
        if let Some(w) = self.single.take() {
            closed.push(w.close()?);
        }
        for (_, w) in self.writers.drain() {
            closed.push(w.close()?);
        }
        Ok(closed)
    }
}

#[derive(Debug)]
struct FileStat {
    path: String,
    num_rows: usize,
    num_bytes: usize,
}

struct FileWriter {
    path: String,
    sink_ctx: Arc<PaimonSinkContext>,
    writer: ArrowWriter<FSDataWriter>,
    bytes_written: Count,
}

impl FileWriter {
    fn try_new(sink_ctx: Arc<PaimonSinkContext>) -> Result<Self> {
        let path = jni_get_string!(
            jni_call_static!(AuronNativePaimonSinkUtils.getTaskOutputPath() -> JObject)?
                .as_obj()
                .into()
        )?;
        log::info!("starts writing paimon data file: {path}");
        let fs = sink_ctx.fs_provider.provide(&path)?;
        let bytes_written = Count::new();
        let fout = Arc::into_inner(fs.create(&path)?).expect("Arc::into_inner");
        let writer = ArrowWriter::try_new(
            FSDataWriter::new(fout, &bytes_written),
            sink_ctx.data_schema.clone(),
            Some(sink_ctx.props.clone()),
        )?;
        Ok(Self {
            path,
            sink_ctx,
            writer,
            bytes_written,
        })
    }

    /// bytes already flushed to the file plus the encoded size of the open row
    /// group
    fn estimated_size(&self) -> usize {
        self.bytes_written.value() + self.writer.in_progress_size()
    }

    /// memory held by the open row group
    fn buffered_size(&self) -> usize {
        self.writer.in_progress_size()
    }

    fn flush_row_group(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer.write(batch)?;
        if self.writer.in_progress_size() >= self.sink_ctx.row_group_block_size {
            self.writer.flush()?;
        }
        Ok(())
    }

    fn close(self) -> Result<FileStat> {
        let mut writer = self.writer;
        writer.flush()?;
        let num_rows = writer
            .flushed_row_groups()
            .iter()
            .map(|rg| rg.num_rows() as usize)
            .sum();
        let data_writer = writer.into_inner()?;
        let num_bytes = data_writer.bytes_written.value();
        data_writer.close()?;
        let stat = FileStat {
            path: self.path,
            num_rows,
            num_bytes,
        };
        log::info!("finished writing paimon data file: {stat:?}");
        Ok(stat)
    }
}

struct FSDataWriter {
    inner: FsDataOutputWrapper,
    bytes_written: Count,
}

impl FSDataWriter {
    fn new(inner: FsDataOutputWrapper, bytes_written: &Count) -> Self {
        Self {
            inner,
            bytes_written: bytes_written.clone(),
        }
    }

    fn close(self) -> Result<()> {
        self.inner.close()
    }
}

impl Write for FSDataWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner
            .write_fully(buf)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
        self.bytes_written.add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
