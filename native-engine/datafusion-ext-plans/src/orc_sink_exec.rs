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

use std::{
    any::Any,
    fmt::Formatter,
    io::Write,
    sync::{Arc, mpsc},
};

use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use auron_jni_bridge::{jni_call_static, jni_get_string, jni_new_global_ref, jni_new_string};
use datafusion::{
    common::{Result, ScalarValue, Statistics},
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{Count, ExecutionPlanMetricsSet, MetricsSet, Time},
    },
};
use datafusion_ext_commons::{
    arrow::{array_size::BatchSize, cast::cast},
    df_execution_err,
    hadoop_fs::{FsDataOutputWrapper, FsProvider},
};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use orc_rust::ArrowWriterBuilder;
use tokio::sync::oneshot;

use crate::common::execution_context::ExecutionContext;

#[derive(Debug)]
pub struct OrcSinkExec {
    fs_resource_id: String,
    input: Arc<dyn ExecutionPlan>,
    num_dyn_parts: usize,
    schema: SchemaRef,
    props: Vec<(String, String)>,
    metrics: ExecutionPlanMetricsSet,
    plan_props: OnceCell<PlanProperties>,
}

impl OrcSinkExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        fs_resource_id: String,
        num_dyn_parts: usize,
        schema: SchemaRef,
        props: Vec<(String, String)>,
    ) -> Self {
        Self {
            input,
            fs_resource_id,
            num_dyn_parts,
            schema,
            props,
            metrics: ExecutionPlanMetricsSet::new(),
            plan_props: OnceCell::new(),
        }
    }
}

impl DisplayAs for OrcSinkExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "OrcSink")
    }
}

impl ExecutionPlan for OrcSinkExec {
    fn name(&self) -> &str {
        "OrcSinkExec"
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
            self.num_dyn_parts,
            self.schema.clone(),
            self.props.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
        let _timer = elapsed_compute.timer();
        let io_time = exec_ctx.register_timer_metric("io_time");

        let orc_sink_context = Arc::new(OrcSinkContext::try_new(
            &self.fs_resource_id,
            self.num_dyn_parts,
            self.schema.clone(),
            &io_time,
            &self.props,
        )?);

        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        execute_orc_sink(orc_sink_context, input, exec_ctx)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

struct OrcSinkContext {
    fs_provider: FsProvider,
    schema: SchemaRef,
    num_dyn_parts: usize,
    batch_size: usize,
    stripe_byte_size: usize,
}

impl OrcSinkContext {
    fn try_new(
        fs_resource_id: &str,
        num_dyn_parts: usize,
        schema: SchemaRef,
        io_time: &Time,
        props: &[(String, String)],
    ) -> Result<Self> {
        let fs_provider = {
            let resource_id = jni_new_string!(&fs_resource_id)?;
            let fs = jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
            FsProvider::new(jni_new_global_ref!(fs.as_obj())?, io_time)
        };

        let batch_size = props
            .iter()
            .find(|(key, _)| key == "orc.row.batch.size")
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .unwrap_or(1024);
        let stripe_byte_size = props
            .iter()
            .find(|(key, _)| key == "orc.stripe.size")
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .unwrap_or(64 * 1024 * 1024);

        Ok(Self {
            fs_provider,
            schema,
            num_dyn_parts,
            batch_size,
            stripe_byte_size,
        })
    }
}

fn execute_orc_sink(
    orc_sink_context: Arc<OrcSinkContext>,
    mut input: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    let bytes_written = exec_ctx.register_counter_metric("bytes_written");

    Ok(exec_ctx
        .clone()
        .output_with_sender("OrcSink", move |sender| async move {
            let (part_writer_tx, part_writer_rx) = mpsc::channel();
            let part_writer_handle = {
                let orc_sink_context = orc_sink_context.clone();
                tokio::task::spawn_blocking(move || {
                    part_writer_worker_loop(orc_sink_context, part_writer_rx)
                })
            };
            let mut active_part_values: Option<Vec<ScalarValue>> = None;

            macro_rules! part_writer_init {
                ($batch:expr, $part_values:expr) => {{
                    log::info!("starts writing partition: {:?}", $part_values);
                    sender.send($batch.slice(0, 1)).await;
                    open_part_writer(&part_writer_tx, $part_values.to_vec()).await?;
                    active_part_values = Some($part_values.to_vec());
                }};
            }
            macro_rules! part_writer_close {
                () => {{
                    if active_part_values.take().is_some() {
                        if let Some(file_stat) = close_part_writer(&part_writer_tx).await? {
                            jni_call_static!(
                                AuronNativeOrcSinkUtils.completeOutput(
                                    jni_new_string!(&file_stat.path)?.as_obj(),
                                    file_stat.num_rows as i64,
                                    file_stat.num_bytes as i64,
                                ) -> ()
                            )?;
                            exec_ctx.baseline_metrics().output_rows().add(file_stat.num_rows);
                            bytes_written.add(file_stat.num_bytes);
                        }
                    }
                }}
            }

            while let Some(mut batch) = input.next().await.transpose()? {
                let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                if batch.num_rows() == 0 {
                    continue;
                }

                while batch.num_rows() > 0 {
                    let part_values =
                        get_dyn_part_values(&batch, orc_sink_context.num_dyn_parts, 0)?;
                    let part_writer_outdated = active_part_values.as_ref() != Some(&part_values);

                    if part_writer_outdated {
                        part_writer_close!();
                        part_writer_init!(batch, &part_values);
                        continue;
                    }

                    let batch_mem_size = batch.get_batch_mem_size();
                    let num_sub_batches = (batch_mem_size / 1048576).max(1);
                    let num_sub_batch_rows = (batch.num_rows() / num_sub_batches).max(16);

                    let m = rfind_part_values(&batch, &part_values)?;
                    let cur_batch = batch.slice(0, m);
                    batch = batch.slice(m, batch.num_rows() - m);

                    let cur_batch = adapt_schema(&cur_batch, &orc_sink_context.schema)?;
                    let mut offset = 0;
                    while offset < cur_batch.num_rows() {
                        let sub_batch_size = num_sub_batch_rows.min(cur_batch.num_rows() - offset);
                        let sub_batch = cur_batch.slice(offset, sub_batch_size);
                        offset += sub_batch_size;

                        write_part_writer(&part_writer_tx, sub_batch).await?;
                    }
                }
            }
            part_writer_close!();
            shutdown_part_writer(&part_writer_tx).await?;
            part_writer_handle
                .await
                .or_else(|e| df_execution_err!("orc writer thread error: {e}"))??;
            Ok(())
        }))
}

enum PartWriterCommand {
    Open {
        part_values: Vec<ScalarValue>,
        response: oneshot::Sender<Result<()>>,
    },
    Write {
        batch: RecordBatch,
        response: oneshot::Sender<Result<()>>,
    },
    Close {
        response: oneshot::Sender<Result<Option<PartFileStat>>>,
    },
    Shutdown {
        response: oneshot::Sender<Result<()>>,
    },
}

fn part_writer_worker_loop(
    orc_sink_context: Arc<OrcSinkContext>,
    command_rx: mpsc::Receiver<PartWriterCommand>,
) -> Result<()> {
    let mut part_writer: Option<PartWriter> = None;
    while let Ok(command) = command_rx.recv() {
        match command {
            PartWriterCommand::Open {
                part_values,
                response,
            } => {
                let result = (|| -> Result<()> {
                    if part_writer.is_some() {
                        return df_execution_err!(
                            "opening orc file error: partition writer already open"
                        );
                    }
                    part_writer = Some(
                        PartWriter::try_new(orc_sink_context.clone(), &part_values)
                            .or_else(|e| df_execution_err!("opening orc file error: {e}"))?,
                    );
                    Ok(())
                })();
                let _ = response.send(result);
            }
            PartWriterCommand::Write { batch, response } => {
                let result = match part_writer.as_mut() {
                    Some(writer) => writer
                        .write(&batch)
                        .or_else(|e| df_execution_err!("writing orc file error: {e}")),
                    None => df_execution_err!("writing orc file error: missing partition writer"),
                };
                let _ = response.send(result);
            }
            PartWriterCommand::Close { response } => {
                let result = close_current_part_writer(&mut part_writer)
                    .or_else(|e| df_execution_err!("closing orc file error: {e}"));
                let _ = response.send(result);
            }
            PartWriterCommand::Shutdown { response } => {
                let result = close_current_part_writer(&mut part_writer)
                    .or_else(|e| df_execution_err!("closing orc file error: {e}"))
                    .map(|_| ());
                let _ = response.send(result);
                break;
            }
        }
    }
    close_current_part_writer(&mut part_writer).map(|_| ())?;
    Ok(())
}

fn close_current_part_writer(part_writer: &mut Option<PartWriter>) -> Result<Option<PartFileStat>> {
    part_writer.take().map(|writer| writer.close()).transpose()
}

async fn open_part_writer(
    part_writer_tx: &mpsc::Sender<PartWriterCommand>,
    part_values: Vec<ScalarValue>,
) -> Result<()> {
    let (response_tx, response_rx) = oneshot::channel();
    part_writer_tx
        .send(PartWriterCommand::Open {
            part_values,
            response: response_tx,
        })
        .or_else(|e| df_execution_err!("opening orc writer command error: {e}"))?;
    response_rx
        .await
        .or_else(|e| df_execution_err!("opening orc writer response error: {e}"))?
}

async fn write_part_writer(
    part_writer_tx: &mpsc::Sender<PartWriterCommand>,
    batch: RecordBatch,
) -> Result<()> {
    let (response_tx, response_rx) = oneshot::channel();
    part_writer_tx
        .send(PartWriterCommand::Write {
            batch,
            response: response_tx,
        })
        .or_else(|e| df_execution_err!("writing orc writer command error: {e}"))?;
    response_rx
        .await
        .or_else(|e| df_execution_err!("writing orc writer response error: {e}"))?
}

async fn close_part_writer(
    part_writer_tx: &mpsc::Sender<PartWriterCommand>,
) -> Result<Option<PartFileStat>> {
    let (response_tx, response_rx) = oneshot::channel();
    part_writer_tx
        .send(PartWriterCommand::Close {
            response: response_tx,
        })
        .or_else(|e| df_execution_err!("closing orc writer command error: {e}"))?;
    response_rx
        .await
        .or_else(|e| df_execution_err!("closing orc writer response error: {e}"))?
}

async fn shutdown_part_writer(part_writer_tx: &mpsc::Sender<PartWriterCommand>) -> Result<()> {
    let (response_tx, response_rx) = oneshot::channel();
    part_writer_tx
        .send(PartWriterCommand::Shutdown {
            response: response_tx,
        })
        .or_else(|e| df_execution_err!("shutting down orc writer command error: {e}"))?;
    response_rx
        .await
        .or_else(|e| df_execution_err!("shutting down orc writer response error: {e}"))?
}

fn adapt_schema(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let mut casted_cols = vec![];

    for (col_idx, casted_field) in schema.fields().iter().enumerate() {
        casted_cols.push(cast(batch.column(col_idx), casted_field.data_type())?);
    }
    Ok(RecordBatch::try_new_with_options(
        schema.clone(),
        casted_cols,
        &RecordBatchOptions::new().with_row_count(Some(num_rows)),
    )?)
}

fn rfind_part_values(batch: &RecordBatch, part_values: &[ScalarValue]) -> Result<usize> {
    for row_idx in (0..batch.num_rows()).rev() {
        if get_dyn_part_values(batch, part_values.len(), row_idx)? == part_values {
            return Ok(row_idx + 1);
        }
    }
    Ok(0)
}

#[derive(Debug)]
struct PartFileStat {
    path: String,
    num_rows: usize,
    num_bytes: usize,
}

struct PartWriter {
    path: String,
    _orc_sink_context: Arc<OrcSinkContext>,
    orc_writer: orc_rust::ArrowWriter<FSDataWriter>,
    part_values: Vec<ScalarValue>,
    rows_written: Count,
    bytes_written: Count,
}

impl PartWriter {
    fn try_new(orc_sink_context: Arc<OrcSinkContext>, part_values: &[ScalarValue]) -> Result<Self> {
        if !part_values.is_empty() {
            log::info!("starts outputting dynamic partition: {part_values:?}");
        }
        let part_file = jni_get_string!(
            jni_call_static!(AuronNativeOrcSinkUtils.getTaskOutputPath() -> JObject)?
                .as_obj()
                .into()
        )?;
        log::info!("starts writing orc file: {part_file}");

        let fs = orc_sink_context.fs_provider.provide(&part_file)?;
        let bytes_written = Count::new();
        let rows_written = Count::new();
        let fout = Arc::into_inner(fs.create(&part_file)?).expect("Arc::into_inner");
        let data_writer = FSDataWriter::new(fout, &bytes_written);
        let orc_writer = ArrowWriterBuilder::new(data_writer, orc_sink_context.schema.clone())
            .with_batch_size(orc_sink_context.batch_size)
            .with_stripe_byte_size(orc_sink_context.stripe_byte_size)
            .try_build()
            .or_else(|e| df_execution_err!("building orc writer error: {e}"))?;
        Ok(Self {
            path: part_file,
            _orc_sink_context: orc_sink_context,
            orc_writer,
            part_values: part_values.to_vec(),
            rows_written,
            bytes_written,
        })
    }

    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.orc_writer
            .write(batch)
            .or_else(|e| df_execution_err!("encoding orc batch error: {e}"))?;
        self.rows_written.add(batch.num_rows());
        Ok(())
    }

    fn close(self) -> Result<PartFileStat> {
        let Self {
            path,
            _orc_sink_context: _,
            orc_writer,
            part_values: _,
            rows_written,
            bytes_written,
        } = self;

        orc_writer
            .close()
            .or_else(|e| df_execution_err!("closing orc writer error: {e}"))?;

        let stat = PartFileStat {
            path,
            num_rows: rows_written.value(),
            num_bytes: bytes_written.value(),
        };
        log::info!("finished writing orc file: {stat:?}");
        Ok(stat)
    }
}

fn get_dyn_part_values(
    batch: &RecordBatch,
    num_dyn_parts: usize,
    row_idx: usize,
) -> Result<Vec<ScalarValue>> {
    batch
        .columns()
        .iter()
        .skip(batch.num_columns() - num_dyn_parts)
        .map(|part_col| ScalarValue::try_from_array(part_col, row_idx))
        .collect()
}

struct FSDataWriter {
    inner: FsDataOutputWrapper,
    bytes_written: Count,
}

impl FSDataWriter {
    pub fn new(inner: FsDataOutputWrapper, bytes_written: &Count) -> Self {
        Self {
            inner,
            bytes_written: bytes_written.clone(),
        }
    }
}

impl Write for FSDataWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.inner
            .write_fully(&buf)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))?;
        self.bytes_written.add(buf.len());
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
