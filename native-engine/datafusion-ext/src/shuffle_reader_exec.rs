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

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::Result as ArrowResult;
use datafusion::arrow::ipc::reader::FileReader;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::DisplayFormatType;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::Statistics;
use futures::Stream;
use jni::errors::Result as JniResult;
use jni::objects::JObject;

use crate::jni_bridge::JavaClasses;
use crate::jni_bridge_call_method;
use crate::jni_bridge_call_static_method;
use crate::util::Util;

#[derive(Debug, Clone)]
pub struct ShuffleReaderExec {
    pub native_shuffle_id: String,
    pub schema: SchemaRef,
    pub metrics: ExecutionPlanMetricsSet,
}
impl ShuffleReaderExec {
    pub fn new(native_shuffle_id: String, schema: SchemaRef) -> ShuffleReaderExec {
        ShuffleReaderExec {
            native_shuffle_id,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleReaderExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "Blaze ShuffleReaderExec does not support with_new_children()".to_owned(),
        ))
    }

    async fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let segments = Util::to_datafusion_external_result(Ok(()).and_then(|_| {
            let env = JavaClasses::get_thread_jnienv();
            let segments = jni_bridge_call_static_method!(
                env,
                JniBridge.getResource,
                env.new_string(&self.native_shuffle_id)?
            )?
            .l()?;
            JniResult::Ok(segments)
        }))?;
        let schema = self.schema.clone();
        let baseline_metrics = BaselineMetrics::new(&self.metrics, 0);
        Ok(Box::pin(ShuffleReaderStream::new(
            schema,
            segments,
            baseline_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

struct ShuffleReaderStream {
    schema: SchemaRef,
    segments: JObject<'static>,
    current_segment: JObject<'static>,
    arrow_file_reader: Option<FileReader<SeekableByteChannelReader>>,
    baseline_metrics: BaselineMetrics,
}
unsafe impl Sync for ShuffleReaderStream {} // safety: segments is safe to be shared
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for ShuffleReaderStream {}

impl ShuffleReaderStream {
    pub fn new(
        schema: SchemaRef,
        segments: JObject<'static>,
        baseline_metrics: BaselineMetrics,
    ) -> ShuffleReaderStream {
        ShuffleReaderStream {
            schema,
            segments,
            current_segment: JObject::null(),
            arrow_file_reader: None,
            baseline_metrics,
        }
    }

    fn next_segment(&mut self) -> Result<bool> {
        let next_segment = Util::to_datafusion_external_result(Ok(()).and_then(|_| {
            let env = JavaClasses::get_thread_jnienv();

            let has_next =
                jni_bridge_call_method!(env, ScalaIterator.hasNext, self.segments)?
                    .z()?;
            if !has_next {
                self.current_segment = JObject::null();
                self.arrow_file_reader = None;
                return JniResult::Ok(false);
            }

            let next_segment =
                jni_bridge_call_method!(env, ScalaIterator.next, self.segments)?.l()?;
            self.current_segment = next_segment;
            JniResult::Ok(true)
        }))?;

        if next_segment {
            self.arrow_file_reader = Some(FileReader::try_new(
                SeekableByteChannelReader(self.current_segment),
                None,
            )?);
            return Ok(true);
        }
        Ok(false)
    }
}

impl Stream for ShuffleReaderStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(arrow_file_reader) = &mut self.arrow_file_reader {
            if let Some(record_batch) = arrow_file_reader.next() {
                return self
                    .baseline_metrics
                    .record_poll(Poll::Ready(Some(record_batch)));
            }
        }

        // current arrow file reader reaches EOF, try next ipc
        if self.next_segment().unwrap() {
            return self.poll_next(cx);
        }
        Poll::Ready(None)
    }
}
impl RecordBatchStream for ShuffleReaderStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

struct SeekableByteChannelReader(JObject<'static>);
impl Read for SeekableByteChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Ok(())
            .and_then(|_| {
                let env = JavaClasses::get_thread_jnienv();
                return JniResult::Ok(
                    jni_bridge_call_method!(
                        env,
                        JavaNioSeekableByteChannel.read,
                        self.0,
                        env.new_direct_byte_buffer(buf)?
                    )?
                    .i()? as usize,
                );
            })
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "JNI error: SeekableByteChannelReader.jni_read",
                )
            })
    }
}
impl Seek for SeekableByteChannelReader {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        Ok(())
            .and_then(|_| {
                let env = JavaClasses::get_thread_jnienv();
                match pos {
                    SeekFrom::Start(position) => {
                        jni_bridge_call_method!(
                            env,
                            JavaNioSeekableByteChannel.setPosition,
                            self.0,
                            position as i64
                        )?;
                        JniResult::Ok(position)
                    }

                    SeekFrom::End(offset) => {
                        let size = jni_bridge_call_method!(
                            env,
                            JavaNioSeekableByteChannel.size,
                            self.0
                        )?
                        .j()? as u64;
                        let position = size + offset as u64;
                        jni_bridge_call_method!(
                            env,
                            JavaNioSeekableByteChannel.setPosition,
                            self.0,
                            position as i64
                        )?;
                        JniResult::Ok(position)
                    }

                    SeekFrom::Current(_) => unimplemented!(),
                }
            })
            .map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "JNI error: SeekableByteChannelReader.jni_seek",
                )
            })
    }
}
