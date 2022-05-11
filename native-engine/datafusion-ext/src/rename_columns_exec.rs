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

use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;

use datafusion::physical_expr::PhysicalSortExpr;

use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use futures::{Stream, StreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug, Clone)]
pub struct RenameColumnsExec {
    input: Arc<dyn ExecutionPlan>,
    renamed_column_names: Vec<String>,
    renamed_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
}

impl RenameColumnsExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        renamed_column_names: Vec<String>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        if renamed_column_names.len() != input_schema.fields().len() {
            return Err(DataFusionError::Plan(
                "renamed_column_names length not matched with input schema".to_string(),
            ));
        }

        let renamed_schema = Arc::new(Schema::new(
            renamed_column_names
                .iter()
                .zip(input_schema.fields())
                .map(|(new_name, field)| {
                    Field::new(new_name, field.data_type().clone(), field.is_nullable())
                })
                .collect(),
        ));

        Ok(Self {
            input,
            renamed_column_names,
            renamed_schema,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

#[async_trait]
impl ExecutionPlan for RenameColumnsExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.renamed_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.input.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(
                "RenameColumnsExec expects one children".to_string(),
            ));
        }
        Ok(Arc::new(RenameColumnsExec::try_new(
            children[0].clone(),
            self.renamed_column_names.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, 0);
        Ok(Box::pin(RenameColumnsStream::new(
            input,
            self.schema(),
            baseline_metrics,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(f, "RenameColumnsExec: {:?}", &self.renamed_column_names)
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

struct RenameColumnsStream {
    input: SendableRecordBatchStream,
    schema: SchemaRef,
    baseline_metrics: BaselineMetrics,
}

impl RenameColumnsStream {
    pub fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
    ) -> RenameColumnsStream {
        RenameColumnsStream {
            input,
            schema,
            baseline_metrics,
        }
    }
}

impl RecordBatchStream for RenameColumnsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RenameColumnsStream {
    type Item = datafusion::arrow::error::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx)? {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(batch)) => {
                self.baseline_metrics.record_poll(Poll::Ready(Some(
                    RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec()),
                )))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.input.size_hint()
    }
}
