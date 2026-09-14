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

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream, Statistics,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::common::execution_context::ExecutionContext;

#[derive(Debug)]
pub struct CoalesceExec {
    input: Arc<dyn ExecutionPlan>,
    num_partitions: usize,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl CoalesceExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, num_partitions: usize) -> Self {
        Self {
            input,
            num_partitions,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for CoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CoalesceExec[num_partitions={}]", self.num_partitions)
    }
}

impl ExecutionPlan for CoalesceExec {
    fn name(&self) -> &str {
        "CoalesceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
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
        Ok(Arc::new(CoalesceExec::new(
            children[0].clone(),
            self.num_partitions,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let mut input = exec_ctx.execute(&self.input)?;
        Ok(
            exec_ctx.output_with_sender("Coalesce", move |sender| async move {
                while let Some(batch) = input.next().await.transpose()? {
                    sender.send(batch).await;
                }
                Ok(())
            }),
        )
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}
