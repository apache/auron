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

//! Defines the External shuffle repartition plan

use std::{any::Any, fmt::Debug, sync::Arc};

use arrow::datatypes::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use auron_memmgr::MemManager;
use datafusion::{
    error::Result,
    execution::context::TaskContext,
    logical_expr::Volatility,
    physical_expr::{
        EquivalenceProperties, PhysicalExprRef, ScalarFunctionExpr, expressions::Column,
    },
    physical_plan,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        Statistics,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
    prelude::create_udf,
};
use datafusion_ext_commons::df_execution_err;
use once_cell::sync::OnceCell;

use crate::{
    common::execution_context::ExecutionContext,
    shuffle::{
        Partitioning, ShuffleRepartitioner, single_repartitioner::SingleShuffleRepartitioner,
        sort_repartitioner::SortShuffleRepartitioner,
    },
    sort_exec::create_default_ascending_sort_exec,
};

fn contains_map(data_type: &DataType) -> bool {
    match data_type {
        DataType::Map(..) => true,
        DataType::List(field) => contains_map(field.data_type()),
        DataType::Struct(fields) => fields.iter().any(|field| contains_map(field.data_type())),
        _ => false,
    }
}

pub(crate) fn create_round_robin_sort_exec(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let sort_exprs = if input
        .schema()
        .fields()
        .iter()
        .any(|field| contains_map(field.data_type()))
    {
        let function_name = "Spark_Murmur3Hash";
        let function =
            datafusion_ext_functions::create_auron_ext_function(function_name, partition)?;
        let args = input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| Arc::new(Column::new(field.name(), index)) as PhysicalExprRef)
            .collect::<Vec<_>>();
        let udf = Arc::new(create_udf(
            function_name,
            args.iter()
                .map(|expr| expr.data_type(&input.schema()))
                .collect::<Result<Vec<_>>>()?,
            DataType::Int32,
            Volatility::Immutable,
            function,
        ));
        vec![Arc::new(ScalarFunctionExpr::new(
            function_name,
            udf,
            args,
            Arc::new(Field::new("round_robin_sort_hash", DataType::Int32, false)),
        )) as PhysicalExprRef]
    } else {
        input
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| Arc::new(Column::new(field.name(), index)) as PhysicalExprRef)
            .collect::<Vec<_>>()
    };
    Ok(create_default_ascending_sort_exec(
        input,
        &sort_exprs,
        None,
        false, // do not record output metric
    ))
}

/// The shuffle writer operator maps each input partition to M output partitions
/// based on a partitioning scheme. No guarantees are made about the order of
/// the resulting partitions.
#[derive(Debug)]
pub struct ShuffleWriterExec {
    input: Arc<dyn ExecutionPlan>,
    partitioning: Partitioning,
    output_data_file: String,
    output_index_file: String,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl DisplayAs for ShuffleWriterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ShuffleWriterExec: partitioning={:?}", self.partitioning)
    }
}

#[async_trait]
impl ExecutionPlan for ShuffleWriterExec {
    fn name(&self) -> &str {
        "ShuffleWriterExec"
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
                physical_plan::Partitioning::UnknownPartitioning(1),
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
        match children.len() {
            1 => Ok(Arc::new(ShuffleWriterExec::try_new(
                children[0].clone(),
                self.partitioning.clone(),
                self.output_data_file.clone(),
                self.output_index_file.clone(),
            )?)),
            _ => df_execution_err!("ShuffleWriterExec wrong number of children"),
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // record uncompressed data size
        let exec_ctx =
            ExecutionContext::new(context.clone(), partition, self.schema(), &self.metrics);
        let output_time = exec_ctx.register_timer_metric("output_io_time");

        let mut input = self.input.clone();

        let repartitioner: Arc<dyn ShuffleRepartitioner> = match &self.partitioning {
            p if p.partition_count() == 1 => Arc::new(SingleShuffleRepartitioner::new(
                self.output_data_file.clone(),
                self.output_index_file.clone(),
                output_time,
            )),
            Partitioning::HashPartitioning(..) | Partitioning::RangePartitioning(..) => {
                let partitioner = Arc::new(SortShuffleRepartitioner::new(
                    exec_ctx.clone(),
                    self.output_data_file.clone(),
                    self.output_index_file.clone(),
                    self.partitioning.clone(),
                    output_time,
                ));
                MemManager::register_consumer(partitioner.clone(), true);
                partitioner
            }
            Partitioning::RoundRobinPartitioning(..) => {
                input = create_round_robin_sort_exec(input, partition)?;
                let partitioner = Arc::new(SortShuffleRepartitioner::new(
                    exec_ctx.clone(),
                    self.output_data_file.clone(),
                    self.output_index_file.clone(),
                    self.partitioning.clone(),
                    output_time,
                ));
                MemManager::register_consumer(partitioner.clone(), true);
                partitioner
            }
            p => unreachable!("unsupported partitioning: {:?}", p),
        };

        let input = exec_ctx.execute_with_input_stats(&input)?;
        repartitioner.execute(exec_ctx, input)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.input.partition_statistics(None)
    }
}

impl ShuffleWriterExec {
    /// Create a new ShuffleWriterExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partitioning: Partitioning,
        output_data_file: String,
        output_index_file: String,
    ) -> Result<Self> {
        Ok(ShuffleWriterExec {
            input,
            partitioning,
            metrics: ExecutionPlanMetricsSet::new(),
            output_data_file,
            output_index_file,
            props: OnceCell::new(),
        })
    }
}
