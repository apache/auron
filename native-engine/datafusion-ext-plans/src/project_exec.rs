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

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use datafusion::{
    common::Result,
    execution::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::downcast_any_dyn;
use futures::StreamExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;

use crate::common::{
    cached_exprs_evaluator::CachedExprsEvaluator,
    column_pruning::prune_columns,
    execution_context::ExecutionContext,
    lazy::{
        ColumnBatch, LazyBatch, LazyBatchRef, SendableLazyBatchStream, column, materialize_stream,
    },
    optimized_batch::{ExecuteOptimizedProps, into_lazy_stream},
    timer_helper::TimerHelper,
};

#[derive(Debug, Clone)]
pub struct ProjectExec {
    expr: Vec<(PhysicalExprRef, String)>,
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<Arc<PlanProperties>>,
}

impl ProjectExec {
    pub fn try_new(
        expr: Vec<(PhysicalExprRef, String)>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let schema = Arc::new(Schema::new(
            expr.iter()
                .map(|(e, name)| {
                    Ok(Field::new(
                        name,
                        e.data_type(&input_schema)?,
                        e.nullable(&input_schema)?,
                    ))
                })
                .collect::<Result<Fields>>()?,
        ));

        Ok(Self {
            expr,
            input,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }

    pub fn execute_optimized(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: Option<Vec<usize>>,
    ) -> Result<SendableLazyBatchStream> {
        let (schema, exprs) = match projection {
            Some(projection) => (
                Arc::new(self.schema.project(&projection)?),
                projection
                    .iter()
                    .map(|&i| self.expr[i].0.clone())
                    .collect::<Vec<_>>(),
            ),
            None => (
                self.schema(),
                self.expr.iter().map(|(expr, _)| expr.clone()).collect(),
            ),
        };
        let exec_ctx = ExecutionContext::new(context, partition, schema.clone(), &self.metrics);

        if let Ok(filter) = downcast_any_dyn!(self.input, crate::filter_exec::FilterExec) {
            return execute_project_with_filtering(
                filter.children()[0],
                exec_ctx,
                filter.predicates().to_vec(),
                exprs,
            );
        }

        let (exprs, projection) = prune_columns(&exprs)?;
        let mut input = into_lazy_stream(
            exec_ctx.execute_optimized(
                &self.input,
                &ExecuteOptimizedProps::default()
                    .with_projection(projection)
                    .with_input_stats(true),
            )?,
        );
        let evaluator = CachedExprsEvaluator::try_new(vec![], exprs.clone(), schema.clone())?;
        let output_ctx = exec_ctx.clone();
        let output =
            exec_ctx
                .clone()
                .output_lazy_with_sender("Project", move |sender| async move {
                    let exec_ctx = output_ctx;
                    let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
                    let _timer = elapsed_compute.timer();
                    sender.exclude_time(&elapsed_compute);

                    while let Some(batch) = elapsed_compute
                        .exclude_timer_async(input.next())
                        .await
                        .transpose()?
                    {
                        let output_batch = if pure_columns(batch.as_ref(), &exprs, &schema) {
                            evaluate_optimized(batch.as_ref(), &exprs, schema.clone())?
                        } else {
                            evaluator.evaluate_optimized(batch.as_ref())?
                        };
                        drop(batch);

                        exec_ctx
                            .baseline_metrics()
                            .record_output(output_batch.num_rows());
                        sender.send(output_batch).await;
                    }
                    Ok(())
                });
        Ok(output)
    }
}

impl DisplayAs for ProjectExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "ProjectExec [{}]",
            self.expr
                .iter()
                .map(|(e, name)| format!("{e} AS {name}"))
                .join(", ")
        )
    }
}

impl ExecutionPlan for ProjectExec {
    fn name(&self) -> &str {
        "ProjectExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.props.get_or_init(|| {
            Arc::new(PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
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
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::try_new(
            self.expr.clone(),
            children[0].clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(materialize_stream(
            self.execute_optimized(partition, context, None)?,
        ))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

fn execute_project_with_filtering(
    input: &Arc<dyn ExecutionPlan>,
    exec_ctx: Arc<ExecutionContext>,
    filters: Vec<PhysicalExprRef>,
    exprs: Vec<PhysicalExprRef>,
) -> Result<SendableLazyBatchStream> {
    let filter_count = filters.len();
    let (mut combined, read_columns) = prune_columns(&[filters, exprs].concat())?;
    let exprs = combined.split_off(filter_count);
    let evaluator = CachedExprsEvaluator::try_new(combined, exprs, exec_ctx.output_schema())?;
    let input = into_lazy_stream(
        exec_ctx.execute_optimized(
            input,
            &ExecuteOptimizedProps::default()
                .with_projection(read_columns)
                .with_input_stats(true),
        )?,
    );
    Ok(evaluate_project_with_filtering(input, evaluator, exec_ctx))
}

fn evaluate_project_with_filtering(
    mut input: SendableLazyBatchStream,
    evaluator: CachedExprsEvaluator,
    exec_ctx: Arc<ExecutionContext>,
) -> SendableLazyBatchStream {
    let output_ctx = exec_ctx.clone();
    exec_ctx.output_lazy_with_sender("Project", move |sender| async move {
        let elapsed_compute = output_ctx.baseline_metrics().elapsed_compute().clone();
        let _timer = elapsed_compute.timer();
        sender.exclude_time(&elapsed_compute);
        while let Some(batch) = elapsed_compute
            .exclude_timer_async(input.next())
            .await
            .transpose()?
        {
            let output = evaluator.evaluate_optimized(batch.as_ref())?;
            drop(batch);
            output_ctx
                .baseline_metrics()
                .record_output(output.num_rows());
            sender.send(output).await;
        }
        Ok(())
    })
}

fn evaluate_optimized(
    batch: &dyn LazyBatch,
    exprs: &[PhysicalExprRef],
    schema: SchemaRef,
) -> Result<LazyBatchRef> {
    if !pure_columns(batch, exprs, &schema) {
        return CachedExprsEvaluator::try_new(vec![], exprs.to_vec(), schema)?
            .evaluate_optimized(batch);
    }
    let columns = exprs
        .iter()
        .map(|expr| {
            batch.get_column(
                expr.downcast_ref::<Column>()
                    .expect("direct column")
                    .index(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    ColumnBatch::try_new(schema, columns, batch.num_rows())
}

fn pure_columns(batch: &dyn LazyBatch, exprs: &[PhysicalExprRef], schema: &SchemaRef) -> bool {
    exprs.iter().zip(schema.fields()).all(|(expr, field)| {
        expr.downcast_ref::<Column>()
            .is_some_and(|col| batch.schema().field(col.index()).data_type() == field.data_type())
    })
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrayRef, Int32Array, Int64Array},
        datatypes::DataType,
        record_batch::RecordBatch,
    };
    use datafusion::{
        common::ScalarValue,
        logical_expr::Operator,
        physical_expr::expressions::{BinaryExpr, Literal},
        physical_plan::{common::collect, test::TestMemoryExec},
        prelude::SessionContext,
    };

    use super::*;
    use crate::{common::optimized_batch::materialize_optimized_stream, filter_exec::FilterExec};

    #[test]
    fn computed_projection_keeps_passthrough_columns() -> Result<()> {
        let columns = arrays().into_iter().map(column).collect::<Vec<_>>();
        let batch = ColumnBatch::try_new(schema(), columns.clone(), 3)?;
        let _ = batch.get_estimated_materialized_mem_size();
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("next", DataType::Int32, false),
            Field::new("again", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
            Field::new("payload", DataType::Int32, false),
        ]));
        let output = evaluate_optimized(
            batch.as_ref(),
            &[
                increment(),
                increment(),
                value(),
                Arc::new(Column::new("payload", 2)),
            ],
            output_schema,
        )?;
        assert_eq!(
            output.get_column(2)?.materialize()?.as_ref(),
            columns[1].materialize()?.as_ref()
        );
        assert!(Arc::ptr_eq(&output.get_column(3)?, &columns[2]));
        let output_schema = Arc::new(schema().project(&[2, 2])?);
        evaluate_optimized(
            batch.as_ref(),
            &[
                Arc::new(Column::new("payload", 2)),
                Arc::new(Column::new("payload", 2)),
            ],
            output_schema,
        )?;
        Ok(())
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("unused", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
            Field::new("payload", DataType::Int32, false),
        ]))
    }

    fn arrays() -> Vec<ArrayRef> {
        vec![
            Arc::new(Int32Array::from(vec![90, 91, 92])),
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ]
    }

    fn value() -> PhysicalExprRef {
        Arc::new(Column::new("value", 1))
    }

    fn increment() -> PhysicalExprRef {
        Arc::new(BinaryExpr::new(
            value(),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ))
    }

    fn input() -> Result<Arc<dyn ExecutionPlan>> {
        let batch = RecordBatch::try_new(schema(), arrays())?;
        Ok(Arc::new(TestMemoryExec::try_new(
            &[vec![batch]],
            schema(),
            None,
        )?))
    }

    #[test]
    fn lazy_columns_preserve_identity_order_and_duplicates() -> Result<()> {
        let columns = arrays().into_iter().map(column).collect::<Vec<_>>();
        let batch = ColumnBatch::try_new(schema(), columns.clone(), 3)?;
        let exprs: Vec<PhysicalExprRef> = vec![
            Arc::new(Column::new("payload", 2)),
            value(),
            Arc::new(Column::new("payload", 2)),
        ];
        let output_schema = Arc::new(schema().project(&[2, 1, 2])?);
        let output = evaluate_optimized(batch.as_ref(), &exprs, output_schema.clone())?;
        assert_eq!(output.schema(), output_schema);
        assert_eq!(output.num_rows(), 3);
        assert!(Arc::ptr_eq(&output.get_column(0)?, &columns[2]));
        assert!(Arc::ptr_eq(&output.get_column(1)?, &columns[1]));
        assert!(Arc::ptr_eq(&output.get_column(2)?, &columns[2]));
        Ok(())
    }

    #[test]
    fn computing_a_column_keeps_passthrough_reference() -> Result<()> {
        let columns = arrays().into_iter().map(column).collect::<Vec<_>>();
        let batch = ColumnBatch::try_new(schema(), columns.clone(), 3)?;
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("next", DataType::Int32, false),
            Field::new("renamed", DataType::Int32, false),
            Field::new("payload", DataType::Int32, false),
        ]));
        let output = evaluate_optimized(
            batch.as_ref(),
            &[increment(), value(), Arc::new(Column::new("payload", 2))],
            output_schema,
        )?;
        assert_eq!(
            output.get_column(1)?.materialize()?.as_ref(),
            columns[1].materialize()?.as_ref()
        );
        assert!(Arc::ptr_eq(&output.get_column(2)?, &columns[2]));
        let materialized = output.materialize()?;
        assert_eq!(
            materialized.column(0).as_ref(),
            &Int32Array::from(vec![2, 3, 4])
        );
        Ok(())
    }

    #[test]
    fn lazy_project_casts_to_target_field() -> Result<()> {
        let batch = ColumnBatch::try_new(schema(), arrays().into_iter().map(column).collect(), 3)?;
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("next", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let output = evaluate_optimized(batch.as_ref(), &[increment(), value()], output_schema)?;
        let materialized = output.materialize()?;
        assert_eq!(
            materialized.column(0).as_ref(),
            &Int64Array::from(vec![2, 3, 4])
        );
        assert_eq!(
            materialized.column(1).as_ref(),
            &Int64Array::from(vec![1, 2, 3])
        );
        Ok(())
    }

    #[test]
    fn lazy_project_preserves_rows_without_columns() -> Result<()> {
        let batch = ColumnBatch::try_new(schema(), arrays().into_iter().map(column).collect(), 3)?;
        let empty = evaluate_optimized(batch.as_ref(), &[], Arc::new(Schema::empty()))?;
        assert_eq!(empty.num_rows(), 3);
        assert_eq!(empty.materialize()?.num_rows(), 3);
        let literal: PhysicalExprRef = Arc::new(Literal::new(ScalarValue::Int32(Some(7))));
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "constant",
            DataType::Int32,
            false,
        )]));
        let output = evaluate_optimized(empty.as_ref(), &[literal], output_schema)?;
        assert_eq!(
            output.materialize()?.column(0).as_ref(),
            &Int32Array::from(vec![7, 7, 7])
        );
        Ok(())
    }

    #[tokio::test]
    async fn optimized_and_legacy_project_agree_after_pruning() -> Result<()> {
        let project: Arc<dyn ExecutionPlan> = Arc::new(ProjectExec::try_new(
            vec![
                (increment(), "next".to_owned()),
                (Arc::new(Column::new("payload", 2)), "payload".to_owned()),
                (value(), "renamed".to_owned()),
            ],
            input()?,
        )?);
        let context = SessionContext::new();
        let batches = collect(project.execute(0, context.task_ctx())?).await?;
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert_eq!(
            batches[0].column(0).as_ref(),
            &Int32Array::from(vec![2, 3, 4])
        );
        let projection = vec![2, 0, 2];
        let exec_ctx = ExecutionContext::new(
            context.task_ctx(),
            0,
            Arc::new(project.schema().project(&projection)?),
            &ExecutionPlanMetricsSet::new(),
        );
        let stream = exec_ctx.execute_optimized(
            &project,
            &ExecuteOptimizedProps::default().with_projection(projection.clone()),
        )?;
        assert_eq!(
            stream.schema(),
            Arc::new(project.schema().project(&projection)?)
        );
        let optimized = collect(materialize_optimized_stream(stream)).await?;
        let legacy = collect(project.execute(0, context.task_ctx())?)
            .await?
            .iter()
            .map(|batch| Ok(batch.project(&projection)?))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(optimized, legacy);
        assert_eq!(optimized[0], batches[0].project(&projection)?);
        let empty_ctx = exec_ctx.with_new_output_schema(Arc::new(project.schema().project(&[])?));
        let empty = collect(materialize_optimized_stream(empty_ctx.execute_optimized(
            &project,
            &ExecuteOptimizedProps::default().with_projection(vec![]),
        )?))
        .await?;
        assert_eq!(empty.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        assert_eq!(empty[0].num_columns(), 0);
        let constant = ProjectExec::try_new(
            vec![(
                Arc::new(Literal::new(ScalarValue::Int32(Some(7)))),
                "constant".to_owned(),
            )],
            input()?,
        )?;
        let constant_batches = collect(constant.execute(0, context.task_ctx())?).await?;
        assert_eq!(
            constant_batches[0].column(0).as_ref(),
            &Int32Array::from(vec![7, 7, 7])
        );
        Ok(())
    }

    #[tokio::test]
    async fn optimized_filter_child_projects_without_recursion() -> Result<()> {
        let predicate: PhysicalExprRef = Arc::new(BinaryExpr::new(
            value(),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));
        let filter = Arc::new(FilterExec::try_new(vec![predicate], input()?)?);
        let mut project = ProjectExec::try_new(
            vec![
                (increment(), "next".to_owned()),
                (Arc::new(Column::new("payload", 2)), "payload".to_owned()),
            ],
            filter,
        )?;
        project.schema = Arc::new(Schema::new_with_metadata(
            project.schema.fields().clone(),
            std::collections::HashMap::from([("fusion".to_owned(), "project".to_owned())]),
        ));
        let project: Arc<dyn ExecutionPlan> = Arc::new(project);
        let context = SessionContext::new();
        let exec_ctx = ExecutionContext::new(
            context.task_ctx(),
            0,
            Arc::new(project.schema().project(&[1, 0])?),
            &ExecutionPlanMetricsSet::new(),
        );
        let stream = exec_ctx.execute_optimized(
            &project,
            &ExecuteOptimizedProps::default().with_projection(vec![1, 0]),
        )?;
        assert_eq!(
            stream.schema(),
            Arc::new(project.schema().project(&[1, 0])?)
        );
        let optimized = collect(materialize_optimized_stream(stream)).await?;
        let legacy = collect(project.execute(0, context.task_ctx())?)
            .await?
            .iter()
            .map(|batch| Ok(batch.project(&[1, 0])?))
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(optimized, legacy);
        assert_eq!(
            optimized[0].column(0).as_ref(),
            &Int32Array::from(vec![20, 30])
        );
        assert_eq!(
            optimized[0].column(1).as_ref(),
            &Int32Array::from(vec![3, 4])
        );
        let batches = collect(project.execute(0, context.task_ctx())?).await?;
        assert_eq!(batches[0].column(0).as_ref(), &Int32Array::from(vec![3, 4]));
        Ok(())
    }
}
