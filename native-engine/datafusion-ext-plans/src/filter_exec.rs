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

use arrow::datatypes::{DataType, SchemaRef};
use datafusion::{
    common::Result,
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalExprRef, expressions::Column},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use datafusion_ext_commons::df_execution_err;
use futures::StreamExt;
use itertools::Itertools;
use once_cell::sync::OnceCell;

use crate::common::{
    cached_exprs_evaluator::CachedExprsEvaluator,
    column_pruning::prune_columns,
    execution_context::ExecutionContext,
    lazy::{SendableLazyBatchStream, materialize_stream},
    optimized_batch::{ExecuteOptimizedProps, into_lazy_stream},
    timer_helper::TimerHelper,
};

#[derive(Debug, Clone)]
pub struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicates: Vec<PhysicalExprRef>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<Arc<PlanProperties>>,
}

impl FilterExec {
    pub fn try_new(
        predicates: Vec<PhysicalExprRef>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        let schema = input.schema();

        if predicates.is_empty() {
            df_execution_err!("Filter requires at least one predicate")?;
        }
        if !predicates
            .iter()
            .all(|pred| matches!(pred.data_type(&schema), Ok(DataType::Boolean)))
        {
            df_execution_err!("Filter predicate must return boolean values")?;
        }
        Ok(Self {
            input,
            predicates,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }

    pub fn predicates(&self) -> &[PhysicalExprRef] {
        &self.predicates
    }

    pub fn execute_optimized(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: Option<Vec<usize>>,
    ) -> Result<SendableLazyBatchStream> {
        let input_schema = self.schema();
        let projection = projection.unwrap_or_else(|| (0..input_schema.fields().len()).collect());
        let schema = Arc::new(input_schema.project(&projection)?);
        let mut exprs = self.predicates.clone();
        exprs.extend(projection.iter().map(|&index| {
            Arc::new(Column::new(input_schema.field(index).name(), index)) as PhysicalExprRef
        }));
        let (exprs, read_projection) = prune_columns(&exprs)?;
        let predicates = exprs[..self.predicates.len()].to_vec();
        let output_projection = exprs[self.predicates.len()..].to_vec();
        let exec_ctx = ExecutionContext::new(context, partition, schema.clone(), &self.metrics);
        let input = into_lazy_stream(
            exec_ctx.execute_optimized(
                &self.input,
                &ExecuteOptimizedProps::default()
                    .with_projection(read_projection)
                    .with_input_stats(true),
            )?,
        );
        execute_filter(input, predicates, output_projection, schema, exec_ctx)
    }
}

impl DisplayAs for FilterExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "FilterExec [{}]",
            self.predicates.iter().map(|e| format!("{e}")).join(", ")
        )
    }
}

impl ExecutionPlan for FilterExec {
    fn name(&self) -> &str {
        "FilterExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
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
            self.predicates.clone(),
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

fn execute_filter(
    mut input: SendableLazyBatchStream,
    predicates: Vec<PhysicalExprRef>,
    projection: Vec<PhysicalExprRef>,
    schema: SchemaRef,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableLazyBatchStream> {
    let evaluator = CachedExprsEvaluator::try_new(predicates, projection, schema)?;
    let output_ctx = exec_ctx.clone();
    let output = exec_ctx
        .clone()
        .output_lazy_with_sender("Filter", move |sender| {
            let exec_ctx = output_ctx;
            async move {
                let elapsed_compute = exec_ctx.baseline_metrics().elapsed_compute().clone();
                let _timer = elapsed_compute.timer();
                sender.exclude_time(&elapsed_compute);
                while let Some(batch) = elapsed_compute
                    .exclude_timer_async(input.next())
                    .await
                    .transpose()?
                {
                    let filtered = evaluator.evaluate_optimized(batch.as_ref())?;
                    exec_ctx
                        .baseline_metrics()
                        .record_output(filtered.num_rows());
                    sender.send(filtered).await;
                }
                Ok(())
            }
        });
    Ok(output)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    use arrow::{
        array::{ArrayRef, BooleanArray, Int32Array},
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{
        common::ScalarValue,
        logical_expr::Operator,
        physical_expr::expressions::{BinaryExpr, Literal},
        physical_plan::{common::collect, test::TestMemoryExec},
        prelude::SessionContext,
    };
    use futures::TryStreamExt;

    use super::*;
    use crate::{
        common::{
            lazy::{ColumnBatch, LazyBatchRef, column},
            optimized_batch::materialize_optimized_stream,
        },
        project_exec::ProjectExec,
    };

    fn evaluate_filtered(
        batch: LazyBatchRef,
        predicates: &[PhysicalExprRef],
        projection: &[usize],
        schema: SchemaRef,
    ) -> Result<LazyBatchRef> {
        let expressions = projection
            .iter()
            .map(|&index| {
                Arc::new(Column::new(batch.schema().field(index).name(), index)) as PhysicalExprRef
            })
            .collect();
        CachedExprsEvaluator::try_new(predicates.to_vec(), expressions, schema)?
            .evaluate_optimized(batch.as_ref())
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("value", DataType::Int32, false),
                Field::new("keep", DataType::Boolean, true),
                Field::new("payload", DataType::Int32, false),
            ],
            HashMap::from([("source".to_owned(), "filter-test".to_owned())]),
        ))
    }

    fn arrays() -> Vec<ArrayRef> {
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(true),
                Some(true),
            ])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
        ]
    }

    fn predicates() -> Vec<PhysicalExprRef> {
        vec![
            Arc::new(Column::new("keep", 1)),
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("value", 0)),
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            )),
        ]
    }

    fn input(batches: Vec<RecordBatch>) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TestMemoryExec::try_new(
            &[batches],
            schema(),
            None,
        )?))
    }

    #[derive(Debug, Clone)]
    struct CountedExpr {
        expr: PhysicalExprRef,
        evaluations: Arc<AtomicUsize>,
    }

    impl PartialEq for CountedExpr {
        fn eq(&self, other: &Self) -> bool {
            self.expr.eq(&other.expr) && Arc::ptr_eq(&self.evaluations, &other.evaluations)
        }
    }

    impl Eq for CountedExpr {}

    impl std::hash::Hash for CountedExpr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.expr.hash(state);
            Arc::as_ptr(&self.evaluations).hash(state);
        }
    }

    impl std::fmt::Display for CountedExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "Counted({})", self.expr)
        }
    }

    impl datafusion::physical_expr::PhysicalExpr for CountedExpr {
        fn data_type(&self, schema: &Schema) -> Result<DataType> {
            self.expr.data_type(schema)
        }

        fn nullable(&self, schema: &Schema) -> Result<bool> {
            self.expr.nullable(schema)
        }

        fn evaluate(
            &self,
            batch: &RecordBatch,
        ) -> Result<datafusion::physical_plan::ColumnarValue> {
            self.evaluations.fetch_add(1, Ordering::SeqCst);
            self.expr.evaluate(batch)
        }

        fn children(&self) -> Vec<&PhysicalExprRef> {
            vec![&self.expr]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<PhysicalExprRef>,
        ) -> Result<PhysicalExprRef> {
            Ok(Arc::new(Self {
                expr: children[0].clone(),
                evaluations: self.evaluations.clone(),
            }))
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            self.expr.fmt_sql(f)
        }
    }

    #[tokio::test]
    async fn consecutive_filter_exec_selected_expressions_match_eager() -> Result<()> {
        let first = Arc::new(FilterExec::try_new(
            vec![Arc::new(Column::new("keep", 1))],
            input(vec![RecordBatch::try_new(schema(), arrays())?])?,
        )?);
        let second = FilterExec::try_new(
            vec![
                predicates()[1].clone(),
                Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
            ],
            first,
        )?;
        let batches = collect(second.execute(0, SessionContext::new().task_ctx())?).await?;
        assert_eq!(
            batches,
            vec![RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(Int32Array::from(vec![2, 3])),
                    Arc::new(BooleanArray::from(vec![true, true])),
                    Arc::new(Int32Array::from(vec![30, 40]))
                ]
            )?]
        );
        Ok(())
    }

    #[tokio::test]
    async fn fused_project_reuses_filter_cache_per_input_batch() -> Result<()> {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let shared: PhysicalExprRef = Arc::new(CountedExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("payload", 2)),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            )),
            evaluations: evaluations.clone(),
        });
        let filters = [10, 20]
            .into_iter()
            .map(|limit| {
                Arc::new(BinaryExpr::new(
                    shared.clone(),
                    Operator::Gt,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(limit)))),
                )) as PhysicalExprRef
            })
            .collect();
        let first = RecordBatch::try_new(schema(), arrays())?;
        let mut second = arrays();
        second[2] = Arc::new(Int32Array::from(vec![50, 60, 70, 80]));
        let filter = Arc::new(FilterExec::try_new(
            filters,
            input(vec![first, RecordBatch::try_new(schema(), second)?])?,
        )?);
        let project = Arc::new(ProjectExec::try_new(
            vec![
                (shared.clone(), "shared".to_owned()),
                (shared, "again".to_owned()),
            ],
            filter.clone(),
        )?);
        let context = SessionContext::new();
        let batches = collect(project.execute(0, context.task_ctx())?).await?;
        assert_eq!(evaluations.load(Ordering::SeqCst), 2);
        assert_eq!(batches.len(), 2);
        for (batch, values) in batches.iter().zip([vec![21, 31, 41], vec![51, 61, 71, 81]]) {
            assert_eq!(batch.column(0).as_ref(), &Int32Array::from(values));
            assert_eq!(batch.column(0), batch.column(1));
        }
        assert!(
            filter
                .metrics()
                .expect("filter metrics")
                .iter()
                .next()
                .is_none()
        );
        assert_eq!(
            project.metrics().expect("project metrics").output_rows(),
            Some(7)
        );
        let project: Arc<dyn ExecutionPlan> = project;
        let exec_ctx = ExecutionContext::new(
            context.task_ctx(),
            0,
            Arc::new(project.schema().project(&[1, 0, 1])?),
            &ExecutionPlanMetricsSet::new(),
        );
        let projected = collect(materialize_optimized_stream(exec_ctx.execute_optimized(
            &project,
            &ExecuteOptimizedProps::default().with_projection(vec![1, 0, 1]),
        )?))
        .await?;
        assert_eq!(evaluations.load(Ordering::SeqCst), 4);
        assert_eq!(projected.len(), 2);
        for (output, batch) in projected.iter().zip(&batches) {
            assert_eq!(output, &batch.project(&[1, 0, 1])?);
        }
        let empty_ctx = exec_ctx.with_new_output_schema(Arc::new(project.schema().project(&[])?));
        let empty = collect(materialize_optimized_stream(empty_ctx.execute_optimized(
            &project,
            &ExecuteOptimizedProps::default().with_projection(vec![]),
        )?))
        .await?;
        assert_eq!(evaluations.load(Ordering::SeqCst), 6);
        assert_eq!(empty[0].num_columns(), 0);
        assert_eq!(empty.iter().map(|batch| batch.num_rows()).sum::<usize>(), 7);
        Ok(())
    }

    #[tokio::test]
    async fn fused_project_short_circuits_rejected_rows_and_unselected_outputs() -> Result<()> {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let division: PhysicalExprRef = Arc::new(CountedExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(12)))),
                Operator::Divide,
                Arc::new(Column::new("value", 0)),
            )),
            evaluations: evaluations.clone(),
        });
        let context = SessionContext::new();
        for retained in [false, true] {
            let filter = Arc::new(FilterExec::try_new(
                if retained {
                    predicates()
                } else {
                    vec![
                        Arc::new(Literal::new(ScalarValue::Boolean(Some(false))))
                            as PhysicalExprRef,
                        Arc::new(BinaryExpr::new(
                            division.clone(),
                            Operator::Gt,
                            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
                        )),
                    ]
                },
                input(vec![RecordBatch::try_new(schema(), arrays())?])?,
            )?);
            let project: Arc<dyn ExecutionPlan> = Arc::new(ProjectExec::try_new(
                vec![(division.clone(), "quotient".to_owned())],
                filter,
            )?);
            let exec_ctx = ExecutionContext::new(
                context.task_ctx(),
                0,
                Arc::new(project.schema().project(&[])?),
                &ExecutionPlanMetricsSet::new(),
            );
            let empty = collect(materialize_optimized_stream(exec_ctx.execute_optimized(
                &project,
                &ExecuteOptimizedProps::default().with_projection(vec![]),
            )?))
            .await?;
            assert_eq!(evaluations.load(Ordering::SeqCst), 0);
            if retained {
                assert_eq!(empty[0].num_rows(), 2);
                let output = collect(project.execute(0, context.task_ctx())?).await?;
                assert_eq!(output[0].column(0).as_ref(), &Int32Array::from(vec![6, 4]));
                assert_eq!(evaluations.load(Ordering::SeqCst), 1);
            } else {
                assert!(empty.is_empty());
                assert!(
                    collect(project.execute(0, context.task_ctx())?)
                        .await?
                        .is_empty()
                );
                assert_eq!(evaluations.load(Ordering::SeqCst), 0);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn lazy_filter_preserves_parsed_json_cache_and_eager_projection() -> Result<()> {
        use arrow::array::StringArray;
        use datafusion::{common::config::ConfigOptions, physical_expr::ScalarFunctionExpr};

        let json_schema = Arc::new(Schema::new(vec![
            Field::new("json", DataType::Utf8, true),
            Field::new("keep", DataType::Boolean, true),
        ]));
        let batch = RecordBatch::try_new(
            json_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("invalid"),
                    Some(r#"{"a":"yes","b":"no","c":"yes"}"#),
                    Some(r#"{"a":"yes","b":"yes","c":"no"}"#),
                    None,
                    Some(r#"{"a":"yes","b":"yes","c":"yes"}"#),
                ])),
                Arc::new(BooleanArray::from(vec![false, true, true, true, true])),
            ],
        )?;
        let function =
            |name: &str, args: Vec<PhysicalExprRef>, ret: DataType| -> Result<PhysicalExprRef> {
                let arg_types = args
                    .iter()
                    .map(|arg| arg.data_type(&json_schema))
                    .collect::<Result<Vec<_>>>()?;
                let udf = if name == "Spark_ParseJson" {
                    // resolved by the planner outside of create_auron_ext_udf
                    Arc::new(datafusion::logical_expr::ScalarUDF::from(
                        datafusion_ext_functions::spark_get_json_object::SparkParseJson::default(),
                    ))
                } else {
                    datafusion_ext_functions::create_auron_ext_udf(name, 0, arg_types, ret.clone())?
                };
                Ok(Arc::new(ScalarFunctionExpr::new(
                    udf.name(),
                    udf.clone(),
                    args,
                    Arc::new(Field::new("result", ret, true)),
                    Arc::new(ConfigOptions::default()),
                )))
            };
        let evaluations = Arc::new(AtomicUsize::new(0));
        let parsed: PhysicalExprRef = Arc::new(CountedExpr {
            expr: function(
                "Spark_ParseJson",
                vec![Arc::new(Column::new("json", 0))],
                DataType::Binary,
            )?,
            evaluations: evaluations.clone(),
        });
        let extract = |path: &str| {
            function(
                "Spark_GetParsedJsonObject",
                vec![
                    parsed.clone(),
                    Arc::new(Literal::new(ScalarValue::Utf8(Some(path.to_owned())))),
                ],
                DataType::Utf8,
            )
        };
        let evaluator = CachedExprsEvaluator::try_new(
            vec![],
            vec![extract("$.a")?, extract("$.b")?],
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("b", DataType::Utf8, true),
            ])),
        )?;
        let projected = evaluator.filter_project(&batch)?;
        assert_eq!(evaluations.load(Ordering::SeqCst), 1);
        assert_eq!(
            projected.column(0).as_ref(),
            &StringArray::from(vec![None, Some("yes"), Some("yes"), None, Some("yes")])
        );
        assert_eq!(
            projected.column(1).as_ref(),
            &StringArray::from(vec![None, Some("no"), Some("yes"), None, Some("yes")])
        );
        evaluations.store(0, Ordering::SeqCst);
        let mut filters: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("keep", 1))];
        for path in ["$.a", "$.b", "$.c"] {
            filters.push(Arc::new(BinaryExpr::new(
                extract(path)?,
                Operator::Eq,
                Arc::new(Literal::new(ScalarValue::Utf8(Some("yes".to_owned())))),
            )));
        }
        let output = evaluate_filtered(
            Arc::new(batch.clone()),
            &filters,
            &[0],
            Arc::new(json_schema.project(&[0])?),
        )?;
        assert_eq!(output.materialize()?, batch.slice(4, 1).project(&[0])?);
        assert_eq!(evaluations.load(Ordering::SeqCst), 1);
        let filter = Arc::new(FilterExec::try_new(
            filters.clone(),
            Arc::new(TestMemoryExec::try_new(
                &[vec![batch.clone(), batch.clone()]],
                json_schema.clone(),
                None,
            )?),
        )?);
        let project: Arc<dyn ExecutionPlan> = Arc::new(ProjectExec::try_new(
            vec![
                (parsed.clone(), "parsed".to_owned()),
                (extract("$.c")?, "c".to_owned()),
            ],
            filter,
        )?);
        let exec_ctx = ExecutionContext::new(
            SessionContext::new().task_ctx(),
            0,
            Arc::new(project.schema().project(&[1, 1])?),
            &ExecutionPlanMetricsSet::new(),
        );
        let projected = collect(materialize_optimized_stream(exec_ctx.execute_optimized(
            &project,
            &ExecuteOptimizedProps::default().with_projection(vec![1, 1]),
        )?))
        .await?;
        assert_eq!(projected.len(), 2);
        for output in &projected {
            assert_eq!(output.column(0).as_ref(), &StringArray::from(vec!["yes"]));
            assert_eq!(output.column(0), output.column(1));
        }
        assert_eq!(evaluations.load(Ordering::SeqCst), 3);
        let projected_schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, true)]));
        let evaluator =
            CachedExprsEvaluator::try_new(filters, vec![extract("$.c")?], projected_schema)?;
        for expected_evaluations in 4..=5 {
            let projected = evaluator.filter_project(&batch)?;
            assert_eq!(
                projected.column(0).as_ref(),
                &StringArray::from(vec!["yes"])
            );
            assert_eq!(evaluations.load(Ordering::SeqCst), expected_evaluations);
        }
        Ok(())
    }

    #[test]
    #[ignore]
    fn lazy_filter_multi_predicate_micro_comparison() -> Result<()> {
        use std::{hint::black_box, time::Instant};

        use crate::common::lazy::{CACHE, evaluate_columns};

        let rows = 8192;
        let iterations = 300;
        let evaluations = Arc::new(AtomicUsize::new(0));
        let shared: PhysicalExprRef = Arc::new(CountedExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("value", 0)),
                Operator::Plus,
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            )),
            evaluations: evaluations.clone(),
        });
        let filters = [0, 2048, 4096]
            .into_iter()
            .map(|limit| {
                Arc::new(BinaryExpr::new(
                    shared.clone(),
                    Operator::Gt,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(limit)))),
                )) as PhysicalExprRef
            })
            .collect::<Vec<_>>();
        let batch = ColumnBatch::try_new(
            schema(),
            vec![
                column(Arc::new(Int32Array::from(
                    (0..rows as i32).collect::<Vec<_>>(),
                ))),
                column(Arc::new(BooleanArray::from(vec![true; rows]))),
                column(Arc::new(Int32Array::from(vec![10; rows]))),
            ],
            rows,
        )?;
        let evaluator = CachedExprsEvaluator::try_new(
            filters.clone(),
            (0..3)
                .map(|index| {
                    Arc::new(Column::new(schema().field(index).name(), index)) as PhysicalExprRef
                })
                .collect(),
            schema(),
        )?;
        for new in [false, true] {
            evaluations.store(0, Ordering::SeqCst);
            let start = Instant::now();
            for _ in 0..iterations {
                let output = if new {
                    evaluator.evaluate_optimized(batch.as_ref())?
                } else {
                    CACHE.with_scope(|| {
                        let mut output = batch.clone();
                        for predicate in &filters {
                            let values =
                                evaluate_columns(output.as_ref(), std::slice::from_ref(predicate))?;
                            output = output
                                .filter(datafusion::common::cast::as_boolean_array(&values[0])?)?;
                        }
                        Ok(output)
                    })?
                };
                assert_eq!(output.num_rows(), 4096);
                black_box(output);
            }
            let elapsed = start.elapsed();
            let count = evaluations.load(Ordering::SeqCst);
            assert_eq!(count, iterations * if new { 1 } else { 3 });
            eprintln!(
                "filter micro new={new} rows={rows} iterations={iterations} elapsed={elapsed:?} shared_evaluations={count}"
            );
        }
        Ok(())
    }

    #[test]
    fn lazy_filter_composes_existing_indices_and_preserves_order() -> Result<()> {
        let batch = ColumnBatch::try_new(schema(), arrays().into_iter().map(column).collect(), 4)?;
        let batch = batch.take(None, &Int32Array::from(vec![3, 0, 2, 3, 1]))?;
        let output = evaluate_filtered(
            batch,
            &predicates(),
            &[2, 0, 2],
            Arc::new(schema().project(&[2, 0, 2])?),
        )?;
        let materialized = output.materialize()?;
        assert_eq!(
            materialized.column(0).as_ref(),
            &Int32Array::from(vec![40, 30, 40])
        );
        assert_eq!(
            materialized.column(1).as_ref(),
            &Int32Array::from(vec![3, 2, 3])
        );
        assert_eq!(materialized.column(0), materialized.column(2));
        Ok(())
    }

    #[test]
    fn lazy_filter_skips_rejected_rows_in_later_predicates() -> Result<()> {
        let division: PhysicalExprRef = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Literal::new(ScalarValue::Int32(Some(12)))),
                Operator::Divide,
                Arc::new(Column::new("value", 0)),
            )),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        ));
        let mut filters = predicates();
        filters.push(division.clone());
        let batch: LazyBatchRef = Arc::new(RecordBatch::try_new(schema(), arrays())?);
        assert_eq!(
            evaluate_filtered(batch, &filters, &[], Arc::new(schema().project(&[])?))?.num_rows(),
            2
        );
        for keep in [vec![Some(false); 4], vec![None; 4]] {
            let mut values = arrays();
            values[1] = Arc::new(BooleanArray::from(keep));
            let batch: LazyBatchRef = Arc::new(RecordBatch::try_new(schema(), values)?);
            let filters: Vec<PhysicalExprRef> =
                vec![Arc::new(Column::new("keep", 1)), division.clone()];
            assert_eq!(
                evaluate_filtered(batch, &filters, &[], Arc::new(schema().project(&[])?))?
                    .num_rows(),
                0
            );
        }
        Ok(())
    }

    #[test]
    fn lazy_filter_all_true_preserves_columns_and_zero_output_metadata() -> Result<()> {
        let columns = arrays().into_iter().map(column).collect::<Vec<_>>();
        let batch = ColumnBatch::try_new(schema(), columns.clone(), 4)?;
        let predicates: Vec<PhysicalExprRef> =
            vec![Arc::new(Literal::new(ScalarValue::Boolean(Some(true))))];
        let output = evaluate_filtered(
            batch.clone(),
            &predicates,
            &[2, 0, 2],
            Arc::new(schema().project(&[2, 0, 2])?),
        )?;
        assert!(Arc::ptr_eq(&output.get_column(0)?, &columns[2]));
        assert!(Arc::ptr_eq(&output.get_column(1)?, &columns[0]));
        let empty = evaluate_filtered(batch, &predicates, &[], Arc::new(schema().project(&[])?))?;
        assert_eq!(empty.num_rows(), 4);
        assert_eq!(empty.materialize()?.num_rows(), 4);
        assert_eq!(empty.schema().metadata(), schema().metadata());
        let zero_columns = ColumnBatch::try_new(empty.schema(), vec![], 4)?;
        assert_eq!(
            evaluate_filtered(zero_columns, &predicates, &[], empty.schema())?.num_rows(),
            4
        );
        Ok(())
    }

    #[tokio::test]
    async fn optimized_filter_plain_and_lazy_inputs_match_projected_wrappers() -> Result<()> {
        for lazy_input in [false, true] {
            let source = input(vec![RecordBatch::try_new(schema(), arrays())?])?;
            let source: Arc<dyn ExecutionPlan> = if lazy_input {
                Arc::new(ProjectExec::try_new(
                    schema()
                        .fields()
                        .iter()
                        .enumerate()
                        .map(|(index, field)| {
                            (
                                Arc::new(Column::new(field.name(), index)) as PhysicalExprRef,
                                field.name().clone(),
                            )
                        })
                        .collect(),
                    source,
                )?)
            } else {
                source
            };
            let filter: Arc<dyn ExecutionPlan> =
                Arc::new(FilterExec::try_new(predicates(), source)?);
            let context = SessionContext::new();
            for projection in [None, Some(vec![2, 0, 2]), Some(vec![2]), Some(vec![])] {
                let expected_projection = projection.clone().unwrap_or_else(|| vec![0, 1, 2]);
                let expected_schema = Arc::new(filter.schema().project(&expected_projection)?);
                let exec_ctx = ExecutionContext::new(
                    context.task_ctx(),
                    0,
                    expected_schema.clone(),
                    &ExecutionPlanMetricsSet::new(),
                );
                let props = match projection {
                    Some(projection) => {
                        ExecuteOptimizedProps::default().with_projection(projection)
                    }
                    None => ExecuteOptimizedProps::default(),
                };
                let stream = exec_ctx.execute_optimized(&filter, &props)?;
                assert_eq!(stream.schema(), expected_schema);
                let optimized = collect(materialize_optimized_stream(stream)).await?;
                let legacy = collect(filter.execute(0, context.task_ctx())?)
                    .await?
                    .iter()
                    .map(|batch| Ok(batch.project(&expected_projection)?))
                    .collect::<Result<Vec<_>>>()?;
                assert_eq!(optimized, legacy);
                assert_eq!(optimized.len(), 1);
                let expected = RecordBatch::try_new(
                    filter.schema(),
                    vec![
                        Arc::new(Int32Array::from(vec![2, 3])),
                        Arc::new(BooleanArray::from(vec![true, true])),
                        Arc::new(Int32Array::from(vec![30, 40])),
                    ],
                )?
                .project(&expected_projection)?;
                assert_eq!(optimized[0], expected);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn filter_project_join_retains_lazy_payload_on_probe_and_build() -> Result<()> {
        use datafusion::{common::JoinSide, physical_plan::joins::utils::build_join_schema};

        use crate::{broadcast_join_exec::BroadcastJoinExec, joins::join_utils::JoinType};

        for build_project in [false, true] {
            let batch = RecordBatch::try_new(schema(), arrays())?;
            let payload_source = batch.column(2).clone();
            let key_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
            let memory_keys = |values: Vec<i32>| -> Result<Arc<dyn ExecutionPlan>> {
                let batch = RecordBatch::try_new(
                    key_schema.clone(),
                    vec![Arc::new(Int32Array::from(values))],
                )?;
                Ok(Arc::new(TestMemoryExec::try_new(
                    &[vec![batch]],
                    key_schema.clone(),
                    None,
                )?))
            };
            let left = input(vec![batch])?;
            let right = memory_keys(vec![0, 1, 2, 3])?;
            let first: Arc<dyn ExecutionPlan> = Arc::new(BroadcastJoinExec::try_new(
                Arc::new(
                    build_join_schema(
                        &left.schema(),
                        &right.schema(),
                        &datafusion::logical_expr::JoinType::Inner,
                    )
                    .0,
                ),
                left,
                right,
                vec![(
                    Arc::new(Column::new("value", 0)),
                    Arc::new(Column::new("key", 0)),
                )],
                JoinType::Inner,
                JoinSide::Right,
                false,
                None,
                false,
                None,
            )?);
            let filter = Arc::new(FilterExec::try_new(predicates(), first)?);
            let project: Arc<dyn ExecutionPlan> = Arc::new(ProjectExec::try_new(
                vec![
                    (Arc::new(Column::new("value", 0)), "value".to_owned()),
                    (Arc::new(Column::new("payload", 2)), "payload".to_owned()),
                ],
                filter,
            )?);
            let other = memory_keys(vec![2, 3, 3])?;
            let (left, right, on, payload_index) = if build_project {
                (
                    other,
                    project,
                    vec![(
                        Arc::new(Column::new("key", 0)) as PhysicalExprRef,
                        Arc::new(Column::new("value", 0)) as PhysicalExprRef,
                    )],
                    2,
                )
            } else {
                (
                    project,
                    other,
                    vec![(
                        Arc::new(Column::new("value", 0)) as PhysicalExprRef,
                        Arc::new(Column::new("key", 0)) as PhysicalExprRef,
                    )],
                    1,
                )
            };
            let join = BroadcastJoinExec::try_new(
                Arc::new(
                    build_join_schema(
                        &left.schema(),
                        &right.schema(),
                        &datafusion::logical_expr::JoinType::Inner,
                    )
                    .0,
                ),
                left,
                right,
                on,
                JoinType::Inner,
                JoinSide::Right,
                false,
                None,
                false,
                None,
            )?;
            let output = join
                .execute_optimized(0, SessionContext::new().task_ctx(), None)?
                .try_collect::<Vec<_>>()
                .await?;
            drop(join);
            assert_eq!(
                output.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                3
            );
            assert!(Arc::strong_count(&payload_source) > 1);
            let mut values = Vec::new();
            for batch in &output {
                let payload = batch.get_column(payload_index)?.materialize()?;
                values.extend(
                    payload
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("payload")
                        .values()
                        .iter()
                        .copied(),
                );
            }
            values.sort_unstable();
            assert_eq!(values, vec![30, 40, 40]);
            drop(output);
            assert_eq!(Arc::strong_count(&payload_source), 1);
        }
        Ok(())
    }

    #[tokio::test]
    async fn optimized_filter_empty_stream_and_all_filtered_emit_no_batches() -> Result<()> {
        for batches in [
            vec![],
            vec![RecordBatch::new_empty(schema())],
            vec![RecordBatch::try_new(schema(), arrays())?],
        ] {
            for predicate in [Some(false), None] {
                let filter = Arc::new(FilterExec::try_new(
                    vec![Arc::new(Literal::new(ScalarValue::Boolean(predicate)))],
                    input(batches.clone())?,
                )?);
                let plan: Arc<dyn ExecutionPlan> = filter.clone();
                let exec_ctx = ExecutionContext::new(
                    SessionContext::new().task_ctx(),
                    0,
                    Arc::new(plan.schema().project(&[])?),
                    &ExecutionPlanMetricsSet::new(),
                );
                let stream = exec_ctx.execute_optimized(
                    &plan,
                    &ExecuteOptimizedProps::default().with_projection(vec![]),
                )?;
                assert_eq!(stream.schema().metadata(), schema().metadata());
                assert!(
                    collect(materialize_optimized_stream(stream))
                        .await?
                        .is_empty()
                );
                assert_eq!(filter.metrics().expect("metrics").output_rows(), Some(0));
            }
        }
        Ok(())
    }
}
