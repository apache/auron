// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{any::Any, fmt::Formatter, sync::Arc};

use arrow::{
    array::{Array, ArrayRef},
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use datafusion::{
    common::{stats::Precision, ColumnStatistics, Result, Statistics},
    execution::context::TaskContext,
    physical_expr::{EquivalenceProperties, PhysicalSortExpr},
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, ExecutionPlanProperties,
        PhysicalExpr, PlanProperties, SendableRecordBatchStream,
    },
};
use datafusion_ext_commons::arrow::cast::cast;
use futures::StreamExt;
use once_cell::sync::OnceCell;

use crate::{
    common::execution_context::ExecutionContext,
    window::{window_context::WindowContext, WindowExpr},
};

#[derive(Debug)]
pub struct WindowExec {
    input: Arc<dyn ExecutionPlan>,
    context: Arc<WindowContext>,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl WindowExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        window_exprs: Vec<WindowExpr>,
        partition_spec: Vec<Arc<dyn PhysicalExpr>>,
        order_spec: Vec<PhysicalSortExpr>,
    ) -> Result<Self> {
        let context = Arc::new(WindowContext::try_new(
            input.schema(),
            window_exprs,
            partition_spec,
            order_spec,
        )?);
        Ok(Self {
            input,
            context,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        })
    }
}

impl DisplayAs for WindowExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Window")
    }
}

impl ExecutionPlan for WindowExec {
    fn name(&self) -> &str {
        "WindowExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.context.output_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                self.input.output_partitioning().clone(),
                ExecutionMode::Bounded,
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
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            self.context.window_exprs.clone(),
            self.context.partition_spec.clone(),
            self.context.order_spec.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // at this moment only supports ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let input = exec_ctx.execute_with_input_stats(&self.input)?;
        let coalesced = exec_ctx.coalesce_with_default_batch_size(input);
        let window_ctx = self.context.clone();
        execute_window(coalesced, exec_ctx, window_ctx)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        let input_stat = self.input.statistics()?;
        let win_cols = self.context.window_exprs.len();
        let input_cols = self.input.schema().fields().len();
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        column_statistics.extend(input_stat.column_statistics);
        for _ in 0..win_cols {
            column_statistics.push(ColumnStatistics::new_unknown())
        }
        Ok(Statistics {
            num_rows: input_stat.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        })
    }
}

fn execute_window(
    mut input: SendableRecordBatchStream,
    exec_ctx: Arc<ExecutionContext>,
    window_ctx: Arc<WindowContext>,
) -> Result<SendableRecordBatchStream> {
    // start processing input batches
    Ok(exec_ctx
        .clone()
        .output_with_sender("Window", |sender| async move {
            sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());

            let mut processors = window_ctx
                .window_exprs
                .iter()
                .map(|expr: &WindowExpr| expr.create_processor(&window_ctx))
                .collect::<Result<Vec<_>>>()?;

            while let Some(batch) = input.next().await.transpose()? {
                let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
                let window_cols: Vec<ArrayRef> = processors
                    .iter_mut()
                    .map(|processor| processor.process_batch(&window_ctx, &batch))
                    .collect::<Result<_>>()?;

                let outputs: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .chain(&window_cols)
                    .zip(window_ctx.output_schema.fields())
                    .map(|(array, field)| {
                        if array.data_type() != field.data_type() {
                            return cast(&array, field.data_type());
                        }
                        Ok(array.clone())
                    })
                    .collect::<Result<_>>()?;
                let output_batch = RecordBatch::try_new_with_options(
                    window_ctx.output_schema.clone(),
                    outputs,
                    &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
                )?;
                exec_ctx
                    .baseline_metrics()
                    .record_output(output_batch.num_rows());
                sender.send(output_batch).await;
            }
            Ok(())
        }))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{array::*, datatypes::*, record_batch::RecordBatch};
    use datafusion::{
        assert_batches_eq,
        common::stats::Precision,
        physical_expr::{expressions::Column, PhysicalSortExpr},
        physical_plan::{memory::MemoryExec, ExecutionPlan},
        prelude::SessionContext,
    };

    use crate::{
        agg::AggFunction,
        window::{WindowExpr, WindowFunction, WindowRankType},
        window_exec::WindowExec,
    };

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    #[tokio::test]
    async fn test_window() -> Result<(), Box<dyn std::error::Error>> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        // test window
        let input = build_table(
            ("a1", &vec![1, 1, 1, 1, 2, 3, 3]),
            ("b1", &vec![1, 2, 2, 3, 4, 1, 1]),
            ("c1", &vec![0, 0, 0, 0, 0, 0, 0]),
        );
        let window_exprs = vec![
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::RowNumber),
                vec![],
                Arc::new(Field::new("b1_row_number", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::Rank),
                vec![],
                Arc::new(Field::new("b1_rank", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::DenseRank),
                vec![],
                Arc::new(Field::new("b1_dense_rank", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::Agg(AggFunction::Sum),
                vec![Arc::new(Column::new("b1", 1))],
                Arc::new(Field::new("b1_sum", DataType::Int64, false)),
                DataType::Int64,
            ),
        ];
        let window = Arc::new(WindowExec::try_new(
            input.clone(),
            window_exprs.clone(),
            vec![Arc::new(Column::new("a1", 0))],
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("b1", 1)),
                options: Default::default(),
            }],
        )?);
        let stream = window.execute(0, task_ctx.clone())?;
        let batches = datafusion::physical_plan::common::collect(stream).await?;
        let row_count = window.statistics()?.num_rows;
        let expected = vec![
            "+----+----+----+---------------+---------+---------------+--------+",
            "| a1 | b1 | c1 | b1_row_number | b1_rank | b1_dense_rank | b1_sum |",
            "+----+----+----+---------------+---------+---------------+--------+",
            "| 1  | 1  | 0  | 1             | 1       | 1             | 1      |",
            "| 1  | 2  | 0  | 2             | 2       | 2             | 3      |",
            "| 1  | 2  | 0  | 3             | 2       | 2             | 5      |",
            "| 1  | 3  | 0  | 4             | 4       | 3             | 8      |",
            "| 2  | 4  | 0  | 1             | 1       | 1             | 4      |",
            "| 3  | 1  | 0  | 1             | 1       | 1             | 1      |",
            "| 3  | 1  | 0  | 2             | 1       | 1             | 2      |",
            "+----+----+----+---------------+---------+---------------+--------+",
        ];
        assert_batches_eq!(expected, &batches);
        assert_eq!(
            row_count,
            Precision::Exact(window_exprs.clone().len() + input.clone().schema().fields().len())
        );

        // test window without partition by clause
        let input = build_table(
            ("a1", &vec![1, 3, 3, 1, 1, 1, 2]),
            ("b1", &vec![1, 1, 1, 2, 2, 3, 4]),
            ("c1", &vec![0, 0, 0, 0, 0, 0, 0]),
        );
        let window_exprs = vec![
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::RowNumber),
                vec![],
                Arc::new(Field::new("b1_row_number", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::Rank),
                vec![],
                Arc::new(Field::new("b1_rank", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::RankLike(WindowRankType::DenseRank),
                vec![],
                Arc::new(Field::new("b1_dense_rank", DataType::Int32, false)),
                DataType::Int32,
            ),
            WindowExpr::new(
                WindowFunction::Agg(AggFunction::Sum),
                vec![Arc::new(Column::new("b1", 1))],
                Arc::new(Field::new("b1_sum", DataType::Int64, false)),
                DataType::Int64,
            ),
        ];
        let window = Arc::new(WindowExec::try_new(
            input.clone(),
            window_exprs.clone(),
            vec![],
            vec![PhysicalSortExpr {
                expr: Arc::new(Column::new("b1", 1)),
                options: Default::default(),
            }],
        )?);
        let stream = window.execute(0, task_ctx.clone())?;
        let batches = datafusion::physical_plan::common::collect(stream).await?;
        let row_count = window.statistics()?.num_rows;
        let expected = vec![
            "+----+----+----+---------------+---------+---------------+--------+",
            "| a1 | b1 | c1 | b1_row_number | b1_rank | b1_dense_rank | b1_sum |",
            "+----+----+----+---------------+---------+---------------+--------+",
            "| 1  | 1  | 0  | 1             | 1       | 1             | 1      |",
            "| 3  | 1  | 0  | 2             | 1       | 1             | 2      |",
            "| 3  | 1  | 0  | 3             | 1       | 1             | 3      |",
            "| 1  | 2  | 0  | 4             | 4       | 2             | 5      |",
            "| 1  | 2  | 0  | 5             | 4       | 2             | 7      |",
            "| 1  | 3  | 0  | 6             | 6       | 3             | 10     |",
            "| 2  | 4  | 0  | 7             | 7       | 4             | 14     |",
            "+----+----+----+---------------+---------+---------------+--------+",
        ];
        assert_batches_eq!(expected, &batches);
        assert_eq!(
            row_count,
            Precision::Exact(window_exprs.clone().len() + input.clone().schema().fields().len())
        );
        Ok(())
    }
}
