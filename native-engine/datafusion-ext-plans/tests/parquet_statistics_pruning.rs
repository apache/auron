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
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use arrow::{
    array::{ArrayRef, Decimal128Array, Int32Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    logical_expr::{Operator, Volatility, create_udf},
    parquet::{
        arrow::{
            ArrowWriter, ParquetRecordBatchStreamBuilder,
            arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
            async_reader::AsyncFileReader,
        },
        auron::get_dictionary_for_pruning,
        errors::{ParquetError, Result as ParquetResult},
        file::{
            metadata::ParquetMetaData,
            properties::{EnabledStatistics, WriterProperties},
        },
    },
    physical_expr::{
        PhysicalExprRef, ScalarFunctionExpr,
        expressions::{BinaryExpr, Column, IsNullExpr, Literal},
    },
    physical_plan::{ColumnarValue, metrics::ExecutionPlanMetricsSet},
};
use datafusion_datasource_parquet::{
    ParquetAccessPlan, ParquetFileMetrics, RowGroupAccessPlanFilter,
};
use datafusion_pruning::PruningPredicateBuilder;
use futures::future::BoxFuture;

#[derive(Clone)]
struct TestReader {
    data: Bytes,
    reads: Arc<AtomicUsize>,
    fail_reads: Arc<AtomicBool>,
}

impl TestReader {
    fn new(data: Bytes) -> Self {
        Self {
            data,
            reads: Arc::default(),
            fail_reads: Arc::default(),
        }
    }
}

impl AsyncFileReader for TestReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(async move { self.get_bytes_sync(range) })
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            Ok(ParquetRecordBatchReaderBuilder::try_new(self.data.clone())?
                .metadata()
                .clone())
        })
    }

    fn get_bytes_sync(&mut self, range: Range<u64>) -> ParquetResult<Bytes> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        if self.fail_reads.load(Ordering::Relaxed) {
            return Err(ParquetError::General(
                "injected dictionary read error".to_owned(),
            ));
        }
        Ok(self.data.slice(range.start as usize..range.end as usize))
    }
}

fn write_parquet(values: ArrayRef, statistics: EnabledStatistics) -> Result<Bytes> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "x",
        values.data_type().clone(),
        values.null_count() > 0,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values])?;
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_max_row_group_size(2)
        .set_statistics_enabled(statistics)
        .build();
    let mut writer = ArrowWriter::try_new(Vec::new(), schema, Some(properties))?;
    writer.write(&batch)?;
    Ok(Bytes::from(writer.into_inner()?))
}

fn comparison(op: Operator, literal: ScalarValue) -> PhysicalExprRef {
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new("x", 0)),
        op,
        Arc::new(Literal::new(literal)),
    ))
}

fn prune(
    builder: &mut ParquetRecordBatchStreamBuilder<TestReader>,
    expression: PhysicalExprRef,
    plan: ParquetAccessPlan,
    metrics: &ParquetFileMetrics,
) -> Result<Vec<usize>> {
    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();
    let predicate = PruningPredicateBuilder::new()
        .with_file_schema(schema.clone())
        .try_build(expression)?;
    let mut filter = RowGroupAccessPlanFilter::new(plan);
    let mut reader: Box<dyn AsyncFileReader + Send> = Box::new(builder.input.0.clone());
    filter.prune_by_statistics(
        &schema,
        metadata.file_metadata().schema_descr(),
        metadata.row_groups(),
        &predicate,
        metrics,
        Some(&mut reader),
    );
    Ok(filter.build().row_group_indexes())
}

fn assert_metrics(metrics: &ParquetFileMetrics, pruned: usize, matched: usize, errors: usize) {
    assert_eq!(metrics.row_groups_pruned_statistics.pruned(), pruned);
    assert_eq!(metrics.row_groups_pruned_statistics.matched(), matched);
    assert_eq!(metrics.predicate_evaluation_errors.value(), errors);
}

#[tokio::test]
async fn decimal_statistics_metrics() -> Result<()> {
    let data = write_parquet(
        Arc::new(Decimal128Array::from(vec![-100, 0, 500, 1000]).with_precision_and_scale(18, 2)?),
        EnabledStatistics::Chunk,
    )?;
    let reader = TestReader::new(data);
    let reads = reader.reads.clone();
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    assert_eq!(builder.metadata().num_row_groups(), 2);
    let metrics = ParquetFileMetrics::new(0, "decimal", &ExecutionPlanMetricsSet::new());
    let mut total_pruned = 0;
    let mut total_matched = 0;
    for (lower, upper, expected) in [
        (500, 500, vec![1]),
        (2000, 2000, vec![]),
        (-1000, -1000, vec![]),
        (-100, 1000, vec![0, 1]),
    ] {
        let expression = Arc::new(BinaryExpr::new(
            comparison(Operator::GtEq, ScalarValue::Decimal128(Some(lower), 18, 2)),
            Operator::And,
            comparison(Operator::LtEq, ScalarValue::Decimal128(Some(upper), 18, 2)),
        ));
        let kept = prune(
            &mut builder,
            expression,
            ParquetAccessPlan::new_all(2),
            &metrics,
        )?;
        assert_eq!(kept, expected);
        total_pruned += 2 - expected.len();
        total_matched += expected.len();
        assert_metrics(&metrics, total_pruned, total_matched, 0);
    }
    assert_eq!(reads.load(Ordering::Relaxed), 0);
    Ok(())
}

#[tokio::test]
async fn dictionary_statistics_metrics() -> Result<()> {
    let data = Bytes::from_static(include_bytes!("data/integer-dictionary.parquet"));
    let reader = TestReader::new(data.clone());
    let reads = reader.reads.clone();
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let metadata = builder.metadata().clone();
    assert_eq!(metadata.num_row_groups(), 3);
    for (index, expected) in [[-10, -1], [0, 10], [5, 10]].iter().enumerate() {
        let dictionary =
            get_dictionary_for_pruning(&mut builder.input.0, metadata.row_group(index), 0)?
                .expect("legacy integer dictionary");
        assert_eq!(
            dictionary
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("integer dictionary")
                .values()
                .as_ref(),
            expected
        );
    }

    for (value, expected, pruned, matched, expected_matches) in [
        (5, vec![2], 2, 1, 50),
        (1, vec![], 3, 0, 0),
        (10, vec![1, 2], 1, 2, 100),
        (100, vec![], 3, 0, 0),
    ] {
        reads.store(0, Ordering::Relaxed);
        let metrics = ParquetFileMetrics::new(0, "dictionary", &ExecutionPlanMetricsSet::new());
        let kept = prune(
            &mut builder,
            comparison(Operator::Eq, ScalarValue::Int32(Some(value))),
            ParquetAccessPlan::new_all(3),
            &metrics,
        )?;
        assert_eq!(kept, expected);
        assert_metrics(&metrics, pruned, matched, 0);
        assert_eq!(reads.load(Ordering::Relaxed) > 0, value != 100);

        let count_matches = |groups| -> Result<usize> {
            let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())?
                .with_row_groups(groups)
                .build()?;
            let mut count = 0;
            for batch in reader {
                let batch = batch?;
                let values = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("integer column");
                count += values
                    .iter()
                    .filter(|candidate| *candidate == Some(value))
                    .count();
            }
            Ok(count)
        };
        assert_eq!(count_matches(vec![0, 1, 2])?, expected_matches);
        assert_eq!(count_matches(kept)?, expected_matches);
    }

    for (plan, expected, pruned, matched) in [
        {
            let mut plan = ParquetAccessPlan::new_all(3);
            plan.skip(1);
            (plan, vec![2], 1, 1)
        },
        (ParquetAccessPlan::new_none(3), vec![], 0, 0),
    ] {
        let metrics = ParquetFileMetrics::new(0, "initial_plan", &ExecutionPlanMetricsSet::new());
        assert_eq!(
            prune(
                &mut builder,
                comparison(Operator::Eq, ScalarValue::Int32(Some(5))),
                plan,
                &metrics,
            )?,
            expected
        );
        assert_metrics(&metrics, pruned, matched, 0);
    }
    Ok(())
}

#[test]
fn unknown_null_counts_do_not_prune_null_rows() -> Result<()> {
    let data = write_parquet(
        Arc::new(Int32Array::from(vec![Some(0), None])),
        EnabledStatistics::Chunk,
    )?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(data)?;
    let metadata = builder.metadata();
    let schema = builder.schema();
    let group = metadata.row_group(0).clone();
    let column = group
        .column(0)
        .clone()
        .into_builder()
        .set_statistics(datafusion::parquet::file::statistics::Statistics::int32(
            Some(0),
            Some(0),
            None,
            None,
            false,
        ))
        .build()?;
    let group = group
        .into_builder()
        .set_column_metadata(vec![column])
        .build()?;
    let predicate = PruningPredicateBuilder::new()
        .with_file_schema(schema.clone())
        .try_build(Arc::new(IsNullExpr::new(Arc::new(Column::new("x", 0)))))?;
    let metrics = ParquetFileMetrics::new(0, "unknown_nulls", &ExecutionPlanMetricsSet::new());
    let mut filter = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(1));
    filter.prune_by_statistics(
        schema,
        metadata.file_metadata().schema_descr(),
        &[group],
        &predicate,
        &metrics,
        None,
    );
    assert_eq!(filter.build().row_group_indexes(), [0]);
    assert_metrics(&metrics, 0, 1, 0);
    Ok(())
}

#[tokio::test]
async fn missing_statistics_count_as_retained() -> Result<()> {
    let data = write_parquet(
        Arc::new(Int32Array::from(vec![Some(0), None, Some(5), Some(10)])),
        EnabledStatistics::None,
    )?;
    let reader = TestReader::new(data);
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    assert!(
        builder
            .metadata()
            .row_groups()
            .iter()
            .all(|group| group.column(0).statistics().is_none())
    );
    let metrics = ParquetFileMetrics::new(0, "missing_stats", &ExecutionPlanMetricsSet::new());
    assert_eq!(
        prune(
            &mut builder,
            comparison(Operator::Eq, ScalarValue::Int32(Some(100))),
            ParquetAccessPlan::new_all(2),
            &metrics,
        )?,
        [0, 1]
    );
    assert_metrics(&metrics, 0, 2, 0);
    Ok(())
}

#[tokio::test]
async fn failed_evaluations_do_not_count_as_matches() -> Result<()> {
    let data = write_parquet(
        Arc::new(Int32Array::from(vec![0, 10, 5, 10])),
        EnabledStatistics::Chunk,
    )?;
    for fail_after in [0, 1] {
        let mut builder =
            ParquetRecordBatchStreamBuilder::new(TestReader::new(data.clone())).await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        // A volatile scalar injects an error in the first or second pruning pass.
        let udf = create_udf(
            "pruning_error",
            vec![],
            DataType::Int32,
            Volatility::Volatile,
            Arc::new(move |_| {
                if observed.fetch_add(1, Ordering::Relaxed) >= fail_after {
                    return Err(DataFusionError::Execution(
                        "injected pruning error".to_owned(),
                    ));
                }
                Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(-100))))
            }),
        );
        let scalar = Arc::new(ScalarFunctionExpr::new(
            "pruning_error",
            Arc::new(udf),
            vec![],
            Arc::new(Field::new("result", DataType::Int32, false)),
            Arc::default(),
        ));
        let expression = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("x", 0)),
            Operator::Gt,
            scalar,
        ));
        let metrics = ParquetFileMetrics::new(0, "error", &ExecutionPlanMetricsSet::new());
        assert_eq!(
            prune(
                &mut builder,
                expression,
                ParquetAccessPlan::new_all(2),
                &metrics
            )?,
            [0, 1]
        );
        // Plain statistics run once for all groups; dictionary pruning runs per group.
        let errors = if fail_after == 0 { 1 } else { 2 };
        assert_eq!(calls.load(Ordering::Relaxed), fail_after + errors);
        assert_metrics(&metrics, 0, 0, errors);
    }
    Ok(())
}

#[tokio::test]
async fn dictionary_read_errors_retain_candidates() -> Result<()> {
    let data = Bytes::from_static(include_bytes!("data/integer-dictionary.parquet"));
    let reader = TestReader::new(data);
    let reads = reader.reads.clone();
    let fail_reads = reader.fail_reads.clone();
    let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let metrics = ParquetFileMetrics::new(0, "read_error", &ExecutionPlanMetricsSet::new());
    fail_reads.store(true, Ordering::Relaxed);
    assert_eq!(
        prune(
            &mut builder,
            comparison(Operator::Eq, ScalarValue::Int32(Some(5))),
            ParquetAccessPlan::new_all(3),
            &metrics,
        )?,
        [1, 2]
    );
    assert!(reads.load(Ordering::Relaxed) > 0);
    // Dictionary I/O failure is treated as missing statistics.
    assert_metrics(&metrics, 1, 2, 0);

    fail_reads.store(false, Ordering::Relaxed);
    assert_eq!(
        prune(
            &mut builder,
            comparison(Operator::Eq, ScalarValue::Int32(Some(5))),
            ParquetAccessPlan::new_all(3),
            &metrics,
        )?,
        [2]
    );
    assert_metrics(&metrics, 3, 3, 0);
    Ok(())
}
