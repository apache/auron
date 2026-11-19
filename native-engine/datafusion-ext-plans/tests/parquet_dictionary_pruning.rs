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

use std::{ops::Range, sync::Arc};

use arrow::{array::Decimal128Array, datatypes::DataType};
use bytes::Bytes;
use datafusion::{
    common::ScalarValue,
    logical_expr::Operator,
    parquet::{
        arrow::{
            ArrowWriter, ParquetRecordBatchStreamBuilder,
            arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder},
            async_reader::AsyncFileReader,
        },
        auron::get_dictionary_for_pruning,
        errors::Result,
        file::{
            metadata::ParquetMetaData,
            properties::{WriterProperties, WriterVersion},
        },
    },
    physical_expr::expressions::{BinaryExpr, Column, Literal},
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use datafusion_datasource_parquet::{
    ParquetAccessPlan, ParquetFileMetrics, RowGroupAccessPlanFilter,
};
use futures::future::BoxFuture;

struct TestReader(Bytes);

impl AsyncFileReader for TestReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        Box::pin(async move { self.get_bytes_sync(range) })
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            Ok(ParquetRecordBatchReaderBuilder::try_new(self.0.clone())?
                .metadata()
                .clone())
        })
    }

    fn get_bytes_sync(&mut self, range: Range<u64>) -> Result<Bytes> {
        Ok(self.0.slice(range.start as usize..range.end as usize))
    }
}

fn prune_row_groups(
    builder: &mut ParquetRecordBatchStreamBuilder<TestReader>,
    literal: ScalarValue,
) -> datafusion::error::Result<Vec<usize>> {
    let schema = builder.schema().clone();
    let metadata = builder.metadata().clone();
    let predicate = PruningPredicate::try_new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("d", 0)),
            Operator::Eq,
            Arc::new(Literal::new(literal)),
        )),
        schema.clone(),
    )?;
    let metrics = ParquetFileMetrics::new(0, "decimal", &ExecutionPlanMetricsSet::new());
    let mut filter =
        RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(metadata.num_row_groups()));
    filter.prune_by_statistics(
        builder,
        &schema,
        metadata.row_groups(),
        &predicate,
        &metrics,
    );
    assert_eq!(metrics.predicate_evaluation_errors.value(), 0);
    Ok(filter.build().row_group_indexes())
}

fn read_decimal_values(
    data: Bytes,
    row_groups: Vec<usize>,
) -> datafusion::error::Result<Vec<Option<i128>>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)?
        .with_row_groups(row_groups)
        .build()?;
    let mut values = vec![];
    for batch in reader {
        let batch = batch?;
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal column");
        values.extend(array.iter());
    }
    Ok(values)
}

#[tokio::test]
async fn decimal_dictionary_pruning() -> datafusion::error::Result<()> {
    for (data, precision) in [
        (
            include_bytes!("data/decimal-int32-dictionary.parquet").as_slice(),
            9,
        ),
        (
            include_bytes!("data/decimal-int64-dictionary.parquet").as_slice(),
            18,
        ),
    ] {
        let data = Bytes::copy_from_slice(data);
        let batch = ParquetRecordBatchReaderBuilder::try_new(data.clone())?
            .build()?
            .next()
            .expect("one record batch")?;

        // Rust writers use modern dictionary encodings, so keep real parquet-mr V1
        // fixtures.
        let mut files = vec![(data, true)];
        for dictionary_enabled in [false, true] {
            let properties = WriterProperties::builder()
                .set_writer_version(WriterVersion::PARQUET_2_0)
                .set_dictionary_enabled(dictionary_enabled)
                .build();
            let mut writer = ArrowWriter::try_new(Vec::new(), batch.schema(), Some(properties))?;
            writer.write(&batch)?;
            files.push((Bytes::from(writer.into_inner()?), false));
        }

        for (data, has_dictionary) in files {
            let mut builder =
                ParquetRecordBatchStreamBuilder::new(TestReader(data.clone())).await?;
            let metadata = builder.metadata().clone();
            assert_eq!(metadata.num_row_groups(), 1);
            let dictionary =
                get_dictionary_for_pruning(&mut builder.input.0, metadata.row_group(0), 0)?;
            assert_eq!(dictionary.is_some(), has_dictionary);

            for raw in [-100, 0, 100, 150, 200] {
                let kept = prune_row_groups(
                    &mut builder,
                    ScalarValue::Decimal128(Some(raw), precision, 2),
                )?;
                let expected = if has_dictionary && raw == 150 {
                    vec![]
                } else {
                    vec![0]
                };
                assert_eq!(kept, expected, "precision={precision}, raw={raw}");
                let matches = read_decimal_values(data.clone(), kept)?
                    .into_iter()
                    .filter(|value| *value == Some(raw))
                    .count();
                assert_eq!(matches, if raw == 150 { 0 } else { 250 });
            }
            if let Some(dictionary) = dictionary {
                assert_eq!(dictionary.data_type(), &DataType::Decimal128(precision, 2));
                let values = dictionary
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .expect("decimal dictionary");
                let mut values = values.values().to_vec();
                values.sort_unstable();
                assert_eq!(values, [-100, 0, 100, 200]);
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn decimal_dictionary_pruning_scales_and_row_groups() -> datafusion::error::Result<()> {
    for (data, precision, scale) in [
        (
            include_bytes!("data/decimal-int32-scale0-dictionary.parquet").as_slice(),
            9,
            0,
        ),
        (
            include_bytes!("data/decimal-int64-scale18-dictionary.parquet").as_slice(),
            18,
            18,
        ),
    ] {
        let data = Bytes::copy_from_slice(data);
        let mut builder = ParquetRecordBatchStreamBuilder::new(TestReader(data.clone())).await?;
        let metadata = builder.metadata().clone();
        assert_eq!(metadata.num_row_groups(), 6);
        assert_eq!(metadata.file_metadata().num_rows(), 600);
        let limit = 10_i128.pow(precision as u32) - 1;
        let mut groups = vec![];
        for group in 0..metadata.num_row_groups() {
            let values = read_decimal_values(data.clone(), vec![group])?;
            assert!(values.contains(&Some(-limit)) && values.contains(&Some(limit)));
            let dictionary =
                get_dictionary_for_pruning(&mut builder.input.0, metadata.row_group(group), 0)?
                    .expect("legacy decimal dictionary");
            assert_eq!(
                dictionary.data_type(),
                &DataType::Decimal128(precision, scale)
            );
            let mut actual = dictionary
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("decimal dictionary")
                .values()
                .to_vec();
            let mut expected = values.iter().flatten().copied().collect::<Vec<_>>();
            expected.sort_unstable();
            expected.dedup();
            actual.sort_unstable();
            assert_eq!(actual, expected);
            groups.push(values);
        }
        assert_eq!(
            groups
                .iter()
                .flatten()
                .filter(|value| value.is_none())
                .count(),
            100
        );

        for (raw, expected_matches) in [
            (-limit, 100),
            (-150, 50),
            (-100, 50),
            (0, 100),
            (1, 0),
            (100, 50),
            (150, 50),
            (limit, 100),
        ] {
            let expected_groups = groups
                .iter()
                .enumerate()
                .filter_map(|(index, values)| values.contains(&Some(raw)).then_some(index))
                .collect::<Vec<_>>();
            let kept = prune_row_groups(
                &mut builder,
                ScalarValue::Decimal128(Some(raw), precision, scale),
            )?;
            assert_eq!(
                kept, expected_groups,
                "precision={precision}, scale={scale}, raw={raw}"
            );
            if raw == 100 {
                assert!(!kept.is_empty() && kept.len() < groups.len());
            }
            let matches = read_decimal_values(data.clone(), kept)?
                .into_iter()
                .filter(|value| *value == Some(raw))
                .count();
            assert_eq!(matches, expected_matches);
        }
    }
    Ok(())
}
