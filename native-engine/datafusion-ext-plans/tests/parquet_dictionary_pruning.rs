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
            let schema = builder.schema().clone();
            let metadata = builder.metadata().clone();
            assert_eq!(metadata.num_row_groups(), 1);
            let dictionary =
                get_dictionary_for_pruning(&mut builder.input.0, metadata.row_group(0), 0)?;
            assert_eq!(dictionary.is_some(), has_dictionary);

            for raw in [-100, 0, 100, 150, 200] {
                let predicate = PruningPredicate::try_new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(Column::new("d", 0)),
                        Operator::Eq,
                        Arc::new(Literal::new(ScalarValue::Decimal128(
                            Some(raw),
                            precision,
                            2,
                        ))),
                    )),
                    schema.clone(),
                )?;
                let metrics =
                    ParquetFileMetrics::new(0, "decimal", &ExecutionPlanMetricsSet::new());
                let mut filter = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(1));
                filter.prune_by_statistics(
                    &mut builder,
                    &schema,
                    metadata.row_groups(),
                    &predicate,
                    &metrics,
                );
                let kept = filter.build().row_group_indexes();
                let expected = if has_dictionary && raw == 150 {
                    vec![]
                } else {
                    vec![0]
                };
                assert_eq!(kept, expected, "precision={precision}, raw={raw}");
                assert_eq!(metrics.predicate_evaluation_errors.value(), 0);

                let reader = ParquetRecordBatchReaderBuilder::try_new(data.clone())?
                    .with_row_groups(kept)
                    .build()?;
                let mut matches = 0;
                for batch in reader {
                    let batch = batch?;
                    let values = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .expect("decimal column");
                    matches += values.iter().filter(|value| *value == Some(raw)).count();
                }
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
