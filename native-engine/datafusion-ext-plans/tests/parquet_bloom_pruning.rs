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

use std::{io::Cursor, sync::Arc};

use arrow::{
    array::Decimal128Array,
    datatypes::{DataType, Field, Schema},
};
use bytes::Bytes;
use datafusion::{
    common::ScalarValue,
    logical_expr::Operator,
    parquet::{
        arrow::{ParquetRecordBatchStreamBuilder, arrow_reader::ParquetRecordBatchReaderBuilder},
        data_type::{FixedLenByteArray, FixedLenByteArrayType},
        file::{properties::WriterProperties, writer::SerializedFileWriter},
        schema::parser::parse_message_type,
    },
    physical_expr::expressions::{BinaryExpr, Column, Literal},
    physical_optimizer::pruning::PruningPredicate,
    physical_plan::metrics::ExecutionPlanMetricsSet,
};
use datafusion_datasource_parquet::{
    ParquetAccessPlan, ParquetFileMetrics, RowGroupAccessPlanFilter,
};

fn decimal_file(
    precision: u8,
    scale: i8,
    width: usize,
    bloom_enabled: bool,
) -> datafusion::error::Result<Bytes> {
    let schema = Arc::new(parse_message_type(&format!(
        "message schema {{ OPTIONAL FIXED_LEN_BYTE_ARRAY ({width}) d (DECIMAL({precision},{scale})); }}"
    ))?);
    let properties = Arc::new(
        WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_bloom_filter_ndv(16)
            .set_bloom_filter_enabled(bloom_enabled)
            .build(),
    );
    let mut output = Vec::new();
    let mut writer = SerializedFileWriter::new(&mut output, schema, properties)?;
    let bound = 10_i128.pow(u32::from(precision)) - 1;
    let point = 500.min(bound / 2);
    for raw in [-point, 0, point] {
        // Shared bounds keep every row group after statistics pruning.
        let values = [-bound, raw, bound]
            .into_iter()
            .map(|value| {
                let mut bytes = vec![if value < 0 { 0xff } else { 0 }; width];
                let len = width.min(16);
                bytes[width - len..].copy_from_slice(&value.to_be_bytes()[16 - len..]);
                FixedLenByteArray::from(bytes)
            })
            .collect::<Vec<_>>();
        let mut row_group = writer.next_row_group()?;
        let mut column = row_group.next_column()?.expect("decimal column");
        column
            .typed::<FixedLenByteArrayType>()
            .write_batch(&values, Some(&[1, 1, 1, 0]), None)?;
        column.close()?;
        row_group.close()?;
    }
    writer.close()?;
    Ok(Bytes::from(output))
}

fn read_values(data: Bytes, groups: Vec<usize>) -> datafusion::error::Result<Vec<Option<i128>>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(data)?
        .with_row_groups(groups)
        .build()?;
    let mut values = vec![];
    for batch in reader {
        let batch = batch?;
        let decimals = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal column");
        values.extend(decimals.iter());
    }
    Ok(values)
}

#[tokio::test]
async fn decimal_fixed_length_bloom_pruning() -> datafusion::error::Result<()> {
    for (precision, width) in [(2, 1), (4, 2), (9, 4), (18, 8), (20, 9), (28, 12), (38, 16)] {
        let bound = 10_i128.pow(u32::from(precision)) - 1;
        let point = 500.min(bound / 2);
        let scales = if precision == 2 {
            [0, 1, 2]
        } else {
            [0, 2, precision as i8]
        };
        for scale in scales {
            for bloom_enabled in [true, false] {
                let data = decimal_file(precision, scale, width, bloom_enabled)?;
                let reference = read_values(data.clone(), vec![0, 1, 2])?;
                assert_eq!(reference.iter().filter(|value| value.is_none()).count(), 3);
                let mut builder =
                    ParquetRecordBatchStreamBuilder::new(Cursor::new(data.clone())).await?;
                let schema = builder.schema().clone();
                let metadata = builder.metadata().clone();
                assert_eq!(
                    builder.parquet_schema().column(0).type_length(),
                    width as i32
                );
                assert_eq!(metadata.num_row_groups(), 3);
                assert_eq!(metadata.file_metadata().num_rows(), 12);

                for (raw, expected) in [
                    (-bound, vec![0, 1, 2]),
                    (-point, vec![0]),
                    (0, vec![1]),
                    (point, vec![2]),
                    (bound, vec![0, 1, 2]),
                    (point / 2, vec![]),
                ] {
                    let predicate = PruningPredicate::try_new(
                        Arc::new(BinaryExpr::new(
                            Arc::new(Column::new("d", 0)),
                            Operator::Eq,
                            Arc::new(Literal::new(ScalarValue::Decimal128(
                                Some(raw),
                                precision,
                                scale,
                            ))),
                        )),
                        schema.clone(),
                    )?;
                    let metrics =
                        ParquetFileMetrics::new(0, "decimal", &ExecutionPlanMetricsSet::new());
                    let mut filter = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(3));
                    filter.prune_by_statistics(
                        &mut builder,
                        &schema,
                        metadata.row_groups(),
                        &predicate,
                        &metrics,
                    );
                    assert_eq!(filter.clone().build().row_group_indexes(), vec![0, 1, 2]);
                    filter
                        .prune_by_bloom_filters(&schema, &mut builder, &predicate, &metrics)
                        .await;
                    let kept = filter.build().row_group_indexes();
                    let expected = if bloom_enabled {
                        expected
                    } else {
                        vec![0, 1, 2]
                    };
                    assert_eq!(
                        kept, expected,
                        "precision={precision}, scale={scale}, width={width}, raw={raw}"
                    );
                    assert_eq!(metrics.predicate_evaluation_errors.value(), 0);
                    assert_eq!(
                        metrics.row_groups_pruned_bloom_filter.value(),
                        if bloom_enabled { 3 - kept.len() } else { 0 },
                    );
                    assert_eq!(
                        metrics.row_groups_matched_bloom_filter.value(),
                        if bloom_enabled { kept.len() } else { 0 },
                    );
                    let matches = |values: Vec<Option<i128>>| {
                        values
                            .into_iter()
                            .filter(|value| *value == Some(raw))
                            .count()
                    };
                    let expected_matches = if raw == point / 2 {
                        0
                    } else if raw == -bound || raw == bound {
                        3
                    } else {
                        1
                    };
                    assert_eq!(matches(reference.clone()), expected_matches);
                    assert_eq!(
                        matches(read_values(data.clone(), kept)?),
                        matches(reference.clone())
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn decimal_bloom_pruning_unsupported_width() -> datafusion::error::Result<()> {
    let data = decimal_file(18, 2, 17, true)?;
    let mut builder = ParquetRecordBatchStreamBuilder::new(Cursor::new(data)).await?;
    // Arrow infers Decimal256 for 17 bytes; explicitly exercise the Decimal128
    // guard.
    let schema = Arc::new(Schema::new(vec![Field::new(
        "d",
        DataType::Decimal128(18, 2),
        true,
    )]));
    let predicate = PruningPredicate::try_new(
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("d", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Decimal128(Some(500), 18, 2))),
        )),
        schema.clone(),
    )?;
    let metrics = ParquetFileMetrics::new(0, "decimal", &ExecutionPlanMetricsSet::new());
    let mut filter = RowGroupAccessPlanFilter::new(ParquetAccessPlan::new_all(3));
    filter
        .prune_by_bloom_filters(&schema, &mut builder, &predicate, &metrics)
        .await;
    assert_eq!(filter.build().row_group_indexes(), vec![0, 1, 2]);
    assert_eq!(metrics.row_groups_pruned_bloom_filter.value(), 0);
    assert_eq!(metrics.row_groups_matched_bloom_filter.value(), 3);
    assert_eq!(metrics.predicate_evaluation_errors.value(), 0);
    Ok(())
}
