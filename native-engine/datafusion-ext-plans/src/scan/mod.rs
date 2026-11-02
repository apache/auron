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

use std::{fmt::Debug, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::{
    array::{Array, ArrayRef, AsArray, ListArray},
    datatypes::{DataType, Schema, SchemaRef},
};
use arrow_schema::Field;
use datafusion::{
    common::{ColumnStatistics, Result, stats::Precision},
    datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper},
    parquet::arrow::PARQUET_FIELD_ID_META_KEY,
};
use datafusion_ext_commons::df_execution_err;

type CastColumn = dyn Fn(&ArrayRef, &Field) -> Result<ArrayRef> + Send + Sync;

/// Replacement for datafusion's removed `SchemaMapping`, re-implementing the
/// same field-remapping + per-column casting behavior that was dropped in
/// favor of `PhysicalExprAdapterFactory`.
pub struct AuronSchemaMapping {
    table_schema: SchemaRef,
    field_mappings: Vec<Option<usize>>,
    cast_column: Arc<CastColumn>,
}

impl Debug for AuronSchemaMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuronSchemaMapping")
            .field("table_schema", &self.table_schema)
            .field("field_mappings", &self.field_mappings)
            .finish()
    }
}

impl AuronSchemaMapping {
    pub fn new(
        table_schema: SchemaRef,
        field_mappings: Vec<Option<usize>>,
        cast_column: Arc<CastColumn>,
    ) -> Self {
        Self {
            table_schema,
            field_mappings,
            cast_column,
        }
    }
}

impl SchemaMapper for AuronSchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let cols = self
            .table_schema
            .fields()
            .iter()
            .zip(&self.field_mappings)
            .map(|(field, file_idx)| match file_idx {
                Some(batch_idx) => (self.cast_column)(batch.column(*batch_idx), field),
                None => Ok(arrow::array::new_null_array(
                    field.data_type(),
                    batch.num_rows(),
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(RecordBatch::try_new_with_options(
            self.table_schema.clone(),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?)
    }

    fn map_column_statistics(
        &self,
        file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        Ok(self
            .field_mappings
            .iter()
            .map(|file_idx| {
                file_idx
                    .and_then(|idx| file_col_statistics.get(idx))
                    .cloned()
                    .unwrap_or_else(|| ColumnStatistics {
                        null_count: Precision::Absent,
                        max_value: Precision::Absent,
                        min_value: Precision::Absent,
                        sum_value: Precision::Absent,
                        distinct_count: Precision::Absent,
                        byte_size: Precision::Absent,
                    })
            })
            .collect())
    }
}

pub mod internal_file_reader;
pub mod parallel;

#[derive(Debug)]
pub struct AuronSchemaAdapterFactory;

impl SchemaAdapterFactory for AuronSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(AuronSchemaAdapter::new(projected_table_schema))
    }
}

pub struct AuronSchemaAdapter {
    table_schema: SchemaRef,
}

impl AuronSchemaAdapter {
    pub fn new(table_schema: SchemaRef) -> Self {
        Self { table_schema }
    }
}

impl SchemaAdapter for AuronSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);

        file_schema
            .fields()
            .iter()
            .position(|file_field| fields_match(field, file_field))
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; self.table_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some(table_idx) = self
                .table_schema
                .fields()
                .iter()
                .position(|table_field| fields_match(table_field, file_field))
            {
                field_mappings[table_idx] = Some(projection.len());
                projection.push(file_idx);
            }
        }

        let schema_mapper: Arc<dyn SchemaMapper> = Arc::new(AuronSchemaMapping::new(
            self.table_schema.clone(),
            field_mappings,
            Arc::new(|col, field| schema_adapter_cast_column(col, field)),
        ));
        Ok((schema_mapper, projection))
    }
}

fn fields_match(table_field: &Field, file_field: &Field) -> bool {
    match table_field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
        Some(table_field_id) => match file_field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
            Some(file_field_id) => {
                if file_field_id == table_field_id {
                    true
                } else {
                    table_field.name().eq_ignore_ascii_case(file_field.name())
                }
            }
            None => table_field.name().eq_ignore_ascii_case(file_field.name()),
        },
        None => table_field.name().eq_ignore_ascii_case(file_field.name()),
    }
}

pub fn create_auron_schema_mapper(
    table_schema: &SchemaRef,
    field_mappings: &[Option<usize>],
) -> Arc<dyn SchemaMapper> {
    Arc::new(AuronSchemaMapping::new(
        table_schema.clone(),
        field_mappings.to_vec(),
        Arc::new(|col, field| schema_adapter_cast_column(col, field)),
    ))
}

fn schema_adapter_cast_column(col: &ArrayRef, field: &Field) -> Result<ArrayRef> {
    macro_rules! handle_decimal {
        ($s:ident, $t:ident, $tnative:ty, $prec:expr, $scale:expr) => {{
            use arrow::{array::*, datatypes::*};
            type DecimalBuilder = paste::paste! {[<$t Builder>]};
            type IntType = paste::paste! {[<$s Type>]};

            let col = col.as_primitive::<IntType>();
            let mut decimal_builder = DecimalBuilder::new();
            for i in 0..col.len() {
                if col.is_valid(i) {
                    decimal_builder.append_value(col.value(i) as $tnative);
                } else {
                    decimal_builder.append_null();
                }
            }
            Ok(Arc::new(
                decimal_builder
                    .finish()
                    .with_precision_and_scale($prec, $scale)?,
            ))
        }};
    }

    let col_dt = col.data_type();
    let cast_dt = field.data_type();

    match cast_dt {
        DataType::Decimal128(prec, scale) => match col_dt {
            DataType::Int8 => handle_decimal!(Int8, Decimal128, i128, *prec, *scale),
            DataType::Int16 => handle_decimal!(Int16, Decimal128, i128, *prec, *scale),
            DataType::Int32 => handle_decimal!(Int32, Decimal128, i128, *prec, *scale),
            DataType::Int64 => handle_decimal!(Int64, Decimal128, i128, *prec, *scale),
            DataType::Decimal128(..) => {
                datafusion_ext_commons::arrow::cast::cast_scan_input_array(col.as_ref(), cast_dt)
            }
            _ => df_execution_err!(
                "schema_adapter_cast_column unsupported type: {col_dt:?} => {cast_dt:?}",
            ),
        },
        DataType::List(to_field) => match col_dt {
            DataType::List(_from_field) => {
                let col = col.as_list::<i32>();
                let from_inner = col.values();
                let to_inner = schema_adapter_cast_column(from_inner, &to_field)?;
                Ok(Arc::new(ListArray::try_new(
                    to_field.clone(),
                    col.offsets().clone(),
                    to_inner,
                    col.nulls().cloned(),
                )?))
            }
            _ => df_execution_err!(
                "schema_adapter_cast_column unsupported type: {col_dt:?} => {cast_dt:?}",
            ),
        },
        _ => datafusion_ext_commons::arrow::cast::cast_scan_input_array(col.as_ref(), cast_dt),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_projection_preserves_row_count() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let adapter = AuronSchemaAdapter::new(schema.clone());
        let file_schema = Schema::new(vec![Field::new("value", DataType::Int32, true)]);
        let (mapper, projection) = adapter.map_schema(&file_schema)?;
        assert!(projection.is_empty());

        for num_rows in [0, 1, 1024] {
            let batch = RecordBatch::try_new_with_options(
                schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(num_rows)),
            )?;
            let mapped = mapper.map_batch(batch)?;
            assert_eq!(mapped.num_columns(), 0);
            assert_eq!(mapped.num_rows(), num_rows);
            assert_eq!(mapped.schema(), schema);
        }
        Ok(())
    }
}
