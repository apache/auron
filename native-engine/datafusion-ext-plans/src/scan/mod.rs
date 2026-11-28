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

use std::{collections::HashMap, fmt::Debug, sync::Arc};

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::{
    array::{Array, ArrayRef, AsArray, ListArray},
    datatypes::{DataType, Schema, SchemaRef},
};
use arrow_schema::Field;
use datafusion::{
    common::{
        ColumnStatistics, Result, ScalarValue,
        stats::Precision,
        tree_node::{Transformed, TransformedResult, TreeNode},
    },
    datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper},
    parquet::arrow::PARQUET_FIELD_ID_META_KEY,
    physical_expr::{
        PhysicalExprRef,
        expressions::{CastExpr, Column, Literal},
    },
    physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory},
};
use datafusion_ext_commons::df_execution_err;

type CastColumn = dyn Fn(&ArrayRef, &Field) -> Result<ArrayRef> + Send + Sync;

fn initial_default<'a>(
    defaults: &'a HashMap<i32, ScalarValue>,
    field: &Field,
) -> Option<&'a ScalarValue> {
    if defaults.is_empty() {
        return None;
    }
    let id = field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()?;
    defaults.get(&id)
}

/// Replacement for datafusion's removed `SchemaMapping`, re-implementing the
/// same field-remapping + per-column casting behavior that was dropped in
/// favor of `PhysicalExprAdapterFactory`.
pub struct AuronSchemaMapping {
    table_schema: SchemaRef,
    field_mappings: Vec<Option<usize>>,
    cast_column: Arc<CastColumn>,
    initial_defaults: Arc<HashMap<i32, ScalarValue>>,
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
        initial_defaults: Arc<HashMap<i32, ScalarValue>>,
    ) -> Self {
        Self {
            table_schema,
            field_mappings,
            cast_column,
            initial_defaults,
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
                None => match initial_default(&self.initial_defaults, field) {
                    Some(value) => value.to_array_of_size(batch.num_rows()),
                    None => Ok(arrow::array::new_null_array(
                        field.data_type(),
                        batch.num_rows(),
                    )),
                },
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

#[derive(Debug, Default)]
pub struct AuronSchemaAdapterFactory {
    initial_defaults: Arc<HashMap<i32, ScalarValue>>,
}

impl AuronSchemaAdapterFactory {
    pub fn new(initial_defaults: HashMap<i32, ScalarValue>) -> Self {
        Self {
            initial_defaults: Arc::new(initial_defaults),
        }
    }

    fn adapter(&self, table_schema: SchemaRef) -> AuronSchemaAdapter {
        AuronSchemaAdapter {
            table_schema,
            initial_defaults: self.initial_defaults.clone(),
        }
    }
}

impl SchemaAdapterFactory for AuronSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        _table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(self.adapter(projected_table_schema))
    }
}

#[derive(Debug)]
struct AuronSchemaAdapter {
    table_schema: SchemaRef,
    initial_defaults: Arc<HashMap<i32, ScalarValue>>,
}

impl AuronSchemaAdapter {
    fn resolve_column_index(&self, index: usize, file_schema: &Schema) -> Result<Option<usize>> {
        let file_index = self.map_column_index(index, file_schema);
        let field = self.table_schema.field(index);
        // An unidentified file field could be an earlier name of the defaulted field.
        if file_index.is_none()
            && initial_default(&self.initial_defaults, field).is_some()
            && file_schema
                .fields()
                .iter()
                .any(|field| !field.metadata().contains_key(PARQUET_FIELD_ID_META_KEY))
        {
            return df_execution_err!(
                "Cannot determine whether Iceberg field {} is missing from a Parquet file without complete field IDs",
                field.name()
            );
        }
        Ok(file_index)
    }
}

impl SchemaAdapter for AuronSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        let requires_id = initial_default(&self.initial_defaults, field).is_some();

        file_schema.fields().iter().position(|file_field| {
            (!requires_id
                || file_field
                    .metadata()
                    .contains_key(PARQUET_FIELD_ID_META_KEY))
                && fields_match(field, file_field)
        })
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; self.table_schema.fields().len()];
        let mut default_matches = HashMap::new();
        let mut fields_without_default = Vec::with_capacity(self.table_schema.fields().len());

        for (table_idx, field) in self.table_schema.fields().iter().enumerate() {
            if initial_default(&self.initial_defaults, field).is_some() {
                if let Some(file_idx) = self.resolve_column_index(table_idx, file_schema)? {
                    default_matches.insert(file_idx, table_idx);
                }
            } else {
                fields_without_default.push((table_idx, field));
            }
        }

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            let table_idx = default_matches.get(&file_idx).copied().or_else(|| {
                fields_without_default
                    .iter()
                    .find_map(|&(table_idx, table_field)| {
                        fields_match(table_field, file_field).then_some(table_idx)
                    })
            });
            if let Some(table_idx) = table_idx {
                field_mappings[table_idx] = Some(projection.len());
                projection.push(file_idx);
            }
        }

        let schema_mapper: Arc<dyn SchemaMapper> = Arc::new(AuronSchemaMapping::new(
            self.table_schema.clone(),
            field_mappings,
            Arc::new(|col, field| schema_adapter_cast_column(col, field)),
            self.initial_defaults.clone(),
        ));
        Ok((schema_mapper, projection))
    }
}

fn fields_match(table_field: &Field, file_field: &Field) -> bool {
    match table_field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
        Some(table_field_id) => match file_field.metadata().get(PARQUET_FIELD_ID_META_KEY) {
            // A dropped and re-added column can reuse its name, but not its id.
            Some(file_field_id) => file_field_id == table_field_id,
            None => table_field.name().eq_ignore_ascii_case(file_field.name()),
        },
        None => table_field.name().eq_ignore_ascii_case(file_field.name()),
    }
}

// Adapt predicates with the same field identities and defaults as the projection.
impl PhysicalExprAdapterFactory for AuronSchemaAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(AuronPhysicalExprAdapter {
            logical: self.adapter(logical_file_schema),
            physical_file_schema,
        }))
    }
}

#[derive(Debug)]
struct AuronPhysicalExprAdapter {
    logical: AuronSchemaAdapter,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for AuronPhysicalExprAdapter {
    fn rewrite(&self, expr: PhysicalExprRef) -> Result<PhysicalExprRef> {
        expr.transform_up(|expr| {
            let Some(column) = expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };
            let index = self.logical.table_schema.index_of(column.name())?;
            let field = self.logical.table_schema.field(index);
            let mapped: PhysicalExprRef = match self
                .logical
                .resolve_column_index(index, &self.physical_file_schema)?
            {
                Some(file_index) => {
                    let file_field = self.physical_file_schema.field(file_index);
                    let column = Arc::new(Column::new(file_field.name(), file_index));
                    if file_field.data_type() == field.data_type() {
                        column
                    } else {
                        Arc::new(CastExpr::new(column, field.data_type().clone(), None))
                    }
                }
                None => {
                    let value = match initial_default(&self.logical.initial_defaults, field) {
                        Some(value) => value.clone(),
                        None => ScalarValue::try_from(field.data_type())?,
                    };
                    Arc::new(Literal::new(value))
                }
            };
            Ok(Transformed::yes(mapped))
        })
        .data()
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
        Arc::default(),
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
    use arrow::array::Int32Array;

    fn field_with_id(name: &str, id: &str) -> Field {
        Field::new(name, DataType::Int32, true)
            .with_metadata([(PARQUET_FIELD_ID_META_KEY.to_string(), id.to_string())].into())
    }

    #[test]
    fn test_field_ids_take_precedence_over_names() -> Result<()> {
        let table_schema = Arc::new(Schema::new(vec![field_with_id("value", "3")]));
        let adapter = AuronSchemaAdapterFactory::default().adapter(table_schema.clone());

        // A dropped and re-added column has the same name but a new identity.
        // If its field id is absent, the old values must not reappear.
        for has_matching_id in [false, true] {
            let mut fields = vec![field_with_id("value", "2")];
            let mut columns = vec![Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef];
            if has_matching_id {
                // A rename keeps its field id, even when another field has its name.
                fields.push(field_with_id("previous_name", "3"));
                columns.push(Arc::new(Int32Array::from(vec![30, 40])));
            }
            let file_schema = Arc::new(Schema::new(fields));
            let file_batch = RecordBatch::try_new(file_schema.clone(), columns)?;
            let (mapper, projection) = adapter.map_schema(&file_schema)?;
            assert_eq!(
                adapter.map_column_index(0, &file_schema),
                has_matching_id.then_some(1)
            );
            assert_eq!(projection, if has_matching_id { vec![1] } else { vec![] });
            let mapped = mapper.map_batch(file_batch.project(&projection)?)?;
            assert_eq!(mapped.schema(), table_schema);
            assert_eq!(mapped.num_rows(), 2);
            assert_eq!(
                mapped
                    .column(0)
                    .as_primitive::<arrow::datatypes::Int32Type>(),
                &Int32Array::from(if has_matching_id {
                    vec![Some(30), Some(40)]
                } else {
                    vec![None, None]
                })
            );
        }
        Ok(())
    }

    #[test]
    fn test_field_names_are_used_when_ids_are_unavailable() {
        for (table, file) in [
            (
                field_with_id("value", "3"),
                Field::new("VALUE", DataType::Int32, true),
            ),
            (
                Field::new("value", DataType::Int32, true),
                field_with_id("VALUE", "2"),
            ),
            (
                Field::new("value", DataType::Int32, true),
                Field::new("VALUE", DataType::Int32, true),
            ),
        ] {
            assert!(fields_match(&table, &file));
            assert!(!fields_match(&table, &file.with_name("different")));
        }
    }

    #[test]
    fn test_predicates_follow_field_ids_and_preserve_promoted_types() -> Result<()> {
        use datafusion::physical_expr::expressions::IsNullExpr;

        let logical_schema = Arc::new(Schema::new(vec![
            field_with_id("value", "3").with_data_type(DataType::Int64),
        ]));
        for has_matching_id in [false, true] {
            let mut fields = vec![field_with_id("value", "2")];
            let mut columns = vec![Arc::new(Int32Array::from(vec![10, 20])) as ArrayRef];
            if has_matching_id {
                fields.push(field_with_id("previous_name", "3"));
                columns.push(Arc::new(Int32Array::from(vec![Some(30), None])));
            }
            let physical_schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(physical_schema.clone(), columns)?;
            let adapter = PhysicalExprAdapterFactory::create(
                &AuronSchemaAdapterFactory::default(),
                logical_schema.clone(),
                physical_schema.clone(),
            )?;
            let column = Arc::new(Column::new("value", 0)) as PhysicalExprRef;
            let rewritten = adapter.rewrite(column.clone())?;
            assert_eq!(rewritten.data_type(&physical_schema)?, DataType::Int64);
            let values = rewritten.evaluate(&batch)?.into_array(2)?;
            assert_eq!(
                values.as_primitive::<arrow::datatypes::Int64Type>(),
                &arrow::array::Int64Array::from(if has_matching_id {
                    vec![Some(30), None]
                } else {
                    vec![None, None]
                }),
            );
            let predicate = adapter.rewrite(Arc::new(IsNullExpr::new(column)))?;
            let result = predicate.evaluate(&batch)?.into_array(2)?;
            assert_eq!(
                result.as_boolean(),
                &arrow::array::BooleanArray::from(vec![!has_matching_id, true]),
            );
        }
        Ok(())
    }

    #[test]
    fn test_initial_default_uses_field_identity() -> Result<()> {
        let logical_schema = Arc::new(Schema::new(vec![field_with_id("value", "3")]));
        let factory = AuronSchemaAdapterFactory::new([(3, ScalarValue::Int32(Some(7)))].into());
        let schema_adapter = factory.adapter(logical_schema.clone());
        for (file_fields, file_columns, expected) in [
            (
                vec![field_with_id("value", "2")],
                vec![Arc::new(Int32Array::from(vec![Some(90)])) as ArrayRef],
                Some(7),
            ),
            (
                vec![
                    Field::new("value", DataType::Int32, true),
                    field_with_id("previous_name", "3"),
                ],
                vec![
                    Arc::new(Int32Array::from(vec![Some(90)])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![None])),
                ],
                None,
            ),
        ] {
            let physical_schema = Arc::new(Schema::new(file_fields));
            let batch = RecordBatch::try_new(physical_schema.clone(), file_columns)?;
            let (mapper, projection) = schema_adapter.map_schema(&physical_schema)?;
            let projected = mapper.map_batch(batch.project(&projection)?)?;
            let expr_adapter = PhysicalExprAdapterFactory::create(
                &factory,
                logical_schema.clone(),
                physical_schema,
            )?;
            let rewritten = expr_adapter.rewrite(Arc::new(Column::new("value", 0)))?;
            let expr_result = rewritten.evaluate(&batch)?.into_array(1)?;
            let expected = Int32Array::from(vec![expected]);
            assert_eq!(
                projected
                    .column(0)
                    .as_primitive::<arrow::datatypes::Int32Type>(),
                &expected,
            );
            assert_eq!(
                expr_result.as_primitive::<arrow::datatypes::Int32Type>(),
                &expected
            );
        }
        let idless = Arc::new(Schema::new(vec![Field::new(
            "unknown",
            DataType::Int32,
            true,
        )]));
        assert!(schema_adapter.map_schema(&idless).is_err());
        let expr_adapter = PhysicalExprAdapterFactory::create(&factory, logical_schema, idless)?;
        assert!(
            expr_adapter
                .rewrite(Arc::new(Column::new("value", 0)))
                .is_err()
        );
        Ok(())
    }

    #[test]
    fn test_empty_projection_preserves_row_count() -> Result<()> {
        let schema = Arc::new(Schema::empty());
        let adapter = AuronSchemaAdapterFactory::default().adapter(schema.clone());
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
