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

use std::{any::Any, fmt, fmt::Formatter, pin::Pin, sync::Arc};

use arrow::{datatypes::SchemaRef, error::ArrowError};
use auron_jni_bridge::{
    conf, conf::BooleanConf, jni_call_static, jni_new_global_ref, jni_new_string,
};
use bytes::Bytes;
use datafusion::{
    datasource::{
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream},
        schema_adapter::SchemaMapper,
    },
    error::Result,
    execution::context::TaskContext,
    logical_expr::Operator,
    physical_expr::{
        EquivalenceProperties, PhysicalExprRef,
        expressions::{
            BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal, NotExpr, SCAndExpr,
            SCOrExpr,
        },
    },
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
        SendableRecordBatchStream, Statistics,
        execution_plan::{Boundedness, EmissionType},
        metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
    },
    scalar::ScalarValue,
};
use datafusion_datasource::PartitionedFile;
use datafusion_ext_commons::{batch_size, df_execution_err, hadoop_fs::FsProvider};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use futures_util::TryStreamExt;
use once_cell::sync::OnceCell;
use orc_rust::{
    TimestampPrecision,
    arrow_reader::ArrowReaderBuilder,
    predicate::{Predicate, PredicateValue},
    projection::ProjectionMask,
    reader::{AsyncChunkReader, metadata::FileMetadata},
};

use crate::{
    common::execution_context::ExecutionContext,
    scan::{create_auron_schema_mapper, internal_file_reader::InternalFileReader},
};

/// Execution plan for scanning one or more Orc partitions
#[derive(Debug, Clone)]
pub struct OrcExec {
    fs_resource_id: String,
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    metrics: ExecutionPlanMetricsSet,
    predicate: Option<PhysicalExprRef>,
    props: OnceCell<PlanProperties>,
}

impl OrcExec {
    /// Create a new Orc reader execution plan provided file list and
    /// schema.
    pub fn new(
        base_config: FileScanConfig,
        fs_resource_id: String,
        predicate: Option<PhysicalExprRef>,
    ) -> Self {
        let metrics = ExecutionPlanMetricsSet::new();

        let (projected_schema, _constraints, projected_statistics, _projected_output_ordering) =
            base_config.project();

        Self {
            fs_resource_id,
            base_config,
            projected_statistics,
            projected_schema,
            metrics,
            predicate,
            props: OnceCell::new(),
        }
    }
}

impl DisplayAs for OrcExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        let limit = self.base_config.limit;
        let projection = self.base_config.projection.clone();
        let file_group = &self.base_config.file_groups;
        let pred = &self.predicate;

        write!(
            f,
            "OrcExec: file_group={:?}, limit={:?}, projection={:?}, predicate={:?}",
            file_group, limit, projection, pred
        )
    }
}

impl ExecutionPlan for OrcExec {
    fn name(&self) -> &str {
        "OrcExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.projected_schema)
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                Partitioning::UnknownPartitioning(self.base_config.file_groups.len()),
                EmissionType::Both,
                Boundedness::Bounded,
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        let io_time = exec_ctx.register_timer_metric("io_time");

        // get fs object from jni bridge resource
        let resource_id = jni_new_string!(&self.fs_resource_id)?;
        let fs = jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
        let fs_provider = Arc::new(FsProvider::new(jni_new_global_ref!(fs.as_obj())?, &io_time));

        let projection = match self.base_config.file_column_projection_indices() {
            Some(proj) => proj,
            None => (0..self.base_config.file_schema.fields().len()).collect(),
        };

        let force_positional_evolution = conf::ORC_FORCE_POSITIONAL_EVOLUTION.value()?;
        let use_microsecond_precision = conf::ORC_TIMESTAMP_USE_MICROSECOND.value()?;
        let is_case_sensitive = conf::ORC_SCHEMA_CASE_SENSITIVE.value()?;

        let opener: Arc<dyn FileOpener> = Arc::new(OrcOpener {
            projection,
            batch_size: batch_size(),
            table_schema: self.base_config.file_schema.clone(),
            fs_provider,
            partition_index: partition,
            metrics: self.metrics.clone(),
            force_positional_evolution,
            use_microsecond_precision,
            is_case_sensitive,
            predicate: self.predicate.clone(),
        });

        let file_stream = Box::pin(FileStream::new(
            &self.base_config,
            partition,
            opener,
            exec_ctx.execution_plan_metrics(),
        )?);

        let timed_stream = execute_orc_scan(file_stream, exec_ctx.clone())?;
        Ok(exec_ctx.coalesce_with_default_batch_size(timed_stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }
}

fn execute_orc_scan(
    mut stream: Pin<Box<FileStream>>,
    exec_ctx: Arc<ExecutionContext>,
) -> Result<SendableRecordBatchStream> {
    Ok(exec_ctx
        .clone()
        .output_with_sender("OrcScan", move |sender| async move {
            sender.exclude_time(exec_ctx.baseline_metrics().elapsed_compute());
            let _timer = exec_ctx.baseline_metrics().elapsed_compute().timer();
            while let Some(batch) = stream.next().await.transpose()? {
                sender.send(batch).await;
            }
            Ok(())
        }))
}

struct OrcOpener {
    projection: Vec<usize>,
    batch_size: usize,
    table_schema: SchemaRef,
    fs_provider: Arc<FsProvider>,
    partition_index: usize,
    metrics: ExecutionPlanMetricsSet,
    force_positional_evolution: bool,
    use_microsecond_precision: bool,
    is_case_sensitive: bool,
    predicate: Option<PhysicalExprRef>,
}

impl FileOpener for OrcOpener {
    fn open(&self, file_meta: FileMeta, _file: PartitionedFile) -> Result<FileOpenFuture> {
        let reader = OrcFileReaderRef {
            inner: Arc::new(InternalFileReader::try_new(
                self.fs_provider.clone(),
                file_meta.object_meta.clone(),
            )?),
            metrics: OrcFileMetrics::new(
                self.partition_index,
                file_meta
                    .object_meta
                    .location
                    .filename()
                    .unwrap_or("__default_filename__"),
                &self.metrics.clone(),
            ),
        };
        let batch_size = self.batch_size;
        let projection = self.projection.clone();
        let projected_schema = SchemaRef::from(self.table_schema.project(&projection)?);
        let schema_adapter = SchemaAdapter::new(
            self.table_schema.clone(),
            projected_schema.clone(),
            self.force_positional_evolution,
        );
        let use_microsecond = self.use_microsecond_precision;
        let is_case = self.is_case_sensitive;
        let predicate = self.predicate.clone();

        Ok(Box::pin(async move {
            let mut builder = ArrowReaderBuilder::try_new_async(reader)
                .await
                .or_else(|err| df_execution_err!("create orc reader error: {err}"))?;
            if use_microsecond {
                builder = builder.with_timestamp_precision(TimestampPrecision::Microsecond);
            }
            if let Some(range) = file_meta.range.clone() {
                let range = range.start as usize..range.end as usize;
                builder = builder.with_file_byte_range(range);
            }

            let (schema_mapping, projection) =
                schema_adapter.map_schema(builder.file_metadata(), is_case)?;

            let projection_mask =
                ProjectionMask::roots(builder.file_metadata().root_data_type(), projection);
            builder = builder
                .with_batch_size(batch_size)
                .with_projection(projection_mask);
            if let Some(orc_predicate) = convert_predicate_to_orc(predicate, &projected_schema) {
                builder = builder.with_predicate(orc_predicate);
            }

            let stream = builder.build_async();

            let adapted = stream
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                .map(move |maybe_batch| {
                    maybe_batch.and_then(|b| schema_mapping.map_batch(b).map_err(Into::into))
                });

            Ok(adapted.boxed())
        }))
    }
}

#[derive(Clone)]
struct OrcFileReaderRef {
    inner: Arc<InternalFileReader>,
    metrics: OrcFileMetrics,
}

impl AsyncChunkReader for OrcFileReaderRef {
    fn len(&mut self) -> BoxFuture<'_, std::io::Result<u64>> {
        async move { Ok(self.inner.get_meta().size as u64) }.boxed()
    }

    fn get_bytes(
        &mut self,
        offset_from_start: u64,
        length: u64,
    ) -> BoxFuture<'_, std::io::Result<Bytes>> {
        let offset_from_start = offset_from_start;
        let length = length;
        let range = offset_from_start..(offset_from_start + length);
        self.metrics.bytes_scanned.add(length as usize);
        async move { self.inner.read_fully(range).map_err(|e| e.into()) }.boxed()
    }
}

struct SchemaAdapter {
    table_schema: SchemaRef,
    projected_schema: SchemaRef,
    force_positional_evolution: bool,
}

impl SchemaAdapter {
    pub fn new(
        table_schema: SchemaRef,
        projected_schema: SchemaRef,
        force_positional_evolution: bool,
    ) -> Self {
        Self {
            table_schema,
            projected_schema,
            force_positional_evolution,
        }
    }

    fn map_schema(
        &self,
        orc_file_meta: &FileMetadata,
        is_case_sensitive: bool,
    ) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(self.projected_schema.fields().len());
        let mut field_mappings = vec![None; self.projected_schema.fields().len()];

        let file_named_columns = orc_file_meta.root_data_type().children();
        if self.force_positional_evolution
            || file_named_columns
                .iter()
                .all(|named_col| named_col.name().starts_with("_col"))
        {
            let table_schema_fields = self.table_schema.fields();
            assert!(
                file_named_columns.len() <= table_schema_fields.len(),
                "The given table schema {:?} (length:{}) has fewer {} fields than \
                the actual ORC physical schema {:?} (length:{})",
                table_schema_fields,
                table_schema_fields.len(),
                file_named_columns.len() - table_schema_fields.len(),
                file_named_columns,
                file_named_columns.len()
            );
            for (proj_idx, project_field) in self.projected_schema.fields.iter().enumerate() {
                match self.table_schema.fields().find(project_field.name()) {
                    Some((tbl_idx, _)) => {
                        if let Some(named_column) = file_named_columns.get(tbl_idx) {
                            field_mappings[proj_idx] = Some(projection.len());
                            projection.push(named_column.data_type().column_index());
                        }
                    }
                    None => {
                        return df_execution_err!(
                            "Cannot find field {} in table schema {:?}",
                            project_field.name(),
                            table_schema_fields
                        );
                    }
                }
            }
        } else if is_case_sensitive {
            for named_column in file_named_columns {
                if let Some((proj_idx, _)) =
                    self.projected_schema.fields().find(named_column.name())
                {
                    field_mappings[proj_idx] = Some(projection.len());
                    projection.push(named_column.data_type().column_index());
                }
            }
        } else {
            for named_column in file_named_columns {
                // Case-insensitive field name matching
                let named_column_name_lower = named_column.name().to_lowercase();
                if let Some((proj_idx, _)) = self
                    .projected_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .find(|(_, f)| f.name().to_lowercase() == named_column_name_lower)
                {
                    field_mappings[proj_idx] = Some(projection.len());
                    projection.push(named_column.data_type().column_index());
                }
            }
        }

        Ok((
            create_auron_schema_mapper(&self.projected_schema, &field_mappings),
            projection,
        ))
    }
}

#[derive(Clone)]
struct OrcFileMetrics {
    bytes_scanned: Count,
}

impl OrcFileMetrics {
    pub fn new(partition: usize, filename: &str, metrics: &ExecutionPlanMetricsSet) -> Self {
        let bytes_scanned = MetricBuilder::new(metrics)
            .with_new_label("filename", filename.to_string())
            .counter("bytes_scanned", partition);
        Self { bytes_scanned }
    }
}

fn convert_predicate_to_orc(
    predicate: Option<PhysicalExprRef>,
    file_schema: &SchemaRef,
) -> Option<Predicate> {
    let predicate = predicate?;
    convert_expr_to_orc(&predicate, file_schema)
}

/// Recursively collect all AND sub-conditions and flatten nested AND
/// structures.
fn collect_and_predicates(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    schema: &SchemaRef,
    predicates: &mut Vec<Predicate>,
) {
    // Handle short-circuit AND expression (SCAndExpr)
    if let Some(sc_and) = expr.as_any().downcast_ref::<SCAndExpr>() {
        // Recursively collect AND sub-conditions from both sides
        collect_and_predicates(&sc_and.left, schema, predicates);
        collect_and_predicates(&sc_and.right, schema, predicates);
        return;
    }

    // Handle BinaryExpr with AND operator
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if matches!(binary.op(), Operator::And) {
            // Recursively collect AND sub-conditions from both sides
            collect_and_predicates(binary.left(), schema, predicates);
            collect_and_predicates(binary.right(), schema, predicates);
            return;
        }
    }

    // Not an AND expression, convert the whole expression
    // (could be OR, comparison, IS NULL, etc.)
    if let Some(pred) = convert_expr_to_orc(expr, schema) {
        predicates.push(pred);
    }
}

/// Recursively collect all OR sub-conditions and flatten nested OR
/// structures.
fn collect_or_predicates(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    schema: &SchemaRef,
    predicates: &mut Vec<Predicate>,
) {
    // Handle short-circuit OR expression (SCOrExpr)
    if let Some(sc_or) = expr.as_any().downcast_ref::<SCOrExpr>() {
        // Recursively collect OR sub-conditions from both sides
        collect_or_predicates(&sc_or.left, schema, predicates);
        collect_or_predicates(&sc_or.right, schema, predicates);
        return;
    }

    // Handle BinaryExpr with OR operator
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if matches!(binary.op(), Operator::Or) {
            // Recursively collect OR sub-conditions from both sides
            collect_or_predicates(binary.left(), schema, predicates);
            collect_or_predicates(binary.right(), schema, predicates);
            return;
        }
    }

    // Not an OR expression, convert the whole expression
    // (could be AND, comparison, IS NULL, etc.)
    if let Some(pred) = convert_expr_to_orc(expr, schema) {
        predicates.push(pred);
    }
}

fn convert_expr_to_orc(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    schema: &SchemaRef,
) -> Option<Predicate> {
    // Handle top-level short-circuit AND expression (SCAndExpr)
    if let Some(_sc_and) = expr.as_any().downcast_ref::<SCAndExpr>() {
        let mut predicates = Vec::new();
        collect_and_predicates(expr, schema, &mut predicates);

        if predicates.is_empty() {
            return None;
        }

        if predicates.len() == 1 {
            return Some(predicates.into_iter().next().unwrap());
        }

        return Some(Predicate::and(predicates));
    }

    // Handle top-level short-circuit OR expression (SCOrExpr)
    if let Some(_sc_or) = expr.as_any().downcast_ref::<SCOrExpr>() {
        let mut predicates = Vec::new();
        collect_or_predicates(expr, schema, &mut predicates);

        if predicates.is_empty() {
            return None;
        }

        if predicates.len() == 1 {
            return Some(predicates.into_iter().next().unwrap());
        }

        return Some(Predicate::or(predicates));
    }

    // Handle top-level AND expression (BinaryExpr with AND operator)
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if matches!(binary.op(), Operator::And) {
            let mut predicates = Vec::new();
            collect_and_predicates(expr, schema, &mut predicates);

            if predicates.is_empty() {
                return None;
            }

            if predicates.len() == 1 {
                return Some(predicates.into_iter().next().unwrap());
            }

            return Some(Predicate::and(predicates));
        }

        // Handle top-level OR expression (BinaryExpr with OR operator)
        if matches!(binary.op(), Operator::Or) {
            let mut predicates = Vec::new();
            collect_or_predicates(expr, schema, &mut predicates);

            if predicates.is_empty() {
                return None;
            }

            if predicates.len() == 1 {
                return Some(predicates.into_iter().next().unwrap());
            }

            return Some(Predicate::or(predicates));
        }
    }

    convert_expr_to_orc_internal(expr, schema)
}

/// Internal conversion function for non-AND/OR expressions.
fn convert_expr_to_orc_internal(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    schema: &SchemaRef,
) -> Option<Predicate> {
    // Handle Literal expressions (WHERE true, WHERE false, etc.)
    if let Some(lit) = expr.as_any().downcast_ref::<Literal>() {
        match lit.value() {
            ScalarValue::Boolean(Some(true)) => {
                // WHERE true - no filtering needed, return None to skip predicate
                return None;
            }
            ScalarValue::Boolean(Some(false)) => {
                // WHERE false - need to filter all data
                // Create an impossible condition using a schema column if available
                // Use: column IS NULL AND column IS NOT NULL (always false)
                if let Some(field) = schema.fields().first() {
                    let col_name = field.name().as_str();
                    return Some(Predicate::and(vec![
                        Predicate::is_null(col_name),
                        Predicate::not(Predicate::is_null(col_name)),
                    ]));
                }
                // Fallback: no columns in schema, can't create a predicate
                // using a synthetic column name to ensure all data is filtered.
                let col_name = "__orc_where_false_constant__";
                return Some(Predicate::and(vec![
                    Predicate::is_null(col_name),
                    Predicate::not(Predicate::is_null(col_name)),
                ]));
            }
            _ => {
                return None;
            }
        }
    }

    // Handle NOT expressions (WHERE NOT condition)
    if let Some(not_expr) = expr.as_any().downcast_ref::<NotExpr>() {
        if let Some(inner_pred) = convert_expr_to_orc(not_expr.arg(), schema) {
            return Some(Predicate::not(inner_pred));
        }
        return None;
    }

    // Handle IS NULL expressions
    if let Some(is_null) = expr.as_any().downcast_ref::<IsNullExpr>() {
        if let Some(col) = is_null.arg().as_any().downcast_ref::<Column>() {
            let col_name = col.name();
            return Some(Predicate::is_null(col_name));
        }
        return None;
    }

    // Handle IS NOT NULL expressions
    if let Some(is_not_null) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
        if let Some(col) = is_not_null.arg().as_any().downcast_ref::<Column>() {
            let col_name = col.name();
            return Some(Predicate::not(Predicate::is_null(col_name)));
        }
        return None;
    }

    // Handle IN expressions (WHERE col IN (val1, val2, ...))
    if let Some(in_list) = expr.as_any().downcast_ref::<InListExpr>() {
        if let Some(col) = in_list.expr().as_any().downcast_ref::<Column>() {
            let col_name = col.name();

            // Convert IN to multiple OR conditions: col = val1 OR col = val2 OR ...
            let mut predicates = Vec::new();
            for list_expr in in_list.list() {
                if let Some(lit) = list_expr.as_any().downcast_ref::<Literal>() {
                    if let Some(pred_value) = convert_scalar_value(lit.value()) {
                        predicates.push(Predicate::eq(col_name, pred_value));
                    }
                }
            }

            if predicates.is_empty() {
                return None;
            }

            // If negated is true, it represents NOT IN
            if in_list.negated() {
                return Some(Predicate::not(Predicate::or(predicates)));
            } else {
                return Some(Predicate::or(predicates));
            }
        }
        return None;
    }

    // Handle BinaryExpr (comparison operations)
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        let left = binary.left();
        let right = binary.right();
        let op = binary.op();

        // AND/OR are already handled at the outer level, skip here
        if matches!(op, Operator::And | Operator::Or) {
            return None;
        }

        if let Some(col) = left.as_any().downcast_ref::<Column>() {
            if let Some(lit) = right.as_any().downcast_ref::<Literal>() {
                let col_name = col.name();
                let value = lit.value();
                return build_comparison_predicate(col_name, op, value);
            }
        }

        if let Some(lit) = left.as_any().downcast_ref::<Literal>() {
            if let Some(col) = right.as_any().downcast_ref::<Column>() {
                let col_name = col.name();
                let value = lit.value();
                return build_comparison_predicate_reversed(col_name, op, value);
            }
        }
    }

    None
}

fn build_comparison_predicate(
    col_name: &str,
    op: &Operator,
    value: &ScalarValue,
) -> Option<Predicate> {
    let predicate_value = convert_scalar_value(value)?;

    match op {
        Operator::Eq => Some(Predicate::eq(col_name, predicate_value)),
        Operator::NotEq => Some(Predicate::ne(col_name, predicate_value)),
        Operator::Lt => Some(Predicate::lt(col_name, predicate_value)),
        Operator::LtEq => Some(Predicate::lte(col_name, predicate_value)),
        Operator::Gt => Some(Predicate::gt(col_name, predicate_value)),
        Operator::GtEq => Some(Predicate::gte(col_name, predicate_value)),
        _ => None,
    }
}

fn build_comparison_predicate_reversed(
    col_name: &str,
    op: &Operator,
    value: &ScalarValue,
) -> Option<Predicate> {
    let predicate_value = convert_scalar_value(value)?;

    match op {
        Operator::Eq => Some(Predicate::eq(col_name, predicate_value)),
        Operator::NotEq => Some(Predicate::ne(col_name, predicate_value)),
        Operator::Lt => Some(Predicate::gt(col_name, predicate_value)),
        Operator::LtEq => Some(Predicate::gte(col_name, predicate_value)),
        Operator::Gt => Some(Predicate::lt(col_name, predicate_value)),
        Operator::GtEq => Some(Predicate::lte(col_name, predicate_value)),
        _ => None,
    }
}

fn convert_scalar_value(value: &ScalarValue) -> Option<PredicateValue> {
    match value {
        ScalarValue::Boolean(v) => Some(PredicateValue::Boolean(*v)),
        ScalarValue::Int8(v) => Some(PredicateValue::Int8(*v)),
        ScalarValue::Int16(v) => Some(PredicateValue::Int16(*v)),
        ScalarValue::Int32(v) => Some(PredicateValue::Int32(*v)),
        ScalarValue::Int64(v) => Some(PredicateValue::Int64(*v)),
        ScalarValue::Float32(v) => Some(PredicateValue::Float32(*v)),
        ScalarValue::Float64(v) => Some(PredicateValue::Float64(*v)),
        ScalarValue::Utf8(v) => Some(PredicateValue::Utf8(v.clone())),
        ScalarValue::LargeUtf8(v) => Some(PredicateValue::Utf8(v.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        logical_expr::Operator,
        physical_expr::expressions::{
            BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal, NotExpr, SCAndExpr,
            SCOrExpr,
        },
        scalar::ScalarValue,
    };

    use super::*;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int32, true),
            Field::new("score", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_literal_true() {
        let schema = create_test_schema();
        let expr = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        // WHERE true should return None (no filtering)
        assert!(result.is_none());
    }

    #[test]
    fn test_literal_false() {
        let schema = create_test_schema();
        let expr = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        // WHERE false should return a predicate that filters all data
        assert!(result.is_some());
    }

    #[test]
    fn test_comparison_eq() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("id", 0));
        let lit = Arc::new(Literal::new(ScalarValue::Int32(Some(42))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        assert_eq!(
            format!("{:?}", predicate),
            "Comparison { column: \"id\", op: Equal, value: Int32(Some(42)) }"
        );
    }

    #[test]
    fn test_comparison_ne() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("name", 1));
        let lit = Arc::new(Literal::new(ScalarValue::Utf8(Some("test".to_string()))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::NotEq, lit));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        assert_eq!(
            format!("{:?}", predicate),
            "Comparison { column: \"name\", op: NotEqual, value: Utf8(Some(\"test\")) }"
        );
    }

    #[test]
    fn test_comparison_lt_gt_lte_gte() {
        let schema = create_test_schema();

        // Test LT
        let col = Arc::new(Column::new("age", 2));
        let lit = Arc::new(Literal::new(ScalarValue::Int32(Some(30))));
        let expr = Arc::new(BinaryExpr::new(col.clone(), Operator::Lt, lit.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());

        // Test GT
        let expr = Arc::new(BinaryExpr::new(col.clone(), Operator::Gt, lit.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());

        // Test LtEq
        let expr = Arc::new(BinaryExpr::new(col.clone(), Operator::LtEq, lit.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());

        // Test GtEq
        let expr = Arc::new(BinaryExpr::new(col, Operator::GtEq, lit));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
    }

    #[test]
    fn test_comparison_reversed() {
        let schema = create_test_schema();

        // Test all reversed comparison operators
        // Format: (operator, expected_debug_string_fragment)

        // Symmetric operators (Eq, NotEq) - order doesn't change semantics
        let lit = Arc::new(Literal::new(ScalarValue::Int32(Some(42))));
        let col = Arc::new(Column::new("id", 0));
        let expr = Arc::new(BinaryExpr::new(lit.clone(), Operator::Eq, col.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        // 42 = id → id = 42

        let expr = Arc::new(BinaryExpr::new(lit.clone(), Operator::NotEq, col.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        // 42 != id → id != 42

        // Asymmetric operators - must be reversed

        // Test: 42 < id → id > 42
        let expr = Arc::new(BinaryExpr::new(lit.clone(), Operator::Lt, col.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be GreaterThan, not LessThan
        assert!(
            debug_str.contains("GreaterThan") || debug_str.contains("Gt"),
            "Expected GreaterThan for reversed Lt, got: {}",
            debug_str
        );

        // Test: 42 <= id → id >= 42
        let expr = Arc::new(BinaryExpr::new(lit.clone(), Operator::LtEq, col.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        assert!(
            debug_str.contains("GreaterThanOrEqual")
                || debug_str.contains("GreaterThanEquals")
                || debug_str.contains("Gte"),
            "Expected GreaterThanOrEqual for reversed LtEq, got: {}",
            debug_str
        );

        // Test: 42 > id → id < 42
        let expr = Arc::new(BinaryExpr::new(lit.clone(), Operator::Gt, col.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        assert!(
            debug_str.contains("LessThan") || debug_str.contains("Lt"),
            "Expected LessThan for reversed Gt, got: {}",
            debug_str
        );

        // Test: 42 >= id → id <= 42
        let expr = Arc::new(BinaryExpr::new(lit, Operator::GtEq, col));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        assert!(
            debug_str.contains("LessThanOrEqual")
                || debug_str.contains("LessThanEquals")
                || debug_str.contains("Lte"),
            "Expected LessThanOrEqual for reversed GtEq, got: {}",
            debug_str
        );
    }

    #[test]
    fn test_is_null() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("name", 1));
        let expr = Arc::new(IsNullExpr::new(col));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        assert_eq!(format!("{:?}", predicate), "IsNull { column: \"name\" }");
    }

    #[test]
    fn test_is_not_null() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("age", 2));
        let expr = Arc::new(IsNotNullExpr::new(col));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        assert_eq!(
            format!("{:?}", predicate),
            "Not(IsNull { column: \"age\" })"
        );
    }

    #[test]
    fn test_not_expr() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("id", 0));
        let lit = Arc::new(Literal::new(ScalarValue::Int32(Some(42))));
        let eq_expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        let not_expr = Arc::new(NotExpr::new(eq_expr));

        let result = convert_predicate_to_orc(Some(not_expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        assert!(format!("{:?}", predicate).starts_with("Not(Comparison"));
    }

    #[test]
    fn test_in_list() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("id", 0));
        let values = vec![
            Arc::new(Literal::new(ScalarValue::Int32(Some(1))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Int32(Some(2))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ];
        let expr = Arc::new(InListExpr::new(col, values, false, None));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        // IN list should be converted to OR of equality predicates
        assert!(format!("{:?}", predicate).starts_with("Or(["));
    }

    #[test]
    fn test_not_in_list() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("name", 1));
        let values = vec![
            Arc::new(Literal::new(ScalarValue::Utf8(Some("foo".to_string()))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("bar".to_string()))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ];
        let expr = Arc::new(InListExpr::new(col, values, true, None));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        // NOT IN should be converted to NOT(OR(...))
        assert!(format!("{:?}", predicate).starts_with("Not(Or(["));
    }

    #[test]
    fn test_and_simple() {
        let schema = create_test_schema();
        // id = 42 AND age > 18
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(42))));
        let expr1 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit1));

        let col2 = Arc::new(Column::new("age", 2));
        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(18))));
        let expr2 = Arc::new(BinaryExpr::new(col2, Operator::Gt, lit2));

        let and_expr = Arc::new(BinaryExpr::new(expr1, Operator::And, expr2));

        let result = convert_predicate_to_orc(Some(and_expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        assert!(format!("{:?}", predicate).starts_with("And(["));
    }

    #[test]
    fn test_and_nested_flattening() {
        let schema = create_test_schema();
        // ((id = 1 AND age = 2) AND name = "foo") should be flattened to And([...])
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let expr1 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit1));

        let col2 = Arc::new(Column::new("age", 2));
        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(2))));
        let expr2 = Arc::new(BinaryExpr::new(col2, Operator::Eq, lit2));

        let and1 = Arc::new(BinaryExpr::new(expr1, Operator::And, expr2));

        let col3 = Arc::new(Column::new("name", 1));
        let lit3 = Arc::new(Literal::new(ScalarValue::Utf8(Some("foo".to_string()))));
        let expr3 = Arc::new(BinaryExpr::new(col3, Operator::Eq, lit3));

        let and2 = Arc::new(BinaryExpr::new(and1, Operator::And, expr3));

        let result = convert_predicate_to_orc(Some(and2), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be flattened to And([cond1, cond2, cond3])
        assert!(debug_str.starts_with("And(["));
        // Count the number of conditions (should be 3)
        let condition_count = debug_str.matches("Comparison").count();
        assert_eq!(condition_count, 3);
    }

    #[test]
    fn test_or_simple() {
        let schema = create_test_schema();
        // id = 1 OR id = 2
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let expr1 = Arc::new(BinaryExpr::new(col1.clone(), Operator::Eq, lit1));

        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(2))));
        let expr2 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit2));

        let or_expr = Arc::new(BinaryExpr::new(expr1, Operator::Or, expr2));

        let result = convert_predicate_to_orc(Some(or_expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        assert!(format!("{:?}", predicate).starts_with("Or(["));
    }

    #[test]
    fn test_or_nested_flattening() {
        let schema = create_test_schema();
        // ((id = 1 OR age = 2) OR score = 3.0) should be flattened
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let expr1 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit1));

        let col2 = Arc::new(Column::new("age", 2));
        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(2))));
        let expr2 = Arc::new(BinaryExpr::new(col2, Operator::Eq, lit2));

        let or1 = Arc::new(BinaryExpr::new(expr1, Operator::Or, expr2));

        let col3 = Arc::new(Column::new("score", 3));
        let lit3 = Arc::new(Literal::new(ScalarValue::Float64(Some(3.0))));
        let expr3 = Arc::new(BinaryExpr::new(col3, Operator::Eq, lit3));

        let or2 = Arc::new(BinaryExpr::new(or1, Operator::Or, expr3));

        let result = convert_predicate_to_orc(Some(or2), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be flattened to Or([cond1, cond2, cond3])
        assert!(debug_str.starts_with("Or(["));
        let condition_count = debug_str.matches("Comparison").count();
        assert_eq!(condition_count, 3);
    }

    #[test]
    fn test_complex_mixed_predicates() {
        let schema = create_test_schema();
        // (id = 1 OR id = 2) AND name IS NOT NULL
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let expr1 = Arc::new(BinaryExpr::new(col1.clone(), Operator::Eq, lit1));

        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(2))));
        let expr2 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit2));

        let or_expr = Arc::new(BinaryExpr::new(expr1, Operator::Or, expr2));

        let col2 = Arc::new(Column::new("name", 1));
        let is_not_null = Arc::new(IsNotNullExpr::new(col2));

        let and_expr = Arc::new(BinaryExpr::new(or_expr, Operator::And, is_not_null));

        let result = convert_predicate_to_orc(Some(and_expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should have And at top level
        assert!(
            debug_str.contains("And"),
            "Expected And, got: {}",
            debug_str
        );
        // Should contain OR for the id conditions
        assert!(debug_str.contains("Or"), "Expected Or, got: {}", debug_str);
        // Should contain the IS NOT NULL condition
        assert!(
            debug_str.contains("IsNull"),
            "Expected IsNull, got: {}",
            debug_str
        );
    }

    #[test]
    fn test_deeply_nested_and() {
        let schema = create_test_schema();
        // Build: (((id = 1 AND age = 2) AND name = "foo") AND score = 3.0)
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let expr1 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit1));

        let col2 = Arc::new(Column::new("age", 2));
        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(2))));
        let expr2 = Arc::new(BinaryExpr::new(col2, Operator::Eq, lit2));

        let and1 = Arc::new(BinaryExpr::new(expr1, Operator::And, expr2));

        let col3 = Arc::new(Column::new("name", 1));
        let lit3 = Arc::new(Literal::new(ScalarValue::Utf8(Some("foo".to_string()))));
        let expr3 = Arc::new(BinaryExpr::new(col3, Operator::Eq, lit3));

        let and2 = Arc::new(BinaryExpr::new(and1, Operator::And, expr3));

        let col4 = Arc::new(Column::new("score", 3));
        let lit4 = Arc::new(Literal::new(ScalarValue::Float64(Some(3.0))));
        let expr4 = Arc::new(BinaryExpr::new(col4, Operator::Eq, lit4));

        let and3 = Arc::new(BinaryExpr::new(and2, Operator::And, expr4));

        let result = convert_predicate_to_orc(Some(and3), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be flattened to And([cond1, cond2, cond3, cond4])
        assert!(debug_str.starts_with("And(["));
        let condition_count = debug_str.matches("Comparison").count();
        assert_eq!(condition_count, 4);
    }

    #[test]
    fn test_all_scalar_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_bool", DataType::Boolean, true),
            Field::new("col_i8", DataType::Int8, true),
            Field::new("col_i16", DataType::Int16, true),
            Field::new("col_i32", DataType::Int32, true),
            Field::new("col_i64", DataType::Int64, true),
            Field::new("col_f32", DataType::Float32, true),
            Field::new("col_f64", DataType::Float64, true),
            Field::new("col_utf8", DataType::Utf8, true),
        ]));

        // Test Boolean
        let col = Arc::new(Column::new("col_bool", 0));
        let lit = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        assert!(convert_predicate_to_orc(Some(expr), &schema).is_some());

        // Test Int8
        let col = Arc::new(Column::new("col_i8", 1));
        let lit = Arc::new(Literal::new(ScalarValue::Int8(Some(42))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        assert!(convert_predicate_to_orc(Some(expr), &schema).is_some());

        // Test Int16
        let col = Arc::new(Column::new("col_i16", 2));
        let lit = Arc::new(Literal::new(ScalarValue::Int16(Some(1000))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        assert!(convert_predicate_to_orc(Some(expr), &schema).is_some());

        // Test Int32
        let col = Arc::new(Column::new("col_i32", 3));
        let lit = Arc::new(Literal::new(ScalarValue::Int32(Some(100000))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        assert!(convert_predicate_to_orc(Some(expr), &schema).is_some());

        // Test Int64
        let col = Arc::new(Column::new("col_i64", 4));
        let lit = Arc::new(Literal::new(ScalarValue::Int64(Some(1000000000))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        assert!(convert_predicate_to_orc(Some(expr), &schema).is_some());

        // Test Float32
        let col = Arc::new(Column::new("col_f32", 5));
        let lit = Arc::new(Literal::new(ScalarValue::Float32(Some(3.14))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        assert!(convert_predicate_to_orc(Some(expr), &schema).is_some());

        // Test Float64
        let col = Arc::new(Column::new("col_f64", 6));
        let lit = Arc::new(Literal::new(ScalarValue::Float64(Some(2.718))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        assert!(convert_predicate_to_orc(Some(expr), &schema).is_some());

        // Test Utf8
        let col = Arc::new(Column::new("col_utf8", 7));
        let lit = Arc::new(Literal::new(ScalarValue::Utf8(Some("test".to_string()))));
        let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, lit));
        assert!(convert_predicate_to_orc(Some(expr), &schema).is_some());
    }

    #[test]
    fn test_null_literal_in_comparison() {
        let schema = create_test_schema();

        // Test: WHERE id = NULL
        // Note: In SQL semantics, "col = NULL" always evaluates to NULL (not true or
        // false) However, we should still handle it gracefully
        let col = Arc::new(Column::new("id", 0));
        let null_lit = Arc::new(Literal::new(ScalarValue::Int32(None)));
        let expr = Arc::new(BinaryExpr::new(col.clone(), Operator::Eq, null_lit.clone()));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        // Should convert to an ORC predicate (even though semantically it won't match
        // anything)
        assert!(result.is_some());

        // Test: WHERE id != NULL
        let expr = Arc::new(BinaryExpr::new(
            col.clone(),
            Operator::NotEq,
            null_lit.clone(),
        ));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());

        // Test: WHERE id < NULL
        let expr = Arc::new(BinaryExpr::new(col.clone(), Operator::Lt, null_lit.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());

        // Test: WHERE id > NULL
        let expr = Arc::new(BinaryExpr::new(col.clone(), Operator::Gt, null_lit.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());

        // Test: WHERE NULL = id (reversed)
        let expr = Arc::new(BinaryExpr::new(null_lit.clone(), Operator::Eq, col.clone()));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());

        // Test: WHERE NULL < id (reversed)
        let expr = Arc::new(BinaryExpr::new(null_lit, Operator::Lt, col));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
    }

    #[test]
    fn test_null_in_in_list() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("id", 0));

        // Test: WHERE id IN (1, NULL, 3)
        // This should handle NULL gracefully
        let values = vec![
            Arc::new(Literal::new(ScalarValue::Int32(Some(1))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Int32(None)))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Int32(Some(3))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ];
        let expr = Arc::new(InListExpr::new(col.clone(), values, false, None));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be converted to OR of equality predicates
        // NULL values should be included (even though they won't match)
        assert!(debug_str.starts_with("Or(["));
    }

    #[test]
    fn test_null_in_not_in_list() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("name", 1));

        // Test: WHERE name NOT IN ('foo', NULL, 'bar')
        let values = vec![
            Arc::new(Literal::new(ScalarValue::Utf8(Some("foo".to_string()))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Utf8(None)))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("bar".to_string()))))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ];
        let expr = Arc::new(InListExpr::new(col, values, true, None));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be NOT(OR(...))
        assert!(debug_str.starts_with("Not(Or(["));
    }

    #[test]
    fn test_all_null_in_list() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("id", 0));

        // Test: WHERE id IN (NULL, NULL, NULL)
        // Edge case: all values are NULL
        let values = vec![
            Arc::new(Literal::new(ScalarValue::Int32(None)))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Int32(None)))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
            Arc::new(Literal::new(ScalarValue::Int32(None)))
                as Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ];
        let expr = Arc::new(InListExpr::new(col, values, false, None));

        let result = convert_predicate_to_orc(Some(expr), &schema);
        // Should still produce a predicate (though it won't match anything)
        assert!(result.is_some());
    }

    #[test]
    fn test_null_with_and_predicate() {
        let schema = create_test_schema();

        // Test: WHERE id = NULL AND age > 18
        let col1 = Arc::new(Column::new("id", 0));
        let null_lit = Arc::new(Literal::new(ScalarValue::Int32(None)));
        let expr1 = Arc::new(BinaryExpr::new(col1, Operator::Eq, null_lit));

        let col2 = Arc::new(Column::new("age", 2));
        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(18))));
        let expr2 = Arc::new(BinaryExpr::new(col2, Operator::Gt, lit2));

        let and_expr = Arc::new(BinaryExpr::new(expr1, Operator::And, expr2));

        let result = convert_predicate_to_orc(Some(and_expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be flattened to And([...])
        assert!(debug_str.contains("And"));
    }

    #[test]
    fn test_null_with_or_predicate() {
        let schema = create_test_schema();

        // Test: WHERE id = NULL OR age > 18
        let col1 = Arc::new(Column::new("id", 0));
        let null_lit = Arc::new(Literal::new(ScalarValue::Int32(None)));
        let expr1 = Arc::new(BinaryExpr::new(col1, Operator::Eq, null_lit));

        let col2 = Arc::new(Column::new("age", 2));
        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(18))));
        let expr2 = Arc::new(BinaryExpr::new(col2, Operator::Gt, lit2));

        let or_expr = Arc::new(BinaryExpr::new(expr1, Operator::Or, expr2));

        let result = convert_predicate_to_orc(Some(or_expr), &schema);
        assert!(result.is_some());
        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be flattened to Or([...])
        assert!(debug_str.contains("Or"));
    }

    #[test]
    fn test_various_null_types() {
        let schema = create_test_schema();

        // Test NULL with different data types
        let test_cases = vec![
            ("id", ScalarValue::Int32(None)),
            ("name", ScalarValue::Utf8(None)),
            ("age", ScalarValue::Int32(None)),
            ("score", ScalarValue::Float64(None)),
        ];

        for (col_name, null_value) in test_cases {
            let col = Arc::new(Column::new(col_name, schema.index_of(col_name).unwrap()));
            let null_lit = Arc::new(Literal::new(null_value));
            let expr = Arc::new(BinaryExpr::new(col, Operator::Eq, null_lit));

            let result = convert_predicate_to_orc(Some(expr), &schema);
            assert!(
                result.is_some(),
                "Failed to convert NULL comparison for column: {}",
                col_name
            );
        }
    }

    #[test]
    fn test_null_literal_edge_cases() {
        let schema = create_test_schema();
        let col = Arc::new(Column::new("id", 0));

        // Test: WHERE id >= NULL
        let null_lit = Arc::new(Literal::new(ScalarValue::Int32(None)));
        let expr = Arc::new(BinaryExpr::new(
            col.clone(),
            Operator::GtEq,
            null_lit.clone(),
        ));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());

        // Test: WHERE id <= NULL
        let expr = Arc::new(BinaryExpr::new(col, Operator::LtEq, null_lit));
        let result = convert_predicate_to_orc(Some(expr), &schema);
        assert!(result.is_some());
    }

    #[test]
    fn test_where_false_with_empty_schema() {
        // Edge case: WHERE false with an empty schema (no columns)
        let empty_schema = Arc::new(Schema::empty());
        let expr = Arc::new(Literal::new(ScalarValue::Boolean(Some(false))));

        let result = convert_predicate_to_orc(Some(expr), &empty_schema);

        // Should still produce a predicate using a synthetic column name
        // to ensure all data is filtered
        assert!(
            result.is_some(),
            "WHERE false should produce a predicate even with empty schema"
        );

        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);

        // Should use the synthetic column name
        assert!(
            debug_str.contains("__orc_where_false_constant__") || debug_str.contains("And"),
            "Expected synthetic column or And predicate, got: {}",
            debug_str
        );
    }

    #[test]
    fn test_where_true_with_empty_schema() {
        // Edge case: WHERE true with an empty schema
        let empty_schema = Arc::new(Schema::empty());
        let expr = Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));

        let result = convert_predicate_to_orc(Some(expr), &empty_schema);

        // WHERE true should always return None (no filtering)
        assert!(
            result.is_none(),
            "WHERE true should not produce any predicate"
        );
    }

    #[test]
    fn test_short_circuit_and_expr() {
        let schema = create_test_schema();

        // Test: SCAndExpr (short-circuit AND)
        // Simulating: WHERE id = 42 AND age > 18
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(42))));
        let expr1 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit1));

        let col2 = Arc::new(Column::new("age", 2));
        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(18))));
        let expr2 = Arc::new(BinaryExpr::new(col2, Operator::Gt, lit2));

        let sc_and_expr = Arc::new(SCAndExpr::new(expr1, expr2));

        let result = convert_predicate_to_orc(Some(sc_and_expr), &schema);
        assert!(result.is_some(), "SCAndExpr should convert to predicate");

        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be flattened to And([...])
        assert!(
            debug_str.contains("And"),
            "Expected And predicate, got: {}",
            debug_str
        );
    }

    #[test]
    fn test_short_circuit_or_expr() {
        let schema = create_test_schema();

        // Test: SCOrExpr (short-circuit OR)
        // Simulating: WHERE id = 1 OR id = 2
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let expr1 = Arc::new(BinaryExpr::new(col1.clone(), Operator::Eq, lit1));

        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(2))));
        let expr2 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit2));

        let sc_or_expr = Arc::new(SCOrExpr::new(expr1, expr2));

        let result = convert_predicate_to_orc(Some(sc_or_expr), &schema);
        assert!(result.is_some(), "SCOrExpr should convert to predicate");

        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should be flattened to Or([...])
        assert!(
            debug_str.contains("Or"),
            "Expected Or predicate, got: {}",
            debug_str
        );
    }

    #[test]
    fn test_nested_short_circuit_exprs() {
        let schema = create_test_schema();

        // Test: Nested SCAndExpr and SCOrExpr
        // Simulating: WHERE (id = 1 OR id = 2) AND age > 18
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
        let expr1 = Arc::new(BinaryExpr::new(col1.clone(), Operator::Eq, lit1));

        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(2))));
        let expr2 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit2));

        let sc_or_expr = Arc::new(SCOrExpr::new(expr1, expr2));

        let col2 = Arc::new(Column::new("age", 2));
        let lit3 = Arc::new(Literal::new(ScalarValue::Int32(Some(18))));
        let expr3 = Arc::new(BinaryExpr::new(col2, Operator::Gt, lit3));

        let sc_and_expr = Arc::new(SCAndExpr::new(sc_or_expr, expr3));

        let result = convert_predicate_to_orc(Some(sc_and_expr), &schema);
        assert!(
            result.is_some(),
            "Nested SCAndExpr/SCOrExpr should convert to predicate"
        );

        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should have And at top level with Or inside
        assert!(
            debug_str.contains("And"),
            "Expected And at top level, got: {}",
            debug_str
        );
        assert!(
            debug_str.contains("Or"),
            "Expected Or inside And, got: {}",
            debug_str
        );
    }

    #[test]
    fn test_mixed_binary_and_short_circuit() {
        let schema = create_test_schema();

        // Test: Mix of BinaryExpr (AND) and SCAndExpr
        // Simulating: WHERE id = 42 AND (age > 18 AND status = 'ACTIVE')
        let col1 = Arc::new(Column::new("id", 0));
        let lit1 = Arc::new(Literal::new(ScalarValue::Int32(Some(42))));
        let expr1 = Arc::new(BinaryExpr::new(col1, Operator::Eq, lit1));

        let col2 = Arc::new(Column::new("age", 2));
        let lit2 = Arc::new(Literal::new(ScalarValue::Int32(Some(18))));
        let expr2 = Arc::new(BinaryExpr::new(col2, Operator::Gt, lit2));

        let col3 = Arc::new(Column::new("name", 1));
        let lit3 = Arc::new(Literal::new(ScalarValue::Utf8(Some("ACTIVE".to_string()))));
        let expr3 = Arc::new(BinaryExpr::new(col3, Operator::Eq, lit3));

        let sc_and_inner = Arc::new(SCAndExpr::new(expr2, expr3));
        let binary_and_outer = Arc::new(BinaryExpr::new(expr1, Operator::And, sc_and_inner));

        let result = convert_predicate_to_orc(Some(binary_and_outer), &schema);
        assert!(
            result.is_some(),
            "Mixed BinaryExpr/SCAndExpr should convert to predicate"
        );

        let predicate = result.unwrap();
        let debug_str = format!("{:?}", predicate);
        // Should all be flattened to And([...])
        assert!(
            debug_str.contains("And"),
            "Expected And predicate, got: {}",
            debug_str
        );
        // Should have 3 conditions flattened
        let condition_count = debug_str.matches("Comparison").count();
        assert_eq!(
            condition_count, 3,
            "Expected 3 comparison conditions, got: {}",
            condition_count
        );
    }
}
