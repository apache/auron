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
        expressions::{BinaryExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal, NotExpr},
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
            log::error!("duanhao--->rust predicate: {:?}", predicate);
            if let Some(orc_predicate) = convert_predicate_to_orc(predicate, &projected_schema) {
                log::error!("duanhao--->orc predicate: {:?}", orc_predicate.clone());
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

/// Recursively collect all AND sub-conditions and flatten nested AND structures.
fn collect_and_predicates(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    schema: &SchemaRef,
    predicates: &mut Vec<Predicate>,
) {
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if matches!(binary.op(), Operator::And) {
            // Recursively collect AND sub-conditions from both sides
            collect_and_predicates(binary.left(), schema, predicates);
            collect_and_predicates(binary.right(), schema, predicates);
            return;
        }
    }

    // Not an AND expression, try to convert to ORC predicate
    if let Some(pred) = convert_expr_to_orc_internal(expr, schema) {
        predicates.push(pred);
    }
}

/// Recursively collect all OR sub-conditions and flatten nested OR structures.
fn collect_or_predicates(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    schema: &SchemaRef,
    predicates: &mut Vec<Predicate>,
) {
    if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if matches!(binary.op(), Operator::Or) {
            // Recursively collect OR sub-conditions from both sides
            collect_or_predicates(binary.left(), schema, predicates);
            collect_or_predicates(binary.right(), schema, predicates);
            return;
        }
    }

    // Not an OR expression, try to convert to ORC predicate
    if let Some(pred) = convert_expr_to_orc_internal(expr, schema) {
        predicates.push(pred);
    }
}

fn convert_expr_to_orc(
    expr: &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
    schema: &SchemaRef,
) -> Option<Predicate> {
    // Handle top-level AND expression, flatten all AND conditions
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

        // Handle top-level OR expression, flatten all OR conditions
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
                // ORC doesn't have a direct "false" predicate, return None
                return None;
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
