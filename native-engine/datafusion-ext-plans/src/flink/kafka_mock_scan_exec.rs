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

use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int8Builder, Int16Builder,
    Int32Builder, Int64Builder, LargeStringBuilder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder, TimestampMillisecondBuilder, UInt8Builder, UInt16Builder,
    UInt32Builder, UInt64Builder,
};
use arrow_schema::{DataType, Field, SchemaRef, TimeUnit};
use datafusion::{
    common::{DataFusionError, Statistics},
    error::Result,
    execution::TaskContext,
    physical_expr::{EquivalenceProperties, Partitioning::UnknownPartitioning},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
        execution_plan::{Boundedness, EmissionType},
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
    },
};
use once_cell::sync::OnceCell;
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use crate::common::execution_context::ExecutionContext;

#[derive(Debug, Clone)]
pub struct KafkaMockScanExec {
    schema: SchemaRef,
    auron_operator_id: String,
    mock_data_json_array: String,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl KafkaMockScanExec {
    pub fn new(schema: SchemaRef, auron_operator_id: String, mock_data_json_array: String) -> Self {
        Self {
            schema,
            auron_operator_id,
            mock_data_json_array,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }

    fn execute_with_ctx(
        &self,
        exec_ctx: Arc<ExecutionContext>,
    ) -> Result<SendableRecordBatchStream> {
        let deserialized_pb_stream = mock_records(
            exec_ctx.output_schema(),
            exec_ctx.clone(),
            self.mock_data_json_array.clone(),
        )?;
        Ok(deserialized_pb_stream)
    }
}

impl DisplayAs for KafkaMockScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "KafkaMockScanExec")
    }
}

impl ExecutionPlan for KafkaMockScanExec {
    fn name(&self) -> &str {
        "KafkaMockScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        self.props.get_or_init(|| {
            PlanProperties::new(
                EquivalenceProperties::new(self.schema()),
                UnknownPartitioning(1),
                EmissionType::Both,
                Boundedness::Unbounded {
                    requires_infinite_memory: false,
                },
            )
        })
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            self.schema.clone(),
            self.auron_operator_id.clone(),
            self.mock_data_json_array.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let exec_ctx = ExecutionContext::new(context, partition, self.schema(), &self.metrics);
        self.execute_with_ctx(exec_ctx)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        todo!()
    }
}

fn mock_records(
    schema: SchemaRef,
    exec_ctx: Arc<ExecutionContext>,
    mock_data_json_array: String,
) -> Result<SendableRecordBatchStream> {
    let json_value: sonic_rs::Value = sonic_rs::from_str(&mock_data_json_array).map_err(|e| {
        DataFusionError::Execution(format!("mock_data_json_array is not valid JSON: {e}"))
    })?;
    let rows = json_value.as_array().ok_or_else(|| {
        DataFusionError::Execution("mock_data_json_array must be a JSON array".to_string())
    })?;

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let column = build_array_from_json(field, rows)?;
        columns.push(column);
    }

    let batch = RecordBatch::try_new(schema.clone(), columns)?;

    Ok(
        exec_ctx.output_with_sender("KafkaMockScanExec.MockRecords", move |sender| async move {
            sender.send(batch).await;
            Ok(())
        }),
    )
}

fn build_array_from_json(field: &Field, rows: &sonic_rs::Array) -> Result<ArrayRef> {
    let field_name = field.name();
    let nullable = field.is_nullable();

    macro_rules! build_typed_array {
        ($builder_ty:ident, $extract:expr) => {{
            let mut builder = $builder_ty::new();
            for row in rows.iter() {
                let val = row.get(field_name);
                match val {
                    Some(v) if !v.is_null() => {
                        let extracted = ($extract)(v).ok_or_else(|| {
                            DataFusionError::Execution(format!(
                                "Field '{}' type mismatch, expected {}",
                                field_name,
                                field.data_type()
                            ))
                        })?;
                        builder.append_value(extracted);
                    }
                    _ => {
                        if nullable {
                            builder.append_null();
                        } else {
                            return Err(DataFusionError::Execution(format!(
                                "Field '{}' is non-nullable but got null/missing value",
                                field_name
                            )));
                        }
                    }
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }};
    }

    match field.data_type() {
        DataType::Boolean => {
            build_typed_array!(BooleanBuilder, |v: &sonic_rs::Value| v.as_bool())
        }
        DataType::Int8 => {
            build_typed_array!(Int8Builder, |v: &sonic_rs::Value| v
                .as_i64()
                .map(|n| n as i8))
        }
        DataType::Int16 => {
            build_typed_array!(Int16Builder, |v: &sonic_rs::Value| v
                .as_i64()
                .map(|n| n as i16))
        }
        DataType::Int32 => {
            build_typed_array!(Int32Builder, |v: &sonic_rs::Value| v
                .as_i64()
                .map(|n| n as i32))
        }
        DataType::Int64 => {
            build_typed_array!(Int64Builder, |v: &sonic_rs::Value| v.as_i64())
        }
        DataType::UInt8 => {
            build_typed_array!(UInt8Builder, |v: &sonic_rs::Value| v
                .as_u64()
                .map(|n| n as u8))
        }
        DataType::UInt16 => {
            build_typed_array!(UInt16Builder, |v: &sonic_rs::Value| v
                .as_u64()
                .map(|n| n as u16))
        }
        DataType::UInt32 => {
            build_typed_array!(UInt32Builder, |v: &sonic_rs::Value| v
                .as_u64()
                .map(|n| n as u32))
        }
        DataType::UInt64 => {
            build_typed_array!(UInt64Builder, |v: &sonic_rs::Value| v.as_u64())
        }
        DataType::Float32 => {
            build_typed_array!(Float32Builder, |v: &sonic_rs::Value| v
                .as_f64()
                .map(|n| n as f32))
        }
        DataType::Float64 => {
            build_typed_array!(Float64Builder, |v: &sonic_rs::Value| v.as_f64())
        }
        DataType::Utf8 => {
            build_typed_array!(StringBuilder, |v: &sonic_rs::Value| v
                .as_str()
                .map(|s| s.to_string()))
        }
        DataType::LargeUtf8 => {
            build_typed_array!(LargeStringBuilder, |v: &sonic_rs::Value| v
                .as_str()
                .map(|s| s.to_string()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            build_typed_array!(TimestampMillisecondBuilder, |v: &sonic_rs::Value| v
                .as_i64())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            build_typed_array!(TimestampMicrosecondBuilder, |v: &sonic_rs::Value| v
                .as_i64())
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "Unsupported data type for mock JSON: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::*;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};

    use super::*;

    #[test]
    fn test_build_array_from_json_basic_types() {
        let json_str = r#"[
            {"name": "Alice", "age": 30, "score": 95.5, "is_active": true, "ts": 1700000000000},
            {"name": "Bob", "age": 25, "score": 88.0, "is_active": false, "ts": 1700000001000}
        ]"#;
        let json_value: sonic_rs::Value =
            sonic_rs::from_str(json_str).expect("Failed to parse JSON");
        let rows = json_value
            .as_array()
            .expect("Failed to get array from JSON");

        // Utf8
        let field = Field::new("name", DataType::Utf8, false);
        let array = build_array_from_json(&field, rows).expect("Failed to build array from JSON");
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray");
        assert_eq!(string_array.value(0), "Alice");
        assert_eq!(string_array.value(1), "Bob");

        // Int32
        let field = Field::new("age", DataType::Int32, false);
        let array = build_array_from_json(&field, rows).expect("Failed to build array from JSON");
        let int_array = array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");
        assert_eq!(int_array.value(0), 30);
        assert_eq!(int_array.value(1), 25);

        // Float64
        let field = Field::new("score", DataType::Float64, false);
        let array = build_array_from_json(&field, rows).expect("Failed to build array from JSON");
        let float_array = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Failed to downcast to Float64Array");
        assert!((float_array.value(0) - 95.5).abs() < f64::EPSILON);
        assert!((float_array.value(1) - 88.0).abs() < f64::EPSILON);

        // Boolean
        let field = Field::new("is_active", DataType::Boolean, false);
        let array = build_array_from_json(&field, rows).expect("Failed to build array from JSON");
        let bool_array = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("Failed to downcast to BooleanArray");
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));

        // Timestamp(Millisecond)
        let field = Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        );
        let array = build_array_from_json(&field, rows).expect("Failed to build array from JSON");
        let ts_array = array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Failed to downcast to TimestampMillisecondArray");
        assert_eq!(ts_array.value(0), 1700000000000);
        assert_eq!(ts_array.value(1), 1700000001000);
    }

    #[test]
    fn test_build_array_from_json_nullable() {
        let json_str = r#"[
            {"value": 100},
            {"value": null},
            {}
        ]"#;
        let json_value: sonic_rs::Value =
            sonic_rs::from_str(json_str).expect("Failed to parse JSON");
        let rows = json_value
            .as_array()
            .expect("Failed to get array from JSON");

        let field = Field::new("value", DataType::Int64, true);
        let array = build_array_from_json(&field, rows).expect("Failed to build array from JSON");
        let int_array = array
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast to Int64Array");
        assert_eq!(int_array.value(0), 100);
        assert!(int_array.is_null(1));
        assert!(int_array.is_null(2));
    }

    #[test]
    fn test_build_array_from_json_non_nullable_null_value_errors() {
        let json_str = r#"[{"value": null}]"#;
        let json_value: sonic_rs::Value =
            sonic_rs::from_str(json_str).expect("Failed to parse JSON");
        let rows = json_value
            .as_array()
            .expect("Failed to get array from JSON");

        let field = Field::new("value", DataType::Int32, false);
        let result = build_array_from_json(&field, rows);
        assert!(result.is_err());
    }

    #[test]
    fn test_build_array_from_json_empty_array() {
        let json_str = r#"[]"#;
        let json_value: sonic_rs::Value =
            sonic_rs::from_str(json_str).expect("Failed to parse JSON");
        let rows = json_value
            .as_array()
            .expect("Failed to get array from JSON");

        let field = Field::new("name", DataType::Utf8, false);
        let array = build_array_from_json(&field, rows).expect("Failed to build array from JSON");
        assert_eq!(array.len(), 0);
    }

    #[test]
    fn test_build_record_batch_from_mock_json() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int32, true),
            Field::new("_kafka_partition", DataType::Int32, false),
            Field::new("_kafka_offset", DataType::Int64, false),
            Field::new("_kafka_timestamp", DataType::Int64, false),
        ]));

        let mock_json = r#"[
            {"name": "Alice", "age": 30, "_kafka_partition": 0, "_kafka_offset": 0, "_kafka_timestamp": 1700000000000},
            {"name": "Bob", "age": null, "_kafka_partition": 0, "_kafka_offset": 1, "_kafka_timestamp": 1700000001000}
        ]"#;

        let json_value: sonic_rs::Value =
            sonic_rs::from_str(mock_json).expect("Failed to parse JSON");
        let rows = json_value
            .as_array()
            .expect("Failed to get array from JSON");

        let mut columns: Vec<ArrayRef> = Vec::new();
        for field in schema.fields() {
            columns
                .push(build_array_from_json(field, rows).expect("Failed to build array from JSON"));
        }

        let batch =
            RecordBatch::try_new(schema.clone(), columns).expect("Failed to create record batch");

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 5);

        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Failed to downcast to StringArray");
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");

        let age_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");
        assert_eq!(age_col.value(0), 30);
        assert!(age_col.is_null(1));

        let partition_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Failed to downcast to Int32Array");
        assert_eq!(partition_col.value(0), 0);
        assert_eq!(partition_col.value(1), 0);

        let offset_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Failed to downcast to Int64Array");
        assert_eq!(offset_col.value(0), 0);
        assert_eq!(offset_col.value(1), 1);
    }
}
