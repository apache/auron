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

use std::{any::Any, collections::HashMap, env, fmt::Formatter, fs, sync::Arc};

use arrow::array::{
    ArrayBuilder, BinaryArray, BinaryBuilder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    RecordBatch,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use auron_jni_bridge::{jni_call_static, jni_get_string, jni_new_string};
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
use futures::StreamExt;
use once_cell::sync::OnceCell;
use rdkafka::{
    ClientConfig, ClientContext, Offset, TopicPartitionList,
    config::RDKafkaLogLevel,
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    error::KafkaResult,
};
use sonic_rs::{JsonContainerTrait, JsonValueTrait};

use crate::{
    common::{column_pruning::ExecuteWithColumnPruning, execution_context::ExecutionContext},
    flink::serde::{flink_deserializer::FlinkDeserializer, pb_deserializer::PbDeserializer},
    rdkafka::Message,
};

struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        log::info!("Kafka Pre re-balance {rebalance:?}");
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        log::info!("Kafka Post re-balance {rebalance:?}");
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        log::info!("Kafka Committing offsets: {result:?}");
    }
}

#[derive(Debug, Clone)]
pub struct KafkaScanExec {
    kafka_topic: String,
    kafka_properties_json: String,
    schema: SchemaRef,
    batch_size: i32,
    startup_mode: i32,
    auron_operator_id: String,
    data_format: i32,
    format_config_json: String,
    metrics: ExecutionPlanMetricsSet,
    props: OnceCell<PlanProperties>,
}

impl KafkaScanExec {
    pub fn new(
        kafka_topic: String,
        kafka_properties_json: String,
        schema: SchemaRef,
        batch_size: i32,
        startup_mode: i32,
        auron_operator_id: String,
        data_format: i32,
        format_config_json: String,
    ) -> Self {
        Self {
            kafka_topic,
            kafka_properties_json,
            schema,
            batch_size,
            startup_mode,
            auron_operator_id,
            data_format,
            format_config_json,
            metrics: ExecutionPlanMetricsSet::new(),
            props: OnceCell::new(),
        }
    }

    fn execute_with_ctx(
        &self,
        exec_ctx: Arc<ExecutionContext>,
    ) -> Result<SendableRecordBatchStream> {
        let serialized_pb_stream = read_serialized_records_from_kafka(
            exec_ctx.clone(),
            self.kafka_topic.clone(),
            self.kafka_properties_json.clone(),
            self.batch_size as usize,
            self.startup_mode,
            self.auron_operator_id.clone(),
            self.data_format,
            self.format_config_json.clone(),
        )?;

        let deserialized_pb_stream = parse_records(
            exec_ctx.output_schema(),
            exec_ctx.clone(),
            serialized_pb_stream,
            self.format_config_json.clone(),
        )?;
        Ok(deserialized_pb_stream)
    }
}

impl DisplayAs for KafkaScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "KafkaScanExec")
    }
}

impl ExecutionPlan for KafkaScanExec {
    fn name(&self) -> &str {
        "KafkaScanExec"
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
            self.kafka_topic.clone(),
            self.kafka_properties_json.clone(),
            self.schema.clone(),
            self.batch_size,
            self.startup_mode,
            self.auron_operator_id.clone(),
            self.data_format,
            self.format_config_json.clone(),
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

impl ExecuteWithColumnPruning for KafkaScanExec {
    fn execute_projected(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        projection: &[usize],
    ) -> Result<SendableRecordBatchStream> {
        let projected_schema = Arc::new(self.schema().project(projection)?);
        let exec_ctx =
            ExecutionContext::new(context, partition, projected_schema.clone(), &self.metrics);
        self.execute_with_ctx(exec_ctx)
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

fn read_serialized_records_from_kafka(
    exec_ctx: Arc<ExecutionContext>,
    kafka_topic: String,
    kafka_properties_json: String,
    batch_size: usize,
    startup_mode: i32,
    auron_operator_id: String,
    _data_format: i32,
    _format_config_json: String,
) -> Result<SendableRecordBatchStream> {
    let context = CustomContext;
    // get source json string from jni bridge resource
    let resource_id = jni_new_string!(&auron_operator_id)?;
    let kafka_task_json_java =
        jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
    let kafka_task_json = jni_get_string!(kafka_task_json_java.as_obj().into())
        .expect("kafka_task_json_java is not valid java string");
    let task_json = sonic_rs::from_str::<sonic_rs::Value>(&kafka_task_json)
        .expect("source_json_str is not valid json");
    let subtask_index = task_json
        .get("subtask_index")
        .as_i64()
        .expect("subtask_index is not valid json") as i32;
    let enable_checkpoint = task_json
        .get("enable_checkpoint")
        .as_bool()
        .expect("enable_checkpoint is not valid json");
    let restored_offsets = task_json
        .get("restored_offsets")
        .expect("restored_offsets is not valid json");
    let mut partitions: Vec<i32> = vec![];
    if let Some(assigned_partitions) = task_json.get("assigned_partitions") {
        if let Some(array) = assigned_partitions.as_array() {
            array.iter().for_each(|v| {
                if let Some(num) = v.as_i64() {
                    partitions.push(num as i32);
                }
            });
        }
    }
    if partitions.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "No partitions found for topic: {kafka_topic}"
        )));
    }
    let kafka_properties = sonic_rs::from_str::<sonic_rs::Value>(&kafka_properties_json)
        .expect("kafka_properties_json is not valid json");
    let mut config = ClientConfig::new();
    config.set_log_level(RDKafkaLogLevel::Info);
    kafka_properties
        .into_object()
        .expect("kafka_properties is not valid json")
        .iter_mut()
        .for_each(|(key, value)| {
            config.set(
                key,
                value
                    .as_str()
                    .expect("kafka property value is not valid json string"),
            );
        });
    if enable_checkpoint {
        config.set("enable.auto.commit", "false");
    } else {
        config.set("enable.auto.commit", "true");
    }
    log::info!("Subtask {subtask_index} consumed kafka config is {config:?}");
    let consumer: Arc<LoggingConsumer> = Arc::new(
        config
            .create_with_context(context)
            .expect("Kafka Consumer creation failed"),
    );

    // GROUP_OFFSET = 0;
    // EARLIEST = 1;
    // LATEST = 2;
    // TIMESTAMP = 3;
    let offset = match startup_mode {
        0 => Offset::Stored,
        1 => Offset::Beginning,
        2 => Offset::End,
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Invalid startup mode: {startup_mode}"
            )));
        }
    };

    log::info!("Subtask {subtask_index} consumed partitions {partitions:?}");
    let mut partition_list = TopicPartitionList::with_capacity(partitions.len());
    for partition in partitions.iter() {
        let partition_key = partition.to_string();
        let partition_offset = if let Some(val) = restored_offsets.get(&partition_key) {
            Offset::Offset(
                val.as_i64()
                    .expect("restored_offsets value is not valid json"),
            )
        } else {
            offset
        };
        let _ = partition_list.add_partition_offset(&kafka_topic, *partition, partition_offset);
    }
    consumer
        .assign(&partition_list)
        .expect("Can't assign partitions to consumer");

    let output_schema = Arc::new(Schema::new(vec![
        Field::new("serialized_kafka_records_partition", DataType::Int32, false),
        Field::new("serialized_kafka_records_offset", DataType::Int64, false),
        Field::new("serialized_kafka_records_timestamp", DataType::Int64, false),
        Field::new("serialized_pb_records", DataType::Binary, false),
    ]));

    Ok(exec_ctx
        .with_new_output_schema(output_schema.clone())
        .output_with_sender("KafkaScanExec.KafkaConsumer", move |sender| async move {
            let mut serialized_kafka_records_partition_builder = Int32Builder::with_capacity(0);
            let mut serialized_kafka_records_offset_builder = Int64Builder::with_capacity(0);
            let mut serialized_kafka_records_timestamp_builder = Int64Builder::with_capacity(0);
            let mut serialized_pb_records_builder = BinaryBuilder::with_capacity(batch_size, 0);
            loop {
                while serialized_pb_records_builder.len() < batch_size {
                    match consumer.recv().await {
                        Err(e) => log::warn!("Kafka error: {e}"),
                        Ok(msg) => {
                            if let Some(payload) = msg.payload() {
                                serialized_kafka_records_partition_builder
                                    .append_value(msg.partition());
                                serialized_kafka_records_offset_builder.append_value(msg.offset());
                                serialized_kafka_records_timestamp_builder
                                    .append_option(msg.timestamp().to_millis());
                                serialized_pb_records_builder.append_value(payload);
                            }
                        }
                    }
                }
                let batch = RecordBatch::try_new(
                    output_schema.clone(),
                    vec![
                        Arc::new(serialized_kafka_records_partition_builder.finish()),
                        Arc::new(serialized_kafka_records_offset_builder.finish()),
                        Arc::new(serialized_kafka_records_timestamp_builder.finish()),
                        Arc::new(serialized_pb_records_builder.finish()),
                    ],
                )?;
                sender.send(batch).await;
                if enable_checkpoint {
                    // if checkpoint is enabled, commit offsets to kafka
                    let offset_to_commit = auron_operator_id.clone() + "-offsets2commit";
                    let resource_id = jni_new_string!(&offset_to_commit)?;
                    let java_json_str =
                        jni_call_static!(JniBridge.getResource(resource_id.as_obj()) -> JObject)?;
                    if !java_json_str.0.is_null() {
                        let offset_json_str = jni_get_string!(java_json_str.as_obj().into())
                            .expect("java_json_str is not valid java string");
                        let offsets_to_commit =
                            sonic_rs::from_str::<sonic_rs::Value>(&offset_json_str)
                                .expect("offset_json_str is not valid json");
                        let mut partition_list = TopicPartitionList::with_capacity(
                            offsets_to_commit
                                .as_object()
                                .expect("offsets_to_commit is not valid json")
                                .len(),
                        );
                        if let Some(obj) = offsets_to_commit.as_object() {
                            if !obj.is_empty() {
                                for (partition, offset) in obj {
                                    let _ = partition_list.add_partition_offset(
                                        &kafka_topic,
                                        partition
                                            .parse::<i32>()
                                            .expect("partition is not valid json"),
                                        Offset::Offset(
                                            offset.as_i64().expect("offset is not valid json"),
                                        ),
                                    );
                                }
                                log::info!("auron consumer to commit offset: {partition_list:?}");
                                let _ = consumer.commit(&partition_list, CommitMode::Async);
                            }
                        }
                    }
                }
            }
        }))
}

fn parse_records(
    schema: SchemaRef,
    exec_ctx: Arc<ExecutionContext>,
    mut input_stream: SendableRecordBatchStream,
    parser_config_json: String,
) -> Result<SendableRecordBatchStream> {
    let parser_config = sonic_rs::from_str::<sonic_rs::Value>(&parser_config_json)
        .expect("parser_config_json is not valid json");
    let pb_desc_file = parser_config
        .get("pb_desc_file")
        .and_then(|v| v.as_str())
        .expect("pb_desc_file is not valid string")
        .to_string();
    let root_message_name = parser_config
        .get("root_message_name")
        .and_then(|v| v.as_str())
        .expect("root_message_name is not valid string")
        .to_string();
    let skip_fields = parser_config
        .get("skip_fields")
        .and_then(|v| v.as_str())
        .expect("skip_fields is not valid string")
        .to_string();
    let nested_col_mapping_json = parser_config
        .get("nested_col_mapping")
        .expect("nested_col_mapping is not valid json");
    let mut nested_msg_mapping: HashMap<String, String> = HashMap::new();
    if let Some(obj) = nested_col_mapping_json.as_object() {
        for (key, value) in obj {
            if let Some(val_str) = value.as_str() {
                nested_msg_mapping.insert(key.to_string(), val_str.to_string());
            }
        }
    }

    let local_pb_desc_file = env::var("PWD").expect("PWD env var is not set") + "/" + &pb_desc_file;
    log::info!("load desc from {local_pb_desc_file}");
    let file_descriptor_bytes = fs::read(local_pb_desc_file).expect("Failed to read file");
    let skip_fields_vec: Vec<String> = skip_fields
        .split(",")
        .map(|s| s.to_string())
        .collect::<Vec<String>>();
    Ok(exec_ctx.clone().output_with_sender(
        "KafkaScanExec.ParseRecords",
        move |sender| async move {
            // TODO: json parser
            let mut parser: Box<dyn FlinkDeserializer> = Box::new(PbDeserializer::new(
                &file_descriptor_bytes,
                &root_message_name,
                schema.clone(),
                &nested_msg_mapping,
                &skip_fields_vec,
            )?);
            while let Some(batch) = input_stream.next().await.transpose()? {
                let kafka_partition = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("input must be Int32Array");
                let kafka_offset = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("input must be Int64Array");
                let kafka_timestamp = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("input must be Int64Array");
                let records = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("input must be BinaryArray");
                let output_batch = parser.parse_messages_with_kafka_meta(
                    &records,
                    &kafka_partition,
                    &kafka_offset,
                    &kafka_timestamp,
                )?;
                sender.send(output_batch).await;
            }
            #[allow(unreachable_code)]
            Ok(())
        },
    ))
}
