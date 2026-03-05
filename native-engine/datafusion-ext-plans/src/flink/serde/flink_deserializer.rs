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

use arrow::array::{BinaryArray, Int32Array, Int64Array, RecordBatch};

/// FlinkDeserializer is used to deserialize messages from kafka.
/// Supports Protobuf, JSON, etc.
pub trait FlinkDeserializer: Send {
    /// Parse messages from kafka, including kafka metadata such as partitions,
    /// offsets, and timestamps.
    fn parse_messages_with_kafka_meta(
        &mut self,
        messages: &BinaryArray,
        kafka_partition: &Int32Array,
        kafka_offset: &Int64Array,
        kafka_timestamp: &Int64Array,
    ) -> datafusion::common::Result<RecordBatch>;
}
