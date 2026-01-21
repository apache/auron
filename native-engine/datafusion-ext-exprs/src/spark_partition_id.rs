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

use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Schema},
};
use datafusion::{
    common::Result,
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
};

pub struct SparkPartitionIdExpr {
    partition_id: i32,
}

impl SparkPartitionIdExpr {
    pub fn new(partition_id: usize) -> Self {
        Self {
            partition_id: partition_id as i32,
        }
    }
}

impl Display for SparkPartitionIdExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkPartitionID")
    }
}

impl Debug for SparkPartitionIdExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SparkPartitionID")
    }
}

impl PartialEq for SparkPartitionIdExpr {
    fn eq(&self, other: &Self) -> bool {
        self.partition_id == other.partition_id
    }
}

impl Eq for SparkPartitionIdExpr {}

impl Hash for SparkPartitionIdExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.partition_id.hash(state)
    }
}

impl PhysicalExpr for SparkPartitionIdExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let array = Int32Array::from_value(self.partition_id, num_rows);
        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int32Array,
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;

    #[test]
    fn test_data_type_and_nullable() {
        let expr = SparkPartitionIdExpr::new(0);
        let schema = Schema::new(vec![] as Vec<Field>);
        assert_eq!(
            expr.data_type(&schema).expect("data_type failed"),
            DataType::Int32
        );
        assert!(!expr.nullable(&schema).expect("nullable failed"));
    }

    #[test]
    fn test_evaluate_returns_constant_partition_id() {
        let expr = SparkPartitionIdExpr::new(5);
        let schema = Schema::new(vec![Field::new("col", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .expect("RecordBatch creation failed");

        let result = expr.evaluate(&batch).expect("evaluate failed");
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("downcast failed");
                assert_eq!(int_arr.len(), 3);
                for i in 0..3 {
                    assert_eq!(int_arr.value(i), 5);
                }
            }
            _ => unreachable!("Expected Array result"),
        }
    }

    #[test]
    fn test_evaluate_different_partition_ids() {
        let schema = Schema::new(vec![Field::new("col", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .expect("RecordBatch creation failed");

        for partition_id in [0, 1, 100, 999] {
            let expr = SparkPartitionIdExpr::new(partition_id);
            let result = expr.evaluate(&batch).expect("evaluate failed");
            match result {
                ColumnarValue::Array(arr) => {
                    let int_arr = arr
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("downcast failed");
                    for i in 0..int_arr.len() {
                        assert_eq!(int_arr.value(i), partition_id as i32);
                    }
                }
                _ => unreachable!("Expected Array result"),
            }
        }
    }

    #[test]
    fn test_equality() {
        let expr1 = SparkPartitionIdExpr::new(5);
        let expr2 = SparkPartitionIdExpr::new(5);
        let expr3 = SparkPartitionIdExpr::new(3);

        assert_eq!(expr1, expr2);
        assert_ne!(expr1, expr3);
    }
}
