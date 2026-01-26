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
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering::SeqCst},
    },
};

use arrow::{
    array::{Int64Array, RecordBatch},
    datatypes::{DataType, Schema},
};
use datafusion::{
    common::Result,
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
};

pub struct SparkMonotonicallyIncreasingIdExpr {
    partition_id: i64,
    row_counter: AtomicI64,
}

impl SparkMonotonicallyIncreasingIdExpr {
    pub fn new(partition_id: usize) -> Self {
        Self {
            partition_id: partition_id as i64,
            row_counter: AtomicI64::new(0),
        }
    }
}

impl Display for SparkMonotonicallyIncreasingIdExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MonotonicallyIncreasingID")
    }
}

impl Debug for SparkMonotonicallyIncreasingIdExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MonotonicallyIncreasingID")
    }
}

impl PartialEq for SparkMonotonicallyIncreasingIdExpr {
    fn eq(&self, other: &Self) -> bool {
        self.partition_id == other.partition_id
    }
}

impl Eq for SparkMonotonicallyIncreasingIdExpr {}

impl Hash for SparkMonotonicallyIncreasingIdExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.partition_id.hash(state);
    }
}

impl PhysicalExpr for SparkMonotonicallyIncreasingIdExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let start_row = self.row_counter.fetch_add(num_rows as i64, SeqCst);

        let partition_offset = self.partition_id << 33;
        let array: Int64Array = (start_row..start_row + num_rows as i64)
            .map(|row_id| partition_offset | row_id)
            .collect();

        Ok(ColumnarValue::Array(Arc::new(array)))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::new(self.partition_id as usize)))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::Int64Array,
        datatypes::{Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;

    #[test]
    fn test_data_type_and_nullable() {
        let expr = SparkMonotonicallyIncreasingIdExpr::new(0);
        let schema = Schema::new(vec![] as Vec<Field>);
        assert_eq!(
            expr.data_type(&schema).expect("data_type failed"),
            DataType::Int64
        );
        assert!(!expr.nullable(&schema).expect("nullable failed"));
    }

    #[test]
    fn test_evaluate_generates_monotonic_ids() {
        let expr = SparkMonotonicallyIncreasingIdExpr::new(0);
        let schema = Schema::new(vec![Field::new("col", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .expect("RecordBatch creation failed");

        let result = expr.evaluate(&batch).expect("evaluate failed");
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("downcast failed");
                assert_eq!(int_arr.len(), 3);
                assert_eq!(int_arr.value(0), 0);
                assert_eq!(int_arr.value(1), 1);
                assert_eq!(int_arr.value(2), 2);
            }
            _ => unreachable!("Expected Array result"),
        }

        let batch2 = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![4, 5]))],
        )
        .expect("RecordBatch creation failed");

        let result2 = expr.evaluate(&batch2).expect("evaluate failed");
        match result2 {
            ColumnarValue::Array(arr) => {
                let int_arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("downcast failed");
                assert_eq!(int_arr.len(), 2);
                assert_eq!(int_arr.value(0), 3);
                assert_eq!(int_arr.value(1), 4);
            }
            _ => unreachable!("Expected Array result"),
        }
    }

    #[test]
    fn test_evaluate_with_partition_offset() {
        let partition_id = 5;
        let expr = SparkMonotonicallyIncreasingIdExpr::new(partition_id);
        let schema = Schema::new(vec![Field::new("col", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("RecordBatch creation failed");

        let result = expr.evaluate(&batch).expect("evaluate failed");
        match result {
            ColumnarValue::Array(arr) => {
                let int_arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("downcast failed");
                let expected_offset = (partition_id as i64) << 33;
                assert_eq!(int_arr.value(0), expected_offset);
                assert_eq!(int_arr.value(1), expected_offset + 1);
            }
            _ => unreachable!("Expected Array result"),
        }
    }

    #[test]
    fn test_different_partitions_have_different_ranges() {
        let schema = Schema::new(vec![Field::new("col", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1, 2]))],
        )
        .expect("RecordBatch creation failed");

        let expr1 = SparkMonotonicallyIncreasingIdExpr::new(0);
        let expr2 = SparkMonotonicallyIncreasingIdExpr::new(1);

        let result1 = expr1.evaluate(&batch).expect("evaluate failed");
        let result2 = expr2.evaluate(&batch).expect("evaluate failed");

        match (result1, result2) {
            (ColumnarValue::Array(arr1), ColumnarValue::Array(arr2)) => {
                let int_arr1 = arr1
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("downcast failed");
                let int_arr2 = arr2
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("downcast failed");

                assert_ne!(int_arr1.value(0), int_arr2.value(0));
                assert_eq!(int_arr1.value(0), 0);
                assert_eq!(int_arr2.value(0), 1i64 << 33);
            }
            _ => unreachable!("Expected Array results"),
        }
    }
}
