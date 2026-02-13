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
    array::{Float64Array, RecordBatch},
    datatypes::{DataType, Schema},
};
use datafusion::{
    common::Result,
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
};
use parking_lot::Mutex;
use rand::{SeedableRng, rngs::StdRng};
use rand_distr::{Distribution, StandardNormal};

use crate::down_cast_any_ref;

/// Returns random values with independent and identically distributed (i.e.d.)
/// samples drawn from the standard normal distribution.
///
/// Matches Spark's behavior:
/// - RNG is seeded with `seed + partition_id`
/// - RNG state advances for each row (stateful across batches)
pub struct RandnExpr {
    seed: i64,
    partition_id: usize,
    rng: Mutex<StdRng>,
}

impl RandnExpr {
    pub fn new(seed: i64, partition_id: usize) -> Self {
        let effective_seed = (seed as u64).wrapping_add(partition_id as u64);
        Self {
            seed,
            partition_id,
            rng: Mutex::new(StdRng::seed_from_u64(effective_seed)),
        }
    }
}

impl Display for RandnExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Randn(seed={}, partition={})",
            self.seed, self.partition_id
        )
    }
}

impl Debug for RandnExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Randn(seed={}, partition={})",
            self.seed, self.partition_id
        )
    }
}

impl PartialEq for RandnExpr {
    fn eq(&self, other: &Self) -> bool {
        self.seed == other.seed && self.partition_id == other.partition_id
    }
}

impl Eq for RandnExpr {}

impl Hash for RandnExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.seed.hash(state);
        self.partition_id.hash(state);
    }
}

impl PhysicalExpr for RandnExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let num_rows = batch.num_rows();
        let mut rng = self.rng.lock();
        let values =
            Float64Array::from_iter_values(StandardNormal.sample_iter(&mut *rng).take(num_rows));
        Ok(ColumnarValue::Array(Arc::new(values)))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::new(self.seed, self.partition_id)))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "randn({})", self.seed)
    }
}

impl PartialEq<dyn Any> for RandnExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|other| self.seed == other.seed && self.partition_id == other.partition_id)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{array::RecordBatch, datatypes::Schema};
    use datafusion::common::{Result, cast::as_float64_array};

    use super::*;

    fn create_empty_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::empty());
        RecordBatch::try_new_with_options(
            schema,
            vec![],
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )
        .expect("Failed to create empty batch")
    }

    #[test]
    fn test_randn_generates_different_values_per_row() -> Result<()> {
        let expr = RandnExpr::new(42, 0);
        let batch = create_empty_batch(5);

        let result = expr.evaluate(&batch)?;
        let array = result.into_array(5)?;
        let float_arr = as_float64_array(&array)?;

        // All values should be different (with extremely high probability)
        let values: Vec<f64> = (0..5).map(|i| float_arr.value(i)).collect();
        for i in 0..values.len() {
            for j in (i + 1)..values.len() {
                assert_ne!(
                    values[i], values[j],
                    "Values at index {i} and {j} should be different"
                );
            }
        }

        Ok(())
    }

    #[test]
    fn test_randn_reproducible_with_same_seed() -> Result<()> {
        let expr1 = RandnExpr::new(42, 0);
        let expr2 = RandnExpr::new(42, 0);
        let batch = create_empty_batch(5);

        let result1 = expr1.evaluate(&batch)?;
        let result2 = expr2.evaluate(&batch)?;

        let arr1_binding = result1.into_array(5)?;
        let arr2_binding = result2.into_array(5)?;
        let arr1 = as_float64_array(&arr1_binding)?;
        let arr2 = as_float64_array(&arr2_binding)?;

        for i in 0..5 {
            assert_eq!(
                arr1.value(i),
                arr2.value(i),
                "Same seed should produce same values"
            );
        }

        Ok(())
    }

    #[test]
    fn test_randn_different_seeds_produce_different_values() -> Result<()> {
        let expr1 = RandnExpr::new(42, 0);
        let expr2 = RandnExpr::new(123, 0);
        let batch = create_empty_batch(5);

        let result1 = expr1.evaluate(&batch)?;
        let result2 = expr2.evaluate(&batch)?;

        let arr1_binding = result1.into_array(5)?;
        let arr2_binding = result2.into_array(5)?;
        let arr1 = as_float64_array(&arr1_binding)?;
        let arr2 = as_float64_array(&arr2_binding)?;

        // At least one value should be different
        let any_different = (0..5).any(|i| arr1.value(i) != arr2.value(i));
        assert!(
            any_different,
            "Different seeds should produce different values"
        );

        Ok(())
    }

    #[test]
    fn test_randn_different_partitions_produce_different_values() -> Result<()> {
        let expr1 = RandnExpr::new(42, 0);
        let expr2 = RandnExpr::new(42, 1);
        let batch = create_empty_batch(5);

        let result1 = expr1.evaluate(&batch)?;
        let result2 = expr2.evaluate(&batch)?;

        let arr1_binding = result1.into_array(5)?;
        let arr2_binding = result2.into_array(5)?;
        let arr1 = as_float64_array(&arr1_binding)?;
        let arr2 = as_float64_array(&arr2_binding)?;

        // At least one value should be different
        let any_different = (0..5).any(|i| arr1.value(i) != arr2.value(i));
        assert!(
            any_different,
            "Different partitions should produce different values"
        );

        Ok(())
    }

    #[test]
    fn test_randn_stateful_across_batches() -> Result<()> {
        let expr = RandnExpr::new(42, 0);
        let batch1 = create_empty_batch(3);
        let batch2 = create_empty_batch(3);

        // Evaluate two batches sequentially
        let result1 = expr.evaluate(&batch1)?;
        let result2 = expr.evaluate(&batch2)?;

        let arr1_binding = result1.into_array(3)?;
        let arr2_binding = result2.into_array(3)?;
        let arr1 = as_float64_array(&arr1_binding)?;
        let arr2 = as_float64_array(&arr2_binding)?;

        // Collect all values
        let values1: Vec<f64> = (0..3).map(|i| arr1.value(i)).collect();
        let values2: Vec<f64> = (0..3).map(|i| arr2.value(i)).collect();

        // Second batch should continue from where first left off (not restart)
        // So values should be different between batches
        assert_ne!(values1, values2, "Batches should have different values");

        // Compare with fresh expr that evaluates both batches together
        let expr_fresh = RandnExpr::new(42, 0);
        let batch_combined = create_empty_batch(6);
        let result_combined = expr_fresh.evaluate(&batch_combined)?;
        let arr_combined_binding = result_combined.into_array(6)?;
        let arr_combined = as_float64_array(&arr_combined_binding)?;

        // First 3 values should match values1, next 3 should match values2
        for i in 0..3 {
            assert_eq!(
                arr_combined.value(i),
                values1[i],
                "First batch values should match"
            );
            assert_eq!(
                arr_combined.value(i + 3),
                values2[i],
                "Second batch values should match continuation"
            );
        }

        Ok(())
    }
}
