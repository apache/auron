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
use datafusion_ext_commons::hash::mur::spark_compatible_murmur3_hash;
use parking_lot::Mutex;

use crate::down_cast_any_ref;

/// Spark's partition-local Rand expression, with RNG state retained across batches.
pub struct SparkRandExpr {
    seed: i64,
    partition_id: usize,
    // Separate occurrences must not share cached results, even with the same seed.
    identity: Arc<()>,
    rng: Mutex<XorShiftRandom>,
}

impl SparkRandExpr {
    pub fn new(seed: i64, partition_id: usize) -> Self {
        Self {
            seed,
            partition_id,
            identity: Arc::new(()),
            rng: Mutex::new(XorShiftRandom::new(seed.wrapping_add(partition_id as i64))),
        }
    }
}

// Matches org.apache.spark.util.random.XORShiftRandom and java.util.Random.nextDouble.
struct XorShiftRandom {
    state: u64,
}

impl XorShiftRandom {
    fn new(seed: i64) -> Self {
        // ByteBuffer.putLong uses big-endian bytes. Eight bytes have no Murmur3 tail.
        let bytes = seed.to_be_bytes();
        let low = spark_compatible_murmur3_hash(bytes, 0x3c074a61);
        let high = spark_compatible_murmur3_hash(bytes, low);
        Self {
            state: ((high as u64) << 32) | (low as u32 as u64),
        }
    }

    fn next_bits(&mut self, bits: u32) -> u64 {
        self.state ^= self.state << 21;
        self.state ^= self.state >> 35;
        self.state ^= self.state << 4;
        self.state & ((1u64 << bits) - 1)
    }

    fn next_double(&mut self) -> f64 {
        let bits = (self.next_bits(26) << 27) + self.next_bits(27);
        bits as f64 / (1u64 << 53) as f64
    }
}

impl Display for SparkRandExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Rand(seed={}, partition={})",
            self.seed, self.partition_id
        )
    }
}

impl Debug for SparkRandExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl PartialEq for SparkRandExpr {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }
}

impl Eq for SparkRandExpr {}

impl Hash for SparkRandExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::ptr::hash(Arc::as_ptr(&self.identity), state);
    }
}

impl PhysicalExpr for SparkRandExpr {
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
        let mut rng = self.rng.lock();
        let values =
            Float64Array::from_iter_values((0..batch.num_rows()).map(|_| rng.next_double()));
        Ok(ColumnarValue::Array(Arc::new(values)))
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self {
            identity: self.identity.clone(),
            ..Self::new(self.seed, self.partition_id)
        }))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "rand({})", self.seed)
    }
}

impl PartialEq<dyn Any> for SparkRandExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|other| self == other)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, RecordBatchOptions};
    use datafusion::common::cast::as_float64_array;

    use super::*;

    fn evaluate(expr: &dyn PhysicalExpr, num_rows: usize) -> Result<Vec<f64>> {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(num_rows)),
        )?;
        let array = expr.evaluate(&batch)?.into_array(num_rows)?;
        let values = as_float64_array(&array)?;
        assert_eq!(values.null_count(), 0);
        Ok(values.values().to_vec())
    }

    #[test]
    fn test_spark_reference_sequences() -> Result<()> {
        // Generated with Spark 3.5.8's XORShiftRandom(seed).nextDouble().
        let cases = [
            (
                0,
                [0.7604953758285915, 0.5234194256885571, 0.0953472826424725],
            ),
            (
                42,
                [0.619189370225301, 0.5096018842446481, 0.8325259388871524],
            ),
            (
                -42,
                [0.8788490308742221, 0.4429886392236586, 0.17533724584371424],
            ),
            (
                i64::MIN,
                [0.18084965112285223, 0.5968436565934606, 0.23269187154255977],
            ),
            (
                i64::MAX,
                [0.17715881442674208, 0.9077141821545979, 0.08354279996053304],
            ),
        ];
        for (seed, expected) in cases {
            assert_eq!(evaluate(&SparkRandExpr::new(seed, 0), 3)?, expected);
        }
        Ok(())
    }

    #[test]
    fn test_partition_seed_and_overflow() -> Result<()> {
        for seed in [0, 42, -42, i64::MIN, i64::MAX] {
            assert_eq!(
                evaluate(&SparkRandExpr::new(seed, 7), 16)?,
                evaluate(&SparkRandExpr::new(seed.wrapping_add(7), 0), 16)?,
            );
            assert_ne!(
                evaluate(&SparkRandExpr::new(seed, 0), 16)?,
                evaluate(&SparkRandExpr::new(seed, 1), 16)?,
            );
        }
        Ok(())
    }

    #[test]
    fn test_state_across_batches_and_empty_input() -> Result<()> {
        let expr = SparkRandExpr::new(42, 2);
        let mut actual = evaluate(&expr, 3)?;
        assert!(evaluate(&expr, 0)?.is_empty());
        actual.extend(evaluate(&expr, 11)?);
        actual.extend(evaluate(&expr, 1)?);
        assert_eq!(actual, evaluate(&SparkRandExpr::new(42, 2), 15)?);
        assert!(actual.iter().all(|v| (0.0..1.0).contains(v)));
        assert_eq!(expr.data_type(&Schema::empty())?, DataType::Float64);
        assert!(!expr.nullable(&Schema::empty())?);
        Ok(())
    }

    #[test]
    fn test_new_expression_has_independent_state() -> Result<()> {
        let expr = Arc::new(SparkRandExpr::new(42, 1));
        let expected = evaluate(expr.as_ref(), 5)?;
        let rebuilt = expr.with_new_children(vec![])?;
        assert_eq!(evaluate(rebuilt.as_ref(), 5)?, expected);
        Ok(())
    }
}
