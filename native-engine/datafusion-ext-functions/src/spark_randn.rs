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

use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, StandardNormal};

/// Spark-style `randn(seed)` implementation.
/// Generates a random column with independent and identically distributed (i.i.d.)
/// samples from the standard normal distribution N(0, 1).
///
/// - Takes an optional seed (i64) for reproducibility
/// - If no seed is provided, uses a random seed
pub fn spark_randn(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Parse seed argument, or generate random seed if not provided
    let seed: u64 = if args.is_empty() {
        rand::random()
    } else {
        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(Some(s))) => *s as u64,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(s))) => *s as u64,
            _ => rand::random(),
        }
    };

    let mut rng = StdRng::seed_from_u64(seed);
    let value: f64 = StandardNormal.sample(&mut rng);

    Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(value))))
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_randn::spark_randn;

    #[test]
    fn test_randn_with_seed_reproducibility() -> Result<(), Box<dyn Error>> {
        // Same seed should produce same result
        let seed = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));

        let result1 = spark_randn(&vec![seed.clone()])?;
        let result2 = spark_randn(&vec![seed])?;

        match (result1, result2) {
            (
                ColumnarValue::Scalar(ScalarValue::Float64(Some(v1))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(v2))),
            ) => {
                assert_eq!(v1, v2, "Same seed should produce same result");
            }
            _ => panic!("Expected Float64 scalar results"),
        }
        Ok(())
    }

    #[test]
    fn test_randn_different_seeds() -> Result<(), Box<dyn Error>> {
        // Different seeds should produce different results (with very high probability)
        let seed1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));
        let seed2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(123)));

        let result1 = spark_randn(&vec![seed1])?;
        let result2 = spark_randn(&vec![seed2])?;

        match (result1, result2) {
            (
                ColumnarValue::Scalar(ScalarValue::Float64(Some(v1))),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(v2))),
            ) => {
                assert_ne!(v1, v2, "Different seeds should produce different results");
            }
            _ => panic!("Expected Float64 scalar results"),
        }
        Ok(())
    }

    #[test]
    fn test_randn_no_seed() -> Result<(), Box<dyn Error>> {
        // Without seed, should still produce a valid float
        let result = spark_randn(&vec![])?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                assert!(v.is_finite(), "Result should be a finite number");
            }
            _ => panic!("Expected Float64 scalar result"),
        }
        Ok(())
    }

    #[test]
    fn test_randn_with_int32_seed() -> Result<(), Box<dyn Error>> {
        // Int32 seed should work
        let seed = ColumnarValue::Scalar(ScalarValue::Int32(Some(42)));

        let result = spark_randn(&vec![seed])?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                assert!(v.is_finite(), "Result should be a finite number");
            }
            _ => panic!("Expected Float64 scalar result"),
        }
        Ok(())
    }

    #[test]
    fn test_randn_with_null_seed() -> Result<(), Box<dyn Error>> {
        // Null seed should be treated as no seed (random)
        let seed = ColumnarValue::Scalar(ScalarValue::Null);

        let result = spark_randn(&vec![seed])?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                assert!(v.is_finite(), "Result should be a finite number");
            }
            _ => panic!("Expected Float64 scalar result"),
        }
        Ok(())
    }

}
