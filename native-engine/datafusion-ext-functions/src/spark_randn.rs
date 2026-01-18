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

use std::sync::Arc;

use arrow::array::Float64Array;
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

/// Spark-style `randn(seed)` implementation.
/// Generates a random column with independent and identically distributed (i.i.d.)
/// samples from the standard normal distribution N(0, 1).
///
/// - Takes an optional seed (i64) for reproducibility
/// - If no seed is provided, uses a random seed
/// - Uses Box-Muller transform to generate normal distribution from uniform
pub fn spark_randn(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Parse optional seed argument
    let seed: Option<i64> = if args.is_empty() {
        None
    } else {
        match &args[0] {
            ColumnarValue::Scalar(ScalarValue::Int64(s)) => *s,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(s))) => Some(*s as i64),
            ColumnarValue::Scalar(ScalarValue::Null) => None,
            _ => None,
        }
    };

    // Create RNG - seeded if seed provided, otherwise random
    let mut rng: StdRng = match seed {
        Some(s) => StdRng::seed_from_u64(s as u64),
        None => StdRng::from_os_rng(),
    };

    // Generate a single standard normal value using Box-Muller transform
    let value = box_muller_single(&mut rng);

    Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(value))))
}

/// Spark-style `randn(seed)` for array output.
/// This variant generates multiple values for use in array contexts.
#[allow(dead_code)]
pub fn spark_randn_array(
    len: usize,
    seed: Option<i64>,
) -> Result<ColumnarValue> {
    // Create RNG - seeded if seed provided, otherwise random
    let mut rng: StdRng = match seed {
        Some(s) => StdRng::seed_from_u64(s as u64),
        None => StdRng::from_os_rng(),
    };

    // Generate `len` standard normal values
    let values: Vec<f64> = (0..len)
        .map(|_| box_muller_single(&mut rng))
        .collect();

    let array = Float64Array::from(values);
    Ok(ColumnarValue::Array(Arc::new(array)))
}

/// Box-Muller transform to generate a single standard normal random value.
/// Takes two uniform random numbers in (0, 1) and produces a standard normal value.
fn box_muller_single<R: Rng>(rng: &mut R) -> f64 {
    // Generate two uniform random numbers in (0, 1)
    // We use gen_range to exclude 0 to avoid log(0)
    let u1: f64 = rng.random_range(f64::MIN_POSITIVE..1.0);
    let u2: f64 = rng.random_range(0.0..1.0);

    // Box-Muller transform
    let two_pi = 2.0 * std::f64::consts::PI;
    (-2.0 * u1.ln()).sqrt() * (two_pi * u2).cos()
}

#[cfg(test)]
mod test {
    use std::error::Error;

    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_randn::{spark_randn, spark_randn_array};

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

    #[test]
    fn test_randn_array_with_seed() -> Result<(), Box<dyn Error>> {
        // Generate array of random values
        let result = spark_randn_array(5, Some(42))?;

        let array = result.into_array(5)?;
        assert_eq!(array.len(), 5);

        // All values should be finite
        let float_array = array
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("Expected Float64Array");

        for i in 0..5 {
            assert!(
                float_array.value(i).is_finite(),
                "Value at index {} should be finite",
                i
            );
        }
        Ok(())
    }

    #[test]
    fn test_randn_array_reproducibility() -> Result<(), Box<dyn Error>> {
        // Same seed should produce same array
        let result1 = spark_randn_array(3, Some(42))?;
        let result2 = spark_randn_array(3, Some(42))?;

        let array1 = result1.into_array(3)?;
        let array2 = result2.into_array(3)?;

        let float_array1 = array1
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("Expected Float64Array");
        let float_array2 = array2
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("Expected Float64Array");

        for i in 0..3 {
            assert_eq!(
                float_array1.value(i),
                float_array2.value(i),
                "Values at index {} should match with same seed",
                i
            );
        }
        Ok(())
    }

    #[test]
    fn test_randn_distribution_properties() -> Result<(), Box<dyn Error>> {
        // Generate a larger sample and check basic statistical properties
        // For standard normal: mean ≈ 0, std ≈ 1
        let n = 10000;
        let result = spark_randn_array(n, Some(12345))?;
        let array = result.into_array(n)?;

        let float_array = array
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .expect("Expected Float64Array");

        let values: Vec<f64> = (0..n).map(|i| float_array.value(i)).collect();

        // Calculate mean
        let mean: f64 = values.iter().sum::<f64>() / n as f64;

        // Calculate standard deviation
        let variance: f64 = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n as f64;
        let std_dev = variance.sqrt();

        // Check that mean is close to 0 (within 0.05)
        assert!(
            mean.abs() < 0.05,
            "Mean {} should be close to 0",
            mean
        );

        // Check that std dev is close to 1 (within 0.05)
        assert!(
            (std_dev - 1.0).abs() < 0.05,
            "Std dev {} should be close to 1",
            std_dev
        );

        Ok(())
    }
}
