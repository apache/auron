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
    common::{
        DataFusionError, Result, ScalarValue,
        cast::{as_int32_array, as_int64_array},
    },
    logical_expr::ColumnarValue,
};
use rand::{SeedableRng, rngs::StdRng};
use rand_distr::{Distribution, StandardNormal};

/// Returns random values with independent and identically distributed (i.i.d.)
/// values drawn from the standard normal distribution.
///
/// - Takes an array of seed values (Int64 or Int32)
/// - For each element: if it's a valid int, use it as the seed; otherwise use a
///   random seed
/// - Null values use a random seed
/// - Returns an array of Float64 random values
pub fn spark_randn(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return Err(DataFusionError::Execution(
            "spark_randn() requires one argument".to_string(),
        ));
    }

    match &args[0] {
        ColumnarValue::Array(arr) => {
            let len = arr.len();
            let results: Vec<Option<f64>> = match arr.data_type() {
                arrow::datatypes::DataType::Int64 => {
                    let int_arr = as_int64_array(arr)?;
                    int_arr
                        .iter()
                        .map(|opt| {
                            let seed = opt.map(|v| v as u64).unwrap_or_else(rand::random);
                            let mut rng = StdRng::seed_from_u64(seed);
                            Some(StandardNormal.sample(&mut rng))
                        })
                        .collect()
                }
                arrow::datatypes::DataType::Int32 => {
                    let int_arr = as_int32_array(arr)?;
                    int_arr
                        .iter()
                        .map(|opt| {
                            let seed = opt.map(|v| v as u64).unwrap_or_else(rand::random);
                            let mut rng = StdRng::seed_from_u64(seed);
                            Some(StandardNormal.sample(&mut rng))
                        })
                        .collect()
                }
                _ => {
                    // For unsupported types, use random seed for each element
                    (0..len)
                        .map(|_| {
                            let seed: u64 = rand::random();
                            let mut rng = StdRng::seed_from_u64(seed);
                            Some(StandardNormal.sample(&mut rng))
                        })
                        .collect()
                }
            };

            Ok(ColumnarValue::Array(Arc::new(Float64Array::from(results))))
        }
        ColumnarValue::Scalar(sv) => {
            let seed: u64 = match sv {
                ScalarValue::Int64(Some(s)) => *s as u64,
                ScalarValue::Int32(Some(s)) => *s as u64,
                _ => rand::random(),
            };
            let mut rng = StdRng::seed_from_u64(seed);
            let value: f64 = StandardNormal.sample(&mut rng);
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(value))))
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array};
    use datafusion::{
        common::{Result, ScalarValue, cast::as_float64_array},
        logical_expr::ColumnarValue,
    };

    use crate::spark_randn::spark_randn;

    #[test]
    fn test_randn_array_with_int64_seeds() -> Result<()> {
        // Create an array of Int64 seeds
        let seeds = Arc::new(Int64Array::from(vec![Some(42), Some(123), Some(42)]));
        let input = ColumnarValue::Array(seeds);

        let result = spark_randn(&[input])?;

        match result {
            ColumnarValue::Array(arr) => {
                assert_eq!(arr.len(), 3, "Should return 3 values");
                let float_arr = as_float64_array(&arr)?;

                // All values should be finite
                for i in 0..float_arr.len() {
                    assert!(
                        float_arr.value(i).is_finite(),
                        "Value at index {} should be finite",
                        i
                    );
                }

                // Same seed (index 0 and 2) should produce same result
                assert_eq!(
                    float_arr.value(0),
                    float_arr.value(2),
                    "Same seed should produce same result"
                );

                // Different seeds should produce different results
                assert_ne!(
                    float_arr.value(0),
                    float_arr.value(1),
                    "Different seeds should produce different results"
                );
            }
            _ => panic!("Expected Array result"),
        }
        Ok(())
    }

    #[test]
    fn test_randn_array_with_int32_seeds() -> Result<()> {
        // Create an array of Int32 seeds
        let seeds = Arc::new(Int32Array::from(vec![Some(42), Some(123), Some(42)]));
        let input = ColumnarValue::Array(seeds);

        let result = spark_randn(&[input])?;

        match result {
            ColumnarValue::Array(arr) => {
                assert_eq!(arr.len(), 3, "Should return 3 values");
                let float_arr = as_float64_array(&arr)?;

                // All values should be finite
                for i in 0..float_arr.len() {
                    assert!(
                        float_arr.value(i).is_finite(),
                        "Value at index {} should be finite",
                        i
                    );
                }

                // Same seed (index 0 and 2) should produce same result
                assert_eq!(
                    float_arr.value(0),
                    float_arr.value(2),
                    "Same seed should produce same result"
                );
            }
            _ => panic!("Expected Array result"),
        }
        Ok(())
    }

    #[test]
    fn test_randn_array_with_null_seeds() -> Result<()> {
        // Create an array with some null values
        let seeds = Arc::new(Int64Array::from(vec![Some(42), None, Some(42)]));
        let input = ColumnarValue::Array(seeds);

        let result = spark_randn(&[input])?;

        match result {
            ColumnarValue::Array(arr) => {
                assert_eq!(arr.len(), 3, "Should return 3 values");
                let float_arr = as_float64_array(&arr)?;

                // All values should be finite (including null seed which gets random)
                for i in 0..float_arr.len() {
                    assert!(
                        float_arr.value(i).is_finite(),
                        "Value at index {} should be finite",
                        i
                    );
                }

                // Same non-null seeds should produce same result
                assert_eq!(
                    float_arr.value(0),
                    float_arr.value(2),
                    "Same seed should produce same result"
                );

                // Null seed (index 1) should produce different result from fixed seeds
                // (with very high probability)
                assert_ne!(
                    float_arr.value(0),
                    float_arr.value(1),
                    "Null seed should produce different random result"
                );
            }
            _ => panic!("Expected Array result"),
        }
        Ok(())
    }

    #[test]
    fn test_randn_scalar_with_seed_reproducibility() -> Result<()> {
        // Same seed should produce same result
        let seed1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));
        let seed2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));

        let result1 = spark_randn(&[seed1])?;
        let result2 = spark_randn(&[seed2])?;

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
    fn test_randn_scalar_different_seeds() -> Result<()> {
        // Different seeds should produce different results
        let seed1 = ColumnarValue::Scalar(ScalarValue::Int64(Some(42)));
        let seed2 = ColumnarValue::Scalar(ScalarValue::Int64(Some(123)));

        let result1 = spark_randn(&[seed1])?;
        let result2 = spark_randn(&[seed2])?;

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
    fn test_randn_scalar_with_int32_seed() -> Result<()> {
        // Int32 seed should work
        let seed = ColumnarValue::Scalar(ScalarValue::Int32(Some(42)));

        let result = spark_randn(&[seed])?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                assert!(v.is_finite(), "Result should be a finite number");
            }
            _ => panic!("Expected Float64 scalar result"),
        }
        Ok(())
    }

    #[test]
    fn test_randn_scalar_with_null_seed() -> Result<()> {
        // Null seed should be treated as random seed
        let seed = ColumnarValue::Scalar(ScalarValue::Null);

        let result = spark_randn(&[seed])?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                assert!(v.is_finite(), "Result should be a finite number");
            }
            _ => panic!("Expected Float64 scalar result"),
        }
        Ok(())
    }
}
