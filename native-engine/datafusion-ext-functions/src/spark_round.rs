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

use arrow::{
    array::{Decimal128Array, Float64Array, Int16Array, Int32Array, Int64Array},
    datatypes::DataType,
};
use datafusion::{
    common::{
        cast::{
            as_decimal128_array, as_float64_array, as_int16_array, as_int32_array, as_int64_array,
        },
        DataFusionError, Result, ScalarValue,
    },
    physical_plan::ColumnarValue,
};

/// Spark-style `round(expr, scale)` implementation.
/// - Uses HALF_UP rounding mode (`0.5 → 1`, `-0.5 → -1`)
/// - Supports negative scales (e.g., `round(123.4, -1) = 120`)
/// - Handles Float, Decimal, Int16/32/64
/// - Null-safe
pub fn spark_round(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(
            "spark_round() requires two arguments".to_string(),
        ));
    }

    let value = &args[0];
    let scale_val = &args[1];

    // Parse scale (must be a literal integer)
    let scale = match scale_val {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(n))) => *n,
        ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => *n as i32,
        _ => {
            return Err(DataFusionError::Execution(
                "spark_round() scale must be a literal integer".to_string(),
            ))
        }
    };

    match value {
        // ---------- Array input ----------
        ColumnarValue::Array(arr) => match arr.data_type() {
            DataType::Decimal128(_, _) => {
                let dec_arr = as_decimal128_array(arr)?;
                let precision = dec_arr.precision();
                let in_scale = dec_arr.scale();

                let result = Decimal128Array::from_iter(dec_arr.iter().map(|opt| {
                    opt.map(|v| {
                        let diff = in_scale as i32 - scale;
                        if diff >= 0 {
                            round_i128_half_up(v, -diff)
                        } else {
                            v * 10_i128.pow((-diff) as u32)
                        }
                    })
                }))
                    .with_precision_and_scale(precision, in_scale)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            DataType::Int64 => Ok(ColumnarValue::Array(Arc::new(Int64Array::from_iter(
                as_int64_array(arr)?
                    .iter()
                    .map(|opt| opt.map(|v| round_i128_half_up(v as i128, scale) as i64)),
            )))),

            DataType::Int32 => Ok(ColumnarValue::Array(Arc::new(Int32Array::from_iter(
                as_int32_array(arr)?
                    .iter()
                    .map(|opt| opt.map(|v| round_i128_half_up(v as i128, scale) as i32)),
            )))),

            DataType::Int16 => Ok(ColumnarValue::Array(Arc::new(Int16Array::from_iter(
                as_int16_array(arr)?
                    .iter()
                    .map(|opt| opt.map(|v| round_i128_half_up(v as i128, scale) as i16)),
            )))),

            // Float64 fallback
            _ => {
                let arr = as_float64_array(arr)?;
                let factor = 10_f64.powi(scale);
                let result = Float64Array::from_iter(arr.iter().map(|opt| {
                    opt.map(|v| {
                        if v.is_nan() || v.is_infinite() {
                            v
                        } else {
                            round_half_up_f64(v * factor) / factor
                        }
                    })
                }));
                Ok(ColumnarValue::Array(Arc::new(result)))
            }
        },

        // ---------- Scalar input ----------
        ColumnarValue::Scalar(sv) => {
            if sv.is_null() {
                return Ok(ColumnarValue::Scalar(sv.clone()));
            }

            Ok(match sv {
                ScalarValue::Float64(Some(v)) => {
                    let f = 10_f64.powi(scale);
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(round_half_up_f64(v * f) / f)))
                }
                ScalarValue::Float32(Some(v)) => {
                    let f = 10_f64.powi(scale);
                    ColumnarValue::Scalar(ScalarValue::Float32(Some((round_half_up_f64((*v as f64) * f) / f) as f32)))
                }
                ScalarValue::Int64(Some(v)) => ColumnarValue::Scalar(ScalarValue::Int64(Some(
                    round_i128_half_up(*v as i128, scale) as i64,
                ))),
                ScalarValue::Int32(Some(v)) => ColumnarValue::Scalar(ScalarValue::Int32(Some(
                    round_i128_half_up(*v as i128, scale) as i32,
                ))),
                ScalarValue::Int16(Some(v)) => ColumnarValue::Scalar(ScalarValue::Int16(Some(
                    round_i128_half_up(*v as i128, scale) as i16,
                ))),
                ScalarValue::Decimal128(Some(v), p, s) => ColumnarValue::Scalar(
                    ScalarValue::Decimal128(Some(round_i128_half_up(*v, scale)), *p, *s),
                ),
                _ => {
                    return Err(DataFusionError::Execution(
                        "Unsupported type for spark_round()".to_string(),
                    ))
                }
            })
        }
    }
}

/// Spark-style HALF_UP rounding (0.5 → 1, -0.5 → -1)
fn round_half_up_f64(x: f64) -> f64 {
    if x >= 0.0 {
        (x + 0.5).floor()
    } else {
        (x - 0.5).ceil()
    }
}

/// Integer rounding using Spark's HALF_UP logic without float precision loss
fn round_i128_half_up(value: i128, scale: i32) -> i128 {
    if scale >= 0 {
        return value;
    }
    let factor = 10_i128.pow((-scale) as u32);
    let remainder = value % factor;
    let base = value - remainder;

    if value >= 0 {
        if remainder * 2 >= factor {
            base + factor
        } else {
            base
        }
    } else if remainder.abs() * 2 >= factor {
        base - factor
    } else {
        base
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::{cast::*, Result, ScalarValue};
    use datafusion::physical_plan::ColumnarValue;

    /// Helper to evaluate `spark_round()` and return an f64 scalar result.
    fn eval_f64(value: f64, scale: i32) -> f64 {
        let result = spark_round(&[
            ColumnarValue::Scalar(ScalarValue::Float64(Some(value))),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
        ])
            .unwrap();

        // ✅ Store ArrayRef first to avoid "temporary dropped while borrowed"
        let arr = result.into_array(1).unwrap();
        let out = as_float64_array(&arr).unwrap();
        out.value(0)
    }

    /// Tests Spark-compatible rounding for 16-bit integer (Short).
    #[test]
    fn test_spark_round_short_pi_scales() -> Result<()> {
        let short_pi: i16 = 31415;
        let expected: Vec<i16> = vec![
            0, 0, 30000, 31000, 31400, 31420,
            31415, 31415, 31415, 31415, 31415, 31415, 31415,
        ];

        for (i, scale) in (-6..=6).enumerate() {
            let result = spark_round(&[
                ColumnarValue::Scalar(ScalarValue::Int16(Some(short_pi))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?;

            // ✅ Fixed temporary value issue
            let arr = result.into_array(1)?;
            let out = as_int16_array(&arr)?;
            assert_eq!(out.value(0), expected[i]);
        }
        Ok(())
    }

    /// Tests Spark-compatible rounding for Float32.
    #[test]
    fn test_spark_round_float_pi_scales() -> Result<()> {
        let float_pi = 3.1415_f32;
        let expected = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            3.0, 3.1, 3.14, 3.141,
            3.1415, 3.1415, 3.1415,
        ];

        for (i, scale) in (-6..=6).enumerate() {
            let result = spark_round(&[
                ColumnarValue::Scalar(ScalarValue::Float32(Some(float_pi))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?;

            // ✅ Safe borrow pattern
            let arr = result.into_array(1)?;
            let out = as_float32_array(&arr)?;
            assert!(
                (out.value(0) - expected[i]).abs() < 1e-6,
                "Mismatch at scale {scale}: expected {}, got {}",
                expected[i],
                out.value(0)
            );
        }
        Ok(())
    }

    /// Tests Spark-compatible rounding for Float64 (Double precision).
    #[test]
    fn test_spark_round_double_pi_scales() -> Result<()> {
        let double_pi = std::f64::consts::PI;
        let expected = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
            3.0, 3.1, 3.14, 3.142,
            3.1416, 3.14159, 3.141593,
        ];

        for (i, scale) in (-6..=6).enumerate() {
            let result = spark_round(&[
                ColumnarValue::Scalar(ScalarValue::Float64(Some(double_pi))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?;

            let arr = result.into_array(1)?;
            let out = as_float64_array(&arr)?;
            let actual = out.value(0);
            assert!(
                (actual - expected[i]).abs() < 1e-9,
                "Mismatch at scale {scale}: expected {}, got {}",
                expected[i],
                actual
            );
        }
        Ok(())
    }

    /// Tests Spark-compatible rounding for Int32.
    #[test]
    fn test_spark_round_int_pi_scales() -> Result<()> {
        let int_pi = 314159265_i32;
        let expected = vec![
            314000000, 314200000, 314160000, 314159000, 314159300, 314159270,
            314159265, 314159265, 314159265, 314159265, 314159265, 314159265, 314159265,
        ];

        for (i, scale) in (-6..=6).enumerate() {
            let result = spark_round(&[
                ColumnarValue::Scalar(ScalarValue::Int32(Some(int_pi))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?;

            let arr = result.into_array(1)?;
            let out = as_int32_array(&arr)?;
            assert_eq!(
                out.value(0),
                expected[i],
                "Mismatch at scale {scale}: expected {}, got {}",
                expected[i],
                out.value(0)
            );
        }
        Ok(())
    }

    /// Tests Spark-compatible rounding for Decimal128 (Long in Spark).
    #[test]
    fn test_spark_round_long_pi_scales() -> Result<()> {
        let long_pi = 31415926535897932_i128;
        let expected = vec![
            31415926536000000, 31415926535900000, 31415926535900000,
            31415926535898000, 31415926535897900, 31415926535897930,
            31415926535897932, 31415926535897932, 31415926535897932,
            31415926535897932, 31415926535897932, 31415926535897932, 31415926535897932,
        ];

        for (i, scale) in (-6..=6).enumerate() {
            let result = spark_round(&[
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(long_pi), 38, 0)),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?;

            let arr = result.into_array(1)?;
            let out = as_decimal128_array(&arr)?;
            assert_eq!(
                out.value(0),
                expected[i],
                "Mismatch at scale {scale}: expected {}, got {}",
                expected[i],
                out.value(0)
            );
        }
        Ok(())
    }
}