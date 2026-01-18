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
    array::{Decimal128Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array},
    datatypes::DataType,
};
use datafusion::{
    common::{
        DataFusionError, Result, ScalarValue,
        cast::{
            as_decimal128_array, as_float32_array, as_float64_array, as_int16_array,
            as_int32_array, as_int64_array,
        },
    },
    physical_plan::ColumnarValue,
};

/// Spark-style `bround(expr, scale)` implementation (HALF_EVEN).
/// - HALF_EVEN (banker's rounding): ties go to the nearest even
/// - Supports negative scales (e.g., bround(123.4, -1) = 120)
/// - Handles Float, Decimal, Int16/32/64
/// - Null-safe
pub fn spark_bround(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Execution(
            "spark_bround() requires two arguments".to_string(),
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
                "spark_bround() scale must be a literal integer".to_string(),
            ));
        }
    };

    match value {
        // ---------- Array input ----------
        ColumnarValue::Array(arr) => match arr.data_type() {
            DataType::Decimal128(..) => {
                let dec_arr = as_decimal128_array(arr)?;
                let precision = dec_arr.precision();
                let in_scale = dec_arr.scale();

                let result = Decimal128Array::from_iter(dec_arr.iter().map(|opt| {
                    opt.map(|v| {
                        let diff = in_scale as i32 - scale;
                        if diff >= 0 {
                            // reduce fractional digits by diff using HALF_EVEN
                            round_i128_half_even(v, -diff)
                        } else {
                            // increasing scale (more fractional digits): multiply
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
                    .map(|opt| opt.map(|v| round_i128_half_even(v as i128, scale) as i64)),
            )))),

            DataType::Int32 => Ok(ColumnarValue::Array(Arc::new(Int32Array::from_iter(
                as_int32_array(arr)?
                    .iter()
                    .map(|opt| opt.map(|v| round_i128_half_even(v as i128, scale) as i32)),
            )))),

            DataType::Int16 => Ok(ColumnarValue::Array(Arc::new(Int16Array::from_iter(
                as_int16_array(arr)?
                    .iter()
                    .map(|opt| opt.map(|v| round_i128_half_even(v as i128, scale) as i16)),
            )))),

            DataType::Float32 => {
                let arr = as_float32_array(arr)?;
                let factor = 10_f32.powi(scale);
                let result = Float32Array::from_iter(arr.iter().map(|opt| {
                    opt.map(|v| {
                        if v.is_nan() || v.is_infinite() {
                            v
                        } else {
                            round_half_even_f32(v * factor) / factor
                        }
                    })
                }));
                Ok(ColumnarValue::Array(Arc::new(result)))
            }

            // Float64 fallback
            _ => {
                let arr = as_float64_array(arr)?;
                let factor = 10_f64.powi(scale);
                let result = Float64Array::from_iter(arr.iter().map(|opt| {
                    opt.map(|v| {
                        if v.is_nan() || v.is_infinite() {
                            v
                        } else {
                            round_half_even_f64(v * factor) / factor
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
                    ColumnarValue::Scalar(ScalarValue::Float64(Some(
                        round_half_even_f64(v * f) / f,
                    )))
                }
                ScalarValue::Float32(Some(v)) => {
                    let f = 10_f64.powi(scale);
                    ColumnarValue::Scalar(ScalarValue::Float32(Some(
                        (round_half_even_f64((*v as f64) * f) / f) as f32,
                    )))
                }
                ScalarValue::Int64(Some(v)) => ColumnarValue::Scalar(ScalarValue::Int64(Some(
                    round_i128_half_even(*v as i128, scale) as i64,
                ))),
                ScalarValue::Int32(Some(v)) => ColumnarValue::Scalar(ScalarValue::Int32(Some(
                    round_i128_half_even(*v as i128, scale) as i32,
                ))),
                ScalarValue::Int16(Some(v)) => ColumnarValue::Scalar(ScalarValue::Int16(Some(
                    round_i128_half_even(*v as i128, scale) as i16,
                ))),
                ScalarValue::Decimal128(Some(v), p, s) => ColumnarValue::Scalar(
                    ScalarValue::Decimal128(Some(round_i128_half_even(*v, scale)), *p, *s),
                ),
                _ => {
                    return Err(DataFusionError::Execution(
                        "Unsupported type for spark_bround()".to_string(),
                    ));
                }
            })
        }
    }
}

/// HALF_EVEN for f64: ties go to nearest even integer
fn round_half_even_f64(x: f64) -> f64 {
    if x.is_nan() || x.is_infinite() {
        return x;
    }
    let sign = x.signum();
    let ax = x.abs();
    let f = ax.floor();
    let diff = ax - f;

    let rounded = if diff > 0.5 {
        f + 1.0
    } else if diff < 0.5 {
        f
    } else {
        // tie: choose the even integer
        if ((f as i64) & 1) == 0 { f } else { f + 1.0 }
    };

    rounded.copysign(sign)
}

/// HALF_EVEN for f32
fn round_half_even_f32(x: f32) -> f32 {
    if x.is_nan() || x.is_infinite() {
        return x;
    }
    let sign = x.signum();
    let ax = x.abs();
    let f = ax.floor();
    let diff = ax - f;

    let rounded = if diff > 0.5 {
        f + 1.0
    } else if diff < 0.5 {
        f
    } else if ((f as i32) & 1) == 0 {
        f
    } else {
        f + 1.0
    };

    rounded.copysign(sign)
}

/// Integer rounding using Spark's HALF_EVEN logic without float precision loss.
/// `scale < 0` means rounding to tens/hundreds/... (10^(-scale)).
fn round_i128_half_even(value: i128, scale: i32) -> i128 {
    if scale >= 0 {
        return value;
    }
    let factor = 10_i128.pow((-scale) as u32);
    let remainder = value % factor;
    let base = value - remainder;
    let twice = remainder.abs() * 2;

    if twice > factor {
        if value >= 0 {
            base + factor
        } else {
            base - factor
        }
    } else if twice < factor {
        base
    } else {
        // tie: choose the even multiple of `factor`
        let q = base / factor; // exact integer
        if q % 2 == 0 {
            base
        } else if value >= 0 {
            base + factor
        } else {
            base - factor
        }
    }
}

#[cfg(test)]
mod bround_tests {
    use datafusion::{common::Result, physical_plan::ColumnarValue};

    use super::*;

    // Test: float64 data type, check "HALF_EVEN" rounding rule (banker's rounding)
    #[test]
    fn test_bround_float64_ties() -> Result<()> {
        let arr = Arc::new(Float64Array::from(vec![
            Some(1.5),
            Some(2.5),
            Some(-0.5),
            Some(-1.5),
            Some(0.5000000000),
        ]));
        let out = spark_bround(&[
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(0))),
        ])?
        .into_array(5)?;
        let a = as_float64_array(&out)?;
        let v: Vec<_> = a.iter().collect();
        // HALF_EVEN: 1.5→2, 2.5→2, -0.5→0, -1.5→-2, 0.5→0
        assert_eq!(
            v,
            vec![Some(2.0), Some(2.0), Some(0.0), Some(-2.0), Some(0.0)]
        );
        Ok(())
    }

    // Test: float64 data type, handle negative scale values
    #[test]
    fn test_bround_negative_scale_float() -> Result<()> {
        let arr = Arc::new(Float64Array::from(vec![
            Some(125.0),
            Some(135.0),
            Some(145.0),
            Some(155.0),
        ]));
        let out = spark_bround(&[
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),
        ])?
        .into_array(4)?;
        let a = as_float64_array(&out)?;
        let v: Vec<_> = a.iter().collect();
        assert_eq!(v, vec![Some(120.0), Some(140.0), Some(140.0), Some(160.0)]);
        Ok(())
    }

    // Test: bround on Decimal array
    #[test]
    fn test_bround_decimal_array() -> Result<()> {
        let arr = Arc::new(
            Decimal128Array::from_iter_values([12345_i128, 67895_i128])
                .with_precision_and_scale(10, 2)?,
        );
        let out = spark_bround(&[
            ColumnarValue::Array(arr.clone()),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ])?
        .into_array(2)?;
        let dec = as_decimal128_array(&out)?;
        let vals: Vec<_> = dec.iter().collect();
        assert_eq!(vals, vec![Some(12340_i128), Some(67900_i128)]);
        Ok(())
    }

    /// scales = -6..=6
    fn scales_range() -> impl Iterator<Item = i32> {
        -6..=6
    }

    // Test: double data type π value across different scales
    #[test]
    fn test_bround_double_pi_scales() -> Result<()> {
        let double_pi = std::f64::consts::PI;
        let expected = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 3.1, 3.14, 3.142, 3.1416, 3.14159, 3.141593,
        ];

        for (i, scale) in scales_range().enumerate() {
            let out = spark_bround(&[
                ColumnarValue::Scalar(ScalarValue::Float64(Some(double_pi))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?
            .into_array(1)?;
            let arr = as_float64_array(&out)?;
            let actual = arr.value(0);
            assert!(
                (actual - expected[i]).abs() < 1e-9,
                "scale={scale}: expected {}, got {}",
                expected[i],
                actual
            );
        }
        Ok(())
    }

    // Test: float data type π value across different scales
    #[test]
    fn test_bround_float_pi_scales() -> Result<()> {
        let float_pi = 3.1415_f32;
        let expected = vec![
            0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 3.0, 3.1, 3.14, 3.141, 3.1415, 3.1415, 3.1415,
        ];

        for (i, scale) in scales_range().enumerate() {
            let out = spark_bround(&[
                ColumnarValue::Scalar(ScalarValue::Float32(Some(float_pi))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?
            .into_array(1)?;
            let arr = as_float32_array(&out)?;
            let actual = arr.value(0);
            assert!(
                (actual - expected[i]).abs() < 1e-6,
                "scale={scale}: expected {}, got {}",
                expected[i],
                actual
            );
        }
        Ok(())
    }

    // Test: short data type π value across different scales
    #[test]
    fn test_bround_short_pi_scales() -> Result<()> {
        let short_pi: i16 = 31415;
        let expected: Vec<i16> = vec![0, 0, 30000, 31000, 31400, 31420]
            .into_iter()
            .chain(std::iter::repeat(31415).take(7))
            .collect();

        for (i, scale) in scales_range().enumerate() {
            let out = spark_bround(&[
                ColumnarValue::Scalar(ScalarValue::Int16(Some(short_pi))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?
            .into_array(1)?;
            let arr = as_int16_array(&out)?;
            assert_eq!(
                arr.value(0),
                expected[i],
                "scale={scale}: expected {}, got {}",
                expected[i],
                arr.value(0)
            );
        }
        Ok(())
    }

    // Test: int data type π value across different scales
    #[test]
    fn test_bround_int_pi_scales() -> Result<()> {
        let int_pi: i32 = 314_159_265;
        let expected: Vec<i32> = vec![
            314000000, 314200000, 314160000, 314159000, 314159300, 314159260,
        ]
        .into_iter()
        .chain(std::iter::repeat(314_159_265).take(7))
        .collect();

        for (i, scale) in scales_range().enumerate() {
            let out = spark_bround(&[
                ColumnarValue::Scalar(ScalarValue::Int32(Some(int_pi))),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?
            .into_array(1)?;
            let arr = as_int32_array(&out)?;
            assert_eq!(
                arr.value(0),
                expected[i],
                "scale={scale}: expected {}, got {}",
                expected[i],
                arr.value(0)
            );
        }
        Ok(())
    }

    // Test: long data type π value across different scales
    #[test]
    fn test_bround_long_pi_scales() -> Result<()> {
        let long_pi: i128 = 31_415_926_535_897_932_i128;
        let expected: Vec<i128> = vec![
            31_415_926_536_000_000,
            31_415_926_535_900_000,
            31_415_926_535_900_000,
            31_415_926_535_898_000,
            31_415_926_535_897_900,
            31_415_926_535_897_930,
        ]
        .into_iter()
        .chain(std::iter::repeat(31_415_926_535_897_932_i128).take(7))
        .collect();

        for (i, scale) in scales_range().enumerate() {
            let out = spark_bround(&[
                ColumnarValue::Scalar(ScalarValue::Decimal128(Some(long_pi), 38, 0)),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?
            .into_array(1)?;
            let arr = as_decimal128_array(&out)?;
            assert_eq!(
                arr.value(0),
                expected[i],
                "scale={scale}: expected {}, got {}",
                expected[i],
                arr.value(0)
            );
        }
        Ok(())
    }

    // Test: bround on Decimal array when scale is less than or equal to the
    // original scale
    #[test]
    fn test_bround_decimal_array_scale_le_in_scale() -> Result<()> {
        let arr = Arc::new(
            Decimal128Array::from_iter_values([12345_i128, 67895_i128])
                .with_precision_and_scale(10, 2)?,
        );
        let out = spark_bround(&[
            ColumnarValue::Array(arr),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
        ])?
        .into_array(2)?;
        let a = as_decimal128_array(&out)?;
        let vals: Vec<_> = a.iter().collect();
        assert_eq!(vals, vec![Some(12340_i128), Some(67900_i128)]);
        Ok(())
    }

    // Test: bround with "HALF_EVEN" rounding (banker's rounding) for both ties and
    // signs
    #[test]
    fn test_bround_half_even_ties_and_signs() -> Result<()> {
        let cases = vec![
            (ScalarValue::Float64(Some(2.5)), 0, 2.0),
            (ScalarValue::Float64(Some(3.5)), 0, 4.0),
            (ScalarValue::Float64(Some(-2.5)), 0, -2.0),
            (ScalarValue::Float64(Some(-3.5)), 0, -4.0),
            (ScalarValue::Float64(Some(-0.35)), 1, -0.4),
            (ScalarValue::Float64(Some(-35.0)), -1, -40.0),
        ];

        for (sv, scale, expected) in cases {
            let out = spark_bround(&[
                ColumnarValue::Scalar(sv),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(scale))),
            ])?
            .into_array(1)?;
            let a = as_float64_array(&out)?;
            assert!(
                (a.value(0) - expected).abs() < 1e-12,
                "scale={scale}: expected {}, got {}",
                expected,
                a.value(0)
            );
        }
        Ok(())
    }
}
