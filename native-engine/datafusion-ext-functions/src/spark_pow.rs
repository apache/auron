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
    array::{Array, ArrayRef, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array},
    datatypes::DataType,
};
use datafusion::{
    common::{Result, ScalarValue},
    error::DataFusionError,
    physical_plan::ColumnarValue,
};

/// Spark-like `pow` for Short/Int/Long/Float/Double:
/// - Scalar×Scalar:
///     * Convert both to f64; if exponent is integer (i16/i32/i64) use `powi`,
///       otherwise `powf`.
///     * Special case: `0 ** negative` => `+∞` (matches array path and common
///       DB behavior).
///     * Returns `Float64`.
/// - Array×Array:
///     * Convert both arrays to `Vec<Option<f64>>`, compute element-wise, nulls
///       propagate.
///     * Special case: `0 ** negative` => `+∞`.
///     * Returns `Float64Array`.
pub fn spark_pow(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return Err(DataFusionError::Plan(
            "Expected 2 arguments for pow function".to_string(),
        ));
    }

    match (&args[0], &args[1]) {
        (ColumnarValue::Scalar(b), ColumnarValue::Scalar(e)) => scalar_pow(b, e),
        (ColumnarValue::Array(lhs), ColumnarValue::Array(rhs)) => {
            let out = pow_arrays(lhs.as_ref(), rhs.as_ref())?;
            Ok(ColumnarValue::Array(out))
        }
        _ => Err(DataFusionError::Plan(
            "pow expects both arguments to be both scalars or both arrays".into(),
        )),
    }
}

// ----------------------------- Scalar × Scalar -----------------------------

fn scalar_pow(base: &ScalarValue, exp: &ScalarValue) -> Result<ColumnarValue> {
    let b = scalar_to_f64(base)
        .ok_or_else(|| DataFusionError::Plan("Unsupported base type for pow".to_string()))?;

    // integer exponent ⇒ prefer powi (faster/more stable). powi accepts negative
    // i32.
    if let Some(e_i32) = scalar_integer_exponent_i32(exp)? {
        // Special case: 0 ** negative => +∞
        if b == 0.0 && e_i32 < 0 {
            return Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
                f64::INFINITY,
            ))));
        }
        let result = b.powi(e_i32);
        return Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(result))));
    }

    // Floating exponent path
    let e = scalar_to_f64(exp)
        .ok_or_else(|| DataFusionError::Plan("Unsupported exponent type for pow".to_string()))?;

    // Special case: 0 ** negative => +∞
    if b == 0.0 && e < 0.0 {
        return Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
            f64::INFINITY,
        ))));
    }

    let result = b.powf(e);
    Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(result))))
}

/// Convert supported ScalarValue to f64 (lossy for integers by design).
fn scalar_to_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Int16(Some(x)) => Some(*x as f64),
        ScalarValue::Int32(Some(x)) => Some(*x as f64),
        ScalarValue::Int64(Some(x)) => Some(*x as f64),
        ScalarValue::Float32(Some(x)) => Some(*x as f64),
        ScalarValue::Float64(Some(x)) => Some(*x),
        _ => None,
    }
}

/// If `v` is an integer scalar exponent, return it as i32 (error if i64 out of
/// i32 range).
fn scalar_integer_exponent_i32(v: &ScalarValue) -> Result<Option<i32>> {
    let out = match v {
        ScalarValue::Int16(Some(x)) => Some(*x as i32),
        ScalarValue::Int32(Some(x)) => Some(*x),
        ScalarValue::Int64(Some(x)) => {
            let e = *x;
            if e < i32::MIN as i64 || e > i32::MAX as i64 {
                return Err(DataFusionError::Plan(format!(
                    "Exponent {} outside i32 range for powi",
                    e
                )));
            }
            Some(e as i32)
        }
        _ => None,
    };
    Ok(out)
}

// ----------------------------- Array × Array ------------------------------

/// Convert both arrays to f64 vectors and compute element-wise powf.
/// - Returns Float64Array
/// - Nulls propagate
/// - 0 ** negative => +∞
/// - Length must match
fn pow_arrays(lhs: &dyn Array, rhs: &dyn Array) -> Result<ArrayRef> {
    let lvals = to_f64_vec(lhs)?;
    let rvals = to_f64_vec(rhs)?;

    if lvals.len() != rvals.len() {
        return Err(DataFusionError::Plan(format!(
            "Length mismatch in pow: lhs {} vs rhs {}",
            lvals.len(),
            rvals.len()
        )));
    }

    let iter = lvals
        .into_iter()
        .zip(rvals.into_iter())
        .map(|(lo, ro)| match (lo, ro) {
            (Some(l), Some(r)) => {
                if l == 0.0 && r < 0.0 {
                    Some(f64::INFINITY)
                } else {
                    Some(l.powf(r))
                }
            }
            _ => None,
        });

    let out = Float64Array::from_iter(iter);
    Ok(Arc::new(out) as ArrayRef)
}

fn to_f64_vec(arr: &dyn Array) -> Result<Vec<Option<f64>>> {
    match arr.data_type() {
        DataType::Int16 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| DataFusionError::Internal("downcast Int16Array failed".into()))?;
            Ok(a.iter().map(|x| x.map(|v| v as f64)).collect())
        }
        DataType::Int32 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("downcast Int32Array failed".into()))?;
            Ok(a.iter().map(|x| x.map(|v| v as f64)).collect())
        }
        DataType::Int64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| DataFusionError::Internal("downcast Int64Array failed".into()))?;
            Ok(a.iter().map(|x| x.map(|v| v as f64)).collect())
        }
        DataType::Float32 => {
            let a = arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| DataFusionError::Internal("downcast Float32Array failed".into()))?;
            Ok(a.iter().map(|x| x.map(|v| v as f64)).collect())
        }
        DataType::Float64 => {
            let a = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| DataFusionError::Internal("downcast Float64Array failed".into()))?;
            Ok(a.iter().collect()) // already Option<f64>
        }
        other => Err(DataFusionError::Plan(format!(
            "Unsupported array type for pow (to_f64_vec): {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{common::ScalarValue, physical_plan::ColumnarValue};

    use super::*;

    /// Run a scalar×scalar test and assert Float64 with special handling:
    /// - if expected is ±∞, assert actual is the same infinity
    /// - if expected is NaN, assert actual is NaN
    /// - else assert |actual - expected| < eps
    fn run_scalar_test(
        base_value: ScalarValue,
        exponent_value: ScalarValue,
        expected_output: f64,
    ) -> Result<()> {
        let base = ColumnarValue::Scalar(base_value);
        let exponent = ColumnarValue::Scalar(exponent_value);
        let result = spark_pow(&[base, exponent])?;

        let ColumnarValue::Scalar(ScalarValue::Float64(Some(actual))) = result else {
            panic!("Expected Float64 result");
        };

        if expected_output.is_infinite() {
            assert!(
                actual.is_infinite()
                    && actual.is_sign_positive() == expected_output.is_sign_positive(),
                "Expected: {:?}, Actual: {:?}",
                expected_output,
                actual
            );
        } else if expected_output.is_nan() {
            assert!(actual.is_nan(), "Expected NaN, Actual: {:?}", actual);
        } else {
            assert!(
                (actual - expected_output).abs() < 1e-9,
                "Expected: {}, Actual: {}",
                expected_output,
                actual
            );
        }
        Ok(())
    }

    #[test]
    fn test_pow_float64_and_float64() -> Result<()> {
        run_scalar_test(
            ScalarValue::Float64(Some(2.0)),
            ScalarValue::Float64(Some(3.0)),
            8.0,
        )
    }

    #[test]
    fn test_pow_int32_and_float64() -> Result<()> {
        run_scalar_test(
            ScalarValue::Int32(Some(2)),
            ScalarValue::Float64(Some(3.0)),
            8.0,
        )
    }

    #[test]
    fn test_pow_float64_and_int32() -> Result<()> {
        run_scalar_test(
            ScalarValue::Float64(Some(2.0)),
            ScalarValue::Int32(Some(3)),
            8.0,
        )
    }

    #[test]
    fn test_pow_int32_and_int32() -> Result<()> {
        run_scalar_test(
            ScalarValue::Int32(Some(2)),
            ScalarValue::Int32(Some(3)),
            8.0,
        )
    }

    #[test]
    fn test_pow_array_array_float64() -> Result<()> {
        let base = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(3.0),
            Some(4.0),
        ])));
        let exponent = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(2.0),
            Some(2.0),
            Some(2.0),
        ])));
        let result = spark_pow(&[base, exponent])?.into_array(3)?;
        let expected: ArrayRef =
            Arc::new(Float64Array::from(vec![Some(4.0), Some(9.0), Some(16.0)]));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_scalar_zero_pow_negative_float() -> Result<()> {
        // 0.0 ** (-2.5) => +∞
        run_scalar_test(
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some(-2.5)),
            f64::INFINITY,
        )
    }

    #[test]
    fn test_scalar_zero_pow_negative_int() -> Result<()> {
        // 0.0 ** (-3) => +∞ (powi path)
        run_scalar_test(
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Int32(Some(-3)),
            f64::INFINITY,
        )
    }

    #[test]
    fn test_array_zero_pow_negative() -> Result<()> {
        let base = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(2.0),
            None,
        ])));
        let exponent = ColumnarValue::Array(Arc::new(Float64Array::from(vec![
            Some(-1.0),
            Some(3.0),
            Some(2.0),
        ])));
        let result = spark_pow(&[base, exponent])?.into_array(3)?;
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::INFINITY),
            Some(8.0),
            None,
        ]));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_null_propagation() -> Result<()> {
        let base = ColumnarValue::Array(Arc::new(Int32Array::from(vec![Some(2), None, Some(3)])));
        let exponent = ColumnarValue::Array(Arc::new(Float32Array::from(vec![
            Some(2.0),
            Some(4.0),
            None,
        ])));
        let result = spark_pow(&[base, exponent])?.into_array(3)?;
        // 2^2 => 4; nulls propagate
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(4.0), None, None]));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_length_mismatch_error() {
        let base = ColumnarValue::Array(Arc::new(Int16Array::from(vec![Some(2), Some(3)])));
        let exponent = ColumnarValue::Array(Arc::new(Int16Array::from(vec![Some(2)])));
        let err = spark_pow(&[base, exponent]).unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("Length mismatch"), "unexpected error: {}", msg);
    }

    #[test]
    fn test_scalar_i64_exponent_out_of_i32_range() {
        // exponent = i64::MAX should error (powi requires i32)
        let base = ColumnarValue::Scalar(ScalarValue::Float64(Some(2.0)));
        let exponent = ColumnarValue::Scalar(ScalarValue::Int64(Some(i64::MAX)));
        let err = spark_pow(&[base, exponent]).unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("outside i32 range"),
            "unexpected error: {}",
            msg
        );
    }
}
