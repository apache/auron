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
    common::{Result, ScalarValue, cast::as_float64_array},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

/// Spark coerces sqrt inputs to DoubleType and returns NaN for negative values.
pub fn spark_sqrt(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float64(value.map(f64::sqrt)),
        )),
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(
            Float64Array::from_unary(as_float64_array(array)?, f64::sqrt),
        ))),
        other => df_execution_err!("spark_sqrt expects Float64, got {}", other.data_type()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Float64Array};
    use datafusion::{
        common::{Result, ScalarValue},
        logical_expr::ColumnarValue,
    };

    use crate::create_auron_ext_function;

    #[test]
    fn test_sqrt_scalar_and_array() -> Result<()> {
        let sqrt = create_auron_ext_function("Spark_Sqrt", 0)?;
        let cases = [
            (Some(-1.0), Some(f64::NAN)),
            (Some(4.0), Some(2.0)),
            (Some(0.0), Some(0.0)),
            (Some(-0.0), Some(-0.0)),
            (None, None),
            (Some(f64::NAN), Some(f64::NAN)),
            (Some(f64::INFINITY), Some(f64::INFINITY)),
            (Some(f64::NEG_INFINITY), Some(f64::NAN)),
        ];
        for (input, expected) in cases {
            let result = sqrt(&[ColumnarValue::Scalar(ScalarValue::Float64(input))])?;
            let ColumnarValue::Scalar(ScalarValue::Float64(actual)) = result else {
                unreachable!("expected a Float64 scalar");
            };
            assert_value(actual, expected);
        }

        let inputs = Float64Array::from_iter(cases.iter().map(|(input, _)| *input));
        let result = sqrt(&[ColumnarValue::Array(Arc::new(inputs))])?.into_array(cases.len())?;
        let actual = result
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 array");
        for (actual, (_, expected)) in actual.iter().zip(cases) {
            assert_value(actual, expected);
        }
        Ok(())
    }

    fn assert_value(actual: Option<f64>, expected: Option<f64>) {
        match (actual, expected) {
            (Some(actual), Some(expected)) if expected.is_nan() => assert!(actual.is_nan()),
            (Some(actual), Some(expected)) => assert_eq!(actual.to_bits(), expected.to_bits()),
            (actual, expected) => assert_eq!(actual, expected),
        }
    }
}
