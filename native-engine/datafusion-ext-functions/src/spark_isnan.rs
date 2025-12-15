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
    array::{Array, BooleanArray, Float32Array, Float64Array},
    datatypes::DataType,
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::arrow::boolean::nulls_to_false;

pub fn spark_isnan(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let value = &args[0];
    match value {
        ColumnarValue::Array(array) => match array.data_type() {
            DataType::Float64 => {
                let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                let cleaned = nulls_to_false(&is_nan);
                Ok(ColumnarValue::Array(Arc::new(cleaned)))
            }
            DataType::Float32 => {
                let array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let is_nan = BooleanArray::from_unary(array, |x| x.is_nan());
                let cleaned = nulls_to_false(&is_nan);
                Ok(ColumnarValue::Array(Arc::new(cleaned)))
            }
            _other => {
                // For non-float arrays, Spark's isnan is effectively false.
                let len = array.len();
                let out = ScalarValue::Boolean(Some(false)).to_array_of_size(len)?;
                Ok(ColumnarValue::Array(out))
            }
        },
        ColumnarValue::Scalar(sv) => Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(
            match sv {
                ScalarValue::Float64(a) => a.map(|x| x.is_nan()).unwrap_or(false),
                ScalarValue::Float32(a) => a.map(|x| x.is_nan()).unwrap_or(false),
                _ => false,
            },
        )))),
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::array::{ArrayRef, BooleanArray, Float32Array, Float64Array};
    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_isnan::spark_isnan;

    #[test]
    fn test_isnan_array_f64() -> Result<(), Box<dyn Error>> {
        let input_data = vec![
            Some(12345678.0),
            Some(f64::NAN),
            Some(-0.0),
            None,
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
        ];
        let input_columnar_value = ColumnarValue::Array(Arc::new(Float64Array::from(input_data)));

        let result = spark_isnan(&vec![input_columnar_value])?.into_array(6)?;

        let expected_data = vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false), // null returns false in Spark
            Some(false),
            Some(false),
        ];
        let expected: ArrayRef = Arc::new(BooleanArray::from(expected_data));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_isnan_array_f32() -> Result<(), Box<dyn Error>> {
        let input_data = vec![
            Some(12345678.0f32),
            Some(f32::NAN),
            Some(-0.0f32),
            None,
            Some(f32::INFINITY),
            Some(f32::NEG_INFINITY),
        ];
        let input_columnar_value = ColumnarValue::Array(Arc::new(Float32Array::from(input_data)));

        let result = spark_isnan(&vec![input_columnar_value])?.into_array(6)?;

        let expected_data = vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false), // null returns false in Spark
            Some(false),
            Some(false),
        ];
        let expected: ArrayRef = Arc::new(BooleanArray::from(expected_data));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_isnan_scalar_f64_nan() -> Result<(), Box<dyn Error>> {
        let input_columnar_value = ColumnarValue::Scalar(ScalarValue::Float64(Some(f64::NAN)));
        let result = spark_isnan(&vec![input_columnar_value])?.into_array(1)?;
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true)]));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_isnan_scalar_f64_null() -> Result<(), Box<dyn Error>> {
        let input_columnar_value = ColumnarValue::Scalar(ScalarValue::Float64(None));
        let result = spark_isnan(&vec![input_columnar_value])?.into_array(1)?;
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false)]));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_isnan_scalar_f32_null() -> Result<(), Box<dyn Error>> {
        let input_columnar_value = ColumnarValue::Scalar(ScalarValue::Float32(None));
        let result = spark_isnan(&vec![input_columnar_value])?.into_array(1)?;
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false)]));
        assert_eq!(&result, &expected);
        Ok(())
    }
}
