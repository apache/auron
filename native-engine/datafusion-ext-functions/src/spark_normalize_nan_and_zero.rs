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
    array::{Array, Float32Array, Float64Array, ListArray},
    datatypes::DataType,
};
use datafusion::{
    common::{
        Result, ScalarValue,
        cast::{as_float32_array, as_float64_array},
    },
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{df_execution_err, downcast_any};
use num::Float;

pub fn spark_normalize_nan_and_zero(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Scalar(ScalarValue::List(list)) => {
            let result = spark_normalize_nan_and_zero(&[ColumnarValue::Array(list.clone())])?
                .into_array(1)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        }
        ColumnarValue::Scalar(ScalarValue::Float32(Some(v))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float32(Some(normalize_float(*v))),
        )),
        ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Float64(Some(normalize_float(*v))),
        )),
        ColumnarValue::Scalar(ScalarValue::Float32(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float32(None)))
        }
        ColumnarValue::Scalar(ScalarValue::Float64(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Float64(None)))
        }
        ColumnarValue::Array(array) => match array.as_ref().data_type() {
            DataType::List(field)
                if matches!(field.data_type(), DataType::Float32 | DataType::Float64) =>
            {
                let list = downcast_any!(array, ListArray)?;
                let values = spark_normalize_nan_and_zero(&[ColumnarValue::Array(Arc::clone(
                    list.values(),
                ))])?
                .into_array(list.values().len())?;
                Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
                    Arc::clone(field),
                    list.offsets().clone(),
                    values,
                    list.nulls().cloned(),
                )?)))
            }
            DataType::Float32 => Ok(ColumnarValue::Array(Arc::new(Float32Array::from_iter(
                as_float32_array(array)?
                    .into_iter()
                    .map(|s| s.map(normalize_float)),
            )))),
            DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(Float64Array::from_iter(
                as_float64_array(array)?
                    .into_iter()
                    .map(|s| s.map(normalize_float)),
            )))),
            other_dt => df_execution_err!(
                "spark_normalize_nan_and_zero for Array type only supports Float32/Float64, not {}",
                other_dt
            ),
        },
        other_dt => df_execution_err!(
            "spark_normalize_nan_and_zero only supports Float32/Float64 scalars or Float32/Float64 arrays, not: {:?}",
            other_dt.data_type()
        ),
    }
}

fn normalize_float<T: Float>(v: T) -> T {
    if v.is_nan() {
        T::nan()
    } else if v == T::neg_zero() {
        T::zero()
    } else {
        v
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{Array, ArrayRef, Float32Array, Float64Array, ListArray},
        datatypes::{DataType, Field, Float64Type},
    };
    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use crate::spark_normalize_nan_and_zero::spark_normalize_nan_and_zero;

    #[test]
    fn test_normalize_floating_lists_preserves_slices_and_nulls() -> Result<(), Box<dyn Error>> {
        let doubles: ArrayRef = Arc::new(
            ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                Some(vec![Some(99.0)]),
                Some(vec![
                    Some(-0.0),
                    None,
                    Some(f64::from_bits(0x7ff8_0000_0000_0001)),
                ]),
                Some(vec![]),
                None,
            ])
            .slice(1, 3),
        );
        let floats = arrow::compute::cast(
            &doubles,
            &DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
        )?;
        for input in [doubles, floats] {
            let result = spark_normalize_nan_and_zero(&[ColumnarValue::Array(Arc::clone(&input))])?
                .into_array(3)?;
            let result = result
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("list result");
            let original = input
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("list input");
            assert_eq!(result.offsets(), original.offsets());
            assert_eq!(result.nulls(), original.nulls());
            let row = result.value(0);
            let scalar = spark_normalize_nan_and_zero(&[ColumnarValue::Scalar(
                ScalarValue::List(Arc::new(original.slice(0, 1))),
            )])?;
            assert!(matches!(scalar, ColumnarValue::Scalar(_)));
            assert_eq!(
                &scalar.into_array(1)?,
                &(Arc::new(result.slice(0, 1)) as ArrayRef)
            );
            match row.data_type() {
                DataType::Float32 => {
                    let values = row.as_any().downcast_ref::<Float32Array>().expect("floats");
                    assert_eq!(values.value(0).to_bits(), 0.0f32.to_bits());
                    assert!(values.is_null(1));
                    assert_eq!(values.value(2).to_bits(), f32::NAN.to_bits());
                }
                _ => {
                    let values = row
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("doubles");
                    assert_eq!(values.value(0).to_bits(), 0.0f64.to_bits());
                    assert!(values.is_null(1));
                    assert_eq!(values.value(2).to_bits(), f64::NAN.to_bits());
                }
            }
        }
        Ok(())
    }

    macro_rules! test_normalize_nan_and_zero_array_type {
        ($test_name:ident, $array_type:ty, $nan_literal:expr) => {
            #[test]
            fn $test_name() -> Result<(), Box<dyn Error>> {
                let input_data = vec![
                    Some(12345678.0),
                    Some(-12345678.0),
                    Some(-0.0),
                    Some(0.0),
                    None,
                    Some($nan_literal),
                ];
                let input_columnar_value =
                    ColumnarValue::Array(Arc::new(<$array_type>::from(input_data)));

                let result =
                    spark_normalize_nan_and_zero(&vec![input_columnar_value])?.into_array(6)?;

                let expected_data = vec![
                    Some(12345678.0),
                    Some(-12345678.0),
                    Some(0.0),
                    Some(0.0),
                    None,
                    Some($nan_literal),
                ];
                let expected_array_obj = <$array_type>::from(expected_data);
                let expected: ArrayRef = Arc::new(expected_array_obj);
                assert_eq!(&result, &expected);
                Ok(())
            }
        };
    }

    test_normalize_nan_and_zero_array_type!(
        test_normalize_nan_and_zero_array_f64,
        Float64Array,
        f64::NAN
    );

    test_normalize_nan_and_zero_array_type!(
        test_normalize_nan_and_zero_array_f32,
        Float32Array,
        f32::NAN
    );

    macro_rules! test_normalize_nan_and_zero_scalar_type {
        ($test_name:ident, $scalar_variant:ident, $array_type:ty) => {
            #[test]
            fn $test_name() -> Result<(), Box<dyn Error>> {
                let input_columnar_value =
                    ColumnarValue::Scalar(ScalarValue::$scalar_variant(Some(-0.0)));
                let result =
                    spark_normalize_nan_and_zero(&vec![input_columnar_value])?.into_array(1)?;
                let expected_array = <$array_type>::from(vec![Some(0.0)]);
                let expected: ArrayRef = Arc::new(expected_array);
                assert_eq!(&result, &expected);
                Ok(())
            }
        };
    }

    test_normalize_nan_and_zero_scalar_type!(
        test_normalize_nan_and_zero_scalar_f64,
        Float64,
        Float64Array
    );

    test_normalize_nan_and_zero_scalar_type!(
        test_normalize_nan_and_zero_scalar_f32,
        Float32,
        Float32Array
    );

    macro_rules! test_normalize_nan_and_zero_null_scalar {
        ($test_name:ident, $scalar_variant:ident) => {
            #[test]
            fn $test_name() -> Result<(), Box<dyn Error>> {
                let input_columnar_value =
                    ColumnarValue::Scalar(ScalarValue::$scalar_variant(None));
                let result = spark_normalize_nan_and_zero(&vec![input_columnar_value])?;
                match result {
                    ColumnarValue::Scalar(ScalarValue::$scalar_variant(None)) => Ok(()),
                    other => panic!(
                        "Expected null {} scalar, got: {:?}",
                        stringify!($scalar_variant),
                        other
                    ),
                }
            }
        };
    }

    test_normalize_nan_and_zero_null_scalar!(test_normalize_nan_and_zero_null_scalar_f64, Float64);

    test_normalize_nan_and_zero_null_scalar!(test_normalize_nan_and_zero_null_scalar_f32, Float32);
}
