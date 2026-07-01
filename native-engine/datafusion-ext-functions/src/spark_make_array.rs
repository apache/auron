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

//! Array expressions

use std::sync::Arc;

use arrow::{
    array::*,
    datatypes::{DataType, Field, Fields},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{arrow::cast::cast, df_execution_err};

macro_rules! downcast_vec {
    ($ARGS:expr, $ARRAY_TYPE:ident) => {{
        $ARGS
            .iter()
            .map(|e| match e.as_any().downcast_ref::<$ARRAY_TYPE>() {
                Some(array) => Ok(array),
                _ => df_execution_err!("failed to downcast"),
            })
    }};
}

macro_rules! new_builder {
    (BooleanBuilder, $len:expr) => {
        BooleanBuilder::with_capacity($len)
    };
    (StringBuilder, $len:expr) => {
        StringBuilder::new()
    };
    (LargeStringBuilder, $len:expr) => {
        LargeStringBuilder::new()
    };
    ($el:ident, $len:expr) => {{ <$el>::with_capacity($len) }};
}

macro_rules! array {
    ($ARGS:expr, $ARRAY_TYPE:ident, $BUILDER_TYPE:ident) => {{
        // compute number of rows
        let num_rows = $ARGS
            .iter()
            .map(|arg| arg.len())
            .filter(|&len| len != 1) // may be result of scalar.to_array()
            .next()
            .unwrap_or(1);
        if $ARGS
            .iter()
            .any(|arg| arg.len() != 1 && arg.len() != num_rows)
        {
            df_execution_err!("all columns of array must have the same length")?;
        }

        // downcast all arguments to their common format
        let args = downcast_vec!($ARGS, $ARRAY_TYPE).collect::<Result<Vec<&$ARRAY_TYPE>>>()?;

        let builder = new_builder!($BUILDER_TYPE, args[0].len());
        let mut builder = ListBuilder::<$BUILDER_TYPE>::new(builder);
        // for each entry in the array
        for index in 0..num_rows {
            for arg in &args {
                let index = index.min(arg.len() - 1); // handles result of scalar.to_array()
                if arg.is_null(index) {
                    builder.values().append_null();
                } else {
                    builder.values().append_value(arg.value(index));
                }
            }
            builder.append(true);
        }
        Arc::new(builder.finish())
    }};
}

fn array_array(args: &[ArrayRef]) -> Result<ArrayRef> {
    // do not accept 0 arguments.
    if args.is_empty() {
        df_execution_err!("array requires at least one argument")?;
    }

    let res = match args[0].data_type() {
        DataType::Utf8 => array!(args, StringArray, StringBuilder),
        DataType::LargeUtf8 => array!(args, LargeStringArray, LargeStringBuilder),
        DataType::Boolean => array!(args, BooleanArray, BooleanBuilder),
        DataType::Float32 => array!(args, Float32Array, Float32Builder),
        DataType::Float64 => array!(args, Float64Array, Float64Builder),
        DataType::Int8 => array!(args, Int8Array, Int8Builder),
        DataType::Int16 => array!(args, Int16Array, Int16Builder),
        DataType::Int32 => array!(args, Int32Array, Int32Builder),
        DataType::Int64 => array!(args, Int64Array, Int64Builder),
        DataType::UInt8 => array!(args, UInt8Array, UInt8Builder),
        DataType::UInt16 => array!(args, UInt16Array, UInt16Builder),
        DataType::UInt32 => array!(args, UInt32Array, UInt32Builder),
        DataType::UInt64 => array!(args, UInt64Array, UInt64Builder),
        _ => {
            // naive implementation with scalar values
            let num_rows = args[0].len();
            let data_type = common_array_element_data_type(args)?;
            let args = args
                .iter()
                .map(|arg| {
                    if arg.data_type() == &data_type {
                        Ok(arg.clone())
                    } else {
                        cast(arg.as_ref(), &data_type)
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let mut output_scalars = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                let row_scalars: Vec<ScalarValue> = args
                    .iter()
                    .map(|arg| ScalarValue::try_from_array(arg, i))
                    .collect::<Result<_>>()?;
                output_scalars.push(ScalarValue::List(ScalarValue::new_list(
                    &row_scalars,
                    &data_type,
                    true,
                )));
            }
            ScalarValue::iter_to_array(output_scalars)?
        }
    };
    Ok(res)
}

fn common_array_element_data_type(args: &[ArrayRef]) -> Result<DataType> {
    let mut data_type = args[0].data_type().clone();
    for arg in &args[1..] {
        data_type = widen_data_type(&data_type, arg.data_type())?;
    }
    Ok(data_type)
}

fn widen_data_type(left: &DataType, right: &DataType) -> Result<DataType> {
    if left == right {
        return Ok(left.clone());
    }

    match (left, right) {
        (DataType::List(left_field), DataType::List(right_field)) => {
            let item_type = widen_data_type(left_field.data_type(), right_field.data_type())?;
            Ok(DataType::List(Arc::new(Field::new(
                left_field.name().as_str(),
                item_type,
                left_field.is_nullable() || right_field.is_nullable(),
            ))))
        }
        (DataType::Struct(left_fields), DataType::Struct(right_fields))
            if left_fields.len() == right_fields.len() =>
        {
            let fields = left_fields
                .iter()
                .zip(right_fields.iter())
                .map(|(left_field, right_field)| {
                    if left_field.name() != right_field.name() {
                        return df_execution_err!(
                            "array child struct fields must have same names, got {} and {}",
                            left_field.name(),
                            right_field.name()
                        );
                    }
                    Ok(Arc::new(Field::new(
                        left_field.name().as_str(),
                        widen_data_type(left_field.data_type(), right_field.data_type())?,
                        left_field.is_nullable() || right_field.is_nullable(),
                    )))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(DataType::Struct(Fields::from(fields)))
        }
        _ => df_execution_err!(
            "array child values must have same data type, got {left:?} and {right:?}"
        ),
    }
}

/// put values in an array.
pub fn array(values: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arrays: Vec<ArrayRef> = values
        .iter()
        .map(|x| {
            Ok(match x {
                ColumnarValue::Array(array) => array.clone(),
                ColumnarValue::Scalar(scalar) => scalar.to_array()?.clone(),
            })
        })
        .collect::<Result<_>>()?;
    Ok(ColumnarValue::Array(array_array(arrays.as_slice())?))
}
#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{ArrayRef, Int32Array, ListArray},
        buffer::{OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field, Float32Type, Int32Type},
    };
    use datafusion::{common::ScalarValue, physical_plan::ColumnarValue};

    use crate::spark_make_array::array;

    #[test]
    fn test_make_array_int() -> Result<(), Box<dyn Error>> {
        let result = array(&vec![ColumnarValue::Array(Arc::new(Int32Array::from(
            vec![Some(12), Some(-123), Some(0), Some(9), None],
        )))])?
        .into_array(5)?;

        let expected = vec![
            Some(vec![Some(12)]),
            Some(vec![Some(-123)]),
            Some(vec![Some(0)]),
            Some(vec![Some(9)]),
            Some(vec![None]),
        ];
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(expected);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_make_array_int_mixed_params() -> Result<(), Box<dyn Error>> {
        let result = array(&vec![
            ColumnarValue::Scalar(ScalarValue::from(123456)),
            ColumnarValue::Array(Arc::new(Int32Array::from(vec![
                Some(12),
                Some(-123),
                Some(0),
                Some(9),
                None,
            ]))),
        ])?
        .into_array(5)?;

        let expected = vec![
            Some(vec![Some(123456), Some(12)]),
            Some(vec![Some(123456), Some(-123)]),
            Some(vec![Some(123456), Some(0)]),
            Some(vec![Some(123456), Some(9)]),
            Some(vec![Some(123456), None]),
        ];
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(expected);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_make_array_float() -> Result<(), Box<dyn Error>> {
        let result = array(&vec![
            ColumnarValue::Scalar(ScalarValue::Float32(Some(2.2))),
            ColumnarValue::Scalar(ScalarValue::Float32(Some(-2.3))),
        ])?
        .into_array(2)?;

        let expected = vec![Some(vec![Some(2.2), Some(-2.3)])];
        let expected = ListArray::from_iter_primitive::<Float32Type, _, _>(expected);
        let expected: ArrayRef = Arc::new(expected);

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_make_array_with_mixed_child_array_nullability() -> Result<(), Box<dyn Error>> {
        let non_nullable_items: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let non_nullable_list: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Int32, false)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            non_nullable_items,
            None,
        )?);

        let nullable_items: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(3)]));
        let nullable_list: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2])),
            nullable_items,
            None,
        )?);

        let result = array(&vec![
            ColumnarValue::Array(non_nullable_list),
            ColumnarValue::Array(nullable_list),
        ])?
        .into_array(2)?;

        let expected_values: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(2), Some(3)])),
            None,
        )?);
        let expected: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(
                expected_values.data_type().clone(),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4])),
            expected_values,
            None,
        )?);

        assert_eq!(&result, &expected);
        Ok(())
    }
}
