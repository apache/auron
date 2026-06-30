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
    array::{Array, ArrayRef, ListArray, MutableArrayData, make_array},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{
    df_execution_err, downcast_any, scalar_value::compacted_scalar_value_from_array,
};

pub fn array_reverse(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return df_execution_err!("array_reverse requires exactly 1 argument");
    }
    match &args[0] {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(reverse_list_array(
            downcast_any!(array, ListArray)?,
        )?)),
        ColumnarValue::Scalar(scalar) if scalar.is_null() => {
            Ok(ColumnarValue::Scalar(scalar.clone()))
        }
        ColumnarValue::Scalar(scalar) => match scalar.data_type() {
            DataType::List(_) => {
                let array = scalar.to_array()?;
                let reversed = reverse_list_array(downcast_any!(&array, ListArray)?)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &reversed, 0,
                )?))
            }
            data_type => df_execution_err!("array_reverse only supports list, got {data_type:?}"),
        },
    }
}

fn reverse_list_array(array: &ListArray) -> Result<ArrayRef> {
    let DataType::List(field) = array.data_type() else {
        return df_execution_err!(
            "array_reverse only supports list, got {:?}",
            array.data_type()
        );
    };

    let mut values = Vec::with_capacity(array.len());
    for row_idx in 0..array.len() {
        if array.is_null(row_idx) {
            values.push(ScalarValue::List(Arc::new(ListArray::new_null(
                field.clone(),
                1,
            ))));
        } else {
            let row = array.value(row_idx);
            let mut reversed = Vec::with_capacity(row.len());
            for value_idx in (0..row.len()).rev() {
                reversed.push(compacted_scalar_value_from_array(&row, value_idx)?);
            }
            values.push(ScalarValue::List(ScalarValue::new_list(
                &reversed,
                field.data_type(),
                field.is_nullable(),
            )));
        }
    }

    ScalarValue::iter_to_array(values)
}

fn as_list_array(array: &ArrayRef) -> Result<ListArray> {
    downcast_any!(array, ListArray).cloned()
}

fn columnar_value_to_list_array(arg: &ColumnarValue) -> Result<ListArray> {
    match arg {
        ColumnarValue::Array(array) if matches!(array.data_type(), DataType::Null) => {
            Ok(ListArray::new_null(
                Arc::new(Field::new_list_field(DataType::Null, true)),
                array.len(),
            ))
        }
        ColumnarValue::Array(array) => as_list_array(array),
        ColumnarValue::Scalar(scalar) if scalar.is_null() => {
            let list_field = match scalar.data_type() {
                DataType::List(field) => field,
                _ => Arc::new(Field::new_list_field(DataType::Null, true)),
            };
            Ok(ListArray::new_null(list_field, 1))
        }
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;
            as_list_array(&array)
        }
    }
}

/// Flatten an array of arrays into a single array.
pub fn array_flatten(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        df_execution_err!("array_flatten expects one argument, got {}", args.len())?;
    }

    let outer = columnar_value_to_list_array(&args[0])?;
    let inner = as_list_array(&outer.values())?;
    let inner_values = inner.values();
    let inner_values_data = inner_values.to_data();
    let mut mutable = MutableArrayData::new(vec![&inner_values_data], true, inner_values.len());

    let mut offsets = Vec::with_capacity(outer.len() + 1);
    let mut valids = Vec::with_capacity(outer.len());
    let mut offset = 0i32;
    offsets.push(offset);

    for row_idx in 0..outer.len() {
        if outer.is_null(row_idx) {
            valids.push(false);
            offsets.push(offset);
            continue;
        }

        let outer_start = outer.value_offsets()[row_idx] as usize;
        let outer_end = outer.value_offsets()[row_idx + 1] as usize;
        let row_valid = (outer_start..outer_end).all(|inner_idx| inner.is_valid(inner_idx));

        valids.push(row_valid);
        if row_valid {
            let mut row_len = 0i32;
            for inner_idx in outer_start..outer_end {
                let inner_start = inner.value_offsets()[inner_idx] as usize;
                let inner_end = inner.value_offsets()[inner_idx + 1] as usize;
                mutable.extend(0, inner_start, inner_end);
                row_len += (inner_end - inner_start) as i32;
            }
            offset += row_len;
        }
        offsets.push(offset);
    }

    let values = make_array(mutable.freeze());
    let field = Arc::new(Field::new_list_field(values.data_type().clone(), true));
    Ok(ColumnarValue::Array(Arc::new(ListArray::try_new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        Some(NullBuffer::from(valids)),
    )?)))
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{ArrayRef, ListArray},
        buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
        datatypes::{Field, Int32Type},
    };
    use datafusion::common::ScalarValue;

    use super::*;

    #[test]
    fn test_array_reverse_int() -> Result<()> {
        let input: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None, Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![]),
            None,
        ]));

        let result = array_reverse(&[ColumnarValue::Array(input)])?.into_array(4)?;
        let expected: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3), None, Some(1)]),
            Some(vec![Some(5), Some(4)]),
            Some(vec![]),
            None,
        ]));

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_reverse_scalar() -> Result<()> {
        let input = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::from(1),
                ScalarValue::from(2),
                ScalarValue::from(3),
            ],
            &DataType::Int32,
            true,
        ));

        let result = array_reverse(&[ColumnarValue::Scalar(input)])?.into_array(1)?;
        let expected = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::from(3),
                ScalarValue::from(2),
                ScalarValue::from(1),
            ],
            &DataType::Int32,
            true,
        ))
        .to_array()?;

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_flatten_int() -> std::result::Result<(), Box<dyn Error>> {
        let inner = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), None]),
            Some(vec![Some(3)]),
            Some(vec![]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(6)]),
            None,
        ]);
        let inner: ArrayRef = Arc::new(inner);
        let input: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(inner.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 4, 6, 6, 6])),
            inner,
            Some(NullBuffer::from(vec![true, true, true, true, false])),
        )?);

        let result = array_flatten(&[ColumnarValue::Array(input)])?.into_array(5)?;

        let expected = vec![
            Some(vec![Some(1), None, Some(3)]),
            Some(vec![Some(4), Some(5)]),
            None,
            Some(vec![]),
            None,
        ];
        let expected: ArrayRef =
            Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(expected));

        assert_eq!(&result, &expected);
        Ok(())
    }
}
