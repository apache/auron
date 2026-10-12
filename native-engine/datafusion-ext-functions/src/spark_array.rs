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
    array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, ListArray, MutableArrayData,
        make_array,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{
    arrow::eq_comparator::{DynEqComparator, make_eq_comparator},
    df_execution_err, downcast_any,
    scalar_value::compacted_scalar_value_from_array,
};

pub fn array_contains(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return df_execution_err!("array_contains requires exactly 2 arguments");
    }
    let list_is_scalar = matches!(args[0], ColumnarValue::Scalar(_));
    let value_is_scalar = matches!(args[1], ColumnarValue::Scalar(_));
    // Keep scalar lists in one row to avoid copying their elements per input row.
    let arrays = args
        .iter()
        .map(|arg| arg.to_array(1))
        .collect::<Result<Vec<_>>>()?;
    let lists = downcast_any!(&arrays[0], ListArray)?;
    let elements = lists.values();
    let values = &arrays[1];
    let num_rows = match (list_is_scalar, value_is_scalar) {
        (true, _) => values.len(),
        (_, true) => lists.len(),
        _ if lists.len() == values.len() => lists.len(),
        _ => return df_execution_err!("array_contains requires matching array lengths"),
    };
    if elements.data_type() != values.data_type() {
        return df_execution_err!("array_contains requires matching element and value types");
    }

    // Spark treats all NaNs as equal and -0.0 as equal to 0.0.
    macro_rules! float_eq {
        ($array_type:ty) => {{
            let left = downcast_any!(elements, $array_type)?.values().clone();
            let right = downcast_any!(values, $array_type)?.values().clone();
            Box::new(move |i: usize, j: usize| {
                left[i] == right[j] || (left[i].is_nan() && right[j].is_nan())
            })
        }};
    }
    let equals: DynEqComparator = match elements.data_type() {
        DataType::Float32 => float_eq!(Float32Array),
        DataType::Float64 => float_eq!(Float64Array),
        _ => make_eq_comparator(elements.as_ref(), values.as_ref(), true)?,
    };

    let result: ArrayRef = Arc::new(BooleanArray::from_iter((0..num_rows).map(|row| {
        let list_row = if list_is_scalar { 0 } else { row };
        let value_row = if value_is_scalar { 0 } else { row };
        if lists.is_null(list_row) || values.is_null(value_row) {
            return None;
        }
        let mut result = Some(false);
        let start = lists.value_offsets()[list_row] as usize;
        let end = lists.value_offsets()[list_row + 1] as usize;
        for index in start..end {
            if elements.is_null(index) {
                result = None;
            } else if equals(index, value_row) {
                return Some(true);
            }
        }
        result
    })));
    if list_is_scalar && value_is_scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

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
        array::{
            ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, ListArray, StringArray,
        },
        buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
        datatypes::{Field, Float32Type, Float64Type, Int32Type},
    };
    use datafusion::common::ScalarValue;

    use super::*;

    #[test]
    fn test_array_contains_nulls_and_slices() -> Result<()> {
        let lists = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(99)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(1), Some(2)]),
            Some(vec![None, Some(2)]),
            Some(vec![Some(2), None]),
            Some(vec![Some(1), None]),
            Some(vec![]),
            None,
            Some(vec![Some(1)]),
            Some(vec![None]),
            Some(vec![]),
        ]);
        let values = Int32Array::from(vec![
            Some(99),
            Some(2),
            Some(3),
            Some(2),
            Some(2),
            Some(3),
            Some(1),
            Some(1),
            None,
            Some(1),
            None,
        ]);
        for (offset, len) in [(0, 11), (1, 10), (2, 0)] {
            let result = array_contains(&[
                ColumnarValue::Array(Arc::new(lists.slice(offset, len))),
                ColumnarValue::Array(Arc::new(values.slice(offset, len))),
            ])?
            .into_array(len)?;
            let expected = BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                None,
                Some(false),
                None,
                None,
                None,
                None,
            ]);
            let expected: ArrayRef = Arc::new(expected.slice(offset, len));
            assert_eq!(&result, &expected);
        }
        Ok(())
    }

    #[test]
    fn test_array_contains_large_scalar_list() -> Result<()> {
        let list = ScalarValue::List(Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            [Some((0..10_000).map(Some))],
        )));
        for len in [0, 10_000] {
            let result = array_contains(&[
                ColumnarValue::Scalar(list.clone()),
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![0; len]))),
            ])?
            .into_array(len)?;
            let expected: ArrayRef = Arc::new(BooleanArray::from(vec![true; len]));
            assert_eq!(&result, &expected);
        }
        Ok(())
    }

    #[test]
    fn test_array_contains_scalars() -> Result<()> {
        let list = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::from("a"), ScalarValue::Utf8(None)],
            &DataType::Utf8,
            true,
        ));
        let result = array_contains(&[
            ColumnarValue::Scalar(list.clone()),
            ColumnarValue::Array(Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                None,
            ]))),
        ])?
        .into_array(3)?;
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, None]));
        assert_eq!(&result, &expected);

        for (value, expected) in [
            (ScalarValue::from("a"), Some(true)),
            (ScalarValue::from("b"), None),
            (ScalarValue::Utf8(None), None),
        ] {
            let result = array_contains(&[
                ColumnarValue::Scalar(list.clone()),
                ColumnarValue::Scalar(value),
            ])?;
            assert!(
                matches!(result, ColumnarValue::Scalar(ScalarValue::Boolean(value))
                if value == expected)
            );
        }

        let lists = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1)]),
            Some(vec![Some(2)]),
            Some(vec![]),
            None,
        ]);
        let result = array_contains(&[
            ColumnarValue::Array(Arc::new(lists)),
            ColumnarValue::Scalar(ScalarValue::from(1)),
        ])?
        .into_array(4)?;
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            None,
        ]));
        assert_eq!(&result, &expected);

        let null_list = ScalarValue::List(Arc::new(ListArray::new_null(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            1,
        )));
        assert!(matches!(
            array_contains(&[
                ColumnarValue::Scalar(null_list),
                ColumnarValue::Scalar(ScalarValue::from(1)),
            ])?,
            ColumnarValue::Scalar(ScalarValue::Boolean(None))
        ));
        Ok(())
    }

    macro_rules! test_array_contains_float {
        ($name:ident, $type:ty, $array:ty, $nan:expr) => {
            #[test]
            fn $name() -> Result<()> {
                let nan = $nan;
                let lists = ListArray::from_iter_primitive::<$type, _, _>(vec![
                    Some(vec![Some(nan)]),
                    Some(vec![Some(-0.0)]),
                    Some(vec![Some(0.0)]),
                    Some(vec![Some(nan), None]),
                    Some(vec![Some(nan)]),
                    Some(vec![None, Some(nan)]),
                ]);
                let values = <$array>::from(vec![-nan, 0.0, -0.0, 1.0, 1.0, -nan]);
                let result = array_contains(&[
                    ColumnarValue::Array(Arc::new(lists)),
                    ColumnarValue::Array(Arc::new(values)),
                ])?
                .into_array(6)?;
                let expected: ArrayRef = Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(true),
                    None,
                    Some(false),
                    Some(true),
                ]));
                assert_eq!(&result, &expected);
                Ok(())
            }
        };
    }

    test_array_contains_float!(
        test_array_contains_float32,
        Float32Type,
        Float32Array,
        f32::from_bits(0x7fc00001)
    );
    test_array_contains_float!(
        test_array_contains_float64,
        Float64Type,
        Float64Array,
        f64::from_bits(0x7ff8000000000001)
    );

    #[test]
    fn test_array_contains_invalid_arguments() -> Result<()> {
        assert!(array_contains(&[]).is_err());
        assert!(
            array_contains(&[
                ColumnarValue::Scalar(ScalarValue::from(1)),
                ColumnarValue::Scalar(ScalarValue::from(1)),
            ])
            .is_err()
        );
        let lists = ColumnarValue::Array(Arc::new(
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(1)])]),
        ));
        assert!(
            array_contains(&[lists.clone(), ColumnarValue::Scalar(ScalarValue::from("a")),])
                .is_err()
        );
        assert!(
            array_contains(&[
                lists,
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 2]))),
            ])
            .is_err()
        );
        Ok(())
    }

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
