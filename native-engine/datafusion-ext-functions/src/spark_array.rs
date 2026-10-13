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

use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, ListArray, MutableArrayData,
        make_array,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    compute::kernels::arity::unary,
    datatypes::{DataType, Field, Float32Type, Float64Type},
    row::{RowConverter, SortField},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{
    df_execution_err, downcast_any, scalar_value::compacted_scalar_value_from_array,
};

pub fn arrays_overlap(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return df_execution_err!("arrays_overlap requires exactly 2 arguments");
    }
    let left_is_scalar = matches!(args[0], ColumnarValue::Scalar(_));
    let right_is_scalar = matches!(args[1], ColumnarValue::Scalar(_));
    let lists = [
        columnar_value_to_list_array(&args[0])?,
        columnar_value_to_list_array(&args[1])?,
    ];
    let num_rows = match (left_is_scalar, right_is_scalar) {
        (true, _) => lists[1].len(),
        (_, true) => lists[0].len(),
        _ if lists[0].len() == lists[1].len() => lists[0].len(),
        _ => return df_execution_err!("arrays_overlap requires matching array lengths"),
    };
    let element_type = lists[0].value_type();
    if element_type != lists[1].value_type() {
        return df_execution_err!("arrays_overlap requires matching element types");
    }
    let converter = RowConverter::new(vec![SortField::new(element_type.clone())])?;
    let element_rows = lists
        .iter()
        .map(|list| {
            // Spark's boxed floats equate NaNs but distinguish signed zeros.
            let values: ArrayRef = match element_type {
                DataType::Float32 => Arc::new(unary::<_, _, Float32Type>(
                    downcast_any!(list.values(), Float32Array)?,
                    |v| {
                        if v.is_nan() { f32::NAN } else { v }
                    },
                )),
                DataType::Float64 => Arc::new(unary::<_, _, Float64Type>(
                    downcast_any!(list.values(), Float64Array)?,
                    |v| {
                        if v.is_nan() { f64::NAN } else { v }
                    },
                )),
                _ => list.values().clone(),
            };
            Ok(converter.convert_columns(&[values])?)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut seen = HashSet::new();
    let result: ArrayRef = Arc::new(BooleanArray::from_iter((0..num_rows).map(|row| {
        let rows = [
            if left_is_scalar { 0 } else { row },
            if right_is_scalar { 0 } else { row },
        ];
        if lists[0].is_null(rows[0]) || lists[1].is_null(rows[1]) {
            return None;
        }
        let ranges = [0, 1].map(|i| {
            lists[i].value_offsets()[rows[i]] as usize
                ..lists[i].value_offsets()[rows[i] + 1] as usize
        });
        if ranges.iter().any(|range| range.is_empty()) {
            return Some(false);
        }
        let (smaller, bigger) = if ranges[0].len() <= ranges[1].len() {
            (0, 1)
        } else {
            (1, 0)
        };
        seen.clear();
        let mut has_null = false;
        for index in ranges[smaller].clone() {
            if lists[smaller].values().is_null(index) {
                has_null = true;
            } else {
                seen.insert(element_rows[smaller].row(index));
            }
        }
        for index in ranges[bigger].clone() {
            if lists[bigger].values().is_null(index) {
                has_null = true;
            } else if seen.contains(&element_rows[bigger].row(index)) {
                return Some(true);
            }
        }
        if has_null { None } else { Some(false) }
    })));
    if left_is_scalar && right_is_scalar {
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
        array::{ArrayRef, ListArray},
        buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
        datatypes::{Field, Float32Type, Float64Type, Int32Type},
    };
    use datafusion::common::ScalarValue;

    use super::*;

    #[test]
    fn test_arrays_overlap_nulls_and_slices() -> Result<()> {
        let cases = vec![
            (
                Some(vec![Some(1), Some(2)]),
                Some(vec![Some(2), Some(3)]),
                Some(true),
            ),
            (Some(vec![Some(1)]), Some(vec![Some(2)]), Some(false)),
            (
                Some(vec![None, Some(2)]),
                Some(vec![Some(2), None]),
                Some(true),
            ),
            (
                Some(vec![Some(2), None]),
                Some(vec![None, Some(2)]),
                Some(true),
            ),
            (Some(vec![None, Some(1)]), Some(vec![Some(2)]), None),
            (Some(vec![Some(1)]), Some(vec![None, Some(2)]), None),
            (Some(vec![None]), Some(vec![None]), None),
            (Some(vec![]), Some(vec![None]), Some(false)),
            (Some(vec![None]), Some(vec![]), Some(false)),
            (Some(vec![]), Some(vec![]), Some(false)),
            (None, Some(vec![Some(1)]), None),
            (Some(vec![Some(1)]), None, None),
            (None, None, None),
            (
                Some(vec![Some(1), Some(1), Some(2)]),
                Some(vec![Some(2), Some(2)]),
                Some(true),
            ),
        ];
        let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            cases.iter().map(|(left, ..)| left.clone()),
        ));
        let right: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
            cases.iter().map(|(_, right, _)| right.clone()),
        ));
        let expected: ArrayRef = Arc::new(BooleanArray::from_iter(
            cases.iter().map(|(_, _, expected)| *expected),
        ));
        for (left, right) in [(left.clone(), right.clone()), (right, left)] {
            let result = arrays_overlap(&[
                ColumnarValue::Array(left.clone()),
                ColumnarValue::Array(right.clone()),
            ])?
            .into_array(cases.len())?;
            assert_eq!(&result, &expected);
            let result = arrays_overlap(&[
                ColumnarValue::Array(left.slice(2, 10)),
                ColumnarValue::Array(right.slice(2, 10)),
            ])?
            .into_array(10)?;
            assert_eq!(&result, &expected.slice(2, 10));
        }
        Ok(())
    }

    #[test]
    fn test_arrays_overlap_scalars_and_empty_batches() -> Result<()> {
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![Some(2), None]),
            Some(vec![]),
            None,
        ]));
        let scalar = ColumnarValue::Scalar(ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int32(Some(2))],
            &DataType::Int32,
            true,
        )));
        let expected: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            None,
        ]));
        for args in [
            [ColumnarValue::Array(lists.clone()), scalar.clone()],
            [scalar.clone(), ColumnarValue::Array(lists.clone())],
        ] {
            assert_eq!(&arrays_overlap(&args)?.into_array(4)?, &expected);
        }
        assert!(matches!(
            arrays_overlap(&[scalar.clone(), scalar.clone()])?,
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        ));
        let null = ColumnarValue::Scalar(ScalarValue::List(Arc::new(ListArray::new_null(
            Arc::new(Field::new_list_field(DataType::Int32, true)),
            1,
        ))));
        assert!(matches!(
            arrays_overlap(&[scalar.clone(), null])?,
            ColumnarValue::Scalar(ScalarValue::Boolean(None))
        ));
        let strings = ColumnarValue::Scalar(ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Utf8(None), ScalarValue::from("spark")],
            &DataType::Utf8,
            true,
        )));
        assert!(matches!(
            arrays_overlap(&[strings.clone(), strings])?,
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)))
        ));
        for args in [
            [scalar.clone(), ColumnarValue::Array(lists.slice(0, 0))],
            [ColumnarValue::Array(lists.slice(0, 0)), scalar],
        ] {
            assert!(arrays_overlap(&args)?.into_array(0)?.is_empty());
        }
        Ok(())
    }

    #[test]
    fn test_arrays_overlap_floats() -> Result<()> {
        macro_rules! check {
            ($type:ty, $nan:expr, $other_nan:expr, $infinity:expr) => {{
                let left: ArrayRef = Arc::new(ListArray::from_iter_primitive::<$type, _, _>([
                    Some(vec![Some($nan)]),
                    Some(vec![Some(-0.0)]),
                    Some(vec![Some(0.0)]),
                    Some(vec![Some($infinity)]),
                    Some(vec![Some(-$infinity)]),
                    Some(vec![Some($nan), None]),
                    Some(vec![Some($nan)]),
                    Some(vec![None, Some($nan)]),
                ]));
                let right: ArrayRef = Arc::new(ListArray::from_iter_primitive::<$type, _, _>([
                    Some(vec![Some($other_nan)]),
                    Some(vec![Some(0.0)]),
                    Some(vec![Some(-0.0)]),
                    Some(vec![Some($infinity)]),
                    Some(vec![Some($infinity)]),
                    Some(vec![Some(1.0)]),
                    Some(vec![Some(1.0)]),
                    Some(vec![Some($other_nan)]),
                ]));
                let expected: ArrayRef = Arc::new(BooleanArray::from(vec![
                    Some(true),
                    Some(false),
                    Some(false),
                    Some(true),
                    Some(false),
                    None,
                    Some(false),
                    Some(true),
                ]));
                let result =
                    arrays_overlap(&[ColumnarValue::Array(left), ColumnarValue::Array(right)])?
                        .into_array(8)?;
                assert_eq!(&result, &expected);
            }};
        }
        check!(
            Float32Type,
            f32::NAN,
            f32::from_bits(0xff80_0001),
            f32::INFINITY
        );
        check!(
            Float64Type,
            f64::NAN,
            f64::from_bits(0xfff0_0000_0000_0001),
            f64::INFINITY
        );
        Ok(())
    }

    #[test]
    fn test_arrays_overlap_invalid_arguments() -> Result<()> {
        assert!(arrays_overlap(&[]).is_err());
        let ints = ColumnarValue::Scalar(ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int32(Some(1))],
            &DataType::Int32,
            true,
        )));
        let strings = ColumnarValue::Scalar(ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::from("1")],
            &DataType::Utf8,
            true,
        )));
        assert!(arrays_overlap(&[ints.clone()]).is_err());
        assert!(arrays_overlap(&[ints.clone(), strings]).is_err());
        assert!(
            arrays_overlap(&[
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ints.clone()
            ])
            .is_err()
        );
        let one = ints.to_array(1)?;
        let two = ints.to_array(2)?;
        assert!(arrays_overlap(&[ColumnarValue::Array(one), ColumnarValue::Array(two)]).is_err());
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
