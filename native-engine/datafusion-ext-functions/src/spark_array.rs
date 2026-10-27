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

use std::{cmp::Ordering, collections::HashSet, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BooleanArray, Float32Array, Float64Array, ListArray, MutableArrayData,
        make_array,
    },
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
    row::{RowConverter, SortField},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{
    arrow::eq_comparator::{DynEqComparator, make_eq_comparator},
    df_execution_err, downcast_any,
    scalar_value::compacted_scalar_value_from_array,
    spark_float::spark_float_cmp,
};

pub fn array_distinct(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return df_execution_err!("array_distinct requires exactly 1 argument");
    }
    let array = columnar_value_to_list_array(&args[0])?;
    if array.value_type().is_nested() {
        return df_execution_err!("array_distinct does not support nested element types");
    }

    // Spark canonicalizes NaNs but keeps signed zeros distinct before Spark 4.2's
    // normalization.
    let values: ArrayRef = match array.value_type() {
        DataType::Float32 => Arc::new(Float32Array::from_iter(
            downcast_any!(array.values(), Float32Array)?
                .iter()
                .map(|v| v.map(|v| if v.is_nan() { f32::NAN } else { v })),
        )),
        DataType::Float64 => Arc::new(Float64Array::from_iter(
            downcast_any!(array.values(), Float64Array)?
                .iter()
                .map(|v| v.map(|v| if v.is_nan() { f64::NAN } else { v })),
        )),
        _ => Arc::clone(array.values()),
    };
    let converter = RowConverter::new(vec![SortField::new(values.data_type().clone())])?;
    let rows = converter.convert_columns(&[Arc::clone(&values)])?;
    let data = values.to_data();
    let mut output = MutableArrayData::new(vec![&data], false, values.len());
    let mut offsets = Vec::with_capacity(array.len() + 1);
    let mut seen = HashSet::new();
    offsets.push(0i32);
    for row in 0..array.len() {
        seen.clear();
        if array.is_valid(row) {
            let start = array.value_offsets()[row] as usize;
            let end = array.value_offsets()[row + 1] as usize;
            for index in start..end {
                if seen.insert(rows.row(index)) {
                    output.extend(0, index, index + 1);
                }
            }
        }
        offsets.push(output.len() as i32);
    }
    let DataType::List(field) = array.data_type() else {
        return df_execution_err!("array_distinct requires a list");
    };
    let result: ArrayRef = Arc::new(ListArray::try_new(
        Arc::clone(field),
        OffsetBuffer::new(offsets.into()),
        make_array(output.freeze()),
        array.nulls().cloned(),
    )?);
    if matches!(args[0], ColumnarValue::Scalar(_)) {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

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
    let equals = spark_eq_comparator(elements, values)?;

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

/// Compares `left[i]` with `right[j]` using Spark's equality: floats treat NaN
/// as equal to NaN and -0.0 as equal to 0.0, other types use
/// `make_eq_comparator`. Both arrays must have the same type, and callers must
/// skip null positions themselves.
fn spark_eq_comparator(left: &ArrayRef, right: &ArrayRef) -> Result<DynEqComparator> {
    macro_rules! float_eq {
        ($array_type:ty) => {{
            let left = downcast_any!(left, $array_type)?.values().clone();
            let right = downcast_any!(right, $array_type)?.values().clone();
            Box::new(move |i: usize, j: usize| {
                spark_float_cmp(left[i], right[j]) == Ordering::Equal
            }) as DynEqComparator
        }};
    }
    Ok(match left.data_type() {
        DataType::Float32 => float_eq!(Float32Array),
        DataType::Float64 => float_eq!(Float64Array),
        _ => make_eq_comparator(left.as_ref(), right.as_ref(), true)?,
    })
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

/// Removes every element equal to `element` from each list, following Spark's
/// `array_remove`: a null list or null element yields null, null list entries
/// are kept, and floats treat NaN as equal to NaN and -0.0 as equal to 0.0.
pub fn array_remove(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return df_execution_err!(
            "array_remove requires exactly 2 arguments, got {}",
            args.len()
        );
    }
    let list_is_scalar = matches!(args[0], ColumnarValue::Scalar(_));
    let value_is_scalar = matches!(args[1], ColumnarValue::Scalar(_));
    // Scalars stay a single row so scalar list values are never copied per input
    // row.
    let lists = columnar_value_to_list_array(&args[0])?;
    let values = args[1].to_array(1)?;
    let num_rows = match (list_is_scalar, value_is_scalar) {
        (true, _) => values.len(),
        (false, true) => lists.len(),
        (false, false) if lists.len() == values.len() => lists.len(),
        (false, false) => {
            return df_execution_err!(
                "array_remove requires matching array lengths, got {} and {}",
                lists.len(),
                values.len()
            );
        }
    };
    let DataType::List(field) = lists.data_type() else {
        return df_execution_err!(
            "array_remove only supports list, got {:?}",
            lists.data_type()
        );
    };
    let field = Arc::clone(field);

    let elements = lists.values();
    let element_nulls = elements.logical_nulls();
    let value_nulls = values.logical_nulls();

    let equals: DynEqComparator = match (elements.data_type(), values.data_type()) {
        // all-null inputs never reach the comparator
        (DataType::Null, _) | (_, DataType::Null) => Box::new(|_, _| false),
        (left, right) if left != right => {
            return df_execution_err!(
                "array_remove requires matching element and value types, got {left:?} and {right:?}"
            );
        }
        _ => spark_eq_comparator(elements, &values)?,
    };

    let next_offset = |len: usize| -> Result<i32> {
        i32::try_from(len)
            .or_else(|_| df_execution_err!("array_remove result exceeds the maximum list size"))
    };
    let list_offsets = lists.value_offsets();
    let visible_len = (list_offsets[lists.len()] - list_offsets[0]) as usize;
    let elements_data = elements.to_data();
    let mut kept = MutableArrayData::new(vec![&elements_data], false, visible_len);
    let mut offsets = Vec::with_capacity(num_rows + 1);
    offsets.push(0i32);
    let mut valids = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        let list_row = if list_is_scalar { 0 } else { row };
        let value_row = if value_is_scalar { 0 } else { row };
        let value_is_null = value_nulls
            .as_ref()
            .is_some_and(|nulls| nulls.is_null(value_row));
        if lists.is_null(list_row) || value_is_null {
            valids.push(false);
            offsets.push(next_offset(kept.len())?);
            continue;
        }

        let start = list_offsets[list_row] as usize;
        let end = list_offsets[list_row + 1] as usize;
        // copy maximal runs of kept elements with one extend per run
        let mut run_start = start;
        for index in start..end {
            let element_is_null = element_nulls
                .as_ref()
                .is_some_and(|nulls| nulls.is_null(index));
            if !element_is_null && equals(index, value_row) {
                if run_start < index {
                    kept.extend(0, run_start, index);
                }
                run_start = index + 1;
            }
        }
        if run_start < end {
            kept.extend(0, run_start, end);
        }
        valids.push(true);
        offsets.push(next_offset(kept.len())?);
    }

    let result: ArrayRef = Arc::new(ListArray::try_new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        make_array(kept.freeze()),
        Some(NullBuffer::from(valids)),
    )?);
    if list_is_scalar && value_is_scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{
            ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, ListArray, ListBuilder,
            NullArray, StringArray, StringBuilder,
        },
        buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
        datatypes::{Field, Float32Type, Float64Type, Int32Type},
    };
    use datafusion::common::ScalarValue;
    use datafusion_ext_commons::downcast_any;

    use super::*;

    #[test]
    fn test_array_distinct_int_and_sliced_lists() -> Result<()> {
        let input = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(99)]),
            Some(vec![
                Some(3),
                Some(1),
                Some(3),
                None,
                Some(1),
                None,
                Some(2),
            ]),
            Some(vec![Some(2), Some(2), Some(1)]),
            Some(vec![]),
            None,
            Some(vec![None, None]),
        ]);
        let result =
            array_distinct(&[ColumnarValue::Array(Arc::new(input.slice(1, 5)))])?.into_array(5)?;
        let expected: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3), Some(1), None, Some(2)]),
            Some(vec![Some(2), Some(1)]),
            Some(vec![]),
            None,
            Some(vec![None]),
        ]));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_distinct_sliced_values_and_field() -> Result<()> {
        let values = Int32Array::from(vec![Some(99), Some(2), Some(2), None, None, Some(1)]);
        let field = Arc::new(Field::new("element", DataType::Int32, true).with_metadata(
            std::collections::HashMap::from([("key".into(), "value".into())]),
        ));
        let input = ListArray::try_new(
            Arc::clone(&field),
            OffsetBuffer::new(vec![0, 2, 4, 5].into()),
            Arc::new(values.slice(1, 5)),
            Some(NullBuffer::from(vec![true, false, true])),
        )?;
        let expected: ArrayRef = Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(vec![0, 1, 1, 2].into()),
            Arc::new(Int32Array::from(vec![2, 1])),
            input.nulls().cloned(),
        )?);
        let result = array_distinct(&[ColumnarValue::Array(Arc::new(input))])?.into_array(3)?;
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_distinct_scalar_and_empty_batch() -> Result<()> {
        let input = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(2), None, Some(2), None, Some(1)]),
            Some(vec![]),
            None,
        ]);
        let input: ArrayRef = Arc::new(input);
        for row in 0..input.len() {
            let scalar = ScalarValue::try_from_array(&input, row)?;
            let result = array_distinct(&[ColumnarValue::Scalar(scalar)])?;
            assert!(matches!(result, ColumnarValue::Scalar(_)));
            let expected: ArrayRef = match row {
                0 => Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(2), None, Some(1)]),
                ])),
                _ => input.slice(row, 1),
            };
            assert_eq!(&result.into_array(1)?, &expected);
        }
        let empty = input.slice(0, 0);
        let result = array_distinct(&[ColumnarValue::Array(Arc::clone(&empty))])?.into_array(0)?;
        assert_eq!(&result, &empty);
        Ok(())
    }

    #[test]
    fn test_array_distinct_null_elements() -> Result<()> {
        let input: ArrayRef = Arc::new(ListArray::try_new(
            Arc::new(Field::new_list_field(DataType::Null, true)),
            OffsetBuffer::new(vec![0, 2, 2].into()),
            Arc::new(NullArray::new(2)),
            None,
        )?);
        let result = array_distinct(&[ColumnarValue::Array(input)])?.into_array(2)?;
        let result = downcast_any!(&result, ListArray)?;
        assert_eq!(result.value_offsets(), &[0, 1, 1]);
        assert_eq!(result.values().len(), 1);
        assert_eq!(result.values().logical_null_count(), 1);
        Ok(())
    }

    macro_rules! test_array_distinct_float {
        ($name:ident, $arrow_type:ty, $array_type:ty, $float_type:ty, $nan_bits:expr) => {
            #[test]
            fn $name() -> Result<()> {
                let input: ArrayRef =
                    Arc::new(ListArray::from_iter_primitive::<$arrow_type, _, _>(vec![
                        Some(vec![
                            Some(-0.0),
                            Some(0.0),
                            Some(-0.0),
                            Some(<$float_type>::from_bits($nan_bits)),
                            Some(-<$float_type>::NAN),
                            Some(<$float_type>::NAN),
                            None,
                            None,
                            Some(<$float_type>::INFINITY),
                            Some(<$float_type>::INFINITY),
                        ]),
                    ]));
                let result = array_distinct(&[ColumnarValue::Array(input)])?.into_array(1)?;
                let list = downcast_any!(&result, ListArray)?;
                let values = downcast_any!(list.values(), $array_type)?;
                assert_eq!(list.value_offsets(), &[0, 5]);
                assert_eq!(values.value(0).to_bits(), (-0.0 as $float_type).to_bits());
                assert_eq!(values.value(1).to_bits(), (0.0 as $float_type).to_bits());
                assert_eq!(values.value(2).to_bits(), <$float_type>::NAN.to_bits());
                assert!(values.is_null(3));
                assert_eq!(values.value(4), <$float_type>::INFINITY);
                Ok(())
            }
        };
    }

    test_array_distinct_float!(
        test_array_distinct_f32,
        Float32Type,
        Float32Array,
        f32,
        0x7fc0_0001
    );
    test_array_distinct_float!(
        test_array_distinct_f64,
        Float64Type,
        Float64Array,
        f64,
        0x7ff8_0000_0000_0001
    );

    #[test]
    fn test_array_distinct_invalid_arguments() {
        assert!(array_distinct(&[]).is_err());
        assert!(array_distinct(&[ColumnarValue::Scalar(ScalarValue::from(1))]).is_err());
        assert!(
            array_distinct(&[
                ColumnarValue::Scalar(ScalarValue::Null),
                ColumnarValue::Scalar(ScalarValue::Null),
            ])
            .is_err()
        );
    }

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

    fn int_lists(rows: Vec<Option<Vec<Option<i32>>>>) -> ArrayRef {
        Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(rows))
    }

    fn f64_rows(result: &ArrayRef) -> Result<Vec<Option<Vec<Option<f64>>>>> {
        let lists = downcast_any!(result, ListArray)?;
        (0..lists.len())
            .map(|i| {
                if lists.is_null(i) {
                    return Ok(None);
                }
                let row = lists.value(i);
                Ok(Some(downcast_any!(row, Float64Array)?.iter().collect()))
            })
            .collect()
    }

    fn f32_rows(result: &ArrayRef) -> Result<Vec<Option<Vec<Option<f32>>>>> {
        let lists = downcast_any!(result, ListArray)?;
        (0..lists.len())
            .map(|i| {
                if lists.is_null(i) {
                    return Ok(None);
                }
                let row = lists.value(i);
                Ok(Some(downcast_any!(row, Float32Array)?.iter().collect()))
            })
            .collect()
    }

    #[test]
    fn test_array_remove_int_scalar_element() -> Result<()> {
        let input = int_lists(vec![
            Some(vec![Some(1), Some(2), Some(3), None, Some(3)]),
            Some(vec![Some(3), Some(3)]),
            Some(vec![]),
            None,
            Some(vec![Some(4), Some(5)]),
        ]);
        let result = array_remove(&[
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(3))),
        ])?
        .into_array(5)?;
        let expected = int_lists(vec![
            Some(vec![Some(1), Some(2), None]),
            Some(vec![]),
            Some(vec![]),
            None,
            Some(vec![Some(4), Some(5)]),
        ]);
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_remove_array_elements() -> Result<()> {
        let input = int_lists(vec![
            Some(vec![Some(1), Some(2), Some(1)]),
            Some(vec![Some(1), Some(2), Some(1)]),
            Some(vec![Some(1), None]),
            Some(vec![Some(1)]),
        ]);
        let elements: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(5)]));
        let result = array_remove(&[ColumnarValue::Array(input), ColumnarValue::Array(elements)])?
            .into_array(4)?;
        let expected = int_lists(vec![
            Some(vec![Some(2)]),
            Some(vec![Some(1), Some(1)]),
            None,
            Some(vec![Some(1)]),
        ]);
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_remove_scalar_list_array_elements() -> Result<()> {
        let list = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::from(1),
                ScalarValue::from(2),
                ScalarValue::from(3),
            ],
            &DataType::Int32,
            true,
        ));
        let elements: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(4)]));
        let result = array_remove(&[ColumnarValue::Scalar(list), ColumnarValue::Array(elements)])?
            .into_array(3)?;
        let expected = int_lists(vec![
            Some(vec![Some(2), Some(3)]),
            None,
            Some(vec![Some(1), Some(2), Some(3)]),
        ]);
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_remove_scalars() -> Result<()> {
        let list = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::from(1),
                ScalarValue::from(2),
                ScalarValue::from(1),
            ],
            &DataType::Int32,
            true,
        ));

        let result = array_remove(&[
            ColumnarValue::Scalar(list.clone()),
            ColumnarValue::Scalar(ScalarValue::from(1)),
        ])?;
        let ColumnarValue::Scalar(result) = result else {
            return df_execution_err!("expected a scalar result");
        };
        let expected = ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::from(2)],
            &DataType::Int32,
            true,
        ));
        assert_eq!(result, expected);

        let result = array_remove(&[
            ColumnarValue::Scalar(list),
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
        ])?;
        let ColumnarValue::Scalar(result) = result else {
            return df_execution_err!("expected a scalar result");
        };
        assert!(result.is_null());
        Ok(())
    }

    #[test]
    fn test_array_remove_float64_nan_and_signed_zero() -> Result<()> {
        let other_nan = f64::from_bits(0x7ff8_0000_0000_0001);
        let input: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(vec![Some(f64::NAN), Some(1.0), Some(other_nan), None]),
            Some(vec![Some(-0.0), Some(0.0), Some(1.0)]),
            Some(vec![Some(f64::NAN), Some(-0.0)]),
        ]));
        let elements: ArrayRef = Arc::new(Float64Array::from(vec![other_nan, 0.0, -0.0]));
        let result = array_remove(&[ColumnarValue::Array(input), ColumnarValue::Array(elements)])?
            .into_array(3)?;
        let rows = f64_rows(&result)?;
        assert_eq!(rows[0], Some(vec![Some(1.0), None]));
        assert_eq!(rows[1], Some(vec![Some(1.0)]));
        let last = rows[2].clone().unwrap_or_default();
        assert_eq!(last.len(), 1);
        assert!(last[0].is_some_and(f64::is_nan));
        Ok(())
    }

    #[test]
    fn test_array_remove_float32_nan_and_signed_zero() -> Result<()> {
        let input: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
            Some(vec![Some(f32::NAN), Some(-0.0), Some(2.0)]),
            Some(vec![Some(f32::NAN), Some(-0.0), Some(2.0)]),
        ]));
        let elements: ArrayRef = Arc::new(Float32Array::from(vec![0.0, f32::NAN]));
        let result = array_remove(&[ColumnarValue::Array(input), ColumnarValue::Array(elements)])?
            .into_array(2)?;
        let rows = f32_rows(&result)?;
        let first = rows[0].clone().unwrap_or_default();
        assert_eq!(first.len(), 2);
        assert!(first[0].is_some_and(f32::is_nan));
        assert_eq!(first[1], Some(2.0));
        assert_eq!(rows[1], Some(vec![Some(-0.0), Some(2.0)]));
        // the surviving -0.0 must keep its sign (elements are copied, not normalized)
        assert!(
            rows[1]
                .as_ref()
                .and_then(|row| row.first().copied().flatten())
                .is_some_and(f32::is_sign_negative)
        );
        Ok(())
    }

    #[test]
    fn test_array_remove_strings() -> Result<()> {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("a");
        builder.values().append_value("b");
        builder.values().append_null();
        builder.values().append_value("a");
        builder.append(true);
        builder.values().append_value("c");
        builder.append(true);
        let input: ArrayRef = Arc::new(builder.finish());

        let result = array_remove(&[
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::from("a")),
        ])?
        .into_array(2)?;

        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("b");
        builder.values().append_null();
        builder.append(true);
        builder.values().append_value("c");
        builder.append(true);
        let expected: ArrayRef = Arc::new(builder.finish());
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_remove_sliced_input() -> Result<()> {
        let input = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(9)]),
            Some(vec![Some(1), Some(2), Some(1)]),
            Some(vec![Some(2), Some(1)]),
            Some(vec![Some(9)]),
        ]);
        let input: ArrayRef = Arc::new(input.slice(1, 2));
        let result = array_remove(&[
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::from(1)),
        ])?
        .into_array(2)?;
        let expected = int_lists(vec![Some(vec![Some(2)]), Some(vec![Some(2)])]);
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_remove_preserves_list_field() -> Result<()> {
        let field = Arc::new(Field::new("element", DataType::Int32, false));
        let input: ArrayRef = Arc::new(ListArray::try_new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 3])),
            Arc::new(Int32Array::from(vec![1, 2, 1])),
            None,
        )?);
        let result = array_remove(&[
            ColumnarValue::Array(Arc::clone(&input)),
            ColumnarValue::Scalar(ScalarValue::from(1)),
        ])?
        .into_array(1)?;
        assert_eq!(result.data_type(), input.data_type());
        let lists = downcast_any!(result, ListArray)?;
        let row = lists.value(0);
        assert_eq!(downcast_any!(row, Int32Array)?.values().to_vec(), vec![2]);
        Ok(())
    }

    #[test]
    fn test_array_remove_invalid_arguments() -> Result<()> {
        let list = int_lists(vec![Some(vec![Some(1)]), Some(vec![Some(2)])]);
        // wrong argument count
        assert!(array_remove(&[ColumnarValue::Array(Arc::clone(&list))]).is_err());
        // first argument is not a list
        assert!(
            array_remove(&[
                ColumnarValue::Scalar(ScalarValue::from(1)),
                ColumnarValue::Scalar(ScalarValue::from(1)),
            ])
            .is_err()
        );
        // element type differs from the list element type
        assert!(
            array_remove(&[
                ColumnarValue::Array(Arc::clone(&list)),
                ColumnarValue::Scalar(ScalarValue::from("a")),
            ])
            .is_err()
        );
        // mismatched lengths
        let elements: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        assert!(
            array_remove(&[ColumnarValue::Array(list), ColumnarValue::Array(elements)]).is_err()
        );
        Ok(())
    }
}
