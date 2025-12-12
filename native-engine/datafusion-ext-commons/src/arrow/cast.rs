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

use std::{str::FromStr, sync::Arc};

use arrow::{array::*, datatypes::*};
use bigdecimal::BigDecimal;
use chrono::Datelike;
use datafusion::common::Result;
use num::{Bounded, FromPrimitive, Integer, Signed};

use crate::df_execution_err;

pub fn cast(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    cast_impl(array, cast_type, false)
}

pub fn cast_scan_input_array(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    cast_impl(array, cast_type, true)
}

pub fn cast_impl(
    array: &dyn Array,
    cast_type: &DataType,
    match_struct_fields: bool,
) -> Result<ArrayRef> {
    Ok(match (&array.data_type(), cast_type) {
        (&t1, t2) if t1 == t2 => make_array(array.to_data()),

        (_, &DataType::Null) => Arc::new(NullArray::new(array.len())),

        // spark compatible str to int
        (&DataType::Utf8, to_dt) if to_dt.is_signed_integer() => {
            return try_cast_string_array_to_integer(array, to_dt);
        }

        // spark compatible str to date
        (&DataType::Utf8, &DataType::Date32) => {
            return try_cast_string_array_to_date(array);
        }

        // float to int
        // use unchecked casting, which is compatible with spark
        (&DataType::Float32, &DataType::Int8) => {
            let input = array.as_primitive::<Float32Type>();
            let output: Int8Array = arrow::compute::unary(input, |v| v as i8);
            Arc::new(output)
        }
        (&DataType::Float32, &DataType::Int16) => {
            let input = array.as_primitive::<Float32Type>();
            let output: Int16Array = arrow::compute::unary(input, |v| v as i16);
            Arc::new(output)
        }
        (&DataType::Float32, &DataType::Int32) => {
            let input = array.as_primitive::<Float32Type>();
            let output: Int32Array = arrow::compute::unary(input, |v| v as i32);
            Arc::new(output)
        }
        (&DataType::Float32, &DataType::Int64) => {
            let input = array.as_primitive::<Float32Type>();
            let output: Int64Array = arrow::compute::unary(input, |v| v as i64);
            Arc::new(output)
        }
        (&DataType::Float64, &DataType::Int8) => {
            let input = array.as_primitive::<Float64Type>();
            let output: Int8Array = arrow::compute::unary(input, |v| v as i8);
            Arc::new(output)
        }
        (&DataType::Float64, &DataType::Int16) => {
            let input = array.as_primitive::<Float64Type>();
            let output: Int16Array = arrow::compute::unary(input, |v| v as i16);
            Arc::new(output)
        }
        (&DataType::Float64, &DataType::Int32) => {
            let input = array.as_primitive::<Float64Type>();
            let output: Int32Array = arrow::compute::unary(input, |v| v as i32);
            Arc::new(output)
        }
        (&DataType::Float64, &DataType::Int64) => {
            let input = array.as_primitive::<Float64Type>();
            let output: Int64Array = arrow::compute::unary(input, |v| v as i64);
            Arc::new(output)
        }

        (&DataType::Timestamp(..), DataType::Float64) => {
            // timestamp to f64 = timestamp to i64 to f64, only used in agg.sum()
            arrow::compute::cast(
                &arrow::compute::cast(array, &DataType::Int64)?,
                &DataType::Float64,
            )?
        }
        (&DataType::Boolean, DataType::Utf8) => {
            // spark compatible boolean to string cast
            Arc::new(
                array
                    .as_boolean()
                    .iter()
                    .map(|value| value.map(|value| if value { "true" } else { "false" }))
                    .collect::<StringArray>(),
            )
        }
        (&DataType::List(_), DataType::List(to_field)) => {
            let list = as_list_array(array);
            let items = cast_impl(list.values(), to_field.data_type(), match_struct_fields)?;
            make_array(
                list.to_data()
                    .into_builder()
                    .data_type(DataType::List(to_field.clone()))
                    .child_data(vec![items.into_data()])
                    .build()?,
            )
        }
        (&DataType::Struct(_), DataType::Struct(to_fields)) => {
            let struct_ = as_struct_array(array);

            if !match_struct_fields {
                if to_fields.len() != struct_.num_columns() {
                    df_execution_err!("cannot cast structs with different numbers of fields")?;
                }

                let casted_arrays = struct_
                    .columns()
                    .iter()
                    .zip(to_fields)
                    .map(|(column, to_field)| {
                        cast_impl(column, to_field.data_type(), match_struct_fields)
                    })
                    .collect::<Result<Vec<_>>>()?;

                make_array(
                    struct_
                        .to_data()
                        .into_builder()
                        .data_type(DataType::Struct(to_fields.clone()))
                        .child_data(
                            casted_arrays
                                .into_iter()
                                .map(|array| array.into_data())
                                .collect(),
                        )
                        .build()?,
                )
            } else {
                let mut null_column_name = vec![];
                let casted_arrays = to_fields
                    .iter()
                    .map(|field| {
                        let origin = field.name().as_str();
                        let mut col = struct_.column_by_name(origin);
                        // correct orc map entries field name from "keys" to "key", "values" to
                        // "value"
                        if col.is_none() && (origin.eq("key") || origin.eq("value")) {
                            let adjust = format!("{}s", origin);
                            col = struct_.column_by_name(adjust.as_str());
                        }
                        if col.is_some() {
                            cast_impl(col.unwrap(), field.data_type(), match_struct_fields)
                        } else {
                            null_column_name.push(field.name().clone());
                            Ok(new_null_array(field.data_type(), struct_.len()))
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;

                let casted_fields = to_fields
                    .iter()
                    .map(|field: &FieldRef| {
                        if null_column_name.contains(field.name()) {
                            Arc::new(Field::new(field.name(), field.data_type().clone(), true))
                        } else {
                            field.clone()
                        }
                    })
                    .collect::<Vec<_>>();

                make_array(
                    struct_
                        .to_data()
                        .into_builder()
                        .data_type(DataType::Struct(Fields::from(casted_fields)))
                        .child_data(
                            casted_arrays
                                .into_iter()
                                .map(|array| array.into_data())
                                .collect(),
                        )
                        .build()?,
                )
            }
        }
        (&DataType::Map(..), &DataType::Map(ref to_entries_field, to_sorted)) => {
            let map = as_map_array(array);
            let entries = cast_impl(
                map.entries(),
                to_entries_field.data_type(),
                match_struct_fields,
            )?;
            make_array(
                map.to_data()
                    .into_builder()
                    .data_type(DataType::Map(to_entries_field.clone(), to_sorted))
                    .child_data(vec![entries.into_data()])
                    .build()?,
            )
        }
        // string to decimal
        (&DataType::Utf8, DataType::Decimal128(..)) => {
            arrow::compute::kernels::cast::cast(&to_plain_string_array(array), cast_type)?
        }
        // map to string (spark compatible)
        (&DataType::Map(..), &DataType::Utf8) => {
            let map_array = as_map_array(array);
            let entries = map_array.entries();
            let keys = entries.column(0);
            let values = entries.column(1);

            let casted_keys = cast_impl(keys, &DataType::Utf8, match_struct_fields)?;
            let casted_values = cast_impl(values, &DataType::Utf8, match_struct_fields)?;

            let string_keys = as_string_array(&casted_keys);
            let string_values = as_string_array(&casted_values);

            let mut builder = StringBuilder::new();

            for row_idx in 0..map_array.len() {
                if map_array.is_null(row_idx) {
                    builder.append_null();
                } else {
                    let mut row_str = String::from("{");
                    let start = map_array.value_offsets()[row_idx] as usize;
                    let end = map_array.value_offsets()[row_idx + 1] as usize;

                    for i in start..end {
                        if i > start {
                            row_str.push_str(", ");
                        }

                        row_str.push_str(string_keys.value(i));
                        row_str.push_str(" ->");

                        if values.is_null(i) {
                            row_str.push_str(" null");
                        } else {
                            row_str.push(' ');
                            row_str.push_str(string_values.value(i));
                        }
                    }

                    row_str.push('}');
                    builder.append_value(&row_str);
                }
            }

            Arc::new(builder.finish())
        }
        // struct to string (spark compatible)
        (&DataType::Struct(_), &DataType::Utf8) => {
            let struct_array = as_struct_array(array);
            let num_columns = struct_array.num_columns();

            let casted_columns: Vec<ArrayRef> = struct_array
                .columns()
                .iter()
                .map(|col| cast_impl(col, &DataType::Utf8, match_struct_fields))
                .collect::<Result<Vec<_>>>()?;

            let string_columns: Vec<&StringArray> = casted_columns
                .iter()
                .map(|col| as_string_array(col))
                .collect();

            let mut builder = StringBuilder::new();

            for row_idx in 0..struct_array.len() {
                if struct_array.is_null(row_idx) {
                    builder.append_null();
                } else {
                    let mut row_str = String::from("{");

                    if num_columns > 0 {
                        if struct_array.column(0).is_null(row_idx) {
                            row_str.push_str("null");
                        } else {
                            row_str.push_str(string_columns[0].value(row_idx));
                        }

                        for col_idx in 1..num_columns {
                            row_str.push(',');
                            if struct_array.column(col_idx).is_null(row_idx) {
                                row_str.push_str(" null");
                            } else {
                                row_str.push(' ');
                                row_str.push_str(string_columns[col_idx].value(row_idx));
                            }
                        }
                    }

                    row_str.push('}');
                    builder.append_value(&row_str);
                }
            }

            Arc::new(builder.finish())
        }
        _ => {
            // default cast
            arrow::compute::kernels::cast::cast(array, cast_type)?
        }
    })
}

fn to_plain_string_array(array: &dyn Array) -> ArrayRef {
    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
    let mut converted_values: Vec<Option<String>> = Vec::with_capacity(array.len());
    for v in array.iter() {
        match v {
            Some(s) => {
                // support to convert scientific notation
                if s.contains('e') || s.contains('E') {
                    match BigDecimal::from_str(s) {
                        Ok(decimal) => converted_values.push(Some(decimal.to_plain_string())),
                        Err(_) => converted_values.push(Some(s.to_string())),
                    }
                } else {
                    converted_values.push(Some(s.to_string()))
                }
            }
            None => converted_values.push(None),
        }
    }
    Arc::new(StringArray::from(converted_values))
}

fn try_cast_string_array_to_integer(array: &dyn Array, cast_type: &DataType) -> Result<ArrayRef> {
    macro_rules! cast {
        ($target_type:ident) => {{
            type B = paste::paste! {[<$target_type Builder>]};
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let mut builder = B::new();

            for v in array.iter() {
                match v {
                    Some(s) => builder.append_option(to_integer(s)),
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }};
    }

    Ok(match cast_type {
        DataType::Int8 => cast!(Int8),
        DataType::Int16 => cast!(Int16),
        DataType::Int32 => cast!(Int32),
        DataType::Int64 => cast!(Int64),
        _ => arrow::compute::cast(array, cast_type)?,
    })
}

fn try_cast_string_array_to_date(array: &dyn Array) -> Result<ArrayRef> {
    let strings = array.as_string::<i32>();
    let mut converted_values = Vec::with_capacity(strings.len());
    for s in strings {
        converted_values.push(s.and_then(|s| to_date(s)));
    }
    Ok(Arc::new(Date32Array::from(converted_values)))
}

// this implementation is original copied from spark UTF8String.scala
// The original implementation included trimming logic, but it was omitted here
// since Auron’s NativeConverters will handle trimming.
fn to_integer<T: Bounded + FromPrimitive + Integer + Signed + Copy>(input: &str) -> Option<T> {
    let bytes = input.as_bytes();

    if bytes.is_empty() {
        return None;
    }

    let b = bytes[0];
    let negative = b == b'-';
    let mut offset = 0;

    if negative || b == b'+' {
        offset += 1;
        if bytes.len() == 1 {
            return None;
        }
    }

    let separator = b'.';
    let radix = T::from_usize(10).unwrap();
    let stop_value = T::min_value() / radix;
    let mut result = T::zero();

    while offset < bytes.len() {
        let b = bytes[offset];
        offset += 1;
        if b == separator {
            // We allow decimals and will return a truncated integral in that case.
            // Therefore, we won't throw an exception here (checking the fractional
            // part happens below.)
            break;
        }

        let digit = if b.is_ascii_digit() {
            b - b'0'
        } else {
            return None;
        };

        // We are going to process the new digit and accumulate the result. However,
        // before doing this, if the result is already smaller than the
        // stopValue(Long.MIN_VALUE / radix), then result * 10 will definitely
        // be smaller than minValue, and we can stop.
        if result < stop_value {
            return None;
        }

        result = result * radix - T::from_u8(digit).unwrap();
        // Since the previous result is less than or equal to stopValue(Long.MIN_VALUE /
        // radix), we can just use `result > 0` to check overflow. If result
        // overflows, we should stop.
        if result > T::zero() {
            return None;
        }
    }

    // This is the case when we've encountered a decimal separator. The fractional
    // part will not change the number, but we will verify that the fractional part
    // is well-formed.
    while offset < bytes.len() {
        let current_byte = bytes[offset];
        if !current_byte.is_ascii_digit() {
            return None;
        }
        offset += 1;
    }

    if !negative {
        result = -result;
        if result < T::zero() {
            return None;
        }
    }
    Some(result)
}

// this implementation is original copied from spark SparkDateTimeUtils.scala
fn to_date(s: &str) -> Option<i32> {
    fn is_valid_digits(segment: usize, digits: i32) -> bool {
        // An integer is able to represent a date within [+-]5 million years.
        let max_digits_year = 7;
        (segment == 0 && digits >= 4 && digits <= max_digits_year)
            || (segment != 0 && digits > 0 && digits <= 2)
    }
    let s_trimmed = s.trim();
    if s_trimmed.is_empty() {
        return None;
    }
    let mut segments = [1, 1, 1];
    let mut sign = 1;
    let mut i = 0usize;
    let mut current_segment_value = 0i32;
    let mut current_segment_digits = 0i32;
    let bytes = s_trimmed.as_bytes();
    let mut j = 0usize;
    if bytes[j] == b'-' || bytes[j] == b'+' {
        sign = if bytes[j] == b'-' { -1 } else { 1 };
        j += 1;
    }
    while j < bytes.len() && (i < 3 && !(bytes[j] == b' ' || bytes[j] == b'T')) {
        let b = bytes[j];
        if i < 2 && b == b'-' {
            if !is_valid_digits(i, current_segment_digits) {
                return None;
            }
            segments[i] = current_segment_value;
            current_segment_value = 0;
            current_segment_digits = 0;
            i += 1;
        } else {
            let parsed_value = (b - b'0') as i32;
            if parsed_value < 0 || parsed_value > 9 {
                return None;
            } else {
                current_segment_value = current_segment_value * 10 + parsed_value;
                current_segment_digits += 1;
            }
        }
        j += 1;
    }
    if !is_valid_digits(i, current_segment_digits) {
        return None;
    }
    if i < 2 && j < bytes.len() {
        // For the `yyyy` and `yyyy-[m]m` formats, entire input must be consumed.
        return None;
    }
    segments[i] = current_segment_value;

    if segments[0] > 9999 || segments[1] > 12 || segments[2] > 31 {
        return None;
    }
    let local_date =
        chrono::NaiveDate::from_ymd_opt(sign * segments[0], segments[1] as u32, segments[2] as u32);
    local_date.map(|local_date| local_date.num_days_from_ce() - 719163)
}

#[cfg(test)]
mod test {
    use datafusion::common::cast::{as_decimal128_array, as_float64_array, as_int32_array};

    use super::*;

    #[test]
    fn test_boolean_to_string() {
        let bool_array: ArrayRef =
            Arc::new(BooleanArray::from_iter(vec![None, Some(true), Some(false)]));
        let casted = cast(&bool_array, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![None, Some("true"), Some("false")])
        );
    }

    #[test]
    fn test_float_to_int() {
        let f64_array: ArrayRef = Arc::new(Float64Array::from_iter(vec![
            None,
            Some(123.456),
            Some(987.654),
            Some(i32::MAX as f64 + 10000.0),
            Some(i32::MIN as f64 - 10000.0),
            Some(f64::INFINITY),
            Some(f64::NEG_INFINITY),
            Some(f64::NAN),
        ]));
        let casted = cast(&f64_array, &DataType::Int32).unwrap();
        assert_eq!(
            as_int32_array(&casted).unwrap(),
            &Int32Array::from_iter(vec![
                None,
                Some(123),
                Some(987),
                Some(i32::MAX),
                Some(i32::MIN),
                Some(i32::MAX),
                Some(i32::MIN),
                Some(0),
            ])
        );
    }

    #[test]
    fn test_int_to_float() {
        let i32_array: ArrayRef = Arc::new(Int32Array::from_iter(vec![
            None,
            Some(123),
            Some(987),
            Some(i32::MAX),
            Some(i32::MIN),
        ]));
        let casted = cast(&i32_array, &DataType::Float64).unwrap();
        assert_eq!(
            as_float64_array(&casted).unwrap(),
            &Float64Array::from_iter(vec![
                None,
                Some(123.0),
                Some(987.0),
                Some(i32::MAX as f64),
                Some(i32::MIN as f64),
            ])
        );
    }

    #[test]
    fn test_int_to_decimal() {
        let i32_array: ArrayRef = Arc::new(Int32Array::from_iter(vec![
            None,
            Some(123),
            Some(987),
            Some(i32::MAX),
            Some(i32::MIN),
        ]));
        let casted = cast(&i32_array, &DataType::Decimal128(38, 18)).unwrap();
        assert_eq!(
            as_decimal128_array(&casted).unwrap(),
            &Decimal128Array::from_iter(vec![
                None,
                Some(123000000000000000000),
                Some(987000000000000000000),
                Some(i32::MAX as i128 * 1000000000000000000),
                Some(i32::MIN as i128 * 1000000000000000000),
            ])
            .with_precision_and_scale(38, 18)
            .unwrap()
        );
    }

    #[test]
    fn test_string_to_decimal() {
        let string_array: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("1e-8"),
            Some("1.012345678911111111e10"),
            Some("1.42e-6"),
            Some("0.00000142"),
            Some("123.456"),
            Some("987.654"),
            Some("123456789012345.678901234567890"),
            Some("-123456789012345.678901234567890"),
        ]));
        let casted = cast(&string_array, &DataType::Decimal128(38, 18)).unwrap();
        assert_eq!(
            as_decimal128_array(&casted).unwrap(),
            &Decimal128Array::from_iter(vec![
                None,
                Some(10000000000),
                Some(10123456789111111110000000000i128),
                Some(1420000000000),
                Some(1420000000000),
                Some(123456000000000000000i128),
                Some(987654000000000000000i128),
                Some(123456789012345678901234567890000i128),
                Some(-123456789012345678901234567890000i128),
            ])
            .with_precision_and_scale(38, 18)
            .unwrap()
        );
    }

    #[test]
    fn test_decimal_to_string() {
        let decimal_array: ArrayRef = Arc::new(
            Decimal128Array::from_iter(vec![
                None,
                Some(123000000000000000000),
                Some(987000000000000000000),
                Some(987654321000000000000),
                Some(i32::MAX as i128 * 1000000000000000000),
                Some(i32::MIN as i128 * 1000000000000000000),
            ])
            .with_precision_and_scale(38, 18)
            .unwrap(),
        );
        let casted = cast(&decimal_array, &DataType::Utf8).unwrap();
        assert_eq!(
            casted.as_any().downcast_ref::<StringArray>().unwrap(),
            &StringArray::from_iter(vec![
                None,
                Some("123.000000000000000000"),
                Some("987.000000000000000000"),
                Some("987.654321000000000000"),
                Some("2147483647.000000000000000000"),
                Some("-2147483648.000000000000000000"),
            ])
        );
    }

    #[test]
    fn test_string_to_bigint() {
        let string_array: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("123"),
            Some("987"),
            Some("987.654"),
            Some("123456789012345"),
            Some("-123456789012345"),
            Some("999999999999999999999999999999999"),
        ]));
        let casted = cast(&string_array, &DataType::Int64).unwrap();
        assert_eq!(
            casted.as_any().downcast_ref::<Int64Array>().unwrap(),
            &Int64Array::from_iter(vec![
                None,
                Some(123),
                Some(987),
                Some(987),
                Some(123456789012345),
                Some(-123456789012345),
                None,
            ])
        );
    }

    #[test]
    fn test_string_to_date() {
        let string_array: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("2001-02-03"),
            Some("2001-03-04"),
            Some("2001-04-05T06:07:08"),
            Some("2001-04"),
            Some("2002"),
            Some("2001-00"),
            Some("2001-13"),
            Some("9999-99"),
            Some("99999-01"),
        ]));
        let casted = cast(&string_array, &DataType::Date32).unwrap();
        assert_eq!(
            arrow::compute::cast(&casted, &DataType::Utf8)
                .unwrap()
                .as_string(),
            &StringArray::from_iter(vec![
                None,
                Some("2001-02-03"),
                Some("2001-03-04"),
                Some("2001-04-05"),
                Some("2001-04-01"),
                Some("2002-01-01"),
                None,
                None,
                None,
                None,
            ])
        );
    }

    #[test]
    fn test_struct_to_string() {
        // Create a struct array with fields: (int32, string, boolean)
        let int_array = Int32Array::from(vec![Some(1), Some(2), None, Some(4), None]);
        let string_array = StringArray::from(vec![Some("a"), None, Some("c"), Some("d"), None]);
        let bool_array = BooleanArray::from(vec![Some(true), Some(false), Some(true), None, None]);

        let struct_array: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("f1", DataType::Int32, true)),
                Arc::new(int_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("f2", DataType::Utf8, true)),
                Arc::new(string_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new("f3", DataType::Boolean, true)),
                Arc::new(bool_array) as ArrayRef,
            ),
        ]));

        let casted = cast(&struct_array, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![
                Some("{1, a, true}"),
                Some("{2, null, false}"),
                Some("{null, c, true}"),
                Some("{4, d, null}"),
                Some("{null, null, null}"),
            ])
        );
    }

    #[test]
    fn test_struct_to_string_with_null_struct() {
        // Create a struct array where some rows are entirely null
        let int_array = Int32Array::from(vec![Some(1), Some(2), Some(3)]);
        let string_array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

        let fields = Fields::from(vec![
            Field::new("f1", DataType::Int32, true),
            Field::new("f2", DataType::Utf8, true),
        ]);

        // Set null at index 1
        let nulls = arrow::buffer::NullBuffer::from(vec![true, false, true]);
        let struct_array_with_nulls: ArrayRef = Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(int_array) as ArrayRef,
                Arc::new(string_array) as ArrayRef,
            ],
            Some(nulls),
        ));

        let casted = cast(&struct_array_with_nulls, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![Some("{1, a}"), None, Some("{3, c}"),])
        );
    }

    #[test]
    fn test_empty_struct_to_string() {
        // Create a struct array with zero fields but 2 rows
        let struct_array: ArrayRef = Arc::new(StructArray::new_empty_fields(2, None));

        let casted = cast(&struct_array, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![Some("{}"), Some("{}")])
        );
    }

    #[test]
    fn test_nested_struct_to_string() {
        // Create a nested struct: struct<int, struct<string, bool>>
        let inner_string = StringArray::from(vec![Some("x"), Some("y")]);
        let inner_bool = BooleanArray::from(vec![Some(true), None]);

        let inner_struct: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("s1", DataType::Utf8, true)),
                Arc::new(inner_string) as ArrayRef,
            ),
            (
                Arc::new(Field::new("s2", DataType::Boolean, true)),
                Arc::new(inner_bool) as ArrayRef,
            ),
        ]));

        let outer_int = Int32Array::from(vec![Some(100), Some(200)]);

        let outer_struct: ArrayRef = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("f1", DataType::Int32, true)),
                Arc::new(outer_int) as ArrayRef,
            ),
            (
                Arc::new(Field::new("f2", inner_struct.data_type().clone(), true)),
                inner_struct,
            ),
        ]));

        let casted = cast(&outer_struct, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![Some("{100, {x, true}}"), Some("{200, {y, null}}"),])
        );
    }

    #[test]
    fn test_map_to_string() {
        // Create a map array: Map<Int32, String>
        let key_field = Arc::new(Field::new("key", DataType::Int32, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            false,
        ));

        let keys = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let values = StringArray::from(vec![Some("a"), Some("b"), None, Some("d"), Some("e")]);

        let entries = StructArray::from(vec![
            (key_field.clone(), Arc::new(keys) as ArrayRef),
            (value_field.clone(), Arc::new(values) as ArrayRef),
        ]);

        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 3, 5].into());
        let map_array: ArrayRef = Arc::new(MapArray::new(
            entries_field.clone(),
            offsets,
            entries,
            None,
            false,
        ));

        let casted = cast(&map_array, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![
                Some("{1 -> a, 2 -> b}"),
                Some("{3 -> null}"),
                Some("{4 -> d, 5 -> e}"),
            ])
        );
    }

    #[test]
    fn test_map_to_string_with_null_map() {
        // Create a map array with null rows
        let key_field = Arc::new(Field::new("key", DataType::Int32, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            false,
        ));

        let keys = Int32Array::from(vec![1, 2, 3]);
        let values = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

        let entries = StructArray::from(vec![
            (key_field.clone(), Arc::new(keys) as ArrayRef),
            (value_field.clone(), Arc::new(values) as ArrayRef),
        ]);

        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 1, 2, 3].into());
        let nulls = arrow::buffer::NullBuffer::from(vec![true, false, true]);
        let map_array: ArrayRef = Arc::new(MapArray::new(
            entries_field.clone(),
            offsets,
            entries,
            Some(nulls),
            false,
        ));

        let casted = cast(&map_array, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![Some("{1 -> a}"), None, Some("{3 -> c}"),])
        );
    }

    #[test]
    fn test_empty_map_to_string() {
        // Create an empty map array
        let key_field = Arc::new(Field::new("key", DataType::Int32, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            false,
        ));

        let keys = Int32Array::from(vec![] as Vec<i32>);
        let values = StringArray::from(vec![] as Vec<Option<&str>>);

        let entries = StructArray::from(vec![
            (key_field.clone(), Arc::new(keys) as ArrayRef),
            (value_field.clone(), Arc::new(values) as ArrayRef),
        ]);

        // Two rows, both empty maps
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 0, 0].into());
        let map_array: ArrayRef = Arc::new(MapArray::new(
            entries_field.clone(),
            offsets,
            entries,
            None,
            false,
        ));

        let casted = cast(&map_array, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![Some("{}"), Some("{}")])
        );
    }

    #[test]
    fn test_nested_map_to_string() {
        // Create a map with struct values: Map<Int32, Struct<String, Boolean>>
        let key_field = Arc::new(Field::new("key", DataType::Int32, false));

        let inner_string_field = Arc::new(Field::new("s1", DataType::Utf8, true));
        let inner_bool_field = Arc::new(Field::new("s2", DataType::Boolean, true));
        let inner_struct_type = DataType::Struct(Fields::from(vec![
            inner_string_field.as_ref().clone(),
            inner_bool_field.as_ref().clone(),
        ]));

        let value_field = Arc::new(Field::new("value", inner_struct_type.clone(), true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            false,
        ));

        let keys = Int32Array::from(vec![1, 2]);
        let inner_strings = StringArray::from(vec![Some("x"), Some("y")]);
        let inner_bools = BooleanArray::from(vec![Some(true), None]);
        let inner_struct: ArrayRef = Arc::new(StructArray::from(vec![
            (inner_string_field, Arc::new(inner_strings) as ArrayRef),
            (inner_bool_field, Arc::new(inner_bools) as ArrayRef),
        ]));

        let entries = StructArray::from(vec![
            (key_field.clone(), Arc::new(keys) as ArrayRef),
            (value_field.clone(), inner_struct),
        ]);

        // One row with 2 entries
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2].into());
        let map_array: ArrayRef = Arc::new(MapArray::new(
            entries_field.clone(),
            offsets,
            entries,
            None,
            false,
        ));

        let casted = cast(&map_array, &DataType::Utf8).unwrap();
        assert_eq!(
            as_string_array(&casted),
            &StringArray::from_iter(vec![Some("{1 -> {x, true}, 2 -> {y, null}}")])
        );
    }
}
