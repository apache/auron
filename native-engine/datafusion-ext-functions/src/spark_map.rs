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

use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, ListArray, MapArray, StructArray, new_empty_array},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{df_execution_err, scalar_value::compacted_scalar_value_from_array};

fn get_map_type(args: &[ColumnarValue]) -> Result<(Arc<Field>, bool)> {
    if args.is_empty() {
        return df_execution_err!("map_concat requires at least one map argument");
    }

    let (entries_field, ordered) = match args.iter().find_map(|arg| match arg.data_type() {
        DataType::Map(entries_field, ordered) => Some((entries_field, ordered)),
        DataType::Null => None,
        _ => None,
    }) {
        Some((entries_field, ordered)) => (entries_field, ordered),
        None => {
            return df_execution_err!("map_concat args must be map");
        }
    };

    validate_map_arg_types(args, &entries_field, ordered)?;
    Ok((entries_field, ordered))
}

fn validate_map_arg_types(
    args: &[ColumnarValue],
    expected_entries_field: &Arc<Field>,
    expected_ordered: bool,
) -> Result<()> {
    for arg in args {
        match arg.data_type() {
            DataType::Map(entries_field, ordered) => {
                if entries_field != *expected_entries_field || ordered != expected_ordered {
                    return df_execution_err!(
                        "map_concat requires all map args to have the same type, expected {:?}, found {:?}",
                        DataType::Map(expected_entries_field.clone(), expected_ordered),
                        DataType::Map(entries_field, ordered)
                    );
                }
            }
            DataType::Null => {}
            data_type => {
                return df_execution_err!("map_concat args must be map, found {data_type:?}");
            }
        }
    }
    Ok(())
}

fn extract_map_entry_fields(entries_field: &Arc<Field>) -> Result<(Arc<Field>, Arc<Field>)> {
    let fields = match entries_field.data_type() {
        DataType::Struct(fields) => fields,
        _ => return df_execution_err!("map_concat map entries field must be struct"),
    };

    if fields.len() != 2 {
        return df_execution_err!(
            "map_concat map entries struct must contain exactly 2 fields, found {}",
            fields.len()
        );
    }

    Ok((fields[0].clone(), fields[1].clone()))
}

fn new_null_map_array(entries_field: Arc<Field>, ordered: bool, len: usize) -> Result<MapArray> {
    let (key_field, value_field) = extract_map_entry_fields(&entries_field)?;

    let entries = StructArray::from(vec![
        (
            key_field.clone(),
            new_empty_array(key_field.data_type()) as ArrayRef,
        ),
        (
            value_field.clone(),
            new_empty_array(value_field.data_type()) as ArrayRef,
        ),
    ]);

    Ok(MapArray::new(
        entries_field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32; len + 1])),
        entries,
        Some(NullBuffer::from(vec![false; len])),
        ordered,
    ))
}

fn as_map_array(array: &ArrayRef) -> Result<MapArray> {
    array
        .as_any()
        .downcast_ref::<MapArray>()
        .cloned()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "map_concat args must be map, found {:?}",
                array.data_type()
            ))
        })
}

fn columnar_value_to_map_array(
    arg: &ColumnarValue,
    entries_field: &Arc<Field>,
    ordered: bool,
) -> Result<MapArray> {
    match arg {
        ColumnarValue::Array(array) if matches!(array.data_type(), DataType::Null) => {
            new_null_map_array(entries_field.clone(), ordered, array.len())
        }
        ColumnarValue::Array(array) => as_map_array(array),
        ColumnarValue::Scalar(scalar) if scalar.is_null() => {
            new_null_map_array(entries_field.clone(), ordered, 1)
        }
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;
            as_map_array(&array)
        }
    }
}

fn get_arg_arrays(
    args: &[ColumnarValue],
    entries_field: &Arc<Field>,
    ordered: bool,
) -> Result<Vec<MapArray>> {
    args.iter()
        .map(|arg| columnar_value_to_map_array(arg, entries_field, ordered))
        .collect()
}

fn as_list_array(array: &ArrayRef) -> Result<ListArray> {
    array
        .as_any()
        .downcast_ref::<ListArray>()
        .cloned()
        .ok_or_else(|| {
            datafusion::error::DataFusionError::Execution(format!(
                "map_from_arrays args must be array, found {:?}",
                array.data_type()
            ))
        })
}

fn new_null_list_field() -> Arc<Field> {
    Arc::new(Field::new_list_field(DataType::Null, true))
}

fn new_null_list_array(list_field: Arc<Field>, len: usize) -> ListArray {
    ListArray::new_null(list_field, len)
}

fn get_list_array_field(array: &ListArray, arg_name: &str) -> Result<Arc<Field>> {
    match array.data_type() {
        DataType::List(field) => Ok(field.clone()),
        data_type => {
            df_execution_err!("map_from_arrays {arg_name} arg must be array, found {data_type:?}")
        }
    }
}

fn columnar_value_to_list_array(arg: &ColumnarValue, arg_name: &str) -> Result<ListArray> {
    match arg {
        ColumnarValue::Array(array) if matches!(array.data_type(), DataType::Null) => {
            Ok(new_null_list_array(new_null_list_field(), array.len()))
        }
        ColumnarValue::Array(array) => as_list_array(array),
        ColumnarValue::Scalar(scalar) if scalar.is_null() => {
            let list_field = match scalar.data_type() {
                DataType::List(field) => field,
                _ => new_null_list_field(),
            };
            Ok(new_null_list_array(list_field, 1))
        }
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;
            if matches!(array.data_type(), DataType::Null) {
                let list_field = match scalar.data_type() {
                    DataType::List(field) => field,
                    _ => new_null_list_field(),
                };
                Ok(new_null_list_array(list_field, array.len()))
            } else {
                as_list_array(&array)
            }
        }
    }
    .map_err(|err| {
        datafusion::error::DataFusionError::Execution(format!(
            "map_from_arrays {arg_name} arg must be array: {err}"
        ))
    })
}

/// Returns the union of all given maps.
///
/// This follows Spark's default duplicate-key behavior by raising an error,
/// and propagates null when any input map for a row is null.
pub fn map_concat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let (entries_field, ordered) = get_map_type(args)?;
    let arg_arrays = get_arg_arrays(args, &entries_field, ordered)?;

    let num_rows = arg_arrays
        .iter()
        .map(|array| array.len())
        .filter(|&len| len != 1)
        .max()
        .unwrap_or(1);

    if arg_arrays
        .iter()
        .any(|array| array.len() != 1 && array.len() != num_rows)
    {
        return df_execution_err!("all maps of map_concat must have the same length");
    }

    let (key_field, value_field) = extract_map_entry_fields(&entries_field)?;

    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();
    let mut offsets = Vec::with_capacity(num_rows + 1);
    let mut valids = Vec::with_capacity(num_rows);
    let mut next_offset = 0i32;

    offsets.push(next_offset);

    for row_idx in 0..num_rows {
        let mut row_keys = HashSet::new();
        let mut row_entries: Vec<(ScalarValue, ScalarValue)> = Vec::new();
        let mut row_is_null = false;

        for array in &arg_arrays {
            let idx = if array.len() == 1 { 0 } else { row_idx };

            if array.is_null(idx) {
                row_is_null = true;
                break;
            }

            let entries = array.value(idx);
            let entries = entries
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "map_concat expects map entries to be struct".to_string(),
                    )
                })?;

            let keys = entries.column(0);
            let values = entries.column(1);

            for i in 0..entries.len() {
                if keys.is_null(i) {
                    return df_execution_err!("map_concat does not support null map keys");
                }

                let key = compacted_scalar_value_from_array(keys.as_ref(), i)?;
                if !row_keys.insert(key.clone()) {
                    return df_execution_err!("map_concat duplicate key found: {key}");
                }

                let value = compacted_scalar_value_from_array(values.as_ref(), i)?;
                row_entries.push((key, value));
            }
        }

        if row_is_null {
            valids.push(false);
            offsets.push(next_offset);
            continue;
        }

        valids.push(true);
        next_offset += row_entries.len() as i32;
        offsets.push(next_offset);

        for (key, value) in row_entries {
            all_keys.push(key);
            all_values.push(value);
        }
    }

    let keys = if all_keys.is_empty() {
        new_empty_array(key_field.data_type())
    } else {
        ScalarValue::iter_to_array(all_keys.into_iter())?
    };

    let values = if all_values.is_empty() {
        new_empty_array(value_field.data_type())
    } else {
        ScalarValue::iter_to_array(all_values.into_iter())?
    };

    let entries = StructArray::from(vec![(key_field, keys), (value_field, values)]);
    let nulls = if valids.iter().all(|valid| *valid) {
        None
    } else {
        Some(NullBuffer::from(valids))
    };

    Ok(ColumnarValue::Array(Arc::new(MapArray::new(
        entries_field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        nulls,
        ordered,
    ))))
}

/// Creates a map from the given key/value arrays.
///
/// This follows Spark semantics by propagating null when either input array
/// for a row is null, rejecting null keys, and raising an error on duplicate
/// keys under Spark's default map-key dedup policy.
pub fn map_from_arrays(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return df_execution_err!("map_from_arrays requires exactly 2 arguments");
    }

    let key_array = columnar_value_to_list_array(&args[0], "keys")?;
    let value_array = columnar_value_to_list_array(&args[1], "values")?;
    let key_list_field = get_list_array_field(&key_array, "keys")?;
    let value_list_field = get_list_array_field(&value_array, "values")?;

    let key_field = Arc::new(Field::new("key", key_list_field.data_type().clone(), false));
    let value_field = Arc::new(Field::new(
        "value",
        value_list_field.data_type().clone(),
        value_list_field.is_nullable(),
    ));
    let entries_field = Arc::new(Field::new(
        "entries",
        DataType::Struct(vec![key_field.as_ref().clone(), value_field.as_ref().clone()].into()),
        false,
    ));

    let num_rows = [key_array.len(), value_array.len()]
        .into_iter()
        .filter(|&len| len != 1)
        .max()
        .unwrap_or(1);

    if [key_array.len(), value_array.len()]
        .into_iter()
        .any(|len| len != 1 && len != num_rows)
    {
        return df_execution_err!("all arrays of map_from_arrays must have the same length");
    }

    let mut all_keys = Vec::new();
    let mut all_values = Vec::new();
    let mut offsets = Vec::with_capacity(num_rows + 1);
    let mut valids = Vec::with_capacity(num_rows);
    let mut next_offset = 0i32;

    offsets.push(next_offset);

    for row_idx in 0..num_rows {
        let key_idx = if key_array.len() == 1 { 0 } else { row_idx };
        let value_idx = if value_array.len() == 1 { 0 } else { row_idx };

        if key_array.is_null(key_idx) || value_array.is_null(value_idx) {
            valids.push(false);
            offsets.push(next_offset);
            continue;
        }

        let keys = key_array.value(key_idx);
        let values = value_array.value(value_idx);

        if keys.len() != values.len() {
            return df_execution_err!(
                "map_from_arrays requires key/value arrays to have the same length"
            );
        }

        let mut row_keys = HashSet::new();
        valids.push(true);

        for i in 0..keys.len() {
            if keys.is_null(i) {
                return df_execution_err!("map_from_arrays does not support null map keys");
            }

            let key = compacted_scalar_value_from_array(keys.as_ref(), i)?;
            if !row_keys.insert(key.clone()) {
                return df_execution_err!("map_from_arrays duplicate key found: {key}");
            }

            let value = compacted_scalar_value_from_array(values.as_ref(), i)?;
            all_keys.push(key);
            all_values.push(value);
        }

        next_offset += keys.len() as i32;
        offsets.push(next_offset);
    }

    let keys = if all_keys.is_empty() {
        new_empty_array(key_field.data_type())
    } else {
        ScalarValue::iter_to_array(all_keys.into_iter())?
    };

    let values = if all_values.is_empty() {
        new_empty_array(value_field.data_type())
    } else {
        ScalarValue::iter_to_array(all_values.into_iter())?
    };

    let entries = StructArray::from(vec![(key_field, keys), (value_field, values)]);
    let nulls = if valids.iter().all(|valid| *valid) {
        None
    } else {
        Some(NullBuffer::from(valids))
    };

    Ok(ColumnarValue::Array(Arc::new(MapArray::new(
        entries_field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        entries,
        nulls,
        false,
    ))))
}

#[cfg(test)]
mod test {
    use arrow::{
        array::{Int32Array, Int32Builder, ListBuilder, NullArray, StringArray, StringBuilder},
        datatypes::Fields,
    };

    use super::*;

    type StringIntMapEntries = Vec<(&'static str, Option<i32>)>;
    type StringIntMapRow = Option<StringIntMapEntries>;
    type StringStringMapEntries = Vec<(&'static str, Option<&'static str>)>;
    type StringStringMapRow = Option<StringStringMapEntries>;

    fn build_string_int_map_array(rows: Vec<StringIntMapRow>) -> MapArray {
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Int32, true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            false,
        ));

        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        let mut valids = Vec::with_capacity(rows.len());
        let mut next_offset = 0i32;
        offsets.push(next_offset);

        for row in rows {
            match row {
                Some(entries) => {
                    valids.push(true);
                    next_offset += entries.len() as i32;
                    offsets.push(next_offset);
                    for (key, value) in entries {
                        keys.push(key);
                        values.push(value);
                    }
                }
                None => {
                    valids.push(false);
                    offsets.push(next_offset);
                }
            }
        }

        let entries = StructArray::from(vec![
            (
                key_field.clone(),
                Arc::new(StringArray::from(keys)) as ArrayRef,
            ),
            (
                value_field.clone(),
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ),
        ]);

        let nulls = if valids.iter().all(|valid| *valid) {
            None
        } else {
            Some(NullBuffer::from(valids))
        };

        MapArray::new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            nulls,
            false,
        )
    }

    fn build_string_string_map_array(rows: Vec<StringStringMapRow>) -> MapArray {
        let key_field = Arc::new(Field::new("key", DataType::Utf8, false));
        let value_field = Arc::new(Field::new("value", DataType::Utf8, true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                key_field.as_ref().clone(),
                value_field.as_ref().clone(),
            ])),
            false,
        ));

        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        let mut valids = Vec::with_capacity(rows.len());
        let mut next_offset = 0i32;
        offsets.push(next_offset);

        for row in rows {
            match row {
                Some(entries) => {
                    valids.push(true);
                    next_offset += entries.len() as i32;
                    offsets.push(next_offset);
                    for (key, value) in entries {
                        keys.push(key);
                        values.push(value);
                    }
                }
                None => {
                    valids.push(false);
                    offsets.push(next_offset);
                }
            }
        }

        let entries = StructArray::from(vec![
            (
                key_field.clone(),
                Arc::new(StringArray::from(keys)) as ArrayRef,
            ),
            (
                value_field.clone(),
                Arc::new(StringArray::from(values)) as ArrayRef,
            ),
        ]);

        let nulls = if valids.iter().all(|valid| *valid) {
            None
        } else {
            Some(NullBuffer::from(valids))
        };

        MapArray::new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            entries,
            nulls,
            false,
        )
    }

    fn build_string_list_array(rows: Vec<Option<Vec<Option<&'static str>>>>) -> ListArray {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for row in rows {
            match row {
                Some(values) => {
                    for value in values {
                        match value {
                            Some(value) => builder.values().append_value(value),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        builder.finish()
    }

    fn build_int_list_array(rows: Vec<Option<Vec<Option<i32>>>>) -> ListArray {
        let mut builder = ListBuilder::new(Int32Builder::new());
        for row in rows {
            match row {
                Some(values) => {
                    for value in values {
                        match value {
                            Some(value) => builder.values().append_value(value),
                            None => builder.values().append_null(),
                        }
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        builder.finish()
    }

    #[test]
    fn test_map_concat() -> Result<()> {
        let left = build_string_int_map_array(vec![
            Some(vec![("a", Some(1)), ("b", Some(2))]),
            Some(vec![("x", Some(10))]),
        ]);
        let right = build_string_int_map_array(vec![
            Some(vec![("c", Some(3))]),
            Some(vec![("y", None), ("z", Some(30))]),
        ]);

        let actual = map_concat(&[
            ColumnarValue::Array(Arc::new(left)),
            ColumnarValue::Array(Arc::new(right)),
        ])?
        .into_array(2)?;

        let expected = Arc::new(build_string_int_map_array(vec![
            Some(vec![("a", Some(1)), ("b", Some(2)), ("c", Some(3))]),
            Some(vec![("x", Some(10)), ("y", None), ("z", Some(30))]),
        ])) as ArrayRef;

        assert_eq!(&actual, &expected);
        Ok(())
    }

    #[test]
    fn test_map_concat_null_propagation() -> Result<()> {
        let left = build_string_int_map_array(vec![Some(vec![("a", Some(1))]), None]);
        let right = build_string_int_map_array(vec![
            Some(vec![("b", Some(2))]),
            Some(vec![("c", Some(3))]),
        ]);

        let actual = map_concat(&[
            ColumnarValue::Array(Arc::new(left)),
            ColumnarValue::Array(Arc::new(right)),
        ])?
        .into_array(2)?;

        let expected = Arc::new(build_string_int_map_array(vec![
            Some(vec![("a", Some(1)), ("b", Some(2))]),
            None,
        ])) as ArrayRef;

        assert_eq!(&actual, &expected);
        Ok(())
    }

    #[test]
    fn test_map_concat_duplicate_keys() {
        let left = build_string_int_map_array(vec![Some(vec![("a", Some(1))])]);
        let right = build_string_int_map_array(vec![Some(vec![("a", Some(2))])]);

        let err = map_concat(&[
            ColumnarValue::Array(Arc::new(left)),
            ColumnarValue::Array(Arc::new(right)),
        ])
        .expect_err("map_concat should fail when duplicate keys exist");

        assert!(err.to_string().contains("duplicate key"));
    }

    #[test]
    fn test_map_concat_mismatched_map_types() {
        let left = build_string_int_map_array(vec![Some(vec![("a", Some(1))])]);
        let right = build_string_string_map_array(vec![Some(vec![("b", Some("x"))])]);

        let err = map_concat(&[
            ColumnarValue::Array(Arc::new(left)),
            ColumnarValue::Array(Arc::new(right)),
        ])
        .expect_err("map_concat should fail when map types differ");

        assert!(err.to_string().contains("same type"));
    }

    #[test]
    fn test_map_concat_length_mismatch() {
        let left = build_string_int_map_array(vec![
            Some(vec![("a", Some(1))]),
            Some(vec![("b", Some(2))]),
        ]);
        let right = build_string_int_map_array(vec![
            Some(vec![("c", Some(3))]),
            Some(vec![("d", Some(4))]),
            Some(vec![("e", Some(5))]),
        ]);

        let err = map_concat(&[
            ColumnarValue::Array(Arc::new(left)),
            ColumnarValue::Array(Arc::new(right)),
        ])
        .expect_err("map_concat should fail when input map array lengths differ");

        assert!(err.to_string().contains("same length"));
    }

    #[test]
    fn test_map_from_arrays() -> Result<()> {
        let keys = Arc::new(build_string_list_array(vec![
            Some(vec![Some("a"), Some("b")]),
            Some(vec![Some("x")]),
            None,
            Some(vec![Some("m"), Some("n")]),
        ])) as ArrayRef;
        let values = Arc::new(build_int_list_array(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(10)]),
            Some(vec![Some(20)]),
            Some(vec![None, Some(30)]),
        ])) as ArrayRef;

        let actual = map_from_arrays(&[ColumnarValue::Array(keys), ColumnarValue::Array(values)])?
            .into_array(4)?;

        let expected = Arc::new(build_string_int_map_array(vec![
            Some(vec![("a", Some(1)), ("b", Some(2))]),
            Some(vec![("x", Some(10))]),
            None,
            Some(vec![("m", None), ("n", Some(30))]),
        ])) as ArrayRef;

        assert_eq!(&actual, &expected);
        Ok(())
    }

    #[test]
    fn test_map_from_arrays_rejects_null_keys() {
        let keys = Arc::new(build_string_list_array(vec![Some(vec![Some("a"), None])])) as ArrayRef;
        let values = Arc::new(build_int_list_array(vec![Some(vec![Some(1), Some(2)])])) as ArrayRef;

        let err = map_from_arrays(&[ColumnarValue::Array(keys), ColumnarValue::Array(values)])
            .expect_err("map_from_arrays should fail when null keys exist");

        assert!(err.to_string().contains("null map keys"));
    }

    #[test]
    fn test_map_from_arrays_null_array_propagation() -> Result<()> {
        let keys = Arc::new(NullArray::new(1)) as ArrayRef;
        let values = Arc::new(build_int_list_array(vec![Some(vec![Some(1), Some(2)])])) as ArrayRef;

        let actual = map_from_arrays(&[ColumnarValue::Array(keys), ColumnarValue::Array(values)])?
            .into_array(1)?;

        assert!(actual.is_null(0));
        Ok(())
    }

    #[test]
    fn test_map_from_arrays_null_scalar_propagation() -> Result<()> {
        let values = Arc::new(build_int_list_array(vec![Some(vec![Some(1), Some(2)])])) as ArrayRef;

        let actual = map_from_arrays(&[
            ColumnarValue::Scalar(ScalarValue::Null),
            ColumnarValue::Array(values),
        ])?
        .into_array(1)?;

        assert!(actual.is_null(0));
        Ok(())
    }
}
