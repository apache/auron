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

use crate::create_auron_ext_udf;
use arrow::{
    array::{
        Array, ArrayRef, AsArray, Int32Array, Int64Array, ListArray, NullArray, StringArray,
        StructArray,
    },
    datatypes::{DataType, Field, Int32Type, TimeUnit},
};
use datafusion::{
    common::{Result, ScalarValue, config::ConfigOptions},
    logical_expr::{ColumnarValue, ScalarFunctionArgs},
};

fn invoke(
    name: &str,
    values: Vec<ColumnarValue>,
    return_type: DataType,
    rows: usize,
) -> Result<ColumnarValue> {
    let types = values
        .iter()
        .map(ColumnarValue::data_type)
        .collect::<Vec<_>>();
    let udf = create_auron_ext_udf(name, 0, types.clone(), return_type.clone())?;
    udf.invoke_with_args(ScalarFunctionArgs {
        args: values,
        arg_fields: types
            .into_iter()
            .map(|dt| Arc::new(Field::new("arg", dt, true)))
            .collect(),
        number_rows: rows,
        return_field: Arc::new(Field::new("result", return_type, true)),
        config_options: Arc::new(ConfigOptions::default()),
    })
}

#[test]
fn sha_widths_are_distinct_udfs() -> Result<()> {
    let sha224 = create_auron_ext_udf("Spark_Sha224", 0, vec![DataType::Binary], DataType::Utf8)?;
    let sha256 = create_auron_ext_udf("Spark_Sha256", 0, vec![DataType::Binary], DataType::Utf8)?;
    assert_ne!(sha224, sha256);
    for (name, expected) in [
        (
            "Spark_Sha224",
            "d14a028c2a3a2bc9476102bb288234c415a2b01f828ea62ac5b3e42f",
        ),
        (
            "Spark_Sha256",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
    ] {
        let value = invoke(
            name,
            vec![ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![])))],
            DataType::Utf8,
            4,
        )?;
        assert_eq!(
            ScalarValue::try_from_array(value.into_array(1)?.as_ref(), 0)?,
            ScalarValue::Utf8(Some(expected.into()))
        );
    }
    Ok(())
}

#[test]
fn xxhash64_scalar_array_and_empty_inputs() -> Result<()> {
    let scalar = ColumnarValue::Scalar(ScalarValue::Int64(Some(1)));
    for rows in [0, 1, 4] {
        let result = invoke(
            "Spark_XxHash64",
            vec![scalar.clone()],
            DataType::Int64,
            rows,
        )?;
        assert!(matches!(result, ColumnarValue::Scalar(_)));
        assert_eq!(
            ScalarValue::try_from_array(result.into_array(1)?.as_ref(), 0)?,
            ScalarValue::Int64(Some(-7001672635703045582))
        );
    }
    for values in [vec![], vec![Some(1)], vec![Some(1), None, Some(0)]] {
        let input = Int64Array::from(values);
        let rows = input.len();
        let expected: ArrayRef = Arc::new(
            Int64Array::from(vec![
                Some(-7001672635703045582),
                Some(42),
                Some(-5252525462095825812),
            ])
            .slice(0, rows),
        );
        let result = invoke(
            "Spark_XxHash64",
            vec![ColumnarValue::Array(Arc::new(input))],
            DataType::Int64,
            rows,
        )?;
        assert!(matches!(result, ColumnarValue::Array(_)));
        assert_eq!(&result.into_array(rows)?, &expected);
    }
    Ok(())
}

#[test]
fn date_parts_keep_function_identity_and_session_timezone() -> Result<()> {
    let timestamp =
        ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(Some(1709251200000), None));
    let timezone = ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/Los_Angeles".into())));
    let types = vec![
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Utf8,
    ];
    let year = create_auron_ext_udf("Spark_Year", 0, types.clone(), DataType::Int32)?;
    let month = create_auron_ext_udf("Spark_Month", 0, types, DataType::Int32)?;
    assert_ne!(year, month);
    for (name, expected) in [
        ("Spark_Year", 2024),
        ("Spark_Month", 2),
        ("Spark_Day", 29),
        ("Spark_Hour", 16),
        ("Spark_DayOfWeek", 5),
    ] {
        let result = invoke(
            name,
            vec![timestamp.clone(), timezone.clone()],
            DataType::Int32,
            4,
        )?;
        assert_eq!(
            &result.into_array(4)?,
            &(Arc::new(Int32Array::from(vec![expected; 4])) as ArrayRef)
        );
    }
    Ok(())
}

#[test]
fn strings_preserve_nulls_unicode_and_scalar_broadcast() -> Result<()> {
    let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![
        Some("你好abc"),
        None,
        Some("xyz"),
    ])));
    let result = invoke(
        "Spark_Substring",
        vec![
            input,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(2))),
        ],
        DataType::Utf8,
        3,
    )?;
    assert_eq!(
        &result.into_array(3)?,
        &(Arc::new(StringArray::from(vec![Some("好a"), None, Some("yz")])) as ArrayRef)
    );
    Ok(())
}

#[test]
fn sha2_coerces_string_arrays_before_invocation() -> Result<()> {
    let input = ColumnarValue::Array(Arc::new(StringArray::from(vec![
        Some(""),
        Some("abc"),
        None,
    ])));
    let result = invoke("Spark_Sha256", vec![input], DataType::Utf8, 3)?;
    let expected: ArrayRef = Arc::new(StringArray::from(vec![
        Some("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
        Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"),
        None,
    ]));
    assert_eq!(&result.into_array(3)?, &expected);
    Ok(())
}

#[test]
fn make_array_coerces_nested_nullability_before_flatten() -> Result<()> {
    let nonnull_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
    let input = ListArray::from_iter_primitive::<Int32Type, _, _>([
        Some(vec![Some(1), Some(2), Some(3)]),
        Some(vec![]),
        None,
    ]);
    let input = datafusion_ext_commons::arrow::cast::cast(&input, &nonnull_type)?;
    let literal = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![None, Some(5)])]);
    let element_type = literal.data_type().clone();
    let return_type = DataType::List(Arc::new(Field::new("item", element_type.clone(), true)));
    for rows in [1, 0, 3] {
        let result = invoke(
            "Spark_MakeArray",
            vec![
                ColumnarValue::Array(input.slice(0, rows)),
                ColumnarValue::Scalar(ScalarValue::List(Arc::new(literal.clone()))),
            ],
            return_type.clone(),
            rows,
        )?;
        assert_eq!(result.data_type(), return_type);
        let flattened = invoke(
            "Spark_ArrayFlatten",
            vec![result],
            element_type.clone(),
            rows,
        )?;
        let expected: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), Some(2), Some(3), None, Some(5)]),
            Some(vec![None, Some(5)]),
            None,
        ]));
        assert_eq!(&flattened.into_array(rows)?, &expected.slice(0, rows));
    }
    Ok(())
}

#[test]
fn make_array_preserves_null_column_row_counts() -> Result<()> {
    let return_type = DataType::List(Arc::new(Field::new("element", DataType::Null, true)));
    for rows in [0, 1, 3] {
        for extra_null in [false, true] {
            let mut args = vec![ColumnarValue::Array(Arc::new(NullArray::new(rows)))];
            if extra_null {
                args.push(ColumnarValue::Scalar(ScalarValue::Null));
            }
            let width = args.len();
            let result = invoke("Spark_MakeArray", args, return_type.clone(), rows)?;
            assert!(matches!(result, ColumnarValue::Array(_)));
            let result = result.into_array(rows)?;
            let lists = result.as_list::<i32>();
            assert_eq!(lists.len(), rows);
            assert_eq!(lists.null_count(), 0);
            for row in 0..rows {
                assert_eq!(lists.value_length(row) as usize, width);
                assert_eq!(lists.value(row).logical_null_count(), width);
            }
        }
    }
    Ok(())
}

#[test]
fn make_array_preserves_scalar_and_empty_results() -> Result<()> {
    for rows in [0, 1, 3] {
        for count in [0, 1, 2] {
            let return_type = DataType::List(Arc::new(Field::new("item", DataType::Null, true)));
            let result = invoke(
                "Spark_MakeArray",
                vec![ColumnarValue::Scalar(ScalarValue::Null); count],
                return_type.clone(),
                rows,
            )?;
            assert!(matches!(result, ColumnarValue::Scalar(_)));
            assert_eq!(result.data_type(), return_type);
            let result = result.into_array(rows)?;
            let lists = result.as_list::<i32>();
            assert_eq!(lists.len(), rows);
            for row in 0..rows {
                assert_eq!(lists.value_length(row) as usize, count);
            }
        }
    }
    let return_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
    let result = invoke(
        "Spark_MakeArray",
        vec![ColumnarValue::Scalar(ScalarValue::Int32(Some(7)))],
        return_type.clone(),
        3,
    )?;
    assert!(matches!(result, ColumnarValue::Scalar(_)));
    assert_eq!(result.data_type(), return_type);
    assert_eq!(
        result
            .into_array(3)?
            .as_list::<i32>()
            .values()
            .as_primitive::<Int32Type>()
            .values(),
        &[7, 7, 7]
    );
    Ok(())
}

#[test]
fn make_array_coerces_struct_fields_by_position() -> Result<()> {
    let input = StructArray::from(vec![(
        Arc::new(Field::new("left", DataType::Int32, false)),
        Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
    )]);
    let literal = StructArray::from(vec![(
        Arc::new(Field::new("right", DataType::Int32, true)),
        Arc::new(Int32Array::from(vec![None])) as ArrayRef,
    )]);
    let element_type = DataType::Struct(vec![Field::new("value", DataType::Int32, true)].into());
    let return_type = DataType::List(Arc::new(Field::new("item", element_type, false)));
    let result = invoke(
        "Spark_MakeArray",
        vec![
            ColumnarValue::Array(Arc::new(input)),
            ColumnarValue::Scalar(ScalarValue::Struct(Arc::new(literal))),
        ],
        return_type.clone(),
        2,
    )?;
    assert_eq!(result.data_type(), return_type);
    let result = result.into_array(2)?;
    let lists = result.as_list::<i32>();
    assert_eq!(lists.value_offsets(), &[0, 2, 4]);
    assert_eq!(
        lists.values().as_struct().column(0).as_ref(),
        &Int32Array::from(vec![Some(1), None, Some(2), None]) as &dyn Array
    );
    Ok(())
}
