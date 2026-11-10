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
    array::{
        Array, ArrayRef, AsArray, BinaryArray, ListArray, ListBuilder, StringArray, StringBuilder,
    },
    datatypes::DataType,
};
use datafusion::{
    common::{
        Result, ScalarValue,
        cast::{as_binary_array, as_int32_array, as_int64_array, as_list_array, as_string_array},
    },
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

pub fn string_chr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let chr = |n: i64| {
        if n < 0 {
            String::new()
        } else {
            char::from(n as u8).to_string()
        }
    };
    match &args[0] {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
            as_int64_array(array)?.iter().map(|n| n.map(chr)),
        )))),
        ColumnarValue::Scalar(ScalarValue::Int64(n)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(n.map(chr))))
        }
        _ => df_execution_err!("chr only supports int64"),
    }
}

pub fn string_lower(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
            as_string_array(array)?
                .into_iter()
                .map(|s| s.map(|s| s.to_lowercase())),
        )))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(str))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(str.to_lowercase())),
        )),
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        _ => df_execution_err!("string_lower only supports literal utf8"),
    }
}

pub fn string_upper(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(StringArray::from_iter(
            as_string_array(array)?
                .into_iter()
                .map(|s| s.map(|s| s.to_uppercase())),
        )))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(str))) => Ok(ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(str.to_uppercase())),
        )),
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)))
        }
        _ => df_execution_err!("string_upper only supports literal utf8"),
    }
}

pub fn string_repeat(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let string_array = args[0].clone().into_array(1)?;
    let n = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(n))) => (*n).max(0),
        ColumnarValue::Scalar(scalar) if scalar.is_null() => {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None)));
        }
        _ => df_execution_err!("string_repeat n only supports literal int32")?,
    };

    let repeated_string_array: ArrayRef = Arc::new(StringArray::from_iter(
        as_string_array(&string_array)?
            .into_iter()
            .map(|s| s.map(|s| s.repeat(n as usize))),
    ));
    Ok(ColumnarValue::Array(repeated_string_array))
}

pub fn string_split(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let string_array = args[0].clone().into_array(1)?;
    let pat = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(pat))) if !pat.is_empty() => pat,
        _ => df_execution_err!("string_split pattern only supports non-empty literal string")?,
    };

    let mut splitted_builder = ListBuilder::new(StringBuilder::new());
    for s in as_string_array(&string_array)? {
        match s {
            Some(s) => {
                for segment in s.split(pat) {
                    splitted_builder.values().append_value(segment);
                }
                splitted_builder.append(true);
            }
            None => {
                splitted_builder.append_null();
            }
        }
    }
    Ok(ColumnarValue::Array(Arc::new(splitted_builder.finish())))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        BinaryArray, Int32Array, Int64Array, ListBuilder, StringArray, StringBuilder,
    };
    use datafusion::{
        common::{
            Result, ScalarValue,
            cast::{as_binary_array, as_list_array, as_string_array},
        },
        physical_plan::ColumnarValue,
    };
    use datafusion_ext_commons::df_execution_err;

    use crate::spark_strings::{string_lower, string_repeat, string_split, string_upper};

    #[test]
    fn test_string_chr() -> Result<()> {
        let function = crate::create_auron_ext_function("Spark_Chr", 0)?;
        let inputs = [
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(65),
            Some(233),
            Some(255),
            Some(256),
            Some(321),
            Some(i64::MAX),
            None,
        ];
        let expected = [
            Some(""),
            Some(""),
            Some("\0"),
            Some("A"),
            Some("é"),
            Some("ÿ"),
            Some("\0"),
            Some("A"),
            Some("ÿ"),
            None,
        ];
        let result = function(&[ColumnarValue::Array(Arc::new(Int64Array::from(
            inputs.to_vec(),
        )))])?
        .into_array(inputs.len())?;
        assert_eq!(
            as_string_array(&result)?,
            &StringArray::from(expected.to_vec())
        );
        for (input, expected) in inputs.into_iter().zip(expected) {
            let ColumnarValue::Scalar(result) =
                function(&[ColumnarValue::Scalar(ScalarValue::Int64(input))])?
            else {
                panic!("expected scalar result");
            };
            assert_eq!(result, ScalarValue::Utf8(expected.map(str::to_owned)));
        }
        Ok(())
    }

    #[test]
    fn test_string_upper() -> Result<()> {
        let r = string_upper(&vec![ColumnarValue::Array(Arc::new(
            StringArray::from_iter(vec![Some("{123}"), Some("A'asd'"), None]),
        ))])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("{123}"), Some("A'ASD'"), None,]
        );
        Ok(())
    }

    #[test]
    fn test_string_lower() -> Result<()> {
        let r = string_lower(&vec![ColumnarValue::Array(Arc::new(
            StringArray::from_iter(vec![Some("{123}"), Some("A'asd'"), None]),
        ))])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("{123}"), Some("a'asd'"), None,]
        );
        Ok(())
    }

    #[test]
    fn test_string_lower_null_scalar() -> Result<()> {
        let r = string_lower(&vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))])?;
        match r {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => Ok(()),
            other => df_execution_err!("Expected null Utf8 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_string_upper_null_scalar() -> Result<()> {
        let r = string_upper(&vec![ColumnarValue::Scalar(ScalarValue::Utf8(None))])?;
        match r {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => Ok(()),
            other => df_execution_err!("Expected null Utf8 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_string_repeat() -> Result<()> {
        // positive case
        let r = string_repeat(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("123".to_string()),
                Some("a".to_string()),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::from(3_i32)),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some("123123123"), Some("aaa"), None,]
        );

        // repeat with n < 0
        let r = string_repeat(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("123".to_string()),
                Some("a".to_string()),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::from(-1_i32)),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(""), Some(""), None,]
        );

        // repeat with n = null
        let r = string_repeat(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("123".to_string()),
                Some("a".to_string()),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::Int32(None)),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_string_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![None, None, None,]
        );
        Ok(())
    }

    #[test]
    fn test_string_split() -> Result<()> {
        // positive case
        let r = string_split(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("123,456,,,789,".to_string()),
                Some("123".to_string()),
                Some(String::new()),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::from(",")),
        ])?;
        let list = r.into_array(4)?;
        let list_offsets = as_list_array(&list)?.value_offsets();
        let list_values = as_list_array(&list)?.values();

        assert_eq!(list_offsets, vec![0, 6, 7, 8, 8]);
        assert_eq!(
            as_string_array(list_values)?
                .into_iter()
                .collect::<Vec<_>>(),
            vec![
                Some("123"),
                Some("456"),
                Some(""),
                Some(""),
                Some("789"),
                Some(""),
                Some("123"),
                Some(""),
            ]
        );
        Ok(())
    }
}
