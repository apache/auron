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

use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use datafusion::{
    common::{
        Result, ScalarValue,
        cast::{as_int32_array, as_string_array},
    },
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

/// instr(str, substr) - Returns the (1-based) index of the first occurrence of
/// substr in str. Compatible with Spark's instr function.
/// Returns 0 if substr is not found or if substr is empty.
/// Returns null if str is null or substr is null.
pub fn spark_instr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        df_execution_err!("instr requires exactly 2 arguments")?;
    }

    let is_scalar = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let len = args
        .iter()
        .map(|arg| match arg {
            ColumnarValue::Array(array) => array.len(),
            ColumnarValue::Scalar(_) => 1,
        })
        .max()
        .unwrap_or(0);

    let arrays = args
        .iter()
        .map(|arg| {
            Ok(match arg {
                ColumnarValue::Array(array) => array.clone(),
                ColumnarValue::Scalar(scalar) => scalar.to_array_of_size(len)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let str_array = as_string_array(&arrays[0])?;
    let substr_array = as_string_array(&arrays[1])?;

    let result_array: ArrayRef = Arc::new(Int32Array::from_iter(
        str_array
            .iter()
            .zip(substr_array.iter())
            .map(|(s, substr)| match (s, substr) {
                (Some(_), None) => None, // substr is null
                (None, _) => None,       // str is null
                (Some(s), Some(substr)) => {
                    if substr.is_empty() {
                        Some(0)
                    } else {
                        Some(find_char_position(s, substr))
                    }
                }
            }),
    ));

    if is_scalar {
        let scalar = as_int32_array(&result_array)?.value(0);
        Ok(ColumnarValue::Scalar(if result_array.is_null(0) {
            ScalarValue::Int32(None)
        } else {
            ScalarValue::Int32(Some(scalar))
        }))
    } else {
        Ok(ColumnarValue::Array(result_array))
    }
}

/// Find the 1-based character position of substr in s
/// Returns 0 if not found
fn find_char_position(s: &str, substr: &str) -> i32 {
    if substr.is_empty() {
        return 0;
    }

    // Use char_indices to get byte offset to char position mapping
    let char_positions: Vec<usize> = s.char_indices().map(|(byte_pos, _)| byte_pos).collect();

    // Find byte offset using find
    if let Some(byte_pos) = s.find(substr) {
        // Find the character position (1-based)
        // char_positions contains the byte offset for each character
        // We need to find which character index corresponds to this byte offset
        for (char_idx, &char_byte_pos) in char_positions.iter().enumerate() {
            if char_byte_pos == byte_pos {
                return (char_idx + 1) as i32;
            }
        }
        // Fallback: if exact match not found, estimate
        char_positions.len() as i32 + 1
    } else {
        0
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use datafusion::{
        common::{Result, ScalarValue, cast::as_int32_array},
        physical_plan::ColumnarValue,
    };

    use super::spark_instr;

    #[test]
    fn test_spark_instr() -> Result<()> {
        // Test basic functionality with scalar substring
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("hello world".to_string()),
                Some("abc".to_string()),
                Some("abcabc".to_string()),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::from("world")),
        ])?;
        let s = r.into_array(4)?;
        assert_eq!(
            as_int32_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(7), Some(0), Some(0), None,]
        );

        // Test with empty substring should return 0
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("hello".to_string()),
                Some("world".to_string()),
                None,
            ]))),
            ColumnarValue::Scalar(ScalarValue::from("")),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_int32_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(0), Some(0), None,]
        );

        // Test with null substring
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![Some(
                "hello".to_string(),
            )]))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        ])?;
        let s = r.into_array(1)?;
        assert_eq!(
            as_int32_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![None,]
        );

        // Test with array substring (element-wise)
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("hello world".to_string()),
                Some("hello".to_string()),
                Some("test".to_string()),
            ]))),
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("world".to_string()),
                Some("test".to_string()),
                Some("test".to_string()),
            ]))),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_int32_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(7), Some(0), Some(1),]
        );

        // Test with both scalars
        let r = spark_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("hello world")),
            ColumnarValue::Scalar(ScalarValue::from("world")),
        ])?;
        assert!(matches!(
            r,
            ColumnarValue::Scalar(ScalarValue::Int32(Some(7)))
        ));

        Ok(())
    }

    #[test]
    fn test_spark_instr_multiple_matches() -> Result<()> {
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("banana".to_string()),
                Some("testtesttest".to_string()),
            ]))),
            ColumnarValue::Scalar(ScalarValue::from("test")),
        ])?;
        let s = r.into_array(2)?;
        assert_eq!(
            as_int32_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(0), Some(1),]
        );
        Ok(())
    }

    #[test]
    fn test_spark_instr_case_sensitive() -> Result<()> {
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("Hello".to_string()),
                Some("HELLO".to_string()),
            ]))),
            ColumnarValue::Scalar(ScalarValue::from("hello")),
        ])?;
        let s = r.into_array(2)?;
        assert_eq!(
            as_int32_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(0), Some(0),]
        );
        Ok(())
    }

    #[test]
    fn test_spark_instr_utf8() -> Result<()> {
        // Test UTF-8 multi-byte characters
        // "你好世界" - "世界" should return 3 (character position), not 6 (byte
        // position)
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![
                Some("你好世界".to_string()),
                Some("hello世界".to_string()),
                Some("test".to_string()),
            ]))),
            ColumnarValue::Scalar(ScalarValue::from("世界")),
        ])?;
        let s = r.into_array(3)?;
        assert_eq!(
            as_int32_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(3), Some(6), Some(0),]
        );

        // Test with emoji (4-byte UTF-8)
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![Some(
                "hello😀world".to_string(),
            )]))),
            ColumnarValue::Scalar(ScalarValue::from("😀")),
        ])?;
        let s = r.into_array(1)?;
        assert_eq!(
            as_int32_array(&s)?.into_iter().collect::<Vec<_>>(),
            vec![Some(6),]
        );

        Ok(())
    }
}
