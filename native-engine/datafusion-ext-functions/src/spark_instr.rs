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
    common::{Result, ScalarValue, cast::as_string_array},
    physical_plan::ColumnarValue,
};

/// instr function implementation
/// Returns the 1-based position of the first occurrence of substr in str
/// If not found, returns 0
/// If any input is null, returns null
pub fn string_instr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match (&args[0], &args[1]) {
        // Both scalars
        (ColumnarValue::Scalar(str_scalar), ColumnarValue::Scalar(substr_scalar)) => {
            match (str_scalar, substr_scalar) {
                (ScalarValue::Utf8(Some(str)), ScalarValue::Utf8(Some(substr))) => {
                    let pos = find_substring(str, substr, 1);
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))))
                }
                _ => Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))),
            }
        }
        // First arg is array, second is scalar
        (ColumnarValue::Array(str_array), ColumnarValue::Scalar(substr_scalar)) => {
            let substr = match substr_scalar {
                ScalarValue::Utf8(Some(s)) => s,
                ScalarValue::Utf8(None) => {
                    // If substr is null, all results are null
                    let len = str_array.len();
                    let result = Int32Array::from(vec![None; len]);
                    return Ok(ColumnarValue::Array(Arc::new(result)));
                }
                _ => {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)));
                }
            };

            let str_array = as_string_array(str_array)?;
            let result: Int32Array = str_array
                .iter()
                .map(|opt_str| opt_str.map(|s| find_substring(s, substr, 1)))
                .collect();

            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        // Both arrays
        (ColumnarValue::Array(str_array), ColumnarValue::Array(substr_array)) => {
            let str_array = as_string_array(str_array)?;
            let substr_array = as_string_array(substr_array)?;

            let result: Int32Array = str_array
                .iter()
                .zip(substr_array.iter())
                .map(|(opt_str, opt_substr)| match (opt_str, opt_substr) {
                    (Some(str), Some(substr)) => Some(find_substring(str, substr, 1)),
                    _ => None,
                })
                .collect();

            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        // First arg is scalar, second is array (should handle but less common)
        (ColumnarValue::Scalar(str_scalar), ColumnarValue::Array(substr_array)) => {
            let str = match str_scalar {
                ScalarValue::Utf8(Some(s)) => s,
                _ => {
                    // If str is null, all results are null
                    let len = substr_array.len();
                    let result = Int32Array::from(vec![None; len]);
                    return Ok(ColumnarValue::Array(Arc::new(result)));
                }
            };

            let substr_array = as_string_array(substr_array)?;
            let result: Int32Array = substr_array
                .iter()
                .map(|opt_substr| opt_substr.map(|s| find_substring(str, s, 1)))
                .collect();

            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        _ => Ok(ColumnarValue::Scalar(ScalarValue::Int32(None))),
    }
}

/// Find the position of substr in str starting from start_pos (1-based)
/// Returns 0 if not found
fn find_substring(str: &str, substr: &str, start_pos: i32) -> i32 {
    // Handle empty substring - return 1 if start_pos is valid
    if substr.is_empty() {
        if start_pos < 1 || start_pos > str.chars().count() as i32 + 1 {
            return 0;
        }
        return start_pos;
    }

    // Handle empty string - always return 0
    if str.is_empty() {
        return 0;
    }

    // Convert start_pos to 0-based byte offset
    let start_pos = start_pos.max(1) as usize;
    let chars: Vec<char> = str.chars().collect();
    
    if start_pos > chars.len() {
        return 0;
    }

    // Search for substring
    let search_str: String = chars[start_pos - 1..].iter().collect();
    
    match search_str.find(substr) {
        Some(byte_pos) => {
            // Convert byte position back to character position (1-based)
            let chars_before = search_str[0..byte_pos].chars().count();
            (start_pos + chars_before) as i32
        }
        None => 0,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_instr_basic() -> Result<()> {
        // Test basic functionality with scalar inputs
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("hello world")),
            ColumnarValue::Scalar(ScalarValue::from("world")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 7);
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_not_found() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("hello world")),
            ColumnarValue::Scalar(ScalarValue::from("xyz")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 0);
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_empty_substring() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("hello")),
            ColumnarValue::Scalar(ScalarValue::from("")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 1);
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_null_string() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ColumnarValue::Scalar(ScalarValue::from("world")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => Ok(()),
            other => panic!("Expected null Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_null_substring() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("hello world")),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(None)) => Ok(()),
            other => panic!("Expected null Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_array_input() -> Result<()> {
        let str_array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("hello world"),
            Some("hello world"),
            Some("hello world"),
            None,
        ])));
        let substr_scalar = ColumnarValue::Scalar(ScalarValue::from("world"));

        let result = string_instr(&vec![str_array, substr_scalar])?;
        let result_array = result.into_array(4)?;
        let int_array = result_array.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(int_array.value(0), 7);
        assert_eq!(int_array.value(1), 7);
        assert_eq!(int_array.value(2), 7);
        assert!(int_array.is_null(3));

        Ok(())
    }

    #[test]
    fn test_instr_case_sensitive() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("Hello World")),
            ColumnarValue::Scalar(ScalarValue::from("hello")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 0); // Not found due to case sensitivity
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_multiple_occurrences() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("abracadabra")),
            ColumnarValue::Scalar(ScalarValue::from("abra")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 1); // First occurrence
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_both_arrays() -> Result<()> {
        let str_array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("hello world"),
            Some("test string"),
            Some("foo bar"),
        ])));
        let substr_array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("world"),
            Some("string"),
            Some("baz"),
        ])));

        let result = string_instr(&vec![str_array, substr_array])?;
        let result_array = result.into_array(3)?;
        let int_array = result_array.as_any().downcast_ref::<Int32Array>().unwrap();

        assert_eq!(int_array.value(0), 7);
        assert_eq!(int_array.value(1), 6);
        assert_eq!(int_array.value(2), 0); // Not found

        Ok(())
    }

    #[test]
    fn test_instr_special_characters() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("test@example.com")),
            ColumnarValue::Scalar(ScalarValue::from("@")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 5);
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_unicode() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("café")),
            ColumnarValue::Scalar(ScalarValue::from("é")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 3);
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_chinese() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("你好世界")),
            ColumnarValue::Scalar(ScalarValue::from("世界")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 3);
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_empty_string() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("")),
            ColumnarValue::Scalar(ScalarValue::from("abc")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 0);
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }

    #[test]
    fn test_instr_single_character() -> Result<()> {
        let result = string_instr(&vec![
            ColumnarValue::Scalar(ScalarValue::from("a")),
            ColumnarValue::Scalar(ScalarValue::from("a")),
        ])?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(pos))) => {
                assert_eq!(pos, 1);
                Ok(())
            }
            other => panic!("Expected Int32 scalar, got: {:?}", other),
        }
    }
}
