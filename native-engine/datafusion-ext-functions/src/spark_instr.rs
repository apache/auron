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
use datafusion_ext_commons::df_execution_err;

/// instr(str, substr) - Returns the (1-based) index of the first occurrence of
/// substr in str Compatible with Spark's instr function
/// Returns 0 if substr is not found or if either argument is null
pub fn spark_instr(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        df_execution_err!("instr requires exactly 2 arguments")?;
    }

    let string_array = args[0].clone().into_array(1)?;
    let substr = match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(substr))) if !substr.is_empty() => substr,
        ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {
            return Ok(ColumnarValue::Scalar(ScalarValue::Int32(None)));
        }
        _ => df_execution_err!("instr substring only supports non-empty literal string")?,
    };

    let result_array: ArrayRef = Arc::new(Int32Array::from_iter(
        as_string_array(&string_array)?
            .into_iter()
            .map(|s| s.map(|s| s.find(substr).map(|pos| (pos + 1) as i32).unwrap_or(0))),
    ));

    Ok(ColumnarValue::Array(result_array))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use datafusion::{
        common::{Result, ScalarValue, cast::as_int32_array},
        physical_plan::ColumnarValue,
    };

    use super::spark_instr;

    #[test]
    fn test_spark_instr() -> Result<()> {
        // Test basic functionality
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
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![Some(
                "hello".to_string(),
            )]))),
            ColumnarValue::Scalar(ScalarValue::from("")),
        ]);
        assert!(r.is_err());

        // Test with null substring
        let r = spark_instr(&vec![
            ColumnarValue::Array(Arc::new(StringArray::from_iter(vec![Some(
                "hello".to_string(),
            )]))),
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
        ])?;
        if !matches!(r, ColumnarValue::Scalar(ScalarValue::Int32(None))) {
            return datafusion::common::internal_err!("Expected null Int32 scalar");
        }
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
}
