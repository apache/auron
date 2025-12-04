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

use arrow::array::{ArrayRef, StringArray};
use datafusion::{
    common::{Result, ScalarValue, cast::as_string_array},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

pub fn string_initcap(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => {
            let input_array = as_string_array(array)?;
            let output_array =
                StringArray::from_iter(input_array.into_iter().map(|s| s.map(|s| initcap(s))));
            Ok(ColumnarValue::Array(Arc::new(output_array) as ArrayRef))
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(str))) => {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(initcap(str)))))
        }
        _ => df_execution_err!("Unsupported args {args:?} for `string_initcap`"),
    }
}

fn initcap(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut prev_is_space = true; // i == 0 or chars[i-1] == ' '

    if input.is_ascii() {
        // ASCII
        for ch in input.chars() {
            if prev_is_space && ch.is_ascii_alphanumeric() {
                out.push(ch.to_ascii_uppercase());
            } else {
                out.push(ch.to_ascii_lowercase());
            };
            prev_is_space = ch == ' ';
        }
    } else {
        // Non-ASCII
        for ch in input.chars() {
            if prev_is_space && ch.is_alphabetic() {
                out.extend(ch.to_uppercase());
            } else {
                out.extend(ch.to_lowercase());
            }
            prev_is_space = ch == ' ';
        }
    }
    out
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use datafusion::{
        common::{Result, ScalarValue},
        physical_plan::ColumnarValue,
    };

    use crate::spark_initcap::string_initcap;

    #[test]
    fn test_initcap_array() -> Result<()> {
        let input_data = vec![
            None,
            Some(""),
            Some("hI THOmAS"),
            Some("James-Smith"),
            Some("michael rose"),
            Some("a1b2   c3D4"),
            Some(" ---abc--- ABC --ABC-- a-b A B eB Ac c d"),
            Some(" 世  界  世界 "),
        ];
        let input_columnar_value = ColumnarValue::Array(Arc::new(StringArray::from(input_data)));

        let result = string_initcap(&vec![input_columnar_value])?.into_array(6)?;

        let expected_data = vec![
            None,
            Some(""),
            Some("Hi Thomas"),
            Some("James-smith"),
            Some("Michael Rose"),
            Some("A1b2   C3d4"),
            Some(" ---abc--- Abc --abc-- A-b A B Eb Ac C D"),
            Some(" 世  界  世界 "),
        ];
        let expected: ArrayRef = Arc::new(StringArray::from(expected_data));
        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_initcap_scalar() -> Result<()> {
        let input_columnar_value = ColumnarValue::Scalar(ScalarValue::from("abC c3D4"));
        let result = string_initcap(&vec![input_columnar_value])?.into_array(1)?;
        let expected: ArrayRef = Arc::new(StringArray::from(vec![Some("Abc C3d4")]));
        assert_eq!(&result, &expected);
        Ok(())
    }
}
