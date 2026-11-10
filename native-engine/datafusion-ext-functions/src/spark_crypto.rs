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

use std::{fmt::Write, sync::Arc};

use arrow::array::StringArray;
use datafusion::{
    common::{Result, ScalarValue, cast::as_binary_array, utils::take_function_args},
    functions::crypto::basic::{DigestAlgorithm, digest_process},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

pub fn spark_md5(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [data] = take_function_args("md5", args)?;
    let value = digest_process(data, DigestAlgorithm::Md5)?;
    to_hex_string(value)
}

fn to_hex_string(value: ColumnarValue) -> Result<ColumnarValue> {
    Ok(match value {
        ColumnarValue::Array(array) => {
            let binary_array = as_binary_array(&array)?;
            let string_array: StringArray = binary_array
                .iter()
                .map(|opt| opt.map(hex_encode::<_>))
                .collect();
            ColumnarValue::Array(Arc::new(string_array))
        }
        ColumnarValue::Scalar(ScalarValue::Binary(opt)) => {
            ColumnarValue::Scalar(ScalarValue::Utf8(opt.map(hex_encode::<_>)))
        }
        _ => {
            return df_execution_err!(
                "digest function should return binary value, but got: {:?}",
                value.data_type()
            );
        }
    })
}

#[inline]
fn hex_encode<T: AsRef<[u8]>>(data: T) -> String {
    let mut s = String::with_capacity(data.as_ref().len() * 2);
    for b in data.as_ref() {
        // Writing to a string never errors, so we can unwrap here.
        write!(&mut s, "{b:02x}").expect("writing to String should not fail");
    }
    s
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use datafusion::{
        common::ScalarValue, error::Result as DataFusionResult, physical_plan::ColumnarValue,
    };

    use crate::spark_crypto::spark_md5;

    /// Helper function to run a test for a given hash function and scalar
    /// input.
    #[allow(clippy::panic)]
    fn run_scalar_test(
        // Accepts any function that matches the signature of the spark_sha* functions.
        hash_fn: impl Fn(&[ColumnarValue]) -> DataFusionResult<ColumnarValue>,
        input_value: ColumnarValue,
        expected_output: &str,
    ) -> Result<(), Box<dyn Error>> {
        // 1. Call the provided hash function.
        let result = hash_fn(&[input_value])?;

        // 2. Unwrap the result, panicking if it's not the expected type.
        let ColumnarValue::Scalar(ScalarValue::Utf8(actual)) = result else {
            panic!("expected UTF-8 scalar");
        };

        // 3. Assert the actual output matches the expected output.
        assert_eq!(actual.as_deref(), Some(expected_output));

        Ok(())
    }

    fn test_md5_scalar_utf8() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("ABC".to_string())));
        let expected = "902fbdd2b1df0c4f70b4a5d23525e932";
        run_scalar_test(spark_md5, input, expected)
    }

    #[test]
    fn test_md5_scalar_binary() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![1, 2, 3, 4, 5, 6])));
        let expected = "6ac1e56bc78f031059be7be854522c4c";
        run_scalar_test(spark_md5, input, expected)
    }
}
