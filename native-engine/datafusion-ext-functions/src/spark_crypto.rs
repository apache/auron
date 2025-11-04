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

use arrow::{
    array::StringArray,
    datatypes::{DataType, Field},
};
use datafusion::{
    common::{Result, ScalarValue, cast::as_binary_array, utils::take_function_args},
    functions::crypto::{
        basic::{DigestAlgorithm, digest_process},
        sha224, sha256, sha384, sha512,
    },
    logical_expr::{ScalarFunctionArgs, ScalarUDF},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

/// `sha224` function that simulates Spark's `sha2` expression with bit width
/// 224
pub fn spark_sha224(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    digest_and_wrap_as_hex(args, sha224())
}

/// `sha256` function that simulates Spark's `sha2` expression with bit width 0
/// or 256
pub fn spark_sha256(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    digest_and_wrap_as_hex(args, sha256())
}

/// `sha384` function that simulates Spark's `sha2` expression with bit width
/// 384
pub fn spark_sha384(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    digest_and_wrap_as_hex(args, sha384())
}

/// `sha512` function that simulates Spark's `sha2` expression with bit width
/// 512
pub fn spark_sha512(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    digest_and_wrap_as_hex(args, sha512())
}

pub fn spark_md5(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let [data] = take_function_args("md5", args)?;
    let value = digest_process(data, DigestAlgorithm::Md5)?;
    to_hex_string(value)
}

/// Spark requires hex string as the result of sha2 and md5 functions, we have
/// to wrap the result of digest functions as hex string
fn digest_and_wrap_as_hex(args: &[ColumnarValue], digest: Arc<ScalarUDF>) -> Result<ColumnarValue> {
    let value = digest.inner().invoke_with_args(ScalarFunctionArgs {
        args: args.to_vec(),
        arg_fields: vec![Arc::new(Field::new("arg", DataType::Binary, true))],
        number_rows: 0,
        return_field: Arc::new(Field::new("result", DataType::Utf8, true)),
    })?;
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
        write!(&mut s, "{b:02x}").unwrap();
    }
    s
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use datafusion::{
        common::ScalarValue, error::Result as DataFusionResult, physical_plan::ColumnarValue,
    };

    use crate::spark_crypto::{spark_md5, spark_sha224, spark_sha256, spark_sha384, spark_sha512};

    /// Helper function to run a test for a given hash function and scalar
    /// input.
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

    #[test]
    fn test_sha224_scalar_utf8() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("ABC".to_string())));
        let expected = "107c5072b799c4771f328304cfe1ebb375eb6ea7f35a3aa753836fad";
        run_scalar_test(spark_sha224, input, expected)
    }

    #[test]
    fn test_sha256_scalar_utf8() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("ABC".to_string())));
        let expected = "b5d4045c3f466fa91fe2cc6abe79232a1a57cdf104f7a26e716e0a1e2789df78";
        run_scalar_test(spark_sha256, input, expected)
    }

    #[test]
    fn test_sha384_scalar_utf8() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("ABC".to_string())));
        let expected = "1e02dc92a41db610c9bcdc9b5935d1fb9be5639116f6c67e97bc1a3ac649753baba7ba021c813e1fe20c0480213ad371";
        run_scalar_test(spark_sha384, input, expected)
    }

    #[test]
    fn test_sha512_scalar_utf8() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Utf8(Some("ABC".to_string())));
        let expected = "397118fdac8d83ad98813c50759c85b8c47565d8268bf10da483153b747a74743a58a90e85aa9f705ce6984ffc128db567489817e4092d050d8a1cc596ddc119";
        run_scalar_test(spark_sha512, input, expected)
    }

    #[test]
    fn test_sha224_scalar_binary() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![1, 2, 3, 4, 5, 6])));
        let expected = "4225cbc32d17010d1a440de9e34504c1fae29b8ee5e527e191ff9a82";
        run_scalar_test(spark_sha224, input, expected)
    }

    #[test]
    fn test_sha256_scalar_binary() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![1, 2, 3, 4, 5, 6])));
        let expected = "7192385c3c0605de55bb9476ce1d90748190ecb32a8eed7f5207b30cf6a1fe89";
        run_scalar_test(spark_sha256, input, expected)
    }

    #[test]
    fn test_sha384_scalar_binary() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![1, 2, 3, 4, 5, 6])));
        let expected = "557cfe660c753b830efa61528fc350ef384a7a4b9d3467c6230049bc59548eb8a404874baff89cb0f9bd18400829fdc2";
        run_scalar_test(spark_sha384, input, expected)
    }

    #[test]
    fn test_sha512_scalar_binary() -> Result<(), Box<dyn Error>> {
        let input = ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![1, 2, 3, 4, 5, 6])));
        let expected = "178d767c364244ede054ebb3cc4af0ac2b307a86fba6a32706ce4f692642674d2ab8f51ee738ecb09bc296918aa85db48abe28fcaef7aa2da81a618cc6d891c3";
        run_scalar_test(spark_sha512, input, expected)
    }

    #[test]
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
