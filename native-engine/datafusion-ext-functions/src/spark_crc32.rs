// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
    array::{ArrayRef, Int64Array},
    datatypes::DataType,
};
use crc32fast::Hasher;
use datafusion::{
    common::{
        Result, ScalarValue,
        cast::{as_binary_array, as_string_array},
    },
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::df_execution_err;

/// Spark-style `crc32(expr)`
/// - Input: Utf8 (string) or Binary
/// - Output: Int64 (unsigned 32-bit CRC packed in i64; always non-negative)
/// - Null-safe: null in -> null out
/// - Supports scalar & array inputs
pub fn spark_crc32(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 1 {
        return df_execution_err!("crc32() expects 1 argument, got {}", args.len());
    }

    match &args[0] {
        ColumnarValue::Scalar(sv) => crc32_scalar(sv),
        ColumnarValue::Array(arr) => crc32_array(arr),
    }
}

fn crc32_scalar(sv: &ScalarValue) -> Result<ColumnarValue> {
    if sv.is_null() {
        return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)));
    }

    let value: Option<i64> = match sv {
        ScalarValue::Utf8(Some(s)) => {
            let mut hasher = Hasher::new();
            hasher.update(s.as_bytes());
            let crc = (hasher.finalize() as u64) & 0xffff_ffffu64;
            Some(crc as i64)
        }
        ScalarValue::Binary(Some(b)) => {
            let mut hasher = Hasher::new();
            hasher.update(b);
            let crc = (hasher.finalize() as u64) & 0xffff_ffffu64;
            Some(crc as i64)
        }
        _ => {
            return df_execution_err!("crc32() expects STRING or BINARY, but got scalar {:?}", sv);
        }
    };

    Ok(ColumnarValue::Scalar(ScalarValue::Int64(value)))
}

fn crc32_array(arr: &ArrayRef) -> Result<ColumnarValue> {
    match arr.data_type() {
        DataType::Utf8 => {
            let sa = as_string_array(arr)?;
            let out = Int64Array::from_iter(sa.iter().map(|opt| {
                opt.map(|s| {
                    let mut h = Hasher::new();
                    h.update(s.as_bytes());
                    let crc = (h.finalize() as u64) & 0xffff_ffffu64;
                    crc as i64
                })
            }));
            Ok(ColumnarValue::Array(Arc::new(out)))
        }
        DataType::Binary => {
            let ba = as_binary_array(arr)?;
            let out = Int64Array::from_iter(ba.iter().map(|opt| {
                opt.map(|b| {
                    let mut h = Hasher::new();
                    h.update(b);
                    let crc = (h.finalize() as u64) & 0xffff_ffffu64;
                    crc as i64
                })
            }));
            Ok(ColumnarValue::Array(Arc::new(out)))
        }
        other => df_execution_err!("crc32() expects STRING or BINARY array, but got {other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{BinaryArray, StringArray};
    use datafusion::{common::Result as DFResult, physical_plan::ColumnarValue};

    use super::*;

    /// Helper for scalar tests returning i64 CRC
    fn run_scalar_crc(input: ColumnarValue, expected: i64) -> DFResult<()> {
        let out = spark_crc32(&[input])?;
        let ColumnarValue::Scalar(ScalarValue::Int64(actual)) = out else {
            panic!("expected Int64 scalar");
        };
        assert_eq!(actual, Some(expected));
        Ok(())
    }

    #[test]
    fn test_crc32_scalar_utf8() -> DFResult<()> {
        // Known vector: "123456789" -> 0xCBF43926 = 3421780262
        run_scalar_crc(
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("123456789".to_string()))),
            3421780262i64,
        )
    }

    #[test]
    fn test_crc32_scalar_utf8_simple() -> DFResult<()> {
        // "ABC" -> 0xA37BBB78 = 2743272264
        run_scalar_crc(
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("ABC".to_string()))),
            2743272264i64,
        )
    }

    #[test]
    fn test_crc32_scalar_binary() -> DFResult<()> {
        // [1,2,3,4,5,6] -> 0x820D8C24 = 2180413220
        run_scalar_crc(
            ColumnarValue::Scalar(ScalarValue::Binary(Some(vec![1, 2, 3, 4, 5, 6]))),
            2180413220i64,
        )
    }

    #[test]
    fn test_crc32_scalar_nulls() -> DFResult<()> {
        let out = spark_crc32(&[ColumnarValue::Scalar(ScalarValue::Utf8(None))])?;
        match out {
            ColumnarValue::Scalar(ScalarValue::Int64(None)) => {}
            _ => panic!("expected null Int64"),
        }
        let out = spark_crc32(&[ColumnarValue::Scalar(ScalarValue::Binary(None))])?;
        match out {
            ColumnarValue::Scalar(ScalarValue::Int64(None)) => {}
            _ => panic!("expected null Int64"),
        }
        Ok(())
    }

    #[test]
    fn test_crc32_array_utf8_and_nulls() -> DFResult<()> {
        let arr = Arc::new(StringArray::from(vec![
            Some("ABC"),
            None,
            Some("123456789"),
        ])) as ArrayRef;
        let out = spark_crc32(&[ColumnarValue::Array(arr)])?.into_array(3)?;
        let out = arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(&out);
        let got: Vec<Option<i64>> = out.iter().collect();
        let expected = vec![Some(2743272264i64), None, Some(3421780262i64)];
        assert_eq!(got, expected);
        Ok(())
    }

    #[test]
    fn test_crc32_array_binary_and_nulls() -> DFResult<()> {
        let arr: ArrayRef = Arc::new(BinaryArray::from_iter(vec![
            Some(&[1u8, 2, 3, 4, 5, 6][..]),
            None,
            Some("ABC".as_bytes()),
        ]));
        let out = spark_crc32(&[ColumnarValue::Array(arr)])?.into_array(3)?;
        let out = arrow::array::as_primitive_array::<arrow::datatypes::Int64Type>(&out);
        let got: Vec<Option<i64>> = out.iter().collect();
        let expected = vec![Some(2180413220i64), None, Some(2743272264i64)];
        assert_eq!(got, expected);
        Ok(())
    }
}
