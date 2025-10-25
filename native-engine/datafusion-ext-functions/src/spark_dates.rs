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
    array::{ArrayRef, Int32Array},
    compute::{DatePart, date_part},
    datatypes::DataType,
};
use datafusion::{common::Result, physical_plan::ColumnarValue};
use datafusion_ext_commons::arrow::cast::cast;

pub fn spark_year(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Year)?))
}

pub fn spark_month(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Month)?))
}

pub fn spark_day(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Day)?))
}

/// `spark_quarter(date/timestamp/compatible-string)`
///
/// Simulates Spark's `quarter()` function.
/// Converts the input to `Date32`, extracts the month (1–12),
/// and computes the quarter as `((month - 1) / 3) + 1`.
/// Null values are propagated transparently.
pub fn spark_quarter(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Cast input to Date32 for compatibility with date_part()
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;

    // Extract month (1–12) using Arrow's date_part
    let month_arr: ArrayRef = date_part(&input, DatePart::Month)?;
    let month_arr = month_arr
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("date_part(Month) must return Int32Array");

    // Compute quarter: ((month - 1) / 3) + 1, preserving NULLs
    let quarter = Int32Array::from_iter(
        month_arr
            .iter()
            .map(|opt_m| opt_m.map(|m| ((m - 1) / 3 + 1) as i32)),
    );

    Ok(ColumnarValue::Array(Arc::new(quarter)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Date32Array, Int32Array};

    use super::*;

    #[test]
    fn test_spark_year() {
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(1000),
            Some(2000),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1970),
            Some(1972),
            Some(1975),
            None,
        ]));
        assert_eq!(
            &spark_year(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }

    #[test]
    fn test_spark_month() {
        let input = Arc::new(Date32Array::from(vec![Some(0), Some(35), Some(65), None]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None]));
        assert_eq!(
            &spark_month(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }

    #[test]
    fn test_spark_day() {
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(11),
            Some(21),
            Some(31),
            Some(10),
            None,
        ]));
        assert_eq!(
            &spark_day(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }

    #[test]
    fn test_spark_quarter_basic() {
        // Date32 days relative to 1970-01-01:
        //  0   -> 1970-01-01 (Q1)
        //  40  -> ~1970-02-10 (Q1)
        // 100  -> ~1970-04-11 (Q2)
        // 200  -> ~1970-07-20 (Q3)
        // 300  -> ~1970-10-28 (Q4)
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(40),
            Some(100),
            Some(200),
            Some(300),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            None,
        ]));

        let out = spark_quarter(&args).unwrap().into_array(1).unwrap();
        assert_eq!(&out, &expected);
    }

    #[test]
    fn test_spark_quarter_null_only() {
        // Ensure NULL propagation
        let input = Arc::new(Date32Array::from(vec![None, None]));
        let args = vec![ColumnarValue::Array(input)];
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));

        let out = spark_quarter(&args).unwrap().into_array(1).unwrap();
        assert_eq!(&out, &expected);
    }
}
