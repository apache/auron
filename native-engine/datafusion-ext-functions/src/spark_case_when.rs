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

use arrow::{array::*, compute::kernels::zip::zip, datatypes::DataType};
use datafusion::{common::Result, physical_plan::ColumnarValue};
use datafusion_ext_commons::df_execution_err;

/// CASE WHEN function implementation
///
/// Syntax: case_when(condition1, value1, condition2, value2, ..., else_value)
///
/// Arguments:
/// - Must have odd number of arguments (at least 3)
/// - Pairs of (condition, value), with optional else_value at the end
/// - If no else_value provided and no conditions match, returns NULL
///
/// Example:
/// - case_when(x > 10, 'big', x > 5, 'medium', 'small')
/// - case_when(x IS NULL, 0, x )
pub fn spark_case_when(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.is_empty() {
        return df_execution_err!("case_when requires at least 1 argument (else value)");
    }

    // Special case: only one argument means it's the else value
    if args.len() == 1 {
        return Ok(args[0].clone());
    }

    // Determine if we have an else value (odd number of args means we do)
    let has_else = args.len() % 2 == 1;
    let num_conditions = if has_else {
        (args.len() - 1) / 2
    } else {
        args.len() / 2
    };

    // Get the batch size
    let batch_size = match &args[0] {
        ColumnarValue::Array(array) => array.len(),
        ColumnarValue::Scalar(_) => {
            // If all inputs are scalars, find the first array to determine size
            let mut size = 1;
            for arg in args {
                if let ColumnarValue::Array(array) = arg {
                    size = array.len();
                    break;
                }
            }
            size
        }
    };

    // Convert all inputs to arrays
    let mut conditions = Vec::with_capacity(num_conditions);
    let mut values = Vec::with_capacity(num_conditions);

    for i in 0..num_conditions {
        let condition_idx = i * 2;
        let value_idx = i * 2 + 1;

        let condition_array = args[condition_idx].clone().into_array(batch_size)?;
        let value_array = args[value_idx].clone().into_array(batch_size)?;

        // Verify condition is boolean
        if condition_array.data_type() != &DataType::Boolean {
            return df_execution_err!(
                "case_when condition at position {} must be boolean, got {:?}",
                condition_idx,
                condition_array.data_type()
            );
        }

        conditions.push(as_boolean_array(&condition_array).clone());
        values.push(value_array);
    }

    // Get else value if present
    let else_array = if has_else {
        Some(args[args.len() - 1].clone().into_array(batch_size)?)
    } else {
        None
    };

    // Determine output data type (from first value)
    let output_type = values[0].data_type().clone();

    // Build the result array
    let result = evaluate_case_when(
        &conditions,
        &values,
        else_array.as_ref(),
        batch_size,
        &output_type,
    )?;

    // If all inputs were scalars, return a scalar
    if batch_size == 1
        && args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
    {
        let scalar = datafusion::common::ScalarValue::try_from_array(&result, 0)?;
        Ok(ColumnarValue::Scalar(scalar))
    } else {
        Ok(ColumnarValue::Array(result))
    }
}

/// Evaluate the case when logic
fn evaluate_case_when(
    conditions: &[BooleanArray],
    values: &[ArrayRef],
    else_value: Option<&ArrayRef>,
    batch_size: usize,
    output_type: &DataType,
) -> Result<ArrayRef> {
    use arrow::array::new_null_array;

    // Initialize result with nulls or else value
    let mut result: ArrayRef = if let Some(else_array) = else_value {
        else_array.clone()
    } else {
        new_null_array(output_type, batch_size)
    };

    // Process conditions in reverse order so earlier conditions take precedence
    for i in (0..conditions.len()).rev() {
        let condition = &conditions[i];
        let value = &values[i];

        // Use arrow's zip kernel to select between current result and value based on
        // condition
        result = zip(condition, value, &result)?;
    }

    Ok(result)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, StringArray};
    use datafusion::{common::ScalarValue, logical_expr::ColumnarValue};

    use super::*;

    #[test]
    fn test_case_when_simple() -> Result<()> {
        // case_when(x > 5, 'big', 'small')
        let condition = Arc::new(BooleanArray::from(vec![true, false, true, false]));
        let value_true = Arc::new(StringArray::from(vec!["big", "big", "big", "big"]));
        let value_else = Arc::new(StringArray::from(vec!["small", "small", "small", "small"]));

        let result = spark_case_when(&[
            ColumnarValue::Array(condition),
            ColumnarValue::Array(value_true),
            ColumnarValue::Array(value_else),
        ])?;

        let expected = StringArray::from(vec!["big", "small", "big", "small"]);
        let result_array = result.into_array(4)?;

        assert_eq!(
            result_array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Failed to downcast to StringArray"),
            &expected
        );
        Ok(())
    }

    #[test]
    fn test_case_when_multiple_conditions() -> Result<()> {
        // case_when(x > 10, 100, x > 5, 50, 0)
        let x = vec![15, 8, 3, 12, 5];

        let condition1 = Arc::new(BooleanArray::from(
            x.iter().map(|&v| v > 10).collect::<Vec<_>>(),
        ));
        let value1 = Arc::new(Int32Array::from(vec![100, 100, 100, 100, 100]));

        let condition2 = Arc::new(BooleanArray::from(
            x.iter().map(|&v| v > 5).collect::<Vec<_>>(),
        ));
        let value2 = Arc::new(Int32Array::from(vec![50, 50, 50, 50, 50]));

        let else_value = Arc::new(Int32Array::from(vec![0, 0, 0, 0, 0]));

        let result = spark_case_when(&[
            ColumnarValue::Array(condition1),
            ColumnarValue::Array(value1),
            ColumnarValue::Array(condition2),
            ColumnarValue::Array(value2),
            ColumnarValue::Array(else_value),
        ])?;

        let expected = Int32Array::from(vec![100, 50, 0, 100, 0]);
        let result_array = result.into_array(5)?;

        assert_eq!(
            result_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Failed to downcast to Int32Array"),
            &expected
        );
        Ok(())
    }

    #[test]
    fn test_case_when_no_else() -> Result<()> {
        // case_when(x > 5, 100) - no else, should return NULL for non-matching
        let condition = Arc::new(BooleanArray::from(vec![true, false, true, false]));
        let value = Arc::new(Int32Array::from(vec![100, 100, 100, 100]));

        let result =
            spark_case_when(&[ColumnarValue::Array(condition), ColumnarValue::Array(value)])?;

        let expected = Int32Array::from(vec![Some(100), None, Some(100), None]);
        let result_array = result.into_array(4)?;

        assert_eq!(
            result_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Failed to downcast to Int32Array"),
            &expected
        );
        Ok(())
    }

    #[test]
    fn test_case_when_with_nulls() -> Result<()> {
        // Test handling of NULL conditions
        let condition = Arc::new(BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
        ]));
        let value = Arc::new(Int32Array::from(vec![10, 10, 10, 10]));
        let else_value = Arc::new(Int32Array::from(vec![20, 20, 20, 20]));

        let result = spark_case_when(&[
            ColumnarValue::Array(condition),
            ColumnarValue::Array(value),
            ColumnarValue::Array(else_value),
        ])?;

        // NULL conditions should be treated as false
        let expected = Int32Array::from(vec![10, 20, 20, 10]);
        let result_array = result.into_array(4)?;

        assert_eq!(
            result_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Failed to downcast to Int32Array"),
            &expected
        );
        Ok(())
    }

    #[test]
    fn test_case_when_scalar() -> Result<()> {
        // Test with scalar inputs
        let condition = ColumnarValue::Scalar(ScalarValue::Boolean(Some(true)));
        let value_true = ColumnarValue::Scalar(ScalarValue::Float64(Some(1.5)));
        let value_else = ColumnarValue::Scalar(ScalarValue::Float64(Some(2.5)));

        let result = spark_case_when(&[condition, value_true, value_else])?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Float64(Some(v))) => {
                assert_eq!(v, 1.5);
            }
            _ => {
                return df_execution_err!("Expected scalar float64");
            }
        }
        Ok(())
    }

    #[test]
    fn test_case_when_only_else() -> Result<()> {
        // Only one argument (else value)
        let else_value = ColumnarValue::Scalar(ScalarValue::Int32(Some(42)));

        let result = spark_case_when(&[else_value.clone()])?;

        match result {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(v))) => {
                assert_eq!(v, 42);
            }
            _ => {
                return df_execution_err!("Expected scalar int32");
            }
        }
        Ok(())
    }

    #[test]
    fn test_case_when_mixed_scalar_array() -> Result<()> {
        // Mix of scalar and array inputs
        let condition = Arc::new(BooleanArray::from(vec![true, false, true]));
        let value_true = ColumnarValue::Scalar(ScalarValue::Int32(Some(100)));
        let value_else = Arc::new(Int32Array::from(vec![1, 2, 3]));

        let result = spark_case_when(&[
            ColumnarValue::Array(condition),
            value_true,
            ColumnarValue::Array(value_else),
        ])?;

        let expected = Int32Array::from(vec![100, 2, 100]);
        let result_array = result.into_array(3)?;

        assert_eq!(
            result_array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("Failed to downcast to Int32Array"),
            &expected
        );
        Ok(())
    }
}
