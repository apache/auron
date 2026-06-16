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

use arrow::{array::*, datatypes::DataType};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::{
    df_execution_err, downcast_any, scalar_value::compacted_scalar_value_from_array,
};

pub fn array_reverse(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    match &args[0] {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(reverse_list_array(
            downcast_any!(array, ListArray)?,
        )?)),
        ColumnarValue::Scalar(scalar) => match scalar.data_type() {
            DataType::List(_) => {
                let array = scalar.to_array()?;
                let reversed = reverse_list_array(downcast_any!(&array, ListArray)?)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &reversed, 0,
                )?))
            }
            data_type => df_execution_err!("array_reverse only supports list, got {data_type:?}"),
        },
    }
}

fn reverse_list_array(array: &ListArray) -> Result<ArrayRef> {
    let DataType::List(field) = array.data_type() else {
        return df_execution_err!(
            "array_reverse only supports list, got {:?}",
            array.data_type()
        );
    };

    let mut values = Vec::with_capacity(array.len());
    for row_idx in 0..array.len() {
        if array.is_null(row_idx) {
            values.push(ScalarValue::List(Arc::new(ListArray::new_null(
                field.clone(),
                1,
            ))));
        } else {
            let row = array.value(row_idx);
            let mut reversed = Vec::with_capacity(row.len());
            for value_idx in (0..row.len()).rev() {
                reversed.push(compacted_scalar_value_from_array(&row, value_idx)?);
            }
            values.push(ScalarValue::List(ScalarValue::new_list(
                &reversed,
                field.data_type(),
                field.is_nullable(),
            )));
        }
    }

    ScalarValue::iter_to_array(values)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::datatypes::Int32Type;
    use datafusion::common::ScalarValue;

    use super::*;

    #[test]
    fn test_array_reverse_int() -> Result<()> {
        let input: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![]),
            None,
        ]));

        let result = array_reverse(&[ColumnarValue::Array(input)])?.into_array(4)?;
        let expected: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(3), Some(2), Some(1)]),
            Some(vec![Some(5), Some(4)]),
            Some(vec![]),
            None,
        ]));

        assert_eq!(&result, &expected);
        Ok(())
    }

    #[test]
    fn test_array_reverse_scalar() -> Result<()> {
        let input = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::from(1),
                ScalarValue::from(2),
                ScalarValue::from(3),
            ],
            &DataType::Int32,
            true,
        ));

        let result = array_reverse(&[ColumnarValue::Scalar(input)])?.into_array(1)?;
        let expected = ScalarValue::List(ScalarValue::new_list(
            &[
                ScalarValue::from(3),
                ScalarValue::from(2),
                ScalarValue::from(1),
            ],
            &DataType::Int32,
            true,
        ))
        .to_array()?;

        assert_eq!(&result, &expected);
        Ok(())
    }
}
