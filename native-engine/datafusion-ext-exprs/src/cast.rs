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

use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

use arrow::{datatypes::*, record_batch::RecordBatch};
use datafusion::{
    common::Result,
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
    scalar::ScalarValue,
};

/// cast expression compatible with spark
#[derive(Debug, Eq, Hash)]
pub struct TryCastExpr {
    pub expr: PhysicalExprRef,
    pub cast_type: DataType,
}

impl PartialEq for TryCastExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.cast_type == other.cast_type
    }
}

impl TryCastExpr {
    pub fn new(expr: PhysicalExprRef, cast_type: DataType) -> Self {
        Self { expr, cast_type }
    }
}

impl Display for TryCastExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "cast({} AS {:?})", self.expr, self.cast_type)
    }
}

impl PhysicalExpr for TryCastExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        Ok(match self.expr.evaluate(batch)? {
            ColumnarValue::Array(array) => ColumnarValue::Array(
                datafusion_ext_commons::arrow::cast::cast(&array, &self.cast_type)?,
            ),
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array()?;
                ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &datafusion_ext_commons::arrow::cast::cast(&array, &self.cast_type)?,
                    0,
                )?)
            }
        })
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.cast_type.clone(),
        )))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Float32Array, Int32Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::physical_expr::{PhysicalExpr, expressions as phys_expr};

    use crate::cast::TryCastExpr;

    fn make_batch(array: ArrayRef, data_type: DataType) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("col", data_type, true)]));
        RecordBatch::try_new(schema, vec![array]).expect("Error creating RecordBatch")
    }

    fn evaluate(expr: Arc<TryCastExpr>, batch: &RecordBatch) -> ArrayRef {
        expr.evaluate(batch)
            .expect("Error evaluating expr")
            .into_array(batch.num_rows())
            .expect("Expression did not return array")
    }

    #[test]
    fn cast_float32_array_into_int32() {
        let batch = make_batch(
            Arc::new(Float32Array::from(vec![
                Some(7.6),
                Some(9.0),
                Some(3.4),
                Some(-0.0),
                Some(-99.9),
                None,
            ])),
            DataType::Float32,
        );

        let expr = Arc::new(TryCastExpr::new(
            phys_expr::col("col", &batch.schema()).unwrap(),
            DataType::Int32,
        ));

        let actual = evaluate(expr, &batch);
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(7),
            Some(9),
            Some(3),
            Some(0),
            Some(-99),
            None,
        ]));
        assert_eq!(&actual, &expected);
    }

    #[test]
    fn cast_utf8_array_into_float32() {
        let batch = make_batch(
            Arc::new(StringArray::from(vec![
                Some("123"),
                Some("321.9"),
                Some("-098"),
                Some("sda"),
                None,
            ])),
            DataType::Utf8,
        );

        let expr = Arc::new(TryCastExpr::new(
            phys_expr::col("col", &batch.schema()).unwrap(),
            DataType::Float32,
        ));

        let actual = evaluate(expr, &batch);
        let expected: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(123.0),
            Some(321.9),
            Some(-98.0),
            None,
            None,
        ]));
        assert_eq!(&actual, &expected);
    }

    #[test]
    fn cast_utf8_scalar_into_float32() {
        let batch = make_batch(
            Arc::new(StringArray::from(vec![
                Some("123"),
                Some("321.9"),
                Some("-098"),
                Some("sda"),
                None,
            ])),
            DataType::Utf8,
        );

        let expr = Arc::new(TryCastExpr::new(phys_expr::lit("123.4"), DataType::Float32));

        let actual = evaluate(expr, &batch);
        let expected: ArrayRef = Arc::new(Float32Array::from(vec![
            Some(123.4),
            Some(123.4),
            Some(123.4),
            Some(123.4),
            Some(123.4),
        ]));
        assert_eq!(&actual, &expected);
    }

    #[test]
    fn cast_trimmed_utf8_into_int32() {
        let batch = make_batch(
            Arc::new(StringArray::from(vec![Some(" 2"), Some("3 "), Some(" 4 ")])),
            DataType::Utf8,
        );

        let expr = Arc::new(TryCastExpr::new(
            phys_expr::col("col", &batch.schema()).unwrap(),
            DataType::Int32,
        ));

        let actual = evaluate(expr, &batch);
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(2), Some(3), Some(4)]));
        assert_eq!(&actual, &expected);
    }

    #[test]
    fn cast_trimmed_utf8_into_float32() {
        let batch = make_batch(
            Arc::new(StringArray::from(vec![
                Some(" 2.5"),
                Some("6.75 "),
                Some(" 8.125 "),
            ])),
            DataType::Utf8,
        );

        let expr = Arc::new(TryCastExpr::new(
            phys_expr::col("col", &batch.schema()).unwrap(),
            DataType::Float32,
        ));

        let actual = evaluate(expr, &batch);
        let expected: ArrayRef =
            Arc::new(Float32Array::from(vec![Some(2.5), Some(6.75), Some(8.125)]));
        assert_eq!(&actual, &expected);
    }
}
