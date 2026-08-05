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
    fmt::{Debug, Display, Formatter},
    hash::{Hash, Hasher},
    sync::Arc,
};

use arrow::{
    array::{ArrayRef, Int8Array, Int16Array, Int32Array, Int64Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{Result, ScalarValue},
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
    physical_plan::expressions::NegativeExpr,
};
use datafusion_ext_commons::{df_execution_err, downcast_any};

pub struct SparkNegativeExpr {
    expr: PhysicalExprRef,
    ansi_enabled: bool,
}

impl SparkNegativeExpr {
    pub fn new(expr: PhysicalExprRef, ansi_enabled: bool) -> Self {
        Self { expr, ansi_enabled }
    }
}

impl Display for SparkNegativeExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "negative({})", self.expr)
    }
}

impl Debug for SparkNegativeExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "negative({})", self.expr)
    }
}

impl PartialEq for SparkNegativeExpr {
    fn eq(&self, other: &Self) -> bool {
        self.expr.eq(&other.expr) && self.ansi_enabled == other.ansi_enabled
    }
}

impl Eq for SparkNegativeExpr {}

impl Hash for SparkNegativeExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
        self.ansi_enabled.hash(state);
    }
}

impl PhysicalExpr for SparkNegativeExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.expr.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.ansi_enabled
            && matches!(
                self.expr.data_type(batch.schema().as_ref())?,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        {
            return checked_negate(self.expr.evaluate(batch)?);
        }

        NegativeExpr::new(self.expr.clone()).evaluate(batch)
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::new(children[0].clone(), self.ansi_enabled)))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

fn checked_negate(value: ColumnarValue) -> Result<ColumnarValue> {
    Ok(match value {
        ColumnarValue::Scalar(scalar) => ColumnarValue::Scalar(checked_negate_scalar(scalar)?),
        ColumnarValue::Array(array) => ColumnarValue::Array(checked_negate_array(array.as_ref())?),
    })
}

fn checked_negate_scalar(scalar: ScalarValue) -> Result<ScalarValue> {
    Ok(match scalar {
        ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(checked_negate_value(v)?)),
        ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(checked_negate_value(v)?)),
        ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(checked_negate_value(v)?)),
        ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(checked_negate_value(v)?)),
        ScalarValue::Int8(None) => ScalarValue::Int8(None),
        ScalarValue::Int16(None) => ScalarValue::Int16(None),
        ScalarValue::Int32(None) => ScalarValue::Int32(None),
        ScalarValue::Int64(None) => ScalarValue::Int64(None),
        other => return df_execution_err!("unsupported ANSI negative data type: {other}"),
    })
}

macro_rules! checked_negate_primitive_array {
    ($array:expr, $array_ty:ty) => {{
        let array = downcast_any!($array, $array_ty)?;
        let mut values = Vec::with_capacity(array.len());
        for value in array.iter() {
            values.push(match value {
                Some(v) => Some(checked_negate_value(v)?),
                None => None,
            });
        }
        Ok(Arc::new(<$array_ty>::from(values)) as ArrayRef)
    }};
}

fn checked_negate_array(array: &dyn arrow::array::Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Int8 => checked_negate_primitive_array!(array, Int8Array),
        DataType::Int16 => checked_negate_primitive_array!(array, Int16Array),
        DataType::Int32 => checked_negate_primitive_array!(array, Int32Array),
        DataType::Int64 => checked_negate_primitive_array!(array, Int64Array),
        other => df_execution_err!("unsupported ANSI negative data type: {other}"),
    }
}

fn checked_negate_value<T>(value: T) -> Result<T>
where
    T: CheckedNeg,
{
    value.checked_neg().ok_or_else(|| {
        datafusion::common::DataFusionError::Execution(
            "[ARITHMETIC_OVERFLOW] arithmetic overflow in unary minus".to_string(),
        )
    })
}

trait CheckedNeg {
    fn checked_neg(self) -> Option<Self>
    where
        Self: Sized;
}

macro_rules! impl_checked_neg {
    ($($ty:ty),+) => {
        $(
            impl CheckedNeg for $ty {
                fn checked_neg(self) -> Option<Self> {
                    <$ty>::checked_neg(self)
                }
            }
        )+
    };
}

impl_checked_neg!(i8, i16, i32, i64);

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{ArrayRef, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::physical_expr::{PhysicalExpr, expressions::Column};

    use super::SparkNegativeExpr;

    #[test]
    fn test_ansi_checked_negation() -> Result<(), Box<dyn Error>> {
        let cases: Vec<(DataType, ArrayRef)> = vec![
            (
                DataType::Int8,
                Arc::new(Int8Array::from(vec![Some(i8::MIN)])),
            ),
            (
                DataType::Int16,
                Arc::new(Int16Array::from(vec![Some(i16::MIN)])),
            ),
            (
                DataType::Int32,
                Arc::new(Int32Array::from(vec![Some(i32::MIN)])),
            ),
            (
                DataType::Int64,
                Arc::new(Int64Array::from(vec![Some(i64::MIN)])),
            ),
        ];

        for (data_type, array) in cases {
            let batch = batch(data_type, array)?;
            let err = expression(true)
                .evaluate(&batch)
                .expect_err("expected overflow");
            assert!(err.to_string().contains("[ARITHMETIC_OVERFLOW]"));
        }
        Ok(())
    }

    #[test]
    fn test_non_ansi_wrapping_negation() -> Result<(), Box<dyn Error>> {
        let batch = batch(
            DataType::Int32,
            Arc::new(Int32Array::from(vec![Some(i32::MIN), Some(1), None])),
        )?;
        let output = expression(false)
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(i32::MIN), Some(-1), None]));
        assert_eq!(&output, &expected);
        Ok(())
    }

    #[test]
    fn test_float_delegates_to_datafusion() -> Result<(), Box<dyn Error>> {
        let batch = batch(
            DataType::Float64,
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
        )?;
        let output = expression(true)
            .evaluate(&batch)?
            .into_array(batch.num_rows())?;
        let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(-1.5), None]));
        assert_eq!(&output, &expected);
        Ok(())
    }

    fn expression(ansi_enabled: bool) -> Arc<SparkNegativeExpr> {
        Arc::new(SparkNegativeExpr::new(
            Arc::new(Column::new("c", 0)),
            ansi_enabled,
        ))
    }

    fn batch(data_type: DataType, array: ArrayRef) -> Result<RecordBatch, Box<dyn Error>> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("c", data_type, true)])),
            vec![array],
        )?)
    }
}
