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
    array::{ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array},
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    common::Result,
    logical_expr::ColumnarValue,
    physical_expr::{PhysicalExpr, PhysicalExprRef},
};
use datafusion_ext_commons::{df_execution_err, downcast_any};

pub struct SparkNegativeExpr {
    expr: PhysicalExprRef,
}

impl SparkNegativeExpr {
    pub fn new(expr: PhysicalExprRef) -> Self {
        Self { expr }
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
        self.expr.eq(&other.expr)
    }
}

impl Eq for SparkNegativeExpr {}

impl Hash for SparkNegativeExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.expr.hash(state);
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
        let value = self.expr.evaluate(batch)?;
        Ok(match value {
            ColumnarValue::Scalar(scalar) => ColumnarValue::Scalar(negate_scalar(scalar)?),
            ColumnarValue::Array(array) => ColumnarValue::Array(negate_array(array.as_ref())?),
        })
    }

    fn children(&self) -> Vec<&PhysicalExprRef> {
        vec![&self.expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<PhysicalExprRef>,
    ) -> Result<PhysicalExprRef> {
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "fmt_sql not used")
    }
}

fn negate_scalar(scalar: datafusion::common::ScalarValue) -> Result<datafusion::common::ScalarValue> {
    use datafusion::common::ScalarValue;

    Ok(match scalar {
        ScalarValue::Int8(Some(v)) => ScalarValue::Int8(Some(negate_value(v)?)),
        ScalarValue::Int16(Some(v)) => ScalarValue::Int16(Some(negate_value(v)?)),
        ScalarValue::Int32(Some(v)) => ScalarValue::Int32(Some(negate_value(v)?)),
        ScalarValue::Int64(Some(v)) => ScalarValue::Int64(Some(negate_value(v)?)),
        ScalarValue::Int8(None) => ScalarValue::Int8(None),
        ScalarValue::Int16(None) => ScalarValue::Int16(None),
        ScalarValue::Int32(None) => ScalarValue::Int32(None),
        ScalarValue::Int64(None) => ScalarValue::Int64(None),
        other => return df_execution_err!("unsupported data type for SparkNegativeExpr: {other}"),
    })
}

macro_rules! negate_primitive_array {
    ($array:expr, $array_ty:ty) => {{
        let array = downcast_any!($array, $array_ty)?;
        let mut values = Vec::with_capacity(array.len());
        for value in array.iter() {
            values.push(match value {
                Some(v) => Some(negate_value(v)?),
                None => None,
            });
        }
        Ok(Arc::new(<$array_ty>::from(values)) as ArrayRef)
    }};
}

fn negate_array(array: &dyn arrow::array::Array) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Int8 => negate_primitive_array!(array, Int8Array),
        DataType::Int16 => negate_primitive_array!(array, Int16Array),
        DataType::Int32 => negate_primitive_array!(array, Int32Array),
        DataType::Int64 => negate_primitive_array!(array, Int64Array),
        other => df_execution_err!("unsupported data type for SparkNegativeExpr: {other}"),
    }
}

fn negate_value<T>(value: T) -> Result<T>
where
    T: CheckedNeg,
{
    value
        .checked_neg()
        .ok_or_else(|| datafusion::common::DataFusionError::Execution(
            "[ARITHMETIC_OVERFLOW] arithmetic overflow in unary minus".to_string(),
        ))
}

trait CheckedNeg {
    fn checked_neg(self) -> Option<Self>
    where
        Self: Sized;
}

impl CheckedNeg for i8 {
    fn checked_neg(self) -> Option<Self> {
        i8::checked_neg(self)
    }
}

impl CheckedNeg for i16 {
    fn checked_neg(self) -> Option<Self> {
        i16::checked_neg(self)
    }
}

impl CheckedNeg for i32 {
    fn checked_neg(self) -> Option<Self> {
        i32::checked_neg(self)
    }
}

impl CheckedNeg for i64 {
    fn checked_neg(self) -> Option<Self> {
        i64::checked_neg(self)
    }
}

#[cfg(test)]
mod test {
    use std::{error::Error, sync::Arc};

    use arrow::{
        array::{ArrayRef, Int32Array, Int64Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::physical_expr::{PhysicalExpr, expressions::Column};

    use super::SparkNegativeExpr;

    #[test]
    fn test_int32_array() -> Result<(), Box<dyn Error>> {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(-2), None, Some(3)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int32, true)])),
            vec![array],
        )?;
        let expr = Arc::new(SparkNegativeExpr::new(Arc::new(Column::new("c", 0))));
        let output = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(-1), Some(2), None, Some(-3)]));
        assert_eq!(&output, &expected);
        Ok(())
    }

    #[test]
    fn test_int64_scalar_overflow() -> Result<(), Box<dyn Error>> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("c", DataType::Int64, true)])),
            vec![Arc::new(Int64Array::from(vec![Some(i64::MIN)])) as ArrayRef],
        )?;
        let expr = Arc::new(SparkNegativeExpr::new(Arc::new(Column::new("c", 0))));
        let err = expr.evaluate(&batch).expect_err("expected overflow");
        assert!(err.to_string().contains("[ARITHMETIC_OVERFLOW]"));
        Ok(())
    }
}
