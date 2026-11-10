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

//! Thin `ScalarUDF` adapters that reconcile Auron's ext-function calling
//! convention with the `datafusion-spark` UDFs they delegate to.
//!
//! Adapters supply Spark's planned array types, SHA-2 width, and session timezone.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int32Array, make_array},
    datatypes::{DataType, Field, TimeUnit},
};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue, config::ConfigOptions, exec_err},
    logical_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility,
    },
};
use datafusion_ext_commons::arrow::cast::cast;

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

/// Reinterprets a naive timestamp array so that `arrow` date-part extraction
/// runs in `tz`. Value buffers are unchanged — only the type's timezone label
/// is set — so this is zero-copy.
fn localize_array(array: &ArrayRef, tz: &str) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Timestamp(unit, None) => {
            let dt = DataType::Timestamp(*unit, Some(Arc::from(tz)));
            let data = array
                .to_data()
                .into_builder()
                .data_type(dt)
                .build()
                .map_err(|e| DataFusionError::Execution(format!("localize timestamp: {e}")))?;
            Ok(make_array(data))
        }
        DataType::Timestamp(_, Some(_)) | DataType::Date32 | DataType::Date64 | DataType::Null => {
            Ok(array.clone())
        }
        other => exec_err!("localize timestamp: expected timestamp/date, got {other:?}"),
    }
}

fn localize_scalar(scalar: &ScalarValue, tz: &str) -> Result<ScalarValue> {
    let tz: Arc<str> = Arc::from(tz);
    Ok(match scalar {
        ScalarValue::TimestampSecond(v, None) => ScalarValue::TimestampSecond(*v, Some(tz)),
        ScalarValue::TimestampMillisecond(v, None) => {
            ScalarValue::TimestampMillisecond(*v, Some(tz))
        }
        ScalarValue::TimestampMicrosecond(v, None) => {
            ScalarValue::TimestampMicrosecond(*v, Some(tz))
        }
        ScalarValue::TimestampNanosecond(v, None) => ScalarValue::TimestampNanosecond(*v, Some(tz)),
        other => other.clone(),
    })
}

fn localize_value(value: &ColumnarValue, tz: &ColumnarValue) -> Result<ColumnarValue> {
    let Some(tz_str) = scalar_str(tz) else {
        return Ok(value.clone());
    };
    Ok(match value {
        ColumnarValue::Array(array) => ColumnarValue::Array(localize_array(array, &tz_str)?),
        ColumnarValue::Scalar(scalar) => ColumnarValue::Scalar(localize_scalar(scalar, &tz_str)?),
    })
}

fn scalar_str(value: &ColumnarValue) -> Option<String> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Some(s.clone()),
        _ => None,
    }
}

fn add_one_i32(value: ColumnarValue) -> Result<ColumnarValue> {
    Ok(match value {
        ColumnarValue::Array(array) => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| DataFusionError::Internal("expected Int32Array".into()))?;
            let result = Int32Array::from_iter(arr.iter().map(|v| v.map(|x| x.wrapping_add(1))));
            ColumnarValue::Array(Arc::new(result))
        }
        ColumnarValue::Scalar(ScalarValue::Int32(v)) => {
            ColumnarValue::Scalar(ScalarValue::Int32(v.map(|x| x.wrapping_add(1))))
        }
        other => return exec_err!("add_one: expected int32, got {other:?}"),
    })
}

fn forward(
    inner: &Arc<ScalarUDF>,
    args: Vec<ColumnarValue>,
    arg_fields: Vec<Arc<Field>>,
    number_rows: usize,
    return_field: Arc<Field>,
    config_options: Arc<ConfigOptions>,
) -> Result<ColumnarValue> {
    inner.invoke_with_args(ScalarFunctionArgs {
        args,
        arg_fields,
        number_rows,
        return_field,
        config_options,
    })
}

// ---------------------------------------------------------------------------
// MakeArrayUdf — coerce nested elements to Spark's planned common type
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MakeArrayUdf {
    inner: Arc<ScalarUDF>,
    return_type: DataType,
    sig: Signature,
}

impl MakeArrayUdf {
    pub fn new(return_type: DataType) -> Self {
        Self {
            inner: datafusion_spark::function::array::array(),
            return_type,
            sig: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MakeArrayUdf {
    fn name(&self) -> &str {
        "Spark_MakeArray"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let DataType::List(element) = &self.return_type else {
            return exec_err!("array: expected a List return type");
        };
        // Auron builds physical expressions directly, bypassing DataFusion's
        // logical coercion of nested field names and nullability.
        args.args = args
            .args
            .into_iter()
            .map(|value| cast_value(value, element.data_type()))
            .collect::<Result<_>>()?;
        args.arg_fields = args
            .arg_fields
            .into_iter()
            .map(|field| {
                Arc::new(
                    field
                        .as_ref()
                        .clone()
                        .with_data_type(element.data_type().clone()),
                )
            })
            .collect();
        args.return_field = self.inner.return_field_from_args(ReturnFieldArgs {
            arg_fields: &args.arg_fields,
            scalar_arguments: &args
                .args
                .iter()
                .map(|value| match value {
                    ColumnarValue::Scalar(scalar) => Some(scalar),
                    ColumnarValue::Array(_) => None,
                })
                .collect::<Vec<_>>(),
        })?;
        // SparkArray always marks its elements nullable; retain Spark's schema.
        cast_value(self.inner.invoke_with_args(args)?, &self.return_type)
    }
}

fn cast_value(value: ColumnarValue, data_type: &DataType) -> Result<ColumnarValue> {
    if value.data_type() == *data_type {
        return Ok(value);
    }
    Ok(match value {
        ColumnarValue::Array(array) => ColumnarValue::Array(cast(array.as_ref(), data_type)?),
        ColumnarValue::Scalar(scalar) => ColumnarValue::Scalar(ScalarValue::try_from_array(
            &cast(scalar.to_array()?.as_ref(), data_type)?,
            0,
        )?),
    })
}

// ---------------------------------------------------------------------------
// Sha2BitsUdf — Spark_Sha224/256/384/512 -> sha2(value, bits)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Sha2BitsUdf {
    bits: i32,
    inner: Arc<ScalarUDF>,
    return_type: DataType,
    sig: Signature,
}

impl Sha2BitsUdf {
    pub fn new(bits: i32, return_type: DataType) -> Self {
        Self {
            bits,
            inner: datafusion_spark::function::hash::sha2(),
            return_type,
            sig: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Sha2BitsUdf {
    fn name(&self) -> &str {
        "spark_sha2"
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value] = args.args.as_mut_slice() else {
            return exec_err!("sha2: expected one value argument");
        };
        // Physical plans bypass DataFusion's logical string-to-binary coercion.
        *value = match value {
            ColumnarValue::Array(array) => {
                ColumnarValue::Array(arrow::compute::cast(array, &DataType::Binary)?)
            }
            ColumnarValue::Scalar(value) => {
                ColumnarValue::Scalar(value.cast_to(&DataType::Binary)?)
            }
        };
        args.arg_fields = vec![Arc::new(Field::new("value", DataType::Binary, true))];
        let number_rows = args.number_rows;
        let return_field = args.return_field.clone();
        let config_options = args.config_options.clone();
        args.args
            .push(ColumnarValue::Scalar(ScalarValue::Int32(Some(self.bits))));
        args.arg_fields
            .push(Arc::new(Field::new("bits", DataType::Int32, false)));
        forward(
            &self.inner,
            args.args,
            args.arg_fields,
            number_rows,
            return_field,
            config_options,
        )
    }
}

// ---------------------------------------------------------------------------
// TzExtractUdf — Spark_Hour/Minute/Second -> hour/minute/second(localized)
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TzExtractUdf {
    inner: Arc<ScalarUDF>,
    name: &'static str,
    return_type: DataType,
    sig: Signature,
}

impl TzExtractUdf {
    pub fn new(inner: Arc<ScalarUDF>, name: &'static str, return_type: DataType) -> Self {
        Self {
            inner,
            name,
            return_type,
            sig: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for TzExtractUdf {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: mut values,
            arg_fields: mut fields,
            number_rows,
            return_field,
            config_options,
        } = args;
        if values.is_empty() {
            return exec_err!("{}: expected at least (value, timezone)", self.name);
        }
        let localized = if values.len() >= 2 {
            let tz = values[1].clone();
            localize_value(&values[0], &tz)?
        } else {
            values[0].clone()
        };
        values.truncate(1);
        fields.truncate(1);
        values[0] = localized;
        forward(
            &self.inner,
            values,
            fields,
            number_rows,
            return_field,
            config_options,
        )
    }
}

// ---------------------------------------------------------------------------
// DatePartTzUdf — Spark_Year/Month/Day/DayOfYear/DayOfWeek/WeekOfYear/Quarter
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct DatePartTzUdf {
    part: &'static str,
    add_one: bool,
    name: &'static str,
    inner: Arc<ScalarUDF>,
    return_type: DataType,
    sig: Signature,
}

impl DatePartTzUdf {
    pub fn new(
        part: &'static str,
        add_one: bool,
        name: &'static str,
        return_type: DataType,
    ) -> Self {
        Self {
            part,
            add_one,
            name,
            inner: datafusion::functions::datetime::date_part(),
            return_type,
            sig: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for DatePartTzUdf {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.sig
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: values,
            arg_fields: fields,
            number_rows,
            return_field,
            config_options,
        } = args;
        if values.len() < 2 {
            return exec_err!("{}: expected (value, timezone)", self.name);
        }
        let tz = values[1].clone();
        let localized = localize_value(&values[0], &tz)?;
        let value_field = fields.first().cloned().unwrap_or_else(|| {
            Arc::new(Field::new(
                "value",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ))
        });
        let out = forward(
            &self.inner,
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(self.part.to_string()))),
                localized,
            ],
            vec![
                Arc::new(Field::new("part", DataType::Utf8, false)),
                value_field,
            ],
            number_rows,
            return_field,
            config_options,
        )?;
        if self.add_one {
            add_one_i32(out)
        } else {
            Ok(out)
        }
    }
}
