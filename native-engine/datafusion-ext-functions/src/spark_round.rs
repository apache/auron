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
    array::{Array, ArrayRef, AsArray, Decimal128Array, PrimitiveArray},
    datatypes::{DataType, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type},
};
use bigdecimal::{BigDecimal, RoundingMode};
use datafusion::{
    common::{Result, ScalarValue, exec_datafusion_err, exec_err},
    logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
    },
};

/// Spark RoundBase: shared HALF_UP/HALF_EVEN implementation with Spark's result type.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkRound {
    half_even: bool,
    signature: Signature,
}

impl SparkRound {
    pub fn new(half_even: bool) -> Self {
        Self {
            half_even,
            signature: Signature::one_of(
                vec![TypeSignature::Any(2), TypeSignature::Any(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkRound {
    fn name(&self) -> &str {
        if self.half_even {
            "Spark_BRound"
        } else {
            "Spark_Round"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // The physical planner supplies Spark's analyzed decimal result type.
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let result_type = args.return_field.data_type();
        let scale = match args.args.get(1) {
            Some(ColumnarValue::Scalar(v)) if v.is_null() => {
                return Ok(ColumnarValue::Scalar(ScalarValue::try_from(result_type)?));
            }
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(v)))) => *v,
            Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(v)))) => {
                i32::try_from(*v).map_err(|_| {
                    datafusion::common::DataFusionError::Execution(
                        "round scale is outside the int32 range".into(),
                    )
                })?
            }
            _ => return exec_err!("round scale must be a constant integer"),
        };
        let ansi = match args.args.get(2) {
            Some(ColumnarValue::Scalar(ScalarValue::Boolean(Some(v)))) => *v,
            None => args.config_options.execution.enable_ansi_mode,
            _ => return exec_err!("round ANSI mode must be a constant boolean"),
        };
        let input = &args.args[0];
        if scale >= 0 && input.data_type().is_integer() {
            return Ok(input.clone());
        }
        let scalar = matches!(input, ColumnarValue::Scalar(_));
        let array = input.to_array(if scalar { 1 } else { args.number_rows })?;

        macro_rules! integer {
            ($t:ty, $native:ty) => {{
                let values = array.as_primitive::<$t>();
                let result: PrimitiveArray<$t> = values.try_unary(|v| {
                    // i128 can represent the intermediate 10^19 for every Spark integer.
                    let rounded = round_integer(i128::from(v), scale, self.half_even);
                    if ansi {
                        <$native>::try_from(rounded).map_err(|_| {
                            datafusion::common::DataFusionError::Execution(format!(
                                "{} overflow on {}({v}, {scale})",
                                stringify!($native),
                                self.name()
                            ))
                        })
                    } else {
                        Ok(rounded as $native)
                    }
                })?;
                Arc::new(result) as ArrayRef
            }};
        }
        let result: ArrayRef = match array.data_type() {
            DataType::Int8 => integer!(Int8Type, i8),
            DataType::Int16 => integer!(Int16Type, i16),
            DataType::Int32 => integer!(Int32Type, i32),
            DataType::Int64 => integer!(Int64Type, i64),
            DataType::Float32 => Arc::new(
                array
                    .as_primitive::<Float32Type>()
                    .try_unary::<_, Float32Type, _>(|v| {
                        round_float(f64::from(v), scale, self.half_even).map(|v| v as f32)
                    })?,
            ),
            DataType::Float64 => Arc::new(
                array
                    .as_primitive::<Float64Type>()
                    .try_unary::<_, Float64Type, _>(|v| round_float(v, scale, self.half_even))?,
            ),
            DataType::Decimal128(_, input_scale) => {
                let DataType::Decimal128(precision, output_scale) = result_type else {
                    return exec_err!("round decimal result must be decimal128");
                };
                // Produce the final unscaled value directly; do not round and then cast
                // an intermediate array carrying the input precision and scale.
                let mut builder = Decimal128Array::builder(array.len());
                for value in array.as_primitive::<arrow::datatypes::Decimal128Type>() {
                    builder.append_option(match value {
                        Some(v) => round_decimal(
                            v,
                            *input_scale,
                            scale,
                            *precision,
                            *output_scale,
                            self.half_even,
                        )?,
                        None => None,
                    });
                }
                Arc::new(
                    builder
                        .finish()
                        .with_precision_and_scale(*precision, *output_scale)?,
                )
            }
            DataType::Null => {
                return Ok(ColumnarValue::Scalar(ScalarValue::try_from(result_type)?));
            }
            t => return exec_err!("unsupported Spark rounding type: {t}"),
        };
        if scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

/// Divide by a power of ten with the requested tie rule. Spark decimals have
/// at most 38 digits; rounding further to the left always produces zero.
fn rounded_quotient(value: i128, digits: i64, half_even: bool) -> i128 {
    if digits <= 0 {
        return value;
    }
    if digits > 38 {
        return 0;
    }
    let factor = 10_i128.pow(digits as u32);
    let quotient = value / factor;
    let remainder = (value % factor).abs();
    let half = factor / 2;
    if remainder > half || (remainder == half && (!half_even || quotient % 2 != 0)) {
        quotient + value.signum()
    } else {
        quotient
    }
}

fn round_integer(value: i128, scale: i32, half_even: bool) -> i128 {
    if scale >= 0 {
        return value;
    }
    let digits = -i64::from(scale);
    // All Spark integral values are i64 or narrower.
    if digits > 19 {
        return 0;
    }
    rounded_quotient(value, digits, half_even) * 10_i128.pow(digits as u32)
}

fn round_decimal(
    value: i128,
    input_scale: i8,
    scale: i32,
    precision: u8,
    output_scale: i8,
    half_even: bool,
) -> Result<Option<i128>> {
    let rounding_scale = i64::from(scale).min(i64::from(input_scale));
    let quotient = rounded_quotient(value, i64::from(input_scale) - rounding_scale, half_even);
    let shift = i64::from(output_scale) - rounding_scale;
    let result = if quotient == 0 {
        Some(0)
    } else if shift < 0 {
        return exec_err!("round result scale is below the rounding scale");
    } else {
        u32::try_from(shift)
            .ok()
            .and_then(|s| 10_i128.checked_pow(s))
            .and_then(|factor| quotient.checked_mul(factor))
    }
    .filter(|v| v.abs() < 10_i128.pow(u32::from(precision)));
    // Spark Decimal.toPrecision returns null; Decimal.set for a negative scale
    // throws on precision overflow, independently of ANSI mode.
    if result.is_none() && scale < 0 {
        return exec_err!("decimal precision exceeds {precision} while rounding");
    }
    Ok(result)
}

fn round_float(value: f64, scale: i32, half_even: bool) -> Result<f64> {
    if !value.is_finite() {
        return Ok(value);
    }
    if value == 0.0 || scale <= -309 {
        return Ok(0.0);
    }
    if scale >= 324 || (scale >= 0 && value.fract() == 0.0) {
        return Ok(value);
    }
    if scale == 0 {
        let result = if half_even {
            value.round_ties_even()
        } else {
            value.round()
        };
        return Ok(if result == 0.0 { 0.0 } else { result });
    }
    // Spark rounds BigDecimal.valueOf(double), including float inputs widened
    // to double, rather than multiplying a binary float by a power of ten.
    let decimal = value
        .to_string()
        .parse::<BigDecimal>()
        .map_err(|e| exec_datafusion_err!("cannot convert floating-point value to decimal: {e}"))?;
    let mode = if half_even {
        RoundingMode::HalfEven
    } else {
        RoundingMode::HalfUp
    };
    let result = decimal
        .with_scale_round(i64::from(scale), mode)
        .to_string()
        .parse::<f64>()
        .map_err(|e| {
            exec_datafusion_err!("cannot convert rounded decimal to floating-point value: {e}")
        })?;
    Ok(if result == 0.0 { 0.0 } else { result })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion::common::config::ConfigOptions;

    fn invoke(
        value: ScalarValue,
        scale: Option<i32>,
        output_type: DataType,
        even: bool,
        ansi: bool,
        array: bool,
    ) -> Result<ColumnarValue> {
        let input = if array {
            ColumnarValue::Array(value.to_array_of_size(2)?)
        } else {
            ColumnarValue::Scalar(value)
        };
        let args = vec![
            input,
            ColumnarValue::Scalar(ScalarValue::Int32(scale)),
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(ansi))),
        ];
        SparkRound::new(even).invoke_with_args(ScalarFunctionArgs {
            arg_fields: args
                .iter()
                .map(|v| Arc::new(Field::new("arg", v.data_type(), true)))
                .collect(),
            args,
            number_rows: 2,
            return_field: Arc::new(Field::new("result", output_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn decimal_ties_scales_and_overflow() -> Result<()> {
        for array in [false, true] {
            for even in [false, true] {
                for (value, input_scale, scale, precision, output_scale, expected) in [
                    (
                        Some(2685),
                        3,
                        Some(2),
                        10,
                        2,
                        Some(if even { 268 } else { 269 }),
                    ),
                    (
                        Some(-2685),
                        3,
                        Some(2),
                        10,
                        2,
                        Some(if even { -268 } else { -269 }),
                    ),
                    (Some(2675), 3, Some(2), 10, 2, Some(268)),
                    (
                        Some(125),
                        0,
                        Some(-1),
                        10,
                        0,
                        Some(if even { 120 } else { 130 }),
                    ),
                    (Some(2675), 3, Some(8), 10, 3, Some(2675)),
                    (None, 3, Some(2), 10, 2, None),
                    (Some(2675), 3, None, 10, 0, None),
                    (
                        Some(10_i128.pow(38) - 1),
                        38,
                        Some(37),
                        38,
                        37,
                        Some(10_i128.pow(37)),
                    ),
                ] {
                    let output_type = DataType::Decimal128(precision, output_scale);
                    let result = invoke(
                        ScalarValue::Decimal128(value, 38, input_scale),
                        scale,
                        output_type.clone(),
                        even,
                        false,
                        array,
                    )?;
                    assert_eq!(result.data_type(), output_type);
                    let expected = ScalarValue::Decimal128(expected, precision, output_scale)
                        .to_array_of_size(2)?;
                    assert_eq!(result.into_array(2)?.as_ref(), expected.as_ref());
                }
                for ansi in [false, true] {
                    assert!(
                        invoke(
                            ScalarValue::Decimal128(Some(10_i128.pow(38) - 1), 38, 0),
                            Some(-1),
                            DataType::Decimal128(38, 0),
                            even,
                            ansi,
                            array
                        )
                        .is_err()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn integer_limits_and_ansi() -> Result<()> {
        for array in [false, true] {
            for even in [false, true] {
                for value in [i64::MIN, i64::MAX] {
                    let expected = (i128::from(value.signum()) * 10_i128.pow(19)) as i64;
                    let result = invoke(
                        ScalarValue::Int64(Some(value)),
                        Some(-19),
                        DataType::Int64,
                        even,
                        false,
                        array,
                    )?;
                    assert_eq!(
                        result.into_array(2)?.as_ref(),
                        ScalarValue::Int64(Some(expected))
                            .to_array_of_size(2)?
                            .as_ref()
                    );
                    assert!(
                        invoke(
                            ScalarValue::Int64(Some(value)),
                            Some(-19),
                            DataType::Int64,
                            even,
                            true,
                            array
                        )
                        .is_err()
                    );
                }
                for value in [
                    ScalarValue::Int8(Some(127)),
                    ScalarValue::Int16(Some(32767)),
                    ScalarValue::Int32(Some(i32::MAX)),
                ] {
                    assert!(
                        invoke(
                            value.clone(),
                            Some(-1),
                            value.data_type(),
                            even,
                            true,
                            array
                        )
                        .is_err()
                    );
                }
                let null = invoke(
                    ScalarValue::Int64(None),
                    Some(-19),
                    DataType::Int64,
                    even,
                    true,
                    array,
                )?;
                assert_eq!(null.into_array(2)?.null_count(), 2);
            }
        }
        Ok(())
    }

    #[test]
    fn floating_point_rounding_and_positive_zero() -> Result<()> {
        for array in [false, true] {
            for even in [false, true] {
                for (value, scale, expected) in [
                    (1.015, 2, 1.02),
                    (2.5, 0, if even { 2.0 } else { 3.0 }),
                    (-2.5, 0, if even { -2.0 } else { -3.0 }),
                    (3.5, 0, 4.0),
                    (1e23, 0, 1e23),
                    (2.685, 2, if even { 2.68 } else { 2.69 }),
                    (-0.0, 2, 0.0),
                    (-0.01, 0, 0.0),
                    (f64::INFINITY, 0, f64::INFINITY),
                ] {
                    let result = invoke(
                        ScalarValue::Float64(Some(value)),
                        Some(scale),
                        DataType::Float64,
                        even,
                        false,
                        array,
                    )?
                    .into_array(2)?;
                    assert_eq!(
                        result.as_primitive::<Float64Type>().value(0).to_bits(),
                        expected.to_bits()
                    );
                }
            }
        }
        Ok(())
    }
}
