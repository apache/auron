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

use std::sync::{Arc, OnceLock};

use datafusion::{
    common::Result,
    logical_expr::{ScalarFunctionImplementation, ScalarUDF, Volatility},
};
use datafusion_ext_commons::df_unimplemented_err;

mod adapters;
mod brickhouse;
mod flink_datetime;
mod spark_array;
mod spark_check_overflow;
mod spark_crypto;
mod spark_dates;
pub mod spark_get_json_object;
mod spark_hash;
mod spark_initcap;
mod spark_isnan;
mod spark_make_decimal;
mod spark_map;
mod spark_normalize_nan_and_zero;
mod spark_null_if;
pub mod spark_round;
mod spark_strings;
mod spark_unscaled_value;
#[cfg(test)]
mod udf_tests;

macro_rules! shared_function {
    ($function:expr) => {{
        static INSTANCE: OnceLock<ScalarFunctionImplementation> = OnceLock::new();
        Arc::clone(INSTANCE.get_or_init(|| Arc::new($function)))
    }};
}

/// Compatibility implementations for functions not delegated by `create_auron_ext_udf`.
#[allow(clippy::panic)] // Temporarily allow panic to refactor to Result later
pub fn create_auron_ext_function(
    name: &str,
    #[allow(unused_variables)] spark_partition_id: usize,
) -> Result<ScalarFunctionImplementation> {
    // auron ext functions, if used for spark should be start with 'Spark_',
    // if used for flink should be start with 'Flink_',
    // same to other engines.
    Ok(match name {
        "Placeholder" => shared_function!(|_| panic!("placeholder() should never be called")),
        "Spark_NullIf" => shared_function!(spark_null_if::spark_null_if),
        "Spark_NullIfZero" => shared_function!(spark_null_if::spark_null_if_zero),
        "Spark_UnscaledValue" => shared_function!(spark_unscaled_value::spark_unscaled_value),
        "Spark_MakeDecimal" => shared_function!(spark_make_decimal::spark_make_decimal),
        "Spark_CheckOverflow" => shared_function!(spark_check_overflow::spark_check_overflow),
        "Spark_Murmur3Hash" => shared_function!(spark_hash::spark_murmur3_hash),
        "Spark_MD5" => shared_function!(spark_crypto::spark_md5),
        "Spark_GetJsonObject" => shared_function!(spark_get_json_object::spark_get_json_object),
        "Spark_GetParsedJsonObject" => {
            shared_function!(spark_get_json_object::spark_get_parsed_json_object)
        }
        "Spark_ParseJson" => shared_function!(spark_get_json_object::spark_parse_json),
        "Spark_ArrayDistinct" => shared_function!(spark_array::array_distinct),
        "Spark_ArrayUnion" => shared_function!(spark_array::array_union),
        "Spark_ArrayContains" => shared_function!(spark_array::array_contains),
        "Spark_ArrayReverse" => shared_function!(spark_array::array_reverse),
        "Spark_ArrayFlatten" => shared_function!(spark_array::array_flatten),
        "Spark_MapConcat" => shared_function!(spark_map::map_concat),
        "Spark_StringRepeat" => shared_function!(spark_strings::string_repeat),
        "Spark_StringSplit" => shared_function!(spark_strings::string_split),
        "Spark_StringLower" => shared_function!(spark_strings::string_lower),
        "Spark_StringUpper" => shared_function!(spark_strings::string_upper),
        "Spark_InitCap" => shared_function!(spark_initcap::string_initcap),
        "Spark_MapFromArrays" => shared_function!(spark_map::map_from_arrays),
        "Spark_MapFromEntries" => shared_function!(spark_map::map_from_entries),
        "Spark_StrToMap" => shared_function!(spark_map::str_to_map),
        "Spark_Year" => shared_function!(spark_dates::spark_year),
        "Spark_Month" => shared_function!(spark_dates::spark_month),
        "Spark_Day" => shared_function!(spark_dates::spark_day),
        "Spark_DayOfYear" => shared_function!(spark_dates::spark_dayofyear),
        "Spark_DayOfWeek" => shared_function!(spark_dates::spark_dayofweek),
        "Spark_WeekOfYear" => shared_function!(spark_dates::spark_weekofyear),
        "Spark_Quarter" => shared_function!(spark_dates::spark_quarter),
        "Spark_LastDay" => shared_function!(spark_dates::spark_last_day),
        "Spark_Trunc" => shared_function!(spark_dates::spark_trunc),
        "Spark_NextDay" => shared_function!(spark_dates::spark_next_day),
        "Spark_DateDiff" => shared_function!(spark_dates::spark_datediff),
        "Spark_DateAdd" => shared_function!(spark_dates::spark_date_add),
        "Spark_DateSub" => shared_function!(spark_dates::spark_date_sub),
        "Spark_AddMonths" => shared_function!(spark_dates::spark_add_months),
        "Spark_Hour" => shared_function!(spark_dates::spark_hour),
        "Spark_Minute" => shared_function!(spark_dates::spark_minute),
        "Spark_Second" => shared_function!(spark_dates::spark_second),
        "Spark_MakeDate" => shared_function!(spark_dates::spark_make_date),
        "Spark_MonthsBetween" => shared_function!(spark_dates::spark_months_between),
        "Spark_BrickhouseArrayUnion" => shared_function!(brickhouse::array_union::array_union),
        "Spark_NormalizeNanAndZero" => {
            shared_function!(spark_normalize_nan_and_zero::spark_normalize_nan_and_zero)
        }
        "Spark_IsNaN" => shared_function!(spark_isnan::spark_isnan),
        "Flink_UnixTimestamp" => shared_function!(flink_datetime::flink_unix_timestamp),
        "Flink_UnixTimestampNow" => shared_function!(flink_datetime::flink_unix_timestamp_now),
        _ => df_unimplemented_err!("auron ext function not implemented: {name}")?,
    })
}

/// Wraps a `ScalarUDFImpl` adapter as an `Arc<ScalarUDF>`.
fn adapted<T: datafusion::logical_expr::ScalarUDFImpl>(impl_: T) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(impl_))
}

/// Creates the UDF for an auron ext function, preferring a
/// `datafusion-spark` implementation where one exists.
///
/// `arg_types` / `return_type` are the plan-level types from the protobuf
/// node; adapters use them to preserve Spark's types where DataFusion's
/// logical type coercion is bypassed.
pub fn create_auron_ext_udf(
    name: &str,
    spark_partition_id: usize,
    arg_types: Vec<datafusion::arrow::datatypes::DataType>,
    return_type: datafusion::arrow::datatypes::DataType,
) -> Result<Arc<ScalarUDF>> {
    use adapters::*;
    use datafusion_spark::function as spark_fn;

    let ret = return_type.clone();

    // Keep Spark-specific ROUND, ANSI next_day, add_months overflow and
    // map null-key validation and Java-regex str_to_map in their existing implementations.
    // The remaining functions can delegate without changing their calling convention.
    let direct: Option<Arc<ScalarUDF>> = match name {
        "Spark_StringSpace" => Some(spark_fn::string::space()),
        "Spark_Substring" => Some(spark_fn::string::substring()),
        "Spark_StringConcat" => Some(spark_fn::string::concat()),
        "Spark_StringConcatWs" => Some(spark_fn::string::concat_ws()),
        "Spark_DateAdd" => Some(spark_fn::datetime::date_add()),
        "Spark_DateSub" => Some(spark_fn::datetime::date_sub()),
        "Spark_LastDay" => Some(spark_fn::datetime::last_day()),
        "Spark_XxHash64" => Some(spark_fn::hash::xxhash64()),
        _ => None,
    };
    if let Some(udf) = direct {
        return Ok(udf);
    }

    // --- datafusion-spark behind a thin calling-convention adapter ---
    let adapted: Option<Arc<ScalarUDF>> = match name {
        "Spark_MakeArray" => Some(adapted(MakeArrayUdf::new(ret))),
        "Spark_Sha224" => Some(adapted(Sha2BitsUdf::new(224, ret))),
        "Spark_Sha256" => Some(adapted(Sha2BitsUdf::new(256, ret))),
        "Spark_Sha384" => Some(adapted(Sha2BitsUdf::new(384, ret))),
        "Spark_Sha512" => Some(adapted(Sha2BitsUdf::new(512, ret))),
        // session timezone arrives as a trailing literal argument
        "Spark_Hour" => Some(adapted(TzExtractUdf::new(
            spark_fn::datetime::hour(),
            "Spark_Hour",
            ret,
        ))),
        "Spark_Minute" => Some(adapted(TzExtractUdf::new(
            spark_fn::datetime::minute(),
            "Spark_Minute",
            ret,
        ))),
        "Spark_Second" => Some(adapted(TzExtractUdf::new(
            spark_fn::datetime::second(),
            "Spark_Second",
            ret,
        ))),
        "Spark_Year" => Some(adapted(DatePartTzUdf::new(
            "year",
            false,
            "Spark_Year",
            ret,
        ))),
        "Spark_Month" => Some(adapted(DatePartTzUdf::new(
            "month",
            false,
            "Spark_Month",
            ret,
        ))),
        "Spark_Day" => Some(adapted(DatePartTzUdf::new("day", false, "Spark_Day", ret))),
        "Spark_DayOfYear" => Some(adapted(DatePartTzUdf::new(
            "doy",
            false,
            "Spark_DayOfYear",
            ret,
        ))),
        // Spark's dayofweek is 1..=7 (Sun=1); core `dow` is 0..=6
        "Spark_DayOfWeek" => Some(adapted(DatePartTzUdf::new(
            "dow",
            true,
            "Spark_DayOfWeek",
            ret,
        ))),
        "Spark_WeekOfYear" => Some(adapted(DatePartTzUdf::new(
            "week",
            false,
            "Spark_WeekOfYear",
            ret,
        ))),
        "Spark_Quarter" => Some(adapted(DatePartTzUdf::new(
            "quarter",
            false,
            "Spark_Quarter",
            ret,
        ))),
        _ => None,
    };
    if let Some(udf) = adapted {
        return Ok(udf);
    }

    // --- legacy ext implementation (no datafusion-spark counterpart) ---
    let fun = create_auron_ext_function(name, spark_partition_id)?;
    Ok(Arc::new(datafusion::prelude::create_udf(
        &format!("spark_ext_function_{name}"),
        arg_types,
        return_type,
        Volatility::Volatile,
        fun,
    )))
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::{
            array::{Decimal128Array, Int64Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        },
        common::{ScalarValue, config::ConfigOptions},
        logical_expr::{ColumnarValue, Operator, ScalarFunctionArgs, Volatility},
        physical_expr::{
            PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr,
            expressions::{
                BinaryExpr, CaseExpr, Column, IsNotNullExpr, IsNullExpr, Literal, TryCastExpr,
            },
        },
        prelude::create_udf,
    };

    use super::*;

    #[test]
    fn case_nullability_does_not_evaluate_aggregate_placeholders() -> Result<()> {
        let udf = Arc::new(create_udf(
            "spark_ext_function_Placeholder",
            vec![],
            DataType::Int64,
            Volatility::Volatile,
            create_auron_ext_function("Placeholder", 0)?,
        ));
        let placeholder: PhysicalExprRef = Arc::new(ScalarFunctionExpr::new(
            udf.name(),
            udf.clone(),
            vec![],
            Arc::new(Field::new("placeholder", DataType::Int64, true)),
            Arc::new(ConfigOptions::default()),
        ));
        for nested in [false, true] {
            let mut predicate: PhysicalExprRef = Arc::new(IsNullExpr::new(placeholder.clone()));
            if nested {
                predicate = Arc::new(BinaryExpr::new(
                    Arc::new(Literal::new(ScalarValue::Boolean(Some(false)))),
                    Operator::Or,
                    predicate,
                ));
            }
            let case = CaseExpr::try_new(
                None,
                vec![(predicate, Arc::new(Literal::new(ScalarValue::Int64(None))))],
                Some(Arc::new(Literal::new(ScalarValue::Int64(Some(1))))),
            )?;
            assert!(case.nullable(&Schema::empty())?);
        }
        Ok(())
    }

    #[test]
    fn case_nullability_preserves_volatile_predicates_and_results() -> Result<()> {
        use std::sync::atomic::{AtomicUsize, Ordering};

        for shared_result in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let udf_calls = calls.clone();
            let udf = Arc::new(create_udf(
                "alternating_nullable",
                vec![],
                DataType::Int64,
                Volatility::Volatile,
                Arc::new(move |_| {
                    let first = udf_calls.fetch_add(1, Ordering::SeqCst) == 0;
                    Ok(ColumnarValue::Scalar(ScalarValue::Int64(
                        first.then_some(1),
                    )))
                }),
            ));
            let value: PhysicalExprRef = Arc::new(ScalarFunctionExpr::new(
                udf.name(),
                udf.clone(),
                vec![],
                Arc::new(Field::new("value", DataType::Int64, true)),
                Arc::new(ConfigOptions::default()),
            ));
            let (predicate, result): (PhysicalExprRef, PhysicalExprRef) = if shared_result {
                (Arc::new(IsNotNullExpr::new(value.clone())), value)
            } else {
                (
                    Arc::new(IsNullExpr::new(value)),
                    Arc::new(Literal::new(ScalarValue::Int64(None))),
                )
            };
            let case = CaseExpr::try_new(
                None,
                vec![(predicate, result)],
                Some(Arc::new(Literal::new(ScalarValue::Int64(Some(0))))),
            )?;
            assert!(case.nullable(&Schema::empty())?);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            if shared_result {
                let batch = RecordBatch::try_new_with_options(
                    Arc::new(Schema::empty()),
                    vec![],
                    &arrow::array::RecordBatchOptions::new().with_row_count(Some(1)),
                )?;
                let result = case.evaluate(&batch)?.into_array(1)?;
                assert_eq!(result.null_count(), 1);
                assert_eq!(calls.load(Ordering::SeqCst), 2);
            }
        }
        Ok(())
    }

    #[test]
    fn case_nullability_still_infers_non_null_guarded_columns() -> Result<()> {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, true)]);
        let column: PhysicalExprRef = Arc::new(Column::new("value", 0));
        let case = CaseExpr::try_new(
            None,
            vec![(Arc::new(IsNotNullExpr::new(column.clone())), column)],
            Some(Arc::new(Literal::new(ScalarValue::Int64(Some(0))))),
        )?;
        assert!(!case.nullable(&schema)?);
        Ok(())
    }

    #[test]
    fn pad_array_lengths_preserve_spark_semantics() -> Result<()> {
        let strings = StringArray::from(vec![
            Some("hi"),
            Some("hi"),
            Some("hi"),
            Some("hi"),
            Some("hi"),
            Some("hi"),
            None,
            Some("é"),
        ]);
        let lengths = Int64Array::from(vec![
            Some(-1),
            Some(i64::MIN),
            Some(0),
            Some(4),
            Some(2),
            None,
            Some(i64::MAX),
            Some(3),
        ]);
        for udf in [
            datafusion::functions::unicode::lpad(),
            datafusion::functions::unicode::rpad(),
        ] {
            for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                for explicit_fill in [false, true] {
                    let mut args = vec![
                        ColumnarValue::Array(arrow::compute::cast(&strings, &data_type)?),
                        ColumnarValue::Array(Arc::new(lengths.clone())),
                    ];
                    if explicit_fill {
                        let fill = StringArray::from(vec![
                            Some("_"),
                            Some("_"),
                            Some("_"),
                            Some("_"),
                            None,
                            Some("_"),
                            Some("_"),
                            Some("_"),
                        ]);
                        args.push(ColumnarValue::Array(arrow::compute::cast(
                            &fill, &data_type,
                        )?));
                    }
                    let arg_types = args
                        .iter()
                        .map(ColumnarValue::data_type)
                        .collect::<Vec<_>>();
                    let return_type = udf.return_type(&arg_types)?;
                    let result = udf
                        .invoke_with_args(ScalarFunctionArgs {
                            args,
                            arg_fields: arg_types
                                .into_iter()
                                .map(|t| Arc::new(Field::new("arg", t, true)))
                                .collect(),
                            number_rows: 8,
                            return_field: Arc::new(Field::new("result", return_type.clone(), true)),
                            config_options: Arc::new(ConfigOptions::default()),
                        })?
                        .into_array(8)?;
                    let pad = if explicit_fill { "__" } else { "  " };
                    let (padded, unicode) = if udf.name() == "lpad" {
                        (format!("{pad}hi"), format!("{pad}é"))
                    } else {
                        (format!("hi{pad}"), format!("é{pad}"))
                    };
                    let expected = StringArray::from(vec![
                        Some(""),
                        Some(""),
                        Some(""),
                        Some(padded.as_str()),
                        if explicit_fill { None } else { Some("hi") },
                        None,
                        None,
                        Some(unicode.as_str()),
                    ]);
                    let expected = arrow::compute::cast(&expected, &return_type)?;
                    assert_eq!(
                        result.as_ref(),
                        expected.as_ref(),
                        "{} {data_type:?} fill={explicit_fill}",
                        udf.name()
                    );
                }
            }
        }
        Ok(())
    }

    #[test]
    fn round_decimal_case_return_type() -> Result<()> {
        let input_type = DataType::Decimal128(28, 8);
        let return_type = DataType::Decimal128(29, 4);
        let schema = Arc::new(Schema::new(vec![
            Field::new("play_cnt", DataType::Int64, true),
            Field::new("ratio", input_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(1)])),
                Arc::new(
                    Decimal128Array::from(vec![None, Some(123_456_789), None])
                        .with_precision_and_scale(28, 8)?,
                ),
            ],
        )?;
        let udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(
            spark_round::SparkRound::new(false),
        ));
        let round: PhysicalExprRef = Arc::new(ScalarFunctionExpr::new(
            udf.name(),
            udf.clone(),
            vec![
                Arc::new(Column::new("ratio", 1)),
                Arc::new(Literal::new(ScalarValue::Int32(Some(4)))),
            ],
            Arc::new(Field::new("result", return_type.clone(), true)),
            Arc::new(ConfigOptions::default()),
        ));
        let rounded = round.evaluate(&batch)?.into_array(batch.num_rows())?;
        assert_eq!(round.data_type(&schema)?, return_type);
        let expected_round = Decimal128Array::from(vec![None, Some(12_346), None])
            .with_precision_and_scale(29, 4)?;
        assert_eq!(rounded.as_ref(), &expected_round);
        let predicate: PhysicalExprRef = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("play_cnt", 0)),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::Int64(Some(0)))),
        ));
        let zero: PhysicalExprRef = Arc::new(TryCastExpr::new(
            Arc::new(Literal::new(ScalarValue::Decimal128(Some(0), 5, 4))),
            return_type.clone(),
        ));
        let case = CaseExpr::try_new(None, vec![(predicate, zero)], Some(round))?;
        assert_eq!(case.data_type(&schema)?, return_type);
        let result = case.evaluate(&batch)?.into_array(batch.num_rows())?;
        let expected = Decimal128Array::from(vec![Some(0), Some(12_346), None])
            .with_precision_and_scale(29, 4)?;
        assert_eq!(result.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn round_decimal_scalar_and_array_return_type() -> Result<()> {
        for (value, scale, precision, output_scale, expected) in [
            (Some(123_456_789), Some(4), 29, 4, Some(12_346)),
            (Some(-123_456_789), Some(4), 29, 4, Some(-12_346)),
            (Some(999_950_000), Some(3), 29, 3, Some(10_000)),
            (Some(12_500_000_000), Some(-1), 22, 0, Some(130)),
            (Some(123_456_789), Some(10), 29, 8, Some(123_456_789)),
            (None, Some(4), 29, 4, None),
            (Some(123_456_789), None, 29, 4, None),
        ] {
            let input_type = DataType::Decimal128(28, 8);
            let return_type = DataType::Decimal128(precision, output_scale);
            let udf = Arc::new(datafusion::logical_expr::ScalarUDF::from(
                spark_round::SparkRound::new(false),
            ));
            let scalar = ScalarValue::Decimal128(value, 28, 8);
            for input in [
                ColumnarValue::Scalar(scalar.clone()),
                ColumnarValue::Array(scalar.to_array_of_size(2)?),
            ] {
                let scalar_input = matches!(input, ColumnarValue::Scalar(_));
                let result = udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![input, ColumnarValue::Scalar(ScalarValue::Int32(scale))],
                    arg_fields: vec![
                        Arc::new(Field::new("value", input_type.clone(), true)),
                        Arc::new(Field::new("scale", DataType::Int32, true)),
                    ],
                    number_rows: 2,
                    return_field: Arc::new(Field::new("result", return_type.clone(), true)),
                    config_options: Arc::new(ConfigOptions::default()),
                })?;
                assert_eq!(result.data_type(), return_type);
                if scalar_input {
                    assert!(matches!(result, ColumnarValue::Scalar(_)));
                }
                let expected = ScalarValue::Decimal128(expected, precision, output_scale)
                    .to_array_of_size(2)?;
                assert_eq!(result.into_array(2)?.as_ref(), expected.as_ref());
            }
        }
        Ok(())
    }

    #[test]
    fn spark_power_special_values_and_scalar_paths() -> Result<()> {
        use arrow::{array::AsArray, datatypes::Float64Type};
        let udf = datafusion_spark::function::math::pow();
        for (base, exponent, expected) in [
            (Some(1.0), Some(f64::NAN), Some(f64::NAN)),
            (Some(1.0), Some(f64::INFINITY), Some(f64::NAN)),
            (Some(-1.0), Some(f64::NEG_INFINITY), Some(f64::NAN)),
            (Some(f64::NAN), Some(0.0), Some(1.0)),
            (Some(-0.0), Some(-3.0), Some(f64::NEG_INFINITY)),
            (Some(-0.0), Some(3.0), Some(-0.0)),
            (Some(-0.0), Some(-2.5), Some(f64::INFINITY)),
            (Some(-2.0), Some(0.5), Some(f64::NAN)),
            (None, Some(0.0), None),
            (Some(1.0), None, None),
        ] {
            for array_base in [false, true] {
                for array_exponent in [false, true] {
                    let args = [(base, array_base), (exponent, array_exponent)]
                        .into_iter()
                        .map(|(v, array)| {
                            let scalar = ScalarValue::Float64(v);
                            if array {
                                Ok(ColumnarValue::Array(scalar.to_array_of_size(2)?))
                            } else {
                                Ok(ColumnarValue::Scalar(scalar))
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let arg_field = Arc::new(Field::new("arg", DataType::Float64, true));
                    let result = udf.invoke_with_args(ScalarFunctionArgs {
                        args,
                        arg_fields: vec![arg_field; 2],
                        number_rows: 2,
                        return_field: Arc::new(Field::new("result", DataType::Float64, true)),
                        config_options: Arc::new(ConfigOptions::default()),
                    })?;
                    assert_eq!(
                        matches!(result, ColumnarValue::Scalar(_)),
                        !array_base && !array_exponent
                    );
                    let array = result.into_array(2)?;
                    for actual in array.as_primitive::<Float64Type>() {
                        match (actual, expected) {
                            (Some(a), Some(e)) if e.is_nan() => assert!(a.is_nan()),
                            _ => assert_eq!(actual.map(f64::to_bits), expected.map(f64::to_bits)),
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn reuses_function_implementation() -> Result<()> {
        let first = create_auron_ext_function("Spark_GetJsonObject", 0)?;
        let second = create_auron_ext_function("Spark_GetJsonObject", 0)?;
        let other = create_auron_ext_function("Spark_ParseJson", 0)?;

        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &other));

        let first_udf = create_udf(
            "spark_ext_function_Spark_GetJsonObject",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
            Volatility::Volatile,
            first,
        );
        let second_udf = create_udf(
            "spark_ext_function_Spark_GetJsonObject",
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
            Volatility::Volatile,
            second,
        );
        assert_eq!(first_udf, second_udf);
        Ok(())
    }
}
