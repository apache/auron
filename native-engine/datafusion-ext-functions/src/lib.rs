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

use datafusion::{common::Result, logical_expr::ScalarFunctionImplementation};
use datafusion_ext_commons::df_unimplemented_err;

mod brickhouse;
mod flink_datetime;
mod spark_array;
mod spark_bround;
mod spark_check_overflow;
mod spark_crypto;
mod spark_dates;
pub mod spark_get_json_object;
mod spark_hash;
mod spark_initcap;
mod spark_isnan;
mod spark_make_array;
mod spark_make_decimal;
mod spark_map;
mod spark_normalize_nan_and_zero;
mod spark_null_if;
mod spark_round;
mod spark_strings;
mod spark_unscaled_value;

macro_rules! shared_function {
    ($function:expr) => {{
        static INSTANCE: OnceLock<ScalarFunctionImplementation> = OnceLock::new();
        Arc::clone(INSTANCE.get_or_init(|| Arc::new($function)))
    }};
}

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
        "Spark_XxHash64" => shared_function!(spark_hash::spark_xxhash64),
        "Spark_Sha224" => shared_function!(spark_crypto::spark_sha224),
        "Spark_Sha256" => shared_function!(spark_crypto::spark_sha256),
        "Spark_Sha384" => shared_function!(spark_crypto::spark_sha384),
        "Spark_Sha512" => shared_function!(spark_crypto::spark_sha512),
        "Spark_MD5" => shared_function!(spark_crypto::spark_md5),
        "Spark_GetJsonObject" => shared_function!(spark_get_json_object::spark_get_json_object),
        "Spark_GetParsedJsonObject" => {
            shared_function!(spark_get_json_object::spark_get_parsed_json_object)
        }
        "Spark_ParseJson" => shared_function!(spark_get_json_object::spark_parse_json),
        "Spark_ArrayReverse" => shared_function!(spark_array::array_reverse),
        "Spark_ArrayFlatten" => shared_function!(spark_array::array_flatten),
        "Spark_MakeArray" => shared_function!(spark_make_array::array),
        "Spark_MapConcat" => shared_function!(spark_map::map_concat),
        "Spark_MapFromArrays" => shared_function!(spark_map::map_from_arrays),
        "Spark_MapFromEntries" => shared_function!(spark_map::map_from_entries),
        "Spark_StrToMap" => shared_function!(spark_map::str_to_map),
        "Spark_StringSpace" => shared_function!(spark_strings::string_space),
        "Spark_StringRepeat" => shared_function!(spark_strings::string_repeat),
        "Spark_StringSplit" => shared_function!(spark_strings::string_split),
        "Spark_StringConcat" => shared_function!(spark_strings::string_concat),
        "Spark_StringConcatWs" => shared_function!(spark_strings::string_concat_ws),
        "Spark_StringLower" => shared_function!(spark_strings::string_lower),
        "Spark_StringUpper" => shared_function!(spark_strings::string_upper),
        "Spark_Substring" => shared_function!(spark_strings::spark_substring),
        "Spark_InitCap" => shared_function!(spark_initcap::string_initcap),
        "Spark_Year" => shared_function!(spark_dates::spark_year),
        "Spark_Month" => shared_function!(spark_dates::spark_month),
        "Spark_Day" => shared_function!(spark_dates::spark_day),
        "Spark_DayOfWeek" => shared_function!(spark_dates::spark_dayofweek),
        "Spark_WeekOfYear" => shared_function!(spark_dates::spark_weekofyear),
        "Spark_Quarter" => shared_function!(spark_dates::spark_quarter),
        "Spark_MakeDate" => shared_function!(spark_dates::spark_make_date),
        "Spark_Hour" => shared_function!(spark_dates::spark_hour),
        "Spark_Minute" => shared_function!(spark_dates::spark_minute),
        "Spark_Second" => shared_function!(spark_dates::spark_second),
        "Spark_MonthsBetween" => shared_function!(spark_dates::spark_months_between),
        "Spark_BrickhouseArrayUnion" => shared_function!(brickhouse::array_union::array_union),
        "Spark_Round" => shared_function!(spark_round::spark_round),
        "Spark_BRound" => shared_function!(spark_bround::spark_bround),
        "Spark_NormalizeNanAndZero" => {
            shared_function!(spark_normalize_nan_and_zero::spark_normalize_nan_and_zero)
        }
        "Spark_IsNaN" => shared_function!(spark_isnan::spark_isnan),
        "Flink_UnixTimestamp" => shared_function!(flink_datetime::flink_unix_timestamp),
        "Flink_UnixTimestampNow" => shared_function!(flink_datetime::flink_unix_timestamp_now),
        _ => df_unimplemented_err!("auron ext function not implemented: {name}")?,
    })
}

#[cfg(test)]
mod tests {
    use datafusion::{arrow::datatypes::DataType, logical_expr::Volatility, prelude::create_udf};

    use super::*;

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
