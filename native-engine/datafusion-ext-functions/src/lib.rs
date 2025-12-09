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

use datafusion::{common::Result, logical_expr::ScalarFunctionImplementation};
use datafusion_ext_commons::df_unimplemented_err;

mod brickhouse;
mod spark_check_overflow;
mod spark_crypto;
mod spark_dates;
pub mod spark_get_json_object;
mod spark_hash;
mod spark_initcap;
mod spark_isnan;
mod spark_make_array;
mod spark_make_decimal;
mod spark_normalize_nan_and_zero;
mod spark_null_if;
mod spark_round;
mod spark_strings;
mod spark_unscaled_value;

pub fn create_auron_ext_function(name: &str) -> Result<ScalarFunctionImplementation> {
    // auron ext functions, if used for spark should be start with 'Spark_',
    // if used for flink should be start with 'Flink_',
    // same to other engines.
    Ok(match name {
        "Placeholder" => Arc::new(|_| panic!("placeholder() should never be called")),
        "Spark_NullIf" => Arc::new(spark_null_if::spark_null_if),
        "Spark_NullIfZero" => Arc::new(spark_null_if::spark_null_if_zero),
        "Spark_UnscaledValue" => Arc::new(spark_unscaled_value::spark_unscaled_value),
        "Spark_MakeDecimal" => Arc::new(spark_make_decimal::spark_make_decimal),
        "Spark_CheckOverflow" => Arc::new(spark_check_overflow::spark_check_overflow),
        "Spark_Murmur3Hash" => Arc::new(spark_hash::spark_murmur3_hash),
        "Spark_XxHash64" => Arc::new(spark_hash::spark_xxhash64),
        "Spark_Sha224" => Arc::new(spark_crypto::spark_sha224),
        "Spark_Sha256" => Arc::new(spark_crypto::spark_sha256),
        "Spark_Sha384" => Arc::new(spark_crypto::spark_sha384),
        "Spark_Sha512" => Arc::new(spark_crypto::spark_sha512),
        "Spark_MD5" => Arc::new(spark_crypto::spark_md5),
        "Spark_GetJsonObject" => Arc::new(spark_get_json_object::spark_get_json_object),
        "Spark_GetParsedJsonObject" => {
            Arc::new(spark_get_json_object::spark_get_parsed_json_object)
        }
        "Spark_ParseJson" => Arc::new(spark_get_json_object::spark_parse_json),
        "Spark_MakeArray" => Arc::new(spark_make_array::array),
        "Spark_StringSpace" => Arc::new(spark_strings::string_space),
        "Spark_StringRepeat" => Arc::new(spark_strings::string_repeat),
        "Spark_StringSplit" => Arc::new(spark_strings::string_split),
        "Spark_StringConcat" => Arc::new(spark_strings::string_concat),
        "Spark_StringConcatWs" => Arc::new(spark_strings::string_concat_ws),
        "Spark_StringLower" => Arc::new(spark_strings::string_lower),
        "Spark_StringUpper" => Arc::new(spark_strings::string_upper),
        "Spark_InitCap" => Arc::new(spark_initcap::string_initcap),
        "Spark_Year" => Arc::new(spark_dates::spark_year),
        "Spark_Month" => Arc::new(spark_dates::spark_month),
        "Spark_Day" => Arc::new(spark_dates::spark_day),
        "Spark_Quarter" => Arc::new(spark_dates::spark_quarter),
        "Spark_Hour" => Arc::new(spark_dates::spark_hour),
        "Spark_Minute" => Arc::new(spark_dates::spark_minute),
        "Spark_Second" => Arc::new(spark_dates::spark_second),
        "Spark_BrickhouseArrayUnion" => Arc::new(brickhouse::array_union::array_union),
        "Spark_Round" => Arc::new(spark_round::spark_round),
        "Spark_NormalizeNanAndZero" => {
            Arc::new(spark_normalize_nan_and_zero::spark_normalize_nan_and_zero)
        }
        "Spark_IsNaN" => Arc::new(spark_isnan::spark_isnan),
        _ => df_unimplemented_err!("spark ext function not implemented: {name}")?,
    })
}
