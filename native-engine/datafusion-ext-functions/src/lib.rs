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

pub fn create_auron_ext_function(
    name: &str,
    spark_partition_id: usize,
) -> Result<ScalarFunctionImplementation> {
    macro_rules! cache {
        ($func:path) => {{
            static CELL: OnceLock<ScalarFunctionImplementation> = OnceLock::new();
            CELL.get_or_init(|| Arc::new($func)).clone()
        }};
    }
    // auron ext functions, if used for spark should be start with 'Spark_',
    // if used for flink should be start with 'Flink_',
    // same to other engines.
    Ok(match name {
        "Placeholder" => Arc::new(|_| panic!("placeholder() should never be called")),
        "Spark_NullIf" => cache!(spark_null_if::spark_null_if),
        "Spark_NullIfZero" => cache!(spark_null_if::spark_null_if_zero),
        "Spark_UnscaledValue" => cache!(spark_unscaled_value::spark_unscaled_value),
        "Spark_MakeDecimal" => cache!(spark_make_decimal::spark_make_decimal),
        "Spark_CheckOverflow" => cache!(spark_check_overflow::spark_check_overflow),
        "Spark_Murmur3Hash" => cache!(spark_hash::spark_murmur3_hash),
        "Spark_XxHash64" => cache!(spark_hash::spark_xxhash64),
        "Spark_Sha224" => cache!(spark_crypto::spark_sha224),
        "Spark_Sha256" => cache!(spark_crypto::spark_sha256),
        "Spark_Sha384" => cache!(spark_crypto::spark_sha384),
        "Spark_Sha512" => cache!(spark_crypto::spark_sha512),
        "Spark_MD5" => cache!(spark_crypto::spark_md5),
        "Spark_GetJsonObject" => cache!(spark_get_json_object::spark_get_json_object),
        "Spark_GetParsedJsonObject" => {
            cache!(spark_get_json_object::spark_get_parsed_json_object)
        }
        "Spark_ParseJson" => cache!(spark_get_json_object::spark_parse_json),
        "Spark_MakeArray" => cache!(spark_make_array::array),
        "Spark_StringSpace" => cache!(spark_strings::string_space),
        "Spark_StringRepeat" => cache!(spark_strings::string_repeat),
        "Spark_StringSplit" => cache!(spark_strings::string_split),
        "Spark_StringConcat" => cache!(spark_strings::string_concat),
        "Spark_StringConcatWs" => cache!(spark_strings::string_concat_ws),
        "Spark_StringLower" => cache!(spark_strings::string_lower),
        "Spark_StringUpper" => cache!(spark_strings::string_upper),
        "Spark_InitCap" => cache!(spark_initcap::string_initcap),
        "Spark_Year" => cache!(spark_dates::spark_year),
        "Spark_Month" => cache!(spark_dates::spark_month),
        "Spark_Day" => cache!(spark_dates::spark_day),
        "Spark_Quarter" => cache!(spark_dates::spark_quarter),
        "Spark_Hour" => cache!(spark_dates::spark_hour),
        "Spark_Minute" => cache!(spark_dates::spark_minute),
        "Spark_Second" => cache!(spark_dates::spark_second),
        "Spark_BrickhouseArrayUnion" => cache!(brickhouse::array_union::array_union),
        "Spark_Round" => cache!(spark_round::spark_round),
        "Spark_NormalizeNanAndZero" => {
            cache!(spark_normalize_nan_and_zero::spark_normalize_nan_and_zero)
        }
        "Spark_IsNaN" => cache!(spark_isnan::spark_isnan),
        _ => df_unimplemented_err!("spark ext function not implemented: {name}")?,
    })
}
