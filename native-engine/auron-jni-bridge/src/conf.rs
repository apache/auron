// Copyright 2022 The Auron Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion::common::Result;

use crate::{ensure_jni_bridge_inited, jni_call_static, jni_get_string, jni_new_string};

macro_rules! define_conf {
    ($conftype:ty, $name:ident) => {
        #[allow(non_camel_case_types)]
        pub struct $name;
        impl $conftype for $name {
            fn key(&self) -> &'static str {
                stringify!($name)
            }
        }
    };
}

define_conf!(IntConf, BATCH_SIZE);
define_conf!(DoubleConf, MEMORY_FRACTION);
define_conf!(DoubleConf, PROCESS_MEMORY_FRACTION);
define_conf!(BooleanConf, SMJ_INEQUALITY_JOIN_ENABLE);
define_conf!(BooleanConf, CASE_CONVERT_FUNCTIONS_ENABLE);
define_conf!(BooleanConf, INPUT_BATCH_STATISTICS_ENABLE);
define_conf!(BooleanConf, IGNORE_CORRUPTED_FILES);
define_conf!(BooleanConf, PARTIAL_AGG_SKIPPING_ENABLE);
define_conf!(DoubleConf, PARTIAL_AGG_SKIPPING_RATIO);
define_conf!(IntConf, PARTIAL_AGG_SKIPPING_MIN_ROWS);
define_conf!(BooleanConf, PARTIAL_AGG_SKIPPING_SKIP_SPILL);
define_conf!(BooleanConf, PARQUET_ENABLE_PAGE_FILTERING);
define_conf!(BooleanConf, PARQUET_ENABLE_BLOOM_FILTER);
define_conf!(IntConf, PARQUET_MAX_OVER_READ_SIZE);
define_conf!(IntConf, PARQUET_METADATA_CACHE_SIZE);
define_conf!(StringConf, SPARK_IO_COMPRESSION_CODEC);
define_conf!(IntConf, SPARK_IO_COMPRESSION_ZSTD_LEVEL);
define_conf!(IntConf, TOKIO_WORKER_THREADS_PER_CPU);
define_conf!(IntConf, SPARK_TASK_CPUS);
define_conf!(IntConf, SHUFFLE_COMPRESSION_TARGET_BUF_SIZE);
define_conf!(StringConf, SPILL_COMPRESSION_CODEC);
define_conf!(BooleanConf, SMJ_FALLBACK_ENABLE);
define_conf!(IntConf, SMJ_FALLBACK_ROWS_THRESHOLD);
define_conf!(IntConf, SMJ_FALLBACK_MEM_SIZE_THRESHOLD);
define_conf!(IntConf, SUGGESTED_BATCH_MEM_SIZE);
define_conf!(IntConf, SUGGESTED_BATCH_MEM_SIZE_KWAY_MERGE);
define_conf!(BooleanConf, ORC_FORCE_POSITIONAL_EVOLUTION);
define_conf!(IntConf, UDAF_FALLBACK_NUM_UDAFS_TRIGGER_SORT_AGG);
define_conf!(BooleanConf, PARSE_JSON_ERROR_FALLBACK);
define_conf!(StringConf, NATIVE_LOG_LEVEL);

pub trait BooleanConf {
    fn key(&self) -> &'static str;
    fn value(&self) -> Result<bool> {
        ensure_jni_bridge_inited()?;
        let key = jni_new_string!(self.key())?;
        jni_call_static!(AuronConf.booleanConf(key.as_obj()) -> bool)
    }
}

pub trait IntConf {
    fn key(&self) -> &'static str;
    fn value(&self) -> Result<i32> {
        ensure_jni_bridge_inited()?;
        let key = jni_new_string!(self.key())?;
        jni_call_static!(AuronConf.intConf(key.as_obj()) -> i32)
    }
}

pub trait LongConf {
    fn key(&self) -> &'static str;
    fn value(&self) -> Result<i64> {
        ensure_jni_bridge_inited()?;
        let key = jni_new_string!(self.key())?;
        jni_call_static!(AuronConf.longConf(key.as_obj()) -> i64)
    }
}

pub trait DoubleConf {
    fn key(&self) -> &'static str;
    fn value(&self) -> Result<f64> {
        ensure_jni_bridge_inited()?;
        let key = jni_new_string!(self.key())?;
        jni_call_static!(AuronConf.doubleConf(key.as_obj()) -> f64)
    }
}

pub trait StringConf {
    fn key(&self) -> &'static str;
    fn value(&self) -> Result<String> {
        ensure_jni_bridge_inited()?;
        let key = jni_new_string!(self.key())?;
        let value = jni_get_string!(
            jni_call_static!(AuronConf.stringConf(key.as_obj()) -> JObject)?
                .as_obj()
                .into()
        )?;
        Ok(value)
    }
}
