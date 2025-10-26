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
    array::{ArrayRef, Int32Array, TimestampMillisecondArray},
    compute::{DatePart, date_part},
    datatypes::{DataType, TimeUnit},
};
use chrono::{TimeZone, Utc, prelude::*};
use chrono_tz::Tz;
use datafusion::{
    common::{Result, ScalarValue},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::arrow::cast::cast;

// ---- date parts on Date32 via Arrow's date_part
// -----------------------------------------------

pub fn spark_year(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Year)?))
}

pub fn spark_month(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Month)?))
}

pub fn spark_day(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
    Ok(ColumnarValue::Array(date_part(&input, DatePart::Day)?))
}

/// `spark_quarter(date/timestamp/compatible-string)`
///
/// Simulates Spark's `quarter()` function.
/// Converts the input to `Date32`, extracts the month (1–12),
/// and computes the quarter as `((month - 1) / 3) + 1`.
/// Null values are propagated transparently.
pub fn spark_quarter(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    // Cast input to Date32 for compatibility with date_part()
    let input = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;

    // Extract month (1–12) using Arrow's date_part
    let month_arr: ArrayRef = date_part(&input, DatePart::Month)?;
    let month_arr = month_arr
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("date_part(Month) must return Int32Array");

    // Compute quarter: ((month - 1) / 3) + 1, preserving NULLs
    let quarter = Int32Array::from_iter(
        month_arr
            .iter()
            .map(|opt_m| opt_m.map(|m| ((m - 1) / 3 + 1) as i32)),
    );

    Ok(ColumnarValue::Array(Arc::new(quarter)))
}

// ---- timezone handling (custom, Spark-like)
// ---------------------------------------------------

/// Parse optional timezone (2nd argument) into `Option<Tz>`.
fn parse_tz(args: &[ColumnarValue]) -> Option<Tz> {
    if args.len() < 2 {
        return None;
    }
    match &args[1] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.parse::<Tz>().ok(),
        _ => None,
    }
}

/// Return the UTC offset in **seconds** for `epoch_ms` at the given `tz`
/// (DST-aware).
fn offset_seconds_at(tz: Tz, epoch_ms: i64) -> i32 {
    // Convert epoch_ms to UTC DateTime, then ask the tz for local offset.
    let dt_utc = Utc.timestamp_millis_opt(epoch_ms).single();
    match dt_utc {
        Some(dt) => tz
            .offset_from_utc_datetime(&dt.naive_utc())
            .fix()
            .local_minus_utc(),
        None => 0, // Gracefully return 0 on invalid inputs to avoid panic.
    }
}

/// Extract hour/minute/second from a `TimestampMillisecondArray` with optional
/// timezone. `which`: "hour" | "minute" | "second"
fn extract_hms_with_tz(
    ts: &TimestampMillisecondArray,
    tz_opt: Option<Tz>,
    which: &str,
) -> Int32Array {
    const MS_PER_SEC: i64 = 1000;
    const MS_PER_MIN: i64 = 60 * MS_PER_SEC;
    const MS_PER_HOUR: i64 = 60 * MS_PER_MIN;
    const MS_PER_DAY: i64 = 24 * MS_PER_HOUR;

    Int32Array::from_iter(ts.iter().map(|opt_ms| {
        opt_ms.map(|epoch_ms| {
            // Localize by applying tz offset in seconds (if provided).
            let local_ms = if let Some(tz) = tz_opt {
                let off_sec = offset_seconds_at(tz, epoch_ms) as i64;
                epoch_ms + off_sec * MS_PER_SEC
            } else {
                epoch_ms // Treat as UTC when tz is None.
            };

            // Milliseconds within the day with positive modulo.
            let mut day_ms = local_ms % MS_PER_DAY;
            if day_ms < 0 {
                day_ms += MS_PER_DAY;
            }

            match which {
                "hour" => (day_ms / MS_PER_HOUR) as i32,
                "minute" => ((day_ms % MS_PER_HOUR) / MS_PER_MIN) as i32,
                "second" => ((day_ms % MS_PER_MIN) / MS_PER_SEC) as i32,
                _ => unreachable!("which must be one of: hour | minute | second"),
            }
        })
    }))
}

// ---- Spark-like hour/minute/second built on custom TZ logic
// -----------------------------------

/// Extract the HOUR component. We first cast any input to
/// `Timestamp(Millisecond, None)` (to get the physical milliseconds) and then
/// apply our own timezone/DST logic.
pub fn spark_hour(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr_ts_ms_none = cast(
        &args[0].clone().into_array(1)?,
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;

    let ts = arr_ts_ms_none
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("internal cast to Timestamp(Millisecond, None) must succeed");

    let tz = parse_tz(args);
    Ok(ColumnarValue::Array(Arc::new(extract_hms_with_tz(
        ts, tz, "hour",
    ))))
}

/// Extract the MINUTE component (same approach as `spark_hour`).
pub fn spark_minute(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr_ts_ms_none = cast(
        &args[0].clone().into_array(1)?,
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;

    let ts = arr_ts_ms_none
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("internal cast to Timestamp(Millisecond, None) must succeed");

    let tz = parse_tz(args);
    Ok(ColumnarValue::Array(Arc::new(extract_hms_with_tz(
        ts, tz, "minute",
    ))))
}

/// Extract the SECOND component (same approach as `spark_hour`).
pub fn spark_second(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let arr_ts_ms_none = cast(
        &args[0].clone().into_array(1)?,
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;

    let ts = arr_ts_ms_none
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("internal cast to Timestamp(Millisecond, None) must succeed");

    let tz = parse_tz(args);
    Ok(ColumnarValue::Array(Arc::new(extract_hms_with_tz(
        ts, tz, "second",
    ))))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Date32Array, Int32Array, TimestampMillisecondArray};

    use super::*;

    #[test]
    fn test_spark_year() {
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(1000),
            Some(2000),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1970),
            Some(1972),
            Some(1975),
            None,
        ]));
        assert_eq!(
            &spark_year(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }

    #[test]
    fn test_spark_month() {
        let input = Arc::new(Date32Array::from(vec![Some(0), Some(35), Some(65), None]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None]));
        assert_eq!(
            &spark_month(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }

    #[test]
    fn test_spark_day() {
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(10),
            Some(20),
            Some(30),
            Some(40),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(11),
            Some(21),
            Some(31),
            Some(10),
            None,
        ]));
        assert_eq!(
            &spark_day(&args).unwrap().into_array(1).unwrap(),
            &expected_ret
        );
    }

    #[test]
    fn test_spark_quarter_basic() {
        // Date32 days relative to 1970-01-01:
        //  0   -> 1970-01-01 (Q1)
        //  40  -> ~1970-02-10 (Q1)
        // 100  -> ~1970-04-11 (Q2)
        // 200  -> ~1970-07-20 (Q3)
        // 300  -> ~1970-10-28 (Q4)
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(40),
            Some(100),
            Some(200),
            Some(300),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            None,
        ]));

        let out = spark_quarter(&args).unwrap().into_array(1).unwrap();
        assert_eq!(&out, &expected);
    }

    #[test]
    fn test_spark_quarter_null_only() {
        // Ensure NULL propagation
        let input = Arc::new(Date32Array::from(vec![None, None]));
        let args = vec![ColumnarValue::Array(input)];
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));

        let out = spark_quarter(&args).unwrap().into_array(1).unwrap();
        assert_eq!(&out, &expected);
    }

    #[inline]
    fn arc_tz(s: &str) -> Option<Arc<str>> {
        Some(Arc::<str>::from(s))
    }

    #[inline]
    fn ms(h: i64, m: i64, s: i64) -> i64 {
        (h * 3600 + m * 60 + s) * 1000
    }

    /// Build ms since epoch helper
    fn hms_to_ms(h: i64, m: i64, s: i64) -> i64 {
        (h * 3600 + m * 60 + s) * 1000
    }

    #[test]
    fn test_spark_hour_minute_second_basic_from_ts() -> Result<()> {
        // 0ms -> 1970-01-01 00:00:00 UTC
        // 5025000ms -> 1970-01-01 01:23:45 UTC
        let ts = Arc::new(TimestampMillisecondArray::from(vec![
            Some(0),
            Some(hms_to_ms(1, 23, 45)),
            None,
        ]));

        let args = vec![ColumnarValue::Array(ts.clone())];

        // hour()
        let expected_h: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(1), None]));
        let out_h = spark_hour(&args)?.into_array(1)?;
        assert_eq!(&out_h, &expected_h);

        // minute()
        let expected_m: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(23), None]));
        let out_m = spark_minute(&args)?.into_array(1)?;
        assert_eq!(&out_m, &expected_m);

        // second()
        let expected_s: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(45), None]));
        let out_s = spark_second(&args)?.into_array(1)?;
        assert_eq!(&out_s, &expected_s);

        Ok(())
    }

    #[test]
    fn test_spark_timeparts_from_date32_midnight() -> Result<()> {
        let d = Arc::new(Date32Array::from(vec![Some(0), Some(1), None]));
        let args = vec![ColumnarValue::Array(d)];

        // hour(date) -> 0
        let expected_h: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(0), None]));
        let out_h = spark_hour(&args)?.into_array(1)?;
        assert_eq!(&out_h, &expected_h);

        // minute(date) -> 0
        let expected_m: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(0), None]));
        let out_m = spark_minute(&args)?.into_array(1)?;
        assert_eq!(&out_m, &expected_m);

        // second(date) -> 0
        let expected_s: ArrayRef = Arc::new(Int32Array::from(vec![Some(0), Some(0), None]));
        let out_s = spark_second(&args)?.into_array(1)?;
        assert_eq!(&out_s, &expected_s);

        Ok(())
    }

    #[test]
    fn test_spark_timeparts_scalar_vs_array_consistency() -> Result<()> {
        // 1970-01-01 12:34:56 UTC
        let ms = hms_to_ms(12, 34, 56);

        // Scalar
        let out_h_scalar = spark_hour(&[ColumnarValue::Scalar(
            datafusion::common::ScalarValue::TimestampMillisecond(Some(ms), None),
        )])?
        .into_array(1)?;
        let out_m_scalar = spark_minute(&[ColumnarValue::Scalar(
            datafusion::common::ScalarValue::TimestampMillisecond(Some(ms), None),
        )])?
        .into_array(1)?;
        let out_s_scalar = spark_second(&[ColumnarValue::Scalar(
            datafusion::common::ScalarValue::TimestampMillisecond(Some(ms), None),
        )])?
        .into_array(1)?;

        // Array
        let arr = Arc::new(TimestampMillisecondArray::from(vec![Some(ms)]));
        let out_h_array = spark_hour(&[ColumnarValue::Array(arr.clone())])?.into_array(1)?;
        let out_m_array = spark_minute(&[ColumnarValue::Array(arr.clone())])?.into_array(1)?;
        let out_s_array = spark_second(&[ColumnarValue::Array(arr)])?.into_array(1)?;

        assert_eq!(&out_h_scalar, &out_h_array);
        assert_eq!(&out_m_scalar, &out_m_array);
        assert_eq!(&out_s_scalar, &out_s_array);

        Ok(())
    }

    #[test]
    fn test_spark_timeparts_pre_epoch_negative_ms() -> Result<()> {
        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(-1000)]));
        let args = vec![ColumnarValue::Array(ts)];

        let expected_h: ArrayRef = Arc::new(Int32Array::from(vec![Some(23)]));
        let expected_m: ArrayRef = Arc::new(Int32Array::from(vec![Some(59)]));
        let expected_s: ArrayRef = Arc::new(Int32Array::from(vec![Some(59)]));

        let out_h = spark_hour(&args)?.into_array(1)?;
        let out_m = spark_minute(&args)?.into_array(1)?;
        let out_s = spark_second(&args)?.into_array(1)?;

        assert_eq!(&out_h, &expected_h);
        assert_eq!(&out_m, &expected_m);
        assert_eq!(&out_s, &expected_s);

        Ok(())
    }

    #[test]
    fn test_spark_timeparts_null_only() -> Result<()> {
        let ts = Arc::new(TimestampMillisecondArray::from(vec![None, None]));
        let args = vec![ColumnarValue::Array(ts)];

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));

        let out_h = spark_hour(&args)?.into_array(1)?;
        let out_m = spark_minute(&args)?.into_array(1)?;
        let out_s = spark_second(&args)?.into_array(1)?;

        assert_eq!(&out_h, &expected);
        assert_eq!(&out_m, &expected);
        assert_eq!(&out_s, &expected);

        Ok(())
    }

    #[test]
    fn test_hour_utc_vs_shanghai() -> Result<()> {
        // 1970-01-01 00:00:00 UTC
        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(0)]));

        // default (None) -> UTC
        let out_utc = spark_hour(&[ColumnarValue::Array(ts.clone())])?.into_array(1)?;
        let expected_utc: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        assert_eq!(&out_utc, &expected_utc);

        let out_cst = spark_hour(&[
            ColumnarValue::Array(ts.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Asia/Shanghai".to_string()))),
        ])?
        .into_array(1)?;
        let expected_cst: ArrayRef = Arc::new(Int32Array::from(vec![Some(8)]));
        assert_eq!(&out_cst, &expected_cst);

        Ok(())
    }

    #[test]
    fn test_hour_scalar_vs_array_and_explicit_utc() -> Result<()> {
        // 1970-01-01 12:34:56 UTC
        let ts_ms = ms(12, 34, 56);

        let out_scalar = spark_hour(&[ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(
            Some(ts_ms),
            arc_tz("UTC"),
        ))])?
        .into_array(1)?;

        // Array + default (None = UTC)
        let arr = Arc::new(TimestampMillisecondArray::from(vec![Some(ts_ms)]));
        let out_array = spark_hour(&[ColumnarValue::Array(arr)])?.into_array(1)?;

        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(12)]));
        assert_eq!(&out_scalar, &expected);
        assert_eq!(&out_array, &expected);
        Ok(())
    }

    /// Helper: build epoch ms from a UTC calendar time.
    fn utc_ms(y: i32, mo: u32, d: u32, h: u32, m: u32, s: u32) -> i64 {
        // chrono 0.4: with_ymd_and_hms(...).single() for strictness
        Utc.with_ymd_and_hms(y, mo, d, h, m, s)
            .single()
            .expect("valid UTC datetime")
            .timestamp_millis()
    }

    #[test]
    fn test_minute_second_utc_vs_shanghai_and_kolkata() -> Result<()> {
        // epoch 0 -> 1970-01-01 00:00:00 UTC
        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(0)]));

        // Default (None) -> UTC
        let out_min_utc = spark_minute(&[ColumnarValue::Array(ts.clone())])?.into_array(1)?;
        let out_sec_utc = spark_second(&[ColumnarValue::Array(ts.clone())])?.into_array(1)?;

        let expected_min_utc: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        let expected_sec_utc: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        assert_eq!(&out_min_utc, &expected_min_utc);
        assert_eq!(&out_sec_utc, &expected_sec_utc);

        // Asia/Shanghai (+08:00) -> 08:00:00 local => minute=0, second=0
        let out_min_cst = spark_minute(&[
            ColumnarValue::Array(ts.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Asia/Shanghai".to_string()))),
        ])?
        .into_array(1)?;
        let out_sec_cst = spark_second(&[
            ColumnarValue::Array(ts.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Asia/Shanghai".to_string()))),
        ])?
        .into_array(1)?;
        let expected_min_cst: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        let expected_sec_cst: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        assert_eq!(&out_min_cst, &expected_min_cst);
        assert_eq!(&out_sec_cst, &expected_sec_cst);

        // Asia/Kolkata (+05:30) -> 05:30:00 local => minute=30, second=0
        let out_min_kol = spark_minute(&[
            ColumnarValue::Array(ts.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Asia/Kolkata".to_string()))),
        ])?
        .into_array(1)?;
        let out_sec_kol = spark_second(&[
            ColumnarValue::Array(ts),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Asia/Kolkata".to_string()))),
        ])?
        .into_array(1)?;
        let expected_min_kol: ArrayRef = Arc::new(Int32Array::from(vec![Some(30)]));
        let expected_sec_kol: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        assert_eq!(&out_min_kol, &expected_min_kol);
        assert_eq!(&out_sec_kol, &expected_sec_kol);

        Ok(())
    }

    #[test]
    fn test_minute_second_nonwhole_offset_kathmandu() -> Result<()> {
        // 1970-01-01 00:00:00 UTC -> Asia/Kathmandu was +05:30
        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(0)]));

        let out_min = spark_minute(&[
            ColumnarValue::Array(ts.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Asia/Kathmandu".to_string()))),
        ])?
        .into_array(1)?;
        let out_sec = spark_second(&[
            ColumnarValue::Array(ts),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Asia/Kathmandu".to_string()))),
        ])?
        .into_array(1)?;

        let expected_min: ArrayRef = Arc::new(Int32Array::from(vec![Some(30)]));
        let expected_sec: ArrayRef = Arc::new(Int32Array::from(vec![Some(0)]));
        assert_eq!(&out_min, &expected_min);
        assert_eq!(&out_sec, &expected_sec);
        Ok(())
    }

    #[test]
    fn test_minute_second_dst_spring_forward_newyork() -> Result<()> {
        // America/New_York DST starts on 2019-03-10.
        // Local time jumps from 01:59:59 to 03:00:00 (02:00:00 - 02:59:59 does not
        // exist).

        // 2019-03-10 06:59:59 UTC -> 01:59:59 local (EST, UTC-5)
        let t1 = utc_ms(2019, 3, 10, 6, 59, 59);
        // 2019-03-10 07:00:00 UTC -> 03:00:00 local (EDT, UTC-4)
        let t2 = utc_ms(2019, 3, 10, 7, 0, 0);

        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(t1), Some(t2)]));

        let out_min = spark_minute(&[
            ColumnarValue::Array(ts.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".to_string()))),
        ])?
        .into_array(2)?;
        let out_sec = spark_second(&[
            ColumnarValue::Array(ts.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".to_string()))),
        ])?
        .into_array(2)?;

        let expected_min: ArrayRef = Arc::new(Int32Array::from(vec![Some(59), Some(0)]));
        let expected_sec: ArrayRef = Arc::new(Int32Array::from(vec![Some(59), Some(0)]));

        assert_eq!(&out_min, &expected_min);
        assert_eq!(&out_sec, &expected_sec);

        Ok(())
    }
}
