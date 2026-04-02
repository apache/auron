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
    array::{
        ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, TimestampMillisecondArray,
    },
    compute::{DatePart, date_part},
    datatypes::{DataType, TimeUnit},
};
use chrono::{Duration, LocalResult, NaiveDate, TimeZone, Utc, prelude::*};
use chrono_tz::Tz;
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::arrow::cast::cast;

/// Spark `weekofyear()`: ISO week number (Monday-based, week 1 has >3 days).
/// For timestamps, localizes to the given timezone before computing the week.
/// Defaults to UTC when no timezone is provided.
pub fn spark_weekofyear(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let array = args[0].clone().into_array(1)?;

    let default_tz = chrono_tz::UTC;
    let tz: Tz = if args.len() > 1 {
        match &args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
            | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => {
                s.parse::<Tz>().map_err(|_| {
                    DataFusionError::Execution(format!("spark_weekofyear invalid timezone: {s}"))
                })?
            }
            _ => default_tz,
        }
    } else {
        default_tz
    };

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let ts_arr = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("internal cast to TimestampMillisecondArray must succeed");

            let weekofyear = Int32Array::from_iter(ts_arr.iter().map(|opt_ms| {
                opt_ms.and_then(|ms| {
                    tz.timestamp_millis_opt(ms)
                        .single()
                        .map(|dt| dt.date_naive().iso_week().week() as i32)
                })
            }));

            Ok(ColumnarValue::Array(Arc::new(weekofyear)))
        }
        _ => {
            let input = cast(&array, &DataType::Date32)?;
            let input = input
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("internal cast to Date32 must succeed");

            let epoch =
                NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 must be a valid date");
            let weekofyear = Int32Array::from_iter(input.iter().map(|opt_days| {
                opt_days.and_then(|days| {
                    epoch
                        .checked_add_signed(Duration::days(days as i64))
                        .map(|date| date.iso_week().week() as i32)
                })
            }));

            Ok(ColumnarValue::Array(Arc::new(weekofyear)))
        }
    }
}

fn parse_tz(args: &[ColumnarValue]) -> Option<Tz> {
    parse_tz_value(args.get(1))
}

fn parse_tz_value(arg: Option<&ColumnarValue>) -> Option<Tz> {
    match arg {
        Some(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))) => s.parse::<Tz>().ok(),
        _ => None,
    }
}

fn local_datetime(epoch_ms: i64, tz_opt: Option<Tz>) -> Option<NaiveDateTime> {
    let dt_utc = Utc.timestamp_millis_opt(epoch_ms).single()?;
    Some(match tz_opt {
        Some(tz) => dt_utc.with_timezone(&tz).naive_local(),
        None => dt_utc.naive_utc(),
    })
}

fn start_of_local_day_ms(local_date: NaiveDate, tz_opt: Option<Tz>) -> Option<i64> {
    let local_midnight = local_date.and_hms_opt(0, 0, 0)?;

    match tz_opt {
        Some(tz) => match tz.from_local_datetime(&local_midnight) {
            LocalResult::Single(dt) => Some(dt.with_timezone(&Utc).timestamp_millis()),
            LocalResult::Ambiguous(dt1, dt2) => {
                Some(dt1.min(dt2).with_timezone(&Utc).timestamp_millis())
            }
            LocalResult::None => {
                for minute in 1..=(24 * 60) {
                    let candidate = local_midnight + chrono::Duration::minutes(minute);
                    match tz.from_local_datetime(&candidate) {
                        LocalResult::Single(dt) => {
                            return Some(dt.with_timezone(&Utc).timestamp_millis());
                        }
                        LocalResult::Ambiguous(dt1, dt2) => {
                            return Some(dt1.min(dt2).with_timezone(&Utc).timestamp_millis());
                        }
                        LocalResult::None => continue,
                    }
                }
                None
            }
        },
        None => Some(local_midnight.and_utc().timestamp_millis()),
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            let leap_year = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap_year { 29 } else { 28 }
        }
        _ => unreachable!("month must be in 1..=12"),
    }
}

fn round_to_8_digits(value: f64) -> f64 {
    const SCALE: f64 = 1.0e8;
    ((value * SCALE) + 0.5).floor() / SCALE
}

fn months_between_value(
    timestamp1_ms: i64,
    timestamp2_ms: i64,
    round_off: bool,
    tz_opt: Option<Tz>,
) -> Option<f64> {
    const SECONDS_PER_DAY: i64 = 86_400;
    const SECONDS_PER_MONTH: i64 = 31 * SECONDS_PER_DAY;

    let local_dt1 = local_datetime(timestamp1_ms, tz_opt)?;
    let local_dt2 = local_datetime(timestamp2_ms, tz_opt)?;
    let date1 = local_dt1.date();
    let date2 = local_dt2.date();

    let months1 = date1.year() * 12 + date1.month() as i32;
    let months2 = date2.year() * 12 + date2.month() as i32;
    let month_diff = (months1 - months2) as f64;

    let day1 = date1.day();
    let day2 = date2.day();
    let days_to_month_end1 = days_in_month(date1.year(), date1.month()) - day1;
    let days_to_month_end2 = days_in_month(date2.year(), date2.month()) - day2;

    if day1 == day2 || (days_to_month_end1 == 0 && days_to_month_end2 == 0) {
        return Some(month_diff);
    }

    let start_of_day1_ms = start_of_local_day_ms(date1, tz_opt)?;
    let start_of_day2_ms = start_of_local_day_ms(date2, tz_opt)?;
    let seconds_in_day1 = (timestamp1_ms - start_of_day1_ms) / 1000;
    let seconds_in_day2 = (timestamp2_ms - start_of_day2_ms) / 1000;
    let seconds_diff =
        (day1 as i64 - day2 as i64) * SECONDS_PER_DAY + seconds_in_day1 - seconds_in_day2;

    let result = month_diff + seconds_diff as f64 / SECONDS_PER_MONTH as f64;
    Some(if round_off {
        round_to_8_digits(result)
    } else {
        result
    })
}

/// DST-aware UTC offset in seconds for a given instant and timezone.
fn offset_seconds_at(tz: Tz, epoch_ms: i64) -> i32 {
    let dt_utc = Utc.timestamp_millis_opt(epoch_ms).single();
    match dt_utc {
        Some(dt) => tz
            .offset_from_utc_datetime(&dt.naive_utc())
            .fix()
            .local_minus_utc(),
        None => 0,
    }
}

/// Convert timestamp millis to local-timezone Date32 (days since epoch).
fn ts_ms_to_local_date32(ts: &TimestampMillisecondArray, tz: Tz) -> Date32Array {
    const MS_PER_SEC: i64 = 1000;
    const MS_PER_DAY: i64 = 86_400_000;

    Date32Array::from_iter(ts.iter().map(|opt_ms| {
        opt_ms.map(|epoch_ms| {
            let local_ms = epoch_ms + offset_seconds_at(tz, epoch_ms) as i64 * MS_PER_SEC;
            let local_days = if local_ms >= 0 {
                local_ms / MS_PER_DAY
            } else {
                (local_ms - MS_PER_DAY + 1) / MS_PER_DAY
            };
            local_days as i32
        })
    }))
}

/// Resolve input to a Date32Array, applying timezone adjustment for timestamps.
fn resolve_local_date32(args: &[ColumnarValue]) -> Result<Date32Array> {
    match parse_tz(args) {
        Some(tz) => {
            let arr = cast(
                &args[0].clone().into_array(1)?,
                &DataType::Timestamp(TimeUnit::Millisecond, None),
            )?;
            let ts = arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("cast to Timestamp(Millisecond, None) must succeed");
            Ok(ts_ms_to_local_date32(ts, tz))
        }
        None => {
            let arr = cast(&args[0].clone().into_array(1)?, &DataType::Date32)?;
            Ok(arr
                .as_any()
                .downcast_ref::<Date32Array>()
                .expect("cast to Date32 must succeed")
                .clone())
        }
    }
}

pub fn spark_year(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let local = resolve_local_date32(args)?;
    Ok(ColumnarValue::Array(date_part(
        &(Arc::new(local) as ArrayRef),
        DatePart::Year,
    )?))
}

pub fn spark_month(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let local = resolve_local_date32(args)?;
    Ok(ColumnarValue::Array(date_part(
        &(Arc::new(local) as ArrayRef),
        DatePart::Month,
    )?))
}

pub fn spark_day(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let local = resolve_local_date32(args)?;
    Ok(ColumnarValue::Array(date_part(
        &(Arc::new(local) as ArrayRef),
        DatePart::Day,
    )?))
}

/// Spark `dayofweek()`: Sunday = 1, Monday = 2, ..., Saturday = 7.
pub fn spark_dayofweek(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let input = resolve_local_date32(args)?;

    // Date32 days since epoch; epoch (1970-01-01) is Thursday (index 4).
    // (days + 4) mod 7 → 0=Sun..6=Sat; Spark wants 1=Sun..7=Sat.
    let dayofweek = Int32Array::from_iter(input.iter().map(|opt_days| {
        opt_days.map(|days| {
            let weekday_index = (days as i64 + 4).rem_euclid(7);
            weekday_index as i32 + 1
        })
    }));

    Ok(ColumnarValue::Array(Arc::new(dayofweek)))
}

pub fn spark_quarter(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let local = resolve_local_date32(args)?;
    let month_arr: ArrayRef = date_part(&(Arc::new(local) as ArrayRef), DatePart::Month)?;
    let month_arr = month_arr
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("date_part(Month) must return Int32Array");
    let quarter = Int32Array::from_iter(
        month_arr
            .iter()
            .map(|opt_m| opt_m.map(|m| ((m - 1) / 3 + 1))),
    );

    Ok(ColumnarValue::Array(Arc::new(quarter)))
}

/// Extract hour/minute/second from a `TimestampMillisecondArray` with optional
/// timezone.
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
            let local_ms = if let Some(tz) = tz_opt {
                let off_sec = offset_seconds_at(tz, epoch_ms) as i64;
                epoch_ms + off_sec * MS_PER_SEC
            } else {
                epoch_ms
            };

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

/// Compute Spark-compatible `months_between(timestamp1, timestamp2, roundOff)`.
///
/// The first two arguments are timestamps in physical UTC milliseconds and the
/// fourth argument is an optional session timezone string used for local date
/// boundary calculations.
pub fn spark_months_between(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 4 {
        return Err(DataFusionError::Execution(
            "spark_months_between() requires four arguments".to_string(),
        ));
    }

    let num_rows = args
        .iter()
        .find_map(|arg| match arg {
            ColumnarValue::Array(arr) => Some(arr.len()),
            ColumnarValue::Scalar(_) => None,
        })
        .unwrap_or(1);

    let timestamp1 = cast(
        &args[0].clone().into_array(num_rows)?,
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    let timestamp2 = cast(
        &args[1].clone().into_array(num_rows)?,
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )?;
    let round_off = cast(&args[2].clone().into_array(num_rows)?, &DataType::Boolean)?;

    let timestamp1 = timestamp1
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("internal cast to Timestamp(Millisecond, None) must succeed");
    let timestamp2 = timestamp2
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("internal cast to Timestamp(Millisecond, None) must succeed");
    let round_off = round_off
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("internal cast to Boolean must succeed");
    let tz = parse_tz_value(args.get(3));

    let result = Float64Array::from_iter(
        timestamp1
            .iter()
            .zip(timestamp2.iter())
            .zip(round_off.iter())
            .map(|((timestamp1_ms, timestamp2_ms), round_off)| {
                match (timestamp1_ms, timestamp2_ms, round_off) {
                    (Some(timestamp1_ms), Some(timestamp2_ms), Some(round_off)) => {
                        months_between_value(timestamp1_ms, timestamp2_ms, round_off, tz)
                    }
                    _ => None,
                }
            }),
    );

    Ok(ColumnarValue::Array(Arc::new(result)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, Date32Array, Float64Array, Int32Array, TimestampMillisecondArray,
    };

    use super::*;

    #[test]
    fn test_spark_year() -> Result<()> {
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
        assert_eq!(&spark_year(&args)?.into_array(1)?, &expected_ret);
        Ok(())
    }

    #[test]
    fn test_spark_month() -> Result<()> {
        let input = Arc::new(Date32Array::from(vec![Some(0), Some(35), Some(65), None]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None]));
        assert_eq!(&spark_month(&args)?.into_array(1)?, &expected_ret);
        Ok(())
    }

    #[test]
    fn test_spark_day() -> Result<()> {
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
        assert_eq!(&spark_day(&args)?.into_array(1)?, &expected_ret);
        Ok(())
    }

    #[test]
    fn test_spark_dayofweek() -> Result<()> {
        let input = Arc::new(Date32Array::from(vec![
            Some(-1),
            Some(0),
            Some(2),
            Some(3),
            Some(4),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(4),
            Some(5),
            Some(7),
            Some(1),
            Some(2),
            None,
        ]));
        assert_eq!(&spark_dayofweek(&args)?.into_array(1)?, &expected_ret);
        Ok(())
    }

    #[test]
    fn test_spark_weekofyear() -> Result<()> {
        let input = Arc::new(Date32Array::from(vec![
            Some(0),
            Some(4017),
            Some(16801),
            Some(17167),
            Some(14455),
            None,
        ]));
        let args = vec![ColumnarValue::Array(input)];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            Some(53),
            Some(52),
            Some(31),
            None,
        ]));
        assert_eq!(&spark_weekofyear(&args)?.into_array(1)?, &expected_ret);
        Ok(())
    }

    #[test]
    fn test_spark_weekofyear_with_timezone() -> Result<()> {
        // In America/New_York:
        // 2021-01-04 04:30:00 UTC -> 2021-01-03 23:30:00 local -> ISO week 53
        // 2021-01-04 05:30:00 UTC -> 2021-01-04 00:30:00 local -> ISO week 1
        let input = Arc::new(TimestampMillisecondArray::from(vec![
            Some(utc_ms(2021, 1, 4, 4, 30, 0)),
            Some(utc_ms(2021, 1, 4, 5, 30, 0)),
            None,
        ]));
        let args = vec![
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".to_string()))),
        ];
        let expected_ret: ArrayRef = Arc::new(Int32Array::from(vec![Some(53), Some(1), None]));
        assert_eq!(&spark_weekofyear(&args)?.into_array(3)?, &expected_ret);
        Ok(())
    }

    #[test]
    fn test_spark_weekofyear_invalid_timezone() {
        let input = Arc::new(TimestampMillisecondArray::from(vec![Some(utc_ms(
            2021, 1, 4, 5, 30, 0,
        ))]));
        let args = vec![
            ColumnarValue::Array(input),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("Mars/Olympus".to_string()))),
        ];

        let err =
            spark_weekofyear(&args).expect_err("spark_weekofyear should fail for invalid timezone");
        assert!(err.to_string().contains("invalid timezone"));
    }

    #[test]
    fn test_spark_quarter_basic() -> Result<()> {
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

        let out = spark_quarter(&args)?.into_array(1)?;
        assert_eq!(&out, &expected);
        Ok(())
    }

    #[test]
    fn test_spark_quarter_null_only() -> Result<()> {
        // Ensure NULL propagation
        let input = Arc::new(Date32Array::from(vec![None, None]));
        let args = vec![ColumnarValue::Array(input)];
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));

        let out = spark_quarter(&args)?.into_array(1)?;
        assert_eq!(&out, &expected);
        Ok(())
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

    fn months_between_args(
        timestamp1_ms: Option<i64>,
        timestamp2_ms: Option<i64>,
        round_off: Option<bool>,
        timezone: Option<&str>,
    ) -> [ColumnarValue; 4] {
        [
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(timestamp1_ms, None)),
            ColumnarValue::Scalar(ScalarValue::TimestampMillisecond(timestamp2_ms, None)),
            ColumnarValue::Scalar(ScalarValue::Boolean(round_off)),
            ColumnarValue::Scalar(ScalarValue::Utf8(timezone.map(str::to_string))),
        ]
    }

    #[test]
    fn test_spark_months_between_ignores_time_for_same_day() -> Result<()> {
        let out = spark_months_between(&months_between_args(
            Some(utc_ms(2024, 3, 15, 23, 59, 59)),
            Some(utc_ms(2024, 1, 15, 0, 0, 0)),
            Some(true),
            Some("UTC"),
        ))?
        .into_array(1)?;

        let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(2.0)]));
        assert_eq!(&out, &expected);
        Ok(())
    }

    #[test]
    fn test_spark_months_between_ignores_time_for_last_day_of_month() -> Result<()> {
        let out = spark_months_between(&months_between_args(
            Some(utc_ms(2024, 2, 29, 12, 0, 0)),
            Some(utc_ms(2024, 1, 31, 0, 0, 0)),
            Some(true),
            Some("UTC"),
        ))?
        .into_array(1)?;

        let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(1.0)]));
        assert_eq!(&out, &expected);
        Ok(())
    }

    #[test]
    fn test_spark_months_between_fractional_rounding() -> Result<()> {
        let rounded = spark_months_between(&months_between_args(
            Some(utc_ms(2024, 3, 2, 12, 0, 0)),
            Some(utc_ms(2024, 1, 1, 0, 0, 0)),
            Some(true),
            Some("UTC"),
        ))?
        .into_array(1)?;
        let expected_rounded: ArrayRef = Arc::new(Float64Array::from(vec![Some(2.0483871)]));
        assert_eq!(&rounded, &expected_rounded);

        let unrounded = spark_months_between(&months_between_args(
            Some(utc_ms(2024, 3, 2, 12, 0, 0)),
            Some(utc_ms(2024, 1, 1, 0, 0, 0)),
            Some(false),
            Some("UTC"),
        ))?
        .into_array(1)?;
        let unrounded = unrounded
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("months_between should return Float64Array");
        assert!((unrounded.value(0) - 2.0483870967741935).abs() < 1e-12);
        Ok(())
    }

    #[test]
    fn test_spark_months_between_respects_dst_gap_in_session_timezone() -> Result<()> {
        let out = spark_months_between(&months_between_args(
            // 2024-03-10 07:30:00 UTC -> 2024-03-10 03:30:00 local in America/New_York.
            // The 02:00-02:59 hour does not exist on this day due to spring-forward DST.
            Some(utc_ms(2024, 3, 10, 7, 30, 0)),
            // 2024-02-09 06:30:00 UTC -> 2024-02-09 01:30:00 local.
            Some(utc_ms(2024, 2, 9, 6, 30, 0)),
            Some(false),
            Some("America/New_York"),
        ))?
        .into_array(1)?;

        let out = out
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("months_between should return Float64Array");
        assert!((out.value(0) - 1.0336021505376345).abs() < 1e-12);
        Ok(())
    }

    #[test]
    fn test_spark_months_between_negative_when_timestamp1_is_earlier() -> Result<()> {
        let out = spark_months_between(&months_between_args(
            Some(utc_ms(2024, 1, 15, 0, 0, 0)),
            Some(utc_ms(2024, 3, 15, 23, 59, 59)),
            Some(true),
            Some("UTC"),
        ))?
        .into_array(1)?;

        let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(-2.0)]));
        assert_eq!(&out, &expected);
        Ok(())
    }

    #[test]
    fn test_spark_months_between_fractional_rounding_keeps_negative_sign() -> Result<()> {
        let out = spark_months_between(&months_between_args(
            Some(utc_ms(2024, 1, 1, 0, 0, 0)),
            Some(utc_ms(2024, 3, 2, 12, 0, 0)),
            Some(true),
            Some("UTC"),
        ))?
        .into_array(1)?;

        let expected: ArrayRef = Arc::new(Float64Array::from(vec![Some(-2.0483871)]));
        assert_eq!(&out, &expected);
        Ok(())
    }

    #[test]
    fn test_spark_months_between_null_propagation() -> Result<()> {
        let out = spark_months_between(&months_between_args(
            None,
            Some(utc_ms(2024, 1, 1, 0, 0, 0)),
            Some(true),
            Some("UTC"),
        ))?
        .into_array(1)?;

        let expected: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        assert_eq!(&out, &expected);
        Ok(())
    }

    #[test]
    fn test_year_month_day_with_tz_new_york() -> Result<()> {
        // 04:30 UTC on Jan 4 is 23:30 on Jan 3 in New York
        let epoch = utc_ms(2021, 1, 4, 4, 30, 0);
        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(epoch)]));
        let tz = ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".to_string())));

        let out_year =
            spark_year(&[ColumnarValue::Array(ts.clone()), tz.clone()])?.into_array(1)?;
        let out_month =
            spark_month(&[ColumnarValue::Array(ts.clone()), tz.clone()])?.into_array(1)?;
        let out_day = spark_day(&[ColumnarValue::Array(ts.clone()), tz.clone()])?.into_array(1)?;

        let expected_year: ArrayRef = Arc::new(Int32Array::from(vec![Some(2021)]));
        let expected_month: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let expected_day: ArrayRef = Arc::new(Int32Array::from(vec![Some(3)]));

        assert_eq!(&out_year, &expected_year);
        assert_eq!(&out_month, &expected_month);
        assert_eq!(&out_day, &expected_day);
        Ok(())
    }

    #[test]
    fn test_dayofweek_with_tz_new_york() -> Result<()> {
        // Same instant as above; Jan 3 2021 is a Sunday (=1 in Spark)
        let epoch = utc_ms(2021, 1, 4, 4, 30, 0);
        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(epoch)]));
        let tz = ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".to_string())));

        let out = spark_dayofweek(&[ColumnarValue::Array(ts), tz])?.into_array(1)?;
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        assert_eq!(&out, &expected);
        Ok(())
    }

    #[test]
    fn test_quarter_with_tz_boundary() -> Result<()> {
        // 03:00 UTC on Apr 1 is 23:00 on Mar 31 in New York → Q1, not Q2
        let epoch = utc_ms(2021, 4, 1, 3, 0, 0);
        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(epoch)]));
        let tz = ColumnarValue::Scalar(ScalarValue::Utf8(Some("America/New_York".to_string())));

        let out = spark_quarter(&[ColumnarValue::Array(ts), tz])?.into_array(1)?;
        let expected: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        assert_eq!(&out, &expected);
        Ok(())
    }

    #[test]
    fn test_date_parts_with_shanghai() -> Result<()> {
        // 17:00 UTC on Dec 31 is 01:00 on Jan 1 in Shanghai → crosses year boundary
        let epoch = utc_ms(2021, 12, 31, 17, 0, 0);
        let ts = Arc::new(TimestampMillisecondArray::from(vec![Some(epoch)]));
        let tz = ColumnarValue::Scalar(ScalarValue::Utf8(Some("Asia/Shanghai".to_string())));

        let out_year =
            spark_year(&[ColumnarValue::Array(ts.clone()), tz.clone()])?.into_array(1)?;
        let out_month =
            spark_month(&[ColumnarValue::Array(ts.clone()), tz.clone()])?.into_array(1)?;
        let out_day = spark_day(&[ColumnarValue::Array(ts.clone()), tz.clone()])?.into_array(1)?;
        let out_quarter =
            spark_quarter(&[ColumnarValue::Array(ts.clone()), tz.clone()])?.into_array(1)?;
        let out_dow = spark_dayofweek(&[ColumnarValue::Array(ts), tz])?.into_array(1)?;

        let expected_year: ArrayRef = Arc::new(Int32Array::from(vec![Some(2022)]));
        let expected_month: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let expected_day: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let expected_quarter: ArrayRef = Arc::new(Int32Array::from(vec![Some(1)]));
        let expected_dow: ArrayRef = Arc::new(Int32Array::from(vec![Some(7)])); // Saturday

        assert_eq!(&out_year, &expected_year);
        assert_eq!(&out_month, &expected_month);
        assert_eq!(&out_day, &expected_day);
        assert_eq!(&out_quarter, &expected_quarter);
        assert_eq!(&out_dow, &expected_dow);
        Ok(())
    }

    #[test]
    fn test_date_parts_null_tz_unchanged() -> Result<()> {
        let input = Arc::new(Date32Array::from(vec![Some(0), Some(100), None]));
        let null_tz = ColumnarValue::Scalar(ScalarValue::Utf8(None));

        let out_year =
            spark_year(&[ColumnarValue::Array(input.clone()), null_tz.clone()])?.into_array(1)?;
        let out_no_tz = spark_year(&[ColumnarValue::Array(input.clone())])?.into_array(1)?;
        assert_eq!(&out_year, &out_no_tz);

        let out_month =
            spark_month(&[ColumnarValue::Array(input.clone()), null_tz.clone()])?.into_array(1)?;
        let out_no_tz = spark_month(&[ColumnarValue::Array(input.clone())])?.into_array(1)?;
        assert_eq!(&out_month, &out_no_tz);

        let out_day =
            spark_day(&[ColumnarValue::Array(input.clone()), null_tz.clone()])?.into_array(1)?;
        let out_no_tz = spark_day(&[ColumnarValue::Array(input)])?.into_array(1)?;
        assert_eq!(&out_day, &out_no_tz);

        Ok(())
    }
}
