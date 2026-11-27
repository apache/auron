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

//! Spark `date_trunc(format, timestamp)` (`TruncTimestamp`).

use std::sync::Arc;

use arrow::{
    array::{Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, TimeUnit},
};
use chrono::{
    DateTime, Datelike, Duration, FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset,
    TimeZone, Timelike,
};
use chrono_tz::Tz;
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::arrow::cast::cast;

const MICROS_PER_MILLIS: i64 = 1_000;
const MICROS_PER_SECOND: i64 = 1_000_000;

/// Truncation levels of Spark's `DateTimeUtils.parseTruncLevel`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TruncLevel {
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

fn parse_trunc_level(format: &str) -> Option<TruncLevel> {
    Some(match format.to_uppercase().as_str() {
        "MICROSECOND" => TruncLevel::Microsecond,
        "MILLISECOND" => TruncLevel::Millisecond,
        "SECOND" => TruncLevel::Second,
        "MINUTE" => TruncLevel::Minute,
        "HOUR" => TruncLevel::Hour,
        "DAY" | "DD" => TruncLevel::Day,
        "WEEK" => TruncLevel::Week,
        "MON" | "MONTH" | "MM" => TruncLevel::Month,
        "QUARTER" => TruncLevel::Quarter,
        "YEAR" | "YYYY" | "YY" => TruncLevel::Year,
        _ => return None,
    })
}

/// Parses a Java `ZoneId` id: a region id (e.g. `Asia/Shanghai`), or a
/// `ZoneOffset` id (`Z`, `+08:00`, `-05:30:15`) as produced by
/// `ZoneId.normalized()`.
enum Zone {
    Region(Tz),
    Fixed(FixedOffset),
}

fn parse_zone(id: &str) -> Result<Zone> {
    if let Ok(tz) = id.parse::<Tz>() {
        return Ok(Zone::Region(tz));
    }
    let invalid = || DataFusionError::Execution(format!("date_trunc: invalid timezone: {id}"));
    if id == "Z" {
        return Ok(Zone::Fixed(FixedOffset::east_opt(0).expect("valid offset")));
    }
    let (sign, rest) = match id.as_bytes().first() {
        Some(b'+') => (1, &id[1..]),
        Some(b'-') => (-1, &id[1..]),
        _ => return Err(invalid()),
    };
    let mut secs = 0;
    for (i, part) in rest.split(':').enumerate() {
        let v: i32 = part.parse().map_err(|_| invalid())?;
        secs += v * [3600, 60, 1].get(i).ok_or_else(invalid)?;
    }
    FixedOffset::east_opt(sign * secs)
        .map(Zone::Fixed)
        .ok_or_else(invalid)
}

/// Resolves a local date-time like Java's `ZonedDateTime.ofLocal`: in an
/// overlap the preferred offset is used if valid, otherwise the earlier
/// offset; in a gap the local time is moved later by the length of the gap.
fn resolve_local<T: TimeZone>(
    tz: &T,
    local: NaiveDateTime,
    preferred: Option<FixedOffset>,
) -> Option<DateTime<T>> {
    match tz.from_local_datetime(&local) {
        LocalResult::Single(dt) => Some(dt),
        LocalResult::Ambiguous(earlier, later) => {
            if preferred == Some(later.offset().fix()) {
                Some(later)
            } else {
                Some(earlier)
            }
        }
        LocalResult::None => {
            // the offset before the gap; DST transitions are far more than a
            // day apart
            let before = tz
                .offset_from_utc_datetime(&(local - Duration::days(1)))
                .fix();
            let utc = local - Duration::seconds(before.local_minus_utc() as i64);
            Some(tz.from_utc_datetime(&utc))
        }
    }
}

fn to_micros<T: TimeZone>(dt: DateTime<T>) -> i64 {
    dt.timestamp_micros()
}

/// Spark `DateTimeUtils.truncTimestamp`.
fn trunc_timestamp<T: TimeZone>(micros: i64, level: TruncLevel, tz: &T) -> Option<i64> {
    match level {
        TruncLevel::Microsecond => return Some(micros),
        TruncLevel::Millisecond => return Some(micros - micros.rem_euclid(MICROS_PER_MILLIS)),
        TruncLevel::Second => return Some(micros - micros.rem_euclid(MICROS_PER_SECOND)),
        _ => {}
    }
    let zoned = tz.timestamp_micros(micros).single()?;
    let local = zoned.naive_local();
    match level {
        // ZonedDateTime.truncatedTo(unit), which keeps the original offset if
        // it's still valid
        TruncLevel::Minute | TruncLevel::Hour | TruncLevel::Day => {
            let truncated = match level {
                TruncLevel::Minute => local.with_nanosecond(0)?.with_second(0)?,
                TruncLevel::Hour => local.with_nanosecond(0)?.with_second(0)?.with_minute(0)?,
                _ => local.date().and_hms_opt(0, 0, 0)?,
            };
            resolve_local(tz, truncated, Some(zoned.offset().fix())).map(to_micros)
        }
        // truncate the local date, then LocalDate.atStartOfDay(zoneId)
        _ => {
            let date = local.date();
            let truncated: NaiveDate = match level {
                TruncLevel::Week => {
                    date - Duration::days(date.weekday().num_days_from_monday() as i64)
                }
                TruncLevel::Month => date.with_day(1)?,
                TruncLevel::Quarter => date.with_day(1)?.with_month(date.month0() / 3 * 3 + 1)?,
                TruncLevel::Year => date.with_ordinal(1)?,
                _ => unreachable!(),
            };
            resolve_local(tz, truncated.and_hms_opt(0, 0, 0)?, None).map(to_micros)
        }
    }
}

/// Spark `date_trunc(format, timestamp)`; args are `(format, timestamp,
/// timezone)`, with `timezone` a string literal holding the expression's
/// Java zone id.
pub fn spark_date_trunc(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return Err(DataFusionError::Execution(
            "date_trunc requires three arguments".to_string(),
        ));
    }
    let zone = match &args[2] {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(id))) => parse_zone(id)?,
        other => {
            return Err(DataFusionError::Execution(format!(
                "date_trunc: timezone must be a string literal, got {other:?}"
            )));
        }
    };
    let arrays = ColumnarValue::values_to_arrays(&args[..2])?;
    let formats = cast(&arrays[0], &DataType::Utf8)?;
    let formats = formats
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("cast to Utf8 must succeed");
    let timestamps = cast(
        &arrays[1],
        &DataType::Timestamp(TimeUnit::Microsecond, None),
    )?;
    let timestamps = timestamps
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .expect("cast to Timestamp(Microsecond) must succeed");

    // the format is almost always a literal: parse it once in that case
    let scalar_level = match &args[0] {
        ColumnarValue::Scalar(_) => {
            Some(formats.iter().next().flatten().and_then(parse_trunc_level))
        }
        ColumnarValue::Array(_) => None,
    };
    let trunc = |micros: Option<i64>, level: Option<TruncLevel>| -> Option<i64> {
        let (micros, level) = (micros?, level?);
        match &zone {
            Zone::Region(tz) => trunc_timestamp(micros, level, tz),
            Zone::Fixed(tz) => trunc_timestamp(micros, level, tz),
        }
    };
    let result: TimestampMicrosecondArray = match scalar_level {
        Some(level) => timestamps
            .iter()
            .map(|micros| trunc(micros, level))
            .collect(),
        None => timestamps
            .iter()
            .zip(formats.iter())
            .map(|(micros, format)| trunc(micros, format.and_then(parse_trunc_level)))
            .collect(),
    };

    if args[..2]
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
    {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &result, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn micros(s: &str) -> i64 {
        NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
            .expect("valid timestamp")
            .and_utc()
            .timestamp_micros()
    }

    fn run(format: ColumnarValue, ts: Vec<Option<i64>>, tz: &str) -> Vec<Option<i64>> {
        let ts = ColumnarValue::Array(Arc::new(TimestampMicrosecondArray::from(ts)));
        let tz = ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz.to_string())));
        let result = spark_date_trunc(&[format, ts, tz])
            .and_then(|result| result.into_array(1))
            .expect("date_trunc succeeds");
        result
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("timestamp result")
            .iter()
            .collect()
    }

    fn lit(format: &str) -> ColumnarValue {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(format.to_string())))
    }

    #[test]
    fn test_week_in_session_timezone() {
        // issue #2590: Monday 2026-09-14 00:00 in Asia/Shanghai is
        // 2026-09-13 16:00 UTC; the week must start on 2026-09-14 local
        let input = vec![
            Some(micros("2026-09-12 16:00:00")), // 09-13 local, Sunday
            Some(micros("2026-09-13 16:00:00")), // 09-14 local, Monday
            Some(micros("2026-09-14 16:00:00")), // 09-15 local
            Some(micros("2026-09-20 16:00:00")), // 09-21 local, Monday
            None,
        ];
        assert_eq!(
            run(lit("week"), input, "Asia/Shanghai"),
            vec![
                Some(micros("2026-09-06 16:00:00")),
                Some(micros("2026-09-13 16:00:00")),
                Some(micros("2026-09-13 16:00:00")),
                Some(micros("2026-09-20 16:00:00")),
                None,
            ]
        );
    }

    #[test]
    fn test_levels_and_aliases() {
        // 2026-09-16 13:45:30.123456 local time in America/Los_Angeles (UTC-7)
        let ts = micros("2026-09-16 20:45:30.123456");
        let cases = [
            ("microsecond", "2026-09-16 20:45:30.123456"),
            ("MILLISECOND", "2026-09-16 20:45:30.123"),
            ("second", "2026-09-16 20:45:30"),
            ("Minute", "2026-09-16 20:45:00"),
            ("hour", "2026-09-16 20:00:00"),
            ("day", "2026-09-16 07:00:00"),
            ("DD", "2026-09-16 07:00:00"),
            ("week", "2026-09-14 07:00:00"),
            ("mon", "2026-09-01 07:00:00"),
            ("MONTH", "2026-09-01 07:00:00"),
            ("mm", "2026-09-01 07:00:00"),
            ("quarter", "2026-07-01 07:00:00"),
            // January 1st is in PST (UTC-8)
            ("year", "2026-01-01 08:00:00"),
            ("YYYY", "2026-01-01 08:00:00"),
            ("yy", "2026-01-01 08:00:00"),
        ];
        for (format, expected) in cases {
            assert_eq!(
                run(lit(format), vec![Some(ts)], "America/Los_Angeles"),
                vec![Some(micros(expected))],
                "format {format}"
            );
        }
        assert_eq!(
            run(lit("bogus"), vec![Some(ts)], "America/Los_Angeles"),
            vec![None]
        );
    }

    #[test]
    fn test_per_row_formats_and_fixed_offsets() {
        let ts = micros("2026-09-16 20:45:30");
        let formats = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some("day"),
            Some("bogus"),
            None,
            Some("hour"),
        ])));
        // a fixed +05:30 offset, as produced by ZoneId.normalized()
        assert_eq!(
            run(formats, vec![Some(ts); 4], "+05:30"),
            vec![
                Some(micros("2026-09-16 18:30:00")),
                None,
                None,
                Some(micros("2026-09-16 20:30:00")),
            ]
        );
        assert_eq!(
            run(lit("day"), vec![Some(ts)], "Z"),
            vec![Some(micros("2026-09-16 00:00:00"))]
        );
    }

    #[test]
    fn test_daylight_saving_transitions() {
        // America/Sao_Paulo moved clocks from 00:00 to 01:00 on 2018-11-04, so
        // that day starts at 01:00 local (03:00 UTC), as in Java's
        // LocalDate.atStartOfDay
        let ts = micros("2018-11-04 15:00:00");
        assert_eq!(
            run(lit("day"), vec![Some(ts)], "America/Sao_Paulo"),
            vec![Some(micros("2018-11-04 03:00:00"))]
        );
        assert_eq!(
            run(lit("week"), vec![Some(ts)], "America/Sao_Paulo"),
            vec![Some(micros("2018-10-29 03:00:00"))]
        );
        // America/New_York repeats 01:00-02:00 on 2026-11-01: truncating
        // 01:30 EST (06:30 UTC) to the hour keeps the EST offset
        assert_eq!(
            run(
                lit("hour"),
                vec![Some(micros("2026-11-01 06:30:00"))],
                "America/New_York"
            ),
            vec![Some(micros("2026-11-01 06:00:00"))]
        );
        // and 01:30 EDT (05:30 UTC) keeps the EDT offset
        assert_eq!(
            run(
                lit("hour"),
                vec![Some(micros("2026-11-01 05:30:00"))],
                "America/New_York"
            ),
            vec![Some(micros("2026-11-01 05:00:00"))]
        );
    }
}
