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
    array::{Int64Array, StringArray},
    datatypes::DataType,
};
use chrono::{DateTime, FixedOffset, LocalResult, NaiveDateTime, Offset, TimeZone, Utc};
use chrono_tz::{OffsetComponents, Tz};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    physical_plan::ColumnarValue,
};
use datafusion_ext_commons::arrow::cast::cast;

/// Native implementation of Flink SQL `UNIX_TIMESTAMP(value, format)`: parse a
/// formatted date-time string to a Unix timestamp in seconds, replicating
/// `java.text.SimpleDateFormat` lenient semantics.
///
/// Arguments are always `[value, chronoFormat, zoneId]` (arity 3). `value` is
/// the column of strings to parse; `chronoFormat` and `zoneId` are literal
/// scalars bound at plan time. The format is a `%`-specifier string built by
/// the JVM-side converter from `%Y %m %d %H %M %S` plus literal characters
/// (`%%` for a literal percent) — it is not the original Java pattern.
///
/// Arity 3 is the normalized form of Flink's two string-parsing arities: the
/// JVM-side converter supplies Flink's default `yyyy-MM-dd HH:mm:ss` for the
/// single-argument call, translates the literal pattern for the two-argument
/// call, and resolves the session time zone at plan time. A non-literal or
/// untranslatable format is rejected there and evaluated by Flink instead, so
/// every call reaching this function carries all three arguments already bound.
///
/// Flink's no-argument `UNIX_TIMESTAMP()` parses nothing and yields the current
/// wall-clock time. It is served by [`flink_unix_timestamp_now`], which shares
/// none of the parsing contract below.
///
/// An unparseable value yields `i64::MIN` (Flink's `Long.MIN_VALUE`), never
/// NULL and never an error; a NULL value yields NULL. Arity, format and
/// timezone problems are hard errors rather than silent defaults, so a plumbing
/// bug cannot surface as silently wrong data.
pub fn flink_unix_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 3 {
        return Err(DataFusionError::Execution(format!(
            "Flink_UnixTimestamp requires 3 arguments [value, chronoFormat, zoneId], got {}",
            args.len()
        )));
    }

    let format = utf8_scalar(&args[1]).ok_or_else(|| {
        DataFusionError::Execution("Flink_UnixTimestamp: format must be a non-null string".into())
    })?;
    let zone_id = utf8_scalar(&args[2]).ok_or_else(|| {
        DataFusionError::Execution("Flink_UnixTimestamp: zoneId must be a non-null string".into())
    })?;
    let zone = parse_zone_spec(&zone_id)?;

    let tokens = parse_format(&format)?;

    let num_rows = match &args[0] {
        ColumnarValue::Array(array) => array.len(),
        ColumnarValue::Scalar(_) => 1,
    };
    let value = cast(&args[0].clone().into_array(num_rows)?, &DataType::Utf8)?;
    let value = value
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("internal cast to Utf8 must succeed");

    let result = Int64Array::from_iter(
        value
            .iter()
            .map(|opt_s| opt_s.map(|s| parse_datetime(s, &tokens, &zone).unwrap_or(i64::MIN))),
    );

    Ok(ColumnarValue::Array(Arc::new(result)))
}

/// Native implementation of Flink SQL `UNIX_TIMESTAMP()`: the current Unix
/// timestamp in seconds.
///
/// Takes no arguments. The result is a `ColumnarValue::Scalar`, which the
/// projection broadcasts to the width of the batch being evaluated, so no
/// operand is needed to size the output. The clock is therefore read once per
/// call site per batch, and every row of that batch carries the same second.
///
/// A non-empty argument list is a hard error rather than an ignored operand,
/// on the same grounds as the arity check in [`flink_unix_timestamp`]: nothing
/// enforces arity before this point, so a plumbing bug is only visible here.
///
/// The clock is read as `timestamp_millis() / 1000` so the truncation matches
/// Flink's `System.currentTimeMillis() / 1000` exactly.
pub fn flink_unix_timestamp_now(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if !args.is_empty() {
        return Err(DataFusionError::Execution(format!(
            "Flink_UnixTimestampNow takes no arguments, got {}",
            args.len()
        )));
    }

    let secs = Utc::now().timestamp_millis() / 1000;
    Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(secs))))
}

fn utf8_scalar(arg: &ColumnarValue) -> Option<String> {
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s)))
        | ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Some(s.clone()),
        _ => None,
    }
}

/// Resolve the session time zone id to a zone specification.
///
/// The id arrives already normalized by `ZoneId.getId()`, so the accepted forms
/// are an IANA region id, `GMT±HH:MM`, or a bare `±HH:MM`. Anything else is an
/// error rather than a default: the plan-time gate admits only what this
/// function resolves, so a disagreement between the two is a plumbing bug, and
/// returning an error keeps it from surfacing as silently wrong data.
fn parse_zone_spec(zone_id: &str) -> Result<ZoneSpec> {
    if let Ok(tz) = zone_id.parse::<Tz>() {
        return Ok(ZoneSpec::Iana(tz));
    }
    if let Some(secs) = parse_fixed_offset_secs(zone_id.strip_prefix("GMT").unwrap_or(zone_id)) {
        if let Some(offset) = FixedOffset::east_opt(secs) {
            return Ok(ZoneSpec::Fixed(offset));
        }
    }
    Err(DataFusionError::Execution(format!(
        "Flink_UnixTimestamp: invalid timezone {zone_id}"
    )))
}

/// Parse exactly `±HH:MM`, rejecting any trailing input so that a
/// second-precision id cannot silently lose its seconds field.
fn parse_fixed_offset_secs(s: &str) -> Option<i32> {
    let b = s.as_bytes();
    if b.len() != 6 || b[3] != b':' {
        return None;
    }
    let sign = match b[0] {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    if !b[1..3].iter().chain(&b[4..6]).all(u8::is_ascii_digit) {
        return None;
    }
    let hours = i32::from(b[1] - b'0') * 10 + i32::from(b[2] - b'0');
    let minutes = i32::from(b[4] - b'0') * 10 + i32::from(b[5] - b'0');
    if minutes > 59 {
        return None;
    }
    let secs = sign * (hours * 3600 + minutes * 60);
    if secs.abs() > 18 * 3600 {
        return None;
    }
    Some(secs)
}

/// A field is one of the six numeric SimpleDateFormat components; every
/// supported specifier maps to exactly one. `width` is the `obeyCount` scan
/// window used when the field is immediately followed by another numeric field.
#[derive(Clone, Copy)]
enum FieldKind {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

impl FieldKind {
    /// The `obeyCount` window width. Run length is erased by the Java→chrono
    /// translation (both `M` and `MM` become `%m`), so a canonical width per
    /// field is used: 4 for the year, 2 for the rest — matching the zero-padded
    /// widths of Flink's default `yyyy-MM-dd HH:mm:ss`.
    fn width(self) -> usize {
        match self {
            FieldKind::Year => 4,
            _ => 2,
        }
    }
}

enum Token {
    Field(FieldKind),
    Literal(u8),
}

/// Tokenize the `%`-specifier format. Errors on an unknown specifier or a
/// dangling `%`, since the format is a plan-time constant and such a format is
/// a wiring bug, not bad row data.
fn parse_format(format: &str) -> Result<Vec<Token>> {
    let bytes = format.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let spec = bytes.get(i + 1).ok_or_else(|| {
                DataFusionError::Execution("Flink_UnixTimestamp: dangling '%' in format".into())
            })?;
            let token = match spec {
                b'Y' => Token::Field(FieldKind::Year),
                b'm' => Token::Field(FieldKind::Month),
                b'd' => Token::Field(FieldKind::Day),
                b'H' => Token::Field(FieldKind::Hour),
                b'M' => Token::Field(FieldKind::Minute),
                b'S' => Token::Field(FieldKind::Second),
                b'%' => Token::Literal(b'%'),
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "Flink_UnixTimestamp: unsupported format specifier %{}",
                        *other as char
                    )));
                }
            };
            tokens.push(token);
            i += 2;
        } else {
            tokens.push(Token::Literal(bytes[i]));
            i += 1;
        }
    }
    Ok(tokens)
}

struct Fields {
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
}

impl Default for Fields {
    fn default() -> Self {
        Fields {
            year: 1970,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
        }
    }
}

impl Fields {
    fn set(&mut self, kind: FieldKind, value: i32) {
        match kind {
            FieldKind::Year => self.year = value,
            FieldKind::Month => self.month = value,
            FieldKind::Day => self.day = value,
            FieldKind::Hour => self.hour = value,
            FieldKind::Minute => self.minute = value,
            FieldKind::Second => self.second = value,
        }
    }
}

/// Walk the tokens over `input`. Returns the Unix timestamp in seconds, or
/// `None` on any parse failure (mapped to `i64::MIN` by the caller).
///
/// An empty token list consumes no input and is a failure, not a match on the
/// epoch defaults: `DateFormat.parse` throws whenever parsing advances the
/// position by zero characters, so Flink yields `Long.MIN_VALUE` for an empty
/// format. Every other token consumes at least one byte — a literal matches
/// one, a field needs at least one digit — so an empty token list is the only
/// way to finish having consumed nothing.
fn parse_datetime(input: &str, tokens: &[Token], zone: &ZoneSpec) -> Option<i64> {
    if tokens.is_empty() {
        return None;
    }

    let bytes = input.as_bytes();
    let mut pos = 0usize;
    let mut fields = Fields::default();

    for (idx, token) in tokens.iter().enumerate() {
        match token {
            Token::Literal(lit) => {
                if bytes.get(pos) != Some(lit) {
                    return None;
                }
                pos += 1;
            }
            Token::Field(kind) => {
                // Skip ' ' and '\t' before the field, but anchor the obeyCount
                // window at the pre-skip position so leading whitespace eats into
                // the field's digit budget (a genuine SimpleDateFormat quirk).
                let start0 = pos;
                while matches!(bytes.get(pos), Some(b' ') | Some(b'\t')) {
                    pos += 1;
                }
                if pos >= bytes.len() {
                    return None;
                }

                // obeyCount holds when the next token is another numeric field;
                // then the scan is bounded to `width` chars, otherwise it is greedy.
                let obey_count = matches!(tokens.get(idx + 1), Some(Token::Field(_)));
                let window_end = if obey_count {
                    let end = start0 + kind.width();
                    if end > bytes.len() {
                        return None;
                    }
                    end
                } else {
                    bytes.len()
                };

                let (value, next) = scan_number(bytes, pos, window_end)?;
                fields.set(*kind, value);
                pos = next;
            }
        }
    }

    let local_sec = normalize(&fields);
    let offset = zone.offset_secs(local_sec);
    Some(local_sec - offset)
}

/// Scan an optional leading `-` then one or more ASCII digits within `[start,
/// end)`. A `+` is not a sign and terminates the scan before any digit. Digits
/// accumulate with wrapping and narrow to `i32` via low-32-bit truncation,
/// mirroring Java's `Number.intValue()`. Returns `None` when no digit is
/// consumed.
fn scan_number(bytes: &[u8], start: usize, end: usize) -> Option<(i32, usize)> {
    let mut pos = start;
    let negative = bytes.get(pos) == Some(&b'-');
    if negative {
        pos += 1;
    }

    let digits_start = pos;
    let mut magnitude: i64 = 0;
    while pos < end {
        let b = bytes[pos];
        if !b.is_ascii_digit() {
            break;
        }
        magnitude = magnitude.wrapping_mul(10).wrapping_add((b - b'0') as i64);
        pos += 1;
    }
    if pos == digits_start {
        return None;
    }

    let signed = if negative {
        magnitude.wrapping_neg()
    } else {
        magnitude
    };
    Some((signed as i32, pos))
}

/// Normalize the (possibly out-of-range, possibly negative) fields into a local
/// wall-clock time in seconds since the Unix epoch. Rollover falls out of a
/// single month-then-day computation, so hour/minute/second overflow needs no
/// special case. Dates before the 1582-10-15 Gregorian cutover use the Julian
/// calendar, matching `GregorianCalendar`'s hybrid behavior (chrono is
/// proleptic Gregorian).
fn normalize(fields: &Fields) -> i64 {
    let year = fields.year as i64;
    let month = fields.month as i64;
    let day = fields.day as i64;
    let hour = fields.hour as i64;
    let minute = fields.minute as i64;
    let second = fields.second as i64;

    let year_adj = year + (month - 1).div_euclid(12);
    let month_idx = (month - 1).rem_euclid(12) + 1;

    let jdn_gregorian = gregorian_jdn(year_adj, month_idx) + (day - 1);
    let jdn = if jdn_gregorian >= 2_299_161 {
        jdn_gregorian
    } else {
        julian_jdn(year_adj, month_idx) + (day - 1)
    };

    let epoch_day = jdn - 2_440_588;
    epoch_day * 86_400 + hour * 3_600 + minute * 60 + second
}

/// Julian Day Number of the first of month `(year, month)` in the proleptic
/// Gregorian calendar.
fn gregorian_jdn(year: i64, month: i64) -> i64 {
    let a = (14 - month).div_euclid(12);
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    1 + (153 * m + 2).div_euclid(5) + 365 * y + y.div_euclid(4) - y.div_euclid(100)
        + y.div_euclid(400)
        - 32045
}

/// Julian Day Number of the first of month `(year, month)` in the Julian
/// calendar.
fn julian_jdn(year: i64, month: i64) -> i64 {
    let a = (14 - month).div_euclid(12);
    let y = year + 4800 - a;
    let m = month + 12 * a - 3;
    1 + (153 * m + 2).div_euclid(5) + 365 * y + y.div_euclid(4) - 32083
}

/// UTC offset in seconds for a local wall-clock instant. When the local time is
/// ambiguous (fall-back overlap) or nonexistent (spring-forward gap), the
/// zone's standard (non-DST) offset is used, matching Flink.
/// Out-of-representable-range inputs (only reachable from far-future garbage)
/// fall back to UTC.
fn resolve_offset_secs(local_sec: i64, tz: Tz) -> i64 {
    let naive: NaiveDateTime = match DateTime::from_timestamp(local_sec, 0) {
        Some(dt) => dt.naive_utc(),
        None => return 0,
    };

    match tz.offset_from_local_datetime(&naive) {
        LocalResult::Single(offset) => offset.fix().local_minus_utc() as i64,
        LocalResult::Ambiguous(offset, _) => offset.base_utc_offset().num_seconds(),
        LocalResult::None => tz
            .offset_from_utc_datetime(&naive)
            .base_utc_offset()
            .num_seconds(),
    }
}

/// Session time zone as resolved at plan time: an IANA region from the time
/// zone database, or a constant UTC offset.
#[derive(Clone, Copy)]
enum ZoneSpec {
    Iana(Tz),
    Fixed(FixedOffset),
}

impl ZoneSpec {
    /// UTC offset in seconds for a local wall-clock instant. A fixed offset is
    /// constant by construction, so it ignores the instant entirely.
    fn offset_secs(&self, local_sec: i64) -> i64 {
        match self {
            ZoneSpec::Iana(tz) => resolve_offset_secs(local_sec, *tz),
            ZoneSpec::Fixed(offset) => offset.local_minus_utc() as i64,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;

    use super::*;

    /// Run one `(value, chrono_format, timezone) -> expected_seconds` case
    /// through the full function and assert the single-row Int64 result.
    fn run(value: &str, format: &str, tz: &str) -> i64 {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(format.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz.to_string()))),
        ];
        let out = flink_unix_timestamp(&args)
            .expect("flink_unix_timestamp must not error on valid scalar args")
            .into_array(1)
            .expect("result must materialize to an array");
        let out = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("result must be Int64Array");
        assert_eq!(out.len(), 1);
        out.value(0)
    }

    /// Whether the zone id is rejected outright. A rejected zone is an error
    /// from the function itself, distinct from the `i64::MIN` sentinel that
    /// a row-level parse failure produces.
    fn zone_rejected(tz: &str) -> bool {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("2020-10-10 00:00:01".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(DEFAULT.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(tz.to_string()))),
        ];
        flink_unix_timestamp(&args).is_err()
    }

    const MIN: i64 = i64::MIN;
    const DEFAULT: &str = "%Y-%m-%d %H:%M:%S";

    // Happy path across formats and timezones.
    #[test]
    fn oracle_happy_path() {
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "UTC"), 1602288001);
        assert_eq!(
            run("2020-10-10 00:00:01", DEFAULT, "Asia/Shanghai"),
            1602259201
        );
        assert_eq!(
            run("2015-07-24 10:00:00", DEFAULT, "Asia/Tokyo"),
            1437699600
        );
        assert_eq!(run("20201010000001", "%Y%m%d%H%M%S", "UTC"), 1602288001);
        assert_eq!(run("2020-10-10", "%Y-%m-%d", "UTC"), 1602288000);
    }

    // Non-padded fields (1-digit values under 2-digit specifiers) — the core
    // leniency requirement.
    #[test]
    fn oracle_non_padded() {
        assert_eq!(run("2020-1-1 0:0:0", DEFAULT, "UTC"), 1577836800);
        assert_eq!(run("2020-01-01 00:00:00", DEFAULT, "UTC"), 1577836800);
    }

    // Greedy scan (field bounded only by the next literal) and obeyCount windows
    // (field bounded to its canonical width when followed by another numeric
    // field).
    #[test]
    fn oracle_greedy_and_obey_count() {
        assert_eq!(run("2020-115-01 00:00:00", DEFAULT, "UTC"), 1877558400);
        assert_eq!(run("12020-01-01 00:00:00", DEFAULT, "UTC"), 317147356800);
        assert_eq!(run("2020-0000001-01 00:00:00", DEFAULT, "UTC"), 1577836800);
        assert_eq!(run("2020101000000", "%Y%m%d%H%M%S", "UTC"), 1602288000);
        assert_eq!(run("202010100000012", "%Y%m%d%H%M%S", "UTC"), 1602288012);
    }

    // Field rollover across month/day/hour/minute/second, including the
    // normalization order that rolls month before day.
    #[test]
    fn oracle_rollover() {
        assert_eq!(run("2020-13-01 00:00:00", DEFAULT, "UTC"), 1609459200);
        assert_eq!(run("2020-00-01 00:00:00", DEFAULT, "UTC"), 1575158400);
        assert_eq!(run("2020-99-01 00:00:00", DEFAULT, "UTC"), 1835481600);
        assert_eq!(run("2020-01-45 00:00:00", DEFAULT, "UTC"), 1581638400);
        assert_eq!(run("2020-01-00 00:00:00", DEFAULT, "UTC"), 1577750400);
        assert_eq!(run("2020-02-30 00:00:00", DEFAULT, "UTC"), 1583020800);
        assert_eq!(run("2021-02-30 00:00:00", DEFAULT, "UTC"), 1614643200);
        assert_eq!(run("2020-01-01 24:00:00", DEFAULT, "UTC"), 1577923200);
        assert_eq!(run("2020-01-01 25:00:00", DEFAULT, "UTC"), 1577926800);
        assert_eq!(run("2020-01-01 00:60:00", DEFAULT, "UTC"), 1577840400);
        assert_eq!(run("2020-01-01 00:00:60", DEFAULT, "UTC"), 1577836860);
        assert_eq!(run("2020-01-01 00:00:99", DEFAULT, "UTC"), 1577836899);
        assert_eq!(run("2020-13-32 00:00:00", DEFAULT, "UTC"), 1612137600);
        assert_eq!(run("2020-25-32 00:00:00", DEFAULT, "UTC"), 1643673600);
        assert_eq!(run("2020-13-32 25:61:61", DEFAULT, "UTC"), 1612231321);
    }

    // Negative fields: a leading '-' is a valid sign (negative month, negative day,
    // BC year through the Julian branch).
    #[test]
    fn oracle_negative_fields() {
        assert_eq!(run("2020--5-10 00:00:01", DEFAULT, "UTC"), 1562716801);
        assert_eq!(run("2020-10--5 00:00:01", DEFAULT, "UTC"), 1600992001);
        assert_eq!(run("-2020-10-10 00:00:01", DEFAULT, "UTC"), -125889292799);
    }

    // Trailing input is ignored; leading/inner whitespace is skipped before a field
    // (and eats the obeyCount budget) but never before a literal.
    #[test]
    fn oracle_whitespace_and_trailing() {
        assert_eq!(run("2020-10-10 00:00:01XYZ", DEFAULT, "UTC"), 1602288001);
        assert_eq!(run("2020-10-10 00:00:01   ", DEFAULT, "UTC"), 1602288001);
        assert_eq!(run("2020-10-10 00:00:01", "%Y-%m-%d", "UTC"), 1602288000);
        assert_eq!(run(" 2020-10-10 00:00:01", DEFAULT, "UTC"), 1602288001);
        assert_eq!(run("\t2020-10-10 00:00:01", DEFAULT, "UTC"), 1602288001);
        assert_eq!(run("X2020-10-10 00:00:01", DEFAULT, "UTC"), MIN);
        assert_eq!(run("2020- 10-10 00:00:01", DEFAULT, "UTC"), 1602288001);
        assert_eq!(run("2020 -10-10 00:00:01", DEFAULT, "UTC"), MIN);
        assert_eq!(run(" 20200101", "%Y%m%d", "UTC"), -55786752000);
    }

    // Parse failures resolve to i64::MIN, never NULL and never an error: literal
    // mismatch, short input, non-digit field, grouping separator, and a '+' sign.
    #[test]
    fn oracle_failures() {
        assert_eq!(run("not a date", DEFAULT, "UTC"), MIN);
        assert_eq!(run("", DEFAULT, "UTC"), MIN);
        assert_eq!(run("   ", DEFAULT, "UTC"), MIN);
        assert_eq!(run("2020-10-10", DEFAULT, "UTC"), MIN);
        assert_eq!(run("2020/10/10 00:00:01", DEFAULT, "UTC"), MIN);
        assert_eq!(run("2020-XX-10 00:00:01", DEFAULT, "UTC"), MIN);
        assert_eq!(run("2,020-01-01 00:00:01", DEFAULT, "UTC"), MIN);
        assert_eq!(run("+2020-10-10 00:00:01", DEFAULT, "UTC"), MIN);
    }

    // Epoch and pre-1970 negatives.
    //
    // Sub-second inputs (e.g. `... .SSS` with millisecond truncation) are not
    // tested here: the supported specifiers carry no sub-second precision, and
    // a format containing `S` is rejected by the upstream scanner, so such
    // inputs fall back to Flink and never reach this function.
    #[test]
    fn oracle_epoch() {
        assert_eq!(run("1970-01-01 00:00:00", DEFAULT, "UTC"), 0);
        assert_eq!(run("1969-12-31 23:59:59", DEFAULT, "UTC"), -1);
        assert_eq!(run("1900-01-01 00:00:00", DEFAULT, "UTC"), -2208988800);
    }

    // DST gap and overlap across several zones — the zone's standard (non-DST)
    // offset wins, so a spring-forward gap collapses to the post-transition
    // instant.
    #[test]
    fn oracle_dst() {
        assert_eq!(run("2005-03-27 02:00:00", DEFAULT, "MET"), 1111885200);
        assert_eq!(run("2005-03-27 03:00:00", DEFAULT, "MET"), 1111885200);
        assert_eq!(
            run("2021-03-14 02:30:00", DEFAULT, "America/New_York"),
            1615707000
        );
        assert_eq!(run("2005-10-30 02:30:00", DEFAULT, "MET"), 1130635800);
        assert_eq!(
            run("2021-11-07 01:30:00", DEFAULT, "America/New_York"),
            1636266600
        );
        assert_eq!(
            run("2021-04-04 02:30:00", DEFAULT, "Australia/Sydney"),
            1617467400
        );
        assert_eq!(
            run("2021-10-03 02:30:00", DEFAULT, "Australia/Sydney"),
            1633192200
        );
    }

    // Every `GMT±HH:MM` zone applies the constant offset `sign × (hh·3600 +
    // mm·60)`, across the whole family. The expected offset is recomputed from
    // the loop indices that built the id, never re-parsed from the id string,
    // so the assertion cannot degenerate into re-running the code under test.
    //
    // `GMT±00:00` is excluded because it normalizes to plain `GMT` and so never
    // reaches this function in that spelling; anything past `±18:00` is not a zone
    // id at all. The counter guards against a skip condition that silently empties
    // the loop.
    #[test]
    fn oracle_fixed_offset_family_exhaustive() {
        const VALUE: &str = "2020-10-10 00:00:01";
        let local_sec = run(VALUE, DEFAULT, "UTC");

        let mut covered = 0;
        for sign in [1i64, -1i64] {
            for hh in 0..=18i64 {
                for mm in 0..=59i64 {
                    let magnitude = hh * 3600 + mm * 60;
                    if magnitude == 0 || magnitude > 18 * 3600 {
                        continue;
                    }
                    let sign_char = if sign > 0 { '+' } else { '-' };
                    let id = format!("GMT{sign_char}{hh:02}:{mm:02}");
                    let expected_offset = sign * magnitude;
                    assert_eq!(
                        run(VALUE, DEFAULT, &id),
                        local_sec - expected_offset,
                        "zone {id}"
                    );
                    covered += 1;
                }
            }
        }
        assert_eq!(covered, 2160);
    }

    // Spot values for the fixed-offset family, taken from the Java oracle rather
    // than from the closed form, so the closed form itself is falsifiable: the
    // exhaustive sweep above recomputes the same arithmetic and therefore cannot
    // detect a sign or unit error shared by both sides.
    //
    // Covers plain `GMT` (which resolves as a region, not an offset), the smallest
    // and largest offsets in both directions, a non-quarter-hour offset, a
    // day-crossing at the epoch, a pre-1900 date, and the 1582 hybrid-calendar
    // boundary.
    //
    // The last three rows are the bare `±HH:MM` spelling, and are the complete
    // reachable set of it: the session zone is only unvalidated when it comes from
    // `ZoneId.systemDefault()`, where `TZ=EST`/`MST`/`HST` map through
    // `ZoneId.SHORT_IDS` to exactly these three. They assert the resolved offset
    // rather than merely the absence of an error, so a resolver that accepted the
    // bare form but mapped it to UTC would still fail here.
    #[test]
    fn oracle_fixed_offset() {
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT"), 1602288001);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT+00:01"), 1602287941);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT-00:01"), 1602288061);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT+18:00"), 1602223201);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT-18:00"), 1602352801);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT+05:07"), 1602269581);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT+05:45"), 1602267301);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT+08:00"), 1602259201);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "GMT-08:00"), 1602316801);
        assert_eq!(run("1970-01-01 00:00:00", DEFAULT, "GMT+08:00"), -28800);
        assert_eq!(
            run("1900-01-01 00:00:00", DEFAULT, "GMT-08:00"),
            -2208960000
        );
        assert_eq!(
            run("1582-10-15 00:00:00", DEFAULT, "GMT-08:00"),
            -12219264000
        );
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "-05:00"), 1602306001);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "-07:00"), 1602313201);
        assert_eq!(run("2020-10-10 00:00:01", DEFAULT, "-10:00"), 1602324001);
    }

    // Literals from quoted patterns: a case-sensitive letter, a literal percent
    // (`%%`), and a literal single quote.
    #[test]
    fn oracle_quoting() {
        assert_eq!(
            run("2020-10-10T00:00:01", "%Y-%m-%dT%H:%M:%S", "UTC"),
            1602288001
        );
        assert_eq!(run("2020-10-10t00:00:01", "%Y-%m-%dT%H:%M:%S", "UTC"), MIN);
        assert_eq!(run("2020%10", "%Y%%%m", "UTC"), 1601510400);
        assert_eq!(run("2020'10", "%Y'%m", "UTC"), 1601510400);
    }

    // Julian/Gregorian hybrid calendar around the 1582-10-15 cutover: the last
    // Julian day, the first Gregorian day, a nonexistent day mapped forward, and a
    // deep-Julian date.
    #[test]
    fn oracle_hybrid_calendar() {
        assert_eq!(run("1582-10-15 00:00:00", DEFAULT, "UTC"), -12219292800);
        assert_eq!(run("1582-10-04 00:00:00", DEFAULT, "UTC"), -12219379200);
        assert_eq!(run("1582-10-05 00:00:00", DEFAULT, "UTC"), -12219292800);
        assert_eq!(run("1000-01-01 00:00:00", DEFAULT, "UTC"), -30609792000);
    }

    // A NULL value yields NULL output, distinct from the i64::MIN parse-failure
    // sentinel.
    #[test]
    fn null_value_yields_null() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(None)),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(DEFAULT.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTC".to_string()))),
        ];
        let out = flink_unix_timestamp(&args)
            .expect("must not error")
            .into_array(1)
            .expect("must materialize");
        let out = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("result must be Int64Array");
        assert_eq!(out.len(), 1);
        assert!(out.is_null(0));
    }

    // Mixed array: NULL propagates per-row, valid rows parse, garbage → i64::MIN.
    #[test]
    fn array_with_null_and_garbage() {
        let value = Arc::new(StringArray::from(vec![
            Some("2020-10-10 00:00:01"),
            None,
            Some("garbage"),
        ]));
        let args = vec![
            ColumnarValue::Array(value),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(DEFAULT.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTC".to_string()))),
        ];
        let out = flink_unix_timestamp(&args)
            .expect("must not error")
            .into_array(3)
            .expect("must materialize");
        let out = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("result must be Int64Array");
        assert_eq!(out.value(0), 1602288001);
        assert!(out.is_null(1));
        assert_eq!(out.value(2), i64::MIN);
    }

    // In numeric adjacency, `%m` scans its canonical 2-digit obeyCount window: on
    // `20201010` with `%Y%m%d`, `%Y` reads 4 (2020), `%m` reads 2 (10), `%d` reads
    // the rest (10), giving 2020-10-10. The run length that would distinguish a
    // 1-digit month is not carried in the chrono format, so the native canonical
    // width defines the contract here (the JVM scanner decides which patterns reach
    // native).
    #[test]
    fn adjacent_month_uses_canonical_width_two() {
        assert_eq!(run("20201010", "%Y%m%d", "UTC"), 1602288000);
    }

    // The year field's canonical obeyCount window is 4: on `20200101` with
    // `%Y%m%d`, `%Y` reads exactly `2020`, leaving `01`/`01` for month and day.
    #[test]
    fn year_uses_canonical_width_four() {
        assert_eq!(run("20200101", "%Y%m%d", "UTC"), 1577836800);
    }

    #[test]
    fn arity_mismatch_errors() {
        let two = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("2020-10-10 00:00:01".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(DEFAULT.to_string()))),
        ];
        assert!(flink_unix_timestamp(&two).is_err());

        let four = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("2020-10-10 00:00:01".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(DEFAULT.to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTC".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("extra".to_string()))),
        ];
        assert!(flink_unix_timestamp(&four).is_err());

        let one = vec![ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            "unexpected".to_string(),
        )))];
        assert!(flink_unix_timestamp_now(&one).is_err());
    }

    // A zone id that is not resolvable is an error, not a silent default. Beyond
    // outright nonsense, the rows here are spellings close enough to a real id that
    // a permissive resolver would accept them: wrong case, surrounding whitespace,
    // and the `SystemV/*` family, which is absent from the time zone database and
    // must keep being rejected rather than being swept in with the fixed offsets.
    #[test]
    fn invalid_timezone_errors() {
        assert!(zone_rejected("Mars/Olympus"));
        assert!(zone_rejected("gmt+08:00"));
        assert!(zone_rejected(" GMT+08:00"));
        assert!(zone_rejected("GMT+08:00 "));
        assert!(zone_rejected("SystemV/PST8"));
    }

    // The fixed-offset grammar is exactly `±HH:MM`, optionally prefixed by `GMT`,
    // with nothing before or after it. Each row is a spelling a permissive offset
    // parser would accept: a trailing seconds field (which a truncating parser
    // silently drops, turning `+08:00:30` into `+08:00`), a separator that is not a
    // colon, a repeated separator, a Unicode minus sign in place of the ASCII
    // hyphen, a single-digit hour, and a single-digit minute.
    #[test]
    fn invalid_fixed_offset_zone_errors() {
        assert!(zone_rejected("GMT+08:00:30"));
        assert!(zone_rejected("GMT+08 00"));
        assert!(zone_rejected("GMT+08::::00"));
        assert!(zone_rejected("GMT\u{2212}08:00")); // U+2212 MINUS SIGN, not '-'
        assert!(zone_rejected("GMT+8"));
        assert!(zone_rejected("+08:0"));
    }

    // The two numeric bounds on a fixed offset: minutes are a sixtieth of an hour,
    // not a free two-digit field, and the magnitude stops at ±18:00.
    //
    // Both are unreachable through the plan-time gate, which only ever emits ids
    // built from a constructed `ZoneId` — `ZoneId.of` throws on all of these. They
    // are asserted because the bounds are the parser's own guarantee rather than an
    // inherited one, and a caller that does not hold the `ZoneId` invariant would
    // otherwise resolve `GMT+08:60` to a well-formed offset an hour past where it
    // should be. Each row fails for exactly one of the two checks, so neither can
    // be deleted while the suite stays green.
    #[test]
    fn out_of_range_fixed_offset_zone_errors() {
        assert!(zone_rejected("GMT+08:60"));
        assert!(zone_rejected("GMT-08:60"));

        assert!(zone_rejected("GMT+18:01"));
        assert!(zone_rejected("GMT-18:01"));
        assert!(zone_rejected("GMT+19:00"));
    }

    // An empty format consumes nothing, so it matches no input at all rather
    // than matching everything at the epoch defaults.
    #[test]
    fn empty_format_never_matches() {
        assert_eq!(run("2020-10-10 00:00:01", "", "UTC"), MIN);
        assert_eq!(run("2020-10-10 00:00:01", "", "Asia/Shanghai"), MIN);
        assert_eq!(run("", "", "UTC"), MIN);
    }

    // The no-argument form returns a Scalar, not an Array. That is what lets it
    // take no operand: the projection broadcasts the scalar to the batch width,
    // giving every row of the batch the same second.
    #[test]
    fn now_returns_scalar_broadcast_over_the_batch() {
        let out = flink_unix_timestamp_now(&[]).expect("flink_unix_timestamp_now must not error");
        assert!(matches!(
            &out,
            ColumnarValue::Scalar(ScalarValue::Int64(Some(_)))
        ));

        let batch_len = 4;
        let out = out.into_array(batch_len).expect("must materialize");
        let out = out
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("result must be Int64Array");
        assert_eq!(out.len(), batch_len);
        for i in 0..batch_len {
            assert!(!out.is_null(i));
            assert_eq!(out.value(i), out.value(0));
        }
    }

    // The result is in seconds, not milliseconds: it lands inside the window
    // bracketed by the test's own clock reads.
    #[test]
    fn now_returns_epoch_seconds() {
        let before = Utc::now().timestamp();
        let out = flink_unix_timestamp_now(&[]).expect("flink_unix_timestamp_now must not error");
        let after = Utc::now().timestamp();

        let secs = match out {
            ColumnarValue::Scalar(ScalarValue::Int64(secs)) => secs,
            _ => None,
        }
        .expect("result must be a non-null Int64 scalar");
        assert!(
            (before..=after).contains(&secs),
            "expected {secs} within [{before}, {after}]"
        );
    }

    #[test]
    fn unsupported_specifier_errors() {
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("2020".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("%Y-%Q".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("UTC".to_string()))),
        ];
        assert!(flink_unix_timestamp(&args).is_err());
    }
}
