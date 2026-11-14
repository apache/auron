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
    array::{Int64Builder, StringArray, as_primitive_array},
    datatypes::{DataType, Date32Type, TimeUnit, TimestampMicrosecondType},
};
use datafusion::{
    common::{DataFusionError, Result, ScalarValue},
    logical_expr::ColumnarValue,
};
use datafusion_ext_commons::arrow::cast::cast;

const MICROS_PER_SECOND: i64 = 1_000_000;

/// Spark ToUnixTimestamp. The converter supplies one of three signatures:
/// [timestamp], [date, offsetSeconds], [string, format, offsetSeconds, failOnError].
/// Formats and offsets are scalar constants; unsupported cases fall back before conversion.
pub fn spark_to_unix_timestamp(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let Some(value) = args.first() else {
        return Err(contract_error("missing input"));
    };
    let scalar = matches!(value, ColumnarValue::Scalar(_));
    let len = match value {
        ColumnarValue::Array(array) => array.len(),
        ColumnarValue::Scalar(_) => 1,
    };
    let array = value.clone().into_array(len)?;
    let mut output = Int64Builder::with_capacity(len);

    match array.data_type() {
        DataType::Timestamp(TimeUnit::Microsecond, _) if args.len() == 1 => {
            for value in as_primitive_array::<TimestampMicrosecondType>(&array) {
                output.append_option(value.map(|micros| micros / MICROS_PER_SECOND));
            }
        }
        DataType::Date32 if args.len() == 2 => {
            let offset = offset_seconds(&args[1])?;
            for value in as_primitive_array::<Date32Type>(&array) {
                output.append_option(
                    value
                        .map(|days| {
                            let seconds = i64::from(days) * 86_400 - offset;
                            // Spark checks daysToMicros overflow before dividing back to seconds.
                            seconds.checked_mul(MICROS_PER_SECOND).ok_or_else(overflow)
                        })
                        .transpose()?
                        .map(|micros| micros / MICROS_PER_SECOND),
                );
            }
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if args.len() == 4 => {
            let format = match &args[1] {
                ColumnarValue::Scalar(ScalarValue::Utf8(value)) => value.as_deref(),
                _ => return Err(contract_error("format must be a string scalar")),
            };
            let pattern = format
                .map(|format| {
                    Pattern::new(format).ok_or_else(|| contract_error("unsupported format"))
                })
                .transpose()?;
            let offset = offset_seconds(&args[2])?;
            let fail_on_error = match &args[3] {
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(value))) => *value,
                _ => return Err(contract_error("failOnError must be a boolean scalar")),
            };
            let strings = cast(&array, &DataType::Utf8)?;
            let strings = strings
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| contract_error("expected UTF8 input"))?;
            for value in strings {
                let parsed = match (value, pattern) {
                    (Some(value), Some(pattern)) => match pattern.parse(value, offset) {
                        Ok(micros) => Some(micros / MICROS_PER_SECOND),
                        Err(ParseError::Invalid) if !fail_on_error => None,
                        Err(ParseError::Invalid) => {
                            return Err(DataFusionError::Execution(format!(
                                "to_unix_timestamp cannot parse '{value}' with format '{}'",
                                format.expect("a compiled pattern has a format")
                            )));
                        }
                        Err(ParseError::Overflow) => return Err(overflow()),
                    },
                    _ => None,
                };
                output.append_option(parsed);
            }
        }
        _ => return Err(contract_error("unsupported input type or argument count")),
    }

    let output = output.finish();
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
            &output, 0,
        )?))
    } else {
        Ok(ColumnarValue::Array(Arc::new(output)))
    }
}

fn contract_error(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("Spark_ToUnixTimestamp: {message}"))
}

fn overflow() -> DataFusionError {
    DataFusionError::Execution("long overflow in to_unix_timestamp".into())
}

fn offset_seconds(value: &ColumnarValue) -> Result<i64> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Int32(Some(seconds)))
            if seconds.unsigned_abs() <= 18 * 3600 =>
        {
            Ok(i64::from(*seconds))
        }
        _ => Err(contract_error("offset must be a scalar within +/-18 hours")),
    }
}

#[derive(Clone, Copy)]
struct Pattern {
    time_separator: Option<u8>,
    fraction_digits: usize,
}

#[derive(Debug)]
enum ParseError {
    Invalid,
    Overflow,
}

impl Pattern {
    fn new(format: &str) -> Option<Self> {
        if format == "yyyy-MM-dd" {
            return Some(Self {
                time_separator: None,
                fraction_digits: 0,
            });
        }
        for (base, separator) in [
            ("yyyy-MM-dd HH:mm:ss", b' '),
            ("yyyy-MM-dd'T'HH:mm:ss", b'T'),
        ] {
            let Some(suffix) = format.strip_prefix(base) else {
                continue;
            };
            let fraction_digits = if suffix.is_empty() {
                0
            } else {
                let fraction = suffix.strip_prefix('.')?;
                if !(1..=9).contains(&fraction.len()) || !fraction.bytes().all(|b| b == b'S') {
                    return None;
                }
                fraction.len()
            };
            return Some(Self {
                time_separator: Some(separator),
                fraction_digits,
            });
        }
        None
    }

    fn parse(self, input: &str, offset: i64) -> std::result::Result<i64, ParseError> {
        let bytes = input.as_bytes();
        let signed = matches!(bytes.first(), Some(b'+' | b'-'));
        let start = usize::from(signed);
        let end = start
            + bytes
                .get(start..)
                .ok_or(ParseError::Invalid)?
                .iter()
                .position(|b| !b.is_ascii_digit())
                .ok_or(ParseError::Invalid)?;
        let digits = end - start;
        if !(4..=19).contains(&digits)
            || (!signed && digits != 4)
            || (bytes.first() == Some(&b'+') && digits <= 4)
        {
            return Err(ParseError::Invalid);
        }
        let mut year = 0i64;
        for digit in &bytes[start..end] {
            year = year
                .checked_mul(10)
                .and_then(|year| year.checked_add(i64::from(digit - b'0')))
                .ok_or(ParseError::Invalid)?;
        }
        if year > 999_999_999 || (bytes.first() == Some(&b'-') && year == 0) {
            return Err(ParseError::Invalid);
        }
        if bytes.first() == Some(&b'-') {
            year = -year;
        }
        let mut cursor = end;
        literal(bytes, &mut cursor, b'-')?;
        let month = number(bytes, &mut cursor)?;
        literal(bytes, &mut cursor, b'-')?;
        let day = number(bytes, &mut cursor)?;
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let max_day = match month {
            2 => 28 + i64::from(leap),
            4 | 6 | 9 | 11 => 30,
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            _ => return Err(ParseError::Invalid),
        };
        if day == 0 || day > max_day {
            return Err(ParseError::Invalid);
        }
        let mut seconds = 0;
        let mut micros = 0;
        if let Some(separator) = self.time_separator {
            literal(bytes, &mut cursor, separator)?;
            let hour = number(bytes, &mut cursor)?;
            literal(bytes, &mut cursor, b':')?;
            let minute = number(bytes, &mut cursor)?;
            literal(bytes, &mut cursor, b':')?;
            let second = number(bytes, &mut cursor)?;
            if hour > 23 || minute > 59 || second > 59 {
                return Err(ParseError::Invalid);
            }
            seconds = hour * 3600 + minute * 60 + second;
            if self.fraction_digits > 0 {
                literal(bytes, &mut cursor, b'.')?;
                let start = cursor;
                while let Some(digit) = bytes.get(cursor).filter(|b| b.is_ascii_digit()) {
                    if cursor - start < 6 {
                        micros = micros * 10 + i64::from(digit - b'0');
                    }
                    cursor += 1;
                }
                let digits = cursor - start;
                if digits == 0 || digits > self.fraction_digits {
                    return Err(ParseError::Invalid);
                }
                micros *= 10i64.pow(6 - digits.min(6) as u32);
            }
        }
        if cursor != bytes.len() {
            return Err(ParseError::Invalid);
        }

        // Gregorian 400-year cycles keep the calculation valid outside Chrono's date range.
        let year = year - i64::from(month <= 2);
        let era = year.div_euclid(400);
        let year_of_era = year - era * 400;
        let march_month = month + if month > 2 { -3 } else { 9 };
        let day_of_year = (153 * march_month + 2) / 5 + day - 1;
        let days = era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100
            + day_of_year
            - 719_468;
        let seconds = days * 86_400 + seconds - offset;
        // Preserve Spark TimestampFormatter's separate multiplyExact/addExact checks.
        seconds
            .checked_mul(MICROS_PER_SECOND)
            .and_then(|seconds| seconds.checked_add(micros))
            .ok_or(ParseError::Overflow)
    }
}

fn literal(bytes: &[u8], cursor: &mut usize, expected: u8) -> std::result::Result<(), ParseError> {
    // Spark's formatter uses case-insensitive parsing, including quoted literals.
    if !bytes
        .get(*cursor)
        .is_some_and(|actual| actual.eq_ignore_ascii_case(&expected))
    {
        return Err(ParseError::Invalid);
    }
    *cursor += 1;
    Ok(())
}

fn number(bytes: &[u8], cursor: &mut usize) -> std::result::Result<i64, ParseError> {
    let digits = bytes.get(*cursor..*cursor + 2).ok_or(ParseError::Invalid)?;
    if !digits.iter().all(u8::is_ascii_digit) {
        return Err(ParseError::Invalid);
    }
    *cursor += 2;
    Ok(i64::from(digits[0] - b'0') * 10 + i64::from(digits[1] - b'0'))
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, Date32Array, Int64Array, TimestampMicrosecondArray},
        datatypes::Int64Type,
    };
    use chrono::NaiveDate;

    use super::*;

    fn scalar(value: ScalarValue) -> ColumnarValue {
        ColumnarValue::Scalar(value)
    }

    fn string_args(value: Option<&str>, format: Option<&str>, ansi: bool) -> Vec<ColumnarValue> {
        vec![
            scalar(ScalarValue::Utf8(value.map(str::to_owned))),
            scalar(ScalarValue::Utf8(format.map(str::to_owned))),
            scalar(ScalarValue::Int32(Some(0))),
            scalar(ScalarValue::Boolean(Some(ansi))),
        ]
    }

    fn run(args: &[ColumnarValue]) -> Result<Option<i64>> {
        match spark_to_unix_timestamp(args)? {
            ColumnarValue::Scalar(ScalarValue::Int64(value)) => Ok(value),
            _ => Err(contract_error("expected scalar Int64 result")),
        }
    }

    #[test]
    fn timestamp_truncation_and_boundaries() -> Result<()> {
        for micros in [
            i64::MIN,
            -1_000_001,
            -1_000_000,
            -999_999,
            -1,
            0,
            1,
            i64::MAX,
        ] {
            assert_eq!(
                run(&[scalar(ScalarValue::TimestampMicrosecond(
                    Some(micros),
                    None
                ))])?,
                Some(micros / MICROS_PER_SECOND)
            );
        }
        assert_eq!(
            run(&[scalar(ScalarValue::TimestampMicrosecond(None, None))])?,
            None
        );
        Ok(())
    }

    #[test]
    fn dates_apply_second_precision_offsets_and_check_micros() -> Result<()> {
        assert_eq!(
            run(&[
                scalar(ScalarValue::Date32(Some(0))),
                scalar(ScalarValue::Int32(Some(28_830)))
            ])?,
            Some(-28_830)
        );
        assert_eq!(
            run(&[
                scalar(ScalarValue::Date32(None)),
                scalar(ScalarValue::Int32(Some(0)))
            ])?,
            None
        );
        for days in [i32::MIN, i32::MAX] {
            assert!(
                run(&[
                    scalar(ScalarValue::Date32(Some(days))),
                    scalar(ScalarValue::Int32(Some(0)))
                ])
                .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn strict_dates_and_historical_calendar() -> Result<()> {
        for year in [
            -200_000, -1, 0, 1, 1500, 1582, 1900, 1970, 2000, 2024, 9999, 200_000,
        ] {
            let year_text = if year < 0 {
                format!("-{abs:04}", abs = -year)
            } else if year > 9999 {
                format!("+{year}")
            } else {
                format!("{year:04}")
            };
            for (month, day) in [(1, 1), (3, 1), (10, 15), (12, 31)] {
                let input = format!("{year_text}-{month:02}-{day:02}");
                let expected = NaiveDate::from_ymd_opt(year, month, day)
                    .expect("valid oracle date")
                    .and_hms_opt(0, 0, 0)
                    .expect("valid midnight")
                    .and_utc()
                    .timestamp();
                assert_eq!(
                    run(&string_args(Some(&input), Some("yyyy-MM-dd"), false))?,
                    Some(expected),
                    "{input}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn invalid_rows_follow_ansi() -> Result<()> {
        for input in [
            "",
            "2021-02-29",
            "2020-02-30",
            "2020-13-01",
            "2020-01-00",
            "2020-1-01",
            "2020-01-01XYZ",
            " 2020-01-01",
            "10000-01-01",
            "+2020-01-01",
            "-0000-01-01",
            "é2020-01-01",
        ] {
            assert_eq!(
                run(&string_args(Some(input), Some("yyyy-MM-dd"), false))?,
                None,
                "{input}"
            );
            assert!(
                run(&string_args(Some(input), Some("yyyy-MM-dd"), true)).is_err(),
                "{input}"
            );
        }
        for input in [
            "2020-01-01 24:00:00",
            "2020-01-01 00:60:00",
            "2020-01-01 00:00:60",
            "2020-01-01 00:00:00 ",
        ] {
            assert_eq!(
                run(&string_args(
                    Some(input),
                    Some("yyyy-MM-dd HH:mm:ss"),
                    false
                ))?,
                None
            );
        }
        Ok(())
    }

    #[test]
    fn fractions_truncate_after_building_negative_micros() -> Result<()> {
        for digits in 1..=9 {
            let format = format!("yyyy-MM-dd HH:mm:ss.{}", "S".repeat(digits));
            let input = format!("1969-12-31 23:59:59.{}", "9".repeat(digits));
            assert_eq!(
                run(&string_args(Some(&input), Some(&format), false))?,
                Some(0)
            );
            assert_eq!(
                run(&string_args(
                    Some("1969-12-31 23:59:59.1"),
                    Some(&format),
                    false
                ))?,
                Some(0)
            );
        }
        assert_eq!(
            run(&string_args(
                Some("1970-01-01T00:00:01"),
                Some("yyyy-MM-dd'T'HH:mm:ss"),
                true
            ))?,
            Some(1)
        );
        assert_eq!(
            run(&string_args(
                Some("1970-01-01 00:00:00.12"),
                Some("yyyy-MM-dd HH:mm:ss.S"),
                false
            ))?,
            None
        );
        Ok(())
    }

    #[test]
    fn quoted_t_accepts_both_cases() -> Result<()> {
        for ansi in [false, true] {
            for separator in ['T', 't'] {
                let input = format!("1970-01-01{separator}00:00:01");
                assert_eq!(
                    run(&string_args(
                        Some(&input),
                        Some("yyyy-MM-dd'T'HH:mm:ss"),
                        ansi
                    ))?,
                    Some(1)
                );
                let input = format!("1969-12-31{separator}23:59:59.999999999");
                assert_eq!(
                    run(&string_args(
                        Some(&input),
                        Some("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS"),
                        ansi
                    ))?,
                    Some(0)
                );
            }
        }
        assert_eq!(
            run(&string_args(
                Some("1970-01-01x00:00:01"),
                Some("yyyy-MM-dd'T'HH:mm:ss"),
                false
            ))?,
            None
        );
        Ok(())
    }

    #[test]
    fn nulls_and_arithmetic_overflow_are_distinct() -> Result<()> {
        for ansi in [false, true] {
            assert_eq!(run(&string_args(None, Some("yyyy-MM-dd"), ansi))?, None);
            assert_eq!(run(&string_args(Some("invalid"), None, ansi))?, None);
            assert!(
                run(&string_args(
                    Some("+300000-01-01"),
                    Some("yyyy-MM-dd"),
                    ansi
                ))
                .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn arrays_keep_nulls_slices_and_empty_inputs() -> Result<()> {
        let input = StringArray::from(vec![
            Some("discard"),
            Some("1970-01-01"),
            None,
            Some("bad"),
            Some("1969-12-31"),
        ]);
        let mut args = string_args(None, Some("yyyy-MM-dd"), false);
        args[0] = ColumnarValue::Array(Arc::new(input.slice(1, 4)));
        let result = spark_to_unix_timestamp(&args)?.into_array(4)?;
        assert_eq!(
            as_primitive_array::<Int64Type>(&result),
            &Int64Array::from(vec![Some(0), None, None, Some(-86_400)])
        );
        let input = Arc::new(Date32Array::from(Vec::<Option<i32>>::new()));
        let result = spark_to_unix_timestamp(&[
            ColumnarValue::Array(input),
            scalar(ScalarValue::Int32(Some(0))),
        ])?
        .into_array(0)?;
        assert!(result.is_empty());
        let input =
            TimestampMicrosecondArray::from(vec![Some(8_000_000), None, Some(-1)]).slice(1, 2);
        let result =
            spark_to_unix_timestamp(&[ColumnarValue::Array(Arc::new(input))])?.into_array(2)?;
        assert_eq!(
            as_primitive_array::<Int64Type>(&result),
            &Int64Array::from(vec![None, Some(0)])
        );
        Ok(())
    }

    #[test]
    fn invalid_contracts_are_errors() {
        assert!(spark_to_unix_timestamp(&[]).is_err());
        assert!(run(&[scalar(ScalarValue::Int64(Some(0)))]).is_err());
        assert!(
            run(&[
                scalar(ScalarValue::Date32(Some(0))),
                scalar(ScalarValue::Int32(Some(64_801)))
            ])
            .is_err()
        );
        assert!(run(&string_args(Some("1970-01-01"), Some("MM/dd/yyyy"), false)).is_err());
        let mut args = string_args(Some("1970-01-01"), Some("yyyy-MM-dd"), false);
        args[3] = scalar(ScalarValue::Boolean(None));
        assert!(run(&args).is_err());
    }
}
