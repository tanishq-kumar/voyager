//! Shared value contract primitives, Arrow metadata constants, parameter IR,
//! result IR, and unified Arrow RecordBatch builder.
//!
//! This module is the single source of truth for the Arrow field metadata keys
//! and canonical values described by `spec/values/contract.md`. It also holds
//! the lightweight, dependency-free temporal, duration, and spatial parts and
//! calendar/ISO formatters shared by [`param::ParamValue`], [`result::ResultValue`],
//! and [`arrow::values_to_record_batch`].

pub mod arrow;
pub mod param;
pub mod result;

pub use arrow::{
    ColumnHint, batch_needs_raw_rows, values_to_record_batch, values_to_record_batch_with_hints,
};
pub use param::{ParamTimezone, ParamValue};
pub use result::{
    ResultDateTime, ResultMap, ResultNode, ResultPath, ResultRelationship, ResultStructure,
    ResultTimezone, ResultTypedText, ResultUnboundRelationship, ResultValue,
};

/// Arrow field metadata key carrying the canonical Voyager value type.
pub const VOYAGER_META_TYPE: &str = "voyager.type";
/// Arrow field metadata key carrying the canonical Voyager encoding.
pub const VOYAGER_META_ENCODING: &str = "voyager.encoding";
/// Arrow field metadata key carrying the Voyager graph entity kind.
pub const VOYAGER_META_KIND: &str = "voyager.kind";
/// Arrow field metadata key naming an Arrow extension type.
pub const ARROW_EXTENSION_NAME: &str = "ARROW:extension:name";
/// Canonical [`ARROW_EXTENSION_NAME`] value for UUID extension fields.
pub const ARROW_UUID_EXTENSION: &str = "arrow.uuid";

/// Canonical [`VOYAGER_META_TYPE`] value for calendar dates.
pub const META_TYPE_DATE: &str = "date";
/// Canonical [`VOYAGER_META_TYPE`] value for naive wall-clock times.
pub const META_TYPE_TIME: &str = "time";
/// Canonical [`VOYAGER_META_TYPE`] value for times with a UTC offset.
pub const META_TYPE_TIME_TZ: &str = "time_tz";
/// Canonical [`VOYAGER_META_TYPE`] value for timezone-naive datetimes.
pub const META_TYPE_LOCAL_DATETIME: &str = "local_datetime";
/// Canonical [`VOYAGER_META_TYPE`] value for timezone-aware datetime text.
pub const META_TYPE_DATETIME_TZ: &str = "datetime_tz";
/// Canonical [`VOYAGER_META_TYPE`] value for calendar/clock durations.
pub const META_TYPE_DURATION: &str = "duration";
/// Canonical [`VOYAGER_META_TYPE`] value for decimal text fallbacks.
pub const META_TYPE_DECIMAL: &str = "decimal";
/// Canonical [`VOYAGER_META_TYPE`] value for UUID text fallbacks.
pub const META_TYPE_UUID: &str = "uuid";
/// Canonical [`VOYAGER_META_TYPE`] value for integers beyond `Int64`.
pub const META_TYPE_HUGEINT: &str = "hugeint";
/// Canonical [`VOYAGER_META_TYPE`] value for spatial points.
pub const META_TYPE_POINT: &str = "point";
/// Canonical [`VOYAGER_META_TYPE`] value for JSON-encoded values.
pub const META_TYPE_JSON: &str = "json";
/// Canonical [`VOYAGER_META_ENCODING`] value for JSON text columns.
pub const META_ENCODING_JSON: &str = "json";
/// Canonical [`VOYAGER_META_KIND`] value for graph nodes.
pub const META_KIND_NODE: &str = "node";
/// Canonical [`VOYAGER_META_KIND`] value for graph relationships.
pub const META_KIND_RELATIONSHIP: &str = "relationship";
/// Canonical [`VOYAGER_META_KIND`] value for graph paths.
pub const META_KIND_PATH: &str = "path";

/// Calendar date without a time or timezone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DateParts {
    /// Calendar year (proleptic Gregorian).
    pub year: i32,
    /// Month of year (`1..=12`).
    pub month: u8,
    /// Day of month (`1..=31`).
    pub day: u8,
}

/// Wall-clock time of day without a date or timezone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimeParts {
    /// Hour of day (`0..=23`).
    pub hour: u8,
    /// Minute of hour (`0..=59`).
    pub minute: u8,
    /// Second of minute (`0..=59`, or `60` for leap seconds).
    pub second: u8,
    /// Sub-second nanoseconds (`0..=999_999_999`).
    pub nanosecond: u32,
}

/// Calendar date and wall-clock time without a timezone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LocalDateTimeParts {
    /// Calendar date.
    pub date: DateParts,
    /// Wall-clock time of day.
    pub time: TimeParts,
}

/// Timezone attachment for an instant.
///
/// A fixed offset is enough to reconstruct an aware Python `datetime`. A named
/// zone additionally keeps the IANA identifier and the offset resolved at the
/// represented instant, so DST transitions and `fold=1` instants survive.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Timezone {
    /// Fixed offset from UTC in seconds (`+02:00` -> `7200`, `UTC` -> `0`).
    FixedOffset {
        /// UTC offset in seconds.
        offset_seconds: i32,
    },
    /// Named IANA timezone with its resolved UTC offset at the instant.
    Named {
        /// IANA timezone identifier (for example `"Europe/Berlin"`).
        id: String,
        /// UTC offset in seconds at the represented instant.
        offset_seconds: i32,
    },
}

impl Timezone {
    /// Returns the UTC offset in seconds at the represented instant.
    pub fn offset_seconds(&self) -> i32 {
        match self {
            Self::FixedOffset { offset_seconds } => *offset_seconds,
            Self::Named { offset_seconds, .. } => *offset_seconds,
        }
    }
}

/// Timezone-aware instant split into an epoch timestamp and a timezone.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DateTimeParts {
    /// Seconds since the Unix epoch (`1970-01-01T00:00:00Z`).
    pub utc_seconds: i64,
    /// Sub-second nanoseconds (`0..=999_999_999`).
    pub nanosecond: u32,
    /// Attached fixed offset or named IANA timezone.
    pub timezone: Timezone,
}

/// Calendar and clock duration components kept separate.
///
/// Months cannot be folded into days without a reference date, so all four
/// components are preserved from the wire representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DurationParts {
    /// Calendar months.
    pub months: i64,
    /// Signed days component.
    pub days: i64,
    /// Signed seconds component.
    pub seconds: i64,
    /// Signed sub-second nanoseconds component.
    pub nanoseconds: i64,
}

/// Two-dimensional spatial point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point2D {
    /// Spatial reference system identifier.
    pub srid: i64,
    /// X coordinate (longitude for geographic points).
    pub x: f64,
    /// Y coordinate (latitude for geographic points).
    pub y: f64,
}

/// Three-dimensional spatial point.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point3D {
    /// Spatial reference system identifier.
    pub srid: i64,
    /// X coordinate (longitude for geographic points).
    pub x: f64,
    /// Y coordinate (latitude for geographic points).
    pub y: f64,
    /// Z coordinate (height for geographic points).
    pub z: f64,
}

/// Gregorian civil calendar conversion: days since `1970-01-01` to `(year, month, day)`.
///
/// Howard Hinnant's civil day algorithm: O(1) arithmetic, zero allocations.
pub fn days_to_ymd(days: i64) -> (i32, u32, u32) {
    let z = days.saturating_add(719_468);
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

/// Gregorian civil calendar conversion: `(year, month, day)` to days since `1970-01-01`.
pub fn ymd_to_days(year: i32, month: u32, day: u32) -> i64 {
    let y = if month <= 2 {
        year as i64 - 1
    } else {
        year as i64
    };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let m = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe as i64 - 719_468
}

/// Converts nanoseconds since midnight into `(hours, minutes, seconds, sub_second_nanoseconds)`.
pub fn nanos_to_hmsn(nanos: i64) -> (u32, u32, u32, u32) {
    let total_secs = (nanos / 1_000_000_000).rem_euclid(86_400);
    let sub_nanos = (nanos.rem_euclid(1_000_000_000)) as u32;
    let hour = (total_secs / 3_600) as u32;
    let minute = ((total_secs % 3_600) / 60) as u32;
    let second = (total_secs % 60) as u32;
    (hour, minute, second, sub_nanos)
}

/// Formats UTC offset in seconds to ISO-8601 offset string (e.g. `"+02:00"`, `"-05:00"`, `"+00:00"`).
pub fn format_tz_offset(offset_seconds: i64) -> String {
    let sign = if offset_seconds >= 0 { '+' } else { '-' };
    let abs_secs = offset_seconds.unsigned_abs();
    let hours = abs_secs / 3_600;
    let minutes = (abs_secs % 3_600) / 60;
    let seconds = abs_secs % 60;
    if seconds == 0 {
        format!("{sign}{hours:02}:{minutes:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    }
}

/// Formats wall-clock time components with minimal fractional precision (ms / us / ns) and optional suffix.
pub(crate) fn format_time_string(
    h: u32,
    m: u32,
    s: u32,
    nanos: u32,
    offset: Option<&str>,
) -> String {
    let base = if nanos == 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else if nanos.is_multiple_of(1_000_000) {
        format!("{h:02}:{m:02}:{s:02}.{:03}", nanos / 1_000_000)
    } else if nanos.is_multiple_of(1_000) {
        format!("{h:02}:{m:02}:{s:02}.{:06}", nanos / 1_000)
    } else {
        format!("{h:02}:{m:02}:{s:02}.{nanos:09}")
    };
    match offset {
        Some(off) => format!("{base}{off}"),
        None => base,
    }
}

/// Formats a proleptic Gregorian calendar date as `"YYYY-MM-DD"`.
pub fn format_iso_date(year: i32, month: u8, day: u8) -> String {
    format!("{year:04}-{month:02}-{day:02}")
}

/// Formats a wall-clock time as `"HH:MM:SS[.ffffff][+HH:MM]"`.
pub fn format_iso_time(
    hour: u8,
    minute: u8,
    second: u8,
    nanosecond: u32,
    tz_offset_seconds: Option<i32>,
) -> String {
    let mut out = if nanosecond == 0 {
        format!("{hour:02}:{minute:02}:{second:02}")
    } else if nanosecond.is_multiple_of(1_000) {
        format!(
            "{hour:02}:{minute:02}:{second:02}.{:06}",
            nanosecond / 1_000
        )
    } else {
        format!("{hour:02}:{minute:02}:{second:02}.{nanosecond:09}")
    };
    if let Some(off) = tz_offset_seconds {
        out.push_str(&format_tz_offset(off as i64));
    }
    out
}

/// Formats a UTC instant as `"YYYY-MM-DDTHH:MM:SS[.ffffff]+HH:MM"` using `offset_seconds`
/// (or `+00:00` when `None`).
pub fn format_datetime_iso(
    utc_seconds: i64,
    nanosecond: u32,
    offset_seconds: Option<i32>,
) -> String {
    let off = offset_seconds.unwrap_or(0);
    let wall_secs = utc_seconds.saturating_add(off as i64);
    let days = wall_secs.div_euclid(86_400);
    let secs_of_day = wall_secs.rem_euclid(86_400);
    let (y, m, d) = days_to_ymd(days);
    let hour = (secs_of_day / 3_600) as u8;
    let minute = ((secs_of_day % 3_600) / 60) as u8;
    let second = (secs_of_day % 60) as u8;
    format!(
        "{}T{}",
        format_iso_date(y, m as u8, d as u8),
        format_iso_time(hour, minute, second, nanosecond, Some(off))
    )
}

/// Parses a decimal text representation into an IEEE-754 `f64`, accepting finite decimals
/// as well as `NaN`, `sNaN`, `Infinity`, and `Inf` spellings.
///
/// Note: converting arbitrary-precision decimals to `f64` loses precision beyond
/// 53 bits of significand (~15–17 significant digits).
pub fn parse_decimal_f64(raw: &str) -> Option<f64> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "nan" | "+nan" | "-nan" | "snan" | "+snan" | "-snan" => Some(f64::NAN),
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => trimmed.parse::<f64>().ok(),
    }
}

/// Formats a duration as an ISO-8601 duration string matching Python's `Duration.to_iso()`.
///
/// Normalizes `(seconds, nanoseconds)` carry into one signed sub-day total without
/// folding `days` into `seconds`. When all non-zero components are negative, emits
/// a leading `"-P..."` prefix; when component signs differ, emits per-component signs.
pub fn format_iso_duration(months: i64, days: i64, seconds: i64, nanoseconds: i64) -> String {
    let total_ns = (seconds as i128) * 1_000_000_000 + (nanoseconds as i128);
    if months == 0 && days == 0 && total_ns == 0 {
        return "PT0S".to_string();
    }

    let all_non_pos = months <= 0 && days <= 0 && total_ns <= 0;
    let (m_val, d_val, ns_val, prefix) = if all_non_pos {
        (
            months.unsigned_abs() as i128,
            days.unsigned_abs() as i128,
            total_ns.unsigned_abs() as i128,
            "-P",
        )
    } else {
        (months as i128, days as i128, total_ns, "P")
    };

    let mut out = String::from(prefix);
    if m_val != 0 {
        let years = m_val / 12;
        let rem_months = m_val % 12;
        if years != 0 {
            out.push_str(&format!("{years}Y"));
        }
        if rem_months != 0 {
            out.push_str(&format!("{rem_months}M"));
        }
    }
    if d_val != 0 {
        out.push_str(&format!("{d_val}D"));
    }
    if ns_val != 0 {
        out.push('T');
        let t_sign = if ns_val < 0 { "-" } else { "" };
        let abs_ns = ns_val.unsigned_abs();
        let total_secs = abs_ns / 1_000_000_000;
        let sub_ns = (abs_ns % 1_000_000_000) as u32;
        let h = total_secs / 3_600;
        let m = (total_secs % 3_600) / 60;
        let s = total_secs % 60;
        if h > 0 {
            out.push_str(&format!("{t_sign}{h}H"));
        }
        if m > 0 {
            out.push_str(&format!("{t_sign}{m}M"));
        }
        if s > 0 || sub_ns > 0 {
            if sub_ns > 0 {
                let mut frac = format!("{sub_ns:09}");
                while frac.ends_with('0') {
                    frac.pop();
                }
                out.push_str(&format!("{t_sign}{s}.{frac}S"));
            } else {
                out.push_str(&format!("{t_sign}{s}S"));
            }
        }
    }
    out
}

/// Formats a 16-byte UUID as canonical lowercase hyphenated text (`8-4-4-4-12`).
pub fn format_uuid_bytes(bytes: &[u8; 16]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// The metadata registry in `spec/values/contract.md` is the normative
    /// source for these names. Keep both sides in lockstep: a renamed constant
    /// or a changed canonical string must update the contract in the same change.
    #[test]
    fn metadata_constants_match_contract() {
        let contract_path =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../spec/values/contract.md");
        let contract = std::fs::read_to_string(&contract_path)
            .unwrap_or_else(|error| panic!("failed to read {}: {error}", contract_path.display()));

        let registry = [
            ("VOYAGER_META_TYPE", VOYAGER_META_TYPE),
            ("VOYAGER_META_ENCODING", VOYAGER_META_ENCODING),
            ("VOYAGER_META_KIND", VOYAGER_META_KIND),
            ("ARROW_EXTENSION_NAME", ARROW_EXTENSION_NAME),
            ("ARROW_UUID_EXTENSION", ARROW_UUID_EXTENSION),
            ("META_TYPE_DATE", META_TYPE_DATE),
            ("META_TYPE_TIME", META_TYPE_TIME),
            ("META_TYPE_TIME_TZ", META_TYPE_TIME_TZ),
            ("META_TYPE_LOCAL_DATETIME", META_TYPE_LOCAL_DATETIME),
            ("META_TYPE_DATETIME_TZ", META_TYPE_DATETIME_TZ),
            ("META_TYPE_DURATION", META_TYPE_DURATION),
            ("META_TYPE_DECIMAL", META_TYPE_DECIMAL),
            ("META_TYPE_UUID", META_TYPE_UUID),
            ("META_TYPE_HUGEINT", META_TYPE_HUGEINT),
            ("META_TYPE_POINT", META_TYPE_POINT),
            ("META_TYPE_JSON", META_TYPE_JSON),
            ("META_ENCODING_JSON", META_ENCODING_JSON),
            ("META_KIND_NODE", META_KIND_NODE),
            ("META_KIND_RELATIONSHIP", META_KIND_RELATIONSHIP),
            ("META_KIND_PATH", META_KIND_PATH),
        ];

        for (name, value) in registry {
            let row = format!("| `{name}` | `{value}` |");
            assert!(
                contract.contains(&row),
                "spec/values/contract.md is missing the registry row {row}"
            );
        }

        let default_zone = Timezone::FixedOffset { offset_seconds: 0 };
        assert_eq!(default_zone.offset_seconds(), 0);
    }
}
