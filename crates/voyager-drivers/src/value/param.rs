//! Typed query parameter values preserved from callers into each native database driver.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

pub use crate::value::{
    format_datetime_iso, format_iso_date, format_iso_duration, format_iso_time, parse_decimal_f64,
};

/// Timezone attachment for a [`ParamValue::DateTime`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ParamTimezone {
    /// Fixed offset from UTC in seconds (for example `+02:00` -> `7200`, `UTC` -> `0`).
    OffsetSeconds(i32),
    /// Named IANA timezone together with its resolved UTC offset at the represented instant.
    ZoneId {
        /// IANA timezone identifier (for example `"Europe/Berlin"`).
        id: String,
        /// Resolved UTC offset in seconds at the represented instant.
        offset_seconds: i32,
    },
}

impl ParamTimezone {
    /// Returns the UTC offset in seconds at the represented instant.
    pub fn offset_seconds(&self) -> i32 {
        match self {
            Self::OffsetSeconds(off) => *off,
            Self::ZoneId { offset_seconds, .. } => *offset_seconds,
        }
    }
}

/// Typed query parameter value preserving temporal, binary, UUID, and decimal types
/// across the FFI boundary and into every native database driver.
///
/// Genuine strings stay `ParamValue::Json(serde_json::Value::String(_))` and are never
/// reinterpreted as dates, UUIDs, or numbers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ParamValue {
    /// Plain JSON scalar or container value (`null`, `bool`, `int`/`float`, `str`, JSON list/map).
    Json(serde_json::Value),
    /// Explicit JSON value (`voyager_ogm.Json(...)`): PostgreSQL binds it as `jsonb`
    /// regardless of the parameter's SQL context, and other drivers pass the inner
    /// value through unchanged.
    Jsonb(Box<ParamValue>),
    /// 64-bit IEEE-754 floating-point value (including `NaN`, `Infinity`, `-Infinity`).
    Float(f64),
    /// Raw binary byte buffer (`bytes` / `bytearray`).
    Bytes(Vec<u8>),
    /// Calendar date without time or timezone (`datetime.date`).
    Date {
        /// Calendar year (proleptic Gregorian).
        year: i32,
        /// Month of year (`1..=12`).
        month: u8,
        /// Day of month (`1..=31`).
        day: u8,
    },
    /// Wall-clock time of day (`datetime.time`), optionally with a fixed UTC offset.
    Time {
        /// Hour of day (`0..=23`).
        hour: u8,
        /// Minute of hour (`0..=59`).
        minute: u8,
        /// Second of minute (`0..=59`, or `60` for leap seconds).
        second: u8,
        /// Sub-second nanoseconds (`0..=999_999_999`).
        nanosecond: u32,
        /// Optional fixed UTC offset in seconds (`Some` -> offset time, `None` -> local time).
        tz_offset_seconds: Option<i32>,
    },
    /// Naive date and time without timezone (`datetime.datetime` with `tzinfo = None`).
    LocalDateTime {
        /// Calendar year.
        year: i32,
        /// Month of year (`1..=12`).
        month: u8,
        /// Day of month (`1..=31`).
        day: u8,
        /// Hour of day (`0..=23`).
        hour: u8,
        /// Minute of hour (`0..=59`).
        minute: u8,
        /// Second of minute (`0..=59`, or `60` for leap seconds).
        second: u8,
        /// Sub-second nanoseconds (`0..=999_999_999`).
        nanosecond: u32,
    },
    /// Timezone-aware date and time (`datetime.datetime` with `tzinfo != None`).
    DateTime {
        /// Seconds since Unix epoch (`1970-01-01T00:00:00Z`).
        utc_seconds: i64,
        /// Sub-second nanoseconds (`0..=999_999_999`).
        nanosecond: u32,
        /// Attached fixed UTC offset or named IANA timezone.
        tz: ParamTimezone,
    },
    /// Calendar/clock duration (`datetime.timedelta`).
    Duration {
        /// Calendar months (`0` for Python `timedelta`).
        months: i64,
        /// Signed days component.
        days: i64,
        /// Seconds component.
        seconds: i64,
        /// Sub-second nanoseconds component.
        nanoseconds: i64,
    },
    /// Canonical lowercase hyphenated UUID string (`uuid.UUID`).
    Uuid(String),
    /// Exact decimal text representation (`decimal.Decimal`).
    Decimal(String),
    /// 2D spatial point (`srid`, `x`, `y`).
    Point2D {
        /// Spatial reference system identifier.
        srid: i64,
        /// X coordinate (longitude for geographic points).
        x: f64,
        /// Y coordinate (latitude for geographic points).
        y: f64,
    },
    /// 3D spatial point (`srid`, `x`, `y`, `z`).
    Point3D {
        /// Spatial reference system identifier.
        srid: i64,
        /// X coordinate (longitude for geographic points).
        x: f64,
        /// Y coordinate (latitude for geographic points).
        y: f64,
        /// Z coordinate (height for geographic points).
        z: f64,
    },
    /// Ordered list of typed parameter values.
    List(Vec<ParamValue>),
    /// Key-value map of typed parameter values.
    Map(HashMap<String, ParamValue>),
}

impl ParamValue {
    /// Returns a `serde_json::Value` representation for JSON-only targets (such as Apache AGE `cypher()` maps).
    pub fn to_json_value(&self) -> serde_json::Value {
        match self {
            Self::Json(v) => v.clone(),
            Self::Jsonb(inner) => inner.to_json_value(),
            Self::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Self::Bytes(b) => serde_json::Value::Array(
                b.iter()
                    .map(|byte| serde_json::Value::Number((*byte).into()))
                    .collect(),
            ),
            Self::Date { year, month, day } => {
                serde_json::Value::String(format_iso_date(*year, *month, *day))
            }
            Self::Time {
                hour,
                minute,
                second,
                nanosecond,
                tz_offset_seconds,
            } => serde_json::Value::String(format_iso_time(
                *hour,
                *minute,
                *second,
                *nanosecond,
                *tz_offset_seconds,
            )),
            Self::LocalDateTime {
                year,
                month,
                day,
                hour,
                minute,
                second,
                nanosecond,
            } => serde_json::Value::String(format!(
                "{}T{}",
                format_iso_date(*year, *month, *day),
                format_iso_time(*hour, *minute, *second, *nanosecond, None)
            )),
            Self::DateTime {
                utc_seconds,
                nanosecond,
                tz,
            } => serde_json::Value::String(format_datetime_iso(
                *utc_seconds,
                *nanosecond,
                Some(tz.offset_seconds()),
            )),
            Self::Duration {
                months,
                days,
                seconds,
                nanoseconds,
            } => serde_json::Value::String(format_iso_duration(
                *months,
                *days,
                *seconds,
                *nanoseconds,
            )),
            Self::Uuid(s) => serde_json::Value::String(s.clone()),
            Self::Decimal(s) => {
                if let Some(f) = parse_decimal_f64(s)
                    && let Some(n) = serde_json::Number::from_f64(f)
                {
                    return serde_json::Value::Number(n);
                }
                serde_json::Value::Null
            }
            Self::Point2D { srid, x, y } => {
                let mut obj = serde_json::Map::with_capacity(3);
                obj.insert(
                    "srid".to_string(),
                    serde_json::Value::Number((*srid).into()),
                );
                obj.insert(
                    "x".to_string(),
                    serde_json::Number::from_f64(*x)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                );
                obj.insert(
                    "y".to_string(),
                    serde_json::Number::from_f64(*y)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                );
                serde_json::Value::Object(obj)
            }
            Self::Point3D { srid, x, y, z } => {
                let mut obj = serde_json::Map::with_capacity(4);
                obj.insert(
                    "srid".to_string(),
                    serde_json::Value::Number((*srid).into()),
                );
                obj.insert(
                    "x".to_string(),
                    serde_json::Number::from_f64(*x)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                );
                obj.insert(
                    "y".to_string(),
                    serde_json::Number::from_f64(*y)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                );
                obj.insert(
                    "z".to_string(),
                    serde_json::Number::from_f64(*z)
                        .map(serde_json::Value::Number)
                        .unwrap_or(serde_json::Value::Null),
                );
                serde_json::Value::Object(obj)
            }
            Self::List(items) => {
                serde_json::Value::Array(items.iter().map(Self::to_json_value).collect())
            }
            Self::Map(map) => {
                let sorted: BTreeMap<&str, &ParamValue> =
                    map.iter().map(|(k, v)| (k.as_str(), v)).collect();
                let mut obj = serde_json::Map::with_capacity(sorted.len());
                for (k, v) in sorted {
                    obj.insert(k.to_string(), v.to_json_value());
                }
                serde_json::Value::Object(obj)
            }
        }
    }
}

impl From<serde_json::Value> for ParamValue {
    fn from(v: serde_json::Value) -> Self {
        Self::Json(v)
    }
}

impl From<&serde_json::Value> for ParamValue {
    fn from(v: &serde_json::Value) -> Self {
        Self::Json(v.clone())
    }
}

impl From<bool> for ParamValue {
    fn from(b: bool) -> Self {
        Self::Json(serde_json::Value::Bool(b))
    }
}

impl From<i64> for ParamValue {
    fn from(i: i64) -> Self {
        Self::Json(serde_json::Value::Number(i.into()))
    }
}

impl From<i32> for ParamValue {
    fn from(i: i32) -> Self {
        Self::Json(serde_json::Value::Number(i.into()))
    }
}

impl From<f64> for ParamValue {
    fn from(f: f64) -> Self {
        match serde_json::Number::from_f64(f) {
            Some(n) => Self::Json(serde_json::Value::Number(n)),
            None => Self::Float(f),
        }
    }
}

impl From<String> for ParamValue {
    fn from(s: String) -> Self {
        Self::Json(serde_json::Value::String(s))
    }
}

impl From<&str> for ParamValue {
    fn from(s: &str) -> Self {
        Self::Json(serde_json::Value::String(s.to_string()))
    }
}

impl From<Vec<u8>> for ParamValue {
    fn from(b: Vec<u8>) -> Self {
        Self::Bytes(b)
    }
}
