//! Shared value contract primitives and Arrow metadata constants.
//!
//! This module is the single source of truth for the Arrow field metadata keys
//! and canonical values described by `spec/values/contract.md`. It also holds
//! the lightweight, dependency-free temporal, duration, and spatial parts that
//! the typed parameter and result representations share, so conversion rules
//! are written once instead of once per engine.
//!
//! The primitives deliberately do not depend on Arrow, a protocol codec, or a
//! database driver. They only describe the value: calendar parts, an instant,
//! a timezone, a duration, or a coordinate. Codecs attach them to wire formats.

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
