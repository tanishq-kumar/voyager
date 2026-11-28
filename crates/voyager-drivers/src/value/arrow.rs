//! Single unified recursive Arrow [`RecordBatch`] builder for [`ResultValue`] rows.
//!
//! Used by Bolt (Neo4j, Memgraph), Redis/FalkorDB, PostgreSQL, and Apache AGE.
//! Propagates `voyager.*` and `ARROW:extension:name` metadata recursively onto
//! child fields of `List`, `LargeList`, `FixedSizeList`, `Map`, `Struct`, and `Union`,
//! using only the metadata constants from [`crate::value`].

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, DurationMicrosecondBuilder,
    DurationNanosecondBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    LargeBinaryBuilder, StringBuilder, StructBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
};
use arrow_array::timezone::Tz;
use arrow_array::{ArrayRef, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

use crate::error::{NetError, Result};
use crate::value::result::{ResultDateTime, ResultTimezone, ResultValue};
use crate::value::{
    ARROW_EXTENSION_NAME, ARROW_UUID_EXTENSION, DurationParts, META_ENCODING_JSON, META_KIND_NODE,
    META_KIND_PATH, META_KIND_RELATIONSHIP, META_TYPE_DATE, META_TYPE_DATETIME_TZ,
    META_TYPE_DECIMAL, META_TYPE_DURATION, META_TYPE_JSON, META_TYPE_LOCAL_DATETIME,
    META_TYPE_POINT, META_TYPE_TIME, META_TYPE_TIME_TZ, META_TYPE_UUID, VOYAGER_META_ENCODING,
    VOYAGER_META_KIND, VOYAGER_META_TYPE,
};

/// Declared Arrow column type used when a typed backend column (such as PostgreSQL)
/// has 0 rows or all-`NULL` values and must still preserve its schema type and metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyColumnKind {
    /// `Boolean`.
    Bool,
    /// `Int64`.
    Int,
    /// `Float64`.
    Float,
    /// `Binary`.
    Bytes,
    /// `Utf8`.
    Str,
    /// `FixedSizeBinary(16)` with `ARROW:extension:name=arrow.uuid` and `voyager.type=uuid`.
    Uuid,
    /// `Date32`.
    Date,
    /// `Time64(Microsecond)`.
    TimeUs,
    /// `Utf8` with `voyager.type=time_tz`.
    TimeTz,
    /// `Timestamp(Microsecond, None)`.
    TimestampUs,
    /// `Timestamp(Microsecond, Some("UTC"))`.
    TimestampTzUtc,
    /// `Duration(Microsecond)`.
    DurationUs,
    /// `Utf8` with `voyager.encoding=json` and `voyager.type=json`.
    Json,
}

/// Optional per-column schema hint passed by typed backends (PostgreSQL, Apache AGE)
/// into [`values_to_record_batch_with_hints`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ColumnHint {
    /// Infer column type dynamically from [`ResultValue`] rows (Bolt, FalkorDB).
    #[default]
    Auto,
    /// Apache AGE `agtype` column: mixed `Int` + `Float` promotes to `Float64`, and
    /// lists/maps/entities serialize as canonical JSON (`voyager.encoding=json`).
    Age,
    /// PostgreSQL `NUMERIC` / `DECIMAL` column with optional `typmod` scale.
    PgDecimal {
        /// PostgreSQL `RowDescription` type modifier (`-1` when unconstrained).
        typmod: Option<i32>,
    },
    /// PostgreSQL `TIME` column (microsecond `Time64(Microsecond)` resolution).
    PgLocalTimeUs,
    /// PostgreSQL typed scalar column whose empty/all-NULL Arrow type is known.
    PgEmptyTyped(EmptyColumnKind),
    /// PostgreSQL fixed-depth array column (`depth >= 1`) with an element hint.
    PgArray {
        /// Array nesting depth (`1` for 1-D, `2` for 2-D, etc.).
        depth: usize,
        /// Hint applied to leaf elements.
        elem_hint: Box<ColumnHint>,
    },
    /// Explicit JSON text column (`voyager.encoding=json`, plus optional `voyager.type`).
    JsonColumn {
        /// Optional `voyager.type` metadata value (for example `META_TYPE_JSON`,
        /// `META_TYPE_DECIMAL`, or `META_TYPE_UUID`).
        meta_type: Option<&'static str>,
    },
    /// PostgreSQL mixed-depth array column: retained as lossless nested
    /// [`ResultValue::List`](crate::value::result::ResultValue::List) in row mode,
    /// and serialized to `Utf8` with `voyager.encoding=json` (plus optional `voyager.type`)
    /// when Arrow is materialized.
    PgMixedArray {
        /// Optional `voyager.type` metadata value (`META_TYPE_DECIMAL` or `META_TYPE_UUID`).
        meta_type: Option<&'static str>,
    },
}

/// Returns `true` when `batch` contains any JSON-encoded column that relies on
/// raw rows for lossless Python conversion.
pub fn batch_needs_raw_rows(batch: &RecordBatch) -> bool {
    batch.schema().fields().iter().any(|field| {
        let meta = field.metadata();
        meta.get(VOYAGER_META_ENCODING)
            .is_some_and(|v| v == META_ENCODING_JSON)
            || meta.contains_key(VOYAGER_META_KIND)
    })
}

/// Checks whether `zone_id` can be resolved as an Arrow/Polars `Timestamp` timezone.
///
/// Uses the same `chrono-tz`-backed parser as Arrow rather than the host's
/// `/usr/share/zoneinfo`, so validation works on Windows and hosts without tzdata.
pub(crate) fn is_valid_arrow_tz(zone_id: &str) -> bool {
    zone_id.parse::<Tz>().is_ok()
}

fn local_wall_secs_for_utc(utc_secs: i64, tz: Tz) -> Option<i64> {
    let dt = arrow_array::temporal_conversions::as_datetime_with_timezone::<
        arrow_array::types::TimestampSecondType,
    >(utc_secs, tz)?;
    Some(dt.naive_local().and_utc().timestamp())
}

/// Resolves a legacy Bolt v4 (`0x66`) local wall-clock timestamp and IANA zone ID
/// into `(utc_seconds, offset_seconds)` using Arrow's `chrono-tz` database.
pub(crate) fn resolve_local_datetime_utc(local_seconds: i64, zone_id: &str) -> Option<(i64, i64)> {
    let tz: Tz = zone_id.parse().ok()?;
    let probe_local = local_wall_secs_for_utc(local_seconds, tz)?;
    let initial_offset = probe_local.checked_sub(local_seconds)?;
    let candidate = local_seconds.checked_sub(initial_offset)?;
    let candidate_local = local_wall_secs_for_utc(candidate, tz)?;
    let resolved_utc = if candidate_local == local_seconds {
        // Prefer the earlier (fold=0) instant across DST fall-back overlaps.
        [3_600i64, 1_800]
            .into_iter()
            .filter_map(|step| candidate.checked_sub(step))
            .find(|&earlier| local_wall_secs_for_utc(earlier, tz) == Some(local_seconds))
            .unwrap_or(candidate)
    } else {
        let adj = local_seconds.checked_sub(candidate_local)?;
        let second_try = candidate.checked_add(adj)?;
        if local_wall_secs_for_utc(second_try, tz) == Some(local_seconds) {
            second_try
        } else {
            return None;
        }
    };
    let offset = local_seconds.checked_sub(resolved_utc)?;
    Some((resolved_utc, offset))
}

fn timestamp_micros(seconds: i64, nanoseconds: i64) -> Option<i64> {
    if nanoseconds % 1_000 != 0 {
        return None;
    }
    seconds
        .checked_mul(1_000_000)?
        .checked_add(nanoseconds / 1_000)
}

fn timestamp_nanos(seconds: i64, nanoseconds: i64) -> Option<i64> {
    seconds.checked_mul(1_000_000_000)?.checked_add(nanoseconds)
}

fn duration_micros(dur: &DurationParts) -> Option<i64> {
    if dur.months != 0
        || dur.days != 0
        || dur.seconds < 0
        || dur.nanoseconds < 0
        || dur.nanoseconds % 1_000 != 0
    {
        return None;
    }
    dur.seconds
        .checked_mul(1_000_000)?
        .checked_add(dur.nanoseconds / 1_000)
}

fn duration_nanos(dur: &DurationParts) -> Option<i64> {
    if dur.months != 0 || dur.days != 0 || dur.seconds < 0 || dur.nanoseconds < 0 {
        return None;
    }
    dur.seconds
        .checked_mul(1_000_000_000)?
        .checked_add(dur.nanoseconds)
}

fn duration_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("months", DataType::Int64, false),
        Field::new("days", DataType::Int64, false),
        Field::new("seconds", DataType::Int64, false),
        Field::new("nanoseconds", DataType::Int64, false),
    ])
}

fn make_duration_struct_builder(capacity: usize) -> StructBuilder {
    StructBuilder::new(
        duration_struct_fields(),
        vec![
            Box::new(Int64Builder::with_capacity(capacity)),
            Box::new(Int64Builder::with_capacity(capacity)),
            Box::new(Int64Builder::with_capacity(capacity)),
            Box::new(Int64Builder::with_capacity(capacity)),
        ],
    )
}

fn append_duration_struct(builder: &mut StructBuilder, dur: Option<&DurationParts>) {
    match dur {
        Some(d) => {
            builder
                .field_builder::<Int64Builder>(0)
                .expect("months")
                .append_value(d.months);
            builder
                .field_builder::<Int64Builder>(1)
                .expect("days")
                .append_value(d.days);
            builder
                .field_builder::<Int64Builder>(2)
                .expect("seconds")
                .append_value(d.seconds);
            builder
                .field_builder::<Int64Builder>(3)
                .expect("nanoseconds")
                .append_value(d.nanoseconds);
            builder.append(true);
        }
        None => {
            builder
                .field_builder::<Int64Builder>(0)
                .expect("months")
                .append_null();
            builder
                .field_builder::<Int64Builder>(1)
                .expect("days")
                .append_null();
            builder
                .field_builder::<Int64Builder>(2)
                .expect("seconds")
                .append_null();
            builder
                .field_builder::<Int64Builder>(3)
                .expect("nanoseconds")
                .append_null();
            builder.append(false);
        }
    }
}

fn point_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("srid", DataType::Int64, false),
        Field::new("x", DataType::Float64, false),
        Field::new("y", DataType::Float64, false),
        Field::new("z", DataType::Float64, true),
    ])
}

fn make_point_struct_builder(capacity: usize) -> StructBuilder {
    StructBuilder::new(
        point_struct_fields(),
        vec![
            Box::new(Int64Builder::with_capacity(capacity)),
            Box::new(Float64Builder::with_capacity(capacity)),
            Box::new(Float64Builder::with_capacity(capacity)),
            Box::new(Float64Builder::with_capacity(capacity)),
        ],
    )
}

fn append_point_struct(builder: &mut StructBuilder, pt: Option<(i64, f64, f64, Option<f64>)>) {
    match pt {
        Some((srid, x, y, z)) => {
            builder
                .field_builder::<Int64Builder>(0)
                .expect("srid")
                .append_value(srid);
            builder
                .field_builder::<Float64Builder>(1)
                .expect("x")
                .append_value(x);
            builder
                .field_builder::<Float64Builder>(2)
                .expect("y")
                .append_value(y);
            builder
                .field_builder::<Float64Builder>(3)
                .expect("z")
                .append_option(z);
            builder.append(true);
        }
        None => {
            builder
                .field_builder::<Int64Builder>(0)
                .expect("srid")
                .append_null();
            builder
                .field_builder::<Float64Builder>(1)
                .expect("x")
                .append_null();
            builder
                .field_builder::<Float64Builder>(2)
                .expect("y")
                .append_null();
            builder
                .field_builder::<Float64Builder>(3)
                .expect("z")
                .append_null();
            builder.append(false);
        }
    }
}

fn field_with_meta(name: &str, data_type: DataType, meta: &[(&str, &str)]) -> Field {
    let mut field = Field::new(name, data_type, true);
    if !meta.is_empty() {
        let mut map = HashMap::with_capacity(meta.len());
        for (k, v) in meta {
            map.insert((*k).to_string(), (*v).to_string());
        }
        field = field.with_metadata(map);
    }
    field
}

/// Extracts the declared scale from a PostgreSQL `NUMERIC(p, s)` type modifier (`typmod`).
///
/// PostgreSQL encodes `typmod` as `((precision << 16) | (scale & 0xffff)) + 4`, or `-1`
/// when unconstrained.
fn pg_numeric_typmod_scale(typmod: i32) -> Option<i16> {
    if typmod < 4 {
        return None;
    }
    Some(((typmod - 4) & 0xffff) as i16)
}

/// Attempts to parse a column of decimal strings into Arrow `Decimal128(38, scale)`.
///
/// Returns `None` if any value is non-finite (`NaN`/`Infinity`), overflows 38 digits,
/// or uses a negative scale.
fn try_parse_decimal128_values(
    values: &[Option<&str>],
    typmod: Option<i32>,
) -> Option<(u8, Vec<Option<i128>>)> {
    struct ParsedDec {
        negative: bool,
        digits: String,
        scale: i32,
    }

    let mut parsed: Vec<Option<ParsedDec>> = Vec::with_capacity(values.len());
    let mut max_scale: i32 = match typmod.and_then(pg_numeric_typmod_scale) {
        Some(s) if s < 0 => return None,
        Some(s) => s as i32,
        None => 0,
    };

    for cell in values {
        let Some(raw) = cell else {
            parsed.push(None);
            continue;
        };
        let s = raw.trim();
        if s.is_empty() {
            return None;
        }
        let lower = s.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "nan"
                | "+nan"
                | "-nan"
                | "snan"
                | "inf"
                | "+inf"
                | "-inf"
                | "infinity"
                | "+infinity"
                | "-infinity"
        ) {
            return None;
        }
        let (negative, rest) = if let Some(r) = s.strip_prefix('-') {
            (true, r)
        } else if let Some(r) = s.strip_prefix('+') {
            (false, r)
        } else {
            (false, s)
        };
        let (mantissa, exp): (&str, i32) = if let Some(idx) = rest.find(['e', 'E']) {
            let e = rest[idx + 1..].parse::<i32>().ok()?;
            (&rest[..idx], e)
        } else {
            (rest, 0)
        };
        let (int_part, frac_part) = if let Some(dot) = mantissa.find('.') {
            (&mantissa[..dot], &mantissa[dot + 1..])
        } else {
            (mantissa, "")
        };
        if (int_part.is_empty() && frac_part.is_empty())
            || !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        let raw_scale = (frac_part.len() as i32).checked_sub(exp)?;
        let mut digits = String::with_capacity(int_part.len() + frac_part.len());
        digits.push_str(int_part);
        digits.push_str(frac_part);
        let scale = if raw_scale < 0 {
            let pad = (-raw_scale) as usize;
            if pad > 38 {
                return None;
            }
            for _ in 0..pad {
                digits.push('0');
            }
            0
        } else {
            raw_scale
        };
        if scale > max_scale {
            max_scale = scale;
        }
        parsed.push(Some(ParsedDec {
            negative,
            digits,
            scale,
        }));
    }

    if max_scale > 38 {
        return None;
    }

    let mut out = Vec::with_capacity(parsed.len());
    for item in parsed {
        let Some(dec) = item else {
            out.push(None);
            continue;
        };
        let pad = (max_scale - dec.scale) as usize;
        let trimmed = dec.digits.trim_start_matches('0');
        let effective_len = trimmed.len() + pad;
        if effective_len > 38 {
            return None;
        }
        let mut acc: i128 = 0;
        for b in trimmed.bytes() {
            acc = acc.checked_mul(10)?.checked_add((b - b'0') as i128)?;
        }
        for _ in 0..pad {
            acc = acc.checked_mul(10)?;
        }
        if dec.negative {
            acc = acc.checked_neg()?;
        }
        out.push(Some(acc));
    }

    Some((max_scale as u8, out))
}

fn tz_label_for_offset(offset_seconds: i32) -> Option<String> {
    if offset_seconds == 0 {
        Some("UTC".to_string())
    } else {
        None
    }
}

#[derive(Clone, PartialEq, Eq)]
enum ScalarKind {
    Int,
    Float,
    IntFloatPromoted,
    Bool,
    Str,
    Bytes,
    Decimal,
    Uuid,
    Json,
    Date,
    LocalTime,
    TimeTz,
    LocalDateTime,
    DateTimeOffset(Option<String>),
    DateTimeZone(Option<String>),
    DateTimeMixed,
    Duration,
    Point,
    TypedText(&'static str),
    Mixed,
}

fn classify_scalar(val: &ResultValue, tz_cache: &mut HashMap<String, bool>) -> Option<ScalarKind> {
    match val {
        ResultValue::Null => None,
        ResultValue::Int(_) => Some(ScalarKind::Int),
        ResultValue::Float(_) => Some(ScalarKind::Float),
        ResultValue::Bool(_) => Some(ScalarKind::Bool),
        ResultValue::Str(_) => Some(ScalarKind::Str),
        ResultValue::Bytes(_) => Some(ScalarKind::Bytes),
        ResultValue::Decimal(_) => Some(ScalarKind::Decimal),
        ResultValue::Uuid(_) => Some(ScalarKind::Uuid),
        ResultValue::Json(_) => Some(ScalarKind::Json),
        ResultValue::Date(_) => Some(ScalarKind::Date),
        ResultValue::LocalTime(_) => Some(ScalarKind::LocalTime),
        ResultValue::Time { .. } => Some(ScalarKind::TimeTz),
        ResultValue::LocalDateTime { .. } => Some(ScalarKind::LocalDateTime),
        ResultValue::DateTime(dt) => match &dt.tz {
            ResultTimezone::Offset(off) => {
                Some(ScalarKind::DateTimeOffset(tz_label_for_offset(*off)))
            }
            ResultTimezone::Zone { id, is_local } => {
                if *is_local {
                    if resolve_local_datetime_utc(dt.seconds, id).is_some() {
                        return Some(ScalarKind::DateTimeZone(Some(id.to_string())));
                    }
                    return Some(ScalarKind::DateTimeZone(None));
                }
                let valid = if let Some(&cached) = tz_cache.get(id.as_ref()) {
                    cached
                } else {
                    let v = is_valid_arrow_tz(id);
                    tz_cache.insert(id.to_string(), v);
                    v
                };
                Some(ScalarKind::DateTimeZone(valid.then(|| id.to_string())))
            }
        },
        ResultValue::Duration(_) => Some(ScalarKind::Duration),
        ResultValue::Point2D(_) | ResultValue::Point3D(_) => Some(ScalarKind::Point),
        ResultValue::TypedText(tt) => Some(ScalarKind::TypedText(tt.meta_type)),
        ResultValue::List(_)
        | ResultValue::Map(_)
        | ResultValue::Node(_)
        | ResultValue::Relationship(_)
        | ResultValue::Path(_)
        | ResultValue::Structure(_) => Some(ScalarKind::Mixed),
    }
}

fn merge_scalar_kind(acc: &mut Option<ScalarKind>, next: ScalarKind, promote_int_float: bool) {
    let Some(cur) = acc.take() else {
        *acc = Some(next);
        return;
    };
    let merged = match (cur, next) {
        (a, b) if a == b => a,
        (ScalarKind::Int | ScalarKind::IntFloatPromoted, ScalarKind::Float)
        | (ScalarKind::Float, ScalarKind::Int | ScalarKind::IntFloatPromoted)
        | (ScalarKind::IntFloatPromoted, ScalarKind::Int)
        | (ScalarKind::Int, ScalarKind::IntFloatPromoted)
            if promote_int_float =>
        {
            ScalarKind::IntFloatPromoted
        }
        (ScalarKind::LocalTime | ScalarKind::TimeTz, ScalarKind::TimeTz)
        | (ScalarKind::TimeTz, ScalarKind::LocalTime) => ScalarKind::TimeTz,
        (ScalarKind::TimeTz, ScalarKind::TypedText(META_TYPE_TIME_TZ))
        | (ScalarKind::TypedText(META_TYPE_TIME_TZ), ScalarKind::TimeTz) => ScalarKind::TimeTz,
        (ScalarKind::DateTimeOffset(a), ScalarKind::DateTimeOffset(b)) => {
            if a.is_some() && a == b {
                ScalarKind::DateTimeOffset(a)
            } else {
                ScalarKind::DateTimeMixed
            }
        }
        (ScalarKind::DateTimeZone(a), ScalarKind::DateTimeZone(b)) => {
            if a.is_some() && a == b {
                ScalarKind::DateTimeZone(a)
            } else {
                ScalarKind::DateTimeMixed
            }
        }
        (
            ScalarKind::LocalDateTime
            | ScalarKind::DateTimeOffset(_)
            | ScalarKind::DateTimeZone(_)
            | ScalarKind::DateTimeMixed
            | ScalarKind::TypedText(META_TYPE_DATETIME_TZ),
            ScalarKind::LocalDateTime
            | ScalarKind::DateTimeOffset(_)
            | ScalarKind::DateTimeZone(_)
            | ScalarKind::DateTimeMixed
            | ScalarKind::TypedText(META_TYPE_DATETIME_TZ),
        ) => ScalarKind::DateTimeMixed,
        // If a column mixes parsed Uuid with unparseable TypedText(uuid), fall back to Utf8 + voyager.type=uuid.
        (ScalarKind::Uuid, ScalarKind::TypedText(META_TYPE_UUID))
        | (ScalarKind::TypedText(META_TYPE_UUID), ScalarKind::Uuid) => {
            ScalarKind::TypedText(META_TYPE_UUID)
        }
        // Similarly for Date, LocalTime, LocalDateTime, Duration mixed with TypedText fallback.
        (ScalarKind::Date, ScalarKind::TypedText(META_TYPE_DATE))
        | (ScalarKind::TypedText(META_TYPE_DATE), ScalarKind::Date) => {
            ScalarKind::TypedText(META_TYPE_DATE)
        }
        (ScalarKind::LocalTime, ScalarKind::TypedText(META_TYPE_TIME))
        | (ScalarKind::TypedText(META_TYPE_TIME), ScalarKind::LocalTime) => {
            ScalarKind::TypedText(META_TYPE_TIME)
        }
        (ScalarKind::LocalDateTime, ScalarKind::TypedText(META_TYPE_LOCAL_DATETIME))
        | (ScalarKind::TypedText(META_TYPE_LOCAL_DATETIME), ScalarKind::LocalDateTime) => {
            ScalarKind::TypedText(META_TYPE_LOCAL_DATETIME)
        }
        (ScalarKind::Duration, ScalarKind::TypedText(META_TYPE_DURATION))
        | (ScalarKind::TypedText(META_TYPE_DURATION), ScalarKind::Duration) => {
            ScalarKind::TypedText(META_TYPE_DURATION)
        }
        _ => ScalarKind::Mixed,
    };
    *acc = Some(merged);
}

fn extract_dt_sn(dt: &ResultDateTime) -> Option<(i64, i64)> {
    match &dt.tz {
        ResultTimezone::Offset(_) => Some((dt.seconds, dt.nanoseconds)),
        ResultTimezone::Zone { id, is_local } => {
            if *is_local {
                let (utc_secs, _) = resolve_local_datetime_utc(dt.seconds, id)?;
                Some((utc_secs, dt.nanoseconds))
            } else {
                Some((dt.seconds, dt.nanoseconds))
            }
        }
    }
}

fn build_datetime_tz_fallback(
    name: &str,
    values: &[Option<&ResultValue>],
) -> Result<(Field, ArrayRef)> {
    let count = values.len();
    let mut builder = StringBuilder::with_capacity(count, count * 32);
    for v in values {
        match v {
            Some(ResultValue::LocalDateTime {
                seconds,
                nanoseconds,
            }) => builder.append_value(ResultValue::format_local_datetime(*seconds, *nanoseconds)),
            Some(ResultValue::DateTime(dt)) => builder.append_value(dt.to_iso_string()),
            Some(ResultValue::TypedText(tt)) => builder.append_value(&*tt.text),
            _ => builder.append_null(),
        }
    }
    Ok((
        field_with_meta(
            name,
            DataType::Utf8,
            &[(VOYAGER_META_TYPE, META_TYPE_DATETIME_TZ)],
        ),
        Arc::new(builder.finish()),
    ))
}

fn build_empty_typed_column(
    name: &str,
    kind: EmptyColumnKind,
    count: usize,
) -> Result<(Field, ArrayRef)> {
    match kind {
        EmptyColumnKind::Bool => {
            let mut b = BooleanBuilder::with_capacity(count);
            b.append_nulls(count);
            Ok((
                Field::new(name, DataType::Boolean, true),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::Int => {
            let mut b = Int64Builder::with_capacity(count);
            b.append_nulls(count);
            Ok((
                Field::new(name, DataType::Int64, true),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::Float => {
            let mut b = Float64Builder::with_capacity(count);
            b.append_nulls(count);
            Ok((
                Field::new(name, DataType::Float64, true),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::Bytes => {
            let mut b = BinaryBuilder::with_capacity(count, 0);
            for _ in 0..count {
                b.append_null();
            }
            Ok((
                Field::new(name, DataType::Binary, true),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::Str => {
            let mut b = StringBuilder::with_capacity(count, 0);
            for _ in 0..count {
                b.append_null();
            }
            Ok((Field::new(name, DataType::Utf8, true), Arc::new(b.finish())))
        }
        EmptyColumnKind::Uuid => {
            let mut b = FixedSizeBinaryBuilder::with_capacity(count, 16);
            for _ in 0..count {
                b.append_null();
            }
            Ok((
                field_with_meta(
                    name,
                    DataType::FixedSizeBinary(16),
                    &[
                        (ARROW_EXTENSION_NAME, ARROW_UUID_EXTENSION),
                        (VOYAGER_META_TYPE, META_TYPE_UUID),
                    ],
                ),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::Date => {
            let mut b = Date32Builder::with_capacity(count);
            b.append_nulls(count);
            Ok((
                Field::new(name, DataType::Date32, true),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::TimeUs => {
            let mut b = Time64MicrosecondBuilder::with_capacity(count);
            b.append_nulls(count);
            Ok((
                Field::new(name, DataType::Time64(TimeUnit::Microsecond), true),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::TimeTz => {
            let mut b = StringBuilder::with_capacity(count, 0);
            for _ in 0..count {
                b.append_null();
            }
            Ok((
                field_with_meta(
                    name,
                    DataType::Utf8,
                    &[(VOYAGER_META_TYPE, META_TYPE_TIME_TZ)],
                ),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::TimestampUs => {
            let mut b = TimestampMicrosecondBuilder::with_capacity(count);
            b.append_nulls(count);
            Ok((
                Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::TimestampTzUtc => {
            let mut b =
                TimestampMicrosecondBuilder::with_capacity(count).with_timezone("UTC".to_string());
            b.append_nulls(count);
            Ok((
                Field::new(
                    name,
                    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                    true,
                ),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::DurationUs => {
            let mut b = DurationMicrosecondBuilder::with_capacity(count);
            b.append_nulls(count);
            Ok((
                Field::new(name, DataType::Duration(TimeUnit::Microsecond), true),
                Arc::new(b.finish()),
            ))
        }
        EmptyColumnKind::Json => {
            let mut b = StringBuilder::with_capacity(count, 0);
            for _ in 0..count {
                b.append_null();
            }
            Ok((
                field_with_meta(
                    name,
                    DataType::Utf8,
                    &[
                        (VOYAGER_META_ENCODING, META_ENCODING_JSON),
                        (VOYAGER_META_TYPE, META_TYPE_JSON),
                    ],
                ),
                Arc::new(b.finish()),
            ))
        }
    }
}

fn build_typed_scalar_column(
    name: &str,
    kind: ScalarKind,
    values: &[Option<&ResultValue>],
    hint: &ColumnHint,
) -> Result<(Field, ArrayRef)> {
    let count = values.len();
    match kind {
        ScalarKind::Int => {
            let mut builder = Int64Builder::with_capacity(count);
            for v in values {
                match v {
                    Some(ResultValue::Int(i)) => builder.append_value(*i),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Int64, true),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::Float | ScalarKind::IntFloatPromoted => {
            let mut builder = Float64Builder::with_capacity(count);
            for v in values {
                match v {
                    Some(ResultValue::Float(f)) => builder.append_value(*f),
                    Some(ResultValue::Int(i)) => builder.append_value(*i as f64),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Float64, true),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::Bool => {
            let mut builder = BooleanBuilder::with_capacity(count);
            for v in values {
                match v {
                    Some(ResultValue::Bool(b)) => builder.append_value(*b),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Boolean, true),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::Str => {
            let mut builder = StringBuilder::with_capacity(count, count * 16);
            for v in values {
                match v {
                    Some(ResultValue::Str(s)) => builder.append_value(&**s),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Utf8, true),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::Bytes => {
            let total_bytes: usize = values
                .iter()
                .filter_map(|v| match v {
                    Some(ResultValue::Bytes(b)) => Some(b.len()),
                    _ => None,
                })
                .sum();
            if total_bytes <= i32::MAX as usize {
                let mut builder = BinaryBuilder::with_capacity(count, total_bytes);
                for v in values {
                    match v {
                        Some(ResultValue::Bytes(b)) => builder.append_value(&**b),
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::Binary, true),
                    Arc::new(builder.finish()),
                ))
            } else {
                let mut builder = LargeBinaryBuilder::with_capacity(count, total_bytes);
                for v in values {
                    match v {
                        Some(ResultValue::Bytes(b)) => builder.append_value(&**b),
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::LargeBinary, true),
                    Arc::new(builder.finish()),
                ))
            }
        }
        ScalarKind::Decimal => {
            let typmod = match hint {
                ColumnHint::PgDecimal { typmod } => *typmod,
                _ => None,
            };
            let raw_cells: Vec<Option<&str>> = values
                .iter()
                .map(|v| match v {
                    Some(ResultValue::Decimal(s)) => Some(&**s),
                    _ => None,
                })
                .collect();
            if let Some((scale, parsed)) = try_parse_decimal128_values(&raw_cells, typmod) {
                let mut builder = Decimal128Builder::with_capacity(count);
                for val in parsed {
                    match val {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                let arr = builder
                    .finish()
                    .with_precision_and_scale(38, scale as i8)
                    .map_err(NetError::ArrowError)?;
                Ok((
                    Field::new(name, DataType::Decimal128(38, scale as i8), true),
                    Arc::new(arr),
                ))
            } else {
                let mut builder = StringBuilder::with_capacity(count, count * 16);
                for cell in raw_cells {
                    match cell {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                Ok((
                    field_with_meta(
                        name,
                        DataType::Utf8,
                        &[(VOYAGER_META_TYPE, META_TYPE_DECIMAL)],
                    ),
                    Arc::new(builder.finish()),
                ))
            }
        }
        ScalarKind::Uuid => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(count, 16);
            for v in values {
                match v {
                    Some(ResultValue::Uuid(bytes)) => {
                        builder.append_value(bytes).map_err(NetError::ArrowError)?;
                    }
                    _ => builder.append_null(),
                }
            }
            Ok((
                field_with_meta(
                    name,
                    DataType::FixedSizeBinary(16),
                    &[
                        (ARROW_EXTENSION_NAME, ARROW_UUID_EXTENSION),
                        (VOYAGER_META_TYPE, META_TYPE_UUID),
                    ],
                ),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::Json => {
            let mut builder = StringBuilder::with_capacity(count, count * 32);
            for v in values {
                match v {
                    Some(ResultValue::Json(raw)) => builder.append_value(&**raw),
                    _ => builder.append_null(),
                }
            }
            Ok((
                field_with_meta(
                    name,
                    DataType::Utf8,
                    &[
                        (VOYAGER_META_ENCODING, META_ENCODING_JSON),
                        (VOYAGER_META_TYPE, META_TYPE_JSON),
                    ],
                ),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::Date => {
            let all_fit_i32 = values.iter().all(|v| match v {
                Some(ResultValue::Date(days)) => i32::try_from(*days).is_ok(),
                _ => true,
            });
            if all_fit_i32 {
                let mut builder = Date32Builder::with_capacity(count);
                for v in values {
                    match v {
                        Some(ResultValue::Date(days)) => builder.append_value(*days as i32),
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::Date32, true),
                    Arc::new(builder.finish()),
                ))
            } else {
                let mut builder = StringBuilder::with_capacity(count, count * 16);
                for v in values {
                    match v {
                        Some(ResultValue::Date(days)) => {
                            builder.append_value(ResultValue::format_date_days(*days));
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    field_with_meta(name, DataType::Utf8, &[(VOYAGER_META_TYPE, META_TYPE_DATE)]),
                    Arc::new(builder.finish()),
                ))
            }
        }
        ScalarKind::LocalTime => {
            let prefer_us = matches!(hint, ColumnHint::PgLocalTimeUs);
            if prefer_us
                && values.iter().all(|v| match v {
                    Some(ResultValue::LocalTime(ns)) => ns % 1_000 == 0,
                    _ => true,
                })
            {
                let mut builder = Time64MicrosecondBuilder::with_capacity(count);
                for v in values {
                    match v {
                        Some(ResultValue::LocalTime(ns)) => builder.append_value(*ns / 1_000),
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::Time64(TimeUnit::Microsecond), true),
                    Arc::new(builder.finish()),
                ))
            } else {
                let mut builder = Time64NanosecondBuilder::with_capacity(count);
                for v in values {
                    match v {
                        Some(ResultValue::LocalTime(ns)) => builder.append_value(*ns),
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::Time64(TimeUnit::Nanosecond), true),
                    Arc::new(builder.finish()),
                ))
            }
        }
        ScalarKind::TimeTz => {
            let mut builder = StringBuilder::with_capacity(count, count * 24);
            for v in values {
                match v {
                    Some(ResultValue::Time {
                        nanoseconds,
                        tz_offset_seconds,
                    }) => builder.append_value(ResultValue::format_time_offset(
                        *nanoseconds,
                        *tz_offset_seconds,
                    )),
                    Some(ResultValue::LocalTime(ns)) => {
                        builder.append_value(ResultValue::format_local_time_nanos(*ns));
                    }
                    Some(ResultValue::TypedText(tt)) => builder.append_value(&*tt.text),
                    _ => builder.append_null(),
                }
            }
            Ok((
                field_with_meta(
                    name,
                    DataType::Utf8,
                    &[(VOYAGER_META_TYPE, META_TYPE_TIME_TZ)],
                ),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::LocalDateTime => {
            let all_us = values.iter().all(|v| match v {
                Some(ResultValue::LocalDateTime {
                    seconds,
                    nanoseconds,
                }) => timestamp_micros(*seconds, *nanoseconds as i64).is_some(),
                _ => true,
            });
            if all_us {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(count);
                for v in values {
                    match v {
                        Some(ResultValue::LocalDateTime {
                            seconds,
                            nanoseconds,
                        }) => builder.append_value(
                            timestamp_micros(*seconds, *nanoseconds as i64).expect("checked"),
                        ),
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true),
                    Arc::new(builder.finish()),
                ))
            } else {
                let all_ns = values.iter().all(|v| match v {
                    Some(ResultValue::LocalDateTime {
                        seconds,
                        nanoseconds,
                    }) => timestamp_nanos(*seconds, *nanoseconds as i64).is_some(),
                    _ => true,
                });
                if all_ns {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(count);
                    for v in values {
                        match v {
                            Some(ResultValue::LocalDateTime {
                                seconds,
                                nanoseconds,
                            }) => builder.append_value(
                                timestamp_nanos(*seconds, *nanoseconds as i64).expect("checked"),
                            ),
                            _ => builder.append_null(),
                        }
                    }
                    Ok((
                        Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true),
                        Arc::new(builder.finish()),
                    ))
                } else {
                    let mut builder = StringBuilder::with_capacity(count, count * 32);
                    for v in values {
                        match v {
                            Some(ResultValue::LocalDateTime {
                                seconds,
                                nanoseconds,
                            }) => builder.append_value(ResultValue::format_local_datetime(
                                *seconds,
                                *nanoseconds,
                            )),
                            _ => builder.append_null(),
                        }
                    }
                    Ok((
                        field_with_meta(
                            name,
                            DataType::Utf8,
                            &[(VOYAGER_META_TYPE, META_TYPE_LOCAL_DATETIME)],
                        ),
                        Arc::new(builder.finish()),
                    ))
                }
            }
        }
        ScalarKind::DateTimeOffset(Some(tz)) | ScalarKind::DateTimeZone(Some(tz)) => {
            let all_us = values.iter().all(|v| match v {
                Some(ResultValue::DateTime(dt)) => extract_dt_sn(dt)
                    .and_then(|(s, n)| timestamp_micros(s, n))
                    .is_some(),
                None => true,
                _ => false,
            });
            if all_us {
                let mut builder =
                    TimestampMicrosecondBuilder::with_capacity(count).with_timezone(tz.clone());
                for v in values {
                    match v {
                        Some(ResultValue::DateTime(dt)) => {
                            let (s, n) = extract_dt_sn(dt).expect("checked");
                            builder.append_value(timestamp_micros(s, n).expect("checked"));
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(
                        name,
                        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from(tz))),
                        true,
                    ),
                    Arc::new(builder.finish()),
                ))
            } else {
                let all_ns = values.iter().all(|v| match v {
                    Some(ResultValue::DateTime(dt)) => extract_dt_sn(dt)
                        .and_then(|(s, n)| timestamp_nanos(s, n))
                        .is_some(),
                    None => true,
                    _ => false,
                });
                if all_ns {
                    let mut builder =
                        TimestampNanosecondBuilder::with_capacity(count).with_timezone(tz.clone());
                    for v in values {
                        match v {
                            Some(ResultValue::DateTime(dt)) => {
                                let (s, n) = extract_dt_sn(dt).expect("checked");
                                builder.append_value(timestamp_nanos(s, n).expect("checked"));
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Ok((
                        Field::new(
                            name,
                            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from(tz))),
                            true,
                        ),
                        Arc::new(builder.finish()),
                    ))
                } else {
                    build_datetime_tz_fallback(name, values)
                }
            }
        }
        ScalarKind::DateTimeOffset(None)
        | ScalarKind::DateTimeZone(None)
        | ScalarKind::DateTimeMixed => build_datetime_tz_fallback(name, values),
        ScalarKind::Duration => {
            let all_us = values.iter().all(|v| match v {
                Some(ResultValue::Duration(d)) => duration_micros(d).is_some(),
                _ => true,
            });
            if all_us {
                let mut builder = DurationMicrosecondBuilder::with_capacity(count);
                for v in values {
                    match v {
                        Some(ResultValue::Duration(d)) => {
                            builder.append_value(duration_micros(d).expect("checked"));
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::Duration(TimeUnit::Microsecond), true),
                    Arc::new(builder.finish()),
                ))
            } else {
                let all_ns = values.iter().all(|v| match v {
                    Some(ResultValue::Duration(d)) => duration_nanos(d).is_some(),
                    _ => true,
                });
                if all_ns {
                    let mut builder = DurationNanosecondBuilder::with_capacity(count);
                    for v in values {
                        match v {
                            Some(ResultValue::Duration(d)) => {
                                builder.append_value(duration_nanos(d).expect("checked"));
                            }
                            _ => builder.append_null(),
                        }
                    }
                    Ok((
                        Field::new(name, DataType::Duration(TimeUnit::Nanosecond), true),
                        Arc::new(builder.finish()),
                    ))
                } else {
                    let mut builder = make_duration_struct_builder(count);
                    for v in values {
                        match v {
                            Some(ResultValue::Duration(d)) => {
                                append_duration_struct(&mut builder, Some(d));
                            }
                            _ => append_duration_struct(&mut builder, None),
                        }
                    }
                    Ok((
                        field_with_meta(
                            name,
                            DataType::Struct(duration_struct_fields()),
                            &[(VOYAGER_META_TYPE, META_TYPE_DURATION)],
                        ),
                        Arc::new(builder.finish()),
                    ))
                }
            }
        }
        ScalarKind::Point => {
            let mut builder = make_point_struct_builder(count);
            for v in values {
                match v {
                    Some(ResultValue::Point2D(pt)) => {
                        append_point_struct(&mut builder, Some((pt.srid, pt.x, pt.y, None)));
                    }
                    Some(ResultValue::Point3D(pt)) => {
                        append_point_struct(&mut builder, Some((pt.srid, pt.x, pt.y, Some(pt.z))));
                    }
                    _ => append_point_struct(&mut builder, None),
                }
            }
            Ok((
                field_with_meta(
                    name,
                    DataType::Struct(point_struct_fields()),
                    &[(VOYAGER_META_TYPE, META_TYPE_POINT)],
                ),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::TypedText(meta_type) => {
            let mut builder = StringBuilder::with_capacity(count, count * 24);
            for v in values {
                match v {
                    Some(ResultValue::TypedText(tt)) => builder.append_value(&*tt.text),
                    Some(ResultValue::Uuid(bytes)) => {
                        builder.append_value(crate::value::format_uuid_bytes(bytes));
                    }
                    Some(ResultValue::Date(days)) => {
                        builder.append_value(ResultValue::format_date_days(*days));
                    }
                    Some(ResultValue::LocalTime(ns)) => {
                        builder.append_value(ResultValue::format_local_time_nanos(*ns));
                    }
                    Some(ResultValue::LocalDateTime {
                        seconds,
                        nanoseconds,
                    }) => {
                        builder.append_value(ResultValue::format_local_datetime(
                            *seconds,
                            *nanoseconds,
                        ));
                    }
                    Some(ResultValue::Duration(d)) => {
                        builder.append_value(crate::value::format_iso_duration(
                            d.months,
                            d.days,
                            d.seconds,
                            d.nanoseconds,
                        ));
                    }
                    _ => builder.append_null(),
                }
            }
            Ok((
                field_with_meta(name, DataType::Utf8, &[(VOYAGER_META_TYPE, meta_type)]),
                Arc::new(builder.finish()),
            ))
        }
        ScalarKind::Mixed => unreachable!("Mixed handled by JSON builder"),
    }
}

/// Wraps a flat child array into an Arrow [`ListArray`] using `lengths`, preserving
/// all metadata on `child_field` recursively.
fn wrap_list_level(
    name: &str,
    child_field: Field,
    child_array: ArrayRef,
    lengths: &[Option<usize>],
) -> Result<(Field, ArrayRef)> {
    let mut offsets: Vec<i32> = Vec::with_capacity(lengths.len() + 1);
    let mut validity = Vec::with_capacity(lengths.len());
    let mut has_null = false;
    let mut offset = 0i32;
    offsets.push(0);

    for length in lengths {
        match length {
            Some(len) => {
                offset = offset.checked_add(*len as i32).ok_or_else(|| {
                    NetError::ProtocolError(
                        "List column exceeds Arrow i32 offset range".to_string(),
                    )
                })?;
                validity.push(true);
            }
            None => {
                has_null = true;
                validity.push(false);
            }
        }
        offsets.push(offset);
    }

    if offset as usize != child_array.len() {
        return Err(NetError::ProtocolError(
            "Inconsistent list offsets when building Arrow ListArray".to_string(),
        ));
    }

    let child_arc = Arc::new(child_field);
    let list_field = Field::new(name, DataType::List(child_arc.clone()), true);
    let nulls = has_null.then(|| NullBuffer::from(validity));
    let list = ListArray::new(
        child_arc,
        OffsetBuffer::new(offsets.into()),
        child_array,
        nulls,
    );
    Ok((list_field, Arc::new(list)))
}

/// Inspects a slice of values to determine if every non-null value is a `List` of uniform
/// nesting depth `>= 1` whose leaves are homogeneous typed scalars (not entities/maps/mixed).
///
/// Returns `Some(depth)` when a native typed nested `List` column can be built, or `None`
/// when the column should fall back to JSON encoding.
fn infer_homogeneous_list_depth(
    values: &[Option<&ResultValue>],
    tz_cache: &mut HashMap<String, bool>,
) -> Option<usize> {
    let mut current: Vec<&ResultValue> = values.iter().filter_map(|v| *v).collect();
    if current.is_empty() {
        return None;
    }

    let mut depth = 0usize;
    loop {
        let all_lists = current.iter().all(|v| matches!(v, ResultValue::List(_)));
        if !all_lists {
            break;
        }
        depth += 1;
        let mut next = Vec::new();
        for v in current {
            if let ResultValue::List(items) = v {
                for item in items.iter() {
                    if !item.is_null() {
                        next.push(item);
                    }
                }
            }
        }
        if next.is_empty() {
            // Lists with no non-null leaf values have no observable element type.
            return None;
        }
        current = next;
    }

    if depth == 0 {
        return None;
    }

    // Verify all non-null leaves are homogeneous typed scalars.
    let mut leaf_kind: Option<ScalarKind> = None;
    for v in &current {
        let k = classify_scalar(v, tz_cache)?;
        merge_scalar_kind(&mut leaf_kind, k, false);
        if matches!(leaf_kind, Some(ScalarKind::Mixed)) {
            return None;
        }
    }
    leaf_kind.filter(|k| *k != ScalarKind::Mixed).map(|_| depth)
}

/// Recursively unrolls `depth` levels of `ResultValue::List`, builds the leaf array with
/// `leaf_hint`, and re-wraps all `depth` levels as nested Arrow `ListArray`s while
/// propagating child-field metadata at every level.
fn build_nested_list_column(
    name: &str,
    values: &[Option<&ResultValue>],
    depth: usize,
    leaf_hint: &ColumnHint,
    tz_cache: &mut HashMap<String, bool>,
) -> Result<(Field, ArrayRef)> {
    let mut level_lengths: Vec<Vec<Option<usize>>> = Vec::with_capacity(depth);
    let mut current: Vec<Option<&ResultValue>> = values.to_vec();

    for _ in 0..depth {
        let mut lengths = Vec::with_capacity(current.len());
        let mut next = Vec::new();
        for cell in current {
            match cell {
                Some(ResultValue::List(items)) => {
                    lengths.push(Some(items.len()));
                    for item in items.iter() {
                        if item.is_null() {
                            next.push(None);
                        } else {
                            next.push(Some(item));
                        }
                    }
                }
                None => lengths.push(None),
                Some(_) => {
                    return Err(NetError::ProtocolError(
                        "Ragged list nesting depth when building Arrow ListArray".to_string(),
                    ));
                }
            }
        }
        level_lengths.push(lengths);
        current = next;
    }

    let (mut child_field, mut child_array) =
        build_column_values("item", &current, leaf_hint, tz_cache)?;

    for level in (0..depth).rev() {
        let field_name = if level == 0 { name } else { "item" };
        let (wrapped_field, wrapped_array) =
            wrap_list_level(field_name, child_field, child_array, &level_lengths[level])?;
        child_field = wrapped_field;
        child_array = wrapped_array;
    }

    Ok((child_field, child_array))
}

fn build_json_column(
    name: &str,
    values: &[Option<&ResultValue>],
    explicit_meta_type: Option<&'static str>,
    node_count: usize,
    rel_count: usize,
    path_count: usize,
    non_null_count: usize,
) -> Result<(Field, ArrayRef)> {
    let count = values.len();
    let mut builder = StringBuilder::with_capacity(count, count * 64);
    let mut json_buf: Vec<u8> = Vec::with_capacity(256);
    for v in values {
        match v {
            None => builder.append_null(),
            Some(ResultValue::Json(raw)) => builder.append_value(&**raw),
            Some(other) => {
                json_buf.clear();
                match serde_json::to_writer(&mut json_buf, &other.as_canonical_json()) {
                    Ok(()) => {
                        let s = std::str::from_utf8(&json_buf).unwrap_or("null");
                        builder.append_value(s);
                    }
                    Err(_) => builder.append_value("null"),
                }
            }
        }
    }
    let mut metadata = HashMap::new();
    metadata.insert(
        VOYAGER_META_ENCODING.to_string(),
        META_ENCODING_JSON.to_string(),
    );
    if let Some(meta_type) = explicit_meta_type {
        metadata.insert(VOYAGER_META_TYPE.to_string(), meta_type.to_string());
    }
    if non_null_count > 0 {
        if node_count == non_null_count {
            metadata.insert(VOYAGER_META_KIND.to_string(), META_KIND_NODE.to_string());
        } else if rel_count == non_null_count {
            metadata.insert(
                VOYAGER_META_KIND.to_string(),
                META_KIND_RELATIONSHIP.to_string(),
            );
        } else if path_count == non_null_count {
            metadata.insert(VOYAGER_META_KIND.to_string(), META_KIND_PATH.to_string());
        }
    }
    Ok((
        Field::new(name, DataType::Utf8, true).with_metadata(metadata),
        Arc::new(builder.finish()),
    ))
}

struct PgMixedArrayJson<'a>(&'a ResultValue);

impl serde::Serialize for PgMixedArrayJson<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        match self.0 {
            ResultValue::Decimal(s) => {
                if let Ok(i) = s.parse::<i64>() {
                    serializer.serialize_i64(i)
                } else if let Ok(f) = s.parse::<f64>() {
                    if f.is_finite() {
                        serializer.serialize_f64(f)
                    } else {
                        serializer.serialize_unit()
                    }
                } else {
                    serializer.serialize_str(s)
                }
            }
            ResultValue::List(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items.iter() {
                    seq.serialize_element(&PgMixedArrayJson(item))?;
                }
                seq.end()
            }
            other => other.as_canonical_json().serialize(serializer),
        }
    }
}

fn build_pg_mixed_array_column(
    name: &str,
    values: &[Option<&ResultValue>],
    explicit_meta_type: Option<&'static str>,
) -> Result<(Field, ArrayRef)> {
    let count = values.len();
    let mut builder = StringBuilder::with_capacity(count, count * 64);
    let mut json_buf: Vec<u8> = Vec::with_capacity(256);
    for v in values {
        match v {
            None => builder.append_null(),
            Some(other) => {
                json_buf.clear();
                match serde_json::to_writer(&mut json_buf, &PgMixedArrayJson(other)) {
                    Ok(()) => {
                        let s = std::str::from_utf8(&json_buf).unwrap_or("null");
                        builder.append_value(s);
                    }
                    Err(_) => builder.append_value("null"),
                }
            }
        }
    }
    let mut metadata = HashMap::new();
    metadata.insert(
        VOYAGER_META_ENCODING.to_string(),
        META_ENCODING_JSON.to_string(),
    );
    if let Some(meta_type) = explicit_meta_type {
        metadata.insert(VOYAGER_META_TYPE.to_string(), meta_type.to_string());
    }
    Ok((
        Field::new(name, DataType::Utf8, true).with_metadata(metadata),
        Arc::new(builder.finish()),
    ))
}

fn build_column_values(
    name: &str,
    values: &[Option<&ResultValue>],
    hint: &ColumnHint,
    tz_cache: &mut HashMap<String, bool>,
) -> Result<(Field, ArrayRef)> {
    let count = values.len();

    // 1. Explicit fixed-depth PG array column (preserves schema even on all-empty / all-NULL).
    if let ColumnHint::PgArray { depth, elem_hint } = hint {
        return build_nested_list_column(name, values, *depth, elem_hint, tz_cache);
    }

    // 2. Explicit PG mixed-depth array column (serialized to JSON only when Arrow is built).
    if let ColumnHint::PgMixedArray { meta_type } = hint {
        return build_pg_mixed_array_column(name, values, *meta_type);
    }

    // 3. Explicit JSON column (PG json/jsonb).
    if let ColumnHint::JsonColumn { meta_type } = hint {
        return build_json_column(name, values, *meta_type, 0, 0, 0, 0);
    }

    // 3. Explicit PG Decimal column (preserves typmod scale even when all values are NULL).
    if let ColumnHint::PgDecimal { .. } = hint {
        return build_typed_scalar_column(name, ScalarKind::Decimal, values, hint);
    }

    // 4. Classify non-null values.
    let promote_int_float = matches!(hint, ColumnHint::Age);
    let force_json_lists = matches!(hint, ColumnHint::Age);

    let mut scalar_kind: Option<ScalarKind> = None;
    let mut node_count = 0usize;
    let mut rel_count = 0usize;
    let mut path_count = 0usize;
    let mut non_null_count = 0usize;
    let mut list_count = 0usize;

    for v in values {
        match v {
            None => {}
            Some(ResultValue::Null) => {}
            Some(ResultValue::List(_)) => {
                list_count += 1;
                non_null_count += 1;
                merge_scalar_kind(&mut scalar_kind, ScalarKind::Mixed, promote_int_float);
            }
            Some(ResultValue::Node(_)) => {
                node_count += 1;
                non_null_count += 1;
                merge_scalar_kind(&mut scalar_kind, ScalarKind::Mixed, promote_int_float);
            }
            Some(ResultValue::Relationship(_)) => {
                rel_count += 1;
                non_null_count += 1;
                merge_scalar_kind(&mut scalar_kind, ScalarKind::Mixed, promote_int_float);
            }
            Some(ResultValue::Path(_)) => {
                path_count += 1;
                non_null_count += 1;
                merge_scalar_kind(&mut scalar_kind, ScalarKind::Mixed, promote_int_float);
            }
            Some(other) => {
                non_null_count += 1;
                if let Some(k) = classify_scalar(other, tz_cache) {
                    merge_scalar_kind(&mut scalar_kind, k, promote_int_float);
                }
            }
        }
    }

    if non_null_count == 0 {
        if let ColumnHint::PgEmptyTyped(empty_kind) = hint {
            return build_empty_typed_column(name, *empty_kind, count);
        }
        if matches!(hint, ColumnHint::PgLocalTimeUs) {
            return build_empty_typed_column(name, EmptyColumnKind::TimeUs, count);
        }
        let mut builder = StringBuilder::with_capacity(count, 0);
        for _ in 0..count {
            builder.append_null();
        }
        return Ok((
            Field::new(name, DataType::Utf8, true),
            Arc::new(builder.finish()),
        ));
    }

    // 5. Homogeneous typed list (including arbitrarily nested multidimensional lists).
    if !force_json_lists
        && list_count == non_null_count
        && let Some(depth) = infer_homogeneous_list_depth(values, tz_cache)
    {
        return build_nested_list_column(name, values, depth, &ColumnHint::Auto, tz_cache);
    }

    // 6. Homogeneous typed scalar column.
    if let Some(kind) = scalar_kind
        && kind != ScalarKind::Mixed
    {
        return build_typed_scalar_column(name, kind, values, hint);
    }

    // 7. Structured / heterogeneous / entity column -> JSON with metadata.
    build_json_column(
        name,
        values,
        None,
        node_count,
        rel_count,
        path_count,
        non_null_count,
    )
}

/// Recursively canonicalizes metadata on a [`Field`] and all nested child fields
/// (`List`, `LargeList`, `FixedSizeList`, `Map`, `Struct`, `Union`).
///
/// Ensures that any `FixedSizeBinary(16)` UUID field tagged with
/// `ARROW:extension:name=arrow.uuid` also carries `voyager.type=uuid` (and vice versa),
/// preserving all existing `voyager.*` and extension metadata at every depth.
pub fn canonicalize_field_metadata(field: &Field) -> Field {
    let mut meta = field.metadata().clone();

    let new_dt = match field.data_type() {
        DataType::List(child) => DataType::List(Arc::new(canonicalize_field_metadata(child))),
        DataType::LargeList(child) => {
            DataType::LargeList(Arc::new(canonicalize_field_metadata(child)))
        }
        DataType::FixedSizeList(child, size) => {
            DataType::FixedSizeList(Arc::new(canonicalize_field_metadata(child)), *size)
        }
        DataType::Map(entries, sorted) => {
            DataType::Map(Arc::new(canonicalize_field_metadata(entries)), *sorted)
        }
        DataType::Struct(children) => DataType::Struct(
            children
                .iter()
                .map(|c| Arc::new(canonicalize_field_metadata(c)))
                .collect(),
        ),
        DataType::Union(union_fields, mode) => {
            let mapped = union_fields
                .iter()
                .map(|(type_id, child)| (type_id, Arc::new(canonicalize_field_metadata(child))))
                .collect();
            DataType::Union(mapped, *mode)
        }
        DataType::FixedSizeBinary(16) => {
            let has_ext = meta
                .get(ARROW_EXTENSION_NAME)
                .is_some_and(|v| v == ARROW_UUID_EXTENSION);
            let has_type = meta
                .get(VOYAGER_META_TYPE)
                .is_some_and(|v| v == META_TYPE_UUID);
            if has_ext && !has_type {
                meta.insert(VOYAGER_META_TYPE.to_string(), META_TYPE_UUID.to_string());
            } else if has_type && !has_ext {
                meta.insert(
                    ARROW_EXTENSION_NAME.to_string(),
                    ARROW_UUID_EXTENSION.to_string(),
                );
            }
            DataType::FixedSizeBinary(16)
        }
        other => other.clone(),
    };

    Field::new(field.name(), new_dt, field.is_nullable()).with_metadata(meta)
}

/// Builds a single Arrow [`Field`] and [`ArrayRef`] directly from a column-major
/// slice of [`ResultValue`]s, avoiding an intermediate row-major matrix and transpose.
pub fn build_result_column(
    name: &str,
    values: &[ResultValue],
    hint: &ColumnHint,
    tz_cache: &mut HashMap<String, bool>,
) -> Result<(Field, ArrayRef)> {
    let col_buf: Vec<Option<&ResultValue>> =
        values.iter().map(|v| (!v.is_null()).then_some(v)).collect();
    build_column_values(name, &col_buf, hint, tz_cache)
}

/// Converts tabular [`ResultValue`] rows into an Apache Arrow [`RecordBatch`].
///
/// This is the single unified Rust Arrow builder used across Bolt, FalkorDB,
/// PostgreSQL, and Apache AGE.
pub fn values_to_record_batch(
    columns: &[String],
    rows: &[Vec<ResultValue>],
) -> Result<RecordBatch> {
    values_to_record_batch_with_hints(columns, rows, &[])
}

/// Converts tabular [`ResultValue`] rows into an Apache Arrow [`RecordBatch`],
/// applying optional per-column [`ColumnHint`]s (for example PostgreSQL OID/typmod
/// metadata or Apache AGE `agtype` promotion rules).
pub fn values_to_record_batch_with_hints(
    columns: &[String],
    rows: &[Vec<ResultValue>],
    hints: &[ColumnHint],
) -> Result<RecordBatch> {
    if columns.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    let mut tz_cache: HashMap<String, bool> = HashMap::new();
    let default_hint = ColumnHint::Auto;

    let mut col_buf: Vec<Option<&ResultValue>> = Vec::with_capacity(rows.len());

    for (col_idx, col_name) in columns.iter().enumerate() {
        col_buf.clear();
        for row in rows {
            match row.get(col_idx) {
                Some(ResultValue::Null) | None => col_buf.push(None),
                Some(v) => col_buf.push(Some(v)),
            }
        }
        let hint = hints.get(col_idx).unwrap_or(&default_hint);
        let (field, array) = build_column_values(col_name, &col_buf, hint, &mut tz_cache)?;
        fields.push(field);
        arrays.push(array);
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays).map_err(NetError::ArrowError)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bolt::BoltValue;
    use crate::value::{Point2D, Point3D};
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::{UnionFields, UnionMode};

    #[test]
    fn result_value_is_compact_and_much_smaller_than_bolt_value() {
        let rv_size = std::mem::size_of::<ResultValue>();
        let bv_size = std::mem::size_of::<BoltValue>();
        assert!(
            rv_size <= 32,
            "ResultValue size {rv_size} exceeds hard 32-byte cap"
        );
        assert!(
            rv_size < bv_size,
            "ResultValue ({rv_size}B) should be strictly smaller than BoltValue ({bv_size}B)"
        );
    }

    #[test]
    fn recursive_child_metadata_on_list_of_time_tz() {
        let cols = vec!["times".to_string()];
        let rows = vec![vec![ResultValue::List(
            vec![
                ResultValue::Time {
                    nanoseconds: 14 * 3_600 * 1_000_000_000 + 30 * 60 * 1_000_000_000,
                    tz_offset_seconds: 7_200,
                },
                ResultValue::Null,
            ]
            .into_boxed_slice(),
        )]];

        let batch = values_to_record_batch(&cols, &rows).unwrap();
        let field = batch.schema().field(0).clone();
        let DataType::List(item_field) = field.data_type() else {
            panic!("expected List, got {:?}", field.data_type());
        };
        assert_eq!(item_field.data_type(), &DataType::Utf8);
        assert_eq!(
            item_field
                .metadata()
                .get(VOYAGER_META_TYPE)
                .map(String::as_str),
            Some(META_TYPE_TIME_TZ)
        );
        let list = batch.column(0).as_list::<i32>();
        let inner = list.value(0);
        let strings = inner.as_string::<i32>();
        assert_eq!(strings.value(0), "14:30:00+02:00");
        assert!(strings.is_null(1));
    }

    #[test]
    fn recursive_child_metadata_on_list_of_datetime_tz() {
        let cols = vec!["dts".to_string()];
        let rows = vec![vec![ResultValue::List(
            vec![
                ResultValue::DateTime(Box::new(ResultDateTime {
                    seconds: 1_700_000_000,
                    nanoseconds: 123_000_000,
                    tz: ResultTimezone::Offset(7_200),
                })),
                ResultValue::DateTime(Box::new(ResultDateTime {
                    seconds: 1_700_003_600,
                    nanoseconds: 0,
                    tz: ResultTimezone::Offset(-18_000),
                })),
            ]
            .into_boxed_slice(),
        )]];

        let batch = values_to_record_batch(&cols, &rows).unwrap();
        let field = batch.schema().field(0).clone();
        let DataType::List(item_field) = field.data_type() else {
            panic!("expected List, got {:?}", field.data_type());
        };
        assert_eq!(item_field.data_type(), &DataType::Utf8);
        assert_eq!(
            item_field
                .metadata()
                .get(VOYAGER_META_TYPE)
                .map(String::as_str),
            Some(META_TYPE_DATETIME_TZ)
        );
    }

    #[test]
    fn recursive_child_metadata_on_list_of_uuid() {
        let u1 = [
            0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78, 0x12, 0x34,
            0x56, 0x78,
        ];
        let cols = vec!["ids".to_string()];
        let rows = vec![vec![ResultValue::List(
            vec![ResultValue::Uuid(u1), ResultValue::Null].into_boxed_slice(),
        )]];

        let batch = values_to_record_batch(&cols, &rows).unwrap();
        let field = batch.schema().field(0).clone();
        let DataType::List(item_field) = field.data_type() else {
            panic!("expected List, got {:?}", field.data_type());
        };
        assert_eq!(item_field.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            item_field
                .metadata()
                .get(ARROW_EXTENSION_NAME)
                .map(String::as_str),
            Some(ARROW_UUID_EXTENSION)
        );
        assert_eq!(
            item_field
                .metadata()
                .get(VOYAGER_META_TYPE)
                .map(String::as_str),
            Some(META_TYPE_UUID)
        );
    }

    #[test]
    fn recursive_child_metadata_on_multidimensional_lists() {
        let u1 = [0xAA; 16];
        let cols = vec![
            "timetz_2d".to_string(),
            "dt_tz_2d".to_string(),
            "uuid_2d".to_string(),
            "point_2d".to_string(),
        ];
        let rows = vec![vec![
            ResultValue::List(
                vec![ResultValue::List(
                    vec![ResultValue::Time {
                        nanoseconds: 3_600 * 1_000_000_000,
                        tz_offset_seconds: 3_600,
                    }]
                    .into_boxed_slice(),
                )]
                .into_boxed_slice(),
            ),
            ResultValue::List(
                vec![ResultValue::List(
                    vec![ResultValue::DateTime(Box::new(ResultDateTime {
                        seconds: 1_700_000_000,
                        nanoseconds: 0,
                        tz: ResultTimezone::Offset(3_600),
                    }))]
                    .into_boxed_slice(),
                )]
                .into_boxed_slice(),
            ),
            ResultValue::List(
                vec![ResultValue::List(
                    vec![ResultValue::Uuid(u1)].into_boxed_slice(),
                )]
                .into_boxed_slice(),
            ),
            ResultValue::List(
                vec![ResultValue::List(
                    vec![
                        ResultValue::Point2D(Box::new(Point2D {
                            srid: 4326,
                            x: 1.0,
                            y: 2.0,
                        })),
                        ResultValue::Point3D(Box::new(Point3D {
                            srid: 4979,
                            x: 3.0,
                            y: 4.0,
                            z: 5.0,
                        })),
                    ]
                    .into_boxed_slice(),
                )]
                .into_boxed_slice(),
            ),
        ]];

        let batch = values_to_record_batch(&cols, &rows).unwrap();
        let schema = batch.schema();

        // 2D timetz
        let DataType::List(outer) = schema.field(0).data_type() else {
            panic!("expected outer List");
        };
        let DataType::List(leaf) = outer.data_type() else {
            panic!("expected inner List");
        };
        assert_eq!(
            leaf.metadata().get(VOYAGER_META_TYPE).map(String::as_str),
            Some(META_TYPE_TIME_TZ)
        );

        // 2D datetime_tz
        let DataType::List(outer) = schema.field(1).data_type() else {
            panic!("expected outer List");
        };
        let DataType::List(leaf) = outer.data_type() else {
            panic!("expected inner List");
        };
        assert_eq!(
            leaf.metadata().get(VOYAGER_META_TYPE).map(String::as_str),
            Some(META_TYPE_DATETIME_TZ)
        );

        // 2D uuid
        let DataType::List(outer) = schema.field(2).data_type() else {
            panic!("expected outer List");
        };
        let DataType::List(leaf) = outer.data_type() else {
            panic!("expected inner List");
        };
        assert_eq!(leaf.data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            leaf.metadata()
                .get(ARROW_EXTENSION_NAME)
                .map(String::as_str),
            Some(ARROW_UUID_EXTENSION)
        );
        assert_eq!(
            leaf.metadata().get(VOYAGER_META_TYPE).map(String::as_str),
            Some(META_TYPE_UUID)
        );

        // 2D point
        let DataType::List(outer) = schema.field(3).data_type() else {
            panic!("expected outer List");
        };
        let DataType::List(leaf) = outer.data_type() else {
            panic!("expected inner List");
        };
        assert!(matches!(leaf.data_type(), DataType::Struct(_)));
        assert_eq!(
            leaf.metadata().get(VOYAGER_META_TYPE).map(String::as_str),
            Some(META_TYPE_POINT)
        );
    }

    #[test]
    fn canonicalize_field_metadata_recurses_through_all_container_types() {
        let uuid_only_ext = field_with_meta(
            "u",
            DataType::FixedSizeBinary(16),
            &[(ARROW_EXTENSION_NAME, ARROW_UUID_EXTENSION)],
        );
        let list_f = Field::new("l", DataType::List(Arc::new(uuid_only_ext.clone())), true);
        let large_list_f = Field::new(
            "ll",
            DataType::LargeList(Arc::new(uuid_only_ext.clone())),
            true,
        );
        let fixed_list_f = Field::new(
            "fl",
            DataType::FixedSizeList(Arc::new(uuid_only_ext.clone()), 2),
            true,
        );
        let map_entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                uuid_only_ext.clone(),
            ])),
            false,
        );
        let map_f = Field::new("m", DataType::Map(Arc::new(map_entries), false), true);
        let union_f = Field::new(
            "un",
            DataType::Union(
                UnionFields::try_new(vec![0i8], vec![uuid_only_ext]).unwrap(),
                UnionMode::Dense,
            ),
            true,
        );
        let root = Field::new(
            "root",
            DataType::Struct(Fields::from(vec![
                list_f,
                large_list_f,
                fixed_list_f,
                map_f,
                union_f,
            ])),
            true,
        );

        let canon = canonicalize_field_metadata(&root);
        let DataType::Struct(children) = canon.data_type() else {
            panic!("expected Struct");
        };
        for child in children {
            match child.data_type() {
                DataType::List(c) | DataType::LargeList(c) | DataType::FixedSizeList(c, _) => {
                    assert_eq!(
                        c.metadata().get(VOYAGER_META_TYPE).map(String::as_str),
                        Some(META_TYPE_UUID)
                    );
                }
                DataType::Map(entries, _) => {
                    let DataType::Struct(kv) = entries.data_type() else {
                        panic!("expected Map struct");
                    };
                    assert_eq!(
                        kv[1].metadata().get(VOYAGER_META_TYPE).map(String::as_str),
                        Some(META_TYPE_UUID)
                    );
                }
                DataType::Union(uf, _) => {
                    let (_, member) = uf.iter().next().unwrap();
                    assert_eq!(
                        member.metadata().get(VOYAGER_META_TYPE).map(String::as_str),
                        Some(META_TYPE_UUID)
                    );
                }
                other => panic!("unexpected type {other:?}"),
            }
        }
    }
}
