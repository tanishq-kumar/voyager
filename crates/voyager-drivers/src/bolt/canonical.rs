//! Canonical JSON representation and Arrow RecordBatch construction for Bolt/graph values.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, DurationMicrosecondBuilder,
    DurationNanosecondBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    LargeBinaryBuilder, ListBuilder, StringBuilder, StructBuilder, Time64MicrosecondBuilder,
    Time64NanosecondBuilder, TimestampMicrosecondBuilder, TimestampNanosecondBuilder,
};
use arrow_array::timezone::Tz;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array,
    DurationMicrosecondArray, DurationNanosecondArray, FixedSizeBinaryArray, Float64Array,
    Int64Array, LargeBinaryArray, RecordBatch, StringArray, StructArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};

use crate::bolt::packstream::{BoltDuration, BoltNode, BoltPath, BoltRelationship, BoltValue};
use crate::error::{NetError, Result};

/// Converts a [`BoltNode`] into the canonical kind-tagged JSON object.
pub fn bolt_node_to_canonical_json(node: &BoltNode) -> serde_json::Value {
    let mut labels = node.labels.clone();
    labels.sort();

    let mut props = serde_json::Map::with_capacity(node.properties.len());
    for (k, v) in &node.properties {
        props.insert(k.clone(), bolt_value_to_canonical_json(v));
    }

    let mut obj = serde_json::Map::with_capacity(5);
    obj.insert(
        "kind".to_string(),
        serde_json::Value::String("node".to_string()),
    );
    obj.insert("id".to_string(), serde_json::Value::Number(node.id.into()));
    obj.insert(
        "element_id".to_string(),
        match &node.element_id {
            Some(eid) => serde_json::Value::String(eid.clone()),
            None => serde_json::Value::Null,
        },
    );
    obj.insert(
        "labels".to_string(),
        serde_json::Value::Array(labels.into_iter().map(serde_json::Value::String).collect()),
    );
    obj.insert("properties".to_string(), serde_json::Value::Object(props));
    serde_json::Value::Object(obj)
}

/// Converts a [`BoltRelationship`] into the canonical kind-tagged JSON object.
pub fn bolt_relationship_to_canonical_json(rel: &BoltRelationship) -> serde_json::Value {
    let mut props = serde_json::Map::with_capacity(rel.properties.len());
    for (k, v) in &rel.properties {
        props.insert(k.clone(), bolt_value_to_canonical_json(v));
    }

    let mut obj = serde_json::Map::with_capacity(9);
    obj.insert(
        "kind".to_string(),
        serde_json::Value::String("relationship".to_string()),
    );
    obj.insert("id".to_string(), serde_json::Value::Number(rel.id.into()));
    obj.insert(
        "element_id".to_string(),
        match &rel.element_id {
            Some(eid) => serde_json::Value::String(eid.clone()),
            None => serde_json::Value::Null,
        },
    );
    obj.insert(
        "type".to_string(),
        serde_json::Value::String(rel.rel_type.clone()),
    );
    obj.insert(
        "start_id".to_string(),
        serde_json::Value::Number(rel.start_node_id.into()),
    );
    obj.insert(
        "end_id".to_string(),
        serde_json::Value::Number(rel.end_node_id.into()),
    );
    obj.insert("properties".to_string(), serde_json::Value::Object(props));
    if let Some(ref start_eid) = rel.start_element_id {
        obj.insert(
            "start_element_id".to_string(),
            serde_json::Value::String(start_eid.clone()),
        );
    }
    if let Some(ref end_eid) = rel.end_element_id {
        obj.insert(
            "end_element_id".to_string(),
            serde_json::Value::String(end_eid.clone()),
        );
    }
    serde_json::Value::Object(obj)
}

/// Converts a [`BoltPath`] into the canonical kind-tagged JSON object.
pub fn bolt_path_to_canonical_json(path: &BoltPath) -> serde_json::Value {
    let mut nodes_json = Vec::new();
    let mut rels_json = Vec::new();

    if path.sequence.is_empty() {
        for node in &path.nodes {
            nodes_json.push(bolt_node_to_canonical_json(node));
        }
    } else {
        if let Some(first) = path.nodes.first() {
            nodes_json.push(bolt_node_to_canonical_json(first));
        }
        let mut current_node_idx = 0usize;
        for step_idx in 0..(path.sequence.len() / 2) {
            let rel_code = path.sequence[2 * step_idx];
            let next_node_raw = path.sequence[2 * step_idx + 1];
            let next_node_idx = usize::try_from(next_node_raw).unwrap_or(0);
            let rel_idx = rel_code.unsigned_abs().saturating_sub(1) as usize;

            if let (Some(prev_node), Some(next_node), Some(unbound_rel)) = (
                path.nodes.get(current_node_idx),
                path.nodes.get(next_node_idx),
                path.relationships.get(rel_idx),
            ) {
                nodes_json.push(bolt_node_to_canonical_json(next_node));

                let (start_node, end_node) = if rel_code > 0 {
                    (prev_node, next_node)
                } else {
                    (next_node, prev_node)
                };

                let bound_rel = BoltRelationship {
                    id: unbound_rel.id,
                    start_node_id: start_node.id,
                    end_node_id: end_node.id,
                    rel_type: unbound_rel.rel_type.clone(),
                    properties: unbound_rel.properties.clone(),
                    element_id: unbound_rel.element_id.clone(),
                    start_element_id: start_node.element_id.clone(),
                    end_element_id: end_node.element_id.clone(),
                };
                rels_json.push(bolt_relationship_to_canonical_json(&bound_rel));
                current_node_idx = next_node_idx;
            }
        }
    }

    let mut obj = serde_json::Map::with_capacity(3);
    obj.insert(
        "kind".to_string(),
        serde_json::Value::String("path".to_string()),
    );
    obj.insert("nodes".to_string(), serde_json::Value::Array(nodes_json));
    obj.insert(
        "relationships".to_string(),
        serde_json::Value::Array(rels_json),
    );
    serde_json::Value::Object(obj)
}

/// Converts any [`BoltValue`] into canonical JSON without serde enum wrapper tags.
pub fn bolt_value_to_canonical_json(value: &BoltValue) -> serde_json::Value {
    match value {
        BoltValue::Null => serde_json::Value::Null,
        BoltValue::Boolean(b) => serde_json::Value::Bool(*b),
        BoltValue::Integer(i) => serde_json::Value::Number((*i).into()),
        BoltValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        BoltValue::Bytes(bytes) => serde_json::Value::Array(
            bytes
                .iter()
                .map(|b| serde_json::Value::Number((*b).into()))
                .collect(),
        ),
        BoltValue::String(s) => serde_json::Value::String(s.clone()),
        BoltValue::List(items) => {
            serde_json::Value::Array(items.iter().map(bolt_value_to_canonical_json).collect())
        }
        BoltValue::Map(map) => {
            let mut obj = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                obj.insert(k.clone(), bolt_value_to_canonical_json(v));
            }
            serde_json::Value::Object(obj)
        }
        BoltValue::Structure { tag, fields } => {
            let mut obj = serde_json::Map::with_capacity(2);
            obj.insert("tag".to_string(), serde_json::Value::Number((*tag).into()));
            obj.insert(
                "fields".to_string(),
                serde_json::Value::Array(fields.iter().map(bolt_value_to_canonical_json).collect()),
            );
            serde_json::Value::Object(obj)
        }
        BoltValue::Node(node) => bolt_node_to_canonical_json(node),
        BoltValue::Relationship(rel) => bolt_relationship_to_canonical_json(rel),
        BoltValue::Path(path) => bolt_path_to_canonical_json(path),
        BoltValue::Point2D(pt) => {
            let mut obj = serde_json::Map::with_capacity(3);
            obj.insert(
                "srid".to_string(),
                serde_json::Value::Number(pt.srid.into()),
            );
            obj.insert(
                "x".to_string(),
                serde_json::Number::from_f64(pt.x)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
            );
            obj.insert(
                "y".to_string(),
                serde_json::Number::from_f64(pt.y)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
            );
            serde_json::Value::Object(obj)
        }
        BoltValue::Point3D(pt) => {
            let mut obj = serde_json::Map::with_capacity(4);
            obj.insert(
                "srid".to_string(),
                serde_json::Value::Number(pt.srid.into()),
            );
            obj.insert(
                "x".to_string(),
                serde_json::Number::from_f64(pt.x)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
            );
            obj.insert(
                "y".to_string(),
                serde_json::Number::from_f64(pt.y)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
            );
            obj.insert(
                "z".to_string(),
                serde_json::Number::from_f64(pt.z)
                    .map(serde_json::Value::Number)
                    .unwrap_or(serde_json::Value::Null),
            );
            serde_json::Value::Object(obj)
        }
        BoltValue::Date(d) => serde_json::Value::String(d.to_iso_string()),
        BoltValue::Time(t) => serde_json::Value::String(t.to_iso_string()),
        BoltValue::LocalTime(lt) => serde_json::Value::String(lt.to_iso_string()),
        BoltValue::DateTime(dt) => serde_json::Value::String(dt.to_iso_string()),
        BoltValue::LocalDateTime(ldt) => serde_json::Value::String(ldt.to_iso_string()),
        BoltValue::DateTimeZoneId(dtz) => serde_json::Value::String(dtz.to_iso_string()),
        BoltValue::Duration(dur) => serde_json::Value::String(dur.to_iso_string()),
    }
}

/// Checks whether `zone_id` can be resolved as an Arrow/Polars `Timestamp` timezone.
///
/// Uses the same `chrono-tz`-backed parser as Arrow rather than the host's
/// `/usr/share/zoneinfo`, so validation works on Windows and hosts without tzdata.
pub(crate) fn is_valid_arrow_tz(zone_id: &str) -> bool {
    zone_id.parse::<Tz>().is_ok()
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

fn duration_micros(dur: &BoltDuration) -> Option<i64> {
    if dur.months != 0 || dur.days != 0 || dur.nanoseconds % 1_000 != 0 {
        return None;
    }
    dur.seconds
        .checked_mul(1_000_000)?
        .checked_add(dur.nanoseconds / 1_000)
}

fn duration_nanos(dur: &BoltDuration) -> Option<i64> {
    if dur.months != 0 || dur.days != 0 {
        return None;
    }
    dur.seconds
        .checked_mul(1_000_000_000)?
        .checked_add(dur.nanoseconds)
}

pub(crate) fn duration_struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("months", DataType::Int64, false),
        Field::new("days", DataType::Int64, false),
        Field::new("seconds", DataType::Int64, false),
        Field::new("nanoseconds", DataType::Int64, false),
    ])
}

pub(crate) fn make_duration_struct_builder(capacity: usize) -> StructBuilder {
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

pub(crate) fn append_duration_struct(builder: &mut StructBuilder, dur: Option<&BoltDuration>) {
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

fn tz_label_for_offset(offset_seconds: i64) -> Option<String> {
    if offset_seconds == 0 {
        Some("UTC".to_string())
    } else {
        None
    }
}

#[derive(Clone, PartialEq, Eq)]
enum BoltScalarKind {
    Int,
    Float,
    Bool,
    String,
    Bytes,
    Date,
    LocalTime,
    TimeTz,
    LocalDateTime,
    DateTimeOffset(Option<String>),
    DateTimeZone(Option<String>),
    DateTimeMixed,
    Duration,
    Mixed,
}

fn classify_bolt_scalar(
    val: &BoltValue,
    tz_cache: &mut HashMap<String, bool>,
) -> Option<BoltScalarKind> {
    match val {
        BoltValue::Null => None,
        BoltValue::Integer(_) => Some(BoltScalarKind::Int),
        BoltValue::Float(_) => Some(BoltScalarKind::Float),
        BoltValue::Boolean(_) => Some(BoltScalarKind::Bool),
        BoltValue::String(_) => Some(BoltScalarKind::String),
        BoltValue::Bytes(_) => Some(BoltScalarKind::Bytes),
        BoltValue::Date(_) => Some(BoltScalarKind::Date),
        BoltValue::LocalTime(_) => Some(BoltScalarKind::LocalTime),
        BoltValue::Time(_) => Some(BoltScalarKind::TimeTz),
        BoltValue::LocalDateTime(_) => Some(BoltScalarKind::LocalDateTime),
        BoltValue::DateTime(dt) => Some(BoltScalarKind::DateTimeOffset(tz_label_for_offset(
            dt.tz_offset_seconds,
        ))),
        BoltValue::DateTimeZoneId(dtz) => {
            if dtz.is_local {
                return Some(BoltScalarKind::DateTimeZone(None));
            }
            let valid = if let Some(&cached) = tz_cache.get(&dtz.zone_id) {
                cached
            } else {
                let v = is_valid_arrow_tz(&dtz.zone_id);
                tz_cache.insert(dtz.zone_id.clone(), v);
                v
            };
            Some(BoltScalarKind::DateTimeZone(
                valid.then(|| dtz.zone_id.clone()),
            ))
        }
        BoltValue::Duration(_) => Some(BoltScalarKind::Duration),
        _ => Some(BoltScalarKind::Mixed),
    }
}

fn merge_scalar_kind(acc: &mut Option<BoltScalarKind>, next: BoltScalarKind) {
    let Some(cur) = acc.take() else {
        *acc = Some(next);
        return;
    };
    let merged = match (cur, next) {
        (a, b) if a == b => a,
        (BoltScalarKind::LocalTime | BoltScalarKind::TimeTz, BoltScalarKind::TimeTz)
        | (BoltScalarKind::TimeTz, BoltScalarKind::LocalTime) => BoltScalarKind::TimeTz,
        (BoltScalarKind::DateTimeOffset(a), BoltScalarKind::DateTimeOffset(b)) => {
            if a.is_some() && a == b {
                BoltScalarKind::DateTimeOffset(a)
            } else {
                BoltScalarKind::DateTimeMixed
            }
        }
        (BoltScalarKind::DateTimeZone(a), BoltScalarKind::DateTimeZone(b)) => {
            if a.is_some() && a == b {
                BoltScalarKind::DateTimeZone(a)
            } else {
                BoltScalarKind::DateTimeMixed
            }
        }
        (
            BoltScalarKind::LocalDateTime
            | BoltScalarKind::DateTimeOffset(_)
            | BoltScalarKind::DateTimeZone(_)
            | BoltScalarKind::DateTimeMixed,
            BoltScalarKind::LocalDateTime
            | BoltScalarKind::DateTimeOffset(_)
            | BoltScalarKind::DateTimeZone(_)
            | BoltScalarKind::DateTimeMixed,
        ) => BoltScalarKind::DateTimeMixed,
        _ => BoltScalarKind::Mixed,
    };
    *acc = Some(merged);
}

fn build_typed_bolt_column<'a, I>(
    name: &str,
    kind: BoltScalarKind,
    values: I,
    count: usize,
) -> Result<(Field, ArrayRef)>
where
    I: Iterator<Item = Option<&'a BoltValue>> + Clone,
{
    match kind {
        BoltScalarKind::Int => {
            let mut builder = Int64Builder::with_capacity(count);
            for v in values {
                match v {
                    Some(BoltValue::Integer(i)) => builder.append_value(*i),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Int64, true),
                Arc::new(builder.finish()),
            ))
        }
        BoltScalarKind::Float => {
            let mut builder = Float64Builder::with_capacity(count);
            for v in values {
                match v {
                    Some(BoltValue::Float(f)) => builder.append_value(*f),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Float64, true),
                Arc::new(builder.finish()),
            ))
        }
        BoltScalarKind::Bool => {
            let mut builder = BooleanBuilder::with_capacity(count);
            for v in values {
                match v {
                    Some(BoltValue::Boolean(b)) => builder.append_value(*b),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Boolean, true),
                Arc::new(builder.finish()),
            ))
        }
        BoltScalarKind::String => {
            let mut builder = StringBuilder::with_capacity(count, count * 16);
            for v in values {
                match v {
                    Some(BoltValue::String(s)) => builder.append_value(s),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Utf8, true),
                Arc::new(builder.finish()),
            ))
        }
        BoltScalarKind::Bytes => {
            let total_bytes: usize = values
                .clone()
                .filter_map(|v| match v {
                    Some(BoltValue::Bytes(b)) => Some(b.len()),
                    _ => None,
                })
                .sum();
            if total_bytes <= i32::MAX as usize {
                let mut builder = BinaryBuilder::with_capacity(count, total_bytes);
                for v in values {
                    match v {
                        Some(BoltValue::Bytes(b)) => builder.append_value(b),
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
                        Some(BoltValue::Bytes(b)) => builder.append_value(b),
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::LargeBinary, true),
                    Arc::new(builder.finish()),
                ))
            }
        }
        BoltScalarKind::Date => {
            let all_fit_i32 = values.clone().all(|v| match v {
                Some(BoltValue::Date(d)) => i32::try_from(d.days).is_ok(),
                _ => true,
            });
            if all_fit_i32 {
                let mut builder = Date32Builder::with_capacity(count);
                for v in values {
                    match v {
                        Some(BoltValue::Date(d)) => builder.append_value(d.days as i32),
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
                        Some(BoltValue::Date(d)) => builder.append_value(d.to_iso_string()),
                        _ => builder.append_null(),
                    }
                }
                let mut meta = HashMap::new();
                meta.insert("voyager.type".to_string(), "date".to_string());
                Ok((
                    Field::new(name, DataType::Utf8, true).with_metadata(meta),
                    Arc::new(builder.finish()),
                ))
            }
        }
        BoltScalarKind::LocalTime => {
            let mut builder = Time64NanosecondBuilder::with_capacity(count);
            for v in values {
                match v {
                    Some(BoltValue::LocalTime(lt)) => builder.append_value(lt.nanoseconds),
                    _ => builder.append_null(),
                }
            }
            Ok((
                Field::new(name, DataType::Time64(TimeUnit::Nanosecond), true),
                Arc::new(builder.finish()),
            ))
        }
        BoltScalarKind::TimeTz => {
            let mut builder = StringBuilder::with_capacity(count, count * 24);
            for v in values {
                match v {
                    Some(BoltValue::Time(t)) => builder.append_value(t.to_iso_string()),
                    Some(BoltValue::LocalTime(lt)) => builder.append_value(lt.to_iso_string()),
                    _ => builder.append_null(),
                }
            }
            let mut meta = HashMap::new();
            meta.insert("voyager.type".to_string(), "time_tz".to_string());
            Ok((
                Field::new(name, DataType::Utf8, true).with_metadata(meta),
                Arc::new(builder.finish()),
            ))
        }
        BoltScalarKind::LocalDateTime => {
            let all_us = values.clone().all(|v| match v {
                Some(BoltValue::LocalDateTime(ldt)) => {
                    timestamp_micros(ldt.seconds, ldt.nanoseconds).is_some()
                }
                _ => true,
            });
            if all_us {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(count);
                for v in values {
                    match v {
                        Some(BoltValue::LocalDateTime(ldt)) => builder.append_value(
                            timestamp_micros(ldt.seconds, ldt.nanoseconds).expect("checked"),
                        ),
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true),
                    Arc::new(builder.finish()),
                ))
            } else {
                let all_ns = values.clone().all(|v| match v {
                    Some(BoltValue::LocalDateTime(ldt)) => {
                        timestamp_nanos(ldt.seconds, ldt.nanoseconds).is_some()
                    }
                    _ => true,
                });
                if all_ns {
                    let mut builder = TimestampNanosecondBuilder::with_capacity(count);
                    for v in values {
                        match v {
                            Some(BoltValue::LocalDateTime(ldt)) => builder.append_value(
                                timestamp_nanos(ldt.seconds, ldt.nanoseconds).expect("checked"),
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
                            Some(BoltValue::LocalDateTime(ldt)) => {
                                builder.append_value(ldt.to_iso_string())
                            }
                            _ => builder.append_null(),
                        }
                    }
                    let mut meta = HashMap::new();
                    meta.insert("voyager.type".to_string(), "local_datetime".to_string());
                    Ok((
                        Field::new(name, DataType::Utf8, true).with_metadata(meta),
                        Arc::new(builder.finish()),
                    ))
                }
            }
        }
        BoltScalarKind::DateTimeOffset(Some(tz)) | BoltScalarKind::DateTimeZone(Some(tz)) => {
            let extract_sn = |v: &BoltValue| -> Option<(i64, i64)> {
                match v {
                    BoltValue::DateTime(dt) => Some((dt.seconds, dt.nanoseconds)),
                    BoltValue::DateTimeZoneId(dtz) => Some((dtz.seconds, dtz.nanoseconds)),
                    _ => None,
                }
            };
            let all_us = values.clone().all(|v| match v {
                Some(inner) => extract_sn(inner)
                    .and_then(|(s, n)| timestamp_micros(s, n))
                    .is_some(),
                None => true,
            });
            if all_us {
                let mut builder =
                    TimestampMicrosecondBuilder::with_capacity(count).with_timezone(tz.clone());
                for v in values {
                    match v.and_then(extract_sn) {
                        Some((s, n)) => {
                            builder.append_value(timestamp_micros(s, n).expect("checked"))
                        }
                        None => builder.append_null(),
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
                let all_ns = values.clone().all(|v| match v {
                    Some(inner) => extract_sn(inner)
                        .and_then(|(s, n)| timestamp_nanos(s, n))
                        .is_some(),
                    None => true,
                });
                if all_ns {
                    let mut builder =
                        TimestampNanosecondBuilder::with_capacity(count).with_timezone(tz.clone());
                    for v in values {
                        match v.and_then(extract_sn) {
                            Some((s, n)) => {
                                builder.append_value(timestamp_nanos(s, n).expect("checked"))
                            }
                            None => builder.append_null(),
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
                    build_datetime_tz_fallback(name, values, count)
                }
            }
        }
        BoltScalarKind::DateTimeOffset(None)
        | BoltScalarKind::DateTimeZone(None)
        | BoltScalarKind::DateTimeMixed => build_datetime_tz_fallback(name, values, count),
        BoltScalarKind::Duration => {
            let all_us = values.clone().all(|v| match v {
                Some(BoltValue::Duration(d)) => duration_micros(d).is_some(),
                _ => true,
            });
            if all_us {
                let mut builder = DurationMicrosecondBuilder::with_capacity(count);
                for v in values {
                    match v {
                        Some(BoltValue::Duration(d)) => {
                            builder.append_value(duration_micros(d).expect("checked"))
                        }
                        _ => builder.append_null(),
                    }
                }
                Ok((
                    Field::new(name, DataType::Duration(TimeUnit::Microsecond), true),
                    Arc::new(builder.finish()),
                ))
            } else {
                let all_ns = values.clone().all(|v| match v {
                    Some(BoltValue::Duration(d)) => duration_nanos(d).is_some(),
                    _ => true,
                });
                if all_ns {
                    let mut builder = DurationNanosecondBuilder::with_capacity(count);
                    for v in values {
                        match v {
                            Some(BoltValue::Duration(d)) => {
                                builder.append_value(duration_nanos(d).expect("checked"))
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
                            Some(BoltValue::Duration(d)) => {
                                append_duration_struct(&mut builder, Some(d))
                            }
                            _ => append_duration_struct(&mut builder, None),
                        }
                    }
                    let mut meta = HashMap::new();
                    meta.insert("voyager.type".to_string(), "duration".to_string());
                    Ok((
                        Field::new(name, DataType::Struct(duration_struct_fields()), true)
                            .with_metadata(meta),
                        Arc::new(builder.finish()),
                    ))
                }
            }
        }
        BoltScalarKind::Mixed => unreachable!("Mixed handled by JSON builder"),
    }
}

fn build_datetime_tz_fallback<'a, I>(
    name: &str,
    values: I,
    count: usize,
) -> Result<(Field, ArrayRef)>
where
    I: Iterator<Item = Option<&'a BoltValue>>,
{
    let mut builder = StringBuilder::with_capacity(count, count * 32);
    for v in values {
        match v {
            Some(BoltValue::LocalDateTime(ldt)) => builder.append_value(ldt.to_iso_string()),
            Some(BoltValue::DateTime(dt)) => builder.append_value(dt.to_iso_string()),
            Some(BoltValue::DateTimeZoneId(dtz)) => builder.append_value(dtz.to_iso_string()),
            _ => builder.append_null(),
        }
    }
    let mut meta = HashMap::new();
    meta.insert("voyager.type".to_string(), "datetime_tz".to_string());
    Ok((
        Field::new(name, DataType::Utf8, true).with_metadata(meta),
        Arc::new(builder.finish()),
    ))
}

pub(crate) fn wrap_flat_array_in_list(
    name: &str,
    item_field: Field,
    flat: &dyn Array,
    row_lengths: &[Option<usize>],
) -> Result<(Field, ArrayRef)> {
    let item_arc = Arc::new(item_field.clone());
    let list_field = Field::new(name, DataType::List(item_arc.clone()), true);

    macro_rules! finish_prim_list {
        ($arr_ty:ty, $builder_expr:expr) => {{
            let src = flat
                .as_any()
                .downcast_ref::<$arr_ty>()
                .expect("typed array downcast");
            let mut lb = ListBuilder::new($builder_expr).with_field(item_arc);
            let mut idx = 0usize;
            for len_opt in row_lengths {
                match len_opt {
                    None => lb.append_null(),
                    Some(len) => {
                        for _ in 0..*len {
                            if src.is_null(idx) {
                                lb.values().append_null();
                            } else {
                                lb.values().append_value(src.value(idx));
                            }
                            idx += 1;
                        }
                        lb.append(true);
                    }
                }
            }
            Ok((list_field, Arc::new(lb.finish())))
        }};
    }

    match item_field.data_type() {
        DataType::Int64 => finish_prim_list!(Int64Array, Int64Builder::new()),
        DataType::Float64 => finish_prim_list!(Float64Array, Float64Builder::new()),
        DataType::Decimal128(p, s) => finish_prim_list!(
            Decimal128Array,
            Decimal128Builder::new().with_data_type(DataType::Decimal128(*p, *s))
        ),
        DataType::Boolean => finish_prim_list!(BooleanArray, BooleanBuilder::new()),
        DataType::Utf8 => finish_prim_list!(StringArray, StringBuilder::new()),
        DataType::Binary => finish_prim_list!(BinaryArray, BinaryBuilder::new()),
        DataType::LargeBinary => finish_prim_list!(LargeBinaryArray, LargeBinaryBuilder::new()),
        DataType::FixedSizeBinary(w) => {
            let src = flat
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("FixedSizeBinaryArray");
            let mut lb = ListBuilder::new(FixedSizeBinaryBuilder::new(*w)).with_field(item_arc);
            let mut idx = 0usize;
            for len_opt in row_lengths {
                match len_opt {
                    None => lb.append_null(),
                    Some(len) => {
                        for _ in 0..*len {
                            if src.is_null(idx) {
                                lb.values().append_null();
                            } else {
                                lb.values()
                                    .append_value(src.value(idx))
                                    .map_err(NetError::ArrowError)?;
                            }
                            idx += 1;
                        }
                        lb.append(true);
                    }
                }
            }
            Ok((list_field, Arc::new(lb.finish())))
        }
        DataType::Date32 => finish_prim_list!(Date32Array, Date32Builder::new()),
        DataType::Time64(TimeUnit::Microsecond) => {
            finish_prim_list!(Time64MicrosecondArray, Time64MicrosecondBuilder::new())
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            finish_prim_list!(Time64NanosecondArray, Time64NanosecondBuilder::new())
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => finish_prim_list!(
            TimestampMicrosecondArray,
            TimestampMicrosecondBuilder::new().with_timezone_opt(tz.clone())
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => finish_prim_list!(
            TimestampNanosecondArray,
            TimestampNanosecondBuilder::new().with_timezone_opt(tz.clone())
        ),
        DataType::Duration(TimeUnit::Microsecond) => {
            finish_prim_list!(DurationMicrosecondArray, DurationMicrosecondBuilder::new())
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            finish_prim_list!(DurationNanosecondArray, DurationNanosecondBuilder::new())
        }
        DataType::Struct(fields) if fields.len() == 4 => {
            let src = flat
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("StructArray");
            let c0 = src
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("m");
            let c1 = src
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("d");
            let c2 = src
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("s");
            let c3 = src
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("ns");
            let mut lb =
                ListBuilder::new(make_duration_struct_builder(src.len())).with_field(item_arc);
            let mut idx = 0usize;
            for len_opt in row_lengths {
                match len_opt {
                    None => lb.append_null(),
                    Some(len) => {
                        for _ in 0..*len {
                            if src.is_null(idx) {
                                append_duration_struct(lb.values(), None);
                            } else {
                                let d = BoltDuration {
                                    months: c0.value(idx),
                                    days: c1.value(idx),
                                    seconds: c2.value(idx),
                                    nanoseconds: c3.value(idx),
                                };
                                append_duration_struct(lb.values(), Some(&d));
                            }
                            idx += 1;
                        }
                        lb.append(true);
                    }
                }
            }
            Ok((list_field, Arc::new(lb.finish())))
        }
        other => Err(NetError::ProtocolError(format!(
            "Unsupported list element Arrow type: {other:?}"
        ))),
    }
}

fn build_typed_bolt_list_column(
    name: &str,
    records: &[Vec<BoltValue>],
    col_idx: usize,
    elem_kind: BoltScalarKind,
) -> Result<(Field, ArrayRef)> {
    let flat_iter = records.iter().flat_map(|row| match row.get(col_idx) {
        Some(BoltValue::List(items)) => items
            .iter()
            .map(|item| {
                if matches!(item, BoltValue::Null) {
                    None
                } else {
                    Some(item)
                }
            })
            .collect::<Vec<_>>(),
        _ => Vec::new(),
    });
    let total_elems = records
        .iter()
        .map(|row| match row.get(col_idx) {
            Some(BoltValue::List(items)) => items.len(),
            _ => 0,
        })
        .sum();
    let (elem_field, flat_array) =
        build_typed_bolt_column("item", elem_kind, flat_iter, total_elems)?;

    let row_lengths: Vec<Option<usize>> = records
        .iter()
        .map(|row| match row.get(col_idx) {
            Some(BoltValue::List(items)) => Some(items.len()),
            _ => None,
        })
        .collect();

    wrap_flat_array_in_list(name, elem_field, flat_array.as_ref(), &row_lengths)
}

/// Returns `true` when `batch` contains any JSON-encoded column that relies on
/// raw [`BoltValue`] rows (`bolt_rows`) for lossless Python conversion.
pub fn batch_needs_bolt_rows(batch: &RecordBatch) -> bool {
    batch.schema().fields().iter().any(|field| {
        let meta = field.metadata();
        meta.get("voyager.encoding").is_some_and(|v| v == "json")
            || meta.contains_key("voyager.kind")
    })
}

/// Converts tabular [`BoltValue`] records into an Apache Arrow [`RecordBatch`].
///
/// Homogeneous temporal, binary, and scalar columns (including homogeneous `List`
/// columns) map to native Arrow types (`Date32`, `Time64`, `Timestamp`, `Duration`,
/// `Struct`, `Binary`, `Int64`, `Float64`, `Boolean`, `Utf8`, `List`).
/// Columns containing graph entities, maps, mixed-type lists, or mixed column types
/// are JSON-encoded using [`bolt_value_to_canonical_json`] and annotated with Arrow
/// field metadata (`voyager.encoding = "json"` and, for homogeneous entity columns,
/// `voyager.kind = "node" | "relationship" | "path"`).
pub fn build_bolt_record_batch(
    columns: &[String],
    records: &[Vec<BoltValue>],
) -> Result<RecordBatch> {
    if columns.is_empty() {
        let schema = Arc::new(Schema::empty());
        return Ok(RecordBatch::new_empty(schema));
    }

    let num_rows = records.len();
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrow_arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut tz_cache: HashMap<String, bool> = HashMap::new();

    for (col_idx, col_name) in columns.iter().enumerate() {
        let mut scalar_kind: Option<BoltScalarKind> = None;
        let mut list_count = 0usize;
        let mut list_elem_kind: Option<BoltScalarKind> = None;
        let mut list_has_non_null_elem = false;
        let mut node_count = 0usize;
        let mut rel_count = 0usize;
        let mut path_count = 0usize;
        let mut non_null_count = 0usize;

        for row in records {
            match row.get(col_idx) {
                Some(BoltValue::Null) | None => {}
                Some(BoltValue::List(items)) => {
                    list_count += 1;
                    non_null_count += 1;
                    merge_scalar_kind(&mut scalar_kind, BoltScalarKind::Mixed);
                    for item in items {
                        if let Some(k) = classify_bolt_scalar(item, &mut tz_cache) {
                            list_has_non_null_elem = true;
                            merge_scalar_kind(&mut list_elem_kind, k);
                        }
                    }
                }
                Some(BoltValue::Node(_)) => {
                    node_count += 1;
                    non_null_count += 1;
                    merge_scalar_kind(&mut scalar_kind, BoltScalarKind::Mixed);
                }
                Some(BoltValue::Relationship(_)) => {
                    rel_count += 1;
                    non_null_count += 1;
                    merge_scalar_kind(&mut scalar_kind, BoltScalarKind::Mixed);
                }
                Some(BoltValue::Path(_)) => {
                    path_count += 1;
                    non_null_count += 1;
                    merge_scalar_kind(&mut scalar_kind, BoltScalarKind::Mixed);
                }
                Some(other) => {
                    non_null_count += 1;
                    if let Some(k) = classify_bolt_scalar(other, &mut tz_cache) {
                        merge_scalar_kind(&mut scalar_kind, k);
                    }
                }
            }
        }

        if non_null_count == 0 {
            let mut builder = StringBuilder::with_capacity(num_rows, 0);
            for _ in 0..num_rows {
                builder.append_null();
            }
            fields.push(Field::new(col_name, DataType::Utf8, true));
            arrow_arrays.push(Arc::new(builder.finish()));
            continue;
        }

        if list_count == non_null_count
            && list_has_non_null_elem
            && let Some(elem_kind) = list_elem_kind
            && elem_kind != BoltScalarKind::Mixed
        {
            let (field, array) =
                build_typed_bolt_list_column(col_name, records, col_idx, elem_kind)?;
            fields.push(field);
            arrow_arrays.push(array);
            continue;
        }

        if let Some(kind) = scalar_kind
            && kind != BoltScalarKind::Mixed
        {
            let col_iter = records.iter().map(|row| match row.get(col_idx) {
                Some(BoltValue::Null) | None => None,
                Some(v) => Some(v),
            });
            let (field, array) = build_typed_bolt_column(col_name, kind, col_iter, num_rows)?;
            fields.push(field);
            arrow_arrays.push(array);
            continue;
        }

        // Structured / heterogeneous / entity column -> JSON with metadata
        let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
        for row in records {
            match row.get(col_idx) {
                Some(BoltValue::Null) | None => builder.append_null(),
                Some(other) => {
                    let json_str = serde_json::to_string(&bolt_value_to_canonical_json(other))
                        .unwrap_or_else(|_| "null".to_string());
                    builder.append_value(&json_str);
                }
            }
        }
        let mut metadata = HashMap::new();
        metadata.insert("voyager.encoding".to_string(), "json".to_string());
        if node_count == non_null_count {
            metadata.insert("voyager.kind".to_string(), "node".to_string());
        } else if rel_count == non_null_count {
            metadata.insert("voyager.kind".to_string(), "relationship".to_string());
        } else if path_count == non_null_count {
            metadata.insert("voyager.kind".to_string(), "path".to_string());
        }
        fields.push(Field::new(col_name, DataType::Utf8, true).with_metadata(metadata));
        arrow_arrays.push(Arc::new(builder.finish()));
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrow_arrays).map_err(NetError::ArrowError)
}
