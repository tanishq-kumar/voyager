//! Lean output value intermediate representation ([`ResultValue`]).
//!
//! [`ResultValue`] is strictly separate from [`crate::value::param::ParamValue`]
//! (query inputs) and from protocol wire types such as [`crate::bolt::BoltValue`].
//! Heavy variants are boxed so `std::mem::size_of::<ResultValue>() <= 32` bytes
//! (enforced at compile time below).

use std::collections::HashMap;

use crate::bolt::packstream::{
    BoltDateTimeZoneId, BoltNode, BoltPath, BoltRelationship, BoltValue,
};
use crate::value::{
    DurationParts, META_KIND_NODE, META_KIND_PATH, META_KIND_RELATIONSHIP, Point2D, Point3D,
    days_to_ymd, format_iso_duration, format_time_string, format_tz_offset, format_uuid_bytes,
    nanos_to_hmsn,
};

/// Timezone attachment on a [`ResultDateTime`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ResultTimezone {
    /// Fixed UTC offset in seconds (`+02:00` -> `7200`, `UTC` -> `0`).
    Offset(i32),
    /// Named IANA timezone identifier.
    ///
    /// When `is_local` is `true`, the paired `seconds` field carries legacy Bolt v4
    /// (`0x66`) local wall-clock seconds rather than UTC epoch seconds.
    Zone {
        /// IANA timezone identifier (for example `"Europe/Berlin"`).
        id: String,
        /// `true` for legacy Bolt v4 (`0x66`) local wall-clock timestamps.
        is_local: bool,
    },
}

/// Timezone-aware datetime value in [`ResultValue`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ResultDateTime {
    /// Seconds since Unix epoch (`1970-01-01T00:00:00Z`), or local wall-clock seconds
    /// when `tz` is [`ResultTimezone::Zone { is_local: true, .. }`].
    pub seconds: i64,
    /// Sub-second nanoseconds (`0..=999_999_999`).
    pub nanoseconds: i64,
    /// Attached fixed offset or named IANA zone.
    pub tz: ResultTimezone,
}

impl ResultDateTime {
    /// Formats this datetime as an ISO-8601 string, resolving legacy local zone offsets when possible.
    pub fn to_iso_string(&self) -> String {
        match &self.tz {
            ResultTimezone::Offset(off) => {
                let wall_secs = self.seconds.saturating_add(*off as i64);
                let days = wall_secs.div_euclid(86_400);
                let secs_of_day = wall_secs.rem_euclid(86_400);
                let (y, m, d) = days_to_ymd(days);
                let h = (secs_of_day / 3_600) as u32;
                let min = ((secs_of_day % 3_600) / 60) as u32;
                let sec = (secs_of_day % 60) as u32;
                let offset = format_tz_offset(*off as i64);
                let time_part =
                    format_time_string(h, min, sec, self.nanoseconds as u32, Some(&offset));
                format!("{y:04}-{m:02}-{d:02}T{time_part}")
            }
            ResultTimezone::Zone { id, is_local } => {
                if *is_local
                    && let Some((_, offset_secs)) =
                        crate::value::arrow::resolve_local_datetime_utc(self.seconds, id)
                {
                    let days = self.seconds.div_euclid(86_400);
                    let secs_of_day = self.seconds.rem_euclid(86_400);
                    let (y, m, d) = days_to_ymd(days);
                    let h = (secs_of_day / 3_600) as u32;
                    let min = ((secs_of_day % 3_600) / 60) as u32;
                    let sec = (secs_of_day % 60) as u32;
                    let offset = format_tz_offset(offset_secs);
                    let time_part =
                        format_time_string(h, min, sec, self.nanoseconds as u32, Some(&offset));
                    return format!("{y:04}-{m:02}-{d:02}T{time_part}[{id}]");
                }
                let days = self.seconds.div_euclid(86_400);
                let secs_of_day = self.seconds.rem_euclid(86_400);
                let (y, m, d) = days_to_ymd(days);
                let h = (secs_of_day / 3_600) as u32;
                let min = ((secs_of_day % 3_600) / 60) as u32;
                let sec = (secs_of_day % 60) as u32;
                let tz_marker = if *is_local { None } else { Some("Z") };
                let time_part = format_time_string(h, min, sec, self.nanoseconds as u32, tz_marker);
                format!("{y:04}-{m:02}-{d:02}T{time_part}[{id}]")
            }
        }
    }
}

/// Graph node decoded into [`ResultValue`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResultNode {
    /// Internal integer node identifier.
    pub id: i64,
    /// Node labels.
    pub labels: Vec<String>,
    /// Property key-value map.
    pub properties: HashMap<String, ResultValue>,
    /// Optional string element identifier (Bolt v5+).
    pub element_id: Option<String>,
}

impl ResultNode {
    /// Converts this node into the canonical kind-tagged JSON object.
    pub fn to_canonical_json(&self) -> serde_json::Value {
        let mut labels = self.labels.clone();
        labels.sort();

        let mut props = serde_json::Map::with_capacity(self.properties.len());
        for (k, v) in &self.properties {
            props.insert(k.clone(), v.to_canonical_json());
        }

        let mut obj = serde_json::Map::with_capacity(5);
        obj.insert(
            "kind".to_string(),
            serde_json::Value::String(META_KIND_NODE.to_string()),
        );
        obj.insert("id".to_string(), serde_json::Value::Number(self.id.into()));
        obj.insert(
            "element_id".to_string(),
            match &self.element_id {
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
}

/// Graph relationship decoded into [`ResultValue`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResultRelationship {
    /// Internal integer relationship identifier.
    pub id: i64,
    /// Start node integer identifier.
    pub start_node_id: i64,
    /// End node integer identifier.
    pub end_node_id: i64,
    /// Relationship type label.
    pub rel_type: String,
    /// Property key-value map.
    pub properties: HashMap<String, ResultValue>,
    /// Optional string element identifier (Bolt v5+).
    pub element_id: Option<String>,
    /// Optional start node element identifier (Bolt v5+).
    pub start_element_id: Option<String>,
    /// Optional end node element identifier (Bolt v5+).
    pub end_element_id: Option<String>,
}

impl ResultRelationship {
    /// Converts this relationship into the canonical kind-tagged JSON object.
    pub fn to_canonical_json(&self) -> serde_json::Value {
        let mut props = serde_json::Map::with_capacity(self.properties.len());
        for (k, v) in &self.properties {
            props.insert(k.clone(), v.to_canonical_json());
        }

        let mut obj = serde_json::Map::with_capacity(9);
        obj.insert(
            "kind".to_string(),
            serde_json::Value::String(META_KIND_RELATIONSHIP.to_string()),
        );
        obj.insert("id".to_string(), serde_json::Value::Number(self.id.into()));
        obj.insert(
            "element_id".to_string(),
            match &self.element_id {
                Some(eid) => serde_json::Value::String(eid.clone()),
                None => serde_json::Value::Null,
            },
        );
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(self.rel_type.clone()),
        );
        obj.insert(
            "start_id".to_string(),
            serde_json::Value::Number(self.start_node_id.into()),
        );
        obj.insert(
            "end_id".to_string(),
            serde_json::Value::Number(self.end_node_id.into()),
        );
        obj.insert("properties".to_string(), serde_json::Value::Object(props));
        if let Some(ref start_eid) = self.start_element_id {
            obj.insert(
                "start_element_id".to_string(),
                serde_json::Value::String(start_eid.clone()),
            );
        }
        if let Some(ref end_eid) = self.end_element_id {
            obj.insert(
                "end_element_id".to_string(),
                serde_json::Value::String(end_eid.clone()),
            );
        }
        serde_json::Value::Object(obj)
    }
}

/// Unbound relationship inside a path before endpoint resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultUnboundRelationship {
    /// Internal integer identifier.
    pub id: i64,
    /// Relationship type label.
    pub rel_type: String,
    /// Property key-value map.
    pub properties: HashMap<String, ResultValue>,
    /// Optional string element identifier.
    pub element_id: Option<String>,
}

/// Graph path decoded into [`ResultValue`], with ordered nodes and bound relationships.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultPath {
    /// Ordered nodes along the path.
    pub nodes: Vec<ResultNode>,
    /// Ordered bound relationships along the path.
    pub relationships: Vec<ResultRelationship>,
}

impl ResultPath {
    /// Converts this path into the canonical kind-tagged JSON object.
    pub fn to_canonical_json(&self) -> serde_json::Value {
        let nodes_json = self
            .nodes
            .iter()
            .map(ResultNode::to_canonical_json)
            .collect();
        let rels_json = self
            .relationships
            .iter()
            .map(ResultRelationship::to_canonical_json)
            .collect();

        let mut obj = serde_json::Map::with_capacity(3);
        obj.insert(
            "kind".to_string(),
            serde_json::Value::String(META_KIND_PATH.to_string()),
        );
        obj.insert("nodes".to_string(), serde_json::Value::Array(nodes_json));
        obj.insert(
            "relationships".to_string(),
            serde_json::Value::Array(rels_json),
        );
        serde_json::Value::Object(obj)
    }
}

/// Generic PackStream structure preserved when no higher-level variant matches.
#[derive(Debug, Clone, PartialEq)]
pub struct ResultStructure {
    /// 1-byte PackStream structure tag.
    pub tag: u8,
    /// Ordered structure fields.
    pub fields: Vec<ResultValue>,
}

/// Typed UTF-8 text fallback preserving a `voyager.type` annotation when a wire value
/// cannot be parsed into a native binary/temporal representation (for example PostgreSQL
/// `timetz` raw strings or out-of-range dates).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultTypedText {
    /// Canonical `voyager.type` metadata value from [`crate::value`].
    pub meta_type: &'static str,
    /// Raw text payload.
    pub text: Box<str>,
}

/// Lean, boxed output IR covering every scalar, temporal, spatial, container, and
/// graph entity result value across Bolt, FalkorDB, PostgreSQL, and Apache AGE.
#[derive(Debug, Clone, PartialEq)]
pub enum ResultValue {
    /// SQL / Cypher `NULL`.
    Null,
    /// Boolean value.
    Bool(bool),
    /// 64-bit signed integer.
    Int(i64),
    /// 64-bit IEEE-754 floating-point value.
    Float(f64),
    /// Raw binary byte buffer.
    Bytes(Box<[u8]>),
    /// UTF-8 string.
    Str(Box<str>),
    /// Exact decimal text representation (for `NUMERIC`/`DECIMAL`, including `NaN`/`Infinity`).
    Decimal(Box<str>),
    /// 16-byte raw UUID (inline, zero heap allocation).
    Uuid([u8; 16]),
    /// Raw JSON document text (`json` / `jsonb`).
    Json(Box<str>),
    /// Calendar date as days since Unix epoch (`1970-01-01`).
    Date(i64),
    /// Naive wall-clock time as nanoseconds since midnight.
    LocalTime(i64),
    /// Wall-clock time with fixed UTC offset.
    Time {
        /// Local wall-clock nanoseconds since midnight.
        nanoseconds: i64,
        /// Fixed UTC offset in seconds.
        tz_offset_seconds: i32,
    },
    /// Naive date and time as seconds since local epoch + sub-second nanoseconds.
    LocalDateTime {
        /// Seconds since local epoch (`1970-01-01T00:00:00`).
        seconds: i64,
        /// Sub-second nanoseconds (`0..=999_999_999`).
        nanoseconds: u32,
    },
    /// Timezone-aware datetime (fixed offset or named IANA zone).
    DateTime(Box<ResultDateTime>),
    /// Calendar and clock duration components.
    Duration(Box<DurationParts>),
    /// 2D spatial point.
    Point2D(Box<Point2D>),
    /// 3D spatial point.
    Point3D(Box<Point3D>),
    /// Ordered list of result values.
    List(Box<[ResultValue]>),
    /// Key-value map of result values.
    Map(Box<HashMap<String, ResultValue>>),
    /// Graph node entity.
    Node(Box<ResultNode>),
    /// Graph relationship entity.
    Relationship(Box<ResultRelationship>),
    /// Graph path entity.
    Path(Box<ResultPath>),
    /// Generic unknown PackStream structure.
    Structure(Box<ResultStructure>),
    /// Typed UTF-8 text fallback with explicit `voyager.type` annotation.
    TypedText(Box<ResultTypedText>),
}

// Hard architectural gate: keep ResultValue lean (<= 32 bytes) to prevent enum bloat.
const _: () = assert!(std::mem::size_of::<ResultValue>() <= 32);

impl ResultValue {
    /// Returns `true` if this value is [`ResultValue::Null`].
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Formats a `Date` day offset as `"YYYY-MM-DD"`.
    pub fn format_date_days(days: i64) -> String {
        let (y, m, d) = days_to_ymd(days);
        format!("{y:04}-{m:02}-{d:02}")
    }

    /// Formats a `LocalTime` nanoseconds-since-midnight value as ISO-8601.
    pub fn format_local_time_nanos(nanoseconds: i64) -> String {
        let (h, m, s, sub) = nanos_to_hmsn(nanoseconds);
        format_time_string(h, m, s, sub, None)
    }

    /// Formats a `Time` (nanoseconds + offset) value as ISO-8601.
    pub fn format_time_offset(nanoseconds: i64, tz_offset_seconds: i32) -> String {
        let (h, m, s, sub) = nanos_to_hmsn(nanoseconds);
        let offset = format_tz_offset(tz_offset_seconds as i64);
        format_time_string(h, m, s, sub, Some(&offset))
    }

    /// Formats a `LocalDateTime` value as ISO-8601.
    pub fn format_local_datetime(seconds: i64, nanoseconds: u32) -> String {
        let days = seconds.div_euclid(86_400);
        let secs_of_day = seconds.rem_euclid(86_400);
        let (y, m, d) = days_to_ymd(days);
        let h = (secs_of_day / 3_600) as u32;
        let min = ((secs_of_day % 3_600) / 60) as u32;
        let sec = (secs_of_day % 60) as u32;
        let time_part = format_time_string(h, min, sec, nanoseconds, None);
        format!("{y:04}-{m:02}-{d:02}T{time_part}")
    }

    /// Converts this value into the canonical JSON fallback representation per `spec/values/contract.md`.
    pub fn to_canonical_json(&self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(b) => serde_json::Value::Bool(*b),
            Self::Int(i) => serde_json::Value::Number((*i).into()),
            Self::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Self::Bytes(bytes) => serde_json::Value::Array(
                bytes
                    .iter()
                    .map(|b| serde_json::Value::Number((*b).into()))
                    .collect(),
            ),
            Self::Str(s) => serde_json::Value::String(s.to_string()),
            Self::Decimal(s) => serde_json::Value::String(s.to_string()),
            Self::Uuid(bytes) => serde_json::Value::String(format_uuid_bytes(bytes)),
            Self::Json(raw) => serde_json::from_str(raw)
                .unwrap_or_else(|_| serde_json::Value::String(raw.to_string())),
            Self::Date(days) => serde_json::Value::String(Self::format_date_days(*days)),
            Self::LocalTime(nanos) => {
                serde_json::Value::String(Self::format_local_time_nanos(*nanos))
            }
            Self::Time {
                nanoseconds,
                tz_offset_seconds,
            } => serde_json::Value::String(Self::format_time_offset(
                *nanoseconds,
                *tz_offset_seconds,
            )),
            Self::LocalDateTime {
                seconds,
                nanoseconds,
            } => serde_json::Value::String(Self::format_local_datetime(*seconds, *nanoseconds)),
            Self::DateTime(dt) => serde_json::Value::String(dt.to_iso_string()),
            Self::Duration(dur) => serde_json::Value::String(format_iso_duration(
                dur.months,
                dur.days,
                dur.seconds,
                dur.nanoseconds,
            )),
            Self::Point2D(pt) => {
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
            Self::Point3D(pt) => {
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
            Self::List(items) => {
                serde_json::Value::Array(items.iter().map(Self::to_canonical_json).collect())
            }
            Self::Map(map) => {
                let mut obj = serde_json::Map::with_capacity(map.len());
                for (k, v) in map.iter() {
                    obj.insert(k.clone(), v.to_canonical_json());
                }
                serde_json::Value::Object(obj)
            }
            Self::Node(node) => node.to_canonical_json(),
            Self::Relationship(rel) => rel.to_canonical_json(),
            Self::Path(path) => path.to_canonical_json(),
            Self::Structure(s) => {
                let mut obj = serde_json::Map::with_capacity(2);
                obj.insert("tag".to_string(), serde_json::Value::Number(s.tag.into()));
                obj.insert(
                    "fields".to_string(),
                    serde_json::Value::Array(
                        s.fields.iter().map(Self::to_canonical_json).collect(),
                    ),
                );
                serde_json::Value::Object(obj)
            }
            Self::TypedText(tt) => serde_json::Value::String(tt.text.to_string()),
        }
    }
}

impl From<&BoltNode> for ResultNode {
    fn from(node: &BoltNode) -> Self {
        Self {
            id: node.id,
            labels: node.labels.clone(),
            properties: node
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), ResultValue::from(v)))
                .collect(),
            element_id: node.element_id.clone(),
        }
    }
}

impl From<BoltNode> for ResultNode {
    fn from(node: BoltNode) -> Self {
        Self {
            id: node.id,
            labels: node.labels,
            properties: node
                .properties
                .into_iter()
                .map(|(k, v)| (k, ResultValue::from(v)))
                .collect(),
            element_id: node.element_id,
        }
    }
}

impl From<&BoltRelationship> for ResultRelationship {
    fn from(rel: &BoltRelationship) -> Self {
        Self {
            id: rel.id,
            start_node_id: rel.start_node_id,
            end_node_id: rel.end_node_id,
            rel_type: rel.rel_type.clone(),
            properties: rel
                .properties
                .iter()
                .map(|(k, v)| (k.clone(), ResultValue::from(v)))
                .collect(),
            element_id: rel.element_id.clone(),
            start_element_id: rel.start_element_id.clone(),
            end_element_id: rel.end_element_id.clone(),
        }
    }
}

impl From<BoltRelationship> for ResultRelationship {
    fn from(rel: BoltRelationship) -> Self {
        Self {
            id: rel.id,
            start_node_id: rel.start_node_id,
            end_node_id: rel.end_node_id,
            rel_type: rel.rel_type,
            properties: rel
                .properties
                .into_iter()
                .map(|(k, v)| (k, ResultValue::from(v)))
                .collect(),
            element_id: rel.element_id,
            start_element_id: rel.start_element_id,
            end_element_id: rel.end_element_id,
        }
    }
}

impl From<&BoltPath> for ResultPath {
    fn from(path: &BoltPath) -> Self {
        let mut nodes = Vec::new();
        let mut relationships = Vec::new();

        if path.sequence.is_empty() {
            for node in &path.nodes {
                nodes.push(ResultNode::from(node));
            }
        } else {
            if let Some(first) = path.nodes.first() {
                nodes.push(ResultNode::from(first));
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
                    nodes.push(ResultNode::from(next_node));

                    let (start_node, end_node) = if rel_code > 0 {
                        (prev_node, next_node)
                    } else {
                        (next_node, prev_node)
                    };

                    relationships.push(ResultRelationship {
                        id: unbound_rel.id,
                        start_node_id: start_node.id,
                        end_node_id: end_node.id,
                        rel_type: unbound_rel.rel_type.clone(),
                        properties: unbound_rel
                            .properties
                            .iter()
                            .map(|(k, v)| (k.clone(), ResultValue::from(v)))
                            .collect(),
                        element_id: unbound_rel.element_id.clone(),
                        start_element_id: start_node.element_id.clone(),
                        end_element_id: end_node.element_id.clone(),
                    });
                    current_node_idx = next_node_idx;
                }
            }
        }

        Self {
            nodes,
            relationships,
        }
    }
}

impl From<&BoltDateTimeZoneId> for ResultDateTime {
    fn from(dtz: &BoltDateTimeZoneId) -> Self {
        Self {
            seconds: dtz.seconds,
            nanoseconds: dtz.nanoseconds,
            tz: ResultTimezone::Zone {
                id: dtz.zone_id.clone(),
                is_local: dtz.is_local,
            },
        }
    }
}

impl From<&BoltValue> for ResultValue {
    fn from(val: &BoltValue) -> Self {
        match val {
            BoltValue::Null => Self::Null,
            BoltValue::Boolean(b) => Self::Bool(*b),
            BoltValue::Integer(i) => Self::Int(*i),
            BoltValue::Float(f) => Self::Float(*f),
            BoltValue::Bytes(b) => Self::Bytes(b.clone().into_boxed_slice()),
            BoltValue::String(s) => Self::Str(s.clone().into_boxed_str()),
            BoltValue::List(items) => Self::List(
                items
                    .iter()
                    .map(Self::from)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            BoltValue::Map(map) => Self::Map(Box::new(
                map.iter()
                    .map(|(k, v)| (k.clone(), Self::from(v)))
                    .collect(),
            )),
            BoltValue::Structure { tag, fields } => Self::Structure(Box::new(ResultStructure {
                tag: *tag,
                fields: fields.iter().map(Self::from).collect(),
            })),
            BoltValue::Node(n) => Self::Node(Box::new(ResultNode::from(n))),
            BoltValue::Relationship(r) => Self::Relationship(Box::new(ResultRelationship::from(r))),
            BoltValue::Path(p) => Self::Path(Box::new(ResultPath::from(p))),
            BoltValue::Point2D(pt) => Self::Point2D(Box::new(Point2D {
                srid: pt.srid,
                x: pt.x,
                y: pt.y,
            })),
            BoltValue::Point3D(pt) => Self::Point3D(Box::new(Point3D {
                srid: pt.srid,
                x: pt.x,
                y: pt.y,
                z: pt.z,
            })),
            BoltValue::Date(d) => Self::Date(d.days),
            BoltValue::LocalTime(lt) => Self::LocalTime(lt.nanoseconds),
            BoltValue::Time(t) => Self::Time {
                nanoseconds: t.nanoseconds,
                tz_offset_seconds: t.tz_offset_seconds as i32,
            },
            BoltValue::LocalDateTime(ldt) => Self::LocalDateTime {
                seconds: ldt.seconds,
                nanoseconds: ldt.nanoseconds as u32,
            },
            BoltValue::DateTime(dt) => Self::DateTime(Box::new(ResultDateTime {
                seconds: dt.seconds,
                nanoseconds: dt.nanoseconds,
                tz: ResultTimezone::Offset(dt.tz_offset_seconds as i32),
            })),
            BoltValue::DateTimeZoneId(dtz) => Self::DateTime(Box::new(ResultDateTime::from(dtz))),
            BoltValue::Duration(d) => Self::Duration(Box::new(DurationParts {
                months: d.months,
                days: d.days,
                seconds: d.seconds,
                nanoseconds: d.nanoseconds,
            })),
        }
    }
}

impl From<BoltValue> for ResultValue {
    fn from(val: BoltValue) -> Self {
        match val {
            BoltValue::Null => Self::Null,
            BoltValue::Boolean(b) => Self::Bool(b),
            BoltValue::Integer(i) => Self::Int(i),
            BoltValue::Float(f) => Self::Float(f),
            BoltValue::Bytes(b) => Self::Bytes(b.into_boxed_slice()),
            BoltValue::String(s) => Self::Str(s.into_boxed_str()),
            BoltValue::List(items) => Self::List(
                items
                    .into_iter()
                    .map(Self::from)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            BoltValue::Map(map) => Self::Map(Box::new(
                map.into_iter().map(|(k, v)| (k, Self::from(v))).collect(),
            )),
            BoltValue::Structure { tag, fields } => Self::Structure(Box::new(ResultStructure {
                tag,
                fields: fields.into_iter().map(Self::from).collect(),
            })),
            BoltValue::Node(n) => Self::Node(Box::new(ResultNode::from(n))),
            BoltValue::Relationship(r) => Self::Relationship(Box::new(ResultRelationship::from(r))),
            BoltValue::Path(p) => Self::Path(Box::new(ResultPath::from(&p))),
            BoltValue::Point2D(pt) => Self::Point2D(Box::new(Point2D {
                srid: pt.srid,
                x: pt.x,
                y: pt.y,
            })),
            BoltValue::Point3D(pt) => Self::Point3D(Box::new(Point3D {
                srid: pt.srid,
                x: pt.x,
                y: pt.y,
                z: pt.z,
            })),
            BoltValue::Date(d) => Self::Date(d.days),
            BoltValue::LocalTime(lt) => Self::LocalTime(lt.nanoseconds),
            BoltValue::Time(t) => Self::Time {
                nanoseconds: t.nanoseconds,
                tz_offset_seconds: t.tz_offset_seconds as i32,
            },
            BoltValue::LocalDateTime(ldt) => Self::LocalDateTime {
                seconds: ldt.seconds,
                nanoseconds: ldt.nanoseconds as u32,
            },
            BoltValue::DateTime(dt) => Self::DateTime(Box::new(ResultDateTime {
                seconds: dt.seconds,
                nanoseconds: dt.nanoseconds,
                tz: ResultTimezone::Offset(dt.tz_offset_seconds as i32),
            })),
            BoltValue::DateTimeZoneId(dtz) => Self::DateTime(Box::new(ResultDateTime {
                seconds: dtz.seconds,
                nanoseconds: dtz.nanoseconds,
                tz: ResultTimezone::Zone {
                    id: dtz.zone_id,
                    is_local: dtz.is_local,
                },
            })),
            BoltValue::Duration(d) => Self::Duration(Box::new(DurationParts {
                months: d.months,
                days: d.days,
                seconds: d.seconds,
                nanoseconds: d.nanoseconds,
            })),
        }
    }
}
