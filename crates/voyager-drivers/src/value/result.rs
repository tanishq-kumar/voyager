//! Lean output value intermediate representation ([`ResultValue`]).
//!
//! [`ResultValue`] is strictly separate from [`crate::value::param::ParamValue`]
//! (query inputs) and from protocol wire types such as [`crate::bolt::BoltValue`].
//! Heavy variants are boxed so `std::mem::size_of::<ResultValue>() <= 32` bytes
//! (enforced at compile time below).

use std::collections::HashMap;
use std::sync::Arc;

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
        id: Arc<str>,
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

/// Compact key-value map for [`ResultValue`] maps and graph entity properties.
///
/// Stores entries in a flat contiguous `Box<[(Arc<str>, ResultValue)]>` slice instead of
/// a `HashMap`, avoiding per-entity SipHash overhead and hash table allocation waste on
/// typical small property maps while preserving order-independent [`PartialEq`] and
/// exact key-value semantics.
#[derive(Debug, Clone, Default)]
pub struct ResultMap(Box<[(Arc<str>, ResultValue)]>);

impl ResultMap {
    /// Creates an empty map without heap allocation.
    #[inline]
    pub fn new() -> Self {
        Self(Box::from([]))
    }

    /// Creates a map from already-deduplicated `(key, value)` entries.
    #[inline]
    pub fn from_entries(entries: Vec<(Arc<str>, ResultValue)>) -> Self {
        Self(entries.into_boxed_slice())
    }

    /// Returns the number of entries in this map.
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if this map contains no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Iterates over `(key, value)` pairs in stored order.
    #[inline]
    pub fn iter(&self) -> std::slice::Iter<'_, (Arc<str>, ResultValue)> {
        self.0.iter()
    }

    /// Looks up a value by key name.
    #[inline]
    pub fn get(&self, key: &str) -> Option<&ResultValue> {
        self.0
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, v)| v)
    }
}

impl PartialEq for ResultMap {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        if self.0.len() <= 16 {
            self.0.iter().all(|(k, v)| other.get(k.as_ref()) == Some(v))
        } else {
            let other_map: HashMap<&str, &ResultValue> =
                other.0.iter().map(|(k, v)| (k.as_ref(), v)).collect();
            self.0
                .iter()
                .all(|(k, v)| other_map.get(k.as_ref()).copied() == Some(v))
        }
    }
}

impl FromIterator<(Arc<str>, ResultValue)> for ResultMap {
    fn from_iter<T: IntoIterator<Item = (Arc<str>, ResultValue)>>(iter: T) -> Self {
        let entries: Vec<(Arc<str>, ResultValue)> = iter.into_iter().collect();
        Self(entries.into_boxed_slice())
    }
}

impl<'a> IntoIterator for &'a ResultMap {
    type Item = &'a (Arc<str>, ResultValue);
    type IntoIter = std::slice::Iter<'a, (Arc<str>, ResultValue)>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

/// Graph node decoded into [`ResultValue`].
#[derive(Debug, Clone, PartialEq)]
pub struct ResultNode {
    /// Internal integer node identifier.
    pub id: i64,
    /// Node labels (internable across rows via [`Arc<str>`]).
    pub labels: Vec<Arc<str>>,
    /// Property key-value map (keys internable across rows via [`Arc<str>`]).
    pub properties: ResultMap,
    /// Optional string element identifier (Bolt v5+).
    pub element_id: Option<Arc<str>>,
}

impl ResultNode {
    /// Converts this node into the canonical kind-tagged JSON object.
    pub fn to_canonical_json(&self) -> serde_json::Value {
        let mut labels: Vec<&str> = self.labels.iter().map(AsRef::as_ref).collect();
        labels.sort_unstable();

        let mut props = serde_json::Map::with_capacity(self.properties.len());
        for (k, v) in &self.properties {
            props.insert(k.to_string(), v.to_canonical_json());
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
                Some(eid) => serde_json::Value::String(eid.to_string()),
                None => serde_json::Value::Null,
            },
        );
        obj.insert(
            "labels".to_string(),
            serde_json::Value::Array(
                labels
                    .into_iter()
                    .map(|l| serde_json::Value::String(l.to_owned()))
                    .collect(),
            ),
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
    /// Relationship type label (internable across rows via [`Arc<str>`]).
    pub rel_type: Arc<str>,
    /// Property key-value map (keys internable across rows via [`Arc<str>`]).
    pub properties: ResultMap,
    /// Optional string element identifier (Bolt v5+).
    pub element_id: Option<Arc<str>>,
    /// Optional start node element identifier (Bolt v5+).
    pub start_element_id: Option<Arc<str>>,
    /// Optional end node element identifier (Bolt v5+).
    pub end_element_id: Option<Arc<str>>,
}

impl ResultRelationship {
    /// Converts this relationship into the canonical kind-tagged JSON object.
    pub fn to_canonical_json(&self) -> serde_json::Value {
        let mut props = serde_json::Map::with_capacity(self.properties.len());
        for (k, v) in &self.properties {
            props.insert(k.to_string(), v.to_canonical_json());
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
                Some(eid) => serde_json::Value::String(eid.to_string()),
                None => serde_json::Value::Null,
            },
        );
        obj.insert(
            "type".to_string(),
            serde_json::Value::String(self.rel_type.to_string()),
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
                serde_json::Value::String(start_eid.to_string()),
            );
        }
        if let Some(ref end_eid) = self.end_element_id {
            obj.insert(
                "end_element_id".to_string(),
                serde_json::Value::String(end_eid.to_string()),
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
    pub rel_type: Arc<str>,
    /// Property key-value map.
    pub properties: ResultMap,
    /// Optional string element identifier.
    pub element_id: Option<Arc<str>>,
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
    Map(ResultMap),
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

    /// Returns a zero-DOM [`serde::Serialize`] view producing JSON byte-identical to
    /// [`Self::to_canonical_json`].
    pub fn as_canonical_json(&self) -> CanonicalJson<'_> {
        CanonicalJson(self)
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
                    obj.insert(k.to_string(), v.to_canonical_json());
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
            labels: node.labels.iter().map(|s| Arc::from(s.as_str())).collect(),
            properties: node
                .properties
                .iter()
                .map(|(k, v)| (Arc::from(k.as_str()), ResultValue::from(v)))
                .collect(),
            element_id: node.element_id.as_deref().map(Arc::from),
        }
    }
}

impl From<BoltNode> for ResultNode {
    fn from(node: BoltNode) -> Self {
        Self {
            id: node.id,
            labels: node.labels.into_iter().map(Arc::from).collect(),
            properties: node
                .properties
                .into_iter()
                .map(|(k, v)| (Arc::from(k), ResultValue::from(v)))
                .collect(),
            element_id: node.element_id.map(Arc::from),
        }
    }
}

impl From<&BoltRelationship> for ResultRelationship {
    fn from(rel: &BoltRelationship) -> Self {
        Self {
            id: rel.id,
            start_node_id: rel.start_node_id,
            end_node_id: rel.end_node_id,
            rel_type: Arc::from(rel.rel_type.as_str()),
            properties: rel
                .properties
                .iter()
                .map(|(k, v)| (Arc::from(k.as_str()), ResultValue::from(v)))
                .collect(),
            element_id: rel.element_id.as_deref().map(Arc::from),
            start_element_id: rel.start_element_id.as_deref().map(Arc::from),
            end_element_id: rel.end_element_id.as_deref().map(Arc::from),
        }
    }
}

impl From<BoltRelationship> for ResultRelationship {
    fn from(rel: BoltRelationship) -> Self {
        Self {
            id: rel.id,
            start_node_id: rel.start_node_id,
            end_node_id: rel.end_node_id,
            rel_type: Arc::from(rel.rel_type),
            properties: rel
                .properties
                .into_iter()
                .map(|(k, v)| (Arc::from(k), ResultValue::from(v)))
                .collect(),
            element_id: rel.element_id.map(Arc::from),
            start_element_id: rel.start_element_id.map(Arc::from),
            end_element_id: rel.end_element_id.map(Arc::from),
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
                        rel_type: Arc::from(unbound_rel.rel_type.as_str()),
                        properties: unbound_rel
                            .properties
                            .iter()
                            .map(|(k, v)| (Arc::from(k.as_str()), ResultValue::from(v)))
                            .collect(),
                        element_id: unbound_rel.element_id.as_deref().map(Arc::from),
                        start_element_id: start_node.element_id.as_deref().map(Arc::from),
                        end_element_id: end_node.element_id.as_deref().map(Arc::from),
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
                id: Arc::from(dtz.zone_id.as_str()),
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
            BoltValue::Map(map) => Self::Map(
                map.iter()
                    .map(|(k, v)| (Arc::from(k.as_str()), Self::from(v)))
                    .collect(),
            ),
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
            BoltValue::Map(map) => Self::Map(
                map.into_iter()
                    .map(|(k, v)| (Arc::from(k), Self::from(v)))
                    .collect(),
            ),
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
                    id: Arc::from(dtz.zone_id),
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

/// Zero-DOM [`serde::Serialize`] view over [`ResultValue`] that emits the exact
/// canonical JSON bytes of [`ResultValue::to_canonical_json`] without building an
/// intermediate [`serde_json::Value`] tree.
pub struct CanonicalJson<'a>(pub &'a ResultValue);

struct FiniteF64(f64);

impl serde::Serialize for FiniteF64 {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_finite() {
            serializer.serialize_f64(self.0)
        } else {
            serializer.serialize_unit()
        }
    }
}

struct SortedLabels<'a>(&'a [Arc<str>]);

impl serde::Serialize for SortedLabels<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        if self.0.len() <= 1 || self.0.windows(2).all(|w| w[0] <= w[1]) {
            let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
            for label in self.0 {
                seq.serialize_element(label.as_ref())?;
            }
            seq.end()
        } else {
            let mut sorted: Vec<&str> = self.0.iter().map(AsRef::as_ref).collect();
            sorted.sort_unstable();
            let mut seq = serializer.serialize_seq(Some(sorted.len()))?;
            for label in sorted {
                seq.serialize_element(label)?;
            }
            seq.end()
        }
    }
}

struct CanonicalMap<'a>(&'a ResultMap);

impl serde::Serialize for CanonicalMap<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0 {
            map.serialize_entry(k.as_ref(), &CanonicalJson(v))?;
        }
        map.end()
    }
}

struct CanonicalNode<'a>(&'a ResultNode);

impl serde::Serialize for CanonicalNode<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("kind", META_KIND_NODE)?;
        map.serialize_entry("id", &self.0.id)?;
        map.serialize_entry("element_id", &self.0.element_id.as_deref())?;
        map.serialize_entry("labels", &SortedLabels(&self.0.labels))?;
        map.serialize_entry("properties", &CanonicalMap(&self.0.properties))?;
        map.end()
    }
}

struct CanonicalRelationship<'a>(&'a ResultRelationship);

impl serde::Serialize for CanonicalRelationship<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let len = 7
            + usize::from(self.0.start_element_id.is_some())
            + usize::from(self.0.end_element_id.is_some());
        let mut map = serializer.serialize_map(Some(len))?;
        map.serialize_entry("kind", META_KIND_RELATIONSHIP)?;
        map.serialize_entry("id", &self.0.id)?;
        map.serialize_entry("element_id", &self.0.element_id.as_deref())?;
        map.serialize_entry("type", self.0.rel_type.as_ref())?;
        map.serialize_entry("start_id", &self.0.start_node_id)?;
        map.serialize_entry("end_id", &self.0.end_node_id)?;
        map.serialize_entry("properties", &CanonicalMap(&self.0.properties))?;
        if let Some(ref start_eid) = self.0.start_element_id {
            map.serialize_entry("start_element_id", start_eid.as_ref())?;
        }
        if let Some(ref end_eid) = self.0.end_element_id {
            map.serialize_entry("end_element_id", end_eid.as_ref())?;
        }
        map.end()
    }
}

struct CanonicalPath<'a>(&'a ResultPath);

impl serde::Serialize for CanonicalPath<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        struct NodeSlice<'a>(&'a [ResultNode]);
        impl serde::Serialize for NodeSlice<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for n in self.0 {
                    seq.serialize_element(&CanonicalNode(n))?;
                }
                seq.end()
            }
        }
        struct RelSlice<'a>(&'a [ResultRelationship]);
        impl serde::Serialize for RelSlice<'_> {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                for r in self.0 {
                    seq.serialize_element(&CanonicalRelationship(r))?;
                }
                seq.end()
            }
        }
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("kind", META_KIND_PATH)?;
        map.serialize_entry("nodes", &NodeSlice(&self.0.nodes))?;
        map.serialize_entry("relationships", &RelSlice(&self.0.relationships))?;
        map.end()
    }
}

impl serde::Serialize for CanonicalJson<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap, SerializeSeq};
        match self.0 {
            ResultValue::Null => serializer.serialize_unit(),
            ResultValue::Bool(b) => serializer.serialize_bool(*b),
            ResultValue::Int(i) => serializer.serialize_i64(*i),
            ResultValue::Float(f) => FiniteF64(*f).serialize(serializer),
            ResultValue::Bytes(bytes) => {
                let mut seq = serializer.serialize_seq(Some(bytes.len()))?;
                for b in bytes.iter() {
                    seq.serialize_element(b)?;
                }
                seq.end()
            }
            ResultValue::Str(s) | ResultValue::Decimal(s) => serializer.serialize_str(s),
            ResultValue::Uuid(bytes) => serializer.serialize_str(&format_uuid_bytes(bytes)),
            ResultValue::Json(raw) => match serde_json::from_str::<serde_json::Value>(raw) {
                Ok(parsed) => parsed.serialize(serializer),
                Err(_) => serializer.serialize_str(raw),
            },
            ResultValue::Date(days) => {
                serializer.serialize_str(&ResultValue::format_date_days(*days))
            }
            ResultValue::LocalTime(nanos) => {
                serializer.serialize_str(&ResultValue::format_local_time_nanos(*nanos))
            }
            ResultValue::Time {
                nanoseconds,
                tz_offset_seconds,
            } => serializer.serialize_str(&ResultValue::format_time_offset(
                *nanoseconds,
                *tz_offset_seconds,
            )),
            ResultValue::LocalDateTime {
                seconds,
                nanoseconds,
            } => serializer
                .serialize_str(&ResultValue::format_local_datetime(*seconds, *nanoseconds)),
            ResultValue::DateTime(dt) => serializer.serialize_str(&dt.to_iso_string()),
            ResultValue::Duration(dur) => serializer.serialize_str(&format_iso_duration(
                dur.months,
                dur.days,
                dur.seconds,
                dur.nanoseconds,
            )),
            ResultValue::Point2D(pt) => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("srid", &pt.srid)?;
                map.serialize_entry("x", &FiniteF64(pt.x))?;
                map.serialize_entry("y", &FiniteF64(pt.y))?;
                map.end()
            }
            ResultValue::Point3D(pt) => {
                let mut map = serializer.serialize_map(Some(4))?;
                map.serialize_entry("srid", &pt.srid)?;
                map.serialize_entry("x", &FiniteF64(pt.x))?;
                map.serialize_entry("y", &FiniteF64(pt.y))?;
                map.serialize_entry("z", &FiniteF64(pt.z))?;
                map.end()
            }
            ResultValue::List(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items.iter() {
                    seq.serialize_element(&CanonicalJson(item))?;
                }
                seq.end()
            }
            ResultValue::Map(map) => CanonicalMap(map).serialize(serializer),
            ResultValue::Node(node) => CanonicalNode(node).serialize(serializer),
            ResultValue::Relationship(rel) => CanonicalRelationship(rel).serialize(serializer),
            ResultValue::Path(path) => CanonicalPath(path).serialize(serializer),
            ResultValue::Structure(s) => {
                struct FieldSlice<'a>(&'a [ResultValue]);
                impl serde::Serialize for FieldSlice<'_> {
                    fn serialize<S: serde::Serializer>(
                        &self,
                        serializer: S,
                    ) -> Result<S::Ok, S::Error> {
                        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
                        for f in self.0 {
                            seq.serialize_element(&CanonicalJson(f))?;
                        }
                        seq.end()
                    }
                }
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("tag", &s.tag)?;
                map.serialize_entry("fields", &FieldSlice(&s.fields))?;
                map.end()
            }
            ResultValue::TypedText(tt) => serializer.serialize_str(&tt.text),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::META_TYPE_DECIMAL;

    fn assert_canonical_json_identical(val: &ResultValue) {
        let via_dom = serde_json::to_string(&val.to_canonical_json()).unwrap();
        let via_stream = serde_json::to_string(&val.as_canonical_json()).unwrap();
        assert_eq!(via_stream, via_dom, "mismatch for {val:?}");
    }

    #[test]
    fn canonical_json_stream_is_byte_identical_to_dom() {
        let node_props = ResultMap::from_entries(vec![
            (Arc::from("name"), ResultValue::Str("Alice".into())),
            (Arc::from("age"), ResultValue::Int(30)),
        ]);
        let node = ResultNode {
            id: 7,
            labels: vec![Arc::from("Zebra"), Arc::from("Alpha")],
            properties: node_props,
            element_id: Some(Arc::from("4:abc:7")),
        };
        let node_no_eid = ResultNode {
            id: 8,
            labels: vec![Arc::from("Only")],
            properties: ResultMap::new(),
            element_id: None,
        };
        let rel_props =
            ResultMap::from_entries(vec![(Arc::from("weight"), ResultValue::Float(1.5))]);
        let rel = ResultRelationship {
            id: 11,
            start_node_id: 7,
            end_node_id: 8,
            rel_type: Arc::from("KNOWS"),
            properties: rel_props,
            element_id: Some(Arc::from("5:abc:11")),
            start_element_id: Some(Arc::from("4:abc:7")),
            end_element_id: Some(Arc::from("4:abc:8")),
        };
        let rel_minimal = ResultRelationship {
            id: 12,
            start_node_id: 7,
            end_node_id: 8,
            rel_type: Arc::from("LIKES"),
            properties: ResultMap::new(),
            element_id: None,
            start_element_id: None,
            end_element_id: None,
        };
        let path = ResultPath {
            nodes: vec![node.clone(), node_no_eid.clone()],
            relationships: vec![rel.clone()],
        };

        let map = ResultMap::from_entries(vec![
            (Arc::from("a"), ResultValue::Int(1)),
            (Arc::from("b"), ResultValue::Float(f64::NAN)),
            (Arc::from("c"), ResultValue::Float(f64::INFINITY)),
            (Arc::from("d"), ResultValue::Float(f64::NEG_INFINITY)),
            (Arc::from("e"), ResultValue::Float(-0.0)),
        ]);

        let cases = vec![
            ResultValue::Null,
            ResultValue::Bool(true),
            ResultValue::Int(-42),
            ResultValue::Float(3.125),
            ResultValue::Float(-0.0),
            ResultValue::Float(f64::NAN),
            ResultValue::Float(f64::INFINITY),
            ResultValue::Float(f64::NEG_INFINITY),
            ResultValue::Bytes(vec![0, 1, 255].into_boxed_slice()),
            ResultValue::Str("hello \"world\"\n".into()),
            ResultValue::Decimal("12345.67890".into()),
            ResultValue::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            ResultValue::Json(r#"{ "k" : [1, true, null] }"#.into()),
            ResultValue::Json("not valid json".into()),
            ResultValue::Date(19_723),
            ResultValue::LocalTime(12 * 3_600 * 1_000_000_000 + 123_456_789),
            ResultValue::Time {
                nanoseconds: 9 * 3_600 * 1_000_000_000,
                tz_offset_seconds: 7_200,
            },
            ResultValue::LocalDateTime {
                seconds: 1_700_000_000,
                nanoseconds: 500_000,
            },
            ResultValue::DateTime(Box::new(ResultDateTime {
                seconds: 1_700_000_000,
                nanoseconds: 250_000,
                tz: ResultTimezone::Offset(-18_000),
            })),
            ResultValue::DateTime(Box::new(ResultDateTime {
                seconds: 1_700_000_000,
                nanoseconds: 0,
                tz: ResultTimezone::Zone {
                    id: Arc::from("Europe/Berlin"),
                    is_local: false,
                },
            })),
            ResultValue::Duration(Box::new(DurationParts {
                months: 14,
                days: 3,
                seconds: 45,
                nanoseconds: 123_000,
            })),
            ResultValue::Point2D(Box::new(Point2D {
                srid: 4326,
                x: 12.5,
                y: f64::NAN,
            })),
            ResultValue::Point3D(Box::new(Point3D {
                srid: 4979,
                x: 1.0,
                y: 2.0,
                z: f64::INFINITY,
            })),
            ResultValue::List(vec![ResultValue::Int(1), ResultValue::Null].into_boxed_slice()),
            ResultValue::Map(map),
            ResultValue::Node(Box::new(node)),
            ResultValue::Node(Box::new(node_no_eid)),
            ResultValue::Relationship(Box::new(rel)),
            ResultValue::Relationship(Box::new(rel_minimal)),
            ResultValue::Path(Box::new(path)),
            ResultValue::Structure(Box::new(ResultStructure {
                tag: 0x4E,
                fields: vec![ResultValue::Int(1), ResultValue::Str("x".into())],
            })),
            ResultValue::TypedText(Box::new(ResultTypedText {
                meta_type: META_TYPE_DECIMAL,
                text: "99.99".into(),
            })),
        ];

        for case in &cases {
            assert_canonical_json_identical(case);
        }
    }
}
