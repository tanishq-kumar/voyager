//! Apache AGE `agtype` parser, graph entity structures, and Apache Arrow RecordBatch deserialization.

use arrow_array::RecordBatch;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::bolt::packstream::{
    BoltDate, BoltDateTime, BoltDuration, BoltLocalDateTime, BoltLocalTime, BoltTime, ymd_to_days,
};
use crate::bolt::{BoltNode, BoltPath, BoltRelationship, BoltValue};
use crate::engine::{QueryResult, QuerySummary, ResultMode};
use crate::error::{NetError, Result};
use crate::value::arrow::{ColumnHint, EmptyColumnKind, values_to_record_batch_with_hints};
use crate::value::result::{
    ResultDateTime, ResultNode, ResultPath, ResultRelationship, ResultTimezone, ResultTypedText,
    ResultValue,
};
use crate::value::{
    DurationParts, META_TYPE_DATE, META_TYPE_DATETIME_TZ, META_TYPE_DECIMAL, META_TYPE_DURATION,
    META_TYPE_LOCAL_DATETIME, META_TYPE_TIME, META_TYPE_TIME_TZ, META_TYPE_UUID,
};

/// Apache AGE Graph Vertex entity (`::vertex`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgeVertex {
    /// Graph internal 64-bit vertex identifier.
    pub id: i64,
    /// Vertex label / type tag.
    pub label: String,
    /// Key-value vertex property map.
    pub properties: HashMap<String, serde_json::Value>,
}

impl AgeVertex {
    /// Converts this `AgeVertex` into a Bolt-compatible [`BoltNode`].
    pub fn to_bolt_node(&self) -> BoltNode {
        let mut props = HashMap::new();
        for (k, v) in &self.properties {
            props.insert(k.clone(), from_json_to_bolt_value(v));
        }
        BoltNode {
            id: self.id,
            labels: vec![self.label.clone()],
            properties: props,
            element_id: Some(self.id.to_string()),
        }
    }
}

/// Apache AGE Graph Edge / Relationship entity (`::edge`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgeEdge {
    /// Graph internal 64-bit edge identifier.
    pub id: i64,
    /// Edge relationship label / type name.
    pub label: String,
    /// Start (source) vertex 64-bit identifier.
    pub start_id: i64,
    /// End (target) vertex 64-bit identifier.
    pub end_id: i64,
    /// Key-value edge property map.
    pub properties: HashMap<String, serde_json::Value>,
}

impl AgeEdge {
    /// Converts this `AgeEdge` into a Bolt-compatible [`BoltRelationship`].
    pub fn to_bolt_relationship(&self) -> BoltRelationship {
        let mut props = HashMap::new();
        for (k, v) in &self.properties {
            props.insert(k.clone(), from_json_to_bolt_value(v));
        }
        BoltRelationship {
            id: self.id,
            start_node_id: self.start_id,
            end_node_id: self.end_id,
            rel_type: self.label.clone(),
            properties: props,
            element_id: Some(self.id.to_string()),
            start_element_id: Some(self.start_id.to_string()),
            end_element_id: Some(self.end_id.to_string()),
        }
    }
}

/// Apache AGE Graph Path entity (`::path`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgePath {
    /// Ordered sequence of vertices in the path.
    pub vertices: Vec<AgeVertex>,
    /// Ordered sequence of edges connecting the vertices in the path.
    pub edges: Vec<AgeEdge>,
}

impl AgePath {
    /// Converts this `AgePath` into a Bolt-compatible [`BoltPath`].
    pub fn to_bolt_path(&self) -> BoltPath {
        let nodes: Vec<BoltNode> = self.vertices.iter().map(|v| v.to_bolt_node()).collect();
        let rels: Vec<crate::bolt::BoltUnboundRelationship> = self
            .edges
            .iter()
            .map(|e| {
                let mut props = HashMap::new();
                for (k, v) in &e.properties {
                    props.insert(k.clone(), from_json_to_bolt_value(v));
                }
                crate::bolt::BoltUnboundRelationship {
                    id: e.id,
                    rel_type: e.label.clone(),
                    properties: props,
                    element_id: Some(e.id.to_string()),
                }
            })
            .collect();

        let sequence: Vec<i64> = (1..=(self.edges.len() as i64)).collect();

        BoltPath {
            nodes,
            relationships: rels,
            sequence,
        }
    }
}

fn from_json_to_bolt_value(val: &serde_json::Value) -> BoltValue {
    match val {
        serde_json::Value::Null => BoltValue::Null,
        serde_json::Value::Bool(b) => BoltValue::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                BoltValue::Integer(i)
            } else if let Some(f) = n.as_f64() {
                BoltValue::Float(f)
            } else {
                BoltValue::String(n.to_string())
            }
        }
        serde_json::Value::String(s) => BoltValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            BoltValue::List(arr.iter().map(from_json_to_bolt_value).collect())
        }
        serde_json::Value::Object(obj) => {
            let mut map = HashMap::new();
            for (k, v) in obj {
                map.insert(k.clone(), from_json_to_bolt_value(v));
            }
            BoltValue::Map(map)
        }
    }
}

/// Parsed Apache AGE dynamic agtype value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AgeValue {
    /// Null value.
    Null,
    /// Boolean scalar.
    Boolean(bool),
    /// 64-bit integer scalar.
    Integer(i64),
    /// 64-bit float scalar.
    Float(f64),
    /// String scalar.
    String(String),
    /// Graph vertex entity.
    Vertex(AgeVertex),
    /// Graph edge entity.
    Edge(AgeEdge),
    /// Graph path entity.
    Path(AgePath),
    /// Homogeneous or heterogeneous array list.
    List(Vec<AgeValue>),
    /// Dynamic property map.
    Map(HashMap<String, AgeValue>),
}

/// Strips Apache AGE type annotations (such as `::vertex`, `::edge`, `::path`, `::numeric`) from a raw string while preserving content inside quotes.
///
/// Operates entirely in byte space and rebuilds the output from whole-byte runs, so
/// multi-byte UTF-8 characters (both inside and outside quotes) are preserved exactly.
pub fn clean_agtype_string(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(s.len());
    let mut in_quote = false;
    let mut escaped = false;
    let mut i = 0;

    while i < bytes.len() {
        let b = bytes[i];
        if in_quote {
            out.push(b);
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_quote = false;
            }
            i += 1;
        } else if b == b'"' {
            in_quote = true;
            out.push(b'"');
            i += 1;
        } else if bytes[i..].starts_with(b"::vertex") {
            i += b"::vertex".len();
        } else if bytes[i..].starts_with(b"::edge") {
            i += b"::edge".len();
        } else if bytes[i..].starts_with(b"::path") {
            i += b"::path".len();
        } else if bytes[i..].starts_with(b"::numeric") {
            i += b"::numeric".len();
        } else {
            out.push(b);
            i += 1;
        }
    }
    // The input is valid UTF-8 and only complete ASCII runs are skipped, so the
    // copied bytes are always valid UTF-8; lossy conversion is a defensive no-op.
    String::from_utf8_lossy(&out).trim().to_string()
}

/// Converts a parsed agtype value into the canonical `.to_dict()` JSON shape
/// used in Arrow/Polars columns and `GraphNode`/`GraphRelationship`/`GraphPath`.
pub fn age_value_to_arrow_json(val: &AgeValue) -> serde_json::Value {
    match val {
        AgeValue::Null => serde_json::Value::Null,
        AgeValue::Boolean(b) => serde_json::Value::Bool(*b),
        AgeValue::Integer(i) => serde_json::Value::Number((*i).into()),
        AgeValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        AgeValue::String(s) => serde_json::Value::String(s.clone()),
        AgeValue::Vertex(v) => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "kind".to_string(),
                serde_json::Value::String("node".to_string()),
            );
            obj.insert("id".to_string(), serde_json::Value::Number(v.id.into()));
            obj.insert("element_id".to_string(), serde_json::Value::Null);
            let labels = if v.label.is_empty() {
                Vec::new()
            } else {
                vec![serde_json::Value::String(v.label.clone())]
            };
            obj.insert("labels".to_string(), serde_json::Value::Array(labels));
            obj.insert(
                "properties".to_string(),
                serde_json::Value::Object(
                    v.properties
                        .iter()
                        .map(|(k, value)| (k.clone(), value.clone()))
                        .collect(),
                ),
            );
            serde_json::Value::Object(obj)
        }
        AgeValue::Edge(e) => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "kind".to_string(),
                serde_json::Value::String("relationship".to_string()),
            );
            obj.insert("id".to_string(), serde_json::Value::Number(e.id.into()));
            obj.insert("element_id".to_string(), serde_json::Value::Null);
            obj.insert(
                "type".to_string(),
                serde_json::Value::String(e.label.clone()),
            );
            obj.insert(
                "start_id".to_string(),
                serde_json::Value::Number(e.start_id.into()),
            );
            obj.insert(
                "end_id".to_string(),
                serde_json::Value::Number(e.end_id.into()),
            );
            obj.insert(
                "properties".to_string(),
                serde_json::Value::Object(
                    e.properties
                        .iter()
                        .map(|(k, value)| (k.clone(), value.clone()))
                        .collect(),
                ),
            );
            serde_json::Value::Object(obj)
        }
        AgeValue::Path(p) => {
            let nodes: Vec<serde_json::Value> = p
                .vertices
                .iter()
                .map(|v| age_value_to_arrow_json(&AgeValue::Vertex(v.clone())))
                .collect();
            let relationships: Vec<serde_json::Value> = p
                .edges
                .iter()
                .map(|e| age_value_to_arrow_json(&AgeValue::Edge(e.clone())))
                .collect();
            let mut obj = serde_json::Map::new();
            obj.insert(
                "kind".to_string(),
                serde_json::Value::String("path".to_string()),
            );
            obj.insert("nodes".to_string(), serde_json::Value::Array(nodes));
            obj.insert(
                "relationships".to_string(),
                serde_json::Value::Array(relationships),
            );
            serde_json::Value::Object(obj)
        }
        AgeValue::List(list) => {
            serde_json::Value::Array(list.iter().map(age_value_to_arrow_json).collect())
        }
        AgeValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), age_value_to_arrow_json(v));
            }
            serde_json::Value::Object(obj)
        }
    }
}

/// Converts a parsed agtype value into the canonical Python-facing shape.
///
/// Vertices become `{"kind": "node", "id", "labels", "properties"}`, edges
/// become `{"kind": "relationship", "id", "type", "start_id", "end_id",
/// "properties"}` and paths become `{"kind": "path", "nodes", "relationships",
/// "sequence"}`. Lists and maps recurse, so a `collect(n)` list contains node
/// maps and a plain map without an AGE annotation stays a map.
pub fn age_value_to_canonical_json(val: &AgeValue) -> serde_json::Value {
    match val {
        AgeValue::Null => serde_json::Value::Null,
        AgeValue::Boolean(b) => serde_json::Value::Bool(*b),
        AgeValue::Integer(i) => serde_json::Value::Number((*i).into()),
        AgeValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        AgeValue::String(s) => serde_json::Value::String(s.clone()),
        AgeValue::Vertex(v) => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "kind".to_string(),
                serde_json::Value::String("node".to_string()),
            );
            obj.insert("id".to_string(), serde_json::Value::Number(v.id.into()));
            let labels = if v.label.is_empty() {
                Vec::new()
            } else {
                vec![serde_json::Value::String(v.label.clone())]
            };
            obj.insert("labels".to_string(), serde_json::Value::Array(labels));
            obj.insert(
                "properties".to_string(),
                serde_json::Value::Object(
                    v.properties
                        .iter()
                        .map(|(k, value)| (k.clone(), value.clone()))
                        .collect(),
                ),
            );
            serde_json::Value::Object(obj)
        }
        AgeValue::Edge(e) => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "kind".to_string(),
                serde_json::Value::String("relationship".to_string()),
            );
            obj.insert("id".to_string(), serde_json::Value::Number(e.id.into()));
            obj.insert(
                "type".to_string(),
                serde_json::Value::String(e.label.clone()),
            );
            obj.insert(
                "start_id".to_string(),
                serde_json::Value::Number(e.start_id.into()),
            );
            obj.insert(
                "end_id".to_string(),
                serde_json::Value::Number(e.end_id.into()),
            );
            obj.insert(
                "properties".to_string(),
                serde_json::Value::Object(
                    e.properties
                        .iter()
                        .map(|(k, value)| (k.clone(), value.clone()))
                        .collect(),
                ),
            );
            serde_json::Value::Object(obj)
        }
        AgeValue::Path(p) => {
            let nodes: Vec<serde_json::Value> = p
                .vertices
                .iter()
                .map(|v| age_value_to_canonical_json(&AgeValue::Vertex(v.clone())))
                .collect();
            let relationships: Vec<serde_json::Value> = p
                .edges
                .iter()
                .map(|e| age_value_to_canonical_json(&AgeValue::Edge(e.clone())))
                .collect();
            let sequence: Vec<serde_json::Value> = (1..=p.edges.len() as i64)
                .map(|i| serde_json::Value::Number(i.into()))
                .collect();
            let mut obj = serde_json::Map::new();
            obj.insert(
                "kind".to_string(),
                serde_json::Value::String("path".to_string()),
            );
            obj.insert("nodes".to_string(), serde_json::Value::Array(nodes));
            obj.insert(
                "relationships".to_string(),
                serde_json::Value::Array(relationships),
            );
            obj.insert("sequence".to_string(), serde_json::Value::Array(sequence));
            serde_json::Value::Object(obj)
        }
        AgeValue::List(list) => {
            serde_json::Value::Array(list.iter().map(age_value_to_canonical_json).collect())
        }
        AgeValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k.clone(), age_value_to_canonical_json(v));
            }
            serde_json::Value::Object(obj)
        }
    }
}

fn age_map_to_json_map(map: HashMap<String, AgeValue>) -> HashMap<String, serde_json::Value> {
    let mut out = HashMap::with_capacity(map.len());
    for (k, v) in map {
        out.insert(k, age_value_to_canonical_json(&v));
    }
    out
}

/// Maximum recursion depth allowed when parsing nested agtype or PostgreSQL array values.
pub const MAX_NESTING_DEPTH: usize = 64;

struct AgtypeParser<'a> {
    input: &'a str,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> AgtypeParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.bytes.len()
            && matches!(self.bytes[self.pos], b' ' | b'\t' | b'\n' | b'\r')
        {
            self.pos += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn consume(&mut self, prefix: &str) -> bool {
        if self.bytes[self.pos..].starts_with(prefix.as_bytes()) {
            self.pos += prefix.len();
            true
        } else {
            false
        }
    }

    fn consume_keyword(&mut self, kw: &str) -> bool {
        if self.bytes[self.pos..].starts_with(kw.as_bytes()) {
            let next_pos = self.pos + kw.len();
            if next_pos < self.bytes.len()
                && (self.bytes[next_pos].is_ascii_alphanumeric() || self.bytes[next_pos] == b'_')
            {
                return false;
            }
            self.pos = next_pos;
            true
        } else {
            false
        }
    }

    fn parse_string(&mut self) -> Result<String> {
        if self.pos >= self.bytes.len() || self.bytes[self.pos] != b'"' {
            return Err(NetError::ProtocolError(format!(
                "Expected '\"' at position {}",
                self.pos
            )));
        }
        let start = self.pos;
        self.pos += 1;
        let mut escaped = false;
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            self.pos += 1;
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                break;
            }
        }
        serde_json::from_str::<String>(&self.input[start..self.pos]).map_err(|e| {
            NetError::ProtocolError(format!(
                "Failed to parse string starting at {}: {}",
                start, e
            ))
        })
    }

    fn parse_number(&mut self) -> Result<AgeValue> {
        let start = self.pos;
        while self.pos < self.bytes.len()
            && matches!(
                self.bytes[self.pos],
                b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E'
            )
        {
            self.pos += 1;
        }
        let num_str = &self.input[start..self.pos];
        self.skip_whitespace();
        let _ = self.consume("::numeric") || self.consume("::float");
        if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
            num_str.parse::<f64>().map(AgeValue::Float).map_err(|e| {
                NetError::ProtocolError(format!("Failed to parse float '{}': {}", num_str, e))
            })
        } else if let Ok(i) = num_str.parse::<i64>() {
            Ok(AgeValue::Integer(i))
        } else {
            num_str.parse::<f64>().map(AgeValue::Float).map_err(|e| {
                NetError::ProtocolError(format!("Failed to parse integer '{}': {}", num_str, e))
            })
        }
    }

    fn parse_object(&mut self, depth: usize) -> Result<AgeValue> {
        if depth >= MAX_NESTING_DEPTH {
            return Err(NetError::ProtocolError(format!(
                "agtype value exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
            )));
        }
        self.pos += 1; // skip '{'
        self.skip_whitespace();
        let mut map = HashMap::new();
        if self.peek() == Some(b'}') {
            self.pos += 1;
        } else {
            loop {
                self.skip_whitespace();
                if self.peek() != Some(b'"') {
                    return Err(NetError::ProtocolError(format!(
                        "Expected object string key at pos {}",
                        self.pos
                    )));
                }
                let key = self.parse_string()?;
                self.skip_whitespace();
                if !self.consume(":") {
                    return Err(NetError::ProtocolError(format!(
                        "Expected ':' after object key '{}' at pos {}",
                        key, self.pos
                    )));
                }
                self.skip_whitespace();
                let val = self.parse_value(depth + 1)?;
                map.insert(key, val);
                self.skip_whitespace();
                if self.consume(",") {
                    continue;
                }
                if self.consume("}") {
                    break;
                }
                return Err(NetError::ProtocolError(format!(
                    "Expected ',' or '}}' at pos {}",
                    self.pos
                )));
            }
        }

        self.skip_whitespace();
        if self.consume("::vertex") {
            let id = match map.get("id") {
                Some(AgeValue::Integer(i)) => *i,
                Some(AgeValue::Float(f)) => *f as i64,
                _ => {
                    return Err(NetError::ProtocolError(
                        "Missing or invalid 'id' in vertex".to_string(),
                    ));
                }
            };
            let label = match map.get("label") {
                Some(AgeValue::String(s)) => s.clone(),
                _ => {
                    return Err(NetError::ProtocolError(
                        "Missing or invalid 'label' in vertex".to_string(),
                    ));
                }
            };
            let properties = match map.remove("properties") {
                Some(AgeValue::Map(props)) => age_map_to_json_map(props),
                _ => HashMap::new(),
            };
            Ok(AgeValue::Vertex(AgeVertex {
                id,
                label,
                properties,
            }))
        } else if self.consume("::edge") {
            let id = match map.get("id") {
                Some(AgeValue::Integer(i)) => *i,
                Some(AgeValue::Float(f)) => *f as i64,
                _ => {
                    return Err(NetError::ProtocolError(
                        "Missing or invalid 'id' in edge".to_string(),
                    ));
                }
            };
            let label = match map.get("label") {
                Some(AgeValue::String(s)) => s.clone(),
                _ => {
                    return Err(NetError::ProtocolError(
                        "Missing or invalid 'label' in edge".to_string(),
                    ));
                }
            };
            let start_id = match map.get("start_id") {
                Some(AgeValue::Integer(i)) => *i,
                Some(AgeValue::Float(f)) => *f as i64,
                _ => {
                    return Err(NetError::ProtocolError(
                        "Missing or invalid 'start_id' in edge".to_string(),
                    ));
                }
            };
            let end_id = match map.get("end_id") {
                Some(AgeValue::Integer(i)) => *i,
                Some(AgeValue::Float(f)) => *f as i64,
                _ => {
                    return Err(NetError::ProtocolError(
                        "Missing or invalid 'end_id' in edge".to_string(),
                    ));
                }
            };
            let properties = match map.remove("properties") {
                Some(AgeValue::Map(props)) => age_map_to_json_map(props),
                _ => HashMap::new(),
            };
            Ok(AgeValue::Edge(AgeEdge {
                id,
                label,
                start_id,
                end_id,
                properties,
            }))
        } else {
            Ok(AgeValue::Map(map))
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<AgeValue> {
        if depth >= MAX_NESTING_DEPTH {
            return Err(NetError::ProtocolError(format!(
                "agtype value exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
            )));
        }
        self.pos += 1; // skip '['
        self.skip_whitespace();
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.pos += 1;
        } else {
            loop {
                let val = self.parse_value(depth + 1)?;
                items.push(val);
                self.skip_whitespace();
                if self.consume(",") {
                    continue;
                }
                if self.consume("]") {
                    break;
                }
                return Err(NetError::ProtocolError(format!(
                    "Expected ',' or ']' at pos {}",
                    self.pos
                )));
            }
        }

        self.skip_whitespace();
        if self.consume("::path") {
            if items.len() < 3 || items.len().is_multiple_of(2) {
                return Err(NetError::ProtocolError(
                    "Invalid path length in agtype".to_string(),
                ));
            }
            let mut vertices = Vec::with_capacity(items.len() / 2 + 1);
            let mut edges = Vec::with_capacity(items.len() / 2);

            for (idx, item) in items.into_iter().enumerate() {
                if idx % 2 == 0 {
                    if let AgeValue::Vertex(v) = item {
                        vertices.push(v);
                    } else {
                        return Err(NetError::ProtocolError(format!(
                            "Expected vertex in path at index {}",
                            idx
                        )));
                    }
                } else if let AgeValue::Edge(e) = item {
                    edges.push(e);
                } else {
                    return Err(NetError::ProtocolError(format!(
                        "Expected edge in path at index {}",
                        idx
                    )));
                }
            }

            Ok(AgeValue::Path(AgePath { vertices, edges }))
        } else {
            Ok(AgeValue::List(items))
        }
    }

    fn parse_value(&mut self, depth: usize) -> Result<AgeValue> {
        if depth > MAX_NESTING_DEPTH {
            return Err(NetError::ProtocolError(format!(
                "agtype value exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
            )));
        }
        self.skip_whitespace();
        if self.pos >= self.bytes.len() {
            return Ok(AgeValue::Null);
        }

        let b = self.bytes[self.pos];
        match b {
            b'"' => self.parse_string().map(AgeValue::String),
            b'{' => self.parse_object(depth),
            b'[' => self.parse_array(depth),
            b't' if self.consume_keyword("true") => Ok(AgeValue::Boolean(true)),
            b'f' if self.consume_keyword("false") => Ok(AgeValue::Boolean(false)),
            b'n' if self.consume_keyword("null") => Ok(AgeValue::Null),
            b'N' if self.consume_keyword("NaN") => {
                self.skip_whitespace();
                let _ = self.consume("::numeric") || self.consume("::float");
                Ok(AgeValue::Float(f64::NAN))
            }
            b'I' if self.consume_keyword("Infinity") => {
                self.skip_whitespace();
                let _ = self.consume("::numeric") || self.consume("::float");
                Ok(AgeValue::Float(f64::INFINITY))
            }
            b'-' if self.consume_keyword("-Infinity") => {
                self.skip_whitespace();
                let _ = self.consume("::numeric") || self.consume("::float");
                Ok(AgeValue::Float(f64::NEG_INFINITY))
            }
            b'+' if self.consume_keyword("+Infinity") => {
                self.skip_whitespace();
                let _ = self.consume("::numeric") || self.consume("::float");
                Ok(AgeValue::Float(f64::INFINITY))
            }
            b'-' | b'+' | b'0'..=b'9' => self.parse_number(),
            _ => {
                let s = self.input[self.pos..].trim().to_string();
                self.pos = self.bytes.len();
                Ok(AgeValue::String(s))
            }
        }
    }
}

/// Parses an Apache AGE `agtype` text string into an [`AgeValue`].
pub fn parse_agtype(raw: &str) -> Result<AgeValue> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return Ok(AgeValue::Null);
    }

    let mut parser = AgtypeParser::new(trimmed);
    let val = parser.parse_value(0)?;
    parser.skip_whitespace();
    Ok(val)
}

/// Maps a PostgreSQL array type OID to its element type OID.
fn pg_array_element_oid(oid: i32) -> Option<i32> {
    match oid {
        1000 => Some(16),                             // _bool -> bool
        1001 => Some(17),                             // _bytea -> bytea
        1005 => Some(21),                             // _int2 -> int2
        1007 => Some(23),                             // _int4 -> int4
        1016 => Some(20),                             // _int8 -> int8
        1021 => Some(700),                            // _float4 -> float4
        1022 => Some(701),                            // _float8 -> float8
        1231 => Some(1700),                           // _numeric -> numeric
        1003 | 1009 | 1014 | 1015 | 1041 => Some(25), // _name, _text, _bpchar, _varchar, _inet
        1182 => Some(1082),                           // _date -> date
        1183 => Some(1083),                           // _time -> time
        1270 => Some(1266),                           // _timetz -> timetz
        1115 => Some(1114),                           // _timestamp -> timestamp
        1185 => Some(1184),                           // _timestamptz -> timestamptz
        1187 => Some(1186),                           // _interval -> interval
        199 => Some(114),                             // _json -> json
        3807 => Some(3802),                           // _jsonb -> jsonb
        2951 => Some(2950),                           // _uuid -> uuid
        _ => None,
    }
}

/// One parsed PostgreSQL array node: either an element or a nested array.
#[derive(Debug)]
enum PgArrayValue {
    /// A scalar array element; `None` is SQL `NULL`.
    Element(Option<String>),
    /// A nested array (the top-level value of every array column is one of these).
    Array(Vec<PgArrayValue>),
}

impl PgArrayValue {
    fn children(&self) -> Option<&[PgArrayValue]> {
        match self {
            Self::Array(items) => Some(items),
            Self::Element(_) => None,
        }
    }
}

/// Parses one PostgreSQL text-format array literal into a nested value tree.
///
/// `None` elements are SQL `NULL`; nested braces describe additional array
/// dimensions. Non-default lower-bound decorations such as `[0:1][2:3]=` are
/// ignored, like psycopg does.
fn parse_pg_array_literal(raw: &str) -> Result<PgArrayValue> {
    let raw = raw.trim();
    let body = strip_array_dimensions(raw)?;
    let mut parser = PgArrayParser::new(raw, body);
    let value = parser.parse_value(0)?;
    parser.skip_whitespace();
    if parser.pos != body.len() {
        return Err(parser.malformed());
    }
    match value {
        PgArrayValue::Array(_) => Ok(value),
        PgArrayValue::Element(_) => Err(parser.malformed()),
    }
}

/// Recursive-descent parser for a PostgreSQL array literal body.
struct PgArrayParser<'a> {
    raw: &'a str,
    input: &'a str,
    pos: usize,
}

impl<'a> PgArrayParser<'a> {
    fn new(raw: &'a str, input: &'a str) -> Self {
        Self { raw, input, pos: 0 }
    }

    fn malformed(&self) -> NetError {
        NetError::ProtocolError(format!(
            "Malformed PostgreSQL array literal: {:?}",
            self.raw
        ))
    }

    fn unterminated(&self) -> NetError {
        NetError::ProtocolError(format!(
            "Unterminated quoted PostgreSQL array element: {:?}",
            self.raw
        ))
    }

    fn trailing_backslash(&self) -> NetError {
        NetError::ProtocolError(format!(
            "Trailing backslash in PostgreSQL array element: {:?}",
            self.raw
        ))
    }

    fn bytes(&self) -> &[u8] {
        self.input.as_bytes()
    }

    fn peek(&self) -> Option<u8> {
        self.bytes().get(self.pos).copied()
    }

    fn next_char(&mut self) -> Option<char> {
        let ch = self.input[self.pos..].chars().next()?;
        self.pos += ch.len_utf8();
        Some(ch)
    }

    fn skip_whitespace(&mut self) {
        while self.peek().is_some_and(|b| b.is_ascii_whitespace()) {
            self.pos += 1;
        }
    }

    fn parse_value(&mut self, depth: usize) -> Result<PgArrayValue> {
        if depth > MAX_NESTING_DEPTH {
            return Err(NetError::ProtocolError(format!(
                "PostgreSQL array literal exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
            )));
        }
        self.skip_whitespace();
        match self.peek() {
            Some(b'{') => self.parse_array(depth),
            Some(_) => self.parse_element(),
            None => Err(self.malformed()),
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<PgArrayValue> {
        if depth >= MAX_NESTING_DEPTH {
            return Err(NetError::ProtocolError(format!(
                "PostgreSQL array literal exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
            )));
        }
        self.pos += 1; // '{'
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(PgArrayValue::Array(items));
        }

        loop {
            items.push(self.parse_value(depth + 1)?);
            self.skip_whitespace();
            match self.peek() {
                Some(b',') => {
                    self.pos += 1;
                    self.skip_whitespace();
                    if self.peek() == Some(b'}') {
                        return Err(self.malformed());
                    }
                }
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(self.malformed()),
            }
        }

        Ok(PgArrayValue::Array(items))
    }

    fn parse_element(&mut self) -> Result<PgArrayValue> {
        if self.peek() == Some(b'"') {
            return Ok(PgArrayValue::Element(Some(self.parse_quoted_element()?)));
        }

        let mut element = String::new();
        while let Some(ch) = self.input[self.pos..].chars().next() {
            match ch {
                ',' | '}' => break,
                '{' => return Err(self.malformed()),
                '\\' => {
                    self.pos += 1;
                    let escaped = self.next_char().ok_or_else(|| self.trailing_backslash())?;
                    element.push(escaped);
                }
                other => {
                    element.push(other);
                    self.pos += other.len_utf8();
                }
            }
        }

        let trimmed = element.trim();
        Ok(PgArrayValue::Element(
            if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("NULL") {
                None
            } else {
                Some(trimmed.to_string())
            },
        ))
    }

    fn parse_quoted_element(&mut self) -> Result<String> {
        self.pos += 1; // opening quote
        let mut element = String::new();
        loop {
            let ch = self.next_char().ok_or_else(|| self.unterminated())?;
            match ch {
                '\\' => {
                    let escaped = self.next_char().ok_or_else(|| self.trailing_backslash())?;
                    element.push(escaped);
                }
                '"' => {
                    // PostgreSQL accepts a doubled quote inside a quoted element.
                    if self.peek() == Some(b'"') {
                        self.pos += 1;
                        element.push('"');
                        continue;
                    }
                    break;
                }
                other => element.push(other),
            }
        }
        self.skip_whitespace();
        Ok(element)
    }
}

/// Validates that a parsed array is rectangular and returns its nesting depth.
///
/// A scalar element has depth 0 and an array of scalars has depth 1, so a 2-D
/// matrix has depth 2. Empty arrays report depth 1 at their own level.
fn pg_array_depth(value: &PgArrayValue) -> Result<usize> {
    pg_array_depth_inner(value, 0)
}

fn pg_array_depth_inner(value: &PgArrayValue, depth: usize) -> Result<usize> {
    if depth > MAX_NESTING_DEPTH {
        return Err(NetError::ProtocolError(format!(
            "PostgreSQL array literal exceeded maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }
    let Some(items) = value.children() else {
        return Ok(0);
    };
    if items.is_empty() {
        return Ok(1);
    }

    let child_depth = pg_array_depth_inner(&items[0], depth + 1)?;
    for item in &items[1..] {
        if pg_array_depth_inner(item, depth + 1)? != child_depth {
            return Err(NetError::ProtocolError(
                "Ragged PostgreSQL array literal: dimensions are inconsistent".to_string(),
            ));
        }
    }
    if child_depth > 0 {
        let first_len = items[0].children().map_or(0, |children| children.len());
        for item in &items[1..] {
            if item.children().map_or(0, |children| children.len()) != first_len {
                return Err(NetError::ProtocolError(
                    "Ragged PostgreSQL array literal: dimensions are inconsistent".to_string(),
                ));
            }
        }
    }
    Ok(1 + child_depth)
}

/// Removes all dimension decorations such as `[0:2]=` or `[0:1][0:1]=` from
/// an array literal.
///
/// PostgreSQL only emits this prefix for non-default lower bounds, and psycopg
/// ignores those bounds when it builds Python lists, so the native decoder
/// ignores them too.
fn strip_array_dimensions(raw: &str) -> Result<&str> {
    if !raw.starts_with('[') {
        return Ok(raw);
    }
    let Some(eq) = raw.find('=') else {
        return Err(NetError::ProtocolError(format!(
            "Malformed PostgreSQL array literal: {raw:?}"
        )));
    };
    let prefix = &raw[..eq];
    let mut depth = 0usize;
    for ch in prefix.chars() {
        match ch {
            '[' => {
                if depth != 0 {
                    return Err(NetError::ProtocolError(format!(
                        "Malformed PostgreSQL array literal: {raw:?}"
                    )));
                }
                depth = 1;
            }
            ']' => {
                if depth != 1 {
                    return Err(NetError::ProtocolError(format!(
                        "Malformed PostgreSQL array literal: {raw:?}"
                    )));
                }
                depth = 0;
            }
            _ => {
                if depth != 1 {
                    return Err(NetError::ProtocolError(format!(
                        "Malformed PostgreSQL array literal: {raw:?}"
                    )));
                }
            }
        }
    }
    if depth != 0 || !prefix.ends_with(']') {
        return Err(NetError::ProtocolError(format!(
            "Malformed PostgreSQL array literal: {raw:?}"
        )));
    }
    Ok(&raw[eq + 1..])
}

fn parse_int_element(value: &str, column: &str) -> Result<i64> {
    value.parse::<i64>().map_err(|_| {
        NetError::ProtocolError(format!(
            "Invalid integer array element {value:?} in column {column:?}"
        ))
    })
}

fn parse_float_element(value: &str, column: &str) -> Result<f64> {
    value.parse::<f64>().map_err(|_| {
        NetError::ProtocolError(format!(
            "Invalid numeric array element {value:?} in column {column:?}"
        ))
    })
}

fn parse_bool_element(value: &str, column: &str) -> Result<bool> {
    if value.eq_ignore_ascii_case("t") || value.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if value.eq_ignore_ascii_case("f") || value.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(NetError::ProtocolError(format!(
            "Invalid boolean array element {value:?} in column {column:?}"
        )))
    }
}

fn parse_pg_uuid(raw: &str) -> Option<[u8; 16]> {
    let mut hex_bytes = [0u8; 32];
    let mut idx = 0usize;
    for b in raw.trim().bytes() {
        if b == b'-' {
            continue;
        }
        if idx >= 32 || !b.is_ascii_hexdigit() {
            return None;
        }
        hex_bytes[idx] = b;
        idx += 1;
    }
    if idx != 32 {
        return None;
    }
    let hex_val = |b: u8| -> u8 {
        match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            _ => b - b'A' + 10,
        }
    };
    let mut out = [0u8; 16];
    for i in 0..16 {
        out[i] = (hex_val(hex_bytes[i * 2]) << 4) | hex_val(hex_bytes[i * 2 + 1]);
    }
    Some(out)
}

fn decode_pg_bytea(raw: &str) -> Option<Vec<u8>> {
    if let Some(hex) = raw.strip_prefix("\\x").or_else(|| raw.strip_prefix("\\X")) {
        if hex.len() % 2 != 0 {
            return None;
        }
        let hex_val = |b: u8| -> Option<u8> {
            match b {
                b'0'..=b'9' => Some(b - b'0'),
                b'a'..=b'f' => Some(b - b'a' + 10),
                b'A'..=b'F' => Some(b - b'A' + 10),
                _ => None,
            }
        };
        let bytes = hex.as_bytes();
        let mut out = Vec::with_capacity(bytes.len() / 2);
        for chunk in bytes.chunks_exact(2) {
            out.push((hex_val(chunk[0])? << 4) | hex_val(chunk[1])?);
        }
        return Some(out);
    }

    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'\\' {
            if i + 1 < bytes.len() && bytes[i + 1] == b'\\' {
                out.push(b'\\');
                i += 2;
                continue;
            }
            if i + 3 < bytes.len()
                && (b'0'..=b'7').contains(&bytes[i + 1])
                && (b'0'..=b'7').contains(&bytes[i + 2])
                && (b'0'..=b'7').contains(&bytes[i + 3])
            {
                let v = ((bytes[i + 1] - b'0') << 6)
                    | ((bytes[i + 2] - b'0') << 3)
                    | (bytes[i + 3] - b'0');
                out.push(v);
                i += 4;
                continue;
            }
            return None;
        }
        out.push(bytes[i]);
        i += 1;
    }
    Some(out)
}

fn parse_pg_ymd(raw: &str) -> Option<(i32, u32, u32)> {
    let s = raw.trim();
    if s.eq_ignore_ascii_case("infinity")
        || s.eq_ignore_ascii_case("-infinity")
        || s.ends_with("BC")
    {
        return None;
    }
    let mut parts = s.split('-');
    let year: i32 = parts.next()?.parse().ok()?;
    let month: u32 = parts.next()?.parse().ok()?;
    let day: u32 = parts.next()?.parse().ok()?;
    if parts.next().is_some()
        || !(1..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
    {
        return None;
    }
    Some((year, month, day))
}

fn parse_pg_date_days(raw: &str) -> Option<i32> {
    let (y, m, d) = parse_pg_ymd(raw)?;
    i32::try_from(ymd_to_days(y, m, d)).ok()
}

fn parse_pg_hms_frac(raw: &str) -> Option<(i64, i64, i64, i64)> {
    let (hms, frac_str) = match raw.split_once('.') {
        Some((l, r)) => (l, Some(r)),
        None => (raw, None),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some()
        || !(0..=23).contains(&h)
        || !(0..=59).contains(&m)
        || !(0..=59).contains(&s)
    {
        return None;
    }
    let nanos = if let Some(frac) = frac_str {
        if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut padded = [b'0'; 9];
        padded[..frac.len()].copy_from_slice(frac.as_bytes());
        std::str::from_utf8(&padded).ok()?.parse::<i64>().ok()?
    } else {
        0
    };
    Some((h, m, s, nanos))
}

fn parse_pg_time_micros(raw: &str) -> Option<i64> {
    let (h, m, s, nanos) = parse_pg_hms_frac(raw.trim())?;
    if nanos % 1_000 != 0 {
        return None;
    }
    Some((h * 3_600 + m * 60 + s) * 1_000_000 + nanos / 1_000)
}

fn parse_pg_timestamp_micros(raw: &str) -> Option<i64> {
    let s = raw.trim();
    let (date_str, time_str) = s.split_once(' ').or_else(|| s.split_once('T'))?;
    let (year, month, day) = parse_pg_ymd(date_str)?;
    let (h, m, sec, nanos) = parse_pg_hms_frac(time_str.trim())?;
    if nanos % 1_000 != 0 {
        return None;
    }
    let days = ymd_to_days(year, month, day);
    let total_secs = days
        .checked_mul(86_400)?
        .checked_add(h * 3_600 + m * 60 + sec)?;
    total_secs
        .checked_mul(1_000_000)?
        .checked_add(nanos / 1_000)
}

fn parse_pg_tz_offset(raw: &str) -> Option<(i32, usize)> {
    let bytes = raw.as_bytes();
    if bytes.ends_with(b"Z") || bytes.ends_with(b"z") {
        return Some((0, raw.len() - 1));
    }
    let mut split_idx = None;
    for (idx, b) in bytes.iter().enumerate().rev() {
        if *b == b'+' || *b == b'-' {
            split_idx = Some(idx);
            break;
        }
        if *b == b' ' || *b == b'T' {
            return None;
        }
    }
    let idx = split_idx?;
    let sign = if bytes[idx] == b'-' { -1 } else { 1 };
    let off_str = &raw[idx + 1..];
    let mut parts = off_str.split(':');
    let oh: i32 = parts.next()?.parse().ok()?;
    let om: i32 = match parts.next() {
        Some(m) => m.parse().ok()?,
        None => 0,
    };
    let os: i32 = match parts.next() {
        Some(s) => s.parse().ok()?,
        None => 0,
    };
    if parts.next().is_some() {
        return None;
    }
    if !(0..=23).contains(&oh) || !(0..=59).contains(&om) || !(0..=59).contains(&os) {
        return None;
    }
    Some((sign * (oh * 3_600 + om * 60 + os), idx))
}

fn parse_pg_timestamptz_micros(raw: &str) -> Option<(i64, i32)> {
    let s = raw.trim();
    let (offset_secs, split_at) = parse_pg_tz_offset(s)?;
    let dt_part = &s[..split_at];
    let local_micros = parse_pg_timestamp_micros(dt_part)?;
    let utc_micros = local_micros.checked_sub((offset_secs as i64).checked_mul(1_000_000)?)?;
    Some((utc_micros, offset_secs))
}

fn parse_pg_timetz_micros(raw: &str) -> Option<(i64, i32)> {
    let s = raw.trim();
    let (offset_secs, split_at) = parse_pg_tz_offset(s)?;
    let local_micros = parse_pg_time_micros(&s[..split_at])?;
    Some((local_micros, offset_secs))
}

fn parse_hms_duration_token(tok: &str) -> Option<(i64, i64)> {
    let (neg, unsigned) = if let Some(rest) = tok.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = tok.strip_prefix('+') {
        (false, rest)
    } else {
        (false, tok)
    };
    let (hms, frac_str) = match unsigned.split_once('.') {
        Some((l, r)) => (l, Some(r)),
        None => (unsigned, None),
    };
    let mut parts = hms.split(':');
    let h: i64 = parts.next()?.parse().ok()?;
    let m: i64 = parts.next()?.parse().ok()?;
    let s: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    let nanos = if let Some(frac) = frac_str {
        if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut padded = [b'0'; 9];
        padded[..frac.len()].copy_from_slice(frac.as_bytes());
        std::str::from_utf8(&padded).ok()?.parse::<i64>().ok()?
    } else {
        0
    };
    let total_secs = h.checked_mul(3_600)?.checked_add(m * 60 + s)?;
    if neg {
        Some((-total_secs, -nanos))
    } else {
        Some((total_secs, nanos))
    }
}

fn parse_pg_interval_sec_decimal(raw: &str) -> Option<(i64, i64)> {
    let (neg, unsigned) = if let Some(rest) = raw.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = raw.strip_prefix('+') {
        (false, rest)
    } else {
        (false, raw)
    };
    let (sec_str, frac_str) = match unsigned.split_once('.') {
        Some((l, r)) => (l, Some(r)),
        None => (unsigned, None),
    };
    let secs: i64 = if sec_str.is_empty() {
        0
    } else {
        sec_str.parse().ok()?
    };
    let nanos = if let Some(frac) = frac_str {
        if frac.is_empty() || frac.len() > 9 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut padded = [b'0'; 9];
        padded[..frac.len()].copy_from_slice(frac.as_bytes());
        std::str::from_utf8(&padded).ok()?.parse::<i64>().ok()?
    } else {
        0
    };
    if neg {
        Some((-secs, -nanos))
    } else {
        Some((secs, nanos))
    }
}

fn parse_pg_iso_duration(raw: &str) -> Option<BoltDuration> {
    let (overall_neg, rest) = if let Some(r) = raw.strip_prefix("-P") {
        (true, r)
    } else {
        let r = raw.strip_prefix('P')?;
        (false, r)
    };
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut seconds: i64 = 0;
    let mut nanoseconds: i64 = 0;

    let mut num_start = 0usize;
    let d_bytes = date_part.as_bytes();
    for (idx, &b) in d_bytes.iter().enumerate() {
        if b.is_ascii_alphabetic() {
            let v: i64 = date_part[num_start..idx].parse().ok()?;
            match b {
                b'Y' => months = months.checked_add(v.checked_mul(12)?)?,
                b'M' => months = months.checked_add(v)?,
                b'W' => days = days.checked_add(v.checked_mul(7)?)?,
                b'D' => days = days.checked_add(v)?,
                _ => return None,
            }
            num_start = idx + 1;
        }
    }
    if num_start != d_bytes.len() {
        return None;
    }

    if let Some(tp) = time_part {
        let mut t_start = 0usize;
        let t_bytes = tp.as_bytes();
        for (idx, &b) in t_bytes.iter().enumerate() {
            if b.is_ascii_alphabetic() {
                let slice = &tp[t_start..idx];
                match b {
                    b'H' => {
                        let v: i64 = slice.parse().ok()?;
                        seconds = seconds.checked_add(v.checked_mul(3_600)?)?;
                    }
                    b'M' => {
                        let v: i64 = slice.parse().ok()?;
                        seconds = seconds.checked_add(v.checked_mul(60)?)?;
                    }
                    b'S' => {
                        let (s, ns) = parse_pg_interval_sec_decimal(slice)?;
                        seconds = seconds.checked_add(s)?;
                        nanoseconds = nanoseconds.checked_add(ns)?;
                    }
                    _ => return None,
                }
                t_start = idx + 1;
            }
        }
        if t_start != t_bytes.len() {
            return None;
        }
    }

    let extra_secs = nanoseconds.div_euclid(1_000_000_000);
    seconds = seconds.checked_add(extra_secs)?;
    nanoseconds = nanoseconds.rem_euclid(1_000_000_000);
    if overall_neg {
        months = -months;
        days = -days;
        seconds = -seconds;
        if nanoseconds != 0 {
            seconds = seconds.checked_sub(1)?;
            nanoseconds = 1_000_000_000 - nanoseconds;
        }
    }
    Some(BoltDuration {
        months,
        days,
        seconds,
        nanoseconds,
    })
}

fn parse_pg_interval(raw: &str) -> Option<BoltDuration> {
    let mut s = raw.trim();
    if s.is_empty() {
        return None;
    }
    if s.starts_with('P') || s.starts_with("-P") {
        return parse_pg_iso_duration(s);
    }
    let mut ago = false;
    if let Some(rest) = s.strip_prefix('@') {
        s = rest.trim_start();
    }
    if let Some(rest) = s.strip_suffix("ago") {
        ago = true;
        s = rest.trim_end();
    }

    let tokens: Vec<&str> = s.split_whitespace().collect();
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut seconds: i64 = 0;
    let mut nanoseconds: i64 = 0;

    let mut i = 0usize;
    while i < tokens.len() {
        let tok = tokens[i];
        if tok.contains(':') {
            let (ts, tns) = parse_hms_duration_token(tok)?;
            seconds = seconds.checked_add(ts)?;
            nanoseconds = nanoseconds.checked_add(tns)?;
            i += 1;
            continue;
        }
        if let Some((y_str, m_str)) = tok.strip_prefix(['+', '-']).unwrap_or(tok).split_once('-')
            && !y_str.is_empty()
            && !m_str.is_empty()
            && y_str.bytes().all(|b| b.is_ascii_digit())
            && m_str.bytes().all(|b| b.is_ascii_digit())
        {
            let sign: i64 = if tok.starts_with('-') { -1 } else { 1 };
            let y: i64 = y_str.parse().ok()?;
            let m: i64 = m_str.parse().ok()?;
            months = months.checked_add(sign.checked_mul(y.checked_mul(12)?.checked_add(m)?)?)?;
            i += 1;
            continue;
        }
        let next_is_unit = i + 1 < tokens.len()
            && matches!(
                tokens[i + 1].to_ascii_lowercase().as_str(),
                "year"
                    | "years"
                    | "yr"
                    | "yrs"
                    | "y"
                    | "mon"
                    | "mons"
                    | "month"
                    | "months"
                    | "day"
                    | "days"
                    | "d"
                    | "hour"
                    | "hours"
                    | "hr"
                    | "hrs"
                    | "h"
                    | "min"
                    | "mins"
                    | "minute"
                    | "minutes"
                    | "m"
                    | "sec"
                    | "secs"
                    | "second"
                    | "seconds"
                    | "s"
            );
        if !next_is_unit {
            if let Ok(d_val) = tok.parse::<i64>() {
                days = days.checked_add(d_val)?;
                i += 1;
                continue;
            }
            return None;
        }
        let unit = tokens[i + 1].to_ascii_lowercase();
        match unit.as_str() {
            "year" | "years" | "yr" | "yrs" | "y" => {
                let v: i64 = tok.parse().ok()?;
                months = months.checked_add(v.checked_mul(12)?)?;
            }
            "mon" | "mons" | "month" | "months" => {
                let v: i64 = tok.parse().ok()?;
                months = months.checked_add(v)?;
            }
            "day" | "days" | "d" => {
                let v: i64 = tok.parse().ok()?;
                days = days.checked_add(v)?;
            }
            "hour" | "hours" | "hr" | "hrs" | "h" => {
                let v: i64 = tok.parse().ok()?;
                seconds = seconds.checked_add(v.checked_mul(3_600)?)?;
            }
            "min" | "mins" | "minute" | "minutes" | "m" => {
                let v: i64 = tok.parse().ok()?;
                seconds = seconds.checked_add(v.checked_mul(60)?)?;
            }
            "sec" | "secs" | "second" | "seconds" | "s" => {
                let (sv, ns) = parse_pg_interval_sec_decimal(tok)?;
                seconds = seconds.checked_add(sv)?;
                nanoseconds = nanoseconds.checked_add(ns)?;
            }
            _ => return None,
        }
        i += 2;
    }

    let total_ns = (seconds as i128)
        .checked_mul(1_000_000_000)?
        .checked_add(nanoseconds as i128)?;
    let total_ns = if ago { -total_ns } else { total_ns };
    if ago {
        months = -months;
        days = -days;
    }
    let norm_secs = i64::try_from(total_ns / 1_000_000_000).ok()?;
    let norm_ns = (total_ns % 1_000_000_000) as i64;
    Some(BoltDuration {
        months,
        days,
        seconds: norm_secs,
        nanoseconds: norm_ns,
    })
}

fn json_to_result_value(val: &serde_json::Value) -> ResultValue {
    match val {
        serde_json::Value::Null => ResultValue::Null,
        serde_json::Value::Bool(b) => ResultValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                ResultValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                ResultValue::Float(f)
            } else {
                ResultValue::Str(n.to_string().into_boxed_str())
            }
        }
        serde_json::Value::String(s) => ResultValue::Str(s.clone().into_boxed_str()),
        serde_json::Value::Array(arr) => ResultValue::List(
            arr.iter()
                .map(json_to_result_value)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        ),
        serde_json::Value::Object(obj) => ResultValue::Map(
            obj.iter()
                .map(|(k, v)| (std::sync::Arc::from(k.as_str()), json_to_result_value(v)))
                .collect(),
        ),
    }
}

fn hash_map_to_result_map<V>(
    map: HashMap<String, V>,
    mut convert: impl FnMut(V) -> ResultValue,
) -> crate::value::result::ResultMap {
    let mut entries: Vec<(std::sync::Arc<str>, ResultValue)> = map
        .into_iter()
        .map(|(k, v)| (std::sync::Arc::from(k), convert(v)))
        .collect();
    // Sort keys so downstream CanonicalJson preserves deterministic JSON output
    // regardless of per-HashMap RandomState iteration order.
    if entries.len() > 1 {
        entries.sort_unstable_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
    }
    crate::value::result::ResultMap::from_entries(entries)
}

fn age_vertex_to_result_node(v: AgeVertex) -> ResultNode {
    let labels = if v.label.is_empty() {
        Vec::new()
    } else {
        vec![std::sync::Arc::from(v.label)]
    };
    let properties = hash_map_to_result_map(v.properties, |val| json_to_result_value(&val));
    ResultNode {
        id: v.id,
        labels,
        properties,
        element_id: None,
    }
}

fn age_edge_to_result_rel(e: AgeEdge) -> ResultRelationship {
    let properties = hash_map_to_result_map(e.properties, |val| json_to_result_value(&val));
    ResultRelationship {
        id: e.id,
        start_node_id: e.start_id,
        end_node_id: e.end_id,
        rel_type: std::sync::Arc::from(e.label),
        properties,
        element_id: None,
        start_element_id: None,
        end_element_id: None,
    }
}

impl From<AgeValue> for ResultValue {
    fn from(val: AgeValue) -> Self {
        match val {
            AgeValue::Null => Self::Null,
            AgeValue::Boolean(b) => Self::Bool(b),
            AgeValue::Integer(i) => Self::Int(i),
            AgeValue::Float(f) => Self::Float(f),
            AgeValue::String(s) => Self::Str(s.into_boxed_str()),
            AgeValue::Vertex(v) => Self::Node(Box::new(age_vertex_to_result_node(v))),
            AgeValue::Edge(e) => Self::Relationship(Box::new(age_edge_to_result_rel(e))),
            AgeValue::Path(p) => Self::Path(Box::new(ResultPath {
                nodes: p
                    .vertices
                    .into_iter()
                    .map(age_vertex_to_result_node)
                    .collect(),
                relationships: p.edges.into_iter().map(age_edge_to_result_rel).collect(),
            })),
            AgeValue::List(items) => Self::List(
                items
                    .into_iter()
                    .map(Self::from)
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            ),
            AgeValue::Map(map) => Self::Map(hash_map_to_result_map(map, Self::from)),
        }
    }
}

fn pg_numeric_typmod_is_fixed(typmod: Option<i32>) -> bool {
    typmod.is_some_and(|t| t >= 4 && ((t - 4) & 0xffff) as i16 >= 0)
}

fn pg_oid_column_hint_with_mode(
    oid: i32,
    typmod: Option<i32>,
    streaming: bool,
) -> Option<ColumnHint> {
    match oid {
        16 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::Bool)),
        20 | 21 | 23 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::Int)),
        700 | 701 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::Float)),
        1700 => Some(if streaming && !pg_numeric_typmod_is_fixed(typmod) {
            ColumnHint::PgDecimalText
        } else {
            ColumnHint::PgDecimal { typmod }
        }),
        2950 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::Uuid)),
        17 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::Bytes)),
        1082 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::Date)),
        1083 => Some(ColumnHint::PgLocalTimeUs),
        1266 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::TimeTz)),
        1114 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::TimestampUs)),
        1184 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::TimestampTzUtc)),
        1186 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::DurationUs)),
        114 | 3802 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::Json)),
        19 | 25 | 869 | 1042 | 1043 => Some(ColumnHint::PgEmptyTyped(EmptyColumnKind::Str)),
        _ => None,
    }
}

fn pg_oid_column_hint(oid: i32, typmod: Option<i32>) -> Option<ColumnHint> {
    pg_oid_column_hint_with_mode(oid, typmod, false)
}

fn decode_pg_scalar_cell(raw: &str, oid: i32, field_name: &str) -> Result<Option<ResultValue>> {
    match oid {
        16 => Ok(Some(ResultValue::Bool(parse_bool_element(
            raw, field_name,
        )?))),
        20 | 21 | 23 => Ok(Some(ResultValue::Int(parse_int_element(raw, field_name)?))),
        700 | 701 => Ok(Some(ResultValue::Float(parse_float_element(
            raw, field_name,
        )?))),
        1700 => Ok(Some(ResultValue::Decimal(raw.into()))),
        2950 => match parse_pg_uuid(raw) {
            Some(bytes) => Ok(Some(ResultValue::Uuid(bytes))),
            None => Ok(Some(ResultValue::TypedText(Box::new(ResultTypedText {
                meta_type: META_TYPE_UUID,
                text: raw.into(),
            })))),
        },
        17 => {
            let bytes = decode_pg_bytea(raw).ok_or_else(|| {
                NetError::ProtocolError(format!("Invalid bytea value in column {field_name:?}"))
            })?;
            Ok(Some(ResultValue::Bytes(bytes.into_boxed_slice())))
        }
        1082 => match parse_pg_date_days(raw) {
            Some(days) => Ok(Some(ResultValue::Date(days as i64))),
            None => Ok(Some(ResultValue::TypedText(Box::new(ResultTypedText {
                meta_type: META_TYPE_DATE,
                text: raw.into(),
            })))),
        },
        1083 => match parse_pg_time_micros(raw) {
            Some(us) => Ok(Some(ResultValue::LocalTime(us * 1_000))),
            None => Ok(Some(ResultValue::TypedText(Box::new(ResultTypedText {
                meta_type: META_TYPE_TIME,
                text: raw.into(),
            })))),
        },
        1266 => Ok(Some(ResultValue::TypedText(Box::new(ResultTypedText {
            meta_type: META_TYPE_TIME_TZ,
            text: raw.into(),
        })))),
        1114 => match parse_pg_timestamp_micros(raw) {
            Some(us) => Ok(Some(ResultValue::LocalDateTime {
                seconds: us.div_euclid(1_000_000),
                nanoseconds: (us.rem_euclid(1_000_000) * 1_000) as u32,
            })),
            None => Ok(Some(ResultValue::TypedText(Box::new(ResultTypedText {
                meta_type: META_TYPE_LOCAL_DATETIME,
                text: raw.into(),
            })))),
        },
        1184 => match parse_pg_timestamptz_micros(raw) {
            Some((us, 0)) => Ok(Some(ResultValue::DateTime(Box::new(ResultDateTime {
                seconds: us.div_euclid(1_000_000),
                nanoseconds: us.rem_euclid(1_000_000) * 1_000,
                tz: ResultTimezone::Offset(0),
            })))),
            _ => Ok(Some(ResultValue::TypedText(Box::new(ResultTypedText {
                meta_type: META_TYPE_DATETIME_TZ,
                text: raw.into(),
            })))),
        },
        1186 => match parse_pg_interval(raw) {
            Some(dur) => Ok(Some(ResultValue::Duration(Box::new(DurationParts {
                months: dur.months,
                days: dur.days,
                seconds: dur.seconds,
                nanoseconds: dur.nanoseconds,
            })))),
            None => Ok(Some(ResultValue::TypedText(Box::new(ResultTypedText {
                meta_type: META_TYPE_DURATION,
                text: raw.into(),
            })))),
        },
        114 | 3802 => Ok(Some(ResultValue::Json(raw.into()))),
        19 | 25 | 869 | 1042 | 1043 => Ok(Some(ResultValue::Str(raw.into()))),
        _ => Ok(None),
    }
}

fn pg_array_value_to_result(
    value: &PgArrayValue,
    elem_oid: i32,
    field_name: &str,
) -> Result<ResultValue> {
    match value {
        PgArrayValue::Element(None) => Ok(ResultValue::Null),
        PgArrayValue::Element(Some(raw)) => Ok(decode_pg_scalar_cell(raw, elem_oid, field_name)?
            .unwrap_or_else(|| ResultValue::Str(raw.as_str().into()))),
        PgArrayValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(pg_array_value_to_result(item, elem_oid, field_name)?);
            }
            Ok(ResultValue::List(out.into_boxed_slice()))
        }
    }
}

fn json_number_from_f64(value: f64) -> serde_json::Value {
    serde_json::Number::from_f64(value)
        .map(serde_json::Value::Number)
        .unwrap_or(serde_json::Value::Null)
}

/// Converts one scalar array element to a JSON value for the text fallback.
fn pg_array_element_to_json(raw: &str, elem_oid: i32) -> serde_json::Value {
    match elem_oid {
        16 => serde_json::Value::Bool(
            raw.trim().eq_ignore_ascii_case("t") || raw.trim().eq_ignore_ascii_case("true"),
        ),
        20 | 21 | 23 => raw
            .parse::<i64>()
            .map(|value| serde_json::Value::Number(value.into()))
            .unwrap_or_else(|_| serde_json::Value::String(raw.to_string())),
        700 | 701 => raw
            .parse::<f64>()
            .map(json_number_from_f64)
            .unwrap_or_else(|_| serde_json::Value::String(raw.to_string())),
        1700 => raw
            .parse::<i64>()
            .map(|value| serde_json::Value::Number(value.into()))
            .or_else(|_| raw.parse::<f64>().map(json_number_from_f64))
            .unwrap_or_else(|_| serde_json::Value::String(raw.to_string())),
        114 | 3802 => {
            serde_json::from_str(raw).unwrap_or_else(|_| serde_json::Value::String(raw.to_string()))
        }
        _ => serde_json::Value::String(raw.to_string()),
    }
}

fn pg_array_to_json(value: &PgArrayValue, elem_oid: i32) -> serde_json::Value {
    match value {
        PgArrayValue::Element(None) => serde_json::Value::Null,
        PgArrayValue::Element(Some(raw)) => pg_array_element_to_json(raw, elem_oid),
        PgArrayValue::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .map(|item| pg_array_to_json(item, elem_oid))
                .collect(),
        ),
    }
}

fn micros_to_seconds_nanos(micros: i64) -> (i64, i64) {
    (
        micros.div_euclid(1_000_000),
        micros.rem_euclid(1_000_000) * 1_000,
    )
}

/// Decodes one scalar array element into a [`BoltValue`] for `execute()`.
fn pg_array_element_to_bolt(raw: &str, elem_oid: i32) -> Result<BoltValue> {
    let value = match elem_oid {
        16 => BoltValue::Boolean(parse_bool_element(raw, "array")?),
        20 | 21 | 23 => BoltValue::Integer(parse_int_element(raw, "array")?),
        700 | 701 => BoltValue::Float(parse_float_element(raw, "array")?),
        // Numeric and UUID elements keep their exact text; the JSON-fallback
        // column is tagged `voyager.type=decimal` / `voyager.type=uuid`, so the
        // Python conversion parses these leaves into Decimal / uuid.UUID.
        1700 | 2950 => BoltValue::String(raw.to_string()),
        17 => BoltValue::Bytes(decode_pg_bytea(raw).ok_or_else(|| {
            NetError::ProtocolError(format!("Invalid bytea array element {raw:?}"))
        })?),
        1082 => match parse_pg_date_days(raw) {
            Some(days) => BoltValue::Date(BoltDate::new(days as i64)),
            None => BoltValue::String(raw.to_string()),
        },
        1083 => match parse_pg_time_micros(raw) {
            Some(micros) => BoltValue::LocalTime(BoltLocalTime::new(micros * 1_000)),
            None => BoltValue::String(raw.to_string()),
        },
        1266 => match parse_pg_timetz_micros(raw) {
            Some((micros, offset_secs)) => {
                BoltValue::Time(BoltTime::new(micros * 1_000, offset_secs as i64))
            }
            None => BoltValue::String(raw.to_string()),
        },
        1114 => match parse_pg_timestamp_micros(raw) {
            Some(micros) => {
                let (seconds, nanoseconds) = micros_to_seconds_nanos(micros);
                BoltValue::LocalDateTime(BoltLocalDateTime::new(seconds, nanoseconds))
            }
            None => BoltValue::String(raw.to_string()),
        },
        1184 => match parse_pg_timestamptz_micros(raw) {
            Some((micros, offset_secs)) => {
                let (seconds, nanoseconds) = micros_to_seconds_nanos(micros);
                BoltValue::DateTime(BoltDateTime::new(seconds, nanoseconds, offset_secs as i64))
            }
            None => BoltValue::String(raw.to_string()),
        },
        1186 => match parse_pg_interval(raw) {
            Some(duration) => BoltValue::Duration(duration),
            None => BoltValue::String(raw.to_string()),
        },
        114 | 3802 => serde_json::from_str::<serde_json::Value>(raw)
            .map(|value| BoltValue::from(&value))
            .unwrap_or_else(|_| BoltValue::String(raw.to_string())),
        _ => BoltValue::String(raw.to_string()),
    };
    Ok(value)
}

fn pg_array_value_to_bolt(value: &PgArrayValue, elem_oid: i32) -> Result<BoltValue> {
    match value {
        PgArrayValue::Element(None) => Ok(BoltValue::Null),
        PgArrayValue::Element(Some(raw)) => pg_array_element_to_bolt(raw, elem_oid),
        PgArrayValue::Array(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(pg_array_value_to_bolt(item, elem_oid)?);
            }
            Ok(BoltValue::List(values))
        }
    }
}

fn pg_array_to_bolt_value(raw: &str, elem_oid: i32) -> Result<BoltValue> {
    let parsed = parse_pg_array_literal(raw)?;
    pg_array_value_to_bolt(&parsed, elem_oid)
}

#[inline]
fn bytes_to_str(bytes: &[u8]) -> std::borrow::Cow<'_, str> {
    match std::str::from_utf8(bytes) {
        Ok(s) => std::borrow::Cow::Borrowed(s),
        Err(_) => String::from_utf8_lossy(bytes),
    }
}

#[inline]
fn clean_agtype_cow(s: &str) -> std::borrow::Cow<'_, str> {
    if !s.as_bytes().windows(2).any(|w| w == b"::") {
        std::borrow::Cow::Borrowed(s.trim())
    } else {
        std::borrow::Cow::Owned(clean_agtype_string(s))
    }
}

#[derive(Debug)]
enum PgColumnDecoder {
    Age,
    Array {
        elem_oid: i32,
        typmod: Option<i32>,
        depth: Option<usize>,
        mixed_depth: bool,
    },
    TypedScalar {
        oid: i32,
        hint: ColumnHint,
    },
    Untyped {
        raw_cells: Vec<Option<bytes::Bytes>>,
        has_non_null: bool,
        can_be_int: bool,
        can_be_float: bool,
        can_be_bool: bool,
    },
}

#[derive(Debug)]
enum PgDecodedStorage {
    Rows(Vec<Vec<ResultValue>>),
    Columns(Vec<Vec<ResultValue>>),
}

/// Streaming PostgreSQL `DataRow` decoder targeting [`ResultValue`] directly
/// from `&[u8]` slices without per-cell `String` allocations.
#[derive(Debug)]
pub(crate) struct PgRowDecoder {
    mode: ResultMode,
    streaming: bool,
    columns: Vec<String>,
    cached_columns: Arc<[String]>,
    cached_hints: Arc<[ColumnHint]>,
    has_dynamic_hints: bool,
    col_decoders: Vec<PgColumnDecoder>,
    storage: PgDecodedStorage,
}

impl PgRowDecoder {
    pub(crate) fn new(mode: ResultMode) -> Self {
        let storage = match mode {
            ResultMode::Rows => PgDecodedStorage::Rows(Vec::new()),
            ResultMode::Arrow => PgDecodedStorage::Columns(Vec::new()),
        };
        Self {
            mode,
            streaming: false,
            columns: Vec::new(),
            cached_columns: Arc::from(Vec::new()),
            cached_hints: Arc::from(Vec::new()),
            has_dynamic_hints: false,
            col_decoders: Vec::new(),
            storage,
        }
    }

    pub(crate) fn new_stream() -> Self {
        Self {
            mode: ResultMode::Rows,
            streaming: true,
            columns: Vec::new(),
            cached_columns: Arc::from(Vec::new()),
            cached_hints: Arc::from(Vec::new()),
            has_dynamic_hints: false,
            col_decoders: Vec::new(),
            storage: PgDecodedStorage::Rows(Vec::new()),
        }
    }

    pub(crate) fn columns_arc(&self) -> Arc<[String]> {
        self.cached_columns.clone()
    }

    pub(crate) fn column_hints_arc(&self) -> Arc<[ColumnHint]> {
        self.cached_hints.clone()
    }

    pub(crate) fn set_schema(
        &mut self,
        columns: Vec<String>,
        field_types: &[i32],
        field_type_modifiers: &[i32],
        age_columns: &[bool],
    ) {
        let num_cols = columns.len();
        self.col_decoders.clear();
        self.col_decoders.reserve(num_cols);
        let mut base_hints = Vec::with_capacity(num_cols);
        let mut has_dynamic = false;

        for col_idx in 0..num_cols {
            if age_columns.get(col_idx).copied().unwrap_or(false) {
                self.col_decoders.push(PgColumnDecoder::Age);
                base_hints.push(ColumnHint::Age);
                continue;
            }
            if let Some(&oid) = field_types.get(col_idx) {
                let typmod = field_type_modifiers.get(col_idx).copied();
                if let Some(elem_oid) = pg_array_element_oid(oid) {
                    has_dynamic = true;
                    let elem_hint = pg_oid_column_hint_with_mode(elem_oid, typmod, self.streaming)
                        .unwrap_or(ColumnHint::PgEmptyTyped(EmptyColumnKind::Str));
                    base_hints.push(ColumnHint::PgArray {
                        depth: 1,
                        elem_hint: Box::new(elem_hint),
                    });
                    self.col_decoders.push(PgColumnDecoder::Array {
                        elem_oid,
                        typmod,
                        depth: None,
                        mixed_depth: false,
                    });
                    continue;
                }
                if let Some(hint) = pg_oid_column_hint_with_mode(oid, typmod, self.streaming) {
                    base_hints.push(hint.clone());
                    self.col_decoders
                        .push(PgColumnDecoder::TypedScalar { oid, hint });
                    continue;
                }
            }
            has_dynamic = true;
            base_hints.push(ColumnHint::Auto);
            self.col_decoders.push(PgColumnDecoder::Untyped {
                raw_cells: Vec::new(),
                has_non_null: false,
                can_be_int: true,
                can_be_float: true,
                can_be_bool: true,
            });
        }

        self.cached_columns = Arc::from(columns.as_slice());
        self.cached_hints = Arc::from(base_hints);
        self.has_dynamic_hints = has_dynamic;
        self.columns = columns;
        if self.mode == ResultMode::Arrow {
            self.storage = PgDecodedStorage::Columns(vec![Vec::new(); num_cols]);
        }
    }

    pub(crate) fn push_row(&mut self, vals: Vec<Option<bytes::Bytes>>) -> Result<()> {
        let num_cols = self.columns.len();
        if num_cols == 0 {
            return Ok(());
        }

        let mut vals_iter = vals.into_iter();
        let mut row_buf = match self.mode {
            ResultMode::Rows => Some(Vec::with_capacity(num_cols)),
            ResultMode::Arrow => None,
        };

        for col_idx in 0..num_cols {
            let cell_opt = vals_iter.next().flatten();
            let decoded = match &mut self.col_decoders[col_idx] {
                PgColumnDecoder::Age => match cell_opt {
                    Some(bytes) => {
                        let s = bytes_to_str(&bytes);
                        let parsed = parse_agtype(&s)?;
                        ResultValue::from(parsed)
                    }
                    None => ResultValue::Null,
                },
                PgColumnDecoder::Array {
                    elem_oid,
                    depth,
                    mixed_depth,
                    ..
                } => match cell_opt {
                    Some(bytes) => {
                        let s = bytes_to_str(&bytes);
                        let parsed = parse_pg_array_literal(&s)?;
                        let empty =
                            matches!(&parsed, PgArrayValue::Array(items) if items.is_empty());
                        if !empty {
                            let cell_depth = pg_array_depth(&parsed)?;
                            match *depth {
                                Some(existing) if existing != cell_depth => {
                                    *mixed_depth = true;
                                }
                                None => *depth = Some(cell_depth),
                                _ => {}
                            }
                        }
                        pg_array_value_to_result(&parsed, *elem_oid, &self.columns[col_idx])?
                    }
                    None => ResultValue::Null,
                },
                PgColumnDecoder::TypedScalar { oid, .. } => match cell_opt {
                    Some(bytes) => {
                        let s = bytes_to_str(&bytes);
                        decode_pg_scalar_cell(&s, *oid, &self.columns[col_idx])?
                            .unwrap_or(ResultValue::Null)
                    }
                    None => ResultValue::Null,
                },
                PgColumnDecoder::Untyped {
                    raw_cells,
                    has_non_null,
                    can_be_int,
                    can_be_float,
                    can_be_bool,
                } => {
                    if let Some(ref bytes) = cell_opt {
                        *has_non_null = true;
                        let s = bytes_to_str(bytes);
                        let trimmed = clean_agtype_cow(&s);
                        if *can_be_bool
                            && trimmed != "t"
                            && trimmed != "f"
                            && trimmed != "true"
                            && trimmed != "false"
                        {
                            *can_be_bool = false;
                        }
                        if *can_be_int && trimmed.parse::<i64>().is_err() {
                            *can_be_int = false;
                        }
                        if *can_be_float && trimmed.parse::<f64>().is_err() {
                            *can_be_float = false;
                        }
                    }
                    raw_cells.push(cell_opt);
                    ResultValue::Null
                }
            };

            match (&mut row_buf, &mut self.storage) {
                (Some(row), _) => row.push(decoded),
                (None, PgDecodedStorage::Columns(cols))
                    if !matches!(self.col_decoders[col_idx], PgColumnDecoder::Untyped { .. }) =>
                {
                    cols[col_idx].push(decoded);
                }
                _ => {}
            }
        }

        if let (Some(row), PgDecodedStorage::Rows(rows)) = (row_buf, &mut self.storage) {
            rows.push(row);
        }
        Ok(())
    }

    pub(crate) fn take_chunk(&mut self) -> Option<crate::stream::ResultChunk> {
        let PgDecodedStorage::Rows(ref mut rows_buf) = self.storage else {
            return None;
        };
        if rows_buf.is_empty() {
            return None;
        }
        let mut rows = std::mem::take(rows_buf);
        if !self.has_dynamic_hints {
            return Some(crate::stream::ResultChunk::new(
                self.cached_columns.clone(),
                rows,
                self.cached_hints.clone(),
            ));
        }

        let num_cols = self.columns.len();
        let mut column_hints = Vec::with_capacity(num_cols);
        for (col_idx, decoder) in self.col_decoders.iter_mut().enumerate() {
            let hint =
                resolve_decoder_hint_and_untyped(col_idx, decoder, self.streaming, &mut rows, None);
            column_hints.push(hint);
        }
        Some(crate::stream::ResultChunk::new(
            self.cached_columns.clone(),
            rows,
            Arc::from(column_hints),
        ))
    }

    pub(crate) fn discard_partial_chunk(&mut self) {
        match &mut self.storage {
            PgDecodedStorage::Rows(rows) => rows.clear(),
            PgDecodedStorage::Columns(cols) => {
                for col in cols {
                    col.clear();
                }
            }
        }
        for decoder in &mut self.col_decoders {
            match decoder {
                PgColumnDecoder::Array {
                    depth, mixed_depth, ..
                } => {
                    *depth = None;
                    *mixed_depth = false;
                }
                PgColumnDecoder::Untyped {
                    raw_cells,
                    has_non_null,
                    can_be_int,
                    can_be_float,
                    can_be_bool,
                } => {
                    raw_cells.clear();
                    *has_non_null = false;
                    *can_be_int = true;
                    *can_be_float = true;
                    *can_be_bool = true;
                }
                PgColumnDecoder::Age | PgColumnDecoder::TypedScalar { .. } => {}
            }
        }
    }

    pub(crate) fn finish(mut self, summary: QuerySummary) -> Result<QueryResult> {
        if self.columns.is_empty() {
            return Ok(match self.mode {
                ResultMode::Rows => {
                    QueryResult::from_rows(Vec::new(), Vec::new(), Vec::new(), summary)
                }
                ResultMode::Arrow => QueryResult::new(Vec::new(), Vec::new(), summary),
            });
        }

        let num_cols = self.columns.len();
        let mut column_hints = Vec::with_capacity(num_cols);

        for (col_idx, decoder) in self.col_decoders.iter_mut().enumerate() {
            let hint = match &mut self.storage {
                PgDecodedStorage::Rows(rows) => {
                    resolve_decoder_hint_and_untyped(col_idx, decoder, self.streaming, rows, None)
                }
                PgDecodedStorage::Columns(cols) => {
                    let mut empty_rows = Vec::new();
                    resolve_decoder_hint_and_untyped(
                        col_idx,
                        decoder,
                        self.streaming,
                        &mut empty_rows,
                        Some(&mut cols[col_idx]),
                    )
                }
            };
            column_hints.push(hint);
        }

        match self.storage {
            PgDecodedStorage::Rows(rows) => Ok(QueryResult::from_rows(
                self.columns,
                rows,
                column_hints,
                summary,
            )),
            PgDecodedStorage::Columns(cols) => {
                let mut fields = Vec::with_capacity(num_cols);
                let mut arrays = Vec::with_capacity(num_cols);
                let mut tz_cache: HashMap<String, bool> = HashMap::new();
                for ((name, col_values), hint) in self
                    .columns
                    .iter()
                    .zip(cols.iter())
                    .zip(column_hints.iter())
                {
                    let (field, array) = crate::value::arrow::build_result_column(
                        name,
                        col_values,
                        hint,
                        &mut tz_cache,
                    )?;
                    fields.push(field);
                    arrays.push(array);
                }
                let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
                let batch = RecordBatch::try_new(schema, arrays).map_err(NetError::ArrowError)?;
                let batches = if batch.num_rows() > 0 || !self.columns.is_empty() {
                    vec![batch]
                } else {
                    Vec::new()
                };
                Ok(QueryResult::new(self.columns, batches, summary))
            }
        }
    }
}

fn resolve_decoder_hint_and_untyped(
    col_idx: usize,
    decoder: &mut PgColumnDecoder,
    streaming: bool,
    rows: &mut [Vec<ResultValue>],
    arrow_col: Option<&mut Vec<ResultValue>>,
) -> ColumnHint {
    match decoder {
        PgColumnDecoder::Age => ColumnHint::Age,
        PgColumnDecoder::Array {
            elem_oid,
            typmod,
            depth,
            mixed_depth,
        } => {
            let chunk_depth = depth.take().unwrap_or(1);
            let is_mixed = std::mem::take(mixed_depth);
            if is_mixed {
                let meta_type = match *elem_oid {
                    1700 => Some(META_TYPE_DECIMAL),
                    2950 => Some(META_TYPE_UUID),
                    _ => None,
                };
                ColumnHint::PgMixedArray { meta_type }
            } else {
                let elem_hint = pg_oid_column_hint_with_mode(*elem_oid, *typmod, streaming)
                    .unwrap_or(ColumnHint::PgEmptyTyped(EmptyColumnKind::Str));
                ColumnHint::PgArray {
                    depth: chunk_depth,
                    elem_hint: Box::new(elem_hint),
                }
            }
        }
        PgColumnDecoder::TypedScalar { hint, .. } => hint.clone(),
        PgColumnDecoder::Untyped {
            raw_cells,
            has_non_null,
            can_be_int,
            can_be_float,
            can_be_bool,
        } => {
            let cells = std::mem::take(raw_cells);
            let non_null = std::mem::take(has_non_null);
            let is_int = std::mem::replace(can_be_int, true);
            let is_float = std::mem::replace(can_be_float, true);
            let is_bool = std::mem::replace(can_be_bool, true);

            if let Some(col_vec) = arrow_col {
                let mut resolved = Vec::with_capacity(cells.len());
                for bytes_opt in cells {
                    resolved.push(resolve_untyped_cell(
                        bytes_opt.as_deref(),
                        non_null,
                        is_int,
                        is_float,
                        is_bool,
                    ));
                }
                *col_vec = resolved;
            } else {
                for (row_idx, bytes_opt) in cells.into_iter().enumerate() {
                    rows[row_idx][col_idx] = resolve_untyped_cell(
                        bytes_opt.as_deref(),
                        non_null,
                        is_int,
                        is_float,
                        is_bool,
                    );
                }
            }
            ColumnHint::Auto
        }
    }
}

fn resolve_untyped_cell(
    bytes_opt: Option<&[u8]>,
    has_non_null: bool,
    can_be_int: bool,
    can_be_float: bool,
    can_be_bool: bool,
) -> ResultValue {
    let Some(bytes) = bytes_opt else {
        return ResultValue::Null;
    };
    if !has_non_null {
        return ResultValue::Null;
    }
    let val = bytes_to_str(bytes);
    if can_be_int {
        clean_agtype_cow(&val)
            .parse::<i64>()
            .map(ResultValue::Int)
            .unwrap_or(ResultValue::Null)
    } else if can_be_float {
        clean_agtype_cow(&val)
            .parse::<f64>()
            .map(ResultValue::Float)
            .unwrap_or(ResultValue::Null)
    } else if can_be_bool {
        let cleaned = clean_agtype_cow(&val);
        ResultValue::Bool(cleaned == "t" || cleaned == "true")
    } else {
        ResultValue::Str(val.into_owned().into_boxed_str())
    }
}

/// Dynamically transforms tabular row values into an Apache Arrow [`RecordBatch`].
pub fn rows_to_record_batch(
    column_names: &[String],
    rows: &[Vec<Option<String>>],
) -> Result<RecordBatch> {
    rows_to_record_batch_with_age_columns(column_names, rows, &[])
}

/// Transforms tabular PostgreSQL / Apache AGE row values into an Apache Arrow [`RecordBatch`].
///
/// Columns marked `true` in `age_columns` are decoded as `agtype`: pure scalar
/// columns map to native Arrow types (`Int64`, `Float64`, `Boolean`, plain `Utf8`),
/// while columns containing vertices, edges, paths, lists, or maps are serialized
/// as kind-tagged JSON text and annotated with `voyager.encoding = "json"` (plus
/// `voyager.kind = "node" | "relationship" | "path"` when homogeneous).
pub fn rows_to_record_batch_with_age_columns(
    column_names: &[String],
    rows: &[Vec<Option<String>>],
    age_columns: &[bool],
) -> Result<RecordBatch> {
    rows_to_record_batch_with_column_types(column_names, rows, age_columns, &[])
}

/// Like [`rows_to_record_batch_with_age_columns`], but also decodes PostgreSQL
/// array columns using the type OIDs reported in `RowDescription`.
///
/// `column_types` holds one PostgreSQL type OID per column. Array columns whose
/// element type is supported become Arrow `List` columns; other columns keep
/// the dynamic inference used by [`rows_to_record_batch_with_age_columns`].
pub fn rows_to_record_batch_with_column_types(
    column_names: &[String],
    rows: &[Vec<Option<String>>],
    age_columns: &[bool],
    column_types: &[i32],
) -> Result<RecordBatch> {
    rows_to_record_batch_with_column_metadata(column_names, rows, age_columns, column_types, &[])
}

/// Like [`rows_to_record_batch_with_column_types`], also accepting PostgreSQL
/// `type_modifier` (`typmod`) values from `RowDescription` (for example to
/// preserve declared `NUMERIC(p, s)` scale).
pub fn rows_to_record_batch_with_column_metadata(
    column_names: &[String],
    rows: &[Vec<Option<String>>],
    age_columns: &[bool],
    column_types: &[i32],
    column_type_modifiers: &[i32],
) -> Result<RecordBatch> {
    rows_to_record_batch_with_column_metadata_and_bolt_rows(
        column_names,
        rows,
        age_columns,
        column_types,
        column_type_modifiers,
        false,
    )
    .map(|(batch, _)| batch)
}

/// Like [`rows_to_record_batch_with_column_metadata`], optionally returning raw
/// Bolt rows for columns that fell back to JSON.
///
/// Decodes every column into [`ResultValue`] and delegates Arrow [`RecordBatch`]
/// construction to the single unified [`crate::value::arrow::values_to_record_batch_with_hints`]
/// builder.
pub fn rows_to_record_batch_with_column_metadata_and_bolt_rows(
    column_names: &[String],
    rows: &[Vec<Option<String>>],
    age_columns: &[bool],
    column_types: &[i32],
    column_type_modifiers: &[i32],
    collect_bolt_rows: bool,
) -> Result<(RecordBatch, Option<Vec<Vec<BoltValue>>>)> {
    if column_names.is_empty() {
        return Ok((values_to_record_batch_with_hints(&[], &[], &[])?, None));
    }

    let num_rows = rows.len();
    let num_cols = column_names.len();

    let mut fields: Vec<arrow_schema::Field> = Vec::with_capacity(num_cols);
    let mut arrays: Vec<arrow_array::ArrayRef> = Vec::with_capacity(num_cols);
    let mut tz_cache: HashMap<String, bool> = HashMap::new();
    let mut col_values: Vec<ResultValue> = Vec::with_capacity(num_rows);
    let mut fallback_columns: Vec<(usize, i32)> = Vec::new();

    for (col_idx, name) in column_names.iter().enumerate() {
        col_values.clear();

        if age_columns.get(col_idx).copied().unwrap_or(false) {
            for row in rows {
                if let Some(raw) = row.get(col_idx).and_then(|v| v.as_deref()) {
                    let parsed = parse_agtype(raw)?;
                    col_values.push(ResultValue::from(parsed));
                } else {
                    col_values.push(ResultValue::Null);
                }
            }
            let (field, array) = crate::value::arrow::build_result_column(
                name,
                &col_values,
                &ColumnHint::Age,
                &mut tz_cache,
            )?;
            fields.push(field);
            arrays.push(array);
            continue;
        }

        if let Some(&oid) = column_types.get(col_idx) {
            let typmod = column_type_modifiers.get(col_idx).copied();
            if let Some(elem_oid) = pg_array_element_oid(oid) {
                let mut parsed_cells: Vec<Option<PgArrayValue>> = Vec::with_capacity(num_rows);
                let mut depth: Option<usize> = None;
                let mut mixed_depth = false;

                for row in rows {
                    match row.get(col_idx).and_then(|value| value.as_deref()) {
                        Some(raw) => {
                            let value = parse_pg_array_literal(raw)?;
                            let empty =
                                matches!(&value, PgArrayValue::Array(items) if items.is_empty());
                            if !empty {
                                let cell_depth = pg_array_depth(&value)?;
                                match depth {
                                    Some(existing) if existing != cell_depth => {
                                        mixed_depth = true;
                                    }
                                    None => depth = Some(cell_depth),
                                    _ => {}
                                }
                            }
                            parsed_cells.push(Some(value));
                        }
                        None => parsed_cells.push(None),
                    }
                }

                let hint = if mixed_depth {
                    for cell in parsed_cells {
                        if let Some(parsed) = cell {
                            let text = serde_json::to_string(&pg_array_to_json(&parsed, elem_oid))
                                .map_err(|error| {
                                    NetError::ProtocolError(format!(
                                        "Failed to encode PostgreSQL array column {name:?} as JSON: {error}"
                                    ))
                                })?;
                            col_values.push(ResultValue::Json(text.into_boxed_str()));
                        } else {
                            col_values.push(ResultValue::Null);
                        }
                    }
                    let meta_type = match elem_oid {
                        1700 => Some(META_TYPE_DECIMAL),
                        2950 => Some(META_TYPE_UUID),
                        _ => None,
                    };
                    if collect_bolt_rows {
                        fallback_columns.push((col_idx, elem_oid));
                    }
                    ColumnHint::JsonColumn { meta_type }
                } else {
                    let depth = depth.unwrap_or(1);
                    for cell in parsed_cells {
                        if let Some(parsed) = cell {
                            col_values.push(pg_array_value_to_result(&parsed, elem_oid, name)?);
                        } else {
                            col_values.push(ResultValue::Null);
                        }
                    }
                    let elem_hint = pg_oid_column_hint(elem_oid, typmod)
                        .unwrap_or(ColumnHint::PgEmptyTyped(EmptyColumnKind::Str));
                    ColumnHint::PgArray {
                        depth,
                        elem_hint: Box::new(elem_hint),
                    }
                };
                let (field, array) = crate::value::arrow::build_result_column(
                    name,
                    &col_values,
                    &hint,
                    &mut tz_cache,
                )?;
                fields.push(field);
                arrays.push(array);
                continue;
            }

            if let Some(hint) = pg_oid_column_hint(oid, typmod) {
                for row in rows {
                    if let Some(raw) = row.get(col_idx).and_then(|v| v.as_deref())
                        && let Some(val) = decode_pg_scalar_cell(raw, oid, name)?
                    {
                        col_values.push(val);
                    } else {
                        col_values.push(ResultValue::Null);
                    }
                }
                let (field, array) = crate::value::arrow::build_result_column(
                    name,
                    &col_values,
                    &hint,
                    &mut tz_cache,
                )?;
                fields.push(field);
                arrays.push(array);
                continue;
            }
        }

        // Untyped fallback: infer Int64 / Float64 / Boolean / Utf8 dynamically.
        let mut has_non_null = false;
        let mut can_be_int = true;
        let mut can_be_float = true;
        let mut can_be_bool = true;

        for row in rows {
            if let Some(Some(val)) = row.get(col_idx) {
                has_non_null = true;
                let trimmed = clean_agtype_string(val);

                if can_be_bool
                    && trimmed != "t"
                    && trimmed != "f"
                    && trimmed != "true"
                    && trimmed != "false"
                {
                    can_be_bool = false;
                }
                if can_be_int && trimmed.parse::<i64>().is_err() {
                    can_be_int = false;
                }
                if can_be_float && trimmed.parse::<f64>().is_err() {
                    can_be_float = false;
                }
            }
        }

        for row in rows {
            if let Some(val) = row.get(col_idx).and_then(|v| v.as_deref()) {
                let rv = if !has_non_null {
                    ResultValue::Null
                } else if can_be_int {
                    clean_agtype_string(val)
                        .parse::<i64>()
                        .map(ResultValue::Int)
                        .unwrap_or(ResultValue::Null)
                } else if can_be_float {
                    clean_agtype_string(val)
                        .parse::<f64>()
                        .map(ResultValue::Float)
                        .unwrap_or(ResultValue::Null)
                } else if can_be_bool {
                    let cleaned = clean_agtype_string(val);
                    ResultValue::Bool(cleaned == "t" || cleaned == "true")
                } else {
                    ResultValue::Str(val.into())
                };
                col_values.push(rv);
            } else {
                col_values.push(ResultValue::Null);
            }
        }
        let (field, array) = crate::value::arrow::build_result_column(
            name,
            &col_values,
            &ColumnHint::Auto,
            &mut tz_cache,
        )?;
        fields.push(field);
        arrays.push(array);
    }

    let bolt_rows = if collect_bolt_rows && !fallback_columns.is_empty() {
        let mut decoded_rows = Vec::with_capacity(num_rows);
        for row in rows {
            let mut values = vec![BoltValue::Null; num_cols];
            for (col_idx, elem_oid) in &fallback_columns {
                if let Some(raw) = row.get(*col_idx).and_then(|value| value.as_deref()) {
                    values[*col_idx] = pg_array_to_bolt_value(raw, *elem_oid)?;
                }
            }
            decoded_rows.push(values);
        }
        Some(decoded_rows)
    } else {
        None
    };

    let schema = std::sync::Arc::new(arrow_schema::Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays).map_err(NetError::ArrowError)?;
    Ok((batch, bolt_rows))
}

#[cfg(test)]
mod pg_array_tests {
    use super::*;
    use crate::value::{
        ARROW_EXTENSION_NAME, ARROW_UUID_EXTENSION, VOYAGER_META_ENCODING, VOYAGER_META_TYPE,
    };
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    fn batch_for(oid: i32, rows: &[Option<&str>]) -> RecordBatch {
        let columns = vec!["value".to_string()];
        let rows: Vec<Vec<Option<String>>> = rows
            .iter()
            .map(|value| vec![value.map(str::to_string)])
            .collect();
        rows_to_record_batch_with_column_types(&columns, &rows, &[false], &[oid])
            .expect("array column should decode")
    }

    #[test]
    fn decodes_integer_arrays_with_nulls_and_empty_arrays() {
        let batch = batch_for(1016, &[Some("{1,2,NULL}"), Some("{}"), None]);
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::List(Arc::new(Field::new("item", DataType::Int64, true)))
        );
        let list = batch.column(0).as_list::<i32>();
        let values = list.value(0);
        let values = values.as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(values.len(), 3);
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        assert!(values.is_null(2));
        assert_eq!(list.value(1).len(), 0);
        assert!(list.is_null(2));
    }

    #[test]
    fn decodes_each_supported_element_type() {
        let ints = batch_for(1007, &[Some("{42,-7}")]);
        assert_eq!(
            ints.column(0)
                .as_list::<i32>()
                .value(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            42
        );

        let floats = batch_for(1022, &[Some("{1.5,-2.25}")]);
        assert_eq!(
            floats
                .column(0)
                .as_list::<i32>()
                .value(0)
                .as_primitive::<arrow_array::types::Float64Type>()
                .value(1),
            -2.25
        );

        let bools = batch_for(1000, &[Some("{t,f,true,false}")]);
        let bool_list = bools.column(0).as_list::<i32>();
        let bool_values = bool_list.value(0);
        let bool_values = bool_values.as_boolean();
        assert!(bool_values.value(0));
        assert!(!bool_values.value(1));
        assert!(bool_values.value(2));
        assert!(!bool_values.value(3));

        let nums = batch_for(1231, &[Some("{1.25,2}")]);
        assert_eq!(
            nums.column(0)
                .as_list::<i32>()
                .value(0)
                .as_primitive::<arrow_array::types::Decimal128Type>()
                .value(1),
            200
        );

        let texts = batch_for(
            1009,
            &[Some(
                r#"{alpha,"beta,gamma","NULL","quote\"d","back\\slash"}"#,
            )],
        );
        let text_list = texts.column(0).as_list::<i32>();
        let text_values = text_list.value(0);
        let values = text_values.as_string::<i32>();
        assert_eq!(values.value(0), "alpha");
        assert_eq!(values.value(1), "beta,gamma");
        assert_eq!(values.value(2), "NULL");
        assert_eq!(values.value(3), "quote\"d");
        assert_eq!(values.value(4), "back\\slash");
    }

    #[test]
    fn decodes_date_and_timestamp_arrays_to_native_arrow_types() {
        let dates = batch_for(1182, &[Some("{2026-01-15,2026-02-20}")]);
        let date_list = dates.column(0).as_list::<i32>();
        let date_values = date_list.value(0);
        assert_eq!(
            date_values
                .as_primitive::<arrow_array::types::Date32Type>()
                .value(1),
            ymd_to_days(2026, 2, 20) as i32
        );

        let timestamps = batch_for(
            1115,
            &[Some(r#"{"2026-01-15 10:11:12","2026-01-16 00:00:00"}"#)],
        );
        let timestamp_list = timestamps.column(0).as_list::<i32>();
        let timestamp_values = timestamp_list.value(0);
        assert_eq!(
            timestamp_values
                .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                .value(0),
            parse_pg_timestamp_micros("2026-01-15 10:11:12").unwrap()
        );
    }

    #[test]
    fn decodes_single_dimension_with_explicit_lower_bound() {
        let batch = batch_for(1007, &[Some("[0:2]={10,20,30}")]);
        let list = batch.column(0).as_list::<i32>();
        assert_eq!(list.value(0).len(), 3);
    }

    #[test]
    fn decodes_multi_dimensional_arrays_with_nulls_and_lower_bounds() {
        let batch = batch_for(
            1016,
            &[
                Some("{{1,2},{3,NULL}}"),
                Some("[0:1][0:1]={{4,5},{6,7}}"),
                Some("{{}}"),
                Some("{}"),
                None,
            ],
        );
        let outer = batch.column(0).as_list::<i32>();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            )))
        );

        let first_values = outer.value(0);
        let first = first_values.as_list::<i32>();
        assert_eq!(first.len(), 2);
        let first_row_values = first.value(0);
        let first_row = first_row_values.as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!((first_row.value(0), first_row.value(1)), (1, 2));
        let second_row_values = first.value(1);
        let second_row = second_row_values.as_primitive::<arrow_array::types::Int64Type>();
        assert_eq!(second_row.value(0), 3);
        assert!(second_row.is_null(1));

        let lowered_values = outer.value(1);
        let lowered = lowered_values.as_list::<i32>();
        assert_eq!(lowered.len(), 2);
        let lowered_row_values = lowered.value(1);
        assert_eq!(
            lowered_row_values
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(1),
            7
        );

        let empty_values = outer.value(2);
        let empty = empty_values.as_list::<i32>();
        assert_eq!(empty.len(), 1);
        assert_eq!(empty.value(0).len(), 0);
        let flat_empty_values = outer.value(3);
        assert_eq!(flat_empty_values.as_list::<i32>().len(), 0);
        assert!(outer.is_null(4));
    }

    #[test]
    fn decodes_three_dimensional_text_arrays_with_nulls() {
        let batch = batch_for(
            1009,
            &[Some(
                r#"{{{alpha,NULL,"beta,gamma"}},{{"NULL",x,"quote""d"}}}"#,
            )],
        );
        let outer = batch.column(0).as_list::<i32>();
        let planes_values = outer.value(0);
        let middle = planes_values.as_list::<i32>();
        assert_eq!(middle.len(), 2);
        let first_values = middle.value(0);
        let first = first_values.as_list::<i32>();
        let values = first.value(0);
        let values = values.as_string::<i32>();
        assert_eq!(values.value(0), "alpha");
        assert!(values.is_null(1));
        assert_eq!(values.value(2), "beta,gamma");
        let second_values = middle.value(1);
        let second = second_values.as_list::<i32>();
        let values = second.value(0);
        let values = values.as_string::<i32>();
        assert_eq!(values.value(0), "NULL");
        assert_eq!(values.value(1), "x");
        assert_eq!(values.value(2), "quote\"d");
    }

    #[test]
    fn decodes_nested_arrays_for_each_supported_element_type() {
        let floats = batch_for(1022, &[Some("{{1.5,NULL},{-2.25,3.5}}")]);
        let outer = floats.column(0).as_list::<i32>();
        let inner_values = outer.value(0);
        let inner = inner_values.as_list::<i32>();
        let row_values = inner.value(0);
        let values = row_values.as_primitive::<arrow_array::types::Float64Type>();
        assert_eq!(values.value(0), 1.5);
        assert!(values.is_null(1));

        let nums = batch_for(1231, &[Some("{{1.25,2},{3,4.5}}")]);
        let outer = nums.column(0).as_list::<i32>();
        let inner_values = outer.value(0);
        let inner = inner_values.as_list::<i32>();
        let row_values = inner.value(0);
        assert_eq!(
            row_values
                .as_primitive::<arrow_array::types::Decimal128Type>()
                .value(1),
            200
        );

        let bools = batch_for(1000, &[Some("{{t,f},{NULL,true}}")]);
        let outer = bools.column(0).as_list::<i32>();
        let inner_values = outer.value(0);
        let inner = inner_values.as_list::<i32>();
        let row_values = inner.value(0);
        let values = row_values.as_boolean();
        assert!(values.value(0));
        assert!(!values.value(1));

        let dates = batch_for(1182, &[Some("{{2026-01-15,NULL},{2026-02-20,2026-03-01}}")]);
        let outer = dates.column(0).as_list::<i32>();
        let inner_values = outer.value(0);
        let inner = inner_values.as_list::<i32>();
        let row_values = inner.value(1);
        assert_eq!(
            row_values
                .as_primitive::<arrow_array::types::Date32Type>()
                .value(0),
            ymd_to_days(2026, 2, 20) as i32
        );

        let timestamps = batch_for(
            1115,
            &[Some(
                r#"{{"2026-01-15 10:11:12",NULL},{"2026-01-16 00:00:00","2026-01-17 01:02:03"}}"#,
            )],
        );
        let outer = timestamps.column(0).as_list::<i32>();
        let inner_values = outer.value(0);
        let inner = inner_values.as_list::<i32>();
        let row_values = inner.value(0);
        assert_eq!(
            row_values
                .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                .value(0),
            parse_pg_timestamp_micros("2026-01-15 10:11:12").unwrap()
        );

        let uuids = batch_for(
            2951,
            &[Some(
                "{{12345678-1234-5678-1234-567812345678,NULL},{NULL,ffffffff-ffff-ffff-ffff-ffffffffffff}}",
            )],
        );
        let outer = uuids.column(0).as_list::<i32>();
        let inner_values = outer.value(0);
        let inner = inner_values.as_list::<i32>();
        let row_values = inner.value(0);
        assert_eq!(
            row_values.as_fixed_size_binary().value(0),
            &parse_pg_uuid("12345678-1234-5678-1234-567812345678").unwrap()
        );

        let json = batch_for(3807, &[Some(r#"{{"{\"a\":1}",NULL},{"[1,2]","null"}}"#)]);
        let outer = json.column(0).as_list::<i32>();
        let inner_values = outer.value(0);
        let inner = inner_values.as_list::<i32>();
        let row_values = inner.value(0);
        let values = row_values.as_string::<i32>();
        assert_eq!(values.value(0), r#"{"a":1}"#);
    }

    #[test]
    fn rejects_ragged_and_mixed_dimensional_arrays() {
        let columns = vec!["value".to_string()];
        let rows = vec![vec![Some("{{1,2},{3}}".to_string())]];
        let error = rows_to_record_batch_with_column_types(&columns, &rows, &[false], &[1007])
            .expect_err("ragged arrays are rejected");
        assert!(error.to_string().contains("Ragged"));

        let rows = vec![
            vec![Some("{1,2}".to_string())],
            vec![Some("{{3,4},{5,6}}".to_string())],
        ];
        let batch = rows_to_record_batch_with_column_types(&columns, &rows, &[false], &[1007])
            .expect("mixed dimensionalities fall back to JSON");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            batch
                .schema()
                .field(0)
                .metadata()
                .get(VOYAGER_META_ENCODING)
                .map(String::as_str),
            Some("json")
        );
        let values = batch.column(0).as_string::<i32>();
        assert_eq!(values.value(0), "[1,2]");
        assert_eq!(values.value(1), "[[3,4],[5,6]]");
    }

    #[test]
    fn mixed_depth_arrays_keep_per_row_shapes_for_execute() {
        let columns = vec!["value".to_string()];
        let rows = vec![
            vec![Some("{1,2}".to_string())],
            vec![Some("{{3,NULL},{5,6}}".to_string())],
            vec![Some("{}".to_string())],
            vec![None],
        ];
        let (batch, bolt_rows) = rows_to_record_batch_with_column_metadata_and_bolt_rows(
            &columns,
            &rows,
            &[false],
            &[1007],
            &[0],
            true,
        )
        .expect("mixed depth arrays fall back");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);

        let bolt_rows = bolt_rows.expect("fallback columns collect raw rows");
        assert_eq!(bolt_rows.len(), 4);
        assert_eq!(
            bolt_rows[0][0],
            BoltValue::List(vec![BoltValue::Integer(1), BoltValue::Integer(2)])
        );
        assert_eq!(
            bolt_rows[1][0],
            BoltValue::List(vec![
                BoltValue::List(vec![BoltValue::Integer(3), BoltValue::Null]),
                BoltValue::List(vec![BoltValue::Integer(5), BoltValue::Integer(6)]),
            ])
        );
        assert_eq!(bolt_rows[2][0], BoltValue::List(Vec::new()));
        assert_eq!(bolt_rows[3][0], BoltValue::Null);
    }

    #[test]
    fn mixed_depth_numeric_and_uuid_arrays_keep_element_type_tags() {
        let columns = vec!["nums".to_string(), "ids".to_string()];
        let rows = vec![
            vec![
                Some("{1.5,2}".to_string()),
                Some("{12345678-1234-5678-1234-567812345678}".to_string()),
            ],
            vec![
                Some("{{3.25},{4}}".to_string()),
                Some("{{87654321-4321-8765-4321-876543218765}}".to_string()),
            ],
        ];
        let (batch, bolt_rows) = rows_to_record_batch_with_column_metadata_and_bolt_rows(
            &columns,
            &rows,
            &[false, false],
            &[1231, 2951],
            &[0, 0],
            true,
        )
        .expect("mixed depth arrays fall back");

        let schema = batch.schema();
        for (idx, expected_type) in [(0, "decimal"), (1, "uuid")] {
            let field = schema.field(idx);
            assert_eq!(field.data_type(), &DataType::Utf8);
            assert_eq!(
                field
                    .metadata()
                    .get(VOYAGER_META_ENCODING)
                    .map(String::as_str),
                Some("json")
            );
            assert_eq!(
                field.metadata().get(VOYAGER_META_TYPE).map(String::as_str),
                Some(expected_type)
            );
        }

        let bolt_rows = bolt_rows.expect("fallback columns collect raw rows");
        assert_eq!(
            bolt_rows[0][0],
            BoltValue::List(vec![
                BoltValue::String("1.5".to_string()),
                BoltValue::String("2".to_string()),
            ])
        );
        assert_eq!(
            bolt_rows[1][0],
            BoltValue::List(vec![
                BoltValue::List(vec![BoltValue::String("3.25".to_string())]),
                BoltValue::List(vec![BoltValue::String("4".to_string())]),
            ])
        );
        assert_eq!(
            bolt_rows[0][1],
            BoltValue::List(vec![BoltValue::String(
                "12345678-1234-5678-1234-567812345678".to_string()
            )])
        );
    }

    #[test]
    fn unknown_array_oids_keep_text_fallback() {
        let columns = vec!["value".to_string()];
        let rows = vec![vec![Some("{x,y}".to_string())]];
        let batch = rows_to_record_batch_with_column_types(&columns, &rows, &[false], &[999999])
            .expect("unknown array OID should fall back");
        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    }

    #[test]
    fn decodes_pg_scalar_typed_columns_and_fallbacks() {
        use arrow_array::cast::AsArray;
        use arrow_schema::TimeUnit;

        let cols = vec![
            "b".to_string(),
            "u".to_string(),
            "num_ok".to_string(),
            "num_nan".to_string(),
            "d".to_string(),
            "t".to_string(),
            "ttz".to_string(),
            "ts".to_string(),
            "tstz".to_string(),
            "iv_hms".to_string(),
            "iv_days".to_string(),
            "iv_cal".to_string(),
            "j".to_string(),
        ];
        let oids = vec![
            17, 2950, 1700, 1700, 1082, 1083, 1266, 1114, 1184, 1186, 1186, 1186, 3802,
        ];
        let rows = vec![vec![
            Some(r"\x0102fe".to_string()),
            Some("12345678-1234-5678-1234-567812345678".to_string()),
            Some("123.45".to_string()),
            Some("NaN".to_string()),
            Some("2024-01-02".to_string()),
            Some("14:30:15.123456".to_string()),
            Some("14:30:15+02".to_string()),
            Some("2024-01-02 03:04:05.123456".to_string()),
            Some("2024-01-02 03:04:05.123456+00".to_string()),
            Some("25:00:00.5".to_string()),
            Some("2 days 01:01:01.5".to_string()),
            Some("1 year 2 mons 3 days 04:05:06".to_string()),
            Some(r#"{"a":1}"#.to_string()),
        ]];
        let batch = rows_to_record_batch_with_column_types(&cols, &rows, &[false; 13], &oids)
            .expect("pg scalar typed batch");
        let schema = batch.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Binary);
        assert_eq!(batch.column(0).as_binary::<i32>().value(0), &[1, 2, 254]);
        assert_eq!(schema.field(1).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(
            schema
                .field(1)
                .metadata()
                .get(ARROW_EXTENSION_NAME)
                .map(String::as_str),
            Some(ARROW_UUID_EXTENSION)
        );
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert_eq!(
            schema
                .field(3)
                .metadata()
                .get(VOYAGER_META_TYPE)
                .map(String::as_str),
            Some("decimal")
        );
        assert_eq!(schema.field(4).data_type(), &DataType::Date32);
        assert_eq!(
            schema.field(5).data_type(),
            &DataType::Time64(TimeUnit::Microsecond)
        );
        assert_eq!(
            schema
                .field(6)
                .metadata()
                .get(VOYAGER_META_TYPE)
                .map(String::as_str),
            Some("time_tz")
        );
        assert_eq!(
            schema.field(7).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert_eq!(
            schema.field(8).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        );
        assert_eq!(
            schema.field(9).data_type(),
            &DataType::Duration(TimeUnit::Microsecond)
        );
        assert!(matches!(schema.field(10).data_type(), DataType::Struct(_)));
        assert_eq!(
            schema
                .field(10)
                .metadata()
                .get(VOYAGER_META_TYPE)
                .map(String::as_str),
            Some("duration")
        );
        assert!(matches!(schema.field(11).data_type(), DataType::Struct(_)));
        assert_eq!(
            schema
                .field(11)
                .metadata()
                .get(VOYAGER_META_TYPE)
                .map(String::as_str),
            Some("duration")
        );
        assert_eq!(
            schema
                .field(12)
                .metadata()
                .get(VOYAGER_META_ENCODING)
                .map(String::as_str),
            Some("json")
        );
    }

    #[test]
    fn preserves_pg_numeric_scale_and_parses_all_interval_styles() {
        use arrow_array::cast::AsArray;

        // NUMERIC(10, 2) typmod = ((10 << 16) | 2) + 4 = 655366
        let typmod_10_2 = ((10 << 16) | 2) + 4;
        let batch = rows_to_record_batch_with_column_metadata(
            &[
                "n_untyped".to_string(),
                "n_typmod".to_string(),
                "n_null".to_string(),
            ],
            &[vec![
                Some("1.50".to_string()),
                Some("1.50".to_string()),
                None,
            ]],
            &[false; 3],
            &[1700, 1700, 1700],
            &[-1, typmod_10_2, typmod_10_2],
        )
        .unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );
        assert_eq!(
            batch.schema().field(1).data_type(),
            &DataType::Decimal128(38, 2)
        );
        assert_eq!(
            batch.schema().field(2).data_type(),
            &DataType::Decimal128(38, 2)
        );
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Decimal128Type>()
                .value(0),
            150
        );

        // postgres_verbose and sql_standard intervals
        assert_eq!(
            parse_pg_interval("@ 0"),
            Some(BoltDuration::new(0, 0, 0, 0))
        );
        assert_eq!(
            parse_pg_interval("@ 5.25 secs ago"),
            Some(BoltDuration::new(0, 0, -5, -250_000_000))
        );
        assert_eq!(
            parse_pg_interval("+1-2 +3 +4:05:06.5"),
            Some(BoltDuration::new(14, 3, 14_706, 500_000_000))
        );
        assert_eq!(
            parse_pg_interval("-1-2"),
            Some(BoltDuration::new(-14, 0, 0, 0))
        );
        assert_eq!(
            parse_pg_interval("-3"),
            Some(BoltDuration::new(0, -3, 0, 0))
        );
    }

    #[test]
    fn timetz_and_uuid_arrays_propagate_recursive_child_metadata() {
        let ttz = batch_for(1270, &[Some("{14:30:15+02,NULL}")]);
        let DataType::List(item) = ttz.schema().field(0).data_type().clone() else {
            panic!("expected List for timetz[]");
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
        assert_eq!(
            item.metadata().get(VOYAGER_META_TYPE).map(String::as_str),
            Some(crate::value::META_TYPE_TIME_TZ)
        );

        let uuids = batch_for(
            2951,
            &[Some("{{12345678-1234-5678-1234-567812345678,NULL}}")],
        );
        let DataType::List(outer) = uuids.schema().field(0).data_type().clone() else {
            panic!("expected outer List for uuid[][]");
        };
        let DataType::List(leaf) = outer.data_type().clone() else {
            panic!("expected inner List for uuid[][]");
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
            Some(crate::value::META_TYPE_UUID)
        );
    }

    #[test]
    fn parses_age_float_specials_and_large_ints() {
        assert!(matches!(parse_agtype("NaN").unwrap(), AgeValue::Float(f) if f.is_nan()));
        assert_eq!(
            parse_agtype("Infinity").unwrap(),
            AgeValue::Float(f64::INFINITY)
        );
        assert_eq!(
            parse_agtype("-Infinity").unwrap(),
            AgeValue::Float(f64::NEG_INFINITY)
        );
        assert!(matches!(
            parse_agtype("99999999999999999999999999").unwrap(),
            AgeValue::Float(_)
        ));
    }
}
