//! Apache AGE `agtype` parser, graph entity structures, and Apache Arrow RecordBatch deserialization.

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, DurationMicrosecondBuilder,
    DurationNanosecondBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    LargeBinaryBuilder, StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use arrow_array::{Array, ArrayRef, ListArray, RecordBatch};
use arrow_buffer::{NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::bolt::canonical::{
    append_duration_struct, duration_struct_fields, make_duration_struct_builder,
};
use crate::bolt::packstream::{BoltDuration, ymd_to_days};
use crate::bolt::{BoltNode, BoltPath, BoltRelationship, BoltValue};
use crate::error::{NetError, Result};

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

    fn parse_object(&mut self) -> Result<AgeValue> {
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
                let val = self.parse_value()?;
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

    fn parse_array(&mut self) -> Result<AgeValue> {
        self.pos += 1; // skip '['
        self.skip_whitespace();
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.pos += 1;
        } else {
            loop {
                let val = self.parse_value()?;
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

    fn parse_value(&mut self) -> Result<AgeValue> {
        self.skip_whitespace();
        if self.pos >= self.bytes.len() {
            return Ok(AgeValue::Null);
        }

        let b = self.bytes[self.pos];
        match b {
            b'"' => self.parse_string().map(AgeValue::String),
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
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
    let val = parser.parse_value()?;
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
    let value = parser.parse_value()?;
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

    fn parse_value(&mut self) -> Result<PgArrayValue> {
        self.skip_whitespace();
        match self.peek() {
            Some(b'{') => self.parse_array(),
            Some(_) => self.parse_element(),
            None => Err(self.malformed()),
        }
    }

    fn parse_array(&mut self) -> Result<PgArrayValue> {
        self.pos += 1; // '{'
        let mut items = Vec::new();
        self.skip_whitespace();
        if self.peek() == Some(b'}') {
            self.pos += 1;
            return Ok(PgArrayValue::Array(items));
        }

        loop {
            items.push(self.parse_value()?);
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
    let Some(items) = value.children() else {
        return Ok(0);
    };
    if items.is_empty() {
        return Ok(1);
    }

    let child_depth = pg_array_depth(&items[0])?;
    for item in &items[1..] {
        if pg_array_depth(item)? != child_depth {
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
    match value.to_ascii_lowercase().as_str() {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        _ => Err(NetError::ProtocolError(format!(
            "Invalid boolean array element {value:?} in column {column:?}"
        ))),
    }
}

fn field_with_meta(name: &str, data_type: DataType, key: &str, value: &str) -> Field {
    let mut meta = HashMap::new();
    meta.insert(key.to_string(), value.to_string());
    Field::new(name, data_type, true).with_metadata(meta)
}

fn build_utf8_with_meta(
    name: &str,
    cells: &[Option<&str>],
    meta: &[(&str, &str)],
) -> (Field, ArrayRef) {
    let mut builder = StringBuilder::with_capacity(cells.len(), cells.len() * 16);
    for cell in cells {
        match cell {
            Some(v) => builder.append_value(v),
            None => builder.append_null(),
        }
    }
    let mut field = Field::new(name, DataType::Utf8, true);
    if !meta.is_empty() {
        let mut map = HashMap::with_capacity(meta.len());
        for (k, v) in meta {
            map.insert((*k).to_string(), (*v).to_string());
        }
        field = field.with_metadata(map);
    }
    (field, Arc::new(builder.finish()))
}

fn pg_numeric_typmod_scale(typmod: i32) -> Option<i16> {
    if typmod >= 4 {
        Some(((typmod - 4) & 0xFFFF) as i16)
    } else {
        None
    }
}

fn try_parse_decimal128_column(
    cells: &[Option<&str>],
    typmod: Option<i32>,
) -> Option<(u8, Vec<Option<i128>>)> {
    const MAX_DECIMAL128: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999_i128;

    let mut max_scale: usize = 0;
    if let Some(tm) = typmod
        && let Some(decl_scale) = pg_numeric_typmod_scale(tm)
    {
        if !(0..=38).contains(&decl_scale) {
            return None;
        }
        max_scale = decl_scale as usize;
    }

    let mut parsed_parts: Vec<Option<(bool, &str, &str)>> = Vec::with_capacity(cells.len());

    for cell in cells {
        let Some(raw) = cell else {
            parsed_parts.push(None);
            continue;
        };
        let s = raw.trim();
        let (neg, unsigned) = if let Some(rest) = s.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = s.strip_prefix('+') {
            (false, rest)
        } else {
            (false, s)
        };
        if unsigned.is_empty() || unsigned.contains(['e', 'E']) {
            return None;
        }
        let (int_part, frac_part) = match unsigned.split_once('.') {
            Some((i, f)) => {
                if f.contains('.') {
                    return None;
                }
                (i, f)
            }
            None => (unsigned, ""),
        };
        if (int_part.is_empty() && frac_part.is_empty())
            || !int_part.bytes().all(|b| b.is_ascii_digit())
            || !frac_part.bytes().all(|b| b.is_ascii_digit())
        {
            return None;
        }
        if frac_part.len() > 38 {
            return None;
        }
        max_scale = max_scale.max(frac_part.len());
        parsed_parts.push(Some((neg, int_part.trim_start_matches('0'), frac_part)));
    }

    let mut values = Vec::with_capacity(cells.len());
    for item in parsed_parts {
        let Some((neg, int_part, frac_part)) = item else {
            values.push(None);
            continue;
        };
        if int_part.len() + max_scale > 38 {
            return None;
        }
        let mut mag: i128 = 0;
        for b in int_part.bytes().chain(frac_part.bytes()) {
            mag = mag.checked_mul(10)?.checked_add((b - b'0') as i128)?;
        }
        for _ in 0..(max_scale - frac_part.len()) {
            mag = mag.checked_mul(10)?;
        }
        if mag > MAX_DECIMAL128 {
            return None;
        }
        let signed = if neg && mag != 0 { -mag } else { mag };
        values.push(Some(signed));
    }
    Some((max_scale as u8, values))
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
    let parts: Vec<&str> = off_str.split(':').collect();
    if parts.is_empty() || parts.len() > 3 {
        return None;
    }
    let oh: i32 = parts[0].parse().ok()?;
    let om: i32 = if parts.len() >= 2 {
        parts[1].parse().ok()?
    } else {
        0
    };
    let os: i32 = if parts.len() == 3 {
        parts[2].parse().ok()?
    } else {
        0
    };
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

fn pg_duration_micros(dur: &BoltDuration) -> Option<i64> {
    if dur.months != 0 || dur.days != 0 || dur.nanoseconds % 1_000 != 0 {
        return None;
    }
    dur.seconds
        .checked_mul(1_000_000)?
        .checked_add(dur.nanoseconds / 1_000)
}

fn pg_duration_nanos(dur: &BoltDuration) -> Option<i64> {
    if dur.months != 0 || dur.days != 0 {
        return None;
    }
    dur.seconds
        .checked_mul(1_000_000_000)?
        .checked_add(dur.nanoseconds)
}

fn build_pg_typed_values(
    field_name: &str,
    oid: i32,
    typmod: Option<i32>,
    cells: &[Option<&str>],
) -> Result<Option<(Field, ArrayRef)>> {
    let n = cells.len();
    match oid {
        16 => {
            let mut builder = BooleanBuilder::with_capacity(n);
            for cell in cells {
                match cell {
                    Some(v) => builder.append_value(parse_bool_element(v, field_name)?),
                    None => builder.append_null(),
                }
            }
            Ok(Some((
                Field::new(field_name, DataType::Boolean, true),
                Arc::new(builder.finish()),
            )))
        }
        20 | 21 | 23 => {
            let mut builder = Int64Builder::with_capacity(n);
            for cell in cells {
                match cell {
                    Some(v) => builder.append_value(parse_int_element(v, field_name)?),
                    None => builder.append_null(),
                }
            }
            Ok(Some((
                Field::new(field_name, DataType::Int64, true),
                Arc::new(builder.finish()),
            )))
        }
        700 | 701 => {
            let mut builder = Float64Builder::with_capacity(n);
            for cell in cells {
                match cell {
                    Some(v) => builder.append_value(parse_float_element(v, field_name)?),
                    None => builder.append_null(),
                }
            }
            Ok(Some((
                Field::new(field_name, DataType::Float64, true),
                Arc::new(builder.finish()),
            )))
        }
        1700 => {
            if let Some((scale, values)) = try_parse_decimal128_column(cells, typmod) {
                let mut builder = Decimal128Builder::with_capacity(n);
                for val in values {
                    match val {
                        Some(v) => builder.append_value(v),
                        None => builder.append_null(),
                    }
                }
                let arr = builder
                    .finish()
                    .with_precision_and_scale(38, scale as i8)
                    .map_err(NetError::ArrowError)?;
                Ok(Some((
                    Field::new(field_name, DataType::Decimal128(38, scale as i8), true),
                    Arc::new(arr),
                )))
            } else {
                Ok(Some(build_utf8_with_meta(
                    field_name,
                    cells,
                    &[("voyager.type", "decimal")],
                )))
            }
        }
        2950 => {
            let mut parsed = Vec::with_capacity(n);
            let mut all_ok = true;
            for cell in cells {
                match cell {
                    Some(v) => match parse_pg_uuid(v) {
                        Some(bytes) => parsed.push(Some(bytes)),
                        None => {
                            all_ok = false;
                            break;
                        }
                    },
                    None => parsed.push(None),
                }
            }
            if all_ok {
                let mut builder = FixedSizeBinaryBuilder::with_capacity(n, 16);
                for cell in parsed {
                    match cell {
                        Some(bytes) => builder.append_value(bytes).map_err(NetError::ArrowError)?,
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    field_with_meta(
                        field_name,
                        DataType::FixedSizeBinary(16),
                        "ARROW:extension:name",
                        "arrow.uuid",
                    ),
                    Arc::new(builder.finish()),
                )))
            } else {
                Ok(Some(build_utf8_with_meta(
                    field_name,
                    cells,
                    &[("voyager.type", "uuid")],
                )))
            }
        }
        17 => {
            let mut decoded: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
            let mut total_len = 0usize;
            for cell in cells {
                match cell {
                    Some(raw) => {
                        let bytes = decode_pg_bytea(raw).ok_or_else(|| {
                            NetError::ProtocolError(format!(
                                "Invalid bytea value in column {field_name:?}"
                            ))
                        })?;
                        total_len += bytes.len();
                        decoded.push(Some(bytes));
                    }
                    None => decoded.push(None),
                }
            }
            if total_len <= i32::MAX as usize {
                let mut builder = BinaryBuilder::with_capacity(n, total_len);
                for cell in decoded {
                    match cell {
                        Some(bytes) => builder.append_value(&bytes),
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    Field::new(field_name, DataType::Binary, true),
                    Arc::new(builder.finish()),
                )))
            } else {
                let mut builder = LargeBinaryBuilder::with_capacity(n, total_len);
                for cell in decoded {
                    match cell {
                        Some(bytes) => builder.append_value(&bytes),
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    Field::new(field_name, DataType::LargeBinary, true),
                    Arc::new(builder.finish()),
                )))
            }
        }
        1082 => {
            let mut parsed = Vec::with_capacity(n);
            let mut all_ok = true;
            for cell in cells {
                match cell {
                    Some(v) => match parse_pg_date_days(v) {
                        Some(days) => parsed.push(Some(days)),
                        None => {
                            all_ok = false;
                            break;
                        }
                    },
                    None => parsed.push(None),
                }
            }
            if all_ok {
                let mut builder = Date32Builder::with_capacity(n);
                for cell in parsed {
                    match cell {
                        Some(days) => builder.append_value(days),
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    Field::new(field_name, DataType::Date32, true),
                    Arc::new(builder.finish()),
                )))
            } else {
                Ok(Some(build_utf8_with_meta(
                    field_name,
                    cells,
                    &[("voyager.type", "date")],
                )))
            }
        }
        1083 => {
            let mut parsed = Vec::with_capacity(n);
            let mut all_ok = true;
            for cell in cells {
                match cell {
                    Some(v) => match parse_pg_time_micros(v) {
                        Some(us) => parsed.push(Some(us)),
                        None => {
                            all_ok = false;
                            break;
                        }
                    },
                    None => parsed.push(None),
                }
            }
            if all_ok {
                let mut builder = Time64MicrosecondBuilder::with_capacity(n);
                for cell in parsed {
                    match cell {
                        Some(us) => builder.append_value(us),
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    Field::new(field_name, DataType::Time64(TimeUnit::Microsecond), true),
                    Arc::new(builder.finish()),
                )))
            } else {
                Ok(Some(build_utf8_with_meta(
                    field_name,
                    cells,
                    &[("voyager.type", "time")],
                )))
            }
        }
        1266 => Ok(Some(build_utf8_with_meta(
            field_name,
            cells,
            &[("voyager.type", "time_tz")],
        ))),
        1114 => {
            let mut parsed = Vec::with_capacity(n);
            let mut all_ok = true;
            for cell in cells {
                match cell {
                    Some(v) => match parse_pg_timestamp_micros(v) {
                        Some(us) => parsed.push(Some(us)),
                        None => {
                            all_ok = false;
                            break;
                        }
                    },
                    None => parsed.push(None),
                }
            }
            if all_ok {
                let mut builder = TimestampMicrosecondBuilder::with_capacity(n);
                for cell in parsed {
                    match cell {
                        Some(us) => builder.append_value(us),
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    Field::new(
                        field_name,
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ),
                    Arc::new(builder.finish()),
                )))
            } else {
                Ok(Some(build_utf8_with_meta(
                    field_name,
                    cells,
                    &[("voyager.type", "local_datetime")],
                )))
            }
        }
        1184 => {
            let mut parsed = Vec::with_capacity(n);
            let mut all_utc = true;
            for cell in cells {
                match cell {
                    Some(v) => match parse_pg_timestamptz_micros(v) {
                        Some((us, 0)) => parsed.push(Some(us)),
                        _ => {
                            all_utc = false;
                            break;
                        }
                    },
                    None => parsed.push(None),
                }
            }
            if all_utc {
                let mut builder =
                    TimestampMicrosecondBuilder::with_capacity(n).with_timezone("UTC".to_string());
                for cell in parsed {
                    match cell {
                        Some(us) => builder.append_value(us),
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    Field::new(
                        field_name,
                        DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                        true,
                    ),
                    Arc::new(builder.finish()),
                )))
            } else {
                Ok(Some(build_utf8_with_meta(
                    field_name,
                    cells,
                    &[("voyager.type", "datetime_tz")],
                )))
            }
        }
        1186 => {
            let mut parsed = Vec::with_capacity(n);
            let mut all_ok = true;
            for cell in cells {
                match cell {
                    Some(v) => match parse_pg_interval(v) {
                        Some(dur) => parsed.push(Some(dur)),
                        None => {
                            all_ok = false;
                            break;
                        }
                    },
                    None => parsed.push(None),
                }
            }
            if !all_ok {
                return Ok(Some(build_utf8_with_meta(
                    field_name,
                    cells,
                    &[("voyager.type", "duration")],
                )));
            }
            if parsed
                .iter()
                .all(|d| d.as_ref().is_none_or(|v| pg_duration_micros(v).is_some()))
            {
                let mut builder = DurationMicrosecondBuilder::with_capacity(n);
                for cell in &parsed {
                    match cell.as_ref().and_then(pg_duration_micros) {
                        Some(us) => builder.append_value(us),
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    Field::new(field_name, DataType::Duration(TimeUnit::Microsecond), true),
                    Arc::new(builder.finish()),
                )))
            } else if parsed
                .iter()
                .all(|d| d.as_ref().is_none_or(|v| pg_duration_nanos(v).is_some()))
            {
                let mut builder = DurationNanosecondBuilder::with_capacity(n);
                for cell in &parsed {
                    match cell.as_ref().and_then(pg_duration_nanos) {
                        Some(ns) => builder.append_value(ns),
                        None => builder.append_null(),
                    }
                }
                Ok(Some((
                    Field::new(field_name, DataType::Duration(TimeUnit::Nanosecond), true),
                    Arc::new(builder.finish()),
                )))
            } else {
                let mut builder = make_duration_struct_builder(n);
                for cell in &parsed {
                    append_duration_struct(&mut builder, cell.as_ref());
                }
                Ok(Some((
                    field_with_meta(
                        field_name,
                        DataType::Struct(duration_struct_fields()),
                        "voyager.type",
                        "duration",
                    ),
                    Arc::new(builder.finish()),
                )))
            }
        }
        114 | 3802 => Ok(Some(build_utf8_with_meta(
            field_name,
            cells,
            &[("voyager.encoding", "json"), ("voyager.type", "json")],
        ))),
        19 | 25 | 869 | 1042 | 1043 => Ok(Some(build_utf8_with_meta(field_name, cells, &[]))),
        _ => Ok(None),
    }
}

fn array_column_batch(
    name: &str,
    elem_oid: i32,
    typmod: Option<i32>,
    rows: &[Vec<Option<String>>],
    col_idx: usize,
) -> Result<(Field, ArrayRef)> {
    let mut parsed: Vec<Option<PgArrayValue>> = Vec::with_capacity(rows.len());
    let mut depth: Option<usize> = None;
    for row in rows {
        match row.get(col_idx).and_then(|value| value.as_deref()) {
            Some(raw) => {
                let value = parse_pg_array_literal(raw)?;
                // An empty literal (`{}`) carries no dimensionality: PostgreSQL
                // serializes an empty array of any rank this way. Treat it as
                // compatible with the dimensions other rows reveal.
                let empty = matches!(&value, PgArrayValue::Array(items) if items.is_empty());
                if !empty {
                    let cell_depth = pg_array_depth(&value)?;
                    match depth {
                        Some(existing) if existing != cell_depth => {
                            return Err(NetError::ProtocolError(format!(
                                "PostgreSQL array column {name:?} mixes {existing}-dimensional and {cell_depth}-dimensional values"
                            )));
                        }
                        None => depth = Some(cell_depth),
                        _ => {}
                    }
                }
                parsed.push(Some(value));
            }
            None => parsed.push(None),
        }
    }

    // An all-NULL or all-empty column has no observable dimensionality; keep
    // the 1-D shape the previous decoder used so the Arrow type stays stable.
    let depth = depth.unwrap_or(1);

    // `level_lengths[level]` holds the child count of every node at that level.
    // Level 0 has one entry per database row (including SQL NULLs); deeper
    // levels are always array nodes because PostgreSQL arrays are rectangular.
    let mut level_lengths: Vec<Vec<Option<usize>>> = Vec::with_capacity(depth);
    let mut level_zero = Vec::with_capacity(parsed.len());
    let mut nodes: Vec<&PgArrayValue> = Vec::new();
    for cell in &parsed {
        match cell {
            Some(value) => {
                let items = value
                    .children()
                    .expect("a parsed array literal is always an array");
                level_zero.push(Some(items.len()));
                nodes.extend(items.iter());
            }
            None => level_zero.push(None),
        }
    }
    level_lengths.push(level_zero);

    for _ in 1..depth {
        let mut lengths = Vec::with_capacity(nodes.len());
        let mut next = Vec::new();
        for node in std::mem::take(&mut nodes) {
            let items = node.children().ok_or_else(|| {
                NetError::ProtocolError(
                    "Ragged PostgreSQL array literal: dimensions are inconsistent".to_string(),
                )
            })?;
            lengths.push(Some(items.len()));
            next.extend(items.iter());
        }
        level_lengths.push(lengths);
        nodes = next;
    }

    let mut flat_cells: Vec<Option<&str>> = Vec::with_capacity(nodes.len());
    for node in &nodes {
        match node {
            PgArrayValue::Element(value) => flat_cells.push(value.as_deref()),
            PgArrayValue::Array(_) => {
                return Err(NetError::ProtocolError(
                    "Ragged PostgreSQL array literal: dimensions are inconsistent".to_string(),
                ));
            }
        }
    }

    let (item_field, leaf_array) =
        match build_pg_typed_values("item", elem_oid, typmod, &flat_cells)? {
            Some(pair) => pair,
            None => build_utf8_with_meta("item", &flat_cells, &[]),
        };

    let mut child_field = item_field;
    let mut child_array = leaf_array;
    for level in (0..depth).rev() {
        let lengths = &level_lengths[level];
        let mut offsets: Vec<i32> = Vec::with_capacity(lengths.len() + 1);
        let mut validity = Vec::with_capacity(lengths.len());
        let mut has_null = false;
        let mut offset = 0i32;
        offsets.push(0);
        for length in lengths {
            match length {
                Some(length) => {
                    offset = offset.checked_add(*length as i32).ok_or_else(|| {
                        NetError::ProtocolError(
                            "PostgreSQL array column is too large to decode".to_string(),
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
                "Ragged PostgreSQL array literal: dimensions are inconsistent".to_string(),
            ));
        }

        let field = if level == 0 {
            Field::new(name, DataType::List(Arc::new(child_field.clone())), true)
        } else {
            Field::new("item", DataType::List(Arc::new(child_field.clone())), true)
        };
        let nulls = has_null.then(|| NullBuffer::from(validity));
        let list = ListArray::new(
            Arc::new(child_field),
            OffsetBuffer::new(offsets.into()),
            child_array,
            nulls,
        );
        child_array = Arc::new(list);
        child_field = field;
    }

    Ok((child_field, child_array))
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
    if column_names.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    let num_rows = rows.len();
    let num_cols = column_names.len();

    let mut fields = Vec::with_capacity(num_cols);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for (col_idx, name) in column_names.iter().enumerate() {
        if age_columns.get(col_idx).copied().unwrap_or(false) {
            let mut parsed_cells: Vec<Option<AgeValue>> = Vec::with_capacity(num_rows);
            let mut int_count = 0usize;
            let mut float_count = 0usize;
            let mut bool_count = 0usize;
            let mut string_count = 0usize;
            let mut node_count = 0usize;
            let mut edge_count = 0usize;
            let mut path_count = 0usize;
            let mut has_structured = false;
            let mut non_null_count = 0usize;

            for row in rows {
                match row.get(col_idx).and_then(|v| v.as_deref()) {
                    Some(raw) => {
                        let parsed = parse_agtype(raw)?;
                        match &parsed {
                            AgeValue::Null => {
                                parsed_cells.push(None);
                            }
                            AgeValue::Integer(_) => {
                                int_count += 1;
                                non_null_count += 1;
                                parsed_cells.push(Some(parsed));
                            }
                            AgeValue::Float(_) => {
                                float_count += 1;
                                non_null_count += 1;
                                parsed_cells.push(Some(parsed));
                            }
                            AgeValue::Boolean(_) => {
                                bool_count += 1;
                                non_null_count += 1;
                                parsed_cells.push(Some(parsed));
                            }
                            AgeValue::Vertex(_) => {
                                node_count += 1;
                                has_structured = true;
                                non_null_count += 1;
                                parsed_cells.push(Some(parsed));
                            }
                            AgeValue::Edge(_) => {
                                edge_count += 1;
                                has_structured = true;
                                non_null_count += 1;
                                parsed_cells.push(Some(parsed));
                            }
                            AgeValue::Path(_) => {
                                path_count += 1;
                                has_structured = true;
                                non_null_count += 1;
                                parsed_cells.push(Some(parsed));
                            }
                            AgeValue::List(_) | AgeValue::Map(_) => {
                                has_structured = true;
                                non_null_count += 1;
                                parsed_cells.push(Some(parsed));
                            }
                            AgeValue::String(_) => {
                                string_count += 1;
                                non_null_count += 1;
                                parsed_cells.push(Some(parsed));
                            }
                        }
                    }
                    None => parsed_cells.push(None),
                }
            }

            if non_null_count > 0 && int_count == non_null_count {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for cell in &parsed_cells {
                    match cell {
                        Some(AgeValue::Integer(i)) => builder.append_value(*i),
                        _ => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Int64, true));
                columns.push(Arc::new(builder.finish()));
            } else if non_null_count > 0 && float_count == non_null_count {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for cell in &parsed_cells {
                    match cell {
                        Some(AgeValue::Float(f)) => builder.append_value(*f),
                        _ => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Float64, true));
                columns.push(Arc::new(builder.finish()));
            } else if non_null_count > 0
                && (int_count + float_count) == non_null_count
                && int_count > 0
                && float_count > 0
            {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for cell in &parsed_cells {
                    match cell {
                        Some(AgeValue::Float(f)) => builder.append_value(*f),
                        Some(AgeValue::Integer(i)) => builder.append_value(*i as f64),
                        _ => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Float64, true));
                columns.push(Arc::new(builder.finish()));
            } else if non_null_count > 0 && bool_count == non_null_count {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for cell in &parsed_cells {
                    match cell {
                        Some(AgeValue::Boolean(b)) => builder.append_value(*b),
                        _ => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Boolean, true));
                columns.push(Arc::new(builder.finish()));
            } else if has_structured || (non_null_count > 0 && string_count != non_null_count) {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 64);
                for cell in &parsed_cells {
                    match cell {
                        Some(val) => {
                            let json_str = serde_json::to_string(&age_value_to_arrow_json(val))
                                .unwrap_or_else(|_| "null".to_string());
                            builder.append_value(&json_str);
                        }
                        None => builder.append_null(),
                    }
                }
                let mut metadata = HashMap::new();
                metadata.insert("voyager.encoding".to_string(), "json".to_string());
                if non_null_count > 0 && node_count == non_null_count {
                    metadata.insert("voyager.kind".to_string(), "node".to_string());
                } else if non_null_count > 0 && edge_count == non_null_count {
                    metadata.insert("voyager.kind".to_string(), "relationship".to_string());
                } else if non_null_count > 0 && path_count == non_null_count {
                    metadata.insert("voyager.kind".to_string(), "path".to_string());
                }
                fields.push(Field::new(name, DataType::Utf8, true).with_metadata(metadata));
                columns.push(Arc::new(builder.finish()));
            } else {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for cell in &parsed_cells {
                    match cell {
                        Some(AgeValue::String(s)) => builder.append_value(s),
                        _ => builder.append_null(),
                    }
                }
                fields.push(Field::new(name, DataType::Utf8, true));
                columns.push(Arc::new(builder.finish()));
            }
            continue;
        }

        if let Some(&oid) = column_types.get(col_idx) {
            let typmod = column_type_modifiers.get(col_idx).copied();
            if let Some(elem_oid) = pg_array_element_oid(oid) {
                let (field, array) = array_column_batch(name, elem_oid, typmod, rows, col_idx)?;
                fields.push(field);
                columns.push(array);
                continue;
            }
            let cells: Vec<Option<&str>> = rows
                .iter()
                .map(|row| row.get(col_idx).and_then(|v| v.as_deref()))
                .collect();
            if let Some((field, array)) = build_pg_typed_values(name, oid, typmod, &cells)? {
                fields.push(field);
                columns.push(array);
                continue;
            }
        }

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

        let data_type = if !has_non_null {
            DataType::Utf8
        } else if can_be_int {
            DataType::Int64
        } else if can_be_float {
            DataType::Float64
        } else if can_be_bool {
            DataType::Boolean
        } else {
            DataType::Utf8
        };

        fields.push(Field::new(name, data_type.clone(), true));

        match data_type {
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).and_then(|v| v.as_deref()) {
                        Some(val) => {
                            let cleaned = clean_agtype_string(val);
                            if let Ok(i) = cleaned.parse::<i64>() {
                                builder.append_value(i);
                            } else {
                                builder.append_null();
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).and_then(|v| v.as_deref()) {
                        Some(val) => {
                            let cleaned = clean_agtype_string(val);
                            if let Ok(f) = cleaned.parse::<f64>() {
                                builder.append_value(f);
                            } else {
                                builder.append_null();
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in rows {
                    match row.get(col_idx).and_then(|v| v.as_deref()) {
                        Some(val) => {
                            let cleaned = clean_agtype_string(val);
                            let b = cleaned == "t" || cleaned == "true";
                            builder.append_value(b);
                        }
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
            _ => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in rows {
                    match row.get(col_idx).and_then(|v| v.as_deref()) {
                        Some(val) => builder.append_value(val),
                        None => builder.append_null(),
                    }
                }
                columns.push(Arc::new(builder.finish()));
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, columns).map_err(NetError::ArrowError)
}

#[cfg(test)]
mod pg_array_tests {
    use super::*;
    use arrow_array::Array;
    use arrow_array::cast::AsArray;

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
        let error = rows_to_record_batch_with_column_types(&columns, &rows, &[false], &[1007])
            .expect_err("mixed dimensionalities are rejected");
        assert!(error.to_string().contains("mixes"));
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
                .get("ARROW:extension:name")
                .map(String::as_str),
            Some("arrow.uuid")
        );
        assert_eq!(schema.field(2).data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(schema.field(3).data_type(), &DataType::Utf8);
        assert_eq!(
            schema
                .field(3)
                .metadata()
                .get("voyager.type")
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
                .get("voyager.type")
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
                .get("voyager.type")
                .map(String::as_str),
            Some("duration")
        );
        assert!(matches!(schema.field(11).data_type(), DataType::Struct(_)));
        assert_eq!(
            schema
                .field(11)
                .metadata()
                .get("voyager.type")
                .map(String::as_str),
            Some("duration")
        );
        assert_eq!(
            schema
                .field(12)
                .metadata()
                .get("voyager.encoding")
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
