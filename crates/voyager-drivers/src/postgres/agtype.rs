//! Apache AGE `agtype` parser, graph entity structures, and Apache Arrow RecordBatch deserialization.

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

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

fn age_value_to_json(val: AgeValue) -> serde_json::Value {
    match val {
        AgeValue::Null => serde_json::Value::Null,
        AgeValue::Boolean(b) => serde_json::Value::Bool(b),
        AgeValue::Integer(i) => serde_json::Value::Number(i.into()),
        AgeValue::Float(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        AgeValue::String(s) => serde_json::Value::String(s),
        AgeValue::Vertex(v) => serde_json::to_value(v).unwrap_or(serde_json::Value::Null),
        AgeValue::Edge(e) => serde_json::to_value(e).unwrap_or(serde_json::Value::Null),
        AgeValue::Path(p) => serde_json::to_value(p).unwrap_or(serde_json::Value::Null),
        AgeValue::List(list) => {
            serde_json::Value::Array(list.into_iter().map(age_value_to_json).collect())
        }
        AgeValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (k, v) in map {
                obj.insert(k, age_value_to_json(v));
            }
            serde_json::Value::Object(obj)
        }
    }
}

fn age_map_to_json_map(map: HashMap<String, AgeValue>) -> HashMap<String, serde_json::Value> {
    let mut out = HashMap::with_capacity(map.len());
    for (k, v) in map {
        out.insert(k, age_value_to_json(v));
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
        let _ = self.consume("::numeric");
        if num_str.contains('.') || num_str.contains('e') || num_str.contains('E') {
            num_str.parse::<f64>().map(AgeValue::Float).map_err(|e| {
                NetError::ProtocolError(format!("Failed to parse float '{}': {}", num_str, e))
            })
        } else {
            num_str.parse::<i64>().map(AgeValue::Integer).map_err(|e| {
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

/// Dynamically transforms tabular row values into an Apache Arrow [`RecordBatch`].
pub fn rows_to_record_batch(
    column_names: &[String],
    rows: &[Vec<Option<String>>],
) -> Result<RecordBatch> {
    if column_names.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
    }

    let num_rows = rows.len();
    let num_cols = column_names.len();

    let mut inferred_types = vec![DataType::Int64; num_cols];

    // First pass: type inference per column
    for (col_idx, inferred_type) in inferred_types.iter_mut().enumerate() {
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

        if !has_non_null {
            *inferred_type = DataType::Utf8;
        } else if can_be_int {
            *inferred_type = DataType::Int64;
        } else if can_be_float {
            *inferred_type = DataType::Float64;
        } else if can_be_bool {
            *inferred_type = DataType::Boolean;
        } else {
            *inferred_type = DataType::Utf8;
        }
    }

    // Build Arrow arrays based on inferred types
    let mut fields = Vec::with_capacity(num_cols);
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

    for (col_idx, name) in column_names.iter().enumerate() {
        let data_type = &inferred_types[col_idx];
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
