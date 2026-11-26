//! FalkorDB Graph Result parser, entity conversion, and Apache Arrow RecordBatch builder.
//!
//! Parses tabular execution results from FalkorDB's `GRAPH.QUERY` and `GRAPH.RO_QUERY`
//! commands (in both RESP2 and RESP3 formats), converting Graph entities (nodes,
//! relationships, paths) into unified [`BoltNode`], [`BoltRelationship`], and columnar
//! Apache Arrow [`RecordBatch`] structures.

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::collections::HashMap;
use std::sync::Arc;

use crate::bolt::{
    BoltDate, BoltDuration, BoltLocalDateTime, BoltLocalTime, BoltNode, BoltPath, BoltPoint2D,
    BoltRelationship, BoltUnboundRelationship, BoltValue, build_bolt_record_batch,
};
use crate::engine::{QueryResult, QuerySummary};
use crate::error::{NetError, Result};
use crate::redis::resp::RespValue;

/// Parsed execution statistics from FalkorDB query execution.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct FalkorStatistics {
    /// Number of graph nodes created.
    pub nodes_created: u64,
    /// Number of graph nodes deleted.
    pub nodes_deleted: u64,
    /// Number of graph relationships created.
    pub relationships_created: u64,
    /// Number of graph relationships deleted.
    pub relationships_deleted: u64,
    /// Number of properties set or updated on nodes/relationships.
    pub properties_set: u64,
    /// Number of node labels added.
    pub labels_added: u64,
    /// Number of node labels removed.
    pub labels_removed: u64,
    /// Number of schema indexes created.
    pub indices_created: u64,
    /// Number of schema indexes deleted.
    pub indices_deleted: u64,
    /// Whether this execution was cached by FalkorDB plan cache.
    pub cached_execution: bool,
    /// Internal execution time in milliseconds as reported by FalkorDB.
    pub internal_execution_time_ms: f64,
    /// Raw unparsed statistics strings returned by server.
    pub raw_stats: Vec<String>,
}

impl FalkorStatistics {
    /// Parses execution statistics from a RESP array of strings.
    pub fn parse(stats_array: &[RespValue]) -> Self {
        let mut stats = Self::default();

        for item in stats_array {
            let line = item.to_string_lossy();
            stats.raw_stats.push(line.clone());

            if let Some((key, val)) = line.split_once(':') {
                let key_trimmed = key.trim();
                let val_trimmed = val.trim();

                match key_trimmed {
                    "Nodes created" => {
                        stats.nodes_created = val_trimmed.parse().unwrap_or(0);
                    }
                    "Nodes deleted" => {
                        stats.nodes_deleted = val_trimmed.parse().unwrap_or(0);
                    }
                    "Relationships created" => {
                        stats.relationships_created = val_trimmed.parse().unwrap_or(0);
                    }
                    "Relationships deleted" => {
                        stats.relationships_deleted = val_trimmed.parse().unwrap_or(0);
                    }
                    "Properties set" => {
                        stats.properties_set = val_trimmed.parse().unwrap_or(0);
                    }
                    "Labels added" => {
                        stats.labels_added = val_trimmed.parse().unwrap_or(0);
                    }
                    "Labels removed" => {
                        stats.labels_removed = val_trimmed.parse().unwrap_or(0);
                    }
                    "Indices created" | "Indexes added" => {
                        stats.indices_created = val_trimmed.parse().unwrap_or(0);
                    }
                    "Indices deleted" | "Indexes removed" => {
                        stats.indices_deleted = val_trimmed.parse().unwrap_or(0);
                    }
                    "Cached execution" => {
                        stats.cached_execution = val_trimmed == "1" || val_trimmed == "true";
                    }
                    "Query internal execution time" => {
                        // Format e.g.: "24.918253 milliseconds"
                        if let Some(num_part) = val_trimmed.split_whitespace().next() {
                            stats.internal_execution_time_ms = num_part.parse().unwrap_or(0.0);
                        }
                    }
                    _ => {}
                }
            }
        }

        stats
    }

    /// Converts into the unified Voyager [`QuerySummary`].
    pub fn to_query_summary(&self) -> QuerySummary {
        QuerySummary {
            nodes_created: self.nodes_created,
            nodes_deleted: self.nodes_deleted,
            relationships_created: self.relationships_created,
            relationships_deleted: self.relationships_deleted,
            properties_set: self.properties_set,
            labels_added: self.labels_added,
            labels_removed: self.labels_removed,
            indexes_added: self.indices_created,
            constraints_added: 0,
            execution_time_ms: self.internal_execution_time_ms.round() as u64,
        }
    }
}

/// Represents a graph Node returned by FalkorDB.
#[derive(Debug, Clone, PartialEq)]
pub struct FalkorNode {
    /// Internal entity integer identifier.
    pub id: i64,
    /// Set of string labels attached to this node.
    pub labels: Vec<String>,
    /// Node properties map.
    pub properties: HashMap<String, RespValue>,
}

impl FalkorNode {
    /// Parses a `FalkorNode` from a RESP array or map representation.
    ///
    /// FalkorDB encodes nodes in RESP2 as:
    /// `[["id", <id>], ["labels", [<label1>, ...]], ["properties", [[k1, v1], [k2, v2]]]]`
    /// or in RESP3 as a map `%{"id": ..., "labels": ..., "properties": ...}`.
    pub fn parse(val: &RespValue) -> Result<Self> {
        let mut id = 0i64;
        let mut labels = Vec::new();
        let mut properties = HashMap::new();

        if let Some(pairs) = val.as_map() {
            for (k, v) in pairs {
                match k.as_str().unwrap_or("") {
                    "id" => id = v.as_i64().unwrap_or(0),
                    "labels" => {
                        if let Some(arr) = v.as_array() {
                            labels = arr.iter().map(|item| item.to_string_lossy()).collect();
                        }
                    }
                    "properties" => {
                        properties = v.to_hashmap();
                    }
                    _ => {}
                }
            }
        } else if let Some(items) = val.as_array() {
            for item in items {
                if let Some(pair) = item.as_array()
                    && pair.len() == 2
                {
                    let key_str = pair[0].to_string_lossy();
                    match key_str.as_str() {
                        "id" => id = pair[1].as_i64().unwrap_or(0),
                        "labels" => {
                            if let Some(arr) = pair[1].as_array() {
                                labels = arr.iter().map(|lbl| lbl.to_string_lossy()).collect();
                            }
                        }
                        "properties" => {
                            properties = pair[1].to_hashmap();
                        }
                        _ => {}
                    }
                }
            }
        } else {
            return Err(NetError::ProtocolError(format!(
                "Expected FalkorDB Node structure, got {:?}",
                val
            )));
        }

        Ok(Self {
            id,
            labels,
            properties,
        })
    }

    /// Converts this FalkorDB node into the unified [`BoltNode`].
    pub fn to_bolt_node(&self) -> BoltNode {
        let mut bolt_props = HashMap::new();
        for (k, v) in &self.properties {
            bolt_props.insert(k.clone(), resp_to_bolt_value(v));
        }

        BoltNode {
            id: self.id,
            labels: self.labels.clone(),
            properties: bolt_props,
            element_id: Some(self.id.to_string()),
        }
    }
}

/// Represents a graph Relationship returned by FalkorDB.
#[derive(Debug, Clone, PartialEq)]
pub struct FalkorRelationship {
    /// Internal entity integer identifier.
    pub id: i64,
    /// Relationship type label (e.g. `"KNOWS"`).
    pub rel_type: String,
    /// Starting node integer ID.
    pub src_node: i64,
    /// Destination node integer ID.
    pub dest_node: i64,
    /// Relationship properties map.
    pub properties: HashMap<String, RespValue>,
}

impl FalkorRelationship {
    /// Parses a `FalkorRelationship` from a RESP array or map representation.
    ///
    /// FalkorDB encodes edges in RESP2 as:
    /// `[["id", <id>], ["type", <type>], ["src_node", <src>], ["dest_node", <dest>], ["properties", [...]]]`
    pub fn parse(val: &RespValue) -> Result<Self> {
        let mut id = 0i64;
        let mut rel_type = String::new();
        let mut src_node = 0i64;
        let mut dest_node = 0i64;
        let mut properties = HashMap::new();

        if let Some(pairs) = val.as_map() {
            for (k, v) in pairs {
                match k.as_str().unwrap_or("") {
                    "id" => id = v.as_i64().unwrap_or(0),
                    "type" => rel_type = v.to_string_lossy(),
                    "src_node" => src_node = v.as_i64().unwrap_or(0),
                    "dest_node" => dest_node = v.as_i64().unwrap_or(0),
                    "properties" => properties = v.to_hashmap(),
                    _ => {}
                }
            }
        } else if let Some(items) = val.as_array() {
            for item in items {
                if let Some(pair) = item.as_array()
                    && pair.len() == 2
                {
                    let key_str = pair[0].to_string_lossy();
                    match key_str.as_str() {
                        "id" => id = pair[1].as_i64().unwrap_or(0),
                        "type" => rel_type = pair[1].to_string_lossy(),
                        "src_node" => src_node = pair[1].as_i64().unwrap_or(0),
                        "dest_node" => dest_node = pair[1].as_i64().unwrap_or(0),
                        "properties" => properties = pair[1].to_hashmap(),
                        _ => {}
                    }
                }
            }
        } else {
            return Err(NetError::ProtocolError(format!(
                "Expected FalkorDB Relationship structure, got {:?}",
                val
            )));
        }

        Ok(Self {
            id,
            rel_type,
            src_node,
            dest_node,
            properties,
        })
    }

    /// Converts this FalkorDB relationship into the unified [`BoltRelationship`].
    pub fn to_bolt_relationship(&self) -> BoltRelationship {
        let mut bolt_props = HashMap::new();
        for (k, v) in &self.properties {
            bolt_props.insert(k.clone(), resp_to_bolt_value(v));
        }

        BoltRelationship {
            id: self.id,
            start_node_id: self.src_node,
            end_node_id: self.dest_node,
            rel_type: self.rel_type.clone(),
            properties: bolt_props,
            element_id: Some(self.id.to_string()),
            start_element_id: Some(self.src_node.to_string()),
            end_element_id: Some(self.dest_node.to_string()),
        }
    }
}

/// Helper converting a [`RespValue`] to a [`BoltValue`].
pub fn resp_to_bolt_value(val: &RespValue) -> BoltValue {
    match val {
        RespValue::Null => BoltValue::Null,
        RespValue::BulkString(None) => BoltValue::Null,
        RespValue::Array(None) => BoltValue::Null,
        RespValue::Boolean(b) => BoltValue::Boolean(*b),
        RespValue::Integer(i) => BoltValue::Integer(*i),
        RespValue::Double(f) => BoltValue::Float(*f),
        RespValue::SimpleString(s) => BoltValue::String(s.clone()),
        RespValue::BigNumber(s) => BoltValue::String(s.clone()),
        RespValue::BulkString(Some(b)) => {
            BoltValue::String(String::from_utf8_lossy(b).into_owned())
        }
        RespValue::Array(Some(arr)) => {
            BoltValue::List(arr.iter().map(resp_to_bolt_value).collect())
        }
        RespValue::Set(arr) | RespValue::Push(arr) => {
            BoltValue::List(arr.iter().map(resp_to_bolt_value).collect())
        }
        RespValue::Map(pairs) => {
            let mut map = HashMap::new();
            for (k, v) in pairs {
                map.insert(k.to_string_lossy(), resp_to_bolt_value(v));
            }
            BoltValue::Map(map)
        }
        other => BoltValue::String(other.to_string_lossy()),
    }
}

/// Complete tabular execution result from a FalkorDB `GRAPH.QUERY` or `GRAPH.RO_QUERY`.
#[derive(Debug, Clone, PartialEq)]
pub struct FalkorQueryResult {
    /// Column names from the `RETURN` projection.
    pub columns: Vec<String>,
    /// Tabular data rows, where each row contains one `RespValue` per column.
    pub rows: Vec<Vec<RespValue>>,
    /// Server-side execution statistics and mutation counters.
    pub statistics: FalkorStatistics,
}

impl FalkorQueryResult {
    /// Parses a complete FalkorDB query reply from the top-level RESP response.
    ///
    /// Handles:
    /// - 3-element arrays: `[header_columns, data_rows, statistics]`
    /// - 1-element arrays: `[statistics]` (queries with no `RETURN` clause)
    pub fn parse(resp: RespValue) -> Result<Self> {
        let items = match resp {
            RespValue::Array(Some(arr)) => arr,
            RespValue::Error(err) => {
                return Err(NetError::ExecutionError(format!(
                    "FalkorDB query error: {}",
                    err
                )));
            }
            other => {
                return Err(NetError::ProtocolError(format!(
                    "Expected RESP array from FalkorDB query, got {:?}",
                    other
                )));
            }
        };

        if items.len() == 3 {
            // [0] Headers
            let columns = match &items[0] {
                RespValue::Array(Some(cols)) => cols.iter().map(|c| c.to_string_lossy()).collect(),
                _ => Vec::new(),
            };

            // [1] Rows
            let mut rows = Vec::new();
            if let RespValue::Array(Some(raw_rows)) = &items[1] {
                for raw_row in raw_rows {
                    if let RespValue::Array(Some(cells)) = raw_row {
                        rows.push(cells.clone());
                    }
                }
            }

            // [2] Statistics
            let statistics = match &items[2] {
                RespValue::Array(Some(stats_arr)) => FalkorStatistics::parse(stats_arr),
                _ => FalkorStatistics::default(),
            };

            Ok(Self {
                columns,
                rows,
                statistics,
            })
        } else if items.len() == 1 {
            // Non-returning query with statistics only
            let statistics = match &items[0] {
                RespValue::Array(Some(stats_arr)) => FalkorStatistics::parse(stats_arr),
                _ => FalkorStatistics::default(),
            };

            Ok(Self {
                columns: Vec::new(),
                rows: Vec::new(),
                statistics,
            })
        } else {
            Err(NetError::ProtocolError(format!(
                "Unexpected FalkorDB query response array length: {} (expected 1 or 3)",
                items.len()
            )))
        }
    }

    /// Converts tabular result rows into an Apache Arrow [`RecordBatch`].
    pub fn to_arrow_record_batch(&self) -> Result<RecordBatch> {
        let num_cols = self.columns.len();
        let num_rows = self.rows.len();

        if num_cols == 0 || num_rows == 0 {
            // Build an empty RecordBatch with matching schema
            let fields: Vec<Field> = self
                .columns
                .iter()
                .map(|name| Field::new(name, DataType::Utf8, true))
                .collect();
            let schema = Arc::new(Schema::new(fields));
            let columns: Vec<ArrayRef> = self
                .columns
                .iter()
                .map(|_| Arc::new(StringBuilder::new().finish()) as ArrayRef)
                .collect();
            return RecordBatch::try_new(schema, columns).map_err(NetError::ArrowError);
        }

        // Dynamic type inference per column
        let mut inferred_types = vec![DataType::Utf8; num_cols];

        for (col_idx, inferred_type) in inferred_types.iter_mut().enumerate() {
            let mut has_non_null = false;
            let mut can_be_int = true;
            let mut can_be_float = true;
            let mut can_be_bool = true;

            for row in &self.rows {
                if let Some(cell) = row.get(col_idx)
                    && !cell.is_null()
                {
                    has_non_null = true;

                    match cell {
                        RespValue::Integer(_) => {
                            can_be_bool = false;
                        }
                        RespValue::Double(_) => {
                            can_be_int = false;
                            can_be_bool = false;
                        }
                        RespValue::Boolean(_) => {
                            can_be_int = false;
                            can_be_float = false;
                        }
                        RespValue::SimpleString(s) => {
                            if can_be_bool && cell.as_bool().is_none() {
                                can_be_bool = false;
                            }
                            if can_be_int && s.parse::<i64>().is_err() {
                                can_be_int = false;
                            }
                            if can_be_float && s.parse::<f64>().is_err() {
                                can_be_float = false;
                            }
                        }
                        RespValue::BulkString(Some(b)) => {
                            if let Ok(s) = std::str::from_utf8(b) {
                                if can_be_bool && cell.as_bool().is_none() {
                                    can_be_bool = false;
                                }
                                if can_be_int && s.parse::<i64>().is_err() {
                                    can_be_int = false;
                                }
                                if can_be_float && s.parse::<f64>().is_err() {
                                    can_be_float = false;
                                }
                            } else {
                                can_be_int = false;
                                can_be_float = false;
                                can_be_bool = false;
                            }
                        }
                        _ => {
                            // Nested structures, nodes, edges promote to Utf8 / JSON
                            can_be_int = false;
                            can_be_float = false;
                            can_be_bool = false;
                        }
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

        // Build Arrow columnar arrays
        let mut fields = Vec::with_capacity(num_cols);
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);

        for (col_idx, name) in self.columns.iter().enumerate() {
            let data_type = &inferred_types[col_idx];
            fields.push(Field::new(name, data_type.clone(), true));

            match data_type {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_rows);
                    for row in &self.rows {
                        match row.get(col_idx) {
                            Some(cell) if !cell.is_null() => {
                                if let Some(i) = cell.as_i64() {
                                    builder.append_value(i);
                                } else {
                                    builder.append_null();
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(num_rows);
                    for row in &self.rows {
                        match row.get(col_idx) {
                            Some(cell) if !cell.is_null() => {
                                if let Some(f) = cell.as_f64() {
                                    builder.append_value(f);
                                } else {
                                    builder.append_null();
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                DataType::Boolean => {
                    let mut builder = BooleanBuilder::with_capacity(num_rows);
                    for row in &self.rows {
                        match row.get(col_idx) {
                            Some(cell) if !cell.is_null() => {
                                if let Some(b) = cell.as_bool() {
                                    builder.append_value(b);
                                } else {
                                    builder.append_null();
                                }
                            }
                            _ => builder.append_null(),
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
                _ => {
                    // String / JSON fallback
                    let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                    for row in &self.rows {
                        match row.get(col_idx) {
                            Some(cell) if !cell.is_null() => {
                                builder.append_value(cell.to_string_lossy());
                            }
                            _ => builder.append_null(),
                        }
                    }
                    columns.push(Arc::new(builder.finish()));
                }
            }
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns).map_err(NetError::ArrowError)
    }

    /// Alias for [`to_arrow_record_batch`].
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        self.to_arrow_record_batch()
    }

    /// Converts this tabular query result into the workspace's unified [`QueryResult`].
    pub fn to_query_result(&self) -> Result<QueryResult> {
        let summary = self.statistics.to_query_summary();

        if self.columns.is_empty() {
            return Ok(QueryResult::empty(summary));
        }

        let batch = self.to_arrow_record_batch()?;
        Ok(QueryResult::new(self.columns.clone(), vec![batch], summary))
    }
}

/// Cached label, relationship type, and property key index tables for one FalkorDB graph.
#[derive(Debug, Clone, Default)]
pub struct FalkorGraphSchema {
    /// Graph version reported in query statistics.
    pub version: Option<i64>,
    /// Label names indexed by compact label ID.
    pub labels: Vec<String>,
    /// Relationship type names indexed by compact type ID.
    pub relationships: Vec<String>,
    /// Property key names indexed by compact property ID.
    pub properties: Vec<String>,
}

/// Parsed `--compact` query response from FalkorDB before schema resolution.
#[derive(Debug, Clone)]
pub struct FalkorCompactResult {
    /// Column names from the `RETURN` projection.
    pub columns: Vec<String>,
    /// Raw compact cell values (`[scalar_type, raw_value]`) per row.
    pub rows: Vec<Vec<RespValue>>,
    /// Parsed query execution statistics.
    pub statistics: FalkorStatistics,
    /// Graph schema version reported in statistics (`Graph version: N`).
    pub graph_version: Option<i64>,
}

/// Extracts `Graph version: N` from a FalkorDB statistics array.
fn parse_graph_version(stats_array: &[RespValue]) -> Option<i64> {
    for item in stats_array {
        let line = item.to_string_lossy();
        if let Some((key, val)) = line.split_once(':')
            && key.trim() == "Graph version"
        {
            return val.trim().parse::<i64>().ok();
        }
    }
    None
}

impl FalkorCompactResult {
    /// Parses a complete `--compact` FalkorDB `GRAPH.QUERY` response.
    pub fn parse(resp: RespValue) -> Result<Self> {
        let items = match resp {
            RespValue::Array(Some(arr)) => arr,
            RespValue::Error(err) => {
                return Err(NetError::ExecutionError(format!(
                    "FalkorDB query error: {}",
                    err
                )));
            }
            other => {
                return Err(NetError::ProtocolError(format!(
                    "Expected RESP array from FalkorDB query, got {:?}",
                    other
                )));
            }
        };

        if items.len() == 3 {
            let columns = match &items[0] {
                RespValue::Array(Some(cols)) => cols
                    .iter()
                    .map(|c| {
                        if let Some(pair) = c.as_array()
                            && pair.len() >= 2
                        {
                            pair[1].to_string_lossy()
                        } else {
                            c.to_string_lossy()
                        }
                    })
                    .collect(),
                _ => Vec::new(),
            };

            let mut rows = Vec::new();
            if let RespValue::Array(Some(raw_rows)) = &items[1] {
                for raw_row in raw_rows {
                    if let RespValue::Array(Some(cells)) = raw_row {
                        rows.push(cells.clone());
                    }
                }
            }

            let (statistics, graph_version) = match &items[2] {
                RespValue::Array(Some(stats_arr)) => (
                    FalkorStatistics::parse(stats_arr),
                    parse_graph_version(stats_arr),
                ),
                _ => (FalkorStatistics::default(), None),
            };

            Ok(Self {
                columns,
                rows,
                statistics,
                graph_version,
            })
        } else if items.len() == 1 {
            let (statistics, graph_version) = match &items[0] {
                RespValue::Array(Some(stats_arr)) => (
                    FalkorStatistics::parse(stats_arr),
                    parse_graph_version(stats_arr),
                ),
                _ => (FalkorStatistics::default(), None),
            };
            Ok(Self {
                columns: Vec::new(),
                rows: Vec::new(),
                statistics,
                graph_version,
            })
        } else {
            Err(NetError::ProtocolError(format!(
                "Unexpected FalkorDB query response array length: {} (expected 1 or 3)",
                items.len()
            )))
        }
    }

    /// Returns `true` when decoding these rows requires schema lookups or refreshes.
    pub fn needs_schema_refresh(&self, schema: Option<&FalkorGraphSchema>) -> bool {
        let mut max_label = None;
        let mut max_rel = None;
        let mut max_prop = None;
        for row in &self.rows {
            for cell in row {
                collect_compact_schema_indices(cell, &mut max_label, &mut max_rel, &mut max_prop);
            }
        }
        if max_label.is_none() && max_rel.is_none() && max_prop.is_none() {
            return false;
        }
        let Some(cached) = schema else {
            return true;
        };
        if self.graph_version.is_some() && cached.version != self.graph_version {
            return true;
        }
        if let Some(idx) = max_label
            && idx >= cached.labels.len()
        {
            return true;
        }
        if let Some(idx) = max_rel
            && idx >= cached.relationships.len()
        {
            return true;
        }
        if let Some(idx) = max_prop
            && idx >= cached.properties.len()
        {
            return true;
        }
        false
    }

    /// Converts compact rows into unified [`BoltValue`]s and builds an Arrow [`QueryResult`].
    pub fn to_query_result(&self, schema: &FalkorGraphSchema) -> Result<QueryResult> {
        let summary = self.statistics.to_query_summary();
        if self.columns.is_empty() {
            return Ok(QueryResult::empty(summary));
        }

        let mut bolt_rows = Vec::with_capacity(self.rows.len());
        for row in &self.rows {
            let mut bolt_row = Vec::with_capacity(row.len());
            for cell in row {
                bolt_row.push(decode_compact_cell(cell, schema)?);
            }
            bolt_rows.push(bolt_row);
        }

        let batch = build_bolt_record_batch(&self.columns, &bolt_rows)?;
        let result = QueryResult::new(self.columns.clone(), vec![batch], summary);
        if result
            .batches
            .iter()
            .any(crate::bolt::canonical::batch_needs_bolt_rows)
        {
            Ok(result.with_bolt_rows(bolt_rows))
        } else {
            Ok(result)
        }
    }
}

fn update_max(slot: &mut Option<usize>, candidate: i64) {
    if let Ok(idx) = usize::try_from(candidate) {
        *slot = Some(slot.map_or(idx, |cur| cur.max(idx)));
    }
}

fn collect_compact_schema_indices(
    cell: &RespValue,
    max_label: &mut Option<usize>,
    max_rel: &mut Option<usize>,
    max_prop: &mut Option<usize>,
) {
    let Some(pair) = cell.as_array() else {
        return;
    };
    if pair.len() < 2 {
        return;
    }
    let Some(scalar_type) = pair[0].as_i64() else {
        return;
    };
    let val = &pair[1];
    match scalar_type {
        6 => {
            if let Some(items) = val.as_array() {
                for item in items {
                    collect_compact_schema_indices(item, max_label, max_rel, max_prop);
                }
            }
        }
        7 => {
            if let Some(fields) = val.as_array()
                && fields.len() >= 5
            {
                if let Some(rel_id) = fields[1].as_i64() {
                    update_max(max_rel, rel_id);
                }
                if let Some(props) = fields[4].as_array() {
                    for prop in props {
                        if let Some(p_arr) = prop.as_array()
                            && p_arr.len() >= 3
                        {
                            if let Some(prop_id) = p_arr[0].as_i64() {
                                update_max(max_prop, prop_id);
                            }
                            let nested = RespValue::Array(Some(p_arr[1..3].to_vec()));
                            collect_compact_schema_indices(&nested, max_label, max_rel, max_prop);
                        }
                    }
                }
            }
        }
        8 => {
            if let Some(fields) = val.as_array()
                && fields.len() >= 3
            {
                if let Some(lbls) = fields[1].as_array() {
                    for lbl in lbls {
                        if let Some(lbl_id) = lbl.as_i64() {
                            update_max(max_label, lbl_id);
                        }
                    }
                }
                if let Some(props) = fields[2].as_array() {
                    for prop in props {
                        if let Some(p_arr) = prop.as_array()
                            && p_arr.len() >= 3
                        {
                            if let Some(prop_id) = p_arr[0].as_i64() {
                                update_max(max_prop, prop_id);
                            }
                            let nested = RespValue::Array(Some(p_arr[1..3].to_vec()));
                            collect_compact_schema_indices(&nested, max_label, max_rel, max_prop);
                        }
                    }
                }
            }
        }
        9 => {
            if let Some(path_parts) = val.as_array()
                && path_parts.len() >= 2
            {
                collect_compact_schema_indices(&path_parts[0], max_label, max_rel, max_prop);
                collect_compact_schema_indices(&path_parts[1], max_label, max_rel, max_prop);
            }
        }
        10 => {
            if let Some(items) = val.as_array() {
                let mut i = 1;
                while i < items.len() {
                    collect_compact_schema_indices(&items[i], max_label, max_rel, max_prop);
                    i += 2;
                }
            }
        }
        _ => {}
    }
}

fn decode_compact_properties(
    props_val: &RespValue,
    schema: &FalkorGraphSchema,
) -> Result<HashMap<String, BoltValue>> {
    let mut map = HashMap::new();
    let Some(items) = props_val.as_array() else {
        return Ok(map);
    };
    for prop in items {
        if let Some(parts) = prop.as_array()
            && parts.len() >= 3
        {
            let prop_idx = parts[0].as_i64().unwrap_or(-1);
            let key = usize::try_from(prop_idx)
                .ok()
                .and_then(|idx| schema.properties.get(idx))
                .cloned()
                .unwrap_or_else(|| format!("prop_{}", prop_idx));
            let cell = RespValue::Array(Some(vec![parts[1].clone(), parts[2].clone()]));
            map.insert(key, decode_compact_cell(&cell, schema)?);
        }
    }
    Ok(map)
}

fn decode_compact_node(val: &RespValue, schema: &FalkorGraphSchema) -> Result<BoltNode> {
    let fields = val.as_array().ok_or_else(|| {
        NetError::ProtocolError(format!(
            "Expected compact FalkorDB node array, got {:?}",
            val
        ))
    })?;
    if fields.len() < 3 {
        return Err(NetError::ProtocolError(format!(
            "Compact FalkorDB node array too short: {:?}",
            fields
        )));
    }
    let id = fields[0].as_i64().unwrap_or(0);
    let mut labels = Vec::new();
    if let Some(lbl_ids) = fields[1].as_array() {
        for lbl_id_val in lbl_ids {
            if let Some(lbl_id) = lbl_id_val.as_i64()
                && let Ok(idx) = usize::try_from(lbl_id)
                && let Some(name) = schema.labels.get(idx)
            {
                labels.push(name.clone());
            }
        }
    }
    let properties = decode_compact_properties(&fields[2], schema)?;
    Ok(BoltNode {
        id,
        labels,
        properties,
        element_id: None,
    })
}

fn decode_compact_edge(val: &RespValue, schema: &FalkorGraphSchema) -> Result<BoltRelationship> {
    let fields = val.as_array().ok_or_else(|| {
        NetError::ProtocolError(format!(
            "Expected compact FalkorDB edge array, got {:?}",
            val
        ))
    })?;
    if fields.len() < 5 {
        return Err(NetError::ProtocolError(format!(
            "Compact FalkorDB edge array too short: {:?}",
            fields
        )));
    }
    let id = fields[0].as_i64().unwrap_or(0);
    let rel_idx = fields[1].as_i64().unwrap_or(-1);
    let rel_type = usize::try_from(rel_idx)
        .ok()
        .and_then(|idx| schema.relationships.get(idx))
        .cloned()
        .unwrap_or_default();
    let start_node_id = fields[2].as_i64().unwrap_or(0);
    let end_node_id = fields[3].as_i64().unwrap_or(0);
    let properties = decode_compact_properties(&fields[4], schema)?;
    Ok(BoltRelationship {
        id,
        start_node_id,
        end_node_id,
        rel_type,
        properties,
        element_id: None,
        start_element_id: None,
        end_element_id: None,
    })
}

/// Decodes one compact FalkorDB `[scalar_type, value]` cell into a [`BoltValue`].
pub fn decode_compact_cell(cell: &RespValue, schema: &FalkorGraphSchema) -> Result<BoltValue> {
    let Some(pair) = cell.as_array() else {
        return Ok(resp_to_bolt_value(cell));
    };
    if pair.len() < 2 {
        return Ok(resp_to_bolt_value(cell));
    }
    let Some(scalar_type) = pair[0].as_i64() else {
        return Ok(resp_to_bolt_value(cell));
    };
    let val = &pair[1];
    match scalar_type {
        0 | 1 => Ok(BoltValue::Null),
        2 => Ok(BoltValue::String(val.to_string_lossy())),
        3 => Ok(BoltValue::Integer(val.as_i64().unwrap_or(0))),
        4 => Ok(BoltValue::Boolean(val.as_bool().unwrap_or(false))),
        5 => Ok(BoltValue::Float(val.as_f64().unwrap_or(0.0))),
        6 => {
            let mut out = Vec::new();
            if let Some(items) = val.as_array() {
                out.reserve(items.len());
                for item in items {
                    out.push(decode_compact_cell(item, schema)?);
                }
            }
            Ok(BoltValue::List(out))
        }
        7 => Ok(BoltValue::Relationship(decode_compact_edge(val, schema)?)),
        8 => Ok(BoltValue::Node(decode_compact_node(val, schema)?)),
        9 => {
            let parts = val.as_array().ok_or_else(|| {
                NetError::ProtocolError(format!(
                    "Expected compact FalkorDB path array, got {:?}",
                    val
                ))
            })?;
            if parts.len() < 2 {
                return Err(NetError::ProtocolError(
                    "Compact FalkorDB path array missing nodes or edges".to_string(),
                ));
            }
            let raw_nodes = match decode_compact_cell(&parts[0], schema)? {
                BoltValue::List(items) => items,
                _ => Vec::new(),
            };
            let raw_edges = match decode_compact_cell(&parts[1], schema)? {
                BoltValue::List(items) => items,
                _ => Vec::new(),
            };
            let mut nodes = Vec::with_capacity(raw_nodes.len());
            for n in raw_nodes {
                if let BoltValue::Node(node) = n {
                    nodes.push(node);
                }
            }
            let mut rels = Vec::with_capacity(raw_edges.len());
            let mut sequence = Vec::with_capacity(raw_edges.len() * 2);
            for (idx, edge_val) in raw_edges.into_iter().enumerate() {
                if let BoltValue::Relationship(rel) = edge_val {
                    let forward = nodes
                        .get(idx)
                        .zip(nodes.get(idx + 1))
                        .map(|(prev, next)| {
                            rel.start_node_id == prev.id && rel.end_node_id == next.id
                        })
                        .unwrap_or(true);
                    let rel_code = if forward {
                        (idx + 1) as i64
                    } else {
                        -((idx + 1) as i64)
                    };
                    sequence.push(rel_code);
                    sequence.push((idx + 1) as i64);
                    rels.push(BoltUnboundRelationship {
                        id: rel.id,
                        rel_type: rel.rel_type,
                        properties: rel.properties,
                        element_id: None,
                    });
                }
            }
            Ok(BoltValue::Path(BoltPath {
                nodes,
                relationships: rels,
                sequence,
            }))
        }
        10 => {
            let mut map = HashMap::new();
            if let Some(items) = val.as_array() {
                let mut i = 0;
                while i + 1 < items.len() {
                    let key = items[i].to_string_lossy();
                    let v = decode_compact_cell(&items[i + 1], schema)?;
                    map.insert(key, v);
                    i += 2;
                }
            }
            Ok(BoltValue::Map(map))
        }
        11 => {
            if let Some(coords) = val.as_array()
                && coords.len() >= 2
            {
                let lat = coords[0].as_f64().unwrap_or(0.0);
                let lon = coords[1].as_f64().unwrap_or(0.0);
                Ok(BoltValue::Point2D(BoltPoint2D {
                    srid: 4326,
                    x: lon,
                    y: lat,
                }))
            } else {
                Ok(resp_to_bolt_value(val))
            }
        }
        12 => {
            let mut floats = Vec::new();
            if let Some(items) = val.as_array() {
                for item in items {
                    if let Some(f) = item.as_f64() {
                        floats.push(BoltValue::Float(f));
                    }
                }
            }
            Ok(BoltValue::List(floats))
        }
        13 => {
            let secs = val.as_i64().ok_or_else(|| {
                NetError::ProtocolError(format!(
                    "Expected integer payload for FalkorDB DATETIME cell (tag 13), got {val:?}"
                ))
            })?;
            Ok(BoltValue::LocalDateTime(BoltLocalDateTime::new(secs, 0)))
        }
        14 => {
            let secs = val.as_i64().ok_or_else(|| {
                NetError::ProtocolError(format!(
                    "Expected integer payload for FalkorDB DATE cell (tag 14), got {val:?}"
                ))
            })?;
            Ok(BoltValue::Date(BoltDate::new(secs.div_euclid(86_400))))
        }
        15 => {
            let raw_secs = val.as_i64().ok_or_else(|| {
                NetError::ProtocolError(format!(
                    "Expected integer payload for FalkorDB TIME cell (tag 15), got {val:?}"
                ))
            })?;
            Ok(BoltValue::LocalTime(BoltLocalTime::new(
                raw_secs.rem_euclid(86_400).saturating_mul(1_000_000_000),
            )))
        }
        16 => {
            let total_secs = val.as_i64().ok_or_else(|| {
                NetError::ProtocolError(format!(
                    "Expected integer payload for FalkorDB DURATION cell (tag 16), got {val:?}"
                ))
            })?;
            // Euclidean split so negative totals match falkordb-py's timedelta
            // normalisation (-93600 s -> -2 days + 79200 s).
            let days = total_secs.div_euclid(86_400);
            let secs = total_secs.rem_euclid(86_400);
            Ok(BoltValue::Duration(BoltDuration::new(0, days, secs, 0)))
        }
        _ => Ok(resp_to_bolt_value(val)),
    }
}
