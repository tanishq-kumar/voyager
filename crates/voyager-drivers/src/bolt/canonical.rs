//! Canonical JSON representation and Arrow RecordBatch construction for Bolt/graph values.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use crate::bolt::packstream::{BoltNode, BoltPath, BoltRelationship, BoltValue};
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

/// Converts tabular [`BoltValue`] records into an Apache Arrow [`RecordBatch`].
///
/// Columns containing graph entities, lists, or maps are JSON-encoded using
/// [`bolt_value_to_canonical_json`] and annotated with Arrow field metadata
/// (`voyager.encoding = "json"` and, for homogeneous entity columns,
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

    for (col_idx, col_name) in columns.iter().enumerate() {
        let mut int_count = 0usize;
        let mut float_count = 0usize;
        let mut bool_count = 0usize;
        let mut node_count = 0usize;
        let mut rel_count = 0usize;
        let mut path_count = 0usize;
        let mut has_structured = false;
        let mut non_null_count = 0usize;

        for row in records {
            match row.get(col_idx) {
                Some(BoltValue::Integer(_)) => {
                    int_count += 1;
                    non_null_count += 1;
                }
                Some(BoltValue::Float(_)) => {
                    float_count += 1;
                    non_null_count += 1;
                }
                Some(BoltValue::Boolean(_)) => {
                    bool_count += 1;
                    non_null_count += 1;
                }
                Some(BoltValue::Node(_)) => {
                    node_count += 1;
                    has_structured = true;
                    non_null_count += 1;
                }
                Some(BoltValue::Relationship(_)) => {
                    rel_count += 1;
                    has_structured = true;
                    non_null_count += 1;
                }
                Some(BoltValue::Path(_)) => {
                    path_count += 1;
                    has_structured = true;
                    non_null_count += 1;
                }
                Some(
                    BoltValue::List(_)
                    | BoltValue::Map(_)
                    | BoltValue::Point2D(_)
                    | BoltValue::Point3D(_)
                    | BoltValue::Bytes(_)
                    | BoltValue::Structure { .. },
                ) => {
                    has_structured = true;
                    non_null_count += 1;
                }
                Some(BoltValue::Null) | None => {}
                Some(_) => {
                    non_null_count += 1;
                }
            }
        }

        if non_null_count > 0 && int_count == non_null_count {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for row in records {
                match row.get(col_idx) {
                    Some(BoltValue::Integer(i)) => builder.append_value(*i),
                    _ => builder.append_null(),
                }
            }
            fields.push(Field::new(col_name, DataType::Int64, true));
            arrow_arrays.push(Arc::new(builder.finish()));
        } else if non_null_count > 0 && float_count == non_null_count {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for row in records {
                match row.get(col_idx) {
                    Some(BoltValue::Float(f)) => builder.append_value(*f),
                    _ => builder.append_null(),
                }
            }
            fields.push(Field::new(col_name, DataType::Float64, true));
            arrow_arrays.push(Arc::new(builder.finish()));
        } else if non_null_count > 0
            && (int_count + float_count) == non_null_count
            && int_count > 0
            && float_count > 0
        {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for row in records {
                match row.get(col_idx) {
                    Some(BoltValue::Float(f)) => builder.append_value(*f),
                    Some(BoltValue::Integer(i)) => builder.append_value(*i as f64),
                    _ => builder.append_null(),
                }
            }
            fields.push(Field::new(col_name, DataType::Float64, true));
            arrow_arrays.push(Arc::new(builder.finish()));
        } else if non_null_count > 0 && bool_count == non_null_count {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for row in records {
                match row.get(col_idx) {
                    Some(BoltValue::Boolean(b)) => builder.append_value(*b),
                    _ => builder.append_null(),
                }
            }
            fields.push(Field::new(col_name, DataType::Boolean, true));
            arrow_arrays.push(Arc::new(builder.finish()));
        } else if has_structured {
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
            if non_null_count > 0 && node_count == non_null_count {
                metadata.insert("voyager.kind".to_string(), "node".to_string());
            } else if non_null_count > 0 && rel_count == non_null_count {
                metadata.insert("voyager.kind".to_string(), "relationship".to_string());
            } else if non_null_count > 0 && path_count == non_null_count {
                metadata.insert("voyager.kind".to_string(), "path".to_string());
            }
            fields.push(Field::new(col_name, DataType::Utf8, true).with_metadata(metadata));
            arrow_arrays.push(Arc::new(builder.finish()));
        } else {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for row in records {
                match row.get(col_idx) {
                    Some(BoltValue::String(s)) => builder.append_value(s),
                    Some(BoltValue::Integer(i)) => builder.append_value(i.to_string()),
                    Some(BoltValue::Float(f)) => builder.append_value(f.to_string()),
                    Some(BoltValue::Boolean(b)) => builder.append_value(b.to_string()),
                    Some(BoltValue::Date(d)) => builder.append_value(d.to_iso_string()),
                    Some(BoltValue::Time(t)) => builder.append_value(t.to_iso_string()),
                    Some(BoltValue::LocalTime(lt)) => builder.append_value(lt.to_iso_string()),
                    Some(BoltValue::DateTime(dt)) => builder.append_value(dt.to_iso_string()),
                    Some(BoltValue::LocalDateTime(ldt)) => {
                        builder.append_value(ldt.to_iso_string())
                    }
                    Some(BoltValue::DateTimeZoneId(dtz)) => {
                        builder.append_value(dtz.to_iso_string())
                    }
                    Some(BoltValue::Duration(dur)) => builder.append_value(dur.to_iso_string()),
                    Some(BoltValue::Null) | None => builder.append_null(),
                    Some(other) => {
                        let json_str = serde_json::to_string(&bolt_value_to_canonical_json(other))
                            .unwrap_or_else(|_| "null".to_string());
                        builder.append_value(&json_str);
                    }
                }
            }
            fields.push(Field::new(col_name, DataType::Utf8, true));
            arrow_arrays.push(Arc::new(builder.finish()));
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrow_arrays).map_err(NetError::ArrowError)
}
