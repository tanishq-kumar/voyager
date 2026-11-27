//! Pure parsers from catalog result rows to schema entries.
//!
//! Each catalog query result is handled as a list of JSON objects (one per
//! row, keyed by column name), so the parsers can be tested without a
//! database. List columns may arrive as JSON arrays or, from FalkorDB verbose
//! replies, as `[a, b]` strings; [`string_list`] accepts both.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::bolt::BoltValue;

use super::{ConstraintEntry, ConstraintKind, EntityKind, IndexEntry, VectorInfo};

/// One catalog result row, keyed by column name.
pub type Row = Map<String, Value>;

/// Converts a Bolt value to plain JSON (temporal values become ISO strings).
pub fn bolt_to_json(value: &BoltValue) -> Value {
    match value {
        BoltValue::Null => Value::Null,
        BoltValue::Boolean(b) => Value::Bool(*b),
        BoltValue::Integer(i) => Value::from(*i),
        BoltValue::Float(f) => serde_json::Number::from_f64(*f)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        BoltValue::String(s) => Value::String(s.clone()),
        BoltValue::List(items) => Value::Array(items.iter().map(bolt_to_json).collect()),
        BoltValue::Map(map) => {
            let mut sorted: Vec<_> = map.iter().collect();
            sorted.sort_by(|left, right| left.0.cmp(right.0));
            Value::Object(
                sorted
                    .into_iter()
                    .map(|(key, value)| (key.clone(), bolt_to_json(value)))
                    .collect(),
            )
        }
        BoltValue::Date(d) => Value::String(d.to_iso_string()),
        BoltValue::Time(t) => Value::String(t.to_iso_string()),
        BoltValue::LocalTime(t) => Value::String(t.to_iso_string()),
        BoltValue::DateTime(t) => Value::String(t.to_iso_string()),
        BoltValue::LocalDateTime(t) => Value::String(t.to_iso_string()),
        BoltValue::DateTimeZoneId(t) => Value::String(t.to_iso_string()),
        BoltValue::Duration(d) => Value::String(d.to_iso_string()),
        other => Value::String(format!("{other:?}")),
    }
}

/// Builds rows from column names and Bolt records.
pub fn rows_from_bolt(columns: &[String], records: &[Vec<BoltValue>]) -> Vec<Row> {
    records
        .iter()
        .map(|record| {
            columns
                .iter()
                .zip(record.iter())
                .map(|(column, value)| (column.clone(), bolt_to_json(value)))
                .collect()
        })
        .collect()
}

/// Returns a string column, or `None` for a missing, null or empty value.
pub fn string(row: &Row, column: &str) -> Option<String> {
    match row.get(column)? {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Returns the first column of a row as a string.
pub fn first_string(row: &Row) -> Option<String> {
    row.values().next().and_then(|value| match value {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    })
}

/// Converts a JSON value to a list of strings. Accepts a JSON array, a single
/// string, or a `[a, b]` string (FalkorDB verbose replies).
fn value_to_string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| match item {
                Value::String(s) => Some(s.clone()),
                Value::Null => None,
                other => Some(other.to_string()),
            })
            .collect(),
        Some(Value::String(s)) => {
            let trimmed = s.trim();
            if let Some(inner) = trimmed
                .strip_prefix('[')
                .and_then(|rest| rest.strip_suffix(']'))
            {
                inner
                    .split(',')
                    .map(|item| item.trim().trim_matches(|c| c == '"' || c == '\''))
                    .filter(|item| !item.is_empty())
                    .map(str::to_string)
                    .collect()
            } else if trimmed.is_empty() {
                Vec::new()
            } else {
                vec![trimmed.to_string()]
            }
        }
        _ => Vec::new(),
    }
}

/// Returns a list-of-strings column. Accepts a JSON array, a single string,
/// or a `[a, b]` string (FalkorDB verbose replies).
pub fn string_list(row: &Row, column: &str) -> Vec<String> {
    value_to_string_list(row.get(column))
}

/// Normalises an engine value type name to the type names of a model export.
///
/// Neo4j `valueType()` (`STRING NOT NULL`, `LIST<INTEGER NOT NULL> NOT NULL`,
/// `ZONED DATETIME NOT NULL`), Memgraph `valueType()` and data type
/// constraints, FalkorDB `typeOf()` (`Integer`, `String`) and Neo4j property
/// type constraints all map to `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`,
/// `DATE`, `DATETIME`, `TIME`, `DURATION`, `LIST`, `MAP`, `POINT`, ... A union
/// type (`STRING | INTEGER`) becomes `ANY`. `NULL` gives `None`.
pub fn normalize_value_type(raw: &str) -> Option<String> {
    let mut upper = raw.trim().to_ascii_uppercase();
    if let Some(stripped) = upper.strip_suffix(" NOT NULL") {
        upper = stripped.trim_end().to_string();
    }
    if upper.is_empty() || upper == "NULL" || upper == "NOTHING" {
        return None;
    }
    if upper.starts_with("LIST") || upper.starts_with("ARRAY") {
        return Some("LIST".to_string());
    }
    if upper.contains(" | ") {
        return Some("ANY".to_string());
    }
    let compact: String = upper.chars().filter(|c| !matches!(c, ' ' | '_')).collect();
    let normalised = match compact.as_str() {
        "STRING" | "STR" | "TEXT" => "STRING",
        "INTEGER" | "INT" | "INT64" => "INTEGER",
        "FLOAT" | "DOUBLE" | "FLOAT64" => "FLOAT",
        "BOOLEAN" | "BOOL" => "BOOLEAN",
        "DATE" => "DATE",
        "DATETIME" | "ZONEDDATETIME" | "LOCALDATETIME" => "DATETIME",
        "TIME" | "ZONEDTIME" | "LOCALTIME" => "TIME",
        "DURATION" => "DURATION",
        "MAP" => "MAP",
        "POINT" => "POINT",
        "ANY" | "PROPERTYVALUE" => "ANY",
        _ => return Some(upper),
    };
    Some(normalised.to_string())
}

/// Normalises an engine index type name (`RANGE` becomes `BTREE`).
pub fn normalize_index_type(raw: &str) -> String {
    let upper = raw.trim().to_ascii_uppercase();
    match upper.as_str() {
        "RANGE" | "BTREE" | "DEFAULT" | "EXACT-MATCH" => "BTREE".to_string(),
        _ => upper,
    }
}

fn entity_kind(raw: Option<&str>) -> EntityKind {
    match raw.map(str::to_ascii_uppercase).as_deref() {
        Some("RELATIONSHIP") | Some("EDGE") => EntityKind::Relationship,
        _ => EntityKind::Node,
    }
}

/// Parses Neo4j `SHOW CONSTRAINTS YIELD *` rows.
pub fn neo4j_constraints(rows: &[Row]) -> Vec<ConstraintEntry> {
    rows.iter()
        .filter_map(|row| {
            let engine_type = string(row, "type")?;
            let upper = engine_type.to_ascii_uppercase();
            let kind = if upper.contains("UNIQUENESS") {
                ConstraintKind::Unique
            } else if upper.contains("EXISTENCE") {
                ConstraintKind::Exists
            } else if upper.contains("KEY") {
                ConstraintKind::Key
            } else if upper.contains("PROPERTY_TYPE") {
                ConstraintKind::Type
            } else {
                return None;
            };
            let label_or_type = string_list(row, "labelsOrTypes").into_iter().next()?;
            Some(ConstraintEntry {
                entity: entity_kind(string(row, "entityType").as_deref()),
                label_or_type,
                properties: string_list(row, "properties"),
                kind,
                property_type: string(row, "propertyType")
                    .as_deref()
                    .and_then(normalize_value_type),
                name: string(row, "name"),
                engine_type,
            })
        })
        .collect()
}

/// Parses Neo4j `SHOW INDEXES YIELD *` rows.
pub fn neo4j_indexes(rows: &[Row]) -> Vec<IndexEntry> {
    rows.iter()
        .filter_map(|row| {
            let engine_type = string(row, "type")?;
            let options = match row.get("options") {
                Some(Value::Object(map)) if engine_type.eq_ignore_ascii_case("VECTOR") => {
                    Some(Value::Object(map.clone()))
                }
                _ => None,
            };
            Some(IndexEntry {
                entity: entity_kind(string(row, "entityType").as_deref()),
                labels_or_types: string_list(row, "labelsOrTypes"),
                properties: string_list(row, "properties"),
                index_type: normalize_index_type(&engine_type),
                name: string(row, "name"),
                owning_constraint: string(row, "owningConstraint"),
                engine_type,
                options,
            })
        })
        .collect()
}

/// Parses Memgraph `SHOW CONSTRAINT INFO` rows.
pub fn memgraph_constraints(rows: &[Row]) -> Vec<ConstraintEntry> {
    rows.iter()
        .filter_map(|row| {
            let engine_type = string(row, "constraint type")?;
            let kind = match engine_type.to_ascii_lowercase().as_str() {
                "unique" => ConstraintKind::Unique,
                "exists" => ConstraintKind::Exists,
                "data_type" | "type" => ConstraintKind::Type,
                _ => return None,
            };
            let property_type = if kind == ConstraintKind::Type {
                string(row, "data_type")
                    .as_deref()
                    .and_then(normalize_value_type)
            } else {
                None
            };
            Some(ConstraintEntry {
                entity: EntityKind::Node,
                label_or_type: string(row, "label")?,
                properties: string_list(row, "properties"),
                kind,
                property_type,
                name: None,
                engine_type,
            })
        })
        .collect()
}

/// Parses Memgraph `SHOW INDEX INFO` rows.
///
/// Index types: `label`, `label+property`, `edge-type`, `edge-type+property`,
/// `edge-property`, `point`, `text`, `vector` and their edge variants.
pub fn memgraph_indexes(rows: &[Row]) -> Vec<IndexEntry> {
    memgraph_indexes_with_types(rows, &BTreeSet::new(), &BTreeSet::new())
}

/// Parses Memgraph `SHOW INDEX INFO` rows, classifying rows whose `label`
/// matches a discovered relationship type (and not a discovered node label) as
/// relationship indexes.
///
/// Memgraph 3.x reports `CREATE INDEX ON :REL(prop)` with
/// `index type = 'label+property'` and no entity column; matching against the
/// discovered node labels and relationship types keeps those indexes under the
/// relationship type without creating a phantom node label.
pub fn memgraph_indexes_with_types(
    rows: &[Row],
    labels: &BTreeSet<String>,
    rel_types: &BTreeSet<String>,
) -> Vec<IndexEntry> {
    rows.iter()
        .filter_map(|row| {
            let engine_type = string(row, "index type")?;
            let lower = engine_type.to_ascii_lowercase();
            let label = string(row, "label");
            let entity = if lower.contains("edge")
                || label
                    .as_deref()
                    .is_some_and(|name| !labels.contains(name) && rel_types.contains(name))
            {
                EntityKind::Relationship
            } else {
                EntityKind::Node
            };
            let properties = string_list(row, "property");
            let index_type = if lower.contains("vector") {
                "VECTOR"
            } else if lower.contains("point") {
                "POINT"
            } else if lower.contains("text") {
                "TEXT"
            } else if properties.is_empty() {
                "LABEL"
            } else {
                "BTREE"
            };
            let labels_or_types = if lower == "edge-property" {
                Vec::new()
            } else {
                label.into_iter().collect()
            };
            Some(IndexEntry {
                entity,
                labels_or_types,
                properties,
                index_type: index_type.to_string(),
                name: None,
                owning_constraint: None,
                engine_type,
                options: None,
            })
        })
        .collect()
}

fn falkordb_single_index(
    entity: EntityKind,
    label: &str,
    property: String,
    engine_type: String,
    constraints: &[ConstraintEntry],
) -> IndexEntry {
    let index_type = normalize_index_type(&engine_type);
    let owning_constraint = if index_type == "BTREE" {
        constraints
            .iter()
            .find(|c| {
                c.entity == entity
                    && c.label_or_type == label
                    && c.kind == ConstraintKind::Unique
                    && c.properties == [property.clone()]
            })
            .map(|c| c.name.clone().unwrap_or_else(|| c.engine_type.clone()))
    } else {
        None
    };
    IndexEntry {
        entity,
        labels_or_types: vec![label.to_string()],
        properties: vec![property],
        index_type,
        name: None,
        owning_constraint,
        engine_type,
        options: None,
    }
}

/// Parses FalkorDB `types` column, accepting either a JSON object or FalkorDB's
/// verbose map string (`{slug: [RANGE], title: [RANGE, FULLTEXT]}`).
fn falkordb_types_map(value: Option<&Value>) -> Option<BTreeMap<String, Vec<String>>> {
    match value? {
        Value::Object(map) => Some(
            map.iter()
                .map(|(k, v)| (k.clone(), value_to_string_list(Some(v))))
                .collect(),
        ),
        Value::String(s) => {
            let inner = s.trim().strip_prefix('{')?.strip_suffix('}')?.trim();
            let mut map = BTreeMap::new();
            if inner.is_empty() {
                return Some(map);
            }
            let mut depth = 0usize;
            let mut start = 0usize;
            let mut parts = Vec::new();
            for (idx, ch) in inner.char_indices() {
                match ch {
                    '[' | '{' => depth += 1,
                    ']' | '}' => depth = depth.saturating_sub(1),
                    ',' if depth == 0 => {
                        parts.push(&inner[start..idx]);
                        start = idx + 1;
                    }
                    _ => {}
                }
            }
            parts.push(&inner[start..]);
            for part in parts {
                let Some((raw_key, raw_val)) = part.split_once(':') else {
                    continue;
                };
                let key = raw_key
                    .trim()
                    .trim_matches(|c| c == '"' || c == '\'')
                    .to_string();
                if key.is_empty() {
                    continue;
                }
                let kinds = value_to_string_list(Some(&Value::String(raw_val.trim().to_string())));
                map.insert(key, kinds);
            }
            Some(map)
        }
        _ => None,
    }
}

/// Parses FalkorDB `db.indexes()` rows.
///
/// Accepts either raw `db.indexes()` rows (`label`, `properties`, `types`,
/// `entitytype`) or pre-unwound rows (`label`, `property`, `kind`,
/// `entitytype`). `FULLTEXT` properties on the same `(entity, label)` are
/// grouped into a single `IndexEntry` with their property list, while
/// single-property `RANGE` (`BTREE`) and `VECTOR` indexes stay per-property so
/// property-level `indexed` and `owning_constraint` flags remain accurate.
pub fn falkordb_indexes(rows: &[Row], constraints: &[ConstraintEntry]) -> Vec<IndexEntry> {
    let mut entries = Vec::new();
    let mut unwound_fulltext: BTreeMap<(EntityKind, String), Vec<String>> = BTreeMap::new();

    for row in rows {
        let Some(label) = string(row, "label") else {
            continue;
        };
        let entity = entity_kind(string(row, "entitytype").as_deref());

        if let Some(types_map) = falkordb_types_map(row.get("types")) {
            let properties = string_list(row, "properties");
            let mut fulltext_properties = Vec::new();
            for property in properties {
                for kind in types_map.get(&property).into_iter().flatten() {
                    if kind.eq_ignore_ascii_case("FULLTEXT") {
                        if !fulltext_properties.contains(&property) {
                            fulltext_properties.push(property.clone());
                        }
                    } else {
                        entries.push(falkordb_single_index(
                            entity,
                            &label,
                            property.clone(),
                            kind.clone(),
                            constraints,
                        ));
                    }
                }
            }
            if !fulltext_properties.is_empty() {
                entries.push(IndexEntry {
                    entity,
                    labels_or_types: vec![label],
                    properties: fulltext_properties,
                    index_type: "FULLTEXT".to_string(),
                    name: None,
                    owning_constraint: None,
                    engine_type: "FULLTEXT".to_string(),
                    options: None,
                });
            }
            continue;
        }

        let (Some(property), Some(kind)) = (string(row, "property"), string(row, "kind")) else {
            continue;
        };
        if kind.eq_ignore_ascii_case("FULLTEXT") {
            let list = unwound_fulltext.entry((entity, label)).or_default();
            if !list.contains(&property) {
                list.push(property);
            }
        } else {
            entries.push(falkordb_single_index(
                entity,
                &label,
                property,
                kind,
                constraints,
            ));
        }
    }

    for ((entity, label), properties) in unwound_fulltext {
        entries.push(IndexEntry {
            entity,
            labels_or_types: vec![label],
            properties,
            index_type: "FULLTEXT".to_string(),
            name: None,
            owning_constraint: None,
            engine_type: "FULLTEXT".to_string(),
            options: None,
        });
    }

    entries
}

/// Parses FalkorDB `db.constraints()` rows (`type` is `UNIQUE` or `MANDATORY`).
pub fn falkordb_constraints(rows: &[Row]) -> Vec<ConstraintEntry> {
    rows.iter()
        .filter_map(|row| {
            let engine_type = string(row, "type")?;
            let kind = match engine_type.to_ascii_uppercase().as_str() {
                "UNIQUE" => ConstraintKind::Unique,
                "MANDATORY" | "EXISTS" => ConstraintKind::Exists,
                _ => return None,
            };
            Some(ConstraintEntry {
                entity: entity_kind(string(row, "entitytype").as_deref()),
                label_or_type: string(row, "label")?,
                properties: string_list(row, "properties"),
                kind,
                property_type: None,
                name: None,
                engine_type,
            })
        })
        .collect()
}

/// Reads vector settings from a vector index's options.
///
/// Neo4j reports `options.indexConfig` with `vector.dimensions` and
/// `vector.similarity_function`; other engines report only the name.
pub fn vector_info(index: &IndexEntry) -> VectorInfo {
    let config = index
        .options
        .as_ref()
        .and_then(|options| options.get("indexConfig"));
    let dimensions = config
        .and_then(|config| config.get("vector.dimensions"))
        .and_then(Value::as_i64);
    let similarity = config
        .and_then(|config| config.get("vector.similarity_function"))
        .and_then(Value::as_str)
        .map(|raw| {
            voyager_ogm::schema::VectorSimilarity::parse_str(raw)
                .map(|similarity| similarity.as_str().to_string())
                .unwrap_or_else(|| raw.to_ascii_lowercase())
        });
    VectorInfo {
        dimensions,
        similarity,
        index_name: index.name.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows(value: Value) -> Vec<Row> {
        value
            .as_array()
            .expect("array")
            .iter()
            .map(|row| row.as_object().expect("object").clone())
            .collect()
    }

    #[test]
    fn value_types_normalise_across_engines() {
        let cases = [
            ("STRING NOT NULL", Some("STRING")),
            ("INTEGER NOT NULL", Some("INTEGER")),
            ("LIST<STRING NOT NULL> NOT NULL", Some("LIST")),
            ("ZONED DATETIME NOT NULL", Some("DATETIME")),
            ("LOCAL DATETIME NOT NULL", Some("DATETIME")),
            ("LOCAL_DATE_TIME", Some("DATETIME")),
            ("Integer", Some("INTEGER")),
            ("String", Some("STRING")),
            ("Boolean", Some("BOOLEAN")),
            ("List", Some("LIST")),
            ("STRING | INTEGER", Some("ANY")),
            ("NULL", None),
            ("", None),
            ("POINT NOT NULL", Some("POINT")),
            ("Vertex", Some("VERTEX")),
        ];
        for (raw, expected) in cases {
            assert_eq!(normalize_value_type(raw).as_deref(), expected, "{raw}");
        }
    }

    #[test]
    fn string_lists_accept_arrays_and_falkordb_strings() {
        let row = rows(json!([{"a": ["x", "y"], "b": "[x, y]", "c": "x", "d": null, "e": "[]"}]))
            .remove(0);
        assert_eq!(string_list(&row, "a"), ["x", "y"]);
        assert_eq!(string_list(&row, "b"), ["x", "y"]);
        assert_eq!(string_list(&row, "c"), ["x"]);
        assert!(string_list(&row, "d").is_empty());
        assert!(string_list(&row, "e").is_empty());
        assert!(string_list(&row, "missing").is_empty());
    }

    #[test]
    fn neo4j_constraint_rows_parse() {
        let parsed = neo4j_constraints(&rows(json!([
            {"name": "c1", "type": "UNIQUENESS", "entityType": "NODE",
             "labelsOrTypes": ["Person"], "properties": ["email"], "propertyType": null},
            {"name": "c2", "type": "NODE_PROPERTY_EXISTENCE", "entityType": "NODE",
             "labelsOrTypes": ["Person"], "properties": ["name"], "propertyType": null},
            {"name": "c3", "type": "NODE_KEY", "entityType": "NODE",
             "labelsOrTypes": ["Person"], "properties": ["id"], "propertyType": null},
            {"name": "c4", "type": "RELATIONSHIP_PROPERTY_TYPE", "entityType": "RELATIONSHIP",
             "labelsOrTypes": ["KNOWS"], "properties": ["since"], "propertyType": "INTEGER"},
            {"name": "c5", "type": "RELATIONSHIP_UNIQUENESS", "entityType": "RELATIONSHIP",
             "labelsOrTypes": ["KNOWS"], "properties": ["id"]}
        ])));
        let kinds: Vec<_> = parsed.iter().map(|c| c.kind).collect();
        assert_eq!(
            kinds,
            [
                ConstraintKind::Unique,
                ConstraintKind::Exists,
                ConstraintKind::Key,
                ConstraintKind::Type,
                ConstraintKind::Unique
            ]
        );
        assert_eq!(parsed[3].entity, EntityKind::Relationship);
        assert_eq!(parsed[3].property_type.as_deref(), Some("INTEGER"));
        assert_eq!(parsed[0].name.as_deref(), Some("c1"));
        assert_eq!(parsed[0].properties, ["email"]);
    }

    #[test]
    fn neo4j_index_rows_parse() {
        let parsed = neo4j_indexes(&rows(json!([
            {"name": "c1", "type": "RANGE", "entityType": "NODE", "labelsOrTypes": ["Person"],
             "properties": ["email"], "owningConstraint": "c1", "options": {}},
            {"name": "idx", "type": "RANGE", "entityType": "RELATIONSHIP",
             "labelsOrTypes": ["KNOWS"], "properties": ["since"], "owningConstraint": null},
            {"name": "lookup", "type": "LOOKUP", "entityType": "NODE", "labelsOrTypes": null,
             "properties": null, "owningConstraint": null},
            {"name": "vec", "type": "VECTOR", "entityType": "NODE", "labelsOrTypes": ["Doc"],
             "properties": ["embedding"], "owningConstraint": null,
             "options": {"indexConfig": {"vector.dimensions": 3,
                                         "vector.similarity_function": "COSINE"}}}
        ])));
        assert_eq!(parsed[0].index_type, "BTREE");
        assert_eq!(parsed[0].owning_constraint.as_deref(), Some("c1"));
        assert!(parsed[0].options.is_none());
        assert_eq!(parsed[1].entity, EntityKind::Relationship);
        assert_eq!(parsed[2].index_type, "LOOKUP");
        assert!(parsed[2].labels_or_types.is_empty());
        let vector = vector_info(&parsed[3]);
        assert_eq!(vector.dimensions, Some(3));
        assert_eq!(vector.similarity.as_deref(), Some("cosine"));
        assert_eq!(vector.index_name.as_deref(), Some("vec"));
    }

    #[test]
    fn memgraph_rows_parse() {
        let constraints = memgraph_constraints(&rows(json!([
            {"constraint type": "exists", "label": "P", "properties": "b", "data_type": ""},
            {"constraint type": "unique", "label": "P", "properties": ["a"], "data_type": ""},
            {"constraint type": "unique", "label": "P", "properties": ["a", "b"], "data_type": ""},
            {"constraint type": "data_type", "label": "P", "properties": "c",
             "data_type": "INTEGER"}
        ])));
        assert_eq!(constraints[0].kind, ConstraintKind::Exists);
        assert_eq!(constraints[0].properties, ["b"]);
        assert_eq!(constraints[2].properties, ["a", "b"]);
        assert_eq!(constraints[3].property_type.as_deref(), Some("INTEGER"));

        let indexes = memgraph_indexes(&rows(json!([
            {"index type": "edge-type+property", "label": "R", "property": "w", "count": 0},
            {"index type": "label", "label": "P", "property": null, "count": 0},
            {"index type": "label+property", "label": "P", "property": ["c"], "count": 0},
            {"index type": "point", "label": "P", "property": ["loc"], "count": 0},
            {"index type": "edge-property", "label": null, "property": "w", "count": 0}
        ])));
        assert_eq!(indexes[0].entity, EntityKind::Relationship);
        assert_eq!(indexes[0].index_type, "BTREE");
        assert_eq!(indexes[0].properties, ["w"]);
        assert_eq!(indexes[1].index_type, "LABEL");
        assert_eq!(indexes[2].properties, ["c"]);
        assert_eq!(indexes[3].index_type, "POINT");
        assert!(indexes[4].labels_or_types.is_empty());

        let labels = BTreeSet::from(["P".to_string()]);
        let rel_types = BTreeSet::from(["ACTED_IN".to_string(), "R".to_string()]);
        let classified = memgraph_indexes_with_types(
            &rows(json!([
                {"index type": "edge-type", "label": "R", "property": null, "count": 1},
                {"index type": "edge-type+property", "label": "R", "property": "w", "count": 1},
                {"index type": "label+property", "label": "ACTED_IN", "property": ["billing"], "count": 0},
                {"index type": "label+property", "label": "P", "property": ["c"], "count": 1}
            ])),
            &labels,
            &rel_types,
        );
        assert_eq!(classified[0].entity, EntityKind::Relationship);
        assert_eq!(classified[0].index_type, "LABEL");
        assert_eq!(classified[1].entity, EntityKind::Relationship);
        assert_eq!(classified[1].index_type, "BTREE");
        assert_eq!(classified[2].entity, EntityKind::Relationship);
        assert_eq!(classified[2].labels_or_types, ["ACTED_IN"]);
        assert_eq!(classified[2].properties, ["billing"]);
        assert_eq!(classified[3].entity, EntityKind::Node);
        assert_eq!(classified[3].labels_or_types, ["P"]);
    }

    #[test]
    fn falkordb_rows_parse() {
        let constraints = falkordb_constraints(&rows(json!([
            {"type": "UNIQUE", "label": "P", "properties": "[email]", "entitytype": "NODE"},
            {"type": "MANDATORY", "label": "P", "properties": ["name"], "entitytype": "NODE"}
        ])));
        assert_eq!(constraints[0].kind, ConstraintKind::Unique);
        assert_eq!(constraints[0].properties, ["email"]);
        assert_eq!(constraints[1].kind, ConstraintKind::Exists);

        let indexes = falkordb_indexes(
            &rows(json!([
                {
                    "label": "Movie",
                    "properties": ["slug", "title", "overview"],
                    "types": {
                        "slug": ["RANGE"],
                        "title": ["RANGE", "FULLTEXT"],
                        "overview": ["FULLTEXT"]
                    },
                    "entitytype": "NODE"
                },
                {
                    "label": "P",
                    "properties": ["email", "name", "bio", "vec"],
                    "types": {
                        "email": ["RANGE"],
                        "name": ["RANGE"],
                        "bio": ["FULLTEXT"],
                        "vec": ["VECTOR"]
                    },
                    "entitytype": "NODE"
                },
                {
                    "label": "R",
                    "properties": ["w", "note", "tag"],
                    "types": {
                        "w": ["RANGE"],
                        "note": ["FULLTEXT"],
                        "tag": ["FULLTEXT"]
                    },
                    "entitytype": "RELATIONSHIP"
                }
            ])),
            &constraints,
        );
        // Movie: 2 range indexes + 1 grouped fulltext index
        assert_eq!(indexes[0].labels_or_types, ["Movie"]);
        assert_eq!(indexes[0].properties, ["slug"]);
        assert_eq!(indexes[0].index_type, "BTREE");
        assert_eq!(indexes[1].properties, ["title"]);
        assert_eq!(indexes[1].index_type, "BTREE");
        assert_eq!(indexes[2].labels_or_types, ["Movie"]);
        assert_eq!(indexes[2].properties, ["title", "overview"]);
        assert_eq!(indexes[2].index_type, "FULLTEXT");
        // P: email (backs UNIQUE), name (BTREE), vec (VECTOR), bio (single FULLTEXT)
        assert_eq!(indexes[3].properties, ["email"]);
        assert_eq!(indexes[3].owning_constraint.as_deref(), Some("UNIQUE"));
        assert_eq!(indexes[4].properties, ["name"]);
        assert!(indexes[4].owning_constraint.is_none());
        assert_eq!(indexes[5].properties, ["vec"]);
        assert_eq!(indexes[5].index_type, "VECTOR");
        assert_eq!(indexes[6].properties, ["bio"]);
        assert_eq!(indexes[6].index_type, "FULLTEXT");
        // R: w (BTREE) + grouped relationship fulltext (note, tag)
        assert_eq!(indexes[7].entity, EntityKind::Relationship);
        assert_eq!(indexes[7].properties, ["w"]);
        assert_eq!(indexes[8].entity, EntityKind::Relationship);
        assert_eq!(indexes[8].properties, ["note", "tag"]);
        assert_eq!(indexes[8].index_type, "FULLTEXT");

        // Legacy unwound rows also group FULLTEXT per (entity, label)
        let legacy = falkordb_indexes(
            &rows(json!([
                {"label": "P", "property": "email", "kind": "RANGE", "entitytype": "NODE"},
                {"label": "P", "property": "bio", "kind": "FULLTEXT", "entitytype": "NODE"},
                {"label": "P", "property": "title", "kind": "FULLTEXT", "entitytype": "NODE"}
            ])),
            &constraints,
        );
        assert_eq!(legacy.len(), 2);
        assert_eq!(legacy[0].owning_constraint.as_deref(), Some("UNIQUE"));
        assert_eq!(legacy[1].index_type, "FULLTEXT");
        assert_eq!(legacy[1].properties, ["bio", "title"]);

        // Verbose non-compact FalkorDB reply strings
        let verbose = falkordb_indexes(
            &rows(json!([
                {
                    "label": "Movie",
                    "properties": "[slug, title, released, overview]",
                    "types": "{slug: [RANGE], title: [RANGE, FULLTEXT], released: [RANGE], overview: [FULLTEXT]}",
                    "entitytype": "NODE"
                }
            ])),
            &constraints,
        );
        assert_eq!(verbose.len(), 4);
        assert_eq!(verbose[0].properties, ["slug"]);
        assert_eq!(verbose[0].index_type, "BTREE");
        assert_eq!(verbose[1].properties, ["title"]);
        assert_eq!(verbose[1].index_type, "BTREE");
        assert_eq!(verbose[2].properties, ["released"]);
        assert_eq!(verbose[2].index_type, "BTREE");
        assert_eq!(verbose[3].properties, ["title", "overview"]);
        assert_eq!(verbose[3].index_type, "FULLTEXT");
    }

    #[test]
    fn bolt_values_convert_to_plain_json() {
        let mut map = std::collections::HashMap::new();
        map.insert("b".to_string(), BoltValue::Integer(2));
        map.insert("a".to_string(), BoltValue::List(vec![BoltValue::from("x")]));
        let value = bolt_to_json(&BoltValue::Map(map));
        assert_eq!(value, json!({"a": ["x"], "b": 2}));
        assert_eq!(
            serde_json::to_string(&value).expect("json"),
            r#"{"a":["x"],"b":2}"#
        );
    }
}
