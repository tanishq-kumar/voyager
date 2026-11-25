//! Introspection of PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds) property graphs.
//!
//! Labels, properties and their declared types, and edge endpoints come from
//! the `pg_propgraph_*` catalogs. Constraint and index facts come from the
//! element tables: a property whose expression is a plain column takes that
//! column's `PRIMARY KEY`, `UNIQUE`, `NOT NULL` and single-column indexes.
//! Nothing is sampled. A label shared by several element tables gets no
//! constraint or index facts, because a table constraint does not hold across
//! tables.

use std::collections::{BTreeMap, HashMap};

use arrow_array::{Array, StringArray};
use serde_json::Value;

use crate::engine::AsyncConnection;
use crate::error::{NetError, Result};
use crate::postgres::PostgresConnection;
use crate::uri::ParsedUri;

use super::parse::{self, Row};
use super::{
    ConstraintEntry, ConstraintKind, EntityKind, GraphSchema, IndexEntry, IntrospectOptions,
    SchemaBuilder, SourceInfo, unsupported,
};

/// Quotes a string as a SQL literal.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Maps a `format_type` name to a model export type name.
pub(crate) fn sql_type_name(sql_type: &str) -> String {
    let lower = sql_type.trim().to_ascii_lowercase();
    if lower.ends_with("[]") {
        return "LIST".to_string();
    }
    let base = lower.split('(').next().unwrap_or(&lower).trim();
    match base {
        "text" | "character varying" | "varchar" | "character" | "char" | "name" | "uuid"
        | "citext" => "STRING",
        "bigint" | "integer" | "smallint" | "int" | "int2" | "int4" | "int8" => "INTEGER",
        "double precision" | "real" | "numeric" | "decimal" | "float4" | "float8" => "FLOAT",
        "boolean" | "bool" => "BOOLEAN",
        "date" => "DATE",
        "timestamp with time zone"
        | "timestamp without time zone"
        | "timestamp"
        | "timestamptz" => "DATETIME",
        "time with time zone" | "time without time zone" | "time" | "timetz" => "TIME",
        "interval" => "DURATION",
        "json" | "jsonb" => "MAP",
        "point" => "POINT",
        "vector" => "LIST",
        _ => return sql_type.trim().to_ascii_uppercase(),
    }
    .to_string()
}

/// Maps an index access method to an index kind.
pub(crate) fn index_kind(method: &str) -> String {
    match method.to_ascii_lowercase().as_str() {
        "btree" => "BTREE".to_string(),
        "hnsw" | "ivfflat" => "VECTOR".to_string(),
        other => other.to_ascii_uppercase(),
    }
}

/// Runs a query returning one text cell holding a JSON array of rows.
async fn json_rows(connection: &mut PostgresConnection, query: &str) -> Result<Vec<Row>> {
    let result = connection.execute(query, &HashMap::new()).await?;
    let text = result
        .batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .and_then(|batch| {
            let column = batch.column(0).as_any().downcast_ref::<StringArray>()?;
            (!column.is_null(0)).then(|| column.value(0).to_string())
        })
        .unwrap_or_else(|| "[]".to_string());
    let value: Value = serde_json::from_str(&text)
        .map_err(|error| NetError::ProtocolError(format!("catalog JSON: {error}")))?;
    Ok(match value {
        Value::Array(items) => items
            .into_iter()
            .filter_map(|item| match item {
                Value::Object(row) => Some(row),
                _ => None,
            })
            .collect(),
        Value::Object(row) => vec![row],
        _ => Vec::new(),
    })
}

fn wrap(select: &str) -> String {
    format!("SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json)::text FROM ({select}) t")
}

/// Property rows: one per (element, label, property).
fn properties_query(graph: &str) -> String {
    wrap(&format!(
        "SELECT e.oid::bigint AS element, e.pgekind::text AS kind, l.pgllabel::text AS label, \
                p.pgpname::text AS property, format_type(p.pgptypid, p.pgptypmod) AS sql_type, \
                a.attnum::int AS attnum, a.attnotnull AS not_null \
         FROM pg_propgraph_element e \
         JOIN pg_propgraph_element_label el ON el.pgelelid = e.oid \
         JOIN pg_propgraph_label l ON l.oid = el.pgellabelid \
         LEFT JOIN pg_propgraph_label_property lp ON lp.plpellabelid = el.oid \
         LEFT JOIN pg_propgraph_property p ON p.oid = lp.plppropid \
         LEFT JOIN pg_attribute a ON a.attrelid = e.pgerelid AND a.attnum > 0 \
              AND NOT a.attisdropped \
              AND quote_ident(a.attname) = pg_get_expr(lp.plpexpr, e.pgerelid) \
         WHERE e.pgepgid = {graph} \
         ORDER BY 3, 4",
        graph = graph_oid(graph),
    ))
}

/// Edge endpoint rows: (edge label, source label, target label).
fn endpoints_query(graph: &str) -> String {
    wrap(&format!(
        "SELECT le.pgllabel::text AS label, ls.pgllabel::text AS source, \
                ld.pgllabel::text AS target \
         FROM pg_propgraph_element e \
         JOIN pg_propgraph_element_label xe ON xe.pgelelid = e.oid \
         JOIN pg_propgraph_label le ON le.oid = xe.pgellabelid \
         JOIN pg_propgraph_element_label xs ON xs.pgelelid = e.pgesrcvertexid \
         JOIN pg_propgraph_label ls ON ls.oid = xs.pgellabelid \
         JOIN pg_propgraph_element_label xd ON xd.pgelelid = e.pgedestvertexid \
         JOIN pg_propgraph_label ld ON ld.oid = xd.pgellabelid \
         WHERE e.pgepgid = {graph} AND e.pgekind = 'e'",
        graph = graph_oid(graph),
    ))
}

/// Primary key and unique constraint rows of the element tables.
fn constraints_query(graph: &str) -> String {
    wrap(&format!(
        "SELECT e.oid::bigint AS element, c.conname::text AS name, c.contype::text AS contype, \
                c.conkey::int[] AS columns \
         FROM pg_propgraph_element e \
         JOIN pg_constraint c ON c.conrelid = e.pgerelid \
         WHERE e.pgepgid = {graph} AND c.contype IN ('p', 'u')",
        graph = graph_oid(graph),
    ))
}

/// Index rows of the element tables.
fn indexes_query(graph: &str) -> String {
    wrap(&format!(
        "SELECT e.oid::bigint AS element, ic.relname::text AS name, am.amname::text AS method, \
                i.indisunique AS is_unique, i.indkey::int2[]::int[] AS columns, \
                (i.indexprs IS NOT NULL OR i.indpred IS NOT NULL) AS partial_or_expression, \
                (SELECT c.conname::text FROM pg_constraint c \
                  WHERE c.conindid = i.indexrelid AND c.conrelid = e.pgerelid LIMIT 1) \
                  AS owning_constraint \
         FROM pg_propgraph_element e \
         JOIN pg_index i ON i.indrelid = e.pgerelid \
         JOIN pg_class ic ON ic.oid = i.indexrelid \
         JOIN pg_am am ON am.oid = ic.relam \
         WHERE e.pgepgid = {graph}",
        graph = graph_oid(graph),
    ))
}

fn graph_oid(graph: &str) -> String {
    format!("to_regclass({})", literal(graph))
}

/// One label of one element table.
#[derive(Debug, Default)]
struct ElementLabel {
    entity: Option<EntityKind>,
    /// Column number to property name.
    columns: BTreeMap<i64, String>,
}

/// Turns catalog rows into builder facts.
pub(crate) fn build(
    builder: &mut SchemaBuilder,
    properties: &[Row],
    endpoints: &[Row],
    constraints: &[Row],
    indexes: &[Row],
) {
    // element -> label -> columns
    let mut elements: BTreeMap<i64, BTreeMap<String, ElementLabel>> = BTreeMap::new();
    // label -> number of element tables carrying it
    let mut label_elements: BTreeMap<String, usize> = BTreeMap::new();

    for row in properties {
        let (Some(element), Some(label)) = (
            row.get("element").and_then(Value::as_i64),
            parse::string(row, "label"),
        ) else {
            continue;
        };
        let entity = if parse::string(row, "kind").as_deref() == Some("e") {
            EntityKind::Relationship
        } else {
            EntityKind::Node
        };
        match entity {
            EntityKind::Node => builder.add_label(label.clone()),
            EntityKind::Relationship => builder.add_relationship_type(label.clone()),
        }
        let entry = elements
            .entry(element)
            .or_default()
            .entry(label.clone())
            .or_default();
        if entry.entity.is_none() {
            entry.entity = Some(entity);
            *label_elements.entry(label.clone()).or_default() += 1;
        }
        let Some(property) = parse::string(row, "property") else {
            continue;
        };
        if let Some(sql_type) = parse::string(row, "sql_type") {
            builder.declare_property_type(entity, &label, &property, &sql_type_name(&sql_type));
        } else {
            builder.add_property_type(entity, &label, &property, None);
        }
        if let Some(attnum) = row.get("attnum").and_then(Value::as_i64) {
            entry.columns.insert(attnum, property.clone());
            if row.get("not_null").and_then(Value::as_bool) == Some(true) {
                builder.add_constraint(ConstraintEntry {
                    entity,
                    label_or_type: label.clone(),
                    properties: vec![property],
                    kind: ConstraintKind::Exists,
                    property_type: None,
                    name: None,
                    engine_type: "NOT NULL".to_string(),
                });
            }
        }
    }

    for row in endpoints {
        if let Some(label) = parse::string(row, "label") {
            builder.add_endpoint(
                &label,
                parse::string(row, "source").as_deref(),
                parse::string(row, "target").as_deref(),
            );
        }
    }

    // Labels of `element` that only this element table carries, with the
    // property names of `columns` (None when a column is not a property).
    let single_labels = |element: i64, columns: &[i64]| -> Vec<(String, EntityKind, Vec<String>)> {
        let Some(labels) = elements.get(&element) else {
            return Vec::new();
        };
        labels
            .iter()
            .filter(|(label, _)| label_elements.get(*label) == Some(&1))
            .filter_map(|(label, entry)| {
                let names: Option<Vec<String>> = columns
                    .iter()
                    .map(|column| entry.columns.get(column).cloned())
                    .collect();
                Some((label.clone(), entry.entity?, names?))
            })
            .collect()
    };
    let int_list = |row: &Row, column: &str| -> Vec<i64> {
        row.get(column)
            .and_then(Value::as_array)
            .map(|items| items.iter().filter_map(Value::as_i64).collect())
            .unwrap_or_default()
    };

    for row in constraints {
        let Some(element) = row.get("element").and_then(Value::as_i64) else {
            continue;
        };
        let (kind, engine_type) = match parse::string(row, "contype").as_deref() {
            Some("p") => (ConstraintKind::Key, "PRIMARY KEY"),
            Some("u") => (ConstraintKind::Unique, "UNIQUE"),
            _ => continue,
        };
        for (label, entity, names) in single_labels(element, &int_list(row, "columns")) {
            builder.add_constraint(ConstraintEntry {
                entity,
                label_or_type: label,
                properties: names,
                kind,
                property_type: None,
                name: parse::string(row, "name"),
                engine_type: engine_type.to_string(),
            });
        }
    }

    for row in indexes {
        let Some(element) = row.get("element").and_then(Value::as_i64) else {
            continue;
        };
        if row.get("partial_or_expression").and_then(Value::as_bool) == Some(true) {
            continue;
        }
        let columns = int_list(row, "columns");
        if columns.is_empty() || columns.contains(&0) {
            continue;
        }
        let name = parse::string(row, "name");
        let mut owning_constraint = parse::string(row, "owning_constraint");
        let is_unique = row.get("is_unique").and_then(Value::as_bool) == Some(true);
        let method = parse::string(row, "method").unwrap_or_else(|| "btree".to_string());
        for (label, entity, names) in single_labels(element, &columns) {
            if is_unique && owning_constraint.is_none() {
                // A unique index without a constraint still enforces uniqueness.
                builder.add_constraint(ConstraintEntry {
                    entity,
                    label_or_type: label.clone(),
                    properties: names.clone(),
                    kind: ConstraintKind::Unique,
                    property_type: None,
                    name: name.clone(),
                    engine_type: "UNIQUE INDEX".to_string(),
                });
                owning_constraint = name.clone();
            }
            builder.add_index(IndexEntry {
                entity,
                labels_or_types: vec![label],
                properties: names,
                index_type: index_kind(&method),
                name: name.clone(),
                owning_constraint: owning_constraint.clone(),
                engine_type: method.clone(),
                options: None,
            });
        }
    }
}

/// Result of probing a PostgreSQL connection for SQL/PGQ property graph support.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgqSupport {
    /// Server exposes the SQL/PGQ `pg_propgraph_*` catalogs.
    Supported,
    /// Server lacks SQL/PGQ support; `message` names the server version when available.
    Unsupported {
        /// Server version reported by `SHOW server_version` (or startup parameters).
        server_version: Option<String>,
        /// Human-readable message explaining why SQL/PGQ is unavailable.
        message: String,
    },
}

/// Formats the error message for a PostgreSQL server without SQL/PGQ support.
pub fn no_pgq_support_message(server_version: Option<&str>) -> String {
    match server_version.map(str::trim).filter(|v| !v.is_empty()) {
        Some(version) => format!(
            "PostgreSQL {version} does not support SQL/PGQ \
             (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)"
        ),
        None => "this PostgreSQL server does not support SQL/PGQ \
                 (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)"
            .to_string(),
    }
}

/// Returns `true` when `error` is a PostgreSQL `undefined_table` (`42P01`) or
/// `syntax_error` (`42601`) from probing SQL/PGQ catalogs or syntax.
pub(crate) fn is_missing_pgq_error(error: &NetError) -> bool {
    let msg = match error {
        NetError::ProtocolError(msg) | NetError::ExecutionError(msg) => msg,
        _ => return false,
    };
    let lower = msg.to_ascii_lowercase();
    lower.contains("sqlstate 42p01")
        || lower.contains("sqlstate 42601")
        || lower.contains("undefined_table")
        || lower.contains("does not exist")
        || lower.contains("syntax error")
}

/// Queries `SHOW server_version`, falling back to handshake `ParameterStatus`.
async fn query_server_version(connection: &mut PostgresConnection) -> Option<String> {
    if let Ok(result) = connection
        .execute("SHOW server_version", &HashMap::new())
        .await
    {
        if let Some(version) = result
            .batches
            .iter()
            .find(|b| b.num_rows() > 0)
            .and_then(|b| {
                let col = b.column(0).as_any().downcast_ref::<StringArray>()?;
                (!col.is_null(0)).then(|| col.value(0).trim().to_string())
            })
            && !version.is_empty()
        {
            return Some(version);
        }
    }
    connection
        .server_parameters()
        .get("server_version")
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

struct CatalogProbe {
    has_pgq: bool,
    has_age: bool,
}

async fn probe_catalogs(connection: &mut PostgresConnection) -> Result<CatalogProbe> {
    match json_rows(
        connection,
        "SELECT json_build_object(\
           'pgq', to_regclass('pg_catalog.pg_propgraph_element') IS NOT NULL, \
           'age', to_regclass('ag_catalog.ag_graph') IS NOT NULL)::text",
    )
    .await
    {
        Ok(rows) => {
            let has = |key: &str| {
                rows.first()
                    .and_then(|row| row.get(key))
                    .and_then(Value::as_bool)
                    == Some(true)
            };
            Ok(CatalogProbe {
                has_pgq: has("pgq"),
                has_age: has("age"),
            })
        }
        Err(error) if is_missing_pgq_error(&error) => Ok(CatalogProbe {
            has_pgq: false,
            has_age: false,
        }),
        Err(error) => Err(error),
    }
}

/// Probes whether the connected PostgreSQL server exposes SQL/PGQ property graph catalogs.
pub async fn probe_pgq_support(connection: &mut PostgresConnection) -> Result<PgqSupport> {
    let probe = probe_catalogs(connection).await?;
    if probe.has_pgq {
        return Ok(PgqSupport::Supported);
    }
    let server_version = query_server_version(connection).await;
    let message = no_pgq_support_message(server_version.as_deref());
    Ok(PgqSupport::Unsupported {
        server_version,
        message,
    })
}

/// Introspects the SQL/PGQ property graph named by the URI's `graph` (or
/// `graph_name`) parameter.
pub(crate) async fn introspect_pgq(
    uri: &ParsedUri,
    options: &IntrospectOptions,
) -> Result<GraphSchema> {
    // `PostgresConnection` loads Apache AGE when the URI names a graph; the
    // property graph name is read from `uri` below, so connect without it.
    let mut plain = uri.clone();
    plain.params.remove("graph");
    plain.params.remove("graph_name");
    let mut connection = PostgresConnection::connect(&plain).await?;
    let result = read(&mut connection, uri, options).await;
    let _ = connection.close().await;
    result
}

async fn read(
    connection: &mut PostgresConnection,
    uri: &ParsedUri,
    options: &IntrospectOptions,
) -> Result<GraphSchema> {
    let probe = probe_catalogs(connection).await?;
    if !probe.has_pgq {
        return Err(if probe.has_age {
            unsupported(
                "Apache AGE",
                "AGE stores properties as untyped agtype and reports indexes only as \
                 PostgreSQL expression indexes, so property types, constraints and indexes \
                 cannot be read without guessing",
            )
        } else {
            let server_version = query_server_version(connection).await;
            unsupported(
                "PostgreSQL",
                &no_pgq_support_message(server_version.as_deref()),
            )
        });
    }

    let graph = uri
        .params
        .get("graph")
        .or_else(|| uri.params.get("graph_name"))
        .cloned();
    let available = match json_rows(
        connection,
        &wrap(
            "SELECT DISTINCT pgepgid::regclass::text AS graph FROM pg_propgraph_element ORDER BY 1",
        ),
    )
    .await
    {
        Ok(rows) => rows,
        Err(error) if is_missing_pgq_error(&error) => {
            let server_version = query_server_version(connection).await;
            return Err(unsupported(
                "PostgreSQL",
                &no_pgq_support_message(server_version.as_deref()),
            ));
        }
        Err(error) => return Err(error),
    };
    let available: Vec<String> = available
        .iter()
        .filter_map(|row| parse::string(row, "graph"))
        .collect();
    let listing = if available.is_empty() {
        "this database has no property graphs".to_string()
    } else {
        format!("property graphs in this database: {}", available.join(", "))
    };
    let Some(graph) = graph else {
        return Err(NetError::InvalidUri(format!(
            "PostgreSQL introspection reads one SQL/PGQ property graph; add ?graph=<name> to the \
             URI ({listing})"
        )));
    };
    let exists = json_rows(
        connection,
        &format!(
            "SELECT json_build_object('found', {} IS NOT NULL)::text",
            graph_oid(&graph)
        ),
    )
    .await?;
    if exists
        .first()
        .and_then(|row| row.get("found"))
        .and_then(Value::as_bool)
        != Some(true)
    {
        return Err(NetError::ExecutionError(format!(
            "property graph '{graph}' does not exist ({listing})"
        )));
    }

    let (properties, endpoints, constraints, indexes) =
        match fetch_pgq_catalogs(connection, &graph).await {
            Ok(catalogs) => catalogs,
            Err(error) if is_missing_pgq_error(&error) => {
                let server_version = query_server_version(connection).await;
                return Err(unsupported(
                    "PostgreSQL",
                    &no_pgq_support_message(server_version.as_deref()),
                ));
            }
            Err(error) => return Err(error),
        };

    let mut builder = SchemaBuilder::new();
    build(
        &mut builder,
        &properties,
        &endpoints,
        &constraints,
        &indexes,
    );
    Ok(builder.finish(SourceInfo {
        engine: "postgresql".to_string(),
        server_version: connection
            .server_parameters()
            .get("server_version")
            .cloned(),
        edition: None,
        database: Some(graph),
        sample_size: options.sample_size,
    }))
}

async fn fetch_pgq_catalogs(
    connection: &mut PostgresConnection,
    graph: &str,
) -> Result<(Vec<Row>, Vec<Row>, Vec<Row>, Vec<Row>)> {
    let properties = json_rows(connection, &properties_query(graph)).await?;
    let endpoints = json_rows(connection, &endpoints_query(graph)).await?;
    let constraints = json_rows(connection, &constraints_query(graph)).await?;
    let indexes = json_rows(connection, &indexes_query(graph)).await?;
    Ok((properties, endpoints, constraints, indexes))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn rows(value: Value) -> Vec<Row> {
        value
            .as_array()
            .expect("array")
            .iter()
            .map(|row| row.as_object().expect("object").clone())
            .collect()
    }

    #[test]
    fn sql_types_map_to_export_type_names() {
        for (sql, expected) in [
            ("text", "STRING"),
            ("character varying(20)", "STRING"),
            ("bigint", "INTEGER"),
            ("double precision", "FLOAT"),
            ("boolean", "BOOLEAN"),
            ("timestamp with time zone", "DATETIME"),
            ("date", "DATE"),
            ("interval", "DURATION"),
            ("jsonb", "MAP"),
            ("text[]", "LIST"),
            ("tsvector", "TSVECTOR"),
        ] {
            assert_eq!(sql_type_name(sql), expected, "{sql}");
        }
        assert_eq!(index_kind("btree"), "BTREE");
        assert_eq!(index_kind("hnsw"), "VECTOR");
        assert_eq!(index_kind("gin"), "GIN");
        assert_eq!(literal("a'b"), "'a''b'");
    }

    #[test]
    fn catalog_rows_build_properties_keys_and_endpoints() {
        let mut builder = SchemaBuilder::new();
        build(
            &mut builder,
            &rows(json!([
                {"element": 1, "kind": "v", "label": "Person", "property": "id",
                 "sql_type": "bigint", "attnum": 1, "not_null": true},
                {"element": 1, "kind": "v", "label": "Person", "property": "email",
                 "sql_type": "text", "attnum": 2, "not_null": false},
                {"element": 1, "kind": "v", "label": "Person", "property": "age",
                 "sql_type": "integer", "attnum": 3, "not_null": false},
                {"element": 1, "kind": "v", "label": "Person", "property": "upper_email",
                 "sql_type": "text", "attnum": null, "not_null": null},
                {"element": 2, "kind": "e", "label": "KNOWS", "property": "since",
                 "sql_type": "integer", "attnum": 3, "not_null": false}
            ])),
            &rows(json!([{"label": "KNOWS", "source": "Person", "target": "Person"}])),
            &rows(json!([
                {"element": 1, "name": "person_pkey", "contype": "p", "columns": [1]},
                {"element": 1, "name": "person_email_key", "contype": "u", "columns": [2]},
                {"element": 2, "name": "knows_pkey", "contype": "p", "columns": [1, 2]}
            ])),
            &rows(json!([
                {"element": 1, "name": "person_pkey", "method": "btree", "is_unique": true,
                 "columns": [1], "partial_or_expression": false,
                 "owning_constraint": "person_pkey"},
                {"element": 1, "name": "person_age", "method": "btree", "is_unique": false,
                 "columns": [3], "partial_or_expression": false, "owning_constraint": null},
                {"element": 1, "name": "person_expr", "method": "btree", "is_unique": false,
                 "columns": [0], "partial_or_expression": true, "owning_constraint": null}
            ])),
        );
        let schema = builder.finish(SourceInfo {
            engine: "postgresql".into(),
            server_version: None,
            edition: None,
            database: Some("g".into()),
            sample_size: 0,
        });
        let person = &schema.nodes[0];
        assert_eq!(person.primary_key.as_deref(), Some("id"));
        let id = &person.properties["id"];
        assert_eq!(id.primary_key, Some(true));
        assert!(id.unique && !id.nullable && !id.indexed);
        assert_eq!(id.type_name.as_deref(), Some("INTEGER"));
        assert!(person.properties["email"].unique);
        assert!(person.properties["age"].indexed);
        assert_eq!(
            person.properties["age"].index_type.as_deref(),
            Some("BTREE")
        );
        assert_eq!(
            person.properties["upper_email"].type_name.as_deref(),
            Some("STRING")
        );
        let knows = &schema.relationships[0];
        assert_eq!(knows.source_labels, ["Person"]);
        assert_eq!(knows.target_labels, ["Person"]);
        // The edge key covers columns that are not properties: not mapped.
        assert!(knows.properties["since"].primary_key.is_none());
        assert_eq!(schema.indexes.len(), 2);
    }

    #[test]
    fn no_pgq_support_message_names_server_version() {
        assert_eq!(
            no_pgq_support_message(Some("19.0")),
            "PostgreSQL 19.0 does not support SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)"
        );
        assert_eq!(
            no_pgq_support_message(Some("18.3 (Debian 18.3-1)")),
            "PostgreSQL 18.3 (Debian 18.3-1) does not support SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)"
        );
        assert_eq!(
            no_pgq_support_message(None),
            "this PostgreSQL server does not support SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)"
        );
        assert_eq!(
            no_pgq_support_message(Some("   ")),
            "this PostgreSQL server does not support SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)"
        );
    }

    #[test]
    fn detects_missing_pgq_catalog_and_syntax_errors() {
        assert!(is_missing_pgq_error(&NetError::ProtocolError(
            "ERROR: relation \"pg_propgraph_element\" does not exist (SQLSTATE 42P01)".to_string(),
        )));
        assert!(is_missing_pgq_error(&NetError::ProtocolError(
            "ERROR: syntax error at or near \"(\" (SQLSTATE 42601)".to_string(),
        )));
        assert!(!is_missing_pgq_error(&NetError::ProtocolError(
            "ERROR: permission denied for table users (SQLSTATE 42501)".to_string(),
        )));
        assert!(!is_missing_pgq_error(&NetError::ConnectionFailed(
            "connection refused".to_string(),
        )));
    }

    fn pg_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(msg_type);
        out.extend_from_slice(&(4 + payload.len() as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn pg_param_status(name: &str, value: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
        pg_frame(b'S', &payload)
    }

    fn pg_single_text_row(col: &str, val: &str) -> Vec<u8> {
        let mut out = Vec::new();
        let mut row_desc = Vec::new();
        row_desc.extend_from_slice(&1i16.to_be_bytes());
        row_desc.extend_from_slice(col.as_bytes());
        row_desc.push(0);
        row_desc.extend_from_slice(&0i32.to_be_bytes());
        row_desc.extend_from_slice(&0i16.to_be_bytes());
        row_desc.extend_from_slice(&25i32.to_be_bytes()); // text OID
        row_desc.extend_from_slice(&(-1i16).to_be_bytes());
        row_desc.extend_from_slice(&(-1i32).to_be_bytes());
        row_desc.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&pg_frame(b'T', &row_desc));

        let mut data_row = Vec::new();
        data_row.extend_from_slice(&1i16.to_be_bytes());
        data_row.extend_from_slice(&(val.len() as i32).to_be_bytes());
        data_row.extend_from_slice(val.as_bytes());
        out.extend_from_slice(&pg_frame(b'D', &data_row));
        out.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));
        out.extend_from_slice(&pg_frame(b'Z', b"I"));
        out
    }

    fn pg_error_reply(sqlstate: &str, message: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"SERROR\0");
        payload.push(b'C');
        payload.extend_from_slice(sqlstate.as_bytes());
        payload.push(0);
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        payload.push(0);
        let mut out = pg_frame(b'E', &payload);
        out.extend_from_slice(&pg_frame(b'Z', b"I"));
        out
    }

    async fn spawn_pg_stub(
        server_version: &'static str,
        probe_reply: Vec<u8>,
    ) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut buf = [0u8; 4096];
            // Read StartupMessage
            let _ = stream.read(&mut buf).await;
            let mut startup_reply = Vec::new();
            startup_reply.extend_from_slice(&pg_frame(b'R', &0i32.to_be_bytes()));
            startup_reply.extend_from_slice(&pg_param_status("server_version", server_version));
            startup_reply.extend_from_slice(&pg_frame(b'Z', b"I"));
            if stream.write_all(&startup_reply).await.is_err() {
                return;
            }
            while let Ok(n) = stream.read(&mut buf).await {
                if n == 0 || buf[0] == b'X' {
                    break;
                }
                let query = String::from_utf8_lossy(&buf[5..n]);
                let reply = if query.contains("SHOW server_version") {
                    pg_single_text_row("server_version", server_version)
                } else {
                    probe_reply.clone()
                };
                if stream.write_all(&reply).await.is_err() {
                    break;
                }
            }
        });
        addr
    }

    #[tokio::test]
    async fn introspect_without_pgq_catalogs_names_server_version() {
        let addr = spawn_pg_stub(
            "19.0",
            pg_single_text_row("json_build_object", r#"{"pgq": false, "age": false}"#),
        )
        .await;
        let uri =
            ParsedUri::parse(&format!("postgresql://postgres:pw@{addr}/db?graph=g")).expect("uri");
        let err = crate::introspect(&uri, &IntrospectOptions::default())
            .await
            .expect_err("expected unsupported error");
        assert_eq!(err.code(), voyager_ogm::ErrorCode::UnsupportedFeature);
        let msg = crate::introspect::describe_error(&err);
        assert!(
            msg.contains("PostgreSQL 19.0 does not support SQL/PGQ"),
            "{msg}"
        );
        assert!(
            msg.contains("experimental: PostgreSQL 19 Beta 1-3 and 20 development builds"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn introspect_with_age_catalogs_keeps_age_error() {
        let addr = spawn_pg_stub(
            "18.3",
            pg_single_text_row("json_build_object", r#"{"pgq": false, "age": true}"#),
        )
        .await;
        let uri =
            ParsedUri::parse(&format!("postgresql://postgres:pw@{addr}/db?graph=g")).expect("uri");
        let err = crate::introspect(&uri, &IntrospectOptions::default())
            .await
            .expect_err("expected unsupported error");
        assert_eq!(err.code(), voyager_ogm::ErrorCode::UnsupportedFeature);
        let msg = crate::introspect::describe_error(&err);
        assert!(msg.contains("Dialect 'Apache AGE'"), "{msg}");
    }

    #[tokio::test]
    async fn probe_pgq_support_handles_undefined_table_and_syntax_errors() {
        let addr_42p01 = spawn_pg_stub(
            "18.4",
            pg_error_reply("42P01", "relation \"pg_propgraph_element\" does not exist"),
        )
        .await;
        let uri =
            ParsedUri::parse(&format!("postgresql://postgres:pw@{addr_42p01}/db")).expect("uri");
        let mut conn = PostgresConnection::connect(&uri).await.expect("connect");
        let support = probe_pgq_support(&mut conn).await.expect("probe");
        assert_eq!(
            support,
            PgqSupport::Unsupported {
                server_version: Some("18.4".to_string()),
                message: "PostgreSQL 18.4 does not support SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)".to_string(),
            }
        );

        let addr_42601 = spawn_pg_stub(
            "19.0",
            pg_error_reply("42601", "syntax error at or near \"GRAPH_TABLE\""),
        )
        .await;
        let uri =
            ParsedUri::parse(&format!("postgresql://postgres:pw@{addr_42601}/db")).expect("uri");
        let mut conn = PostgresConnection::connect(&uri).await.expect("connect");
        let support = probe_pgq_support(&mut conn).await.expect("probe");
        assert_eq!(
            support,
            PgqSupport::Unsupported {
                server_version: Some("19.0".to_string()),
                message: "PostgreSQL 19.0 does not support SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds)".to_string(),
            }
        );
    }
}
