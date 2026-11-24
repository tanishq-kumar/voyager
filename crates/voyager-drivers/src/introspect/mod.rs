//! Live schema introspection.
//!
//! [`introspect`] connects to a database and reads its graph schema (labels,
//! relationship types and their endpoints, properties, constraints and
//! indexes) into a [`GraphSchema`]. Serialised to JSON, a [`GraphSchema`] has
//! the shape of the Python `export_schema(models)` dict: `nodes` and
//! `relationships` carry the same keys, so a model export and a live database
//! can be compared key by key. It adds three top-level keys a model export
//! does not have: `constraints` and `indexes` (the catalog as the engine
//! reports it, including composite, full-text and token-lookup entries that do
//! not map onto one property) and `source` (engine, version, sample size).
//!
//! # What a live database cannot report
//!
//! Some keys of a model export describe the Python declaration and have no
//! database counterpart. They are present and `null` in a live schema:
//! `model`, `description` (node, relationship and property), `attribute`,
//! `has_default`, `constraints` (the `Field` value constraints) and the node's
//! `relationship_attributes`. `default`
//! is omitted. `primary_key` is `null` unless the engine has a key constraint
//! (Neo4j Enterprise `NODE_KEY`, a PostgreSQL `PRIMARY KEY`): a uniqueness
//! constraint does not tell a primary key apart from a unique property.
//!
//! # How property facts are derived
//!
//! - `unique`: a single-property uniqueness or key constraint.
//! - `nullable`: `false` only with an existence (`NOT NULL`) or key constraint.
//!   Without such a constraint the engine cannot promise a value, so a
//!   property that happens to be set on every sampled entity stays nullable.
//! - `indexed` / `index_type`: a single-property index that does not back a
//!   constraint. An index created by a uniqueness constraint is listed under
//!   `indexes` with `owning_constraint` set, but does not set `indexed`; this
//!   matches the DDL generated from models, which creates no separate index
//!   for unique or primary-key properties.
//! - `type`: a property type constraint when one exists; otherwise the value
//!   types seen in a bounded sample of at most
//!   [`IntrospectOptions::sample_size`] entities per label or relationship
//!   type. One type gives that type, several give `ANY`, none (a property known
//!   only from a constraint or an index) gives `null`.
//! - Relationship `source_labels` / `target_labels`: the labels of the
//!   endpoints of at most `sample_size` sampled relationships.
//!
//! # Engines
//!
//! - Neo4j 5.13+ and Memgraph: catalogs through `SHOW CONSTRAINTS` /
//!   `SHOW INDEXES` (Neo4j) and `SHOW CONSTRAINT INFO` / `SHOW INDEX INFO`
//!   (Memgraph); property types through `valueType()`.
//! - FalkorDB: `db.labels()`, `db.relationshipTypes()`, `db.indexes()` and
//!   `db.constraints()`; property types through `typeOf()`.
//! - PostgreSQL 19 SQL/PGQ property graphs: the `pg_propgraph_*` catalogs,
//!   with types, keys, `NOT NULL`, unique constraints and indexes read from the
//!   element tables. Requires the graph name (`?graph=` in the URI).
//! - Apache AGE and DuckDB return an `UNSUPPORTED_FEATURE` error.

#[cfg(any(feature = "bolt", feature = "redis"))]
mod cypher;
pub mod parse;
#[cfg(feature = "postgres")]
mod pgq;

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::{NetError, Result};
use crate::uri::{DatabaseProtocol, ParsedUri};

/// Default number of entities sampled per label or relationship type.
pub const DEFAULT_SAMPLE_SIZE: usize = 1000;

/// Options for [`introspect`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntrospectOptions {
    /// Maximum number of nodes sampled per label (and relationships per type)
    /// to derive property value types and relationship endpoints.
    pub sample_size: usize,
}

impl Default for IntrospectOptions {
    fn default() -> Self {
        Self {
            sample_size: DEFAULT_SAMPLE_SIZE,
        }
    }
}

/// A database's graph schema, in the shape of the Python `export_schema` dict.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct GraphSchema {
    /// One entry per node label, sorted by label.
    pub nodes: Vec<NodeEntry>,
    /// One entry per relationship type, sorted by type.
    pub relationships: Vec<RelationshipEntry>,
    /// Every constraint the engine reports.
    pub constraints: Vec<ConstraintEntry>,
    /// Every index the engine reports.
    pub indexes: Vec<IndexEntry>,
    /// Where the schema was read from.
    pub source: SourceInfo,
}

impl GraphSchema {
    /// Returns the schema as a JSON value (key order is stable).
    pub fn to_json_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// A node label.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct NodeEntry {
    /// The label.
    pub name: String,
    /// Always `null`: a database has no model class.
    pub model: Option<String>,
    /// `[name]`.
    pub labels: Vec<String>,
    /// Always `null`.
    pub description: Option<String>,
    /// The property of a single-property key constraint, else `null`.
    pub primary_key: Option<String>,
    /// Properties keyed by property name, sorted by name.
    pub properties: BTreeMap<String, PropertyEntry>,
    /// Always `null`: relationship attributes are declared on Node models.
    pub relationship_attributes: Option<Vec<Value>>,
}

/// A relationship type.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RelationshipEntry {
    /// The relationship type.
    pub name: String,
    /// Always `null`.
    pub model: Option<String>,
    /// The relationship type.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Labels of sampled (or declared) source nodes, sorted.
    pub source_labels: Vec<String>,
    /// Labels of sampled (or declared) target nodes, sorted.
    pub target_labels: Vec<String>,
    /// Always `true`: stored relationships are directed.
    pub directed: bool,
    /// Always `null`.
    pub description: Option<String>,
    /// Properties keyed by property name, sorted by name.
    pub properties: BTreeMap<String, PropertyEntry>,
}

/// A property of a node label or relationship type.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PropertyEntry {
    /// Property name.
    pub name: String,
    /// Always `null`: a database has no Python attribute name.
    pub attribute: Option<String>,
    /// Type name (`STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `DATE`, `DATETIME`,
    /// `DURATION`, `LIST`, `MAP`, `POINT`, ...), `ANY` for mixed values,
    /// `null` when unknown.
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    /// `false` only when an existence or key constraint requires a value.
    pub nullable: bool,
    /// `true` for a single-property key constraint, else `null`.
    pub primary_key: Option<bool>,
    /// `true` for a single-property uniqueness or key constraint.
    pub unique: bool,
    /// `true` for a single-property index that does not back a constraint.
    pub indexed: bool,
    /// The kind of that index (`BTREE`, `TEXT`, `POINT`, `VECTOR`, ...).
    pub index_type: Option<String>,
    /// Always `null`.
    pub description: Option<String>,
    /// Always `null`.
    pub has_default: Option<bool>,
    /// Always `null`: `Field` value constraints are not stored by the engine.
    pub constraints: Option<Map<String, Value>>,
    /// Vector index settings, when a vector index reports them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorInfo>,
}

impl PropertyEntry {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            attribute: None,
            type_name: None,
            nullable: true,
            primary_key: None,
            unique: false,
            indexed: false,
            index_type: None,
            description: None,
            has_default: None,
            constraints: None,
            vector: None,
        }
    }
}

/// Vector index settings of a property.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct VectorInfo {
    /// Vector dimensions, when reported.
    pub dimensions: Option<i64>,
    /// Similarity function (`cosine`, `euclidean`, `dot`), when reported.
    pub similarity: Option<String>,
    /// Index name, when reported.
    pub index_name: Option<String>,
}

/// Whether a catalog entry applies to nodes or relationships.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EntityKind {
    /// A node label.
    Node,
    /// A relationship type.
    Relationship,
}

/// Normalised constraint kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ConstraintKind {
    /// Uniqueness over the listed properties.
    Unique,
    /// Existence (`NOT NULL`) of each listed property.
    Exists,
    /// Key: uniqueness and existence (Neo4j `NODE_KEY`, SQL `PRIMARY KEY`).
    Key,
    /// Property value type.
    Type,
}

/// A constraint as the engine reports it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ConstraintEntry {
    /// Node or relationship.
    pub entity: EntityKind,
    /// The constrained label or relationship type.
    pub label_or_type: String,
    /// Constrained properties, in constraint order.
    pub properties: Vec<String>,
    /// Normalised kind.
    pub kind: ConstraintKind,
    /// Normalised property type of a type constraint.
    pub property_type: Option<String>,
    /// Constraint name, when the engine names constraints.
    pub name: Option<String>,
    /// The engine's own constraint type string.
    pub engine_type: String,
}

/// An index as the engine reports it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IndexEntry {
    /// Node or relationship.
    pub entity: EntityKind,
    /// Indexed labels or relationship types (empty for a token lookup index).
    pub labels_or_types: Vec<String>,
    /// Indexed properties, in index order (empty for a label or type index).
    pub properties: Vec<String>,
    /// Normalised kind: `BTREE`, `TEXT`, `POINT`, `VECTOR`, `FULLTEXT`,
    /// `LOOKUP` (token lookup) or `LABEL` (label or type only).
    pub index_type: String,
    /// Index name, when the engine names indexes.
    pub name: Option<String>,
    /// Name of the constraint the index backs, if any.
    pub owning_constraint: Option<String>,
    /// The engine's own index type string.
    pub engine_type: String,
    /// Engine-specific index options (for example a vector configuration).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Value>,
}

/// Where a [`GraphSchema`] was read from.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SourceInfo {
    /// `neo4j`, `memgraph`, `falkordb` or `postgresql`.
    pub engine: String,
    /// Server version, when reported.
    pub server_version: Option<String>,
    /// Server edition (Neo4j `community` / `enterprise`), when reported.
    pub edition: Option<String>,
    /// Database (Neo4j), graph key (FalkorDB) or property graph (PostgreSQL).
    pub database: Option<String>,
    /// The sample size used for property types and relationship endpoints.
    pub sample_size: usize,
}

/// Collects catalog facts and assembles a [`GraphSchema`].
#[derive(Debug, Default)]
pub struct SchemaBuilder {
    labels: BTreeSet<String>,
    rel_types: BTreeSet<String>,
    node_types: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    rel_types_props: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    endpoints: BTreeMap<String, (BTreeSet<String>, BTreeSet<String>)>,
    constraints: Vec<ConstraintEntry>,
    indexes: Vec<IndexEntry>,
    declared_types: BTreeMap<(EntityKind, String, String), String>,
}

impl SchemaBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a node label.
    pub fn add_label(&mut self, label: impl Into<String>) {
        let label = label.into();
        if !label.is_empty() {
            self.labels.insert(label);
        }
    }

    /// Records a relationship type.
    pub fn add_relationship_type(&mut self, rel_type: impl Into<String>) {
        let rel_type = rel_type.into();
        if !rel_type.is_empty() {
            self.rel_types.insert(rel_type);
        }
    }

    /// Records a value type seen for a property (`type_name` is normalised).
    pub fn add_property_type(
        &mut self,
        entity: EntityKind,
        owner: &str,
        property: &str,
        type_name: Option<&str>,
    ) {
        let map = match entity {
            EntityKind::Node => {
                self.add_label(owner);
                &mut self.node_types
            }
            EntityKind::Relationship => {
                self.add_relationship_type(owner);
                &mut self.rel_types_props
            }
        };
        let types = map
            .entry(owner.to_string())
            .or_default()
            .entry(property.to_string())
            .or_default();
        if let Some(normalised) = type_name.and_then(parse::normalize_value_type) {
            types.insert(normalised);
        }
    }

    /// Records one sampled source and target label of a relationship type.
    pub fn add_endpoint(&mut self, rel_type: &str, source: Option<&str>, target: Option<&str>) {
        self.add_relationship_type(rel_type);
        let entry = self.endpoints.entry(rel_type.to_string()).or_default();
        if let Some(source) = source.filter(|label| !label.is_empty()) {
            entry.0.insert(source.to_string());
        }
        if let Some(target) = target.filter(|label| !label.is_empty()) {
            entry.1.insert(target.to_string());
        }
    }

    /// Records a constraint.
    pub fn add_constraint(&mut self, constraint: ConstraintEntry) {
        match constraint.entity {
            EntityKind::Node => self.add_label(constraint.label_or_type.clone()),
            EntityKind::Relationship => {
                self.add_relationship_type(constraint.label_or_type.clone())
            }
        }
        self.constraints.push(constraint);
    }

    /// Records an index.
    pub fn add_index(&mut self, index: IndexEntry) {
        for owner in &index.labels_or_types {
            match index.entity {
                EntityKind::Node => self.add_label(owner.clone()),
                EntityKind::Relationship => self.add_relationship_type(owner.clone()),
            }
        }
        self.indexes.push(index);
    }

    /// Records a declared column or property type (PostgreSQL), which wins
    /// over sampled types.
    pub fn declare_property_type(
        &mut self,
        entity: EntityKind,
        owner: &str,
        property: &str,
        type_name: &str,
    ) {
        self.add_property_type(entity, owner, property, None);
        self.declared_types.insert(
            (entity, owner.to_string(), property.to_string()),
            type_name.to_string(),
        );
    }

    /// Assembles the schema.
    pub fn finish(self, source: SourceInfo) -> GraphSchema {
        let mut constraints = self.constraints;
        constraints.sort();
        constraints.dedup();
        let mut indexes = self.indexes;
        indexes.sort_by(|left, right| {
            (
                left.entity,
                &left.labels_or_types,
                &left.properties,
                &left.index_type,
                &left.name,
            )
                .cmp(&(
                    right.entity,
                    &right.labels_or_types,
                    &right.properties,
                    &right.index_type,
                    &right.name,
                ))
        });
        indexes.dedup();

        let nodes = self
            .labels
            .iter()
            .map(|label| {
                let properties = build_properties(
                    EntityKind::Node,
                    label,
                    self.node_types.get(label),
                    &constraints,
                    &indexes,
                    &self.declared_types,
                );
                let primary_key = key_property(EntityKind::Node, label, &constraints);
                NodeEntry {
                    name: label.clone(),
                    model: None,
                    labels: vec![label.clone()],
                    description: None,
                    primary_key,
                    properties,
                    relationship_attributes: None,
                }
            })
            .collect();

        let relationships = self
            .rel_types
            .iter()
            .map(|rel_type| {
                let properties = build_properties(
                    EntityKind::Relationship,
                    rel_type,
                    self.rel_types_props.get(rel_type),
                    &constraints,
                    &indexes,
                    &self.declared_types,
                );
                let (source_labels, target_labels) = self
                    .endpoints
                    .get(rel_type)
                    .map(|(source, target)| {
                        (
                            source.iter().cloned().collect(),
                            target.iter().cloned().collect(),
                        )
                    })
                    .unwrap_or_default();
                RelationshipEntry {
                    name: rel_type.clone(),
                    model: None,
                    type_name: rel_type.clone(),
                    source_labels,
                    target_labels,
                    directed: true,
                    description: None,
                    properties,
                }
            })
            .collect();

        GraphSchema {
            nodes,
            relationships,
            constraints,
            indexes,
            source,
        }
    }
}

fn key_property(
    entity: EntityKind,
    owner: &str,
    constraints: &[ConstraintEntry],
) -> Option<String> {
    constraints
        .iter()
        .find(|c| {
            c.entity == entity
                && c.label_or_type == owner
                && c.kind == ConstraintKind::Key
                && c.properties.len() == 1
        })
        .map(|c| c.properties[0].clone())
}

fn build_properties(
    entity: EntityKind,
    owner: &str,
    sampled: Option<&BTreeMap<String, BTreeSet<String>>>,
    constraints: &[ConstraintEntry],
    indexes: &[IndexEntry],
    declared: &BTreeMap<(EntityKind, String, String), String>,
) -> BTreeMap<String, PropertyEntry> {
    let mut properties: BTreeMap<String, PropertyEntry> = BTreeMap::new();

    if let Some(sampled) = sampled {
        for (name, types) in sampled {
            let mut property = PropertyEntry::new(name);
            property.type_name = match types.len() {
                0 => None,
                1 => types.iter().next().cloned(),
                _ => Some("ANY".to_string()),
            };
            properties.insert(name.clone(), property);
        }
    }

    for constraint in constraints
        .iter()
        .filter(|c| c.entity == entity && c.label_or_type == owner)
    {
        for name in &constraint.properties {
            let property = properties
                .entry(name.clone())
                .or_insert_with(|| PropertyEntry::new(name));
            let single = constraint.properties.len() == 1;
            match constraint.kind {
                ConstraintKind::Unique => {
                    if single {
                        property.unique = true;
                    }
                }
                ConstraintKind::Exists => property.nullable = false,
                ConstraintKind::Key => {
                    property.nullable = false;
                    if single {
                        property.unique = true;
                        property.primary_key = Some(true);
                    }
                }
                ConstraintKind::Type => {
                    if let Some(type_name) = &constraint.property_type {
                        property.type_name = Some(type_name.clone());
                    }
                }
            }
        }
    }

    for index in indexes.iter().filter(|i| {
        i.entity == entity
            && i.labels_or_types.len() == 1
            && i.labels_or_types[0] == owner
            && !i.properties.is_empty()
    }) {
        for name in &index.properties {
            properties
                .entry(name.clone())
                .or_insert_with(|| PropertyEntry::new(name));
        }
        if index.properties.len() != 1 || index.owning_constraint.is_some() {
            continue;
        }
        let name = &index.properties[0];
        let Some(property) = properties.get_mut(name) else {
            continue;
        };
        if index.index_type == "FULLTEXT" {
            continue;
        }
        if !property.indexed || property.index_type.as_deref() == Some("BTREE") {
            property.indexed = true;
            property.index_type = Some(index.index_type.clone());
        }
        if index.index_type == "VECTOR" {
            property.vector = Some(parse::vector_info(index));
        }
    }

    for ((kind, declared_owner, name), type_name) in declared {
        if *kind == entity && declared_owner == owner {
            if let Some(property) = properties.get_mut(name) {
                property.type_name = Some(type_name.clone());
            }
        }
    }

    properties
}

/// Describes an introspection error for users: core errors (such as
/// `UNSUPPORTED_FEATURE`) without the "Core compiler error" prefix.
pub fn describe_error(error: &NetError) -> String {
    match error {
        NetError::CoreError(inner) => inner.to_string(),
        other => other.to_string(),
    }
}

/// Error for an engine that has no live introspection.
pub(crate) fn unsupported(engine: &str, reason: &str) -> NetError {
    NetError::CoreError(voyager_ogm::Error::UnsupportedFeature {
        dialect: engine.to_string(),
        feature: format!("live schema introspection ({reason})"),
    })
}

/// Connects to `uri` and reads its graph schema.
///
/// Credentials come from `uri.auth`. Bolt connections choose Neo4j or Memgraph
/// catalog queries from the server agent. Opens one connection and closes it
/// before returning.
///
/// # Errors
///
/// Connection and query errors, and `UNSUPPORTED_FEATURE`
/// ([`voyager_ogm::Error::UnsupportedFeature`]) for Apache AGE, DuckDB, and a
/// protocol whose Cargo feature is disabled.
pub async fn introspect(uri: &ParsedUri, options: &IntrospectOptions) -> Result<GraphSchema> {
    // Without any protocol feature every branch below is an error.
    #[cfg(not(any(feature = "bolt", feature = "redis", feature = "postgres")))]
    let _ = options;
    match uri.protocol {
        DatabaseProtocol::Bolt => {
            #[cfg(feature = "bolt")]
            {
                cypher::introspect_bolt(uri, options).await
            }
            #[cfg(not(feature = "bolt"))]
            {
                Err(unsupported(
                    "Bolt",
                    "voyager-drivers was built without the `bolt` feature",
                ))
            }
        }
        DatabaseProtocol::Redis => {
            #[cfg(feature = "redis")]
            {
                cypher::introspect_falkordb(uri, options).await
            }
            #[cfg(not(feature = "redis"))]
            {
                Err(unsupported(
                    "FalkorDB",
                    "voyager-drivers was built without the `redis` feature",
                ))
            }
        }
        DatabaseProtocol::Postgres => {
            if uses_age(uri) {
                return Err(unsupported(
                    "Apache AGE",
                    "AGE stores properties as untyped agtype and reports indexes only as \
                     PostgreSQL expression indexes, so property types, constraints and \
                     indexes cannot be read without guessing",
                ));
            }
            #[cfg(feature = "postgres")]
            {
                pgq::introspect_pgq(uri, options).await
            }
            #[cfg(not(feature = "postgres"))]
            {
                Err(unsupported(
                    "PostgreSQL",
                    "voyager-drivers was built without the `postgres` feature",
                ))
            }
        }
        DatabaseProtocol::DuckDb => Err(unsupported(
            "DuckDB",
            "DuckDB is embedded and has no network connection",
        )),
    }
}

/// Whether a PostgreSQL URI explicitly targets Apache AGE: an `age://` scheme
/// (optionally behind a `voyager:` style prefix), `?age` or `?dialect=age`.
pub fn uses_age(uri: &ParsedUri) -> bool {
    let scheme = uri.raw.trim().split("://").next().unwrap_or_default();
    let scheme = scheme.rsplit(':').next().unwrap_or(scheme);
    scheme.eq_ignore_ascii_case("age")
        || uri.params.contains_key("age")
        || uri.params.get("dialect").map(String::as_str) == Some("age")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_uris_are_recognised() {
        for (raw, expected) in [
            ("age://postgres@localhost:5455/db", true),
            ("voyager:age://postgres@localhost:5455/db", true),
            ("postgresql://postgres@localhost:5456/db?dialect=age", true),
            ("postgresql://postgres@localhost:5456/db?graph=g", false),
        ] {
            let parsed = ParsedUri::parse(raw).expect("parse");
            assert_eq!(uses_age(&parsed), expected, "{raw}");
        }
    }

    #[tokio::test]
    async fn duckdb_is_unsupported() {
        let parsed = ParsedUri::parse("duckdb:///tmp/x.db").expect("parse");
        let error = introspect(&parsed, &IntrospectOptions::default())
            .await
            .expect_err("unsupported");
        assert_eq!(error.code(), voyager_ogm::ErrorCode::UnsupportedFeature);
        let message = describe_error(&error);
        assert!(
            message.starts_with("Dialect 'DuckDB' does not support"),
            "{message}"
        );
    }
}
