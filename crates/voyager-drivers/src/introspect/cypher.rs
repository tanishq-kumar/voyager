//! Introspection for openCypher engines: Neo4j and Memgraph over Bolt, and
//! FalkorDB over Redis.

use async_trait::async_trait;

use crate::error::Result;
use crate::uri::ParsedUri;

use super::parse::{self, Row};
use super::{EntityKind, GraphSchema, IntrospectOptions, SchemaBuilder, SourceInfo};

/// Which catalog dialect a Cypher engine speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Flavor {
    #[cfg(feature = "bolt")]
    Neo4j,
    #[cfg(feature = "bolt")]
    Memgraph,
    #[cfg(feature = "redis")]
    FalkorDb,
}

impl Flavor {
    fn engine(self) -> &'static str {
        match self {
            #[cfg(feature = "bolt")]
            Self::Neo4j => "neo4j",
            #[cfg(feature = "bolt")]
            Self::Memgraph => "memgraph",
            #[cfg(feature = "redis")]
            Self::FalkorDb => "falkordb",
        }
    }

    /// The function returning a value's type name.
    fn type_function(self) -> &'static str {
        match self {
            #[cfg(feature = "bolt")]
            Self::Neo4j | Self::Memgraph => "valueType",
            #[cfg(feature = "redis")]
            Self::FalkorDb => "typeOf",
        }
    }
}

/// Runs read-only catalog queries and returns rows as JSON objects.
#[async_trait]
pub(crate) trait CatalogSource: Send {
    async fn rows(&mut self, query: &str) -> Result<Vec<Row>>;
}

/// Quotes a label, relationship type or property name for Cypher.
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Sampling query: distinct (property, value type) pairs of at most `limit`
/// nodes with `label`.
pub(crate) fn node_types_query(flavor: Flavor, label: &str, limit: usize) -> String {
    format!(
        "MATCH (n:{label}) WITH n LIMIT {limit} UNWIND keys(n) AS key \
         RETURN DISTINCT key, {function}(n[key]) AS type",
        label = quote_identifier(label),
        function = flavor.type_function(),
    )
}

/// Sampling query: distinct (property, value type) pairs of at most `limit`
/// relationships of `rel_type`.
pub(crate) fn rel_types_query(flavor: Flavor, rel_type: &str, limit: usize) -> String {
    format!(
        "MATCH ()-[r:{rel_type}]->() WITH r LIMIT {limit} UNWIND keys(r) AS key \
         RETURN DISTINCT key, {function}(r[key]) AS type",
        rel_type = quote_identifier(rel_type),
        function = flavor.type_function(),
    )
}

/// Sampling query: distinct (source label, target label) pairs of at most
/// `limit` relationships of `rel_type`.
pub(crate) fn endpoints_query(rel_type: &str, limit: usize) -> String {
    format!(
        "MATCH (a)-[r:{rel_type}]->(b) WITH a, b LIMIT {limit} \
         UNWIND labels(a) AS source UNWIND labels(b) AS target \
         RETURN DISTINCT source, target",
        rel_type = quote_identifier(rel_type),
    )
}

/// Reads labels, relationship types, catalogs and samples into `builder`.
pub(crate) async fn collect(
    source: &mut dyn CatalogSource,
    flavor: Flavor,
    options: &IntrospectOptions,
    builder: &mut SchemaBuilder,
) -> Result<()> {
    // Labels and relationship types.
    let (labels, rel_types) = match flavor {
        #[cfg(feature = "bolt")]
        Flavor::Neo4j => (
            source
                .rows("CALL db.labels() YIELD label RETURN label")
                .await?,
            source
                .rows("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
                .await?,
        ),
        #[cfg(feature = "redis")]
        Flavor::FalkorDb => (
            source.rows("CALL db.labels()").await?,
            source.rows("CALL db.relationshipTypes()").await?,
        ),
        #[cfg(feature = "bolt")]
        Flavor::Memgraph => {
            // The label and edge type metadata commands need
            // --storage-enable-schema-metadata; without it, scan the graph.
            let labels = match source.rows("SHOW NODE_LABELS INFO").await {
                Ok(rows) => rows,
                Err(_) => {
                    source
                        .rows("MATCH (n) UNWIND labels(n) AS label RETURN DISTINCT label")
                        .await?
                }
            };
            let types = match source.rows("SHOW EDGE_TYPES INFO").await {
                Ok(rows) => rows,
                Err(_) => {
                    source
                        .rows("MATCH ()-[r]->() RETURN DISTINCT type(r) AS type")
                        .await?
                }
            };
            (labels, types)
        }
    };
    for row in &labels {
        if let Some(label) = parse::first_string(row) {
            builder.add_label(label);
        }
    }
    for row in &rel_types {
        if let Some(rel_type) = parse::first_string(row) {
            builder.add_relationship_type(rel_type);
        }
    }

    // Constraints and indexes.
    match flavor {
        #[cfg(feature = "bolt")]
        Flavor::Neo4j => {
            let constraints = source.rows("SHOW CONSTRAINTS YIELD *").await?;
            for constraint in parse::neo4j_constraints(&constraints) {
                builder.add_constraint(constraint);
            }
            let indexes = source.rows("SHOW INDEXES YIELD *").await?;
            for index in parse::neo4j_indexes(&indexes) {
                builder.add_index(index);
            }
        }
        #[cfg(feature = "bolt")]
        Flavor::Memgraph => {
            let constraints = source.rows("SHOW CONSTRAINT INFO").await?;
            for constraint in parse::memgraph_constraints(&constraints) {
                builder.add_constraint(constraint);
            }
            let indexes = source.rows("SHOW INDEX INFO").await?;
            for index in
                parse::memgraph_indexes_with_types(&indexes, &builder.labels, &builder.rel_types)
            {
                builder.add_index_without_creating_labels(index);
            }
        }
        #[cfg(feature = "redis")]
        Flavor::FalkorDb => {
            let constraint_rows = source
                .rows(
                    "CALL db.constraints() YIELD type, label, properties, entitytype \
                     RETURN type, label, properties, entitytype",
                )
                .await?;
            let constraints = parse::falkordb_constraints(&constraint_rows);
            let index_rows = source
                .rows(
                    "CALL db.indexes() YIELD label, properties, types, entitytype \
                     RETURN label, properties, types, entitytype",
                )
                .await?;
            for index in parse::falkordb_indexes(&index_rows, &constraints) {
                builder.add_index(index);
            }
            for constraint in constraints {
                builder.add_constraint(constraint);
            }
        }
    }

    // Bounded samples: property value types and relationship endpoints.
    let limit = options.sample_size;
    if limit == 0 {
        return Ok(());
    }
    let labels: Vec<String> = builder.labels.iter().cloned().collect();
    for label in &labels {
        let rows = source.rows(&node_types_query(flavor, label, limit)).await?;
        for row in &rows {
            if let Some(key) = parse::string(row, "key") {
                let type_name = parse::string(row, "type");
                builder.add_property_type(EntityKind::Node, label, &key, type_name.as_deref());
            }
        }
    }
    let rel_types: Vec<String> = builder.rel_types.iter().cloned().collect();
    for rel_type in &rel_types {
        let rows = source
            .rows(&rel_types_query(flavor, rel_type, limit))
            .await?;
        for row in &rows {
            if let Some(key) = parse::string(row, "key") {
                let type_name = parse::string(row, "type");
                builder.add_property_type(
                    EntityKind::Relationship,
                    rel_type,
                    &key,
                    type_name.as_deref(),
                );
            }
        }
        let rows = source.rows(&endpoints_query(rel_type, limit)).await?;
        for row in &rows {
            builder.add_endpoint(
                rel_type,
                parse::string(row, "source").as_deref(),
                parse::string(row, "target").as_deref(),
            );
        }
    }
    Ok(())
}

#[cfg(feature = "bolt")]
mod bolt_source {
    use std::collections::HashMap;

    use async_trait::async_trait;

    use super::{CatalogSource, Row};
    use crate::bolt::BoltConnection;
    use crate::error::{NetError, Result};
    use crate::introspect::parse;

    pub(crate) struct BoltSource {
        pub(crate) connection: BoltConnection,
        pub(crate) database: Option<String>,
    }

    #[async_trait]
    impl CatalogSource for BoltSource {
        async fn rows(&mut self, query: &str) -> Result<Vec<Row>> {
            let (columns, records, _summary, error) = self
                .connection
                .execute_raw_records_with_options(
                    query,
                    &HashMap::new(),
                    self.database.as_deref(),
                    None,
                    None,
                    None,
                )
                .await?;
            if let Some((code, message)) = error {
                return Err(NetError::ExecutionError(format!("{code}: {message}")));
            }
            Ok(parse::rows_from_bolt(&columns, &records))
        }
    }
}

/// Introspects a Neo4j or Memgraph database over Bolt.
#[cfg(feature = "bolt")]
pub(crate) async fn introspect_bolt(
    uri: &ParsedUri,
    options: &IntrospectOptions,
) -> Result<GraphSchema> {
    use crate::AsyncConnection;
    use crate::bolt::BoltConnection;
    use crate::config::ConnectionConfig;

    let mut config = ConnectionConfig::from_uri(uri.raw.clone());
    config.auth = uri.auth.clone();
    config.database = uri.database.clone();
    config.tls = uri.tls;
    let connection = BoltConnection::connect(&config).await?;
    let agent = connection.server_agent().unwrap_or_default().to_string();
    let flavor = if agent.to_ascii_lowercase().contains("memgraph") {
        Flavor::Memgraph
    } else {
        Flavor::Neo4j
    };
    let mut source = bolt_source::BoltSource {
        connection,
        database: match flavor {
            Flavor::Neo4j => uri.database.clone(),
            _ => None,
        },
    };

    let mut server_version = agent
        .split_once('/')
        .map(|(_, version)| version.to_string())
        .filter(|version| !version.is_empty());
    let mut edition = None;
    match flavor {
        Flavor::Neo4j => {
            if let Ok(rows) = source
                .rows("CALL dbms.components() YIELD versions, edition RETURN versions, edition")
                .await
            {
                if let Some(row) = rows.first() {
                    edition = parse::string(row, "edition");
                    if let Some(version) = parse::string_list(row, "versions").into_iter().next() {
                        server_version = Some(version);
                    }
                }
            }
        }
        _ => {
            if let Ok(rows) = source.rows("SHOW VERSION").await {
                if let Some(version) = rows.first().and_then(parse::first_string) {
                    server_version = Some(version);
                }
            }
        }
    }

    let mut builder = SchemaBuilder::new();
    let result = collect(&mut source, flavor, options, &mut builder).await;
    let _ = source.connection.close().await;
    result?;
    Ok(builder.finish(SourceInfo {
        engine: flavor.engine().to_string(),
        server_version,
        edition,
        database: match flavor {
            Flavor::Neo4j => uri.database.clone(),
            _ => None,
        },
        sample_size: options.sample_size,
    }))
}

#[cfg(feature = "redis")]
mod falkor_source {
    use std::collections::HashMap;

    use async_trait::async_trait;

    use super::{CatalogSource, Row};
    use crate::error::Result;
    use crate::introspect::parse;
    use crate::redis::RedisConnection;
    use crate::redis::graph::resp_to_bolt_value;

    pub(crate) struct FalkorSource {
        pub(crate) connection: RedisConnection,
        pub(crate) graph: String,
    }

    #[async_trait]
    impl CatalogSource for FalkorSource {
        async fn rows(&mut self, query: &str) -> Result<Vec<Row>> {
            let result = self
                .connection
                .graph_ro_query(&self.graph, query, &HashMap::new())
                .await?;
            let records: Vec<Vec<_>> = result
                .rows
                .iter()
                .map(|row| row.iter().map(resp_to_bolt_value).collect())
                .collect();
            Ok(parse::rows_from_bolt(&result.columns, &records))
        }
    }
}

/// Introspects one FalkorDB graph (the URI's graph name, as for queries).
#[cfg(feature = "redis")]
pub(crate) async fn introspect_falkordb(
    uri: &ParsedUri,
    options: &IntrospectOptions,
) -> Result<GraphSchema> {
    use crate::AsyncConnection;
    use crate::redis::RedisConnection;

    let connection = RedisConnection::connect(uri).await?;
    let graph = connection.default_graph().to_string();
    let server_version = ["falkor_version", "redis_version", "version"]
        .iter()
        .find_map(|key| connection.server_info().get(*key))
        .map(|value| value.to_string_lossy())
        .filter(|version| !version.is_empty());
    let mut source = falkor_source::FalkorSource { connection, graph };

    let mut builder = SchemaBuilder::new();
    let result = collect(&mut source, Flavor::FalkorDb, options, &mut builder).await;
    let _ = source.connection.close().await;
    result?;
    Ok(builder.finish(SourceInfo {
        engine: Flavor::FalkorDb.engine().to_string(),
        server_version,
        edition: None,
        database: Some(source.graph),
        sample_size: options.sample_size,
    }))
}

#[cfg(all(test, feature = "bolt", feature = "redis"))]
mod tests {
    use std::collections::VecDeque;

    use serde_json::{Value, json};

    use super::*;

    /// Replays canned rows, keyed by a query prefix, in order.
    struct Replay {
        answers: VecDeque<(&'static str, std::result::Result<Value, &'static str>)>,
    }

    #[async_trait]
    impl CatalogSource for Replay {
        async fn rows(&mut self, query: &str) -> Result<Vec<Row>> {
            let (prefix, answer) = self.answers.pop_front().expect("unexpected query");
            assert!(
                query.starts_with(prefix),
                "expected a query starting with {prefix:?}, got {query:?}"
            );
            match answer {
                Ok(value) => Ok(value
                    .as_array()
                    .expect("rows")
                    .iter()
                    .map(|row| row.as_object().expect("row").clone())
                    .collect()),
                Err(message) => Err(crate::error::NetError::ExecutionError(message.into())),
            }
        }
    }

    fn source_info(engine: &str) -> SourceInfo {
        SourceInfo {
            engine: engine.to_string(),
            server_version: None,
            edition: None,
            database: None,
            sample_size: 10,
        }
    }

    #[test]
    fn identifiers_are_backtick_quoted() {
        assert_eq!(quote_identifier("Person"), "`Person`");
        assert_eq!(quote_identifier("we`ird"), "`we``ird`");
        assert_eq!(
            node_types_query(Flavor::FalkorDb, "P", 5),
            "MATCH (n:`P`) WITH n LIMIT 5 UNWIND keys(n) AS key RETURN DISTINCT key, typeOf(n[key]) AS type"
        );
    }

    #[tokio::test]
    async fn neo4j_catalog_builds_the_export_shape() {
        let mut source = Replay {
            answers: VecDeque::from([
                ("CALL db.labels()", Ok(json!([{"label": "Person"}]))),
                (
                    "CALL db.relationshipTypes()",
                    Ok(json!([{"relationshipType": "KNOWS"}])),
                ),
                (
                    "SHOW CONSTRAINTS",
                    Ok(json!([
                        {"name": "u", "type": "UNIQUENESS", "entityType": "NODE",
                         "labelsOrTypes": ["Person"], "properties": ["email"]}
                    ])),
                ),
                (
                    "SHOW INDEXES",
                    Ok(json!([
                        {"name": "u", "type": "RANGE", "entityType": "NODE",
                         "labelsOrTypes": ["Person"], "properties": ["email"],
                         "owningConstraint": "u"},
                        {"name": "i", "type": "RANGE", "entityType": "NODE",
                         "labelsOrTypes": ["Person"], "properties": ["name"],
                         "owningConstraint": null},
                        {"name": "r", "type": "RANGE", "entityType": "RELATIONSHIP",
                         "labelsOrTypes": ["KNOWS"], "properties": ["since"],
                         "owningConstraint": null}
                    ])),
                ),
                (
                    "MATCH (n:`Person`)",
                    Ok(json!([
                        {"key": "email", "type": "STRING NOT NULL"},
                        {"key": "name", "type": "STRING NOT NULL"},
                        {"key": "age", "type": "INTEGER NOT NULL"},
                        {"key": "age", "type": "FLOAT NOT NULL"}
                    ])),
                ),
                (
                    "MATCH ()-[r:`KNOWS`]",
                    Ok(json!([{"key": "since", "type": "INTEGER NOT NULL"}])),
                ),
                (
                    "MATCH (a)-[r:`KNOWS`]",
                    Ok(json!([{"source": "Person", "target": "Person"}])),
                ),
            ]),
        };
        let mut builder = SchemaBuilder::new();
        collect(
            &mut source,
            Flavor::Neo4j,
            &IntrospectOptions { sample_size: 10 },
            &mut builder,
        )
        .await
        .expect("collect");
        assert!(source.answers.is_empty());
        let schema = builder.finish(source_info("neo4j")).to_json_value();

        let person = &schema["nodes"][0];
        assert_eq!(person["name"], "Person");
        assert_eq!(person["labels"], json!(["Person"]));
        assert_eq!(person["model"], Value::Null);
        assert_eq!(person["primary_key"], Value::Null);
        let email = &person["properties"]["email"];
        assert_eq!(email["type"], "STRING");
        assert_eq!(email["unique"], true);
        assert_eq!(email["indexed"], false);
        assert_eq!(email["nullable"], true);
        assert_eq!(email["attribute"], Value::Null);
        assert_eq!(person["properties"]["name"]["indexed"], true);
        assert_eq!(person["properties"]["name"]["index_type"], "BTREE");
        assert_eq!(person["properties"]["age"]["type"], "ANY");

        let knows = &schema["relationships"][0];
        assert_eq!(knows["type"], "KNOWS");
        assert_eq!(knows["source_labels"], json!(["Person"]));
        assert_eq!(knows["target_labels"], json!(["Person"]));
        assert_eq!(knows["directed"], true);
        assert_eq!(knows["properties"]["since"]["type"], "INTEGER");
        assert_eq!(knows["properties"]["since"]["indexed"], true);

        assert_eq!(schema["constraints"].as_array().map(Vec::len), Some(1));
        assert_eq!(schema["indexes"].as_array().map(Vec::len), Some(3));
        assert_eq!(
            schema["nodes"][0]
                .as_object()
                .map(|o| o.keys().cloned().collect::<Vec<_>>()),
            Some(
                [
                    "name",
                    "model",
                    "labels",
                    "description",
                    "primary_key",
                    "properties",
                    "relationship_attributes"
                ]
                .map(String::from)
                .to_vec()
            )
        );
    }

    #[tokio::test]
    async fn memgraph_scans_labels_without_schema_metadata() {
        let mut source = Replay {
            answers: VecDeque::from([
                ("SHOW NODE_LABELS INFO", Err("metadata disabled")),
                ("MATCH (n) UNWIND labels(n)", Ok(json!([{"label": "P"}]))),
                ("SHOW EDGE_TYPES INFO", Err("metadata disabled")),
                ("MATCH ()-[r]->() RETURN DISTINCT type(r)", Ok(json!([]))),
                (
                    "SHOW CONSTRAINT INFO",
                    Ok(json!([
                        {"constraint type": "exists", "label": "P", "properties": "id",
                         "data_type": ""},
                        {"constraint type": "unique", "label": "P", "properties": ["id"],
                         "data_type": ""},
                        {"constraint type": "data_type", "label": "P", "properties": "id",
                         "data_type": "STRING"}
                    ])),
                ),
                ("SHOW INDEX INFO", Ok(json!([]))),
                ("MATCH (n:`P`)", Ok(json!([]))),
            ]),
        };
        let mut builder = SchemaBuilder::new();
        collect(
            &mut source,
            Flavor::Memgraph,
            &IntrospectOptions { sample_size: 10 },
            &mut builder,
        )
        .await
        .expect("collect");
        let schema = builder.finish(source_info("memgraph"));
        let id = &schema.nodes[0].properties["id"];
        assert!(!id.nullable);
        assert!(id.unique);
        assert_eq!(id.type_name.as_deref(), Some("STRING"));
        assert!(schema.relationships.is_empty());
    }

    #[tokio::test]
    async fn zero_sample_size_skips_sampling() {
        let mut source = Replay {
            answers: VecDeque::from([
                ("CALL db.labels()", Ok(json!([{"label": "P"}]))),
                ("CALL db.relationshipTypes()", Ok(json!([]))),
                (
                    "CALL db.constraints()",
                    Ok(json!([{"type": "UNIQUE", "label": "P", "properties": "[k]",
                               "entitytype": "NODE"}])),
                ),
                (
                    "CALL db.indexes()",
                    Ok(
                        json!([{"label": "P", "properties": ["k"], "types": {"k": ["RANGE"]},
                               "entitytype": "NODE"}]),
                    ),
                ),
            ]),
        };
        let mut builder = SchemaBuilder::new();
        collect(
            &mut source,
            Flavor::FalkorDb,
            &IntrospectOptions { sample_size: 0 },
            &mut builder,
        )
        .await
        .expect("collect");
        let schema = builder.finish(source_info("falkordb"));
        let k = &schema.nodes[0].properties["k"];
        assert!(k.unique);
        assert!(!k.indexed, "the range index backs the unique constraint");
        assert_eq!(k.type_name, None);
    }

    #[tokio::test]
    async fn memgraph_classifies_rel_label_property_index_without_phantom_label() {
        let mut source = Replay {
            answers: VecDeque::from([
                ("SHOW NODE_LABELS INFO", Err("metadata disabled")),
                (
                    "MATCH (n) UNWIND labels(n)",
                    Ok(json!([{"label": "Movie"}, {"label": "Person"}])),
                ),
                ("SHOW EDGE_TYPES INFO", Err("metadata disabled")),
                (
                    "MATCH ()-[r]->() RETURN DISTINCT type(r)",
                    Ok(json!([{"type": "ACTED_IN"}, {"type": "KNOWS"}])),
                ),
                ("SHOW CONSTRAINT INFO", Ok(json!([]))),
                (
                    "SHOW INDEX INFO",
                    Ok(json!([
                        {"index type": "label", "label": "Movie", "property": null, "count": 1},
                        {"index type": "label+property", "label": "Movie", "property": ["title"], "count": 1},
                        {"index type": "label+property", "label": "ACTED_IN", "property": ["billing"], "count": 0},
                        {"index type": "edge-type", "label": "KNOWS", "property": null, "count": 1},
                        {"index type": "edge-type+property", "label": "KNOWS", "property": "since", "count": 1},
                        {"index type": "label+property", "label": "PhantomNode", "property": ["ghost"], "count": 0}
                    ])),
                ),
                (
                    "MATCH (n:`Movie`)",
                    Ok(json!([{"key": "title", "type": "STRING NOT NULL"}])),
                ),
                ("MATCH (n:`Person`)", Ok(json!([]))),
                (
                    "MATCH ()-[r:`ACTED_IN`]",
                    Ok(json!([{"key": "billing", "type": "INTEGER NOT NULL"}])),
                ),
                (
                    "MATCH (a)-[r:`ACTED_IN`]",
                    Ok(json!([{"source": "Person", "target": "Movie"}])),
                ),
                (
                    "MATCH ()-[r:`KNOWS`]",
                    Ok(json!([{"key": "since", "type": "INTEGER NOT NULL"}])),
                ),
                (
                    "MATCH (a)-[r:`KNOWS`]",
                    Ok(json!([{"source": "Person", "target": "Person"}])),
                ),
            ]),
        };
        let mut builder = SchemaBuilder::new();
        collect(
            &mut source,
            Flavor::Memgraph,
            &IntrospectOptions { sample_size: 10 },
            &mut builder,
        )
        .await
        .expect("collect");
        assert!(source.answers.is_empty());
        let schema = builder.finish(source_info("memgraph"));

        let node_names: Vec<&str> = schema.nodes.iter().map(|n| n.name.as_str()).collect();
        assert_eq!(node_names, vec!["Movie", "Person"]);
        assert!(schema.nodes[0].properties["title"].indexed);

        let acted_in = schema
            .relationships
            .iter()
            .find(|r| r.name == "ACTED_IN")
            .expect("ACTED_IN");
        assert!(acted_in.properties["billing"].indexed);
        assert_eq!(
            acted_in.properties["billing"].index_type.as_deref(),
            Some("BTREE")
        );

        let knows = schema
            .relationships
            .iter()
            .find(|r| r.name == "KNOWS")
            .expect("KNOWS");
        assert!(knows.properties["since"].indexed);

        assert!(schema.indexes.iter().any(|i| {
            i.entity == EntityKind::Relationship
                && i.labels_or_types == ["ACTED_IN"]
                && i.properties == ["billing"]
        }));
        assert!(schema.indexes.iter().any(|i| {
            i.entity == EntityKind::Relationship
                && i.labels_or_types == ["KNOWS"]
                && i.properties.is_empty()
                && i.index_type == "LABEL"
        }));
    }

    #[tokio::test]
    async fn falkordb_groups_multi_property_fulltext_index() {
        let mut source = Replay {
            answers: VecDeque::from([
                ("CALL db.labels()", Ok(json!([{"label": "Movie"}]))),
                ("CALL db.relationshipTypes()", Ok(json!([]))),
                ("CALL db.constraints()", Ok(json!([]))),
                (
                    "CALL db.indexes()",
                    Ok(json!([{
                        "label": "Movie",
                        "properties": ["title", "overview", "released"],
                        "types": {
                            "title": ["RANGE", "FULLTEXT"],
                            "overview": ["FULLTEXT"],
                            "released": ["RANGE"]
                        },
                        "entitytype": "NODE"
                    }])),
                ),
            ]),
        };
        let mut builder = SchemaBuilder::new();
        collect(
            &mut source,
            Flavor::FalkorDb,
            &IntrospectOptions { sample_size: 0 },
            &mut builder,
        )
        .await
        .expect("collect");
        let schema = builder.finish(source_info("falkordb"));

        let fulltext: Vec<_> = schema
            .indexes
            .iter()
            .filter(|i| i.index_type == "FULLTEXT")
            .collect();
        assert_eq!(fulltext.len(), 1);
        assert_eq!(fulltext[0].labels_or_types, ["Movie"]);
        assert_eq!(fulltext[0].properties, ["title", "overview"]);

        let movie = &schema.nodes[0];
        assert!(movie.properties["title"].indexed);
        assert!(movie.properties["released"].indexed);
        assert!(!movie.properties["overview"].indexed);
    }
}
