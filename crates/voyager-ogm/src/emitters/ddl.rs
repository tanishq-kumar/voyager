//! Multi-Dialect DDL Migration Emitters for Voyager OGM.
//!
//! Generates deterministic, standard-compliant schema migration statements
//! across openCypher (Neo4j / Memgraph), ISO GQL, and SQL:2023 PGQ / DuckPGQ
//! directly from native `SchemaRegistry` metadata.

use crate::emitters::age::{DEFAULT_AGE_GRAPH, is_valid_age_graph_name};
use crate::error::{Error, Result};
use crate::schema::{
    DEFAULT_MEMGRAPH_VECTOR_CAPACITY, DEFAULT_VECTOR_DIMENSIONS, FieldDescriptor, FieldType,
    IndexType, NodeSchema, RelationshipSchema, VectorSimilarity, canonical_fulltext_index_name,
    canonical_vector_index_name,
};

/// Maps a canonical `FieldType` to the openCypher / Neo4j 5.x property type identifier.
pub fn field_type_to_neo4j(ft: &FieldType) -> &'static str {
    match ft {
        FieldType::String => "STRING",
        FieldType::Int64 => "INTEGER",
        FieldType::Float64 => "FLOAT",
        FieldType::Boolean => "BOOLEAN",
        FieldType::DateTime => "DATETIME",
        FieldType::Date => "DATE",
        FieldType::Duration => "DURATION",
        FieldType::List(_) => "LIST",
        FieldType::Map => "MAP",
        FieldType::Any => "ANY",
        FieldType::Custom(s) => match s.to_ascii_uppercase().as_str() {
            "STRING" | "STR" | "TEXT" | "VARCHAR" => "STRING",
            "INTEGER" | "INT" | "INT64" | "BIGINT" => "INTEGER",
            "FLOAT" | "FLOAT64" | "DOUBLE" => "FLOAT",
            "BOOLEAN" | "BOOL" => "BOOLEAN",
            "DATETIME" | "TIMESTAMP" => "DATETIME",
            "DATE" => "DATE",
            "DURATION" | "INTERVAL" => "DURATION",
            "LIST" | "ARRAY" => "LIST",
            "MAP" | "DICT" => "MAP",
            _ => "ANY",
        },
    }
}

/// Maps a canonical `FieldType` to the ISO GQL standard type name.
pub fn field_type_to_gql(ft: &FieldType) -> &'static str {
    match ft {
        FieldType::String => "STRING",
        FieldType::Int64 => "INTEGER",
        FieldType::Float64 => "FLOAT",
        FieldType::Boolean => "BOOLEAN",
        FieldType::DateTime => "DATETIME",
        FieldType::Date => "DATE",
        FieldType::Duration => "DURATION",
        FieldType::List(_) => "LIST",
        FieldType::Map => "MAP",
        FieldType::Any => "ANY",
        FieldType::Custom(s) => match s.to_ascii_uppercase().as_str() {
            "STRING" | "STR" | "TEXT" | "VARCHAR" => "STRING",
            "INTEGER" | "INT" | "INT64" | "BIGINT" => "INTEGER",
            "FLOAT" | "FLOAT64" | "DOUBLE" => "FLOAT",
            "BOOLEAN" | "BOOL" => "BOOLEAN",
            "DATETIME" | "TIMESTAMP" => "DATETIME",
            "DATE" => "DATE",
            "DURATION" | "INTERVAL" => "DURATION",
            "LIST" | "ARRAY" => "LIST",
            _ => "ANY",
        },
    }
}

/// Maps a canonical `FieldType` to standard SQL / SQL:2023 PGQ data types.
pub fn field_type_to_pgq(ft: &FieldType) -> &'static str {
    match ft {
        FieldType::String => "VARCHAR",
        FieldType::Int64 => "BIGINT",
        FieldType::Float64 => "DOUBLE PRECISION",
        FieldType::Boolean => "BOOLEAN",
        FieldType::DateTime => "TIMESTAMP WITH TIME ZONE",
        FieldType::Date => "DATE",
        FieldType::Duration => "INTERVAL",
        FieldType::List(_) => "ARRAY",
        FieldType::Map => "JSON",
        FieldType::Any => "VARCHAR",
        FieldType::Custom(s) => match s.to_ascii_uppercase().as_str() {
            "STRING" | "STR" | "TEXT" | "VARCHAR" => "VARCHAR",
            "INTEGER" | "INT" | "INT64" | "BIGINT" => "BIGINT",
            "FLOAT" | "FLOAT64" | "DOUBLE" => "DOUBLE PRECISION",
            "BOOLEAN" | "BOOL" => "BOOLEAN",
            "DATETIME" | "TIMESTAMP" => "TIMESTAMP WITH TIME ZONE",
            "DATE" => "DATE",
            "DURATION" | "INTERVAL" => "INTERVAL",
            "LIST" | "ARRAY" => "ARRAY",
            "MAP" | "JSON" | "JSONB" => "JSON",
            _ => "VARCHAR",
        },
    }
}

/// Constructs a deterministic openCypher constraint identifier name.
pub fn cypher_constraint_name(label: &str, prop: &str, kind: &str) -> String {
    format!(
        "constraint_{}_{}_{}",
        label.to_ascii_lowercase(),
        prop.to_ascii_lowercase(),
        kind
    )
}

/// Constructs a deterministic openCypher relationship constraint identifier name.
pub fn cypher_rel_constraint_name(rel_type: &str, prop: &str, kind: &str) -> String {
    format!(
        "constraint_rel_{}_{}_{}",
        rel_type.to_ascii_lowercase(),
        prop.to_ascii_lowercase(),
        kind
    )
}

/// Constructs a deterministic openCypher secondary index identifier name.
pub fn cypher_index_name(label: &str, prop: &str) -> String {
    format!(
        "index_{}_{}",
        label.to_ascii_lowercase(),
        prop.to_ascii_lowercase()
    )
}

/// Constructs a deterministic openCypher relationship index identifier name.
pub fn cypher_rel_index_name(rel_type: &str, prop: &str) -> String {
    format!(
        "index_rel_{}_{}",
        rel_type.to_ascii_lowercase(),
        prop.to_ascii_lowercase()
    )
}

// ---------------------------------------------------------------------------
// openCypher DDL Emitter (Neo4j / Memgraph)
// ---------------------------------------------------------------------------

/// Emits openCypher constraint and index creation statements for a node schema.
pub fn emit_cypher_node_ddl(node: &NodeSchema, include_type_constraints: bool) -> Vec<String> {
    let mut statements = Vec::new();
    let primary_label = node.primary_label();

    for field in node.fields.values() {
        let db_name = &field.name;

        // Unique / Primary Key constraint. Emitted before the NOT NULL/existence
        // constraint so Neo4j Community still gets uniqueness when the
        // Enterprise-only existence statement is rejected.
        if field.unique || field.primary_key {
            let c_name = cypher_constraint_name(primary_label, db_name, "unique");
            statements.push(format!(
                "CREATE CONSTRAINT {c_name} IF NOT EXISTS FOR (n:{primary_label}) REQUIRE n.{db_name} IS UNIQUE"
            ));
        }

        // Property existence (NOT NULL) constraint
        if !field.nullable {
            let c_name = cypher_constraint_name(primary_label, db_name, "not_null");
            statements.push(format!(
                "CREATE CONSTRAINT {c_name} IF NOT EXISTS FOR (n:{primary_label}) REQUIRE n.{db_name} IS NOT NULL"
            ));
        }

        // Secondary index (only if not already protected by unique constraint)
        if field.indexed && !field.unique && !field.primary_key {
            let i_name = field
                .vector_config
                .as_ref()
                .and_then(|vc| vc.index_name.clone())
                .unwrap_or_else(|| {
                    if field.index_type == Some(IndexType::Vector) {
                        canonical_vector_index_name(primary_label, db_name, DdlDialect::Cypher)
                    } else {
                        cypher_index_name(primary_label, db_name)
                    }
                });
            match field.index_type {
                Some(IndexType::Text) => {
                    statements.push(format!(
                        "CREATE TEXT INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON (n.{db_name})"
                    ));
                }
                Some(IndexType::Point) => {
                    statements.push(format!(
                        "CREATE POINT INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON (n.{db_name})"
                    ));
                }
                Some(IndexType::Vector) => {
                    let (dims, sim) = if let Some(ref vc) = field.vector_config {
                        (vc.dimensions, vc.similarity.metric_for(DdlDialect::Cypher))
                    } else {
                        (
                            DEFAULT_VECTOR_DIMENSIONS,
                            VectorSimilarity::Cosine.metric_for(DdlDialect::Cypher),
                        )
                    };
                    statements.push(format!(
                        "CREATE VECTOR INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON (n.{db_name}) OPTIONS {{indexConfig: {{`vector.dimensions`: {dims}, `vector.similarity_function`: '{sim}'}}}}"
                    ));
                }
                _ => {
                    statements.push(format!(
                        "CREATE INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON (n.{db_name})"
                    ));
                }
            }
        }

        // Neo4j 5.x Property Type Constraints (Graph Types)
        if include_type_constraints {
            let neo4j_type = field_type_to_neo4j(&field.field_type);
            let t_name = cypher_constraint_name(primary_label, db_name, "type");
            statements.push(format!(
                "CREATE CONSTRAINT {t_name} IF NOT EXISTS FOR (n:{primary_label}) REQUIRE n.{db_name} :: {neo4j_type}"
            ));
        }
    }

    for ft in &node.fulltext_indexes {
        let i_name = if ft.name.is_empty() {
            canonical_fulltext_index_name(primary_label, &ft.properties, DdlDialect::Cypher)
        } else {
            ft.name.clone()
        };
        let props_str = ft
            .properties
            .iter()
            .map(|p| format!("n.{p}"))
            .collect::<Vec<_>>()
            .join(", ");
        if let Some(ref analyzer) = ft.analyzer {
            statements.push(format!(
                "CREATE FULLTEXT INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON EACH [{props_str}] OPTIONS {{indexConfig: {{`fulltext.analyzer`: '{analyzer}'}}}}"
            ));
        } else {
            statements.push(format!(
                "CREATE FULLTEXT INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON EACH [{props_str}]"
            ));
        }
    }

    statements
}

/// Emits openCypher constraint and index statements for a relationship schema.
pub fn emit_cypher_rel_ddl(
    rel: &RelationshipSchema,
    include_type_constraints: bool,
) -> Vec<String> {
    let mut statements = Vec::new();
    let type_name = &rel.type_name;

    for field in rel.fields.values() {
        let db_name = &field.name;

        // Unique / Primary Key constraint. Emitted before the NOT NULL/existence
        // constraint so Neo4j Community still gets uniqueness when the
        // Enterprise-only existence statement is rejected.
        if field.unique || field.primary_key {
            let c_name = cypher_rel_constraint_name(type_name, db_name, "unique");
            statements.push(format!(
                "CREATE CONSTRAINT {c_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() REQUIRE r.{db_name} IS UNIQUE"
            ));
        }

        // Property existence (NOT NULL) constraint
        if !field.nullable {
            let c_name = cypher_rel_constraint_name(type_name, db_name, "not_null");
            statements.push(format!(
                "CREATE CONSTRAINT {c_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() REQUIRE r.{db_name} IS NOT NULL"
            ));
        }

        // Secondary index (only if not already protected by unique constraint)
        if field.indexed && !field.primary_key && !field.unique {
            let i_name = cypher_rel_index_name(type_name, db_name);
            statements.push(format!(
                "CREATE INDEX {i_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() ON (r.{db_name})"
            ));
        }

        if include_type_constraints {
            let neo4j_type = field_type_to_neo4j(&field.field_type);
            let t_name = cypher_rel_constraint_name(type_name, db_name, "type");
            statements.push(format!(
                "CREATE CONSTRAINT {t_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() REQUIRE r.{db_name} :: {neo4j_type}"
            ));
        }
    }

    for ft in &rel.fulltext_indexes {
        let i_name = if ft.name.is_empty() {
            canonical_fulltext_index_name(type_name, &ft.properties, DdlDialect::Cypher)
        } else {
            ft.name.clone()
        };
        let props_str = ft
            .properties
            .iter()
            .map(|p| format!("r.{p}"))
            .collect::<Vec<_>>()
            .join(", ");
        if let Some(ref analyzer) = ft.analyzer {
            statements.push(format!(
                "CREATE FULLTEXT INDEX {i_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() ON EACH [{props_str}] OPTIONS {{indexConfig: {{`fulltext.analyzer`: '{analyzer}'}}}}"
            ));
        } else {
            statements.push(format!(
                "CREATE FULLTEXT INDEX {i_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() ON EACH [{props_str}]"
            ));
        }
    }

    statements
}

/// Emits openCypher DROP statements for a node schema.
pub fn emit_cypher_drop_node_ddl(node: &NodeSchema, include_type_constraints: bool) -> Vec<String> {
    let mut statements = Vec::new();
    let primary_label = node.primary_label();

    for field in node.fields.values() {
        let db_name = &field.name;
        // Mirror creation order: DROP UNIQUE before DROP NOT NULL. Both use
        // IF EXISTS, so dropping a constraint that was never created is safe.
        if field.unique || field.primary_key {
            let c_name = cypher_constraint_name(primary_label, db_name, "unique");
            statements.push(format!("DROP CONSTRAINT {c_name} IF EXISTS"));
        }
        if !field.nullable {
            let c_name = cypher_constraint_name(primary_label, db_name, "not_null");
            statements.push(format!("DROP CONSTRAINT {c_name} IF EXISTS"));
        }
        if field.indexed {
            let i_name = field
                .vector_config
                .as_ref()
                .and_then(|vc| vc.index_name.clone())
                .unwrap_or_else(|| {
                    if field.index_type == Some(IndexType::Vector) {
                        canonical_vector_index_name(primary_label, db_name, DdlDialect::Cypher)
                    } else {
                        cypher_index_name(primary_label, db_name)
                    }
                });
            statements.push(format!("DROP INDEX {i_name} IF EXISTS"));
        }
        if include_type_constraints {
            let t_name = cypher_constraint_name(primary_label, db_name, "type");
            statements.push(format!("DROP CONSTRAINT {t_name} IF EXISTS"));
        }
    }

    for ft in &node.fulltext_indexes {
        let i_name = if ft.name.is_empty() {
            canonical_fulltext_index_name(primary_label, &ft.properties, DdlDialect::Cypher)
        } else {
            ft.name.clone()
        };
        statements.push(format!("DROP INDEX {i_name} IF EXISTS"));
    }

    statements
}

/// Emits openCypher DROP statements for a relationship schema.
pub fn emit_cypher_drop_rel_ddl(
    rel: &RelationshipSchema,
    include_type_constraints: bool,
) -> Vec<String> {
    let mut statements = Vec::new();
    let type_name = &rel.type_name;

    for field in rel.fields.values() {
        let db_name = &field.name;
        // Mirror creation order: DROP UNIQUE before DROP NOT NULL. Both use
        // IF EXISTS, so dropping a constraint that was never created is safe.
        if field.unique || field.primary_key {
            let c_name = cypher_rel_constraint_name(type_name, db_name, "unique");
            statements.push(format!("DROP CONSTRAINT {c_name} IF EXISTS"));
        }
        if !field.nullable {
            let c_name = cypher_rel_constraint_name(type_name, db_name, "not_null");
            statements.push(format!("DROP CONSTRAINT {c_name} IF EXISTS"));
        }
        if field.indexed && !field.primary_key && !field.unique {
            let i_name = cypher_rel_index_name(type_name, db_name);
            statements.push(format!("DROP INDEX {i_name} IF EXISTS"));
        }
        if include_type_constraints {
            let t_name = cypher_rel_constraint_name(type_name, db_name, "type");
            statements.push(format!("DROP CONSTRAINT {t_name} IF EXISTS"));
        }
    }

    for ft in &rel.fulltext_indexes {
        let i_name = if ft.name.is_empty() {
            canonical_fulltext_index_name(type_name, &ft.properties, DdlDialect::Cypher)
        } else {
            ft.name.clone()
        };
        statements.push(format!("DROP INDEX {i_name} IF EXISTS"));
    }

    statements
}

// ---------------------------------------------------------------------------
// Neo4j Cypher 25 Graph Types Emitter (ALTER CURRENT GRAPH TYPE SET)
// ---------------------------------------------------------------------------

/// Emits Neo4j Cypher 25 `ALTER CURRENT GRAPH TYPE SET { ... }` declarative schema statement.
pub fn emit_cypher25_graph_type_ddl(
    nodes: &[&NodeSchema],
    relationships: &[&RelationshipSchema],
) -> String {
    let mut elements = Vec::new();

    for node in nodes {
        let primary_label = node.primary_label();
        let implied: Vec<&str> = node.labels.iter().skip(1).map(|s| s.as_str()).collect();
        let implied_str = if implied.is_empty() {
            String::new()
        } else {
            format!(":{}", implied.join("&"))
        };

        let mut prop_defs = Vec::new();
        for field in node.fields.values() {
            let neo4j_type = field_type_to_neo4j(&field.field_type);
            let constraint_suffix = if field.primary_key {
                " IS KEY".to_string()
            } else {
                let mut s = String::new();
                if !field.nullable {
                    s.push_str(" NOT NULL");
                }
                if field.unique {
                    s.push_str(" IS UNIQUE");
                }
                s
            };
            prop_defs.push(format!("{} :: {neo4j_type}{constraint_suffix}", field.name));
        }
        let props_str = if prop_defs.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", prop_defs.join(", "))
        };

        if implied_str.is_empty() {
            if props_str.is_empty() {
                elements.push(format!("    (:{primary_label})"));
            } else {
                elements.push(format!("    (:{primary_label} =>{props_str})"));
            }
        } else {
            elements.push(format!(
                "    (:{primary_label} => {implied_str}{props_str})"
            ));
        }
    }

    for rel in relationships {
        let type_name = &rel.type_name;
        let mut prop_defs = Vec::new();
        for field in rel.fields.values() {
            let neo4j_type = field_type_to_neo4j(&field.field_type);
            let constraint_suffix = if field.primary_key {
                " IS KEY".to_string()
            } else {
                let mut s = String::new();
                if !field.nullable {
                    s.push_str(" NOT NULL");
                }
                if field.unique {
                    s.push_str(" IS UNIQUE");
                }
                s
            };
            prop_defs.push(format!("{} :: {neo4j_type}{constraint_suffix}", field.name));
        }
        let props_str = if prop_defs.is_empty() {
            String::new()
        } else {
            format!(" {{{}}}", prop_defs.join(", "))
        };

        let src_label = rel.source_labels.first().map(|s| s.as_str());
        let tgt_label = rel.target_labels.first().map(|s| s.as_str());

        let src_pattern = match src_label {
            Some(s) if !s.is_empty() => format!("(:{s})"),
            _ => "()".to_string(),
        };
        let tgt_pattern = match tgt_label {
            Some(s) if !s.is_empty() => format!("(:{s})"),
            _ => "()".to_string(),
        };

        if props_str.is_empty() {
            elements.push(format!("    {src_pattern}-[:{type_name}]->{tgt_pattern}"));
        } else {
            elements.push(format!(
                "    {src_pattern}-[:{type_name} =>{props_str}]->{tgt_pattern}"
            ));
        }
    }

    let body = elements.join(",\n");
    format!("ALTER CURRENT GRAPH TYPE SET {{\n{body}\n}}")
}

/// Emits Neo4j Cypher 25 `ALTER CURRENT GRAPH TYPE SET {}` to reset the graph type.
pub fn emit_cypher25_drop_graph_type_ddl() -> String {
    "ALTER CURRENT GRAPH TYPE SET {}".to_string()
}

// ---------------------------------------------------------------------------
// ISO GQL DDL Emitter
// ---------------------------------------------------------------------------

/// Emits an ISO GQL `CREATE GRAPH TYPE <name> AS { ... }` DDL definition.
pub fn emit_gql_graph_type_ddl(
    graph_type_name: &str,
    nodes: &[&NodeSchema],
    relationships: &[&RelationshipSchema],
) -> String {
    let mut elements = Vec::new();

    for node in nodes {
        let primary_label = node.primary_label();
        let mut prop_defs = Vec::new();
        for field in node.fields.values() {
            let gql_type = field_type_to_gql(&field.field_type);
            let not_null = if !field.nullable || field.primary_key {
                " NOT NULL"
            } else {
                ""
            };
            prop_defs.push(format!("{} {}{not_null}", field.name, gql_type));
        }
        let props_str = if prop_defs.is_empty() {
            String::new()
        } else {
            format!(" ({})", prop_defs.join(", "))
        };
        let key_str = if let Some(ref pk) = node.primary_key {
            format!(" KEY ({pk})")
        } else {
            String::new()
        };
        elements.push(format!("    NODE {primary_label}{props_str}{key_str}"));
    }

    for rel in relationships {
        let type_name = &rel.type_name;
        let mut prop_defs = Vec::new();
        for field in rel.fields.values() {
            let gql_type = field_type_to_gql(&field.field_type);
            let not_null = if !field.nullable || field.primary_key {
                " NOT NULL"
            } else {
                ""
            };
            prop_defs.push(format!("{} {}{not_null}", field.name, gql_type));
        }
        let props_str = if prop_defs.is_empty() {
            String::new()
        } else {
            format!(" ({})", prop_defs.join(", "))
        };

        let src_label = rel.source_labels.first().map(|s| s.as_str());
        let tgt_label = rel.target_labels.first().map(|s| s.as_str());
        let connecting_str = match (src_label, tgt_label) {
            (Some(s), Some(t)) if !s.is_empty() && !t.is_empty() => {
                format!(" CONNECTING ({s} TO {t})")
            }
            _ => String::new(),
        };

        elements.push(format!("    EDGE {type_name}{connecting_str}{props_str}"));
    }

    let body = elements.join(",\n");
    format!("CREATE GRAPH TYPE {graph_type_name} AS {{\n{body}\n}}")
}

/// Emits an experimental `ALTER CURRENT GRAPH TYPE ADD NODE TYPE` DDL statement.
pub fn emit_gql_alter_node_ddl(node: &NodeSchema) -> String {
    let primary_label = node.primary_label();
    let mut prop_defs = Vec::new();

    for field in node.fields.values() {
        let gql_type = field_type_to_gql(&field.field_type);
        let optional_marker = if field.primary_key || field.unique || !field.nullable {
            ""
        } else {
            "?"
        };
        prop_defs.push(format!("{} :: {gql_type}{optional_marker}", field.name));
    }

    let props_str = if prop_defs.is_empty() {
        String::new()
    } else {
        format!(" {{{}}}", prop_defs.join(", "))
    };

    format!("ALTER CURRENT GRAPH TYPE ADD NODE TYPE (:{primary_label}{props_str})")
}

/// Emits an experimental `ALTER CURRENT GRAPH TYPE ADD RELATIONSHIP TYPE` DDL statement.
pub fn emit_gql_alter_rel_ddl(
    rel: &RelationshipSchema,
    source_label: Option<&str>,
    target_label: Option<&str>,
) -> String {
    let type_name = &rel.type_name;
    let mut prop_defs = Vec::new();

    for field in rel.fields.values() {
        let gql_type = field_type_to_gql(&field.field_type);
        prop_defs.push(format!("{} :: {gql_type}", field.name));
    }

    let props_str = if prop_defs.is_empty() {
        String::new()
    } else {
        format!(" {{{}}}", prop_defs.join(", "))
    };

    let src_label = source_label.or_else(|| rel.source_labels.first().map(|s| s.as_str()));
    let tgt_label = target_label.or_else(|| rel.target_labels.first().map(|s| s.as_str()));

    let src_pattern = match src_label {
        Some(s) if !s.is_empty() => format!("(:{s})"),
        _ => "()".to_string(),
    };
    let tgt_pattern = match tgt_label {
        Some(s) if !s.is_empty() => format!("(:{s})"),
        _ => "()".to_string(),
    };

    format!(
        "ALTER CURRENT GRAPH TYPE ADD RELATIONSHIP TYPE {src_pattern}-[:{type_name}{props_str}]->{tgt_pattern}"
    )
}

/// Emits an ISO GQL `DROP GRAPH TYPE` DDL statement.
pub fn emit_gql_drop_graph_type_ddl(graph_type_name: &str) -> String {
    format!("DROP GRAPH TYPE {graph_type_name} IF EXISTS")
}

// ---------------------------------------------------------------------------
// SQL:2023 PGQ DDL Emitter (PostgreSQL / DuckPGQ)
// ---------------------------------------------------------------------------

pub(crate) fn escape_sql_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Parses and validates a dialect string for SQL table DDL generation.
pub fn parse_table_ddl_dialect(dialect: &str) -> Result<DdlDialect> {
    let norm = dialect.trim().to_ascii_lowercase();
    match norm.as_str() {
        "sql" | "postgres" | "postgresql" | "sql_pgq" | "pgq" => Ok(DdlDialect::Sql),
        "duckdb" | "duckpgq" => Ok(DdlDialect::DuckDb),
        _ => Err(Error::UnsupportedDialect {
            dialect: dialect.to_string(),
            feature: "relational table DDL generation (supported: 'postgres', 'duckdb')"
                .to_string(),
        }),
    }
}

/// Maps a canonical `FieldType` to dialect-specific SQL column data type.
///
/// Supported dialects: PostgreSQL (`DdlDialect::Sql`) and DuckDB (`DdlDialect::DuckDb`).
/// For types that cannot be represented in the chosen dialect, returns
/// `Error::UnsupportedFeature` naming the field.
pub fn field_type_to_sql(ft: &FieldType, dialect: DdlDialect, field_name: &str) -> Result<String> {
    let dialect_name = match dialect {
        DdlDialect::DuckDb => "duckdb",
        _ => "postgres",
    };

    match ft {
        FieldType::String => Ok(match dialect {
            DdlDialect::DuckDb => "VARCHAR".to_string(),
            _ => "TEXT".to_string(),
        }),
        FieldType::Int64 => Ok("BIGINT".to_string()),
        FieldType::Float64 => Ok("DOUBLE PRECISION".to_string()),
        FieldType::Boolean => Ok("BOOLEAN".to_string()),
        FieldType::DateTime => Ok(match dialect {
            DdlDialect::DuckDb => "TIMESTAMPTZ".to_string(),
            _ => "TIMESTAMP WITH TIME ZONE".to_string(),
        }),
        FieldType::Date => Ok("DATE".to_string()),
        FieldType::Duration => Ok("INTERVAL".to_string()),
        FieldType::Map => Ok(match dialect {
            DdlDialect::DuckDb => "JSON".to_string(),
            _ => "JSONB".to_string(),
        }),
        FieldType::Any => Ok(match dialect {
            DdlDialect::DuckDb => "VARCHAR".to_string(),
            _ => "TEXT".to_string(),
        }),
        FieldType::List(inner) => match inner.as_ref() {
            FieldType::String => Ok(match dialect {
                DdlDialect::DuckDb => "VARCHAR[]".to_string(),
                _ => "TEXT[]".to_string(),
            }),
            FieldType::Int64 => Ok("BIGINT[]".to_string()),
            FieldType::Float64 => Ok("DOUBLE PRECISION[]".to_string()),
            FieldType::Boolean => Ok("BOOLEAN[]".to_string()),
            FieldType::Date => Ok("DATE[]".to_string()),
            FieldType::DateTime => Ok(match dialect {
                DdlDialect::DuckDb => "TIMESTAMPTZ[]".to_string(),
                _ => "TIMESTAMP WITH TIME ZONE[]".to_string(),
            }),
            FieldType::Any => Ok(match dialect {
                DdlDialect::DuckDb => "VARCHAR[]".to_string(),
                _ => "TEXT[]".to_string(),
            }),
            FieldType::Map | FieldType::List(_) => Ok(match dialect {
                DdlDialect::DuckDb => "JSON".to_string(),
                _ => "JSONB".to_string(),
            }),
            FieldType::Custom(custom) => {
                let inner_sql =
                    field_type_to_sql(&FieldType::parse_str(custom), dialect, field_name)?;
                Ok(format!("{inner_sql}[]"))
            }
            FieldType::Duration => Ok("INTERVAL[]".to_string()),
        },
        FieldType::Custom(s) => {
            let upper = s.trim().to_ascii_uppercase();
            match upper.as_str() {
                "STR" | "STRING" | "TEXT" | "VARCHAR" => Ok(match dialect {
                    DdlDialect::DuckDb => "VARCHAR".to_string(),
                    _ => "TEXT".to_string(),
                }),
                "INT" | "INT64" | "INTEGER" | "BIGINT" | "INT8" => Ok("BIGINT".to_string()),
                "SMALLINT" | "INT2" | "INT4" => Ok("INTEGER".to_string()),
                "FLOAT" | "FLOAT64" | "DOUBLE" | "REAL" | "FLOAT8" => {
                    Ok("DOUBLE PRECISION".to_string())
                }
                "FLOAT4" => Ok("REAL".to_string()),
                "BOOL" | "BOOLEAN" => Ok("BOOLEAN".to_string()),
                "DATETIME" | "TIMESTAMP" | "TIMESTAMPTZ" => Ok(match dialect {
                    DdlDialect::DuckDb => "TIMESTAMPTZ".to_string(),
                    _ => "TIMESTAMP WITH TIME ZONE".to_string(),
                }),
                "DATE" => Ok("DATE".to_string()),
                "TIME" | "TIMETZ" => Ok(match dialect {
                    DdlDialect::DuckDb => "TIME".to_string(),
                    _ => "TIME WITHOUT TIME ZONE".to_string(),
                }),
                "DURATION" | "INTERVAL" => Ok("INTERVAL".to_string()),
                "DECIMAL" | "NUMERIC" => Ok("NUMERIC".to_string()),
                "BYTES" | "BYTEA" | "BLOB" => Ok(match dialect {
                    DdlDialect::DuckDb => "BLOB".to_string(),
                    _ => "BYTEA".to_string(),
                }),
                "JSON" | "JSONB" | "MAP" | "DICT" => Ok(match dialect {
                    DdlDialect::DuckDb => "JSON".to_string(),
                    _ => "JSONB".to_string(),
                }),
                "LIST" | "ARRAY" => Ok(match dialect {
                    DdlDialect::DuckDb => "VARCHAR[]".to_string(),
                    _ => "TEXT[]".to_string(),
                }),
                "POINT" => match dialect {
                    DdlDialect::DuckDb => Err(Error::UnsupportedFeature {
                        dialect: dialect_name.to_string(),
                        feature: format!("column '{field_name}' of type POINT"),
                    }),
                    _ => Ok("POINT".to_string()),
                },
                "VECTOR" => match dialect {
                    DdlDialect::DuckDb => Ok("FLOAT[]".to_string()),
                    _ => Err(Error::UnsupportedFeature {
                        dialect: dialect_name.to_string(),
                        feature: format!(
                            "column '{field_name}' of type VECTOR (requires pgvector extension)"
                        ),
                    }),
                },
                _ => Err(Error::UnsupportedFeature {
                    dialect: dialect_name.to_string(),
                    feature: format!("column '{field_name}' with unsupported type '{s}'"),
                }),
            }
        }
    }
}

/// Returns the SQL:2023 PGQ vertex table name for a node label.
///
/// The table name is the primary label folded to ASCII lower case. That is the
/// same rule the SQL constraint and index emitters use, so the property graph
/// statement, `ALTER TABLE` constraints and `CREATE INDEX` statements target the
/// same table.
pub fn pgq_vertex_table_name(label: &str) -> String {
    label.to_ascii_lowercase()
}

/// Returns the SQL:2023 PGQ edge table name for a relationship type.
///
/// Like [`pgq_vertex_table_name`], the name is the type folded to ASCII lower
/// case, matching the SQL constraint and index emitters.
pub fn pgq_edge_table_name(type_name: &str) -> String {
    type_name.to_ascii_lowercase()
}

/// Returns the source endpoint foreign-key column name for an edge table.
///
/// The role (`src`) plus the endpoint node's primary-key column (`pid`) keeps
/// the source and destination columns distinct even for a self-relationship.
pub fn pgq_edge_source_key(primary_key: &str) -> String {
    format!("src_{primary_key}")
}

/// Returns the destination endpoint foreign-key column name for an edge table.
///
/// See [`pgq_edge_source_key`] for the naming rule.
pub fn pgq_edge_destination_key(primary_key: &str) -> String {
    format!("dst_{primary_key}")
}

/// Returns the sequence name for a DuckDB edge table surrogate key.
pub fn pgq_edge_sequence_name(type_name: &str, surrogate_col: &str) -> String {
    format!("seq_{}_{}", pgq_edge_table_name(type_name), surrogate_col)
}

/// Resolves a non-clashing surrogate key column name for an edge table.
///
/// Defaults to `"id"`. If a field named `"id"` exists in the relationship model,
/// tries `"<edge_table>_id"`, then `"edge_id"`, and finally `"id_{counter}"`,
/// ensuring no collision with declared fields or endpoint foreign-key columns.
pub fn resolve_edge_surrogate_key_name(
    rel: &RelationshipSchema,
    src_key: &str,
    dst_key: &str,
) -> String {
    let edge_table = pgq_edge_table_name(&rel.type_name);
    let is_available =
        |name: &str| !rel.fields.contains_key(name) && name != src_key && name != dst_key;

    if is_available("id") {
        return "id".to_string();
    }
    let candidate_table_id = format!("{edge_table}_id");
    if is_available(&candidate_table_id) {
        return candidate_table_id;
    }
    if is_available("edge_id") {
        return "edge_id".to_string();
    }
    let mut counter = 1;
    loop {
        let candidate_n = format!("id_{counter}");
        if is_available(&candidate_n) {
            return candidate_n;
        }
        counter += 1;
    }
}

/// Emits a `CREATE TABLE IF NOT EXISTS` statement for a single node schema.
pub fn emit_node_table_ddl(node: &NodeSchema, dialect: &str) -> Result<String> {
    let ddl_dialect = parse_table_ddl_dialect(dialect)?;
    let table = pgq_vertex_table_name(node.primary_label());
    let q_table = escape_sql_ident(&table);

    let mut columns = Vec::new();
    for field in node.fields.values() {
        let q_col = escape_sql_ident(&field.name);
        let sql_type = field_type_to_sql(&field.field_type, ddl_dialect, &field.name)?;
        let nullability = if field.primary_key || !field.nullable {
            " NOT NULL"
        } else {
            ""
        };
        columns.push(format!("{q_col} {sql_type}{nullability}"));
    }

    if let Some(ref pk) = node.primary_key {
        columns.push(format!("PRIMARY KEY ({})", escape_sql_ident(pk)));
    }

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {q_table} (\n  {}\n);",
        columns.join(",\n  ")
    ))
}

/// Emits a `CREATE TABLE IF NOT EXISTS` statement for a single relationship schema.
pub fn emit_rel_table_ddl(
    rel: &RelationshipSchema,
    nodes: &[&NodeSchema],
    dialect: &str,
) -> Result<String> {
    let ddl_dialect = parse_table_ddl_dialect(dialect)?;
    let table = pgq_edge_table_name(&rel.type_name);
    let q_table = escape_sql_ident(&table);

    let src_label = rel
        .source_labels
        .first()
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::SchemaError(format!(
                "Relationship '{}' is missing source_labels. SQL table DDL requires explicit vertex source endpoints.",
                rel.name
            ))
        })?;
    let tgt_label = rel
        .target_labels
        .first()
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            Error::SchemaError(format!(
                "Relationship '{}' is missing target_labels. SQL table DDL requires explicit vertex target endpoints.",
                rel.name
            ))
        })?;

    let src_table = pgq_vertex_table_name(src_label);
    let tgt_table = pgq_vertex_table_name(tgt_label);

    let src_node = nodes
        .iter()
        .find(|n| n.labels.iter().any(|l| l == src_label) || n.name == src_label);
    let tgt_node = nodes
        .iter()
        .find(|n| n.labels.iter().any(|l| l == tgt_label) || n.name == tgt_label);

    let src_ref_col = src_node
        .and_then(|n| n.primary_key.as_deref())
        .unwrap_or("id");
    let tgt_ref_col = tgt_node
        .and_then(|n| n.primary_key.as_deref())
        .unwrap_or("id");

    let src_col_type = if let Some(n) = src_node {
        if let Some(f) = n.fields.get(src_ref_col) {
            field_type_to_sql(&f.field_type, ddl_dialect, &f.name)?
        } else {
            "BIGINT".to_string()
        }
    } else {
        "BIGINT".to_string()
    };

    let tgt_col_type = if let Some(n) = tgt_node {
        if let Some(f) = n.fields.get(tgt_ref_col) {
            field_type_to_sql(&f.field_type, ddl_dialect, &f.name)?
        } else {
            "BIGINT".to_string()
        }
    } else {
        "BIGINT".to_string()
    };

    let src_key = pgq_edge_source_key(src_ref_col);
    let tgt_key = pgq_edge_destination_key(tgt_ref_col);

    let q_src_key = escape_sql_ident(&src_key);
    let q_tgt_key = escape_sql_ident(&tgt_key);
    let q_src_table = escape_sql_ident(&src_table);
    let q_tgt_table = escape_sql_ident(&tgt_table);
    let q_src_ref_col = escape_sql_ident(src_ref_col);
    let q_tgt_ref_col = escape_sql_ident(tgt_ref_col);

    let mut columns = Vec::new();

    let explicit_pk = rel.derived_primary_key()?;
    if explicit_pk.is_none() {
        let surrogate_name = resolve_edge_surrogate_key_name(rel, &src_key, &tgt_key);
        let q_surrogate = escape_sql_ident(&surrogate_name);
        let surrogate_col_def = match ddl_dialect {
            DdlDialect::DuckDb => {
                let seq_name = pgq_edge_sequence_name(&rel.type_name, &surrogate_name);
                format!("{q_surrogate} BIGINT DEFAULT nextval('{seq_name}') PRIMARY KEY")
            }
            _ => {
                format!("{q_surrogate} BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY")
            }
        };
        columns.push(surrogate_col_def);
    }

    columns.push(format!(
        "{q_src_key} {src_col_type} NOT NULL REFERENCES {q_src_table} ({q_src_ref_col})"
    ));
    columns.push(format!(
        "{q_tgt_key} {tgt_col_type} NOT NULL REFERENCES {q_tgt_table} ({q_tgt_ref_col})"
    ));

    for field in rel.fields.values() {
        let q_col = escape_sql_ident(&field.name);
        let sql_type = field_type_to_sql(&field.field_type, ddl_dialect, &field.name)?;
        let nullability = if field.primary_key || !field.nullable {
            " NOT NULL"
        } else {
            ""
        };
        columns.push(format!("{q_col} {sql_type}{nullability}"));
    }

    if let Some(ref pk_name) = explicit_pk {
        columns.push(format!("PRIMARY KEY ({})", escape_sql_ident(pk_name)));
    }

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {q_table} (\n  {}\n);",
        columns.join(",\n  ")
    ))
}

/// Emits `CREATE TABLE IF NOT EXISTS` statements for a set of nodes and relationships.
///
/// Node tables are emitted before edge tables so that foreign-key references succeed.
/// For DuckDB, sequences for edge table surrogate keys are emitted before the edge tables.
pub fn emit_table_ddl(
    nodes: &[&NodeSchema],
    relationships: &[&RelationshipSchema],
    dialect: &str,
) -> Result<Vec<String>> {
    let ddl_dialect = parse_table_ddl_dialect(dialect)?;
    let mut statements = Vec::new();
    for node in nodes {
        statements.push(emit_node_table_ddl(node, dialect)?);
    }
    for rel in relationships {
        if matches!(ddl_dialect, DdlDialect::DuckDb) {
            let explicit_pk = rel.derived_primary_key()?;
            if explicit_pk.is_none() {
                let src_label = rel.source_labels.first().map(|s| s.as_str()).unwrap_or("");
                let tgt_label = rel.target_labels.first().map(|s| s.as_str()).unwrap_or("");
                let src_node = nodes
                    .iter()
                    .find(|n| n.labels.iter().any(|l| l == src_label) || n.name == src_label);
                let tgt_node = nodes
                    .iter()
                    .find(|n| n.labels.iter().any(|l| l == tgt_label) || n.name == tgt_label);
                let src_ref_col = src_node
                    .and_then(|n| n.primary_key.as_deref())
                    .unwrap_or("id");
                let tgt_ref_col = tgt_node
                    .and_then(|n| n.primary_key.as_deref())
                    .unwrap_or("id");
                let src_key = pgq_edge_source_key(src_ref_col);
                let tgt_key = pgq_edge_destination_key(tgt_ref_col);
                let surrogate_name = resolve_edge_surrogate_key_name(rel, &src_key, &tgt_key);
                let seq_name = pgq_edge_sequence_name(&rel.type_name, &surrogate_name);
                statements.push(format!("CREATE SEQUENCE IF NOT EXISTS {seq_name};"));
            }
        }
        statements.push(emit_rel_table_ddl(rel, nodes, dialect)?);
    }
    Ok(statements)
}

/// Emits a SQL:2023 PGQ / DuckPGQ `CREATE PROPERTY GRAPH` DDL statement.
///
/// # Endpoints and Key Convention Note
/// SQL:2023 PGQ edge tables require explicit vertex endpoints (`source_labels` and `target_labels`).
/// If any relationship has empty endpoints, this function returns an `Err`.
///
/// Vertex tables are named after the node's primary label in lower case and
/// edge tables after the relationship type in lower case (see
/// [`pgq_vertex_table_name`] and [`pgq_edge_table_name`]). Edge source and
/// destination columns are `src_<node key>` and `dst_<node key>` (see
/// [`pgq_edge_source_key`] and [`pgq_edge_destination_key`]), so a
/// self-relationship gets two distinct columns. All table, label, key and
/// property identifiers are double-quoted to preserve their case on PostgreSQL.
///
/// When an endpoint node is not present in `nodes`, the emitter cannot know its
/// key column and falls back to the `id` key so the statement remains
/// well-formed. Callers that only have a relationship should resolve its
/// endpoint nodes from their registry first.
pub fn emit_pgq_property_graph_ddl(
    graph_name: &str,
    nodes: &[&NodeSchema],
    relationships: &[&RelationshipSchema],
) -> Result<String> {
    emit_pgq_property_graph_ddl_with_dialect(graph_name, nodes, relationships, "postgres")
}

/// Emits a SQL:2023 PGQ / DuckPGQ `CREATE PROPERTY GRAPH` DDL statement with explicit dialect targeting.
pub fn emit_pgq_property_graph_ddl_with_dialect(
    graph_name: &str,
    nodes: &[&NodeSchema],
    relationships: &[&RelationshipSchema],
    dialect: &str,
) -> Result<String> {
    let ddl_dialect = parse_table_ddl_dialect(dialect)?;
    let is_duckdb = matches!(ddl_dialect, DdlDialect::DuckDb);

    let mut v_clauses = Vec::new();
    for node in nodes {
        let primary_label = node.primary_label();
        let table_name = pgq_vertex_table_name(primary_label);
        let q_table = escape_sql_ident(&table_name);
        let q_label = escape_sql_ident(primary_label);

        if is_duckdb {
            // DuckPGQ automatically exposes all columns and rejects KEY and PROPERTIES clauses.
            v_clauses.push(format!("    {q_table} LABEL {q_label}"));
        } else {
            let props_clause = if node.fields.is_empty() {
                String::new()
            } else {
                let field_names: Vec<String> =
                    node.fields.keys().map(|k| escape_sql_ident(k)).collect();
                format!(" PROPERTIES ({})", field_names.join(", "))
            };

            if let Some(ref pk) = node.primary_key {
                v_clauses.push(format!(
                    "    {q_table} KEY ({}) LABEL {q_label}{props_clause}",
                    escape_sql_ident(pk)
                ));
            } else {
                v_clauses.push(format!("    {q_table} LABEL {q_label}{props_clause}"));
            }
        }
    }

    let mut e_clauses = Vec::new();
    for rel in relationships {
        let table_name = pgq_edge_table_name(&rel.type_name);
        let q_table = escape_sql_ident(&table_name);
        let q_label = escape_sql_ident(&rel.type_name);

        let src_label = rel
            .source_labels
            .first()
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::SchemaError(format!(
                    "Relationship '{}' is missing source_labels. SQL:2023 PGQ EDGE TABLES require explicit vertex source endpoints.",
                    rel.name
                ))
            })?;
        let tgt_label = rel
            .target_labels
            .first()
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                Error::SchemaError(format!(
                    "Relationship '{}' is missing target_labels. SQL:2023 PGQ EDGE TABLES require explicit vertex target endpoints.",
                    rel.name
                ))
            })?;

        let src_table = pgq_vertex_table_name(src_label);
        let tgt_table = pgq_vertex_table_name(tgt_label);

        let src_node_pk = nodes
            .iter()
            .find(|n| n.labels.iter().any(|l| l == src_label) || n.name == src_label)
            .and_then(|n| n.primary_key.as_deref());
        let tgt_node_pk = nodes
            .iter()
            .find(|n| n.labels.iter().any(|l| l == tgt_label) || n.name == tgt_label)
            .and_then(|n| n.primary_key.as_deref());

        let src_ref_col = src_node_pk.unwrap_or("id");
        let tgt_ref_col = tgt_node_pk.unwrap_or("id");
        let src_key = pgq_edge_source_key(src_ref_col);
        let tgt_key = pgq_edge_destination_key(tgt_ref_col);

        if is_duckdb {
            // DuckPGQ rejects PROPERTIES clauses on EDGE TABLES.
            e_clauses.push(format!(
                "    {q_table}\n      SOURCE KEY ({}) REFERENCES {} ({})\n      DESTINATION KEY ({}) REFERENCES {} ({})\n      LABEL {q_label}",
                escape_sql_ident(&src_key),
                escape_sql_ident(&src_table),
                escape_sql_ident(src_ref_col),
                escape_sql_ident(&tgt_key),
                escape_sql_ident(&tgt_table),
                escape_sql_ident(tgt_ref_col)
            ));
        } else {
            let props_clause = if rel.fields.is_empty() {
                String::new()
            } else {
                let field_names: Vec<String> =
                    rel.fields.keys().map(|k| escape_sql_ident(k)).collect();
                format!(" PROPERTIES ({})", field_names.join(", "))
            };

            e_clauses.push(format!(
                "    {q_table}\n      SOURCE KEY ({}) REFERENCES {} ({})\n      DESTINATION KEY ({}) REFERENCES {} ({})\n      LABEL {q_label}{props_clause}",
                escape_sql_ident(&src_key),
                escape_sql_ident(&src_table),
                escape_sql_ident(src_ref_col),
                escape_sql_ident(&tgt_key),
                escape_sql_ident(&tgt_table),
                escape_sql_ident(tgt_ref_col)
            ));
        }
    }

    let vertices_block = v_clauses.join(",\n");
    let edges_block = e_clauses.join(",\n");

    let mut ddl = format!("CREATE PROPERTY GRAPH {graph_name}\n");
    if !vertices_block.is_empty() {
        ddl.push_str(&format!("  VERTEX TABLES (\n{vertices_block}\n  )\n"));
    }
    if !edges_block.is_empty() {
        ddl.push_str(&format!("  EDGE TABLES (\n{edges_block}\n  )"));
    }
    ddl.push(';');
    Ok(ddl)
}

/// Emits full SQL/PGQ schema DDL in the order they must be executed:
/// 1. Vertex and edge tables (`CREATE TABLE IF NOT EXISTS`)
/// 2. Unique constraints (`ALTER TABLE ADD CONSTRAINT` or `CREATE UNIQUE INDEX`)
/// 3. Secondary indexes (`CREATE INDEX IF NOT EXISTS`)
/// 4. Property graph definition (`CREATE PROPERTY GRAPH`)
pub fn emit_pgq_schema_ddl(
    graph_name: &str,
    nodes: &[&NodeSchema],
    relationships: &[&RelationshipSchema],
    dialect: &str,
) -> Result<Vec<String>> {
    let mut statements = emit_table_ddl(nodes, relationships, dialect)?;

    for node in nodes {
        let mut node_constraints = emit_node_constraint_ddl(node, dialect, false)?;
        statements.append(&mut node_constraints);
    }
    for rel in relationships {
        let mut rel_constraints = emit_rel_constraint_ddl(rel, dialect, false)?;
        statements.append(&mut rel_constraints);
    }

    for node in nodes {
        let mut node_indexes = emit_node_index_ddl(node, dialect)?;
        statements.append(&mut node_indexes);
    }
    for rel in relationships {
        let mut rel_indexes = emit_rel_index_ddl(rel, dialect)?;
        statements.append(&mut rel_indexes);
    }

    let pg_ddl =
        emit_pgq_property_graph_ddl_with_dialect(graph_name, nodes, relationships, dialect)?;
    statements.push(pg_ddl);

    Ok(statements)
}

/// Emits a SQL:2023 PGQ / DuckPGQ `DROP PROPERTY GRAPH` DDL statement.
pub fn emit_pgq_drop_property_graph_ddl(graph_name: &str) -> String {
    format!("DROP PROPERTY GRAPH IF EXISTS {graph_name};")
}

// ---------------------------------------------------------------------------
// Multi-Dialect Pure Index & Constraint DDL Emitters
// ---------------------------------------------------------------------------

/// DDL operation kind for capability checking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlOp {
    /// CREATE INDEX statement generation.
    CreateIndex,
    /// CREATE CONSTRAINT statement generation.
    CreateConstraint,
    /// DROP INDEX statement generation.
    DropIndex,
    /// DROP CONSTRAINT statement generation.
    DropConstraint,
}

impl DdlOp {
    /// Returns canonical DDL query name.
    pub fn name(&self) -> &'static str {
        match self {
            Self::CreateIndex => "CREATE INDEX",
            Self::CreateConstraint => "CREATE CONSTRAINT",
            Self::DropIndex => "DROP INDEX",
            Self::DropConstraint => "DROP CONSTRAINT",
        }
    }
}

pub use crate::schema::DdlDialect;

/// Checks dialect compatibility for a given DDL operation, returning the dialect family.
pub fn ensure_ddl_supported(dialect: &str, op: DdlOp) -> Result<DdlDialect> {
    let norm = dialect.to_ascii_lowercase();
    if norm == "memgraph" {
        return Ok(DdlDialect::Memgraph);
    }
    if norm == "duckdb" || norm == "duckpgq" {
        return Ok(DdlDialect::DuckDb);
    }
    if norm == "age" || norm == "apache_age" {
        if matches!(op, DdlOp::CreateIndex | DdlOp::DropIndex) {
            return Ok(DdlDialect::Age);
        }
        return Err(Error::UnsupportedDialect {
            dialect: "Apache AGE".to_string(),
            feature: format!(
                "Cypher '{}' queries. Indexes and constraints in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. CREATE INDEX ON \"<graph>\".\"<Label>\" USING gin (properties)).",
                op.name()
            ),
        });
    }

    if norm == "falkordb" || norm == "falkor" {
        if matches!(op, DdlOp::CreateConstraint | DdlOp::DropConstraint) {
            return Err(Error::UnsupportedDialect {
                dialect: "FalkorDB".to_string(),
                feature: "openCypher constraint queries in GRAPH.QUERY. Constraints in FalkorDB must be managed via native Redis commands ('GRAPH.CONSTRAINT CREATE/DROP <graph_name> UNIQUE NODE <label> PROPERTIES 1 <prop>').".to_string(),
            });
        }
        return Ok(DdlDialect::FalkorDb);
    }

    if matches!(
        norm.as_str(),
        "sql" | "postgres" | "postgresql" | "sql_pgq" | "pgq"
    ) {
        return Ok(DdlDialect::Sql);
    }

    Ok(DdlDialect::Cypher)
}

/// Predicate identifying whether a field requires a pure index (excluding unique/primary_key).
#[inline]
pub fn is_pure_index_field(field: &FieldDescriptor) -> bool {
    field.indexed && !field.unique && !field.primary_key
}

/// Emits CREATE INDEX DDL statements for a single node across dialects, using default graph targeting.
pub fn emit_node_index_ddl(node: &NodeSchema, dialect: &str) -> Result<Vec<String>> {
    emit_node_index_ddl_with_graph(node, dialect, None)
}

/// Emits CREATE INDEX DDL statements for a single node across dialects with an optional graph name.
pub fn emit_node_index_ddl_with_graph(
    node: &NodeSchema,
    dialect: &str,
    graph_name: Option<&str>,
) -> Result<Vec<String>> {
    let ddl_dialect = ensure_ddl_supported(dialect, DdlOp::CreateIndex)?;
    let mut statements = Vec::new();
    let primary_label = node.primary_label();

    match ddl_dialect {
        DdlDialect::Memgraph => {
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    if field.index_type == Some(IndexType::Vector) {
                        let (dims, metric, custom_name) = if let Some(ref vc) = field.vector_config
                        {
                            (
                                vc.dimensions,
                                vc.similarity.metric_for(ddl_dialect),
                                vc.index_name.clone(),
                            )
                        } else {
                            (
                                DEFAULT_VECTOR_DIMENSIONS,
                                VectorSimilarity::Cosine.metric_for(ddl_dialect),
                                None,
                            )
                        };
                        let idx_name = custom_name.unwrap_or_else(|| {
                            canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                        });
                        statements.push(format!(
                            "CREATE VECTOR INDEX {idx_name} ON :{primary_label}({}) WITH CONFIG {{\"dimension\": {dims}, \"capacity\": {DEFAULT_MEMGRAPH_VECTOR_CAPACITY}, \"metric\": \"{metric}\"}};",
                            field.name
                        ));
                    } else {
                        statements
                            .push(format!("CREATE INDEX ON :{primary_label}({});", field.name));
                    }
                }
            }
            for ft in &node.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(primary_label, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                let props_str = ft.properties.join(", ");
                statements.push(format!(
                    "CREATE TEXT INDEX {idx_name} ON :{primary_label}({props_str});"
                ));
            }
        }
        DdlDialect::DuckDb => {
            let table = pgq_vertex_table_name(primary_label);
            let q_table = escape_sql_ident(&table);
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    let q_col = escape_sql_ident(&field.name);
                    if field.index_type == Some(IndexType::Vector) {
                        let (metric, custom_name) = if let Some(ref vc) = field.vector_config {
                            (vc.similarity.metric_for(ddl_dialect), vc.index_name.clone())
                        } else {
                            (VectorSimilarity::Cosine.metric_for(ddl_dialect), None)
                        };
                        let idx_name = custom_name.unwrap_or_else(|| {
                            canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                        });
                        statements.push(format!(
                            "CREATE INDEX IF NOT EXISTS {idx_name} ON {q_table} USING HNSW ({q_col}) WITH (metric = '{metric}');"
                        ));
                    } else {
                        let idx_name = format!("idx_{table}_{}", field.name);
                        statements.push(format!(
                            "CREATE INDEX IF NOT EXISTS {idx_name} ON {q_table} ({q_col});"
                        ));
                    }
                }
            }
            for ft in &node.fulltext_indexes {
                let pk = node.primary_key.as_deref().unwrap_or("id");
                let props_str = ft
                    .properties
                    .iter()
                    .map(|p| format!("'{p}'"))
                    .collect::<Vec<_>>()
                    .join(", ");
                statements.push(format!(
                    "PRAGMA create_fts_index('{table}', '{pk}', {props_str});"
                ));
            }
        }
        DdlDialect::FalkorDb => {
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    if field.index_type == Some(IndexType::Vector) {
                        let (dims, sim) = if let Some(ref vc) = field.vector_config {
                            (vc.dimensions, vc.similarity.metric_for(ddl_dialect))
                        } else {
                            (
                                DEFAULT_VECTOR_DIMENSIONS,
                                VectorSimilarity::Cosine.metric_for(ddl_dialect),
                            )
                        };
                        statements.push(format!(
                            "CREATE VECTOR INDEX FOR (n:{primary_label}) ON (n.{}) OPTIONS {{dimension: {dims}, similarityFunction: '{sim}'}}",
                            field.name
                        ));
                    } else {
                        statements.push(format!(
                            "CREATE INDEX FOR (n:{primary_label}) ON (n.{})",
                            field.name
                        ));
                    }
                }
            }
            for ft in &node.fulltext_indexes {
                let props_str = ft
                    .properties
                    .iter()
                    .map(|p| format!("n.{p}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                statements.push(format!(
                    "CREATE FULLTEXT INDEX FOR (n:{primary_label}) ON ({props_str})"
                ));
            }
        }
        DdlDialect::Age => {
            let target_graph = graph_name.unwrap_or(DEFAULT_AGE_GRAPH);
            if !is_valid_age_graph_name(target_graph) {
                return Err(Error::EmissionError(format!(
                    "Invalid Apache AGE graph name '{target_graph}': must be 1-63 alphanumeric/underscore characters starting with a letter or underscore"
                )));
            }
            let has_vector = node
                .fields
                .values()
                .any(|f| f.index_type == Some(IndexType::Vector));
            let has_fulltext = !node.fulltext_indexes.is_empty();
            if !has_vector && !has_fulltext {
                return Err(Error::UnsupportedDialect {
                    dialect: "Apache AGE".to_string(),
                    feature: format!(
                        "Cypher 'CREATE INDEX' queries. Indexes and constraints in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. CREATE INDEX ON \"{target_graph}\".\"{primary_label}\" USING gin (properties))."
                    ),
                });
            }
            for field in node.fields.values() {
                if is_pure_index_field(field) && field.index_type == Some(IndexType::Vector) {
                    let (opclass, custom_name) = if let Some(ref vc) = field.vector_config {
                        (vc.similarity.metric_for(ddl_dialect), vc.index_name.clone())
                    } else {
                        (VectorSimilarity::Cosine.metric_for(ddl_dialect), None)
                    };
                    let idx_name = custom_name.unwrap_or_else(|| {
                        canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                    });
                    statements.push(format!(
                        "CREATE INDEX IF NOT EXISTS {idx_name} ON \"{target_graph}\".\"{primary_label}\" USING hnsw ({} {opclass});",
                        field.name
                    ));
                }
            }
            for ft in &node.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(primary_label, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                let analyzer = match ft.analyzer.as_deref() {
                    Some("standard") | None | Some("") => "english",
                    Some(other) => other,
                };
                let expr = ft
                    .properties
                    .iter()
                    .map(|p| format!("(properties->>'{p}'::text)"))
                    .collect::<Vec<_>>()
                    .join(" || ' ' || ");
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {idx_name} ON \"{target_graph}\".\"{primary_label}\" USING gin (to_tsvector('{analyzer}', {expr}));"
                ));
            }
        }
        DdlDialect::Sql => {
            let table = pgq_vertex_table_name(primary_label);
            let q_table = escape_sql_ident(&table);
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    let q_col = escape_sql_ident(&field.name);
                    if field.index_type == Some(IndexType::Vector) {
                        let (opclass, custom_name) = if let Some(ref vc) = field.vector_config {
                            (vc.similarity.metric_for(ddl_dialect), vc.index_name.clone())
                        } else {
                            (VectorSimilarity::Cosine.metric_for(ddl_dialect), None)
                        };
                        let idx_name = custom_name.unwrap_or_else(|| {
                            canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                        });
                        statements.push(format!(
                            "CREATE INDEX IF NOT EXISTS {idx_name} ON {q_table} USING hnsw ({q_col} {opclass});"
                        ));
                    } else {
                        let idx_name = format!("idx_{table}_{}", field.name);
                        statements.push(format!(
                            "CREATE INDEX IF NOT EXISTS {idx_name} ON {q_table} ({q_col});"
                        ));
                    }
                }
            }
            for ft in &node.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(primary_label, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                let analyzer = match ft.analyzer.as_deref() {
                    Some("standard") | None | Some("") => "english",
                    Some(other) => other,
                };
                let expr = ft
                    .properties
                    .iter()
                    .map(|p| escape_sql_ident(p))
                    .collect::<Vec<_>>()
                    .join(" || ' ' || ");
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {idx_name} ON {q_table} USING gin (to_tsvector('{analyzer}', {expr}));"
                ));
            }
        }
        DdlDialect::Cypher => {
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    let i_name = field
                        .vector_config
                        .as_ref()
                        .and_then(|vc| vc.index_name.clone())
                        .unwrap_or_else(|| {
                            if field.index_type == Some(IndexType::Vector) {
                                canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                            } else {
                                cypher_index_name(primary_label, &field.name)
                            }
                        });
                    match field.index_type {
                        Some(IndexType::Text) => {
                            statements.push(format!(
                                "CREATE TEXT INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON (n.{})",
                                field.name
                            ));
                        }
                        Some(IndexType::Point) => {
                            statements.push(format!(
                                "CREATE POINT INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON (n.{})",
                                field.name
                            ));
                        }
                        Some(IndexType::Vector) => {
                            let (dims, sim) = if let Some(ref vc) = field.vector_config {
                                (vc.dimensions, vc.similarity.metric_for(ddl_dialect))
                            } else {
                                (
                                    DEFAULT_VECTOR_DIMENSIONS,
                                    VectorSimilarity::Cosine.metric_for(ddl_dialect),
                                )
                            };
                            statements.push(format!(
                                "CREATE VECTOR INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON (n.{}) OPTIONS {{indexConfig: {{`vector.dimensions`: {dims}, `vector.similarity_function`: '{sim}'}}}}",
                                field.name
                            ));
                        }
                        _ => {
                            statements.push(format!(
                                "CREATE INDEX {i_name} IF NOT EXISTS FOR (n:{primary_label}) ON (n.{})",
                                field.name
                            ));
                        }
                    }
                }
            }
            for ft in &node.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(primary_label, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                let props_str = ft
                    .properties
                    .iter()
                    .map(|p| format!("n.{p}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                if let Some(ref analyzer) = ft.analyzer {
                    statements.push(format!(
                        "CREATE FULLTEXT INDEX {idx_name} IF NOT EXISTS FOR (n:{primary_label}) ON EACH [{props_str}] OPTIONS {{indexConfig: {{`fulltext.analyzer`: '{analyzer}'}}}}"
                    ));
                } else {
                    statements.push(format!(
                        "CREATE FULLTEXT INDEX {idx_name} IF NOT EXISTS FOR (n:{primary_label}) ON EACH [{props_str}]"
                    ));
                }
            }
        }
    }
    Ok(statements)
}

/// Emits CREATE INDEX DDL statements for a single relationship across dialects, using default graph targeting.
pub fn emit_rel_index_ddl(rel: &RelationshipSchema, dialect: &str) -> Result<Vec<String>> {
    emit_rel_index_ddl_with_graph(rel, dialect, None)
}

/// Emits CREATE INDEX DDL statements for a single relationship across dialects with an optional graph name.
pub fn emit_rel_index_ddl_with_graph(
    rel: &RelationshipSchema,
    dialect: &str,
    graph_name: Option<&str>,
) -> Result<Vec<String>> {
    let ddl_dialect = ensure_ddl_supported(dialect, DdlOp::CreateIndex)?;
    let mut statements = Vec::new();
    let type_name = &rel.type_name;

    match ddl_dialect {
        DdlDialect::Memgraph => {
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    statements.push(format!(
                        "CREATE EDGE INDEX ON :{type_name}({});",
                        field.name
                    ));
                }
            }
            for ft in &rel.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(type_name, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                let props_str = ft.properties.join(", ");
                statements.push(format!(
                    "CREATE TEXT EDGE INDEX {idx_name} ON :{type_name}({props_str});"
                ));
            }
        }
        DdlDialect::DuckDb => {
            let table = pgq_edge_table_name(type_name);
            let q_table = escape_sql_ident(&table);
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    let q_col = escape_sql_ident(&field.name);
                    statements.push(format!(
                        "CREATE INDEX IF NOT EXISTS idx_{table}_{} ON {q_table} ({q_col});",
                        field.name
                    ));
                }
            }
            for ft in &rel.fulltext_indexes {
                let props_str = ft
                    .properties
                    .iter()
                    .map(|p| format!("'{p}'"))
                    .collect::<Vec<_>>()
                    .join(", ");
                statements.push(format!(
                    "PRAGMA create_fts_index('{table}', 'id', {props_str});"
                ));
            }
        }
        DdlDialect::FalkorDb => {
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    statements.push(format!(
                        "CREATE INDEX FOR ()-[r:{type_name}]-() ON (r.{})",
                        field.name
                    ));
                }
            }
            for ft in &rel.fulltext_indexes {
                let props_str = ft
                    .properties
                    .iter()
                    .map(|p| format!("r.{p}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                statements.push(format!(
                    "CREATE FULLTEXT INDEX FOR ()-[r:{type_name}]-() ON ({props_str})"
                ));
            }
        }
        DdlDialect::Age => {
            let target_graph = graph_name.unwrap_or(DEFAULT_AGE_GRAPH);
            if !is_valid_age_graph_name(target_graph) {
                return Err(Error::EmissionError(format!(
                    "Invalid Apache AGE graph name '{target_graph}': must be 1-63 alphanumeric/underscore characters starting with a letter or underscore"
                )));
            }
            return Err(Error::UnsupportedDialect {
                dialect: "Apache AGE".to_string(),
                feature: format!(
                    "Cypher 'CREATE INDEX' queries on relationships. Indexes in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. CREATE INDEX ON \"{target_graph}\".\"{type_name}\" ...)."
                ),
            });
        }
        DdlDialect::Sql => {
            let table = pgq_edge_table_name(type_name);
            let q_table = escape_sql_ident(&table);
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    let q_col = escape_sql_ident(&field.name);
                    statements.push(format!(
                        "CREATE INDEX IF NOT EXISTS idx_{table}_{} ON {q_table} ({q_col});",
                        field.name
                    ));
                }
            }
            for ft in &rel.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(type_name, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                let analyzer = match ft.analyzer.as_deref() {
                    Some("standard") | None | Some("") => "english",
                    Some(other) => other,
                };
                let expr = ft
                    .properties
                    .iter()
                    .map(|p| escape_sql_ident(p))
                    .collect::<Vec<_>>()
                    .join(" || ' ' || ");
                statements.push(format!(
                    "CREATE INDEX IF NOT EXISTS {idx_name} ON {q_table} USING gin (to_tsvector('{analyzer}', {expr}));"
                ));
            }
        }
        DdlDialect::Cypher => {
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    let i_name = cypher_rel_index_name(type_name, &field.name);
                    statements.push(format!(
                        "CREATE INDEX {i_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() ON (r.{})",
                        field.name
                    ));
                }
            }
            for ft in &rel.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(type_name, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                let props_str = ft
                    .properties
                    .iter()
                    .map(|p| format!("r.{p}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                if let Some(ref analyzer) = ft.analyzer {
                    statements.push(format!(
                        "CREATE FULLTEXT INDEX {idx_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() ON EACH [{props_str}] OPTIONS {{indexConfig: {{`fulltext.analyzer`: '{analyzer}'}}}}"
                    ));
                } else {
                    statements.push(format!(
                        "CREATE FULLTEXT INDEX {idx_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() ON EACH [{props_str}]"
                    ));
                }
            }
        }
    }
    Ok(statements)
}

/// Emits CREATE CONSTRAINT DDL statements for a single node across dialects.
pub fn emit_node_constraint_ddl(
    node: &NodeSchema,
    dialect: &str,
    include_type_constraints: bool,
) -> Result<Vec<String>> {
    emit_node_constraint_ddl_with_graph(node, dialect, include_type_constraints, None)
}

/// Emits CREATE CONSTRAINT DDL statements for a single node across dialects with an optional graph name.
pub fn emit_node_constraint_ddl_with_graph(
    node: &NodeSchema,
    dialect: &str,
    include_type_constraints: bool,
    graph_name: Option<&str>,
) -> Result<Vec<String>> {
    let ddl_dialect = ensure_ddl_supported(dialect, DdlOp::CreateConstraint)?;
    let mut statements = Vec::new();
    let primary_label = node.primary_label();

    match ddl_dialect {
        DdlDialect::FalkorDb => {
            return Err(Error::UnsupportedDialect {
                dialect: dialect.to_string(),
                feature: "constraint DDL queries".to_string(),
            });
        }
        DdlDialect::Age => {
            let target_graph = graph_name.unwrap_or(DEFAULT_AGE_GRAPH);
            if !is_valid_age_graph_name(target_graph) {
                return Err(Error::EmissionError(format!(
                    "Invalid Apache AGE graph name '{target_graph}': must be 1-63 alphanumeric/underscore characters starting with a letter or underscore"
                )));
            }
            return Err(Error::UnsupportedDialect {
                dialect: "Apache AGE".to_string(),
                feature: format!(
                    "Cypher 'CREATE CONSTRAINT' queries. Indexes and constraints in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. ALTER TABLE \"{target_graph}\".\"{primary_label}\" ADD CONSTRAINT ...)."
                ),
            });
        }
        DdlDialect::Memgraph => {
            for field in node.fields.values() {
                let db_name = &field.name;
                // Mirror the Cypher ordering: uniqueness before existence.
                if field.unique || field.primary_key {
                    statements.push(format!(
                        "CREATE CONSTRAINT ON (n:{primary_label}) ASSERT n.{db_name} IS UNIQUE;"
                    ));
                }
                if !field.nullable {
                    statements.push(format!(
                        "CREATE CONSTRAINT ON (n:{primary_label}) ASSERT EXISTS (n.{db_name});"
                    ));
                }
            }
        }
        DdlDialect::Sql => {
            let table = pgq_vertex_table_name(primary_label);
            let q_table = escape_sql_ident(&table);
            for field in node.fields.values() {
                if field.unique && !field.primary_key {
                    let q_col = escape_sql_ident(&field.name);
                    statements.push(format!(
                        "ALTER TABLE {q_table} ADD CONSTRAINT uq_{table}_{} UNIQUE ({q_col});",
                        field.name
                    ));
                }
            }
        }
        DdlDialect::DuckDb => {
            let table = pgq_vertex_table_name(primary_label);
            let q_table = escape_sql_ident(&table);
            for field in node.fields.values() {
                if field.unique && !field.primary_key {
                    let q_col = escape_sql_ident(&field.name);
                    statements.push(format!(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_{} ON {q_table} ({q_col});",
                        field.name
                    ));
                }
            }
        }
        DdlDialect::Cypher => {
            for field in node.fields.values() {
                let db_name = &field.name;
                // Emit UNIQUE before NOT NULL so Community still gets uniqueness
                // when the Enterprise-only existence constraint is rejected.
                if field.unique || field.primary_key {
                    let c_name = cypher_constraint_name(primary_label, db_name, "unique");
                    statements.push(format!(
                        "CREATE CONSTRAINT {c_name} IF NOT EXISTS FOR (n:{primary_label}) REQUIRE n.{db_name} IS UNIQUE"
                    ));
                }
                if !field.nullable {
                    let c_name = cypher_constraint_name(primary_label, db_name, "not_null");
                    statements.push(format!(
                        "CREATE CONSTRAINT {c_name} IF NOT EXISTS FOR (n:{primary_label}) REQUIRE n.{db_name} IS NOT NULL"
                    ));
                }
                if include_type_constraints {
                    let neo4j_type = field_type_to_neo4j(&field.field_type);
                    let t_name = cypher_constraint_name(primary_label, db_name, "type");
                    statements.push(format!(
                        "CREATE CONSTRAINT {t_name} IF NOT EXISTS FOR (n:{primary_label}) REQUIRE n.{db_name} :: {neo4j_type}"
                    ));
                }
            }
        }
    }
    Ok(statements)
}

/// Emits CREATE CONSTRAINT DDL statements for a single relationship across dialects.
pub fn emit_rel_constraint_ddl(
    rel: &RelationshipSchema,
    dialect: &str,
    include_type_constraints: bool,
) -> Result<Vec<String>> {
    emit_rel_constraint_ddl_with_graph(rel, dialect, include_type_constraints, None)
}

/// Emits CREATE CONSTRAINT DDL statements for a single relationship across dialects with an optional graph name.
pub fn emit_rel_constraint_ddl_with_graph(
    rel: &RelationshipSchema,
    dialect: &str,
    include_type_constraints: bool,
    graph_name: Option<&str>,
) -> Result<Vec<String>> {
    let ddl_dialect = ensure_ddl_supported(dialect, DdlOp::CreateConstraint)?;
    let mut statements = Vec::new();
    let type_name = &rel.type_name;

    match ddl_dialect {
        DdlDialect::FalkorDb => {
            return Err(Error::UnsupportedDialect {
                dialect: dialect.to_string(),
                feature: "constraint DDL queries".to_string(),
            });
        }
        DdlDialect::Age => {
            let target_graph = graph_name.unwrap_or(DEFAULT_AGE_GRAPH);
            if !is_valid_age_graph_name(target_graph) {
                return Err(Error::EmissionError(format!(
                    "Invalid Apache AGE graph name '{target_graph}': must be 1-63 alphanumeric/underscore characters starting with a letter or underscore"
                )));
            }
            return Err(Error::UnsupportedDialect {
                dialect: "Apache AGE".to_string(),
                feature: format!(
                    "Cypher 'CREATE CONSTRAINT' queries on relationships. Constraints in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. ALTER TABLE \"{target_graph}\".\"{type_name}\" ADD CONSTRAINT ...)."
                ),
            });
        }
        DdlDialect::Memgraph => {
            // Memgraph does not support edge constraints; no-op.
        }
        DdlDialect::Sql => {
            let table = pgq_edge_table_name(type_name);
            let q_table = escape_sql_ident(&table);
            for field in rel.fields.values() {
                if field.unique && !field.primary_key {
                    let q_col = escape_sql_ident(&field.name);
                    statements.push(format!(
                        "ALTER TABLE {q_table} ADD CONSTRAINT uq_{table}_{} UNIQUE ({q_col});",
                        field.name
                    ));
                }
            }
        }
        DdlDialect::DuckDb => {
            let table = pgq_edge_table_name(type_name);
            let q_table = escape_sql_ident(&table);
            for field in rel.fields.values() {
                if field.unique && !field.primary_key {
                    let q_col = escape_sql_ident(&field.name);
                    statements.push(format!(
                        "CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_{} ON {q_table} ({q_col});",
                        field.name
                    ));
                }
            }
        }
        DdlDialect::Cypher => {
            for field in rel.fields.values() {
                let db_name = &field.name;
                // Emit UNIQUE before NOT NULL so Community still gets uniqueness
                // when the Enterprise-only existence constraint is rejected.
                if field.unique || field.primary_key {
                    let c_name = cypher_rel_constraint_name(type_name, db_name, "unique");
                    statements.push(format!(
                        "CREATE CONSTRAINT {c_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() REQUIRE r.{db_name} IS UNIQUE"
                    ));
                }
                if !field.nullable {
                    let c_name = cypher_rel_constraint_name(type_name, db_name, "not_null");
                    statements.push(format!(
                        "CREATE CONSTRAINT {c_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() REQUIRE r.{db_name} IS NOT NULL"
                    ));
                }
                if include_type_constraints {
                    let neo4j_type = field_type_to_neo4j(&field.field_type);
                    let t_name = cypher_rel_constraint_name(type_name, db_name, "type");
                    statements.push(format!(
                        "CREATE CONSTRAINT {t_name} IF NOT EXISTS FOR ()-[r:{type_name}]-() REQUIRE r.{db_name} :: {neo4j_type}"
                    ));
                }
            }
        }
    }
    Ok(statements)
}

/// Emits DROP INDEX DDL statements for a single node across dialects.
pub fn emit_node_drop_index_ddl(node: &NodeSchema, dialect: &str) -> Result<Vec<String>> {
    emit_node_drop_index_ddl_with_graph(node, dialect, None)
}

/// Emits DROP INDEX DDL statements for a single node across dialects with an optional graph name.
pub fn emit_node_drop_index_ddl_with_graph(
    node: &NodeSchema,
    dialect: &str,
    graph_name: Option<&str>,
) -> Result<Vec<String>> {
    let ddl_dialect = ensure_ddl_supported(dialect, DdlOp::DropIndex)?;
    let mut statements = Vec::new();
    let primary_label = node.primary_label();

    match ddl_dialect {
        DdlDialect::Memgraph => {
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    if field.index_type == Some(IndexType::Vector) {
                        let idx_name = field
                            .vector_config
                            .as_ref()
                            .and_then(|vc| vc.index_name.clone())
                            .unwrap_or_else(|| {
                                canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                            });
                        statements.push(format!("DROP VECTOR INDEX {idx_name};"));
                    } else {
                        statements.push(format!("DROP INDEX ON :{primary_label}({});", field.name));
                    }
                }
            }
            for ft in &node.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(primary_label, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                statements.push(format!("DROP TEXT INDEX {idx_name};"));
            }
        }
        DdlDialect::DuckDb => {
            let table = pgq_vertex_table_name(primary_label);
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    let idx_name = field
                        .vector_config
                        .as_ref()
                        .and_then(|vc| vc.index_name.clone())
                        .unwrap_or_else(|| {
                            if field.index_type == Some(IndexType::Vector) {
                                canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                            } else {
                                format!("idx_{table}_{}", field.name)
                            }
                        });
                    statements.push(format!("DROP INDEX IF EXISTS {idx_name};"));
                }
            }
            for _ in &node.fulltext_indexes {
                statements.push(format!("PRAGMA drop_fts_index('{table}');"));
            }
        }
        DdlDialect::FalkorDb => {
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    if field.index_type == Some(IndexType::Vector) {
                        statements.push(format!(
                            "DROP VECTOR INDEX FOR (n:{primary_label}) ON (n.{})",
                            field.name
                        ));
                    } else {
                        statements.push(format!(
                            "DROP INDEX FOR (n:{primary_label}) ON (n.{})",
                            field.name
                        ));
                    }
                }
            }
            for ft in &node.fulltext_indexes {
                let props_str = ft
                    .properties
                    .iter()
                    .map(|p| format!("n.{p}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                statements.push(format!(
                    "DROP FULLTEXT INDEX FOR (n:{primary_label}) ON ({props_str})"
                ));
            }
        }
        DdlDialect::Age => {
            let target_graph = graph_name.unwrap_or(DEFAULT_AGE_GRAPH);
            if !is_valid_age_graph_name(target_graph) {
                return Err(Error::EmissionError(format!(
                    "Invalid Apache AGE graph name '{target_graph}': must be 1-63 alphanumeric/underscore characters starting with a letter or underscore"
                )));
            }
            let has_vector = node
                .fields
                .values()
                .any(|f| f.index_type == Some(IndexType::Vector));
            let has_fulltext = !node.fulltext_indexes.is_empty();
            if !has_vector && !has_fulltext {
                return Err(Error::UnsupportedDialect {
                    dialect: "Apache AGE".to_string(),
                    feature: format!(
                        "Cypher 'DROP INDEX' queries. Indexes and constraints in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. DROP INDEX ON \"{target_graph}\".\"{primary_label}\" or DROP INDEX \"{target_graph}\".<index_name>)."
                    ),
                });
            }
            for field in node.fields.values() {
                if is_pure_index_field(field) && field.index_type == Some(IndexType::Vector) {
                    let idx_name = field
                        .vector_config
                        .as_ref()
                        .and_then(|vc| vc.index_name.clone())
                        .unwrap_or_else(|| {
                            canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                        });
                    statements.push(format!(
                        "DROP INDEX IF EXISTS \"{target_graph}\".{idx_name};"
                    ));
                }
            }
            for ft in &node.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(primary_label, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                statements.push(format!(
                    "DROP INDEX IF EXISTS \"{target_graph}\".{idx_name};"
                ));
            }
        }
        DdlDialect::Sql => {
            let table = pgq_vertex_table_name(primary_label);
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    let idx_name = field
                        .vector_config
                        .as_ref()
                        .and_then(|vc| vc.index_name.clone())
                        .unwrap_or_else(|| {
                            if field.index_type == Some(IndexType::Vector) {
                                canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                            } else {
                                format!("idx_{table}_{}", field.name)
                            }
                        });
                    statements.push(format!("DROP INDEX IF EXISTS {idx_name};"));
                }
            }
            for ft in &node.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(primary_label, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                statements.push(format!("DROP INDEX IF EXISTS {idx_name};"));
            }
        }
        DdlDialect::Cypher => {
            for field in node.fields.values() {
                if is_pure_index_field(field) {
                    let i_name = field
                        .vector_config
                        .as_ref()
                        .and_then(|vc| vc.index_name.clone())
                        .unwrap_or_else(|| {
                            if field.index_type == Some(IndexType::Vector) {
                                canonical_vector_index_name(primary_label, &field.name, ddl_dialect)
                            } else {
                                cypher_index_name(primary_label, &field.name)
                            }
                        });
                    statements.push(format!("DROP INDEX {i_name} IF EXISTS"));
                }
            }
            for ft in &node.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(primary_label, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                statements.push(format!("DROP INDEX {idx_name} IF EXISTS"));
            }
        }
    }
    Ok(statements)
}

/// Emits DROP INDEX DDL statements for a single relationship across dialects.
pub fn emit_rel_drop_index_ddl(rel: &RelationshipSchema, dialect: &str) -> Result<Vec<String>> {
    emit_rel_drop_index_ddl_with_graph(rel, dialect, None)
}

/// Emits DROP INDEX DDL statements for a single relationship across dialects with an optional graph name.
pub fn emit_rel_drop_index_ddl_with_graph(
    rel: &RelationshipSchema,
    dialect: &str,
    graph_name: Option<&str>,
) -> Result<Vec<String>> {
    let ddl_dialect = ensure_ddl_supported(dialect, DdlOp::DropIndex)?;
    let mut statements = Vec::new();
    let type_name = &rel.type_name;

    match ddl_dialect {
        DdlDialect::Memgraph => {
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    statements.push(format!("DROP EDGE INDEX ON :{type_name}({});", field.name));
                }
            }
            for ft in &rel.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(type_name, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                statements.push(format!("DROP TEXT INDEX {idx_name};"));
            }
        }
        DdlDialect::DuckDb => {
            let table = pgq_edge_table_name(type_name);
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    statements.push(format!("DROP INDEX IF EXISTS idx_{table}_{};", field.name));
                }
            }
            for _ in &rel.fulltext_indexes {
                statements.push(format!("PRAGMA drop_fts_index('{table}');"));
            }
        }
        DdlDialect::FalkorDb => {
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    statements.push(format!(
                        "DROP INDEX FOR ()-[r:{type_name}]-() ON (r.{})",
                        field.name
                    ));
                }
            }
            for ft in &rel.fulltext_indexes {
                let props_str = ft
                    .properties
                    .iter()
                    .map(|p| format!("r.{p}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                statements.push(format!(
                    "DROP FULLTEXT INDEX FOR ()-[r:{type_name}]-() ON ({props_str})"
                ));
            }
        }
        DdlDialect::Age => {
            let target_graph = graph_name.unwrap_or(DEFAULT_AGE_GRAPH);
            if !is_valid_age_graph_name(target_graph) {
                return Err(Error::EmissionError(format!(
                    "Invalid Apache AGE graph name '{target_graph}': must be 1-63 alphanumeric/underscore characters starting with a letter or underscore"
                )));
            }
            return Err(Error::UnsupportedDialect {
                dialect: "Apache AGE".to_string(),
                feature: format!(
                    "Cypher 'DROP INDEX' queries on relationships. Indexes in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. DROP INDEX ON \"{target_graph}\".\"{type_name}\" or DROP INDEX \"{target_graph}\".<index_name>)."
                ),
            });
        }
        DdlDialect::Sql => {
            let table = pgq_edge_table_name(type_name);
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    statements.push(format!("DROP INDEX IF EXISTS idx_{table}_{};", field.name));
                }
            }
            for ft in &rel.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(type_name, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                statements.push(format!("DROP INDEX IF EXISTS {idx_name};"));
            }
        }
        DdlDialect::Cypher => {
            for field in rel.fields.values() {
                if is_pure_index_field(field) {
                    let i_name = cypher_rel_index_name(type_name, &field.name);
                    statements.push(format!("DROP INDEX {i_name} IF EXISTS"));
                }
            }
            for ft in &rel.fulltext_indexes {
                let idx_name = if ft.name.is_empty() {
                    canonical_fulltext_index_name(type_name, &ft.properties, ddl_dialect)
                } else {
                    ft.name.clone()
                };
                statements.push(format!("DROP INDEX {idx_name} IF EXISTS"));
            }
        }
    }
    Ok(statements)
}

/// Emits DROP CONSTRAINT DDL statements for a single node across dialects.
pub fn emit_node_drop_constraint_ddl(
    node: &NodeSchema,
    dialect: &str,
    include_type_constraints: bool,
) -> Result<Vec<String>> {
    emit_node_drop_constraint_ddl_with_graph(node, dialect, include_type_constraints, None)
}

/// Emits DROP CONSTRAINT DDL statements for a single node across dialects with an optional graph name.
pub fn emit_node_drop_constraint_ddl_with_graph(
    node: &NodeSchema,
    dialect: &str,
    include_type_constraints: bool,
    graph_name: Option<&str>,
) -> Result<Vec<String>> {
    let ddl_dialect = ensure_ddl_supported(dialect, DdlOp::DropConstraint)?;
    let mut statements = Vec::new();
    let primary_label = node.primary_label();

    match ddl_dialect {
        DdlDialect::FalkorDb => {
            return Err(Error::UnsupportedDialect {
                dialect: dialect.to_string(),
                feature: "constraint DDL queries".to_string(),
            });
        }
        DdlDialect::Age => {
            let target_graph = graph_name.unwrap_or(DEFAULT_AGE_GRAPH);
            if !is_valid_age_graph_name(target_graph) {
                return Err(Error::EmissionError(format!(
                    "Invalid Apache AGE graph name '{target_graph}': must be 1-63 alphanumeric/underscore characters starting with a letter or underscore"
                )));
            }
            return Err(Error::UnsupportedDialect {
                dialect: "Apache AGE".to_string(),
                feature: format!(
                    "Cypher 'DROP CONSTRAINT' queries. Indexes and constraints in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. ALTER TABLE \"{target_graph}\".\"{primary_label}\" DROP CONSTRAINT ...)."
                ),
            });
        }
        DdlDialect::Memgraph => {
            for field in node.fields.values() {
                let db_name = &field.name;
                // Mirror creation order: uniqueness before existence.
                if field.unique || field.primary_key {
                    statements.push(format!(
                        "DROP CONSTRAINT ON (n:{primary_label}) ASSERT n.{db_name} IS UNIQUE;"
                    ));
                }
                if !field.nullable {
                    statements.push(format!(
                        "DROP CONSTRAINT ON (n:{primary_label}) ASSERT EXISTS (n.{db_name});"
                    ));
                }
            }
        }
        DdlDialect::Sql => {
            let table = pgq_vertex_table_name(primary_label);
            let q_table = escape_sql_ident(&table);
            for field in node.fields.values() {
                if field.unique && !field.primary_key {
                    statements.push(format!(
                        "ALTER TABLE {q_table} DROP CONSTRAINT IF EXISTS uq_{table}_{};",
                        field.name
                    ));
                }
            }
        }
        DdlDialect::DuckDb => {
            let table = pgq_vertex_table_name(primary_label);
            for field in node.fields.values() {
                if field.unique && !field.primary_key {
                    statements.push(format!("DROP INDEX IF EXISTS uq_{table}_{};", field.name));
                }
            }
        }
        DdlDialect::Cypher => {
            for field in node.fields.values() {
                let db_name = &field.name;
                // Mirror creation order: DROP UNIQUE before DROP NOT NULL.
                if field.unique || field.primary_key {
                    let c_name = cypher_constraint_name(primary_label, db_name, "unique");
                    statements.push(format!("DROP CONSTRAINT {c_name} IF EXISTS"));
                }
                if !field.nullable {
                    let c_name = cypher_constraint_name(primary_label, db_name, "not_null");
                    statements.push(format!("DROP CONSTRAINT {c_name} IF EXISTS"));
                }
                if include_type_constraints {
                    let t_name = cypher_constraint_name(primary_label, db_name, "type");
                    statements.push(format!("DROP CONSTRAINT {t_name} IF EXISTS"));
                }
            }
        }
    }
    Ok(statements)
}

/// Emits DROP CONSTRAINT DDL statements for a single relationship across dialects.
pub fn emit_rel_drop_constraint_ddl(
    rel: &RelationshipSchema,
    dialect: &str,
    include_type_constraints: bool,
) -> Result<Vec<String>> {
    emit_rel_drop_constraint_ddl_with_graph(rel, dialect, include_type_constraints, None)
}

/// Emits DROP CONSTRAINT DDL statements for a single relationship across dialects with an optional graph name.
pub fn emit_rel_drop_constraint_ddl_with_graph(
    rel: &RelationshipSchema,
    dialect: &str,
    include_type_constraints: bool,
    graph_name: Option<&str>,
) -> Result<Vec<String>> {
    let ddl_dialect = ensure_ddl_supported(dialect, DdlOp::DropConstraint)?;
    let mut statements = Vec::new();
    let type_name = &rel.type_name;

    match ddl_dialect {
        DdlDialect::FalkorDb => {
            return Err(Error::UnsupportedDialect {
                dialect: dialect.to_string(),
                feature: "constraint DDL queries".to_string(),
            });
        }
        DdlDialect::Age => {
            let target_graph = graph_name.unwrap_or(DEFAULT_AGE_GRAPH);
            if !is_valid_age_graph_name(target_graph) {
                return Err(Error::EmissionError(format!(
                    "Invalid Apache AGE graph name '{target_graph}': must be 1-63 alphanumeric/underscore characters starting with a letter or underscore"
                )));
            }
            return Err(Error::UnsupportedDialect {
                dialect: "Apache AGE".to_string(),
                feature: format!(
                    "Cypher 'DROP CONSTRAINT' queries on relationships. Constraints in Apache AGE must be defined directly on the underlying PostgreSQL relational tables (e.g. ALTER TABLE \"{target_graph}\".\"{type_name}\" DROP CONSTRAINT ...)."
                ),
            });
        }
        DdlDialect::Memgraph => {
            // Memgraph does not support edge constraints; no-op.
        }
        DdlDialect::Sql => {
            let table = pgq_edge_table_name(type_name);
            let q_table = escape_sql_ident(&table);
            for field in rel.fields.values() {
                if field.unique && !field.primary_key {
                    statements.push(format!(
                        "ALTER TABLE {q_table} DROP CONSTRAINT IF EXISTS uq_{table}_{};",
                        field.name
                    ));
                }
            }
        }
        DdlDialect::DuckDb => {
            let table = pgq_edge_table_name(type_name);
            for field in rel.fields.values() {
                if field.unique && !field.primary_key {
                    statements.push(format!("DROP INDEX IF EXISTS uq_{table}_{};", field.name));
                }
            }
        }
        DdlDialect::Cypher => {
            for field in rel.fields.values() {
                let db_name = &field.name;
                // Mirror creation order: DROP UNIQUE before DROP NOT NULL.
                if field.unique || field.primary_key {
                    let c_name = cypher_rel_constraint_name(type_name, db_name, "unique");
                    statements.push(format!("DROP CONSTRAINT {c_name} IF EXISTS"));
                }
                if !field.nullable {
                    let c_name = cypher_rel_constraint_name(type_name, db_name, "not_null");
                    statements.push(format!("DROP CONSTRAINT {c_name} IF EXISTS"));
                }
                if include_type_constraints {
                    let t_name = cypher_rel_constraint_name(type_name, db_name, "type");
                    statements.push(format!("DROP CONSTRAINT {t_name} IF EXISTS"));
                }
            }
        }
    }
    Ok(statements)
}
