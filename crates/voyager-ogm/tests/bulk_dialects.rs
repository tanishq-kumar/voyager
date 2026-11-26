//! Per-dialect bulk query compilation tests (#397).
//!
//! SQL/PGQ and DuckPGQ bulk writes raise `UNSUPPORTED_FEATURE` because the bulk
//! compiler has no element-table schema; an unknown dialect is `UNSUPPORTED_DIALECT`.

use voyager_ogm::bulk::{
    BulkMergeConfig, BulkRelConfig, compile_bulk_create, compile_bulk_create_rel,
    compile_bulk_create_with_graph, compile_bulk_merge, compile_bulk_merge_split,
};
use voyager_ogm::{Error, ErrorCode};

#[test]
fn cypher_aliases_keep_emitting_cypher() {
    for dialect in [
        "cypher",
        "opencypher",
        "neo4j",
        "memgraph",
        "falkordb",
        "falkor",
    ] {
        let query = compile_bulk_create("Person", &["name"], "batch", "row", dialect)
            .unwrap_or_else(|e| panic!("{dialect}: {e}"));
        assert!(
            query
                .statement
                .starts_with("UNWIND $batch AS row CREATE (_person_0:Person)"),
            "{dialect}: {}",
            query.statement
        );
    }
}

#[test]
fn iso_gql_and_age_keep_their_emitters() {
    let gql = compile_bulk_create("Person", &["name"], "batch", "row", "iso_gql").unwrap();
    assert!(
        gql.statement
            .starts_with("UNWIND $batch AS row INSERT (_person_0:Person)"),
        "{}",
        gql.statement
    );

    let age = compile_bulk_create_with_graph(
        "Person",
        &["name"],
        "batch",
        "row",
        "apache_age",
        Some("zz_graph"),
    )
    .unwrap();
    assert!(
        age.statement.starts_with(
            "SELECT * FROM cypher('zz_graph', $$ UNWIND $batch AS row CREATE (_person_0:Person)"
        ),
        "{}",
        age.statement
    );
}

fn assert_sql_pgq_rejected(dialect: &str) {
    let error = compile_bulk_create("Person", &["name"], "batch", "row", dialect)
        .expect_err("SQL/PGQ bulk create must not fall back to Cypher");
    assert_eq!(error.code(), ErrorCode::UnsupportedFeature);
    assert!(
        matches!(&error, Error::UnsupportedFeature { dialect: named, feature }
            if named == dialect && feature.contains("element tables")),
        "{error}"
    );
}

#[test]
fn sql_pgq_dialects_raise_unsupported_feature() {
    for dialect in ["sql_pgq", "pgq", "sql", "duckpgq", "duck_pgq"] {
        assert_sql_pgq_rejected(dialect);
    }
}

#[test]
fn sql_pgq_merge_and_relationship_bulk_writes_are_rejected() {
    let merge = compile_bulk_merge("Person", "id", &["name"], "batch", "row", "sql_pgq")
        .expect_err("SQL/PGQ bulk merge must not fall back to Cypher");
    assert_eq!(merge.code(), ErrorCode::UnsupportedFeature);

    let merge_split = compile_bulk_merge_split(&BulkMergeConfig {
        label: "Person",
        key_property: "id",
        properties: &["name"],
        create_only_properties: &[],
        batch_param: "batch",
        row_alias: "row",
        dialect: "duckpgq",
        graph_name: Some("zz_graph"),
    })
    .expect_err("DuckPGQ bulk merge must not fall back to Cypher");
    assert_eq!(merge_split.code(), ErrorCode::UnsupportedFeature);

    let rel = compile_bulk_create_rel(
        "KNOWS",
        &[],
        "Person",
        "id",
        "Person",
        "id",
        "batch",
        "row",
        "pgq",
    )
    .expect_err("SQL/PGQ bulk relationship write must not fall back to Cypher");
    assert_eq!(rel.code(), ErrorCode::UnsupportedFeature);

    let rel_config = BulkRelConfig {
        rel_type: "KNOWS",
        properties: &[],
        from_label: "Person",
        from_key: "id",
        to_label: "Person",
        to_key: "id",
        batch_param: "batch",
        row_alias: "row",
        dialect: "duck_pgq",
        graph_name: None,
    };
    let rel_config_error = voyager_ogm::bulk::compile_bulk_create_rel_with_config(&rel_config)
        .expect_err("DuckPGQ bulk relationship write must not fall back to Cypher");
    assert_eq!(rel_config_error.code(), ErrorCode::UnsupportedFeature);
}

#[test]
fn unknown_dialect_is_rejected() {
    let error = compile_bulk_create("Person", &["name"], "batch", "row", "not_a_dialect")
        .expect_err("an unknown dialect must not fall back to Cypher");
    assert_eq!(error.code(), ErrorCode::UnsupportedDialect);
    assert!(error.to_string().contains("not_a_dialect"), "{error}");
}
