//! Regression tests for inequality spelling (#301) and `NOT IN` lowering (#302).

use voyager_ogm::ast::{BinaryOp, LiteralValue};
use voyager_ogm::builder::QueryBuilder;
use voyager_ogm::emitters::{AgeEmitter, CypherEmitter, IsoGqlEmitter, SqlPgqEmitter};
use voyager_ogm::visitor::AstVisitor;

fn neq_age_builder() -> QueryBuilder {
    let mut builder = QueryBuilder::new();
    builder
        .match_node(Some("p"), vec!["Person"])
        .where_property("p", "age", BinaryOp::Neq, LiteralValue::Int64(30))
        .field("p", "name", None::<String>);
    builder
}

#[test]
fn cypher_and_age_use_angle_bracket_inequality() {
    let (arena, root) = neq_age_builder().build();
    assert_eq!(
        CypherEmitter::new()
            .visit_query(&arena, root)
            .unwrap()
            .statement,
        "MATCH (p:Person) WHERE p.age <> $p0 RETURN p.name"
    );

    let mut age = AgeEmitter::new("age_graph");
    assert_eq!(
        age.visit_query(&arena, root).unwrap().statement,
        "SELECT * FROM cypher('age_graph', $$ MATCH (p:Person) WHERE p.age <> $p0 RETURN p.name $$, %s) AS (name agtype)"
    );
}

#[test]
fn iso_gql_uses_angle_bracket_inequality() {
    let (arena, root) = neq_age_builder().build();
    let mut gql = IsoGqlEmitter::new();
    assert_eq!(
        gql.visit_query(&arena, root).unwrap().statement,
        "MATCH (p:Person) WHERE p.age <> $p0 RETURN p.name"
    );
}

#[test]
fn sql_pgq_uses_angle_bracket_inequality() {
    let (arena, root) = neq_age_builder().build();
    let mut pgq = SqlPgqEmitter::new("g");
    assert_eq!(
        pgq.visit_query(&arena, root).unwrap().statement,
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) WHERE p.age <> $p0 COLUMNS (p.name))"
    );
}

fn not_in_builder() -> QueryBuilder {
    let mut builder = QueryBuilder::new();
    builder
        .match_node(Some("p"), vec!["Person"])
        .where_property(
            "p",
            "name",
            BinaryOp::NotIn,
            LiteralValue::List(vec![
                LiteralValue::String("Mallory".into()),
                LiteralValue::String("Eve".into()),
            ]),
        )
        .field("p", "name", None::<String>);
    builder
}

#[test]
fn cypher_and_age_lower_not_in_to_a_negated_in() {
    let (arena, root) = not_in_builder().build();
    assert_eq!(
        CypherEmitter::new()
            .visit_query(&arena, root)
            .unwrap()
            .statement,
        "MATCH (p:Person) WHERE NOT (p.name IN $p0) RETURN p.name"
    );

    let mut age = AgeEmitter::new("age_graph");
    assert_eq!(
        age.visit_query(&arena, root).unwrap().statement,
        "SELECT * FROM cypher('age_graph', $$ MATCH (p:Person) WHERE NOT (p.name IN $p0) RETURN p.name $$, %s) AS (name agtype)"
    );
}

#[test]
fn iso_gql_lowers_not_in_to_a_negated_in() {
    let (arena, root) = not_in_builder().build();
    let mut gql = IsoGqlEmitter::new();
    assert_eq!(
        gql.visit_query(&arena, root).unwrap().statement,
        "MATCH (p:Person) WHERE NOT (p.name IN $p0) RETURN p.name"
    );
}

#[test]
fn sql_pgq_lowers_not_in_to_not_all() {
    let (arena, root) = not_in_builder().build();
    let mut pgq = SqlPgqEmitter::new("g");
    assert_eq!(
        pgq.visit_query(&arena, root).unwrap().statement,
        "SELECT * FROM GRAPH_TABLE (g MATCH (p IS Person) WHERE p.name <> ALL($p0) COLUMNS (p.name))"
    );
}
