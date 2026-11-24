use voyager_ogm::ast::*;
use voyager_ogm::builder::QueryBuilder;
use voyager_ogm::emitters::{CypherEmitter, IsoGqlEmitter, SqlPgqEmitter};
use voyager_ogm::visitor::AstVisitor;

#[test]
fn test_cypher_emitter_simple_query() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .where_gt("p", "age", 21)
        .r#return()
        .field("p", "name", Some("actor"))
        .limit(10);

    let (arena, root) = builder.build();
    let mut emitter = CypherEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("Cypher emission failed");

    assert_eq!(
        compiled.statement,
        "MATCH (p:Person) WHERE p.age > $p0 RETURN p.name AS actor LIMIT 10"
    );
    assert_eq!(
        compiled.parameters.get("p0"),
        Some(&LiteralValue::Int64(21))
    );
}

#[test]
fn test_cypher_emitter_multi_hop_traversal() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .to(vec!["ACTED_IN"], Some("r"))
        .hops(1, 2)
        .node(Some("m"), vec!["Movie"])
        .from(vec!["DIRECTED"], Some("d_rel"))
        .node(Some("d"), vec!["Director"])
        .where_contains("p", "name", "Keanu")
        .r#return()
        .field("p", "name", Some("actor"))
        .field("m", "title", Some("movie"))
        .field("d", "name", Some("director"))
        .order_by_asc("p", "name")
        .limit(5);

    let (arena, root) = builder.build();
    let mut emitter = CypherEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("Cypher emission failed");

    assert_eq!(
        compiled.statement,
        "MATCH (p:Person)-[r:ACTED_IN*1..2]->(m:Movie)<-[d_rel:DIRECTED]-(d:Director) WHERE p.name CONTAINS $p0 RETURN p.name AS actor, m.title AS movie, d.name AS director ORDER BY p.name ASC LIMIT 5"
    );
    assert_eq!(
        compiled.parameters.get("p0"),
        Some(&LiteralValue::String("Keanu".into()))
    );
}

#[test]
fn test_sql_pgq_emitter_graph_table() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .to(vec!["ACTED_IN"], Some("r"))
        .node(Some("m"), vec!["Movie"])
        .where_gt("p", "age", 25)
        .r#return()
        .field("p", "name", Some("actor"))
        .field("m", "title", Some("movie"))
        .order_by_asc("p", "name")
        .limit(10);

    let (arena, root) = builder.build();
    let mut emitter = SqlPgqEmitter::new("movies_graph");
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("SQL:PGQ emission failed");

    assert_eq!(
        compiled.statement,
        "SELECT * FROM GRAPH_TABLE (movies_graph MATCH (p IS \"Person\") -[r IS \"ACTED_IN\"]-> (m IS \"Movie\") WHERE p.age > $p0 COLUMNS (p.name AS actor, m.title AS movie)) ORDER BY actor ASC NULLS LAST LIMIT 10"
    );
    assert_eq!(
        compiled.parameters.get("p0"),
        Some(&LiteralValue::Int64(25))
    );
}

#[test]
fn test_sql_pgq_emitter_like_translation() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .where_contains("p", "name", "Matrix")
        .r#return()
        .field("p", "name", Some("name"));

    let (arena, root) = builder.build();
    let mut emitter = SqlPgqEmitter::new("social_network");
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("SQL:PGQ emission failed");

    assert_eq!(
        compiled.statement,
        "SELECT * FROM GRAPH_TABLE (social_network MATCH (p IS \"Person\") WHERE strpos(p.name, $p0) > 0 COLUMNS (p.name AS name))"
    );
    assert_eq!(
        compiled.parameters.get("p0"),
        Some(&LiteralValue::String("Matrix".into()))
    );
}

#[test]
fn test_iso_gql_emitter_standard() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person", "Actor"])
        .to(vec!["ACTED_IN"], Some("r"))
        .node(Some("m"), vec!["Movie"])
        .where_eq("m", "released", 1999)
        .r#return()
        .field("p", "name", Some("actor"))
        .field("m", "title", Some("movie"))
        .order_by_desc("m", "released")
        .limit(15);

    let (arena, root) = builder.build();
    let mut emitter = IsoGqlEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("ISO GQL emission failed");

    assert_eq!(
        compiled.statement,
        "MATCH (p:Person&Actor)-[r:ACTED_IN]->(m:Movie) WHERE m.released = $p0 RETURN p.name AS actor, m.title AS movie ORDER BY m.released DESC LIMIT 15"
    );
    assert_eq!(
        compiled.parameters.get("p0"),
        Some(&LiteralValue::Int64(1999))
    );
}

#[test]
fn test_cypher_emitter_procedure_call() {
    let mut arena = QueryAstArena::new();
    let arg = arena.alloc(AstNode::Literal(LiteralValue::String("Person".into())));

    let proc_handle = arena.alloc(AstNode::ProcedureCall {
        execution_mode: ExecutionMode::Normal,
        namespace: Some("apoc.path".into()),
        procedure: "subgraphNodes".into(),
        arguments: vec![arg],
        yield_items: vec!["node".into()],
    });

    let mut emitter = CypherEmitter::new();
    let compiled = emitter
        .visit_query(&arena, proc_handle)
        .expect("Procedure call emission failed");

    assert_eq!(
        compiled.statement,
        "CALL apoc.path.subgraphNodes($p0) YIELD node"
    );
    assert_eq!(
        compiled.parameters.get("p0"),
        Some(&LiteralValue::String("Person".into()))
    );
}

#[test]
fn test_cypher_emitter_with_clause() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .where_gt("p", "age", 21);
    let age_ident = builder.ident("age");
    let threshold = builder.literal(50);
    let age_filter = builder.binary_expr(age_ident, BinaryOp::Lt, threshold);
    let actor = builder.ident("actor");
    builder
        .r#with()
        .field("p", "name", Some("actor"))
        .field("p", "age", Some("age"))
        .where_expr(age_filter)
        .r#return()
        .select_expr(actor, None::<&str>)
        .limit(10);

    let (arena, root) = builder.build();
    let mut emitter = CypherEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("Cypher emission failed");

    assert_eq!(
        compiled.statement,
        "MATCH (p:Person) WHERE p.age > $p0 WITH p.name AS actor, p.age AS age WHERE age < $p1 RETURN actor LIMIT 10"
    );
    assert_eq!(
        compiled.parameters.get("p0"),
        Some(&LiteralValue::Int64(21))
    );
    assert_eq!(
        compiled.parameters.get("p1"),
        Some(&LiteralValue::Int64(50))
    );
}

#[test]
fn test_with_projection_needs_alias_unless_bare_variable() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .r#with()
        .field("p", "name", None::<&str>);
    match builder.try_build() {
        Err(voyager_ogm::Error::InvalidClause(message)) => {
            assert!(
                message.contains("WITH projection `p.name` needs an alias"),
                "{message}"
            );
            assert!(
                message.contains("only a bare variable may be projected without one"),
                "{message}"
            );
        }
        Err(other) => panic!("expected InvalidClause, got {other:?}"),
        Ok(_) => panic!("expected InvalidClause, got a built query"),
    }

    let mut builder = QueryBuilder::new();
    builder.r#match().node(Some("p"), vec!["Person"]);
    let bare = builder.ident("p");
    builder.r#with().select_expr(bare, None::<&str>);
    assert!(builder.try_build().is_ok());

    // `field(var, "")` projects the bare variable and needs no alias.
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .r#with()
        .field("p", "", None::<&str>)
        .r#return()
        .field("p", "name", None::<&str>);
    let (arena, root) = builder
        .try_build()
        .expect("a bare variable projected with field(var, \"\") needs no alias");
    let compiled = CypherEmitter::new()
        .visit_query(&arena, root)
        .expect("Cypher emission failed");
    assert_eq!(compiled.statement, "MATCH (p:Person) WITH p RETURN p.name");
}

#[test]
fn test_sql_pgq_emitter_rejects_with_clause() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .r#with()
        .field("p", "name", Some("name"))
        .r#return()
        .field("p", "name", Some("name"));

    let (arena, root) = builder.build();
    let mut emitter = SqlPgqEmitter::new("social_graph");
    let result = emitter.visit_query(&arena, root);

    assert!(result.is_err());
    match result.unwrap_err() {
        voyager_ogm::Error::UnsupportedFeature { dialect, feature } => {
            assert_eq!(dialect, "sql_pgq");
            assert!(feature.contains("WITH clauses"));
        }
        other => panic!("Expected UnsupportedFeature error, got {other:?}"),
    }
}

#[test]
fn test_sql_pgq_emitter_rejects_linear_clauses() {
    let mut builder = QueryBuilder::new();
    let lit_expr = builder.literal(42i64);
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .let_("x", lit_expr)
        .r#return()
        .field("p", "name", None::<&str>);

    let (arena, root) = builder.build();
    let mut emitter = SqlPgqEmitter::new("social_graph");
    let result = emitter.visit_query(&arena, root);

    assert!(result.is_err());
    match result.unwrap_err() {
        voyager_ogm::Error::UnsupportedFeature { dialect, feature } => {
            assert_eq!(dialect, "sql_pgq");
            assert!(feature.contains("Linear statements (LET, FILTER)"));
        }
        other => panic!("Expected UnsupportedFeature error, got {other:?}"),
    }
}

#[test]
fn test_sql_pgq_emitter_rejects_path_variable_and_mode() {
    let mut b1 = QueryBuilder::new();
    b1.r#match()
        .path_variable("p_var")
        .node(Some("p"), vec!["Person"])
        .r#return()
        .field("p", "name", None::<&str>);
    let (arena1, root1) = b1.build();
    let mut emitter1 = SqlPgqEmitter::new("social_graph");
    let res1 = emitter1.visit_query(&arena1, root1);
    assert!(res1.is_err());
    match res1.unwrap_err() {
        voyager_ogm::Error::UnsupportedFeature { dialect, feature } => {
            assert_eq!(dialect, "sql_pgq");
            assert!(feature.contains("Path variable assignment"));
        }
        other => panic!("Expected UnsupportedFeature error, got {other:?}"),
    }

    let mut b2 = QueryBuilder::new();
    b2.r#match()
        .trail()
        .node(Some("p"), vec!["Person"])
        .r#return()
        .field("p", "name", None::<&str>);
    let (arena2, root2) = b2.build();
    let mut emitter2 = SqlPgqEmitter::new("social_graph");
    let res2 = emitter2.visit_query(&arena2, root2);
    assert!(res2.is_err());
    match res2.unwrap_err() {
        voyager_ogm::Error::UnsupportedFeature { dialect, feature } => {
            assert_eq!(dialect, "sql_pgq");
            assert!(feature.contains("Path search mode"));
        }
        other => panic!("Expected UnsupportedFeature error, got {other:?}"),
    }
}

#[test]
fn test_cypher_emitter_with_where_order_by_skip_limit() {
    let mut builder = QueryBuilder::new();
    builder.r#match().node(Some("p"), vec!["Person"]);
    let age_ident = builder.ident("age");
    let threshold = builder.literal(18);
    let age_filter = builder.binary_expr(age_ident, BinaryOp::Gt, threshold);
    let order_age = builder.ident("age");
    let return_name = builder.ident("name");
    builder
        .r#with()
        .field("p", "name", Some("name"))
        .field("p", "age", Some("age"))
        .where_expr(age_filter)
        .order_by(order_age, true)
        .skip(2)
        .limit(10)
        .r#return()
        .select_expr(return_name, None::<&str>);

    let (arena, root) = builder.build();
    let mut emitter = CypherEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("Cypher emission failed");

    assert_eq!(
        compiled.statement,
        "MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age > $p0 ORDER BY age ASC SKIP 2 LIMIT 10 RETURN name"
    );
}

#[test]
fn test_iso_gql_emitter_with_where_order_by_offset_limit() {
    let mut builder = QueryBuilder::new();
    builder.r#match().node(Some("p"), vec!["Person"]);
    let age_ident = builder.ident("age");
    let threshold = builder.literal(18);
    let age_filter = builder.binary_expr(age_ident, BinaryOp::Gt, threshold);
    let order_age = builder.ident("age");
    let return_name = builder.ident("name");
    builder
        .r#with()
        .field("p", "name", Some("name"))
        .field("p", "age", Some("age"))
        .where_expr(age_filter)
        .order_by(order_age, true)
        .skip(5)
        .limit(10)
        .r#return()
        .select_expr(return_name, None::<&str>);

    let (arena, root) = builder.build();
    let mut emitter = IsoGqlEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("ISO GQL emission failed");

    assert_eq!(
        compiled.statement,
        "MATCH (p:Person) WITH p.name AS name, p.age AS age WHERE age > $p0 ORDER BY age ASC OFFSET 5 LIMIT 10 RETURN name"
    );
}

#[test]
fn test_pagination_parameters_emit_as_parameters() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .r#return()
        .field("p", "name", None::<&str>)
        .skip_param("s")
        .limit_param("l");
    let (arena, root) = builder.build();

    let mut cypher = CypherEmitter::new();
    assert_eq!(
        cypher.visit_query(&arena, root).unwrap().statement,
        "MATCH (p:Person) RETURN p.name SKIP $s LIMIT $l"
    );

    let mut iso = IsoGqlEmitter::new();
    assert_eq!(
        iso.visit_query(&arena, root).unwrap().statement,
        "MATCH (p:Person) RETURN p.name OFFSET $s LIMIT $l"
    );

    let mut pgq = SqlPgqEmitter::new("graph");
    assert_eq!(
        pgq.visit_query(&arena, root).unwrap().statement,
        "SELECT * FROM GRAPH_TABLE (graph MATCH (p IS \"Person\") COLUMNS (p.name)) LIMIT $l OFFSET $s"
    );
}

#[test]
fn test_empty_with_is_safely_ignored() {
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .r#with()
        .r#return()
        .field("p", "name", None::<&str>);

    let (arena, root) = builder.build();
    let mut emitter = CypherEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("Cypher emission failed");

    assert_eq!(compiled.statement, "MATCH (p:Person) RETURN p.name");
}
