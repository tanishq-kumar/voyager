//! Tests for declarative relationship navigation builders and schema metadata.

use voyager_ogm::ast::{BinaryOp, Direction, LiteralValue};
use voyager_ogm::builder::{
    QueryBuilder, RelationshipBatchSpec, RelationshipConnectSpec, RelationshipDisconnectSpec,
    RelationshipEndpoint, RelationshipNavigationSpec,
};
use voyager_ogm::emitters::CypherEmitter;
use voyager_ogm::schema::{
    FieldDescriptor, FieldType, NodeSchema, RelationshipDeclaration, SchemaRegistry,
};
use voyager_ogm::visitor::AstVisitor;

fn compile(builder: &mut QueryBuilder) -> (String, usize) {
    let (arena, root) = builder.clone().build();
    let mut emitter = CypherEmitter::new();
    let result = emitter.visit_query(&arena, root).unwrap();
    (result.statement, result.parameters.len())
}

fn endpoint(variable: &str, key_value: impl Into<LiteralValue>) -> RelationshipEndpoint {
    RelationshipEndpoint::new(variable, vec!["Person"], "id", key_value)
}

fn target(variable: &str) -> RelationshipEndpoint {
    RelationshipEndpoint::new(variable, vec!["Person"], "id", LiteralValue::Null)
}

#[test]
fn navigation_matches_source_by_key_and_traverses_outgoing() {
    let mut builder = QueryBuilder::new();
    builder.navigate_relationship(&RelationshipNavigationSpec {
        source: endpoint("src", "alice"),
        target: target("dst"),
        rel_type: "KNOWS".to_string(),
        direction: Direction::Outgoing,
        rel_variable: None,
    });
    builder.r#return();
    let dst = builder.ident("dst");
    builder.select_expr(dst, None::<String>);

    let (statement, params) = compile(&mut builder);
    assert_eq!(
        statement,
        "MATCH (src:Person {id: $p0})-[:KNOWS]->(dst:Person) RETURN dst"
    );
    assert_eq!(params, 1);
}

#[test]
fn navigation_traverses_incoming_and_applies_a_filter() {
    let mut builder = QueryBuilder::new();
    builder.navigate_relationship(&RelationshipNavigationSpec {
        source: endpoint("src", "alice"),
        target: target("dst"),
        rel_type: "FOLLOWS".to_string(),
        direction: Direction::Incoming,
        rel_variable: Some("rel".to_string()),
    });
    builder.where_gte("dst", "age", 18i64);
    builder.r#return();
    let dst = builder.ident("dst");
    builder.select_expr(dst, None::<String>);

    let (statement, params) = compile(&mut builder);
    assert_eq!(
        statement,
        "MATCH (src:Person {id: $p0})<-[rel:FOLLOWS]-(dst:Person) WHERE dst.age >= $p1 RETURN dst"
    );
    assert_eq!(params, 2);
}

#[test]
fn complete_navigation_query_applies_optional_filters_and_returns_target() {
    let spec = RelationshipNavigationSpec {
        source: endpoint("src", "alice"),
        target: target("dst"),
        rel_type: "KNOWS".to_string(),
        direction: Direction::Outgoing,
        rel_variable: None,
    };

    let mut unfiltered = QueryBuilder::new();
    unfiltered.build_navigation_query(&spec, &[]);
    let (statement, _) = compile(&mut unfiltered);
    assert_eq!(
        statement,
        "MATCH (src:Person {id: $p0})-[:KNOWS]->(dst:Person) RETURN dst"
    );

    let mut filtered = QueryBuilder::new();
    filtered.build_navigation_query(
        &spec,
        &[("age".to_string(), BinaryOp::Gte, LiteralValue::Int64(18))],
    );
    let (statement, params) = compile(&mut filtered);
    assert_eq!(
        statement,
        "MATCH (src:Person {id: $p0})-[:KNOWS]->(dst:Person) WHERE dst.age >= $p1 RETURN dst"
    );
    assert_eq!(params, 2);
}

#[test]
fn connect_matches_both_nodes_and_creates_the_edge_with_properties() {
    let mut builder = QueryBuilder::new();
    builder
        .connect_relationship(&RelationshipConnectSpec {
            source: endpoint("src", "alice"),
            target: endpoint("dst", "bob"),
            rel_type: "KNOWS".to_string(),
            direction: Direction::Outgoing,
            properties: vec![("since".to_string(), LiteralValue::Int64(2026))],
        })
        .unwrap();

    let (statement, params) = compile(&mut builder);
    assert_eq!(
        statement,
        "MATCH (src:Person {id: $p0}) MATCH (dst:Person {id: $p1}) CREATE (src)-[:KNOWS {since: $p2}]->(dst)"
    );
    assert_eq!(params, 3);
}

#[test]
fn connect_incoming_creates_the_edge_from_target_to_source() {
    let mut builder = QueryBuilder::new();
    builder
        .connect_relationship(&RelationshipConnectSpec {
            source: endpoint("src", "alice"),
            target: endpoint("dst", "bob"),
            rel_type: "FOLLOWS".to_string(),
            direction: Direction::Incoming,
            properties: Vec::new(),
        })
        .unwrap();

    let (statement, _) = compile(&mut builder);
    assert_eq!(
        statement,
        "MATCH (src:Person {id: $p0}) MATCH (dst:Person {id: $p1}) CREATE (src)<-[:FOLLOWS]-(dst)"
    );
}

#[test]
fn disconnect_deletes_the_matched_edge() {
    let mut builder = QueryBuilder::new();
    builder
        .disconnect_relationship(&RelationshipDisconnectSpec {
            source: endpoint("src", "alice"),
            target: endpoint("dst", "bob"),
            rel_type: "KNOWS".to_string(),
            direction: Direction::Outgoing,
            rel_variable: "rel".to_string(),
        })
        .unwrap();

    let (statement, params) = compile(&mut builder);
    assert_eq!(
        statement,
        "MATCH (src:Person {id: $p0})-[rel:KNOWS]->(dst:Person {id: $p1}) DELETE rel"
    );
    assert_eq!(params, 2);
}

#[test]
fn batched_connect_and_disconnect_unwind_target_keys() {
    let mut connect = QueryBuilder::new();
    connect
        .relationship_batch(&RelationshipBatchSpec {
            source: endpoint("src", "alice"),
            target: target("dst"),
            rel_type: "KNOWS".to_string(),
            direction: Direction::Outgoing,
            rel_variable: None,
            properties: vec![("since".to_string(), LiteralValue::Int64(2026))],
            batch_param: "targets".to_string(),
            row_alias: "target_key".to_string(),
        })
        .unwrap();
    let (statement, _) = compile(&mut connect);
    assert_eq!(
        statement,
        "UNWIND $targets AS target_key MATCH (src:Person {id: $p0}) MATCH (dst:Person {id: target_key}) CREATE (src)-[:KNOWS {since: $p1}]->(dst)"
    );

    let mut disconnect = QueryBuilder::new();
    disconnect
        .relationship_batch(&RelationshipBatchSpec {
            source: endpoint("src", "alice"),
            target: target("dst"),
            rel_type: "FOLLOWS".to_string(),
            direction: Direction::Incoming,
            rel_variable: Some("rel".to_string()),
            properties: Vec::new(),
            batch_param: "targets".to_string(),
            row_alias: "target_key".to_string(),
        })
        .unwrap();
    let (statement, _) = compile(&mut disconnect);
    assert_eq!(
        statement,
        "UNWIND $targets AS target_key MATCH (src:Person {id: $p0})<-[rel:FOLLOWS]-(dst:Person {id: target_key}) DELETE rel"
    );
}

#[test]
fn node_schema_stores_relationship_declarations_through_snapshot_restore() {
    let node = NodeSchema::new("Person", vec!["Person".to_string()])
        .with_field(FieldDescriptor::new("id", FieldType::String).primary_key())
        .with_relationship_declaration(
            RelationshipDeclaration::new("friends", "KNOWS", Direction::Outgoing, "Person")
                .with_relationship_model("Knows"),
        )
        .with_relationship_declaration(RelationshipDeclaration::new(
            "followers",
            "FOLLOWS",
            Direction::Incoming,
            "Person",
        ));

    assert_eq!(
        node.relationship_declaration("friends")
            .unwrap()
            .relationship_model
            .as_deref(),
        Some("Knows")
    );

    let registry = SchemaRegistry::new();
    registry
        .register_node_with_id("social.Person", node)
        .unwrap();
    let json = registry.to_json().unwrap();
    assert!(json.contains("relationship_declarations"));
    assert!(json.contains("friends"));

    let restored = SchemaRegistry::new();
    restored.from_json(&json).unwrap();
    let entry = restored.get_node("Person").unwrap();
    assert_eq!(entry.relationship_declarations.len(), 2);
    assert_eq!(
        entry.relationship_declarations[0].direction,
        Direction::Outgoing
    );
    assert_eq!(
        entry.relationship_declarations[1].direction,
        Direction::Incoming
    );
}

#[test]
fn relationship_declarations_attach_to_a_registered_node_by_identity() {
    let registry = SchemaRegistry::new();
    registry
        .register_node_with_id(
            "social.Person",
            NodeSchema::new("Person", vec!["Person".to_string()])
                .with_field(FieldDescriptor::new("id", FieldType::String).primary_key()),
        )
        .unwrap();
    assert!(
        registry
            .get_node("social.Person")
            .unwrap()
            .relationship_declarations
            .is_empty()
    );

    registry
        .add_relationship_declarations(
            "social.Person",
            vec![RelationshipDeclaration::new(
                "friends",
                "KNOWS",
                Direction::Outgoing,
                "Person",
            )],
        )
        .unwrap();

    let node = registry.get_node("social.Person").unwrap();
    assert_eq!(node.name, "Person");
    assert_eq!(node.relationship_declarations.len(), 1);
    assert_eq!(node.relationship_declarations[0].name, "friends");
    assert_eq!(registry.node_schemas().len(), 1);

    let missing = registry.add_relationship_declarations("Unknown", Vec::new());
    assert!(missing.is_err());
}

#[test]
fn snapshots_without_relationship_declarations_still_restore() {
    let json = r#"{
        "nodes": {
            "Person": {
                "name": "Person",
                "labels": ["Person"],
                "fields": {},
                "primary_key": null,
                "fulltext_indexes": []
            }
        },
        "relationships": {}
    }"#;
    let registry = SchemaRegistry::new();
    registry.from_json(json).unwrap();
    assert!(
        registry
            .get_node("Person")
            .unwrap()
            .relationship_declarations
            .is_empty()
    );
}
