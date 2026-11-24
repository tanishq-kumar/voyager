//! Snapshot & Conformance Tests for Multi-Dialect DDL Migration Emitters.
//!
//! Asserts deterministic query emission across openCypher (Neo4j / Memgraph),
//! ISO GQL, and SQL:2023 PGQ / DuckPGQ directly from SchemaRegistry metadata.

use voyager_ogm::schema::{
    FieldDescriptor, FieldType, IndexType, NodeSchema, RelationshipSchema, SchemaRegistry,
};
use voyager_ogm::{
    emit_cypher_drop_node_ddl, emit_cypher_drop_rel_ddl, emit_cypher_node_ddl, emit_cypher_rel_ddl,
    emit_cypher25_drop_graph_type_ddl, emit_cypher25_graph_type_ddl, emit_gql_alter_node_ddl,
    emit_gql_alter_rel_ddl, emit_gql_drop_graph_type_ddl, emit_gql_graph_type_ddl,
    emit_memgraph_drop_node_ddl, emit_memgraph_drop_rel_ddl, emit_memgraph_node_ddl,
    emit_memgraph_rel_ddl, emit_node_constraint_ddl, emit_node_drop_constraint_ddl,
    emit_node_table_ddl, emit_pgq_drop_property_graph_ddl, emit_pgq_property_graph_ddl,
    emit_pgq_property_graph_ddl_with_dialect, emit_pgq_schema_ddl, emit_rel_constraint_ddl,
    emit_rel_drop_constraint_ddl, emit_rel_table_ddl, emit_table_ddl, field_type_to_sql,
};

fn create_test_node_schema() -> NodeSchema {
    NodeSchema::new("User", vec!["User".to_string(), "Account".to_string()])
        .with_field(FieldDescriptor::new("user_id", FieldType::String).primary_key())
        .with_field(FieldDescriptor::new("email", FieldType::String).unique())
        .with_field(FieldDescriptor::new("age", FieldType::Int64).indexed())
        .with_field(FieldDescriptor::new("bio", FieldType::String).with_index_type(IndexType::Text))
}

fn create_test_rel_schema() -> RelationshipSchema {
    RelationshipSchema::new("Follows", "FOLLOWS")
        .with_endpoints(vec!["User".to_string()], vec!["User".to_string()])
        .with_field(FieldDescriptor::new("since", FieldType::Int64).nullable(false))
        .with_field(FieldDescriptor::new("role", FieldType::String).indexed())
}

#[test]
fn test_cypher_node_ddl_and_drop() {
    let node = create_test_node_schema();

    // 1. DDL generation without property type constraints (Community Edition)
    let ddl = emit_cypher_node_ddl(&node, false);
    assert_eq!(ddl.len(), 5);
    assert_eq!(
        ddl[0],
        "CREATE INDEX index_user_age IF NOT EXISTS FOR (n:User) ON (n.age)"
    );
    assert_eq!(
        ddl[1],
        "CREATE TEXT INDEX index_user_bio IF NOT EXISTS FOR (n:User) ON (n.bio)"
    );
    assert_eq!(
        ddl[2],
        "CREATE CONSTRAINT constraint_user_email_unique IF NOT EXISTS FOR (n:User) REQUIRE n.email IS UNIQUE"
    );
    assert_eq!(
        ddl[3],
        "CREATE CONSTRAINT constraint_user_user_id_unique IF NOT EXISTS FOR (n:User) REQUIRE n.user_id IS UNIQUE"
    );
    assert_eq!(
        ddl[4],
        "CREATE CONSTRAINT constraint_user_user_id_not_null IF NOT EXISTS FOR (n:User) REQUIRE n.user_id IS NOT NULL"
    );

    // 2. DDL generation with Neo4j 5.x property type constraints (Enterprise Edition)
    let enterprise_ddl = emit_cypher_node_ddl(&node, true);
    assert_eq!(enterprise_ddl.len(), 9);
    assert!(enterprise_ddl.contains(
        &"CREATE CONSTRAINT constraint_user_user_id_type IF NOT EXISTS FOR (n:User) REQUIRE n.user_id :: STRING".to_string()
    ));
    assert!(enterprise_ddl.contains(
        &"CREATE CONSTRAINT constraint_user_email_type IF NOT EXISTS FOR (n:User) REQUIRE n.email :: STRING".to_string()
    ));
    assert!(enterprise_ddl.contains(
        &"CREATE CONSTRAINT constraint_user_age_type IF NOT EXISTS FOR (n:User) REQUIRE n.age :: INTEGER".to_string()
    ));

    // 3. DROP DDL generation
    let drop_ddl = emit_cypher_drop_node_ddl(&node, true);
    assert_eq!(drop_ddl.len(), 9);
    assert!(
        drop_ddl
            .contains(&"DROP CONSTRAINT constraint_user_user_id_not_null IF EXISTS".to_string())
    );
    assert!(
        drop_ddl.contains(&"DROP CONSTRAINT constraint_user_user_id_unique IF EXISTS".to_string())
    );
    assert!(
        drop_ddl.contains(&"DROP CONSTRAINT constraint_user_email_unique IF EXISTS".to_string())
    );
    assert!(drop_ddl.contains(&"DROP INDEX index_user_age IF EXISTS".to_string()));
    assert!(drop_ddl.contains(&"DROP INDEX index_user_bio IF EXISTS".to_string()));
    assert!(
        drop_ddl.contains(&"DROP CONSTRAINT constraint_user_user_id_type IF EXISTS".to_string())
    );
}

#[test]
fn test_memgraph_node_ddl_and_drop() {
    let node = create_test_node_schema();

    // 1. Memgraph DDL generation (un-named native constraints and indexes)
    let ddl = emit_memgraph_node_ddl(&node);
    assert_eq!(ddl.len(), 5);
    assert_eq!(
        ddl,
        vec![
            "CREATE CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE;",
            "CREATE CONSTRAINT ON (n:User) ASSERT n.user_id IS UNIQUE;",
            "CREATE CONSTRAINT ON (n:User) ASSERT EXISTS (n.user_id);",
            "CREATE INDEX ON :User(age);",
            "CREATE INDEX ON :User(bio);",
        ]
    );

    // 2. Memgraph DROP DDL generation (matches creation in reverse/symmetry)
    let drop_ddl = emit_memgraph_drop_node_ddl(&node);
    assert_eq!(drop_ddl.len(), 5);
    assert_eq!(
        drop_ddl,
        vec![
            "DROP CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE;",
            "DROP CONSTRAINT ON (n:User) ASSERT n.user_id IS UNIQUE;",
            "DROP CONSTRAINT ON (n:User) ASSERT EXISTS (n.user_id);",
            "DROP INDEX ON :User(age);",
            "DROP INDEX ON :User(bio);",
        ]
    );

    // 3. Standalone constraint emitters
    let c_ddl = emit_node_constraint_ddl(&node, "memgraph", false).unwrap();
    assert_eq!(
        c_ddl,
        vec![
            "CREATE CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE;",
            "CREATE CONSTRAINT ON (n:User) ASSERT n.user_id IS UNIQUE;",
            "CREATE CONSTRAINT ON (n:User) ASSERT EXISTS (n.user_id);",
        ]
    );
    let drop_c_ddl = emit_node_drop_constraint_ddl(&node, "memgraph", false).unwrap();
    assert_eq!(
        drop_c_ddl,
        vec![
            "DROP CONSTRAINT ON (n:User) ASSERT n.email IS UNIQUE;",
            "DROP CONSTRAINT ON (n:User) ASSERT n.user_id IS UNIQUE;",
            "DROP CONSTRAINT ON (n:User) ASSERT EXISTS (n.user_id);",
        ]
    );

    // 4. Standalone relationship emitters
    let rel = create_test_rel_schema();
    let rel_ddl = emit_memgraph_rel_ddl(&rel);
    assert_eq!(rel_ddl, vec!["CREATE EDGE INDEX ON :FOLLOWS(role);"]);
    let rel_drop_ddl = emit_memgraph_drop_rel_ddl(&rel);
    assert_eq!(rel_drop_ddl, vec!["DROP EDGE INDEX ON :FOLLOWS(role);"]);
    assert!(
        emit_rel_constraint_ddl(&rel, "memgraph", false)
            .unwrap()
            .is_empty()
    );
    assert!(
        emit_rel_drop_constraint_ddl(&rel, "memgraph", false)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn test_inferred_non_nullability_does_not_emit_existence_constraint() {
    // Python inference records `nullable: false` without `nullable_explicit`;
    // only the latter (or a primary key) may emit an executable constraint.
    let mut inferred = FieldDescriptor::new("name", FieldType::String);
    inferred.nullable = false;
    assert_eq!(inferred.nullable_explicit, None);
    assert!(!inferred.emits_existence_constraint());

    let node = NodeSchema::new("Inferred", vec!["Inferred".to_string()]).with_field(inferred);
    let ddl = emit_cypher_node_ddl(&node, false);
    assert!(!ddl.iter().any(|s| s.contains("not_null")));

    // The Rust builder records the explicit request and keeps today's DDL.
    let explicit = FieldDescriptor::new("name", FieldType::String).nullable(false);
    assert!(explicit.emits_existence_constraint());
    let explicit_node =
        NodeSchema::new("Explicit", vec!["Explicit".to_string()]).with_field(explicit);
    let explicit_ddl = emit_cypher_node_ddl(&explicit_node, false);
    assert!(
        explicit_ddl
            .iter()
            .any(|s| s.contains("REQUIRE n.name IS NOT NULL"))
    );
}

#[test]
fn test_cypher_rel_ddl_and_drop() {
    let rel = create_test_rel_schema();

    // 1. DDL generation
    let ddl = emit_cypher_rel_ddl(&rel, false);
    assert_eq!(ddl.len(), 2);
    assert_eq!(
        ddl[0],
        "CREATE INDEX index_rel_follows_role IF NOT EXISTS FOR ()-[r:FOLLOWS]-() ON (r.role)"
    );
    assert_eq!(
        ddl[1],
        "CREATE CONSTRAINT constraint_rel_follows_since_not_null IF NOT EXISTS FOR ()-[r:FOLLOWS]-() REQUIRE r.since IS NOT NULL"
    );

    // 2. Enterprise type constraints
    let ent_ddl = emit_cypher_rel_ddl(&rel, true);
    assert_eq!(ent_ddl.len(), 4);
    assert!(ent_ddl.contains(
        &"CREATE CONSTRAINT constraint_rel_follows_since_type IF NOT EXISTS FOR ()-[r:FOLLOWS]-() REQUIRE r.since :: INTEGER".to_string()
    ));

    // 3. DROP statements
    let drop_ddl = emit_cypher_drop_rel_ddl(&rel, true);
    assert_eq!(drop_ddl.len(), 4);
    assert!(
        drop_ddl.contains(
            &"DROP CONSTRAINT constraint_rel_follows_since_not_null IF EXISTS".to_string()
        )
    );
    assert!(drop_ddl.contains(&"DROP INDEX index_rel_follows_role IF EXISTS".to_string()));
    assert!(
        drop_ddl
            .contains(&"DROP CONSTRAINT constraint_rel_follows_since_type IF EXISTS".to_string())
    );
}

#[test]
fn test_cypher_rel_unique_and_primary_key_constraints() {
    // Unique fields get a uniqueness constraint; existence is emitted only when
    // the field is actually non-nullable (a nullable unique field stays optional).
    let rel = RelationshipSchema::new("Transfer", "TRANSFER")
        .with_endpoints(vec!["Account".to_string()], vec!["Account".to_string()])
        .with_field(FieldDescriptor::new("code", FieldType::String).nullable(false))
        .with_field(FieldDescriptor::new("ref", FieldType::String).unique())
        .with_field(
            FieldDescriptor::new("slug", FieldType::String)
                .unique()
                .indexed(),
        )
        .with_field(
            FieldDescriptor::new("token", FieldType::String)
                .unique()
                .nullable(false),
        )
        .with_field(FieldDescriptor::new("transfer_id", FieldType::String).primary_key());

    let ddl = emit_cypher_rel_ddl(&rel, false);
    assert_eq!(
        ddl,
        vec![
            "CREATE CONSTRAINT constraint_rel_transfer_code_not_null IF NOT EXISTS FOR ()-[r:TRANSFER]-() REQUIRE r.code IS NOT NULL",
            "CREATE CONSTRAINT constraint_rel_transfer_ref_unique IF NOT EXISTS FOR ()-[r:TRANSFER]-() REQUIRE r.ref IS UNIQUE",
            "CREATE CONSTRAINT constraint_rel_transfer_slug_unique IF NOT EXISTS FOR ()-[r:TRANSFER]-() REQUIRE r.slug IS UNIQUE",
            "CREATE CONSTRAINT constraint_rel_transfer_token_unique IF NOT EXISTS FOR ()-[r:TRANSFER]-() REQUIRE r.token IS UNIQUE",
            "CREATE CONSTRAINT constraint_rel_transfer_token_not_null IF NOT EXISTS FOR ()-[r:TRANSFER]-() REQUIRE r.token IS NOT NULL",
            "CREATE CONSTRAINT constraint_rel_transfer_transfer_id_unique IF NOT EXISTS FOR ()-[r:TRANSFER]-() REQUIRE r.transfer_id IS UNIQUE",
            "CREATE CONSTRAINT constraint_rel_transfer_transfer_id_not_null IF NOT EXISTS FOR ()-[r:TRANSFER]-() REQUIRE r.transfer_id IS NOT NULL",
        ]
    );

    // The uniqueness constraint brings its own index, so no secondary index is emitted.
    assert!(!ddl.iter().any(|s| s.contains("CREATE INDEX")));

    let drop_ddl = emit_cypher_drop_rel_ddl(&rel, false);
    assert_eq!(
        drop_ddl,
        vec![
            "DROP CONSTRAINT constraint_rel_transfer_code_not_null IF EXISTS",
            "DROP CONSTRAINT constraint_rel_transfer_ref_unique IF EXISTS",
            "DROP CONSTRAINT constraint_rel_transfer_slug_unique IF EXISTS",
            "DROP CONSTRAINT constraint_rel_transfer_token_unique IF EXISTS",
            "DROP CONSTRAINT constraint_rel_transfer_token_not_null IF EXISTS",
            "DROP CONSTRAINT constraint_rel_transfer_transfer_id_unique IF EXISTS",
            "DROP CONSTRAINT constraint_rel_transfer_transfer_id_not_null IF EXISTS",
        ]
    );

    // The per-model constraint/drop-constraint emitters agree with the aggregate ones.
    assert_eq!(
        voyager_ogm::emit_rel_constraint_ddl(&rel, "cypher", false).unwrap(),
        ddl
    );
    assert_eq!(
        voyager_ogm::emit_rel_drop_constraint_ddl(&rel, "cypher", false).unwrap(),
        drop_ddl
    );
}

#[test]
fn test_cypher_unique_constraints_emitted_before_not_null() {
    // Community Edition accepts uniqueness constraints but rejects existence
    // constraints, so the UNIQUE statement must run first.
    let node = NodeSchema::new("Ordered", vec!["Ordered".to_string()])
        .with_field(FieldDescriptor::new("id", FieldType::String).primary_key());
    let node_ddl = emit_cypher_node_ddl(&node, false);
    let node_unique = node_ddl
        .iter()
        .position(|s| s.contains("IS UNIQUE"))
        .expect("node UNIQUE constraint");
    let node_not_null = node_ddl
        .iter()
        .position(|s| s.contains("IS NOT NULL"))
        .expect("node NOT NULL constraint");
    assert!(node_unique < node_not_null);

    let rel = RelationshipSchema::new("OrderedRel", "ORDERED_REL")
        .with_endpoints(vec!["Ordered".to_string()], vec!["Ordered".to_string()])
        .with_field(FieldDescriptor::new("id", FieldType::String).primary_key());
    let rel_ddl = emit_cypher_rel_ddl(&rel, false);
    let rel_unique = rel_ddl
        .iter()
        .position(|s| s.contains("IS UNIQUE"))
        .expect("relationship UNIQUE constraint");
    let rel_not_null = rel_ddl
        .iter()
        .position(|s| s.contains("IS NOT NULL"))
        .expect("relationship NOT NULL constraint");
    assert!(rel_unique < rel_not_null);

    // Constraint-only emitters follow the same ordering.
    assert_eq!(
        voyager_ogm::emit_node_constraint_ddl(&node, "cypher", false).unwrap(),
        node_ddl
    );
    assert_eq!(
        voyager_ogm::emit_rel_constraint_ddl(&rel, "cypher", false).unwrap(),
        rel_ddl
    );
}

#[test]
fn test_cypher25_graph_type_ddl() {
    let node = create_test_node_schema();
    let rel = create_test_rel_schema();

    let ddl = emit_cypher25_graph_type_ddl(&[&node], &[&rel]);
    let expected = "\
ALTER CURRENT GRAPH TYPE SET {
    (:User => :Account {age :: INTEGER, bio :: STRING, email :: STRING IS UNIQUE, user_id :: STRING IS KEY}),
    (:User)-[:FOLLOWS => {role :: STRING, since :: INTEGER NOT NULL}]->(:User)
}";
    assert_eq!(ddl, expected);
    assert_eq!(
        emit_cypher25_drop_graph_type_ddl(),
        "ALTER CURRENT GRAPH TYPE SET {}"
    );
}

#[test]
fn test_gql_create_graph_type_ddl() {
    let node = create_test_node_schema();
    let rel = create_test_rel_schema();

    let ddl = emit_gql_graph_type_ddl("SocialGraph", &[&node], &[&rel]);
    let expected = "\
CREATE GRAPH TYPE SocialGraph AS {
    NODE User (age INTEGER, bio STRING, email STRING, user_id STRING NOT NULL) KEY (user_id),
    EDGE FOLLOWS CONNECTING (User TO User) (role STRING, since INTEGER NOT NULL)
}";
    assert_eq!(ddl, expected);
}

#[test]
fn test_gql_alter_and_drop_ddl() {
    let node = create_test_node_schema();
    let rel = create_test_rel_schema();

    let node_alter = emit_gql_alter_node_ddl(&node);
    assert_eq!(
        node_alter,
        "ALTER CURRENT GRAPH TYPE ADD NODE TYPE (:User {age :: INTEGER?, bio :: STRING?, email :: STRING, user_id :: STRING})"
    );

    let rel_alter = emit_gql_alter_rel_ddl(&rel, Some("User"), Some("User"));
    assert_eq!(
        rel_alter,
        "ALTER CURRENT GRAPH TYPE ADD RELATIONSHIP TYPE (:User)-[:FOLLOWS {role :: STRING, since :: INTEGER}]->(:User)"
    );

    let drop_ddl = emit_gql_drop_graph_type_ddl("SocialGraph");
    assert_eq!(drop_ddl, "DROP GRAPH TYPE SocialGraph IF EXISTS");
}

#[test]
fn test_pgq_property_graph_ddl() {
    let node = create_test_node_schema();
    let rel = create_test_rel_schema();

    let ddl = emit_pgq_property_graph_ddl("social_graph", &[&node], &[&rel]).unwrap();
    let expected = "\
CREATE PROPERTY GRAPH social_graph
  VERTEX TABLES (
    \"user\" KEY (\"user_id\") LABEL \"User\" PROPERTIES (\"age\", \"bio\", \"email\", \"user_id\")
  )
  EDGE TABLES (
    \"follows\"
      SOURCE KEY (\"src_user_id\") REFERENCES \"user\" (\"user_id\")
      DESTINATION KEY (\"dst_user_id\") REFERENCES \"user\" (\"user_id\")
      LABEL \"FOLLOWS\" PROPERTIES (\"role\", \"since\")
  );";
    assert_eq!(ddl, expected);

    let drop_ddl = emit_pgq_drop_property_graph_ddl("social_graph");
    assert_eq!(drop_ddl, "DROP PROPERTY GRAPH IF EXISTS social_graph;");
}

#[test]
fn test_pgq_vertex_and_edge_table_names_follow_labels_and_types() {
    // A model whose class name differs from its label, a second model, a
    // self-relationship and a relationship between the two models.
    let person = NodeSchema::new("P", vec!["ZzPerson".to_string()])
        .with_field(FieldDescriptor::new("pid", FieldType::String).primary_key());
    let company = NodeSchema::new("C", vec!["ZzCompany".to_string()])
        .with_field(FieldDescriptor::new("cid", FieldType::String).primary_key());
    let knows = RelationshipSchema::new("K", "KNOWS")
        .with_endpoints(vec!["ZzPerson".to_string()], vec!["ZzPerson".to_string()])
        .with_field(FieldDescriptor::new("since", FieldType::Int64));
    let works_at = RelationshipSchema::new("W", "WORKS_AT")
        .with_endpoints(vec!["ZzPerson".to_string()], vec!["ZzCompany".to_string()])
        .with_field(FieldDescriptor::new("since", FieldType::Int64));

    let ddl = emit_pgq_property_graph_ddl("g", &[&person, &company], &[&knows, &works_at]).unwrap();
    let expected = "\
CREATE PROPERTY GRAPH g
  VERTEX TABLES (
    \"zzperson\" KEY (\"pid\") LABEL \"ZzPerson\" PROPERTIES (\"pid\"),
    \"zzcompany\" KEY (\"cid\") LABEL \"ZzCompany\" PROPERTIES (\"cid\")
  )
  EDGE TABLES (
    \"knows\"
      SOURCE KEY (\"src_pid\") REFERENCES \"zzperson\" (\"pid\")
      DESTINATION KEY (\"dst_pid\") REFERENCES \"zzperson\" (\"pid\")
      LABEL \"KNOWS\" PROPERTIES (\"since\"),
    \"works_at\"
      SOURCE KEY (\"src_pid\") REFERENCES \"zzperson\" (\"pid\")
      DESTINATION KEY (\"dst_cid\") REFERENCES \"zzcompany\" (\"cid\")
      LABEL \"WORKS_AT\" PROPERTIES (\"since\")
  );";
    assert_eq!(ddl, expected);
}

#[test]
fn test_pgq_edge_only_ddl_uses_endpoint_node_key() {
    let person = NodeSchema::new("P", vec!["ZzPerson".to_string()])
        .with_field(FieldDescriptor::new("pid", FieldType::String).primary_key());
    let knows = RelationshipSchema::new("K", "KNOWS")
        .with_endpoints(vec!["ZzPerson".to_string()], vec!["ZzPerson".to_string()]);

    let ddl = emit_pgq_property_graph_ddl("g", &[&person], &[&knows]).unwrap();
    assert!(ddl.contains("SOURCE KEY (\"src_pid\") REFERENCES \"zzperson\" (\"pid\")"));
    assert!(ddl.contains("DESTINATION KEY (\"dst_pid\") REFERENCES \"zzperson\" (\"pid\")"));
}

#[test]
fn test_pgq_missing_endpoints_error() {
    let node = create_test_node_schema();
    let rel_no_endpoints = RelationshipSchema::new("follows", "FOLLOWS");
    let err = emit_pgq_property_graph_ddl("social_graph", &[&node], &[&rel_no_endpoints]);
    assert!(err.is_err());
    assert!(
        err.unwrap_err()
            .to_string()
            .contains("missing source_labels")
    );
}

#[test]
fn test_cypher25_empty_and_composite_facets() {
    // 1. Empty fields node and relationship have no dangling =>
    let empty_node = NodeSchema::new("Empty", vec!["Empty".to_string()]);
    let empty_rel = RelationshipSchema::new("EmptyRel", "EMPTY_REL")
        .with_endpoints(vec!["Empty".to_string()], vec!["Empty".to_string()]);
    let ddl = emit_cypher25_graph_type_ddl(&[&empty_node], &[&empty_rel]);
    assert!(ddl.contains("(:Empty)"));
    assert!(!ddl.contains("(:Empty =>)"));
    assert!(ddl.contains("(:Empty)-[:EMPTY_REL]->(:Empty)"));
    assert!(!ddl.contains("[:EMPTY_REL =>]"));

    // 2. Composed NOT NULL and IS UNIQUE facets
    let composite_node = NodeSchema::new("Member", vec!["Member".to_string()]).with_field(
        FieldDescriptor::new("email", FieldType::String)
            .unique()
            .nullable(false),
    );
    let composite_ddl = emit_cypher25_graph_type_ddl(&[&composite_node], &[]);
    assert!(composite_ddl.contains("email :: STRING NOT NULL IS UNIQUE"));
}

#[test]
fn test_schema_registry_ddl_methods() {
    let registry = SchemaRegistry::new();
    let node = create_test_node_schema();
    let rel = create_test_rel_schema();

    registry.register_node(node).unwrap();
    registry.register_relationship(rel).unwrap();

    let cypher_ddl = registry.generate_cypher_ddl(false);
    assert_eq!(cypher_ddl.len(), 7);

    let node_cypher = registry.generate_node_cypher_ddl("User", false);
    assert!(node_cypher.is_some());
    assert_eq!(node_cypher.unwrap().len(), 5);

    let rel_cypher = registry.generate_rel_cypher_ddl("Follows", false);
    assert!(rel_cypher.is_some());
    assert_eq!(rel_cypher.unwrap().len(), 2);

    let memgraph_ddl = registry.generate_memgraph_ddl();
    assert_eq!(memgraph_ddl.len(), 6);

    let node_memgraph = registry.generate_node_memgraph_ddl("User");
    assert!(node_memgraph.is_some());
    assert_eq!(node_memgraph.unwrap().len(), 5);

    let rel_memgraph = registry.generate_rel_memgraph_ddl("Follows");
    assert!(rel_memgraph.is_some());
    assert_eq!(rel_memgraph.unwrap().len(), 1);

    let memgraph_drop = registry.generate_memgraph_drop_ddl();
    assert_eq!(memgraph_drop.len(), 6);

    let node_memgraph_drop = registry.generate_node_memgraph_drop_ddl("User");
    assert!(node_memgraph_drop.is_some());
    assert_eq!(node_memgraph_drop.unwrap().len(), 5);

    let rel_memgraph_drop = registry.generate_rel_memgraph_drop_ddl("Follows");
    assert!(rel_memgraph_drop.is_some());
    assert_eq!(rel_memgraph_drop.unwrap().len(), 1);

    let cypher25_ddl = registry.generate_cypher25_graph_type_ddl();
    assert!(cypher25_ddl.starts_with("ALTER CURRENT GRAPH TYPE SET {"));
    assert_eq!(
        registry.generate_cypher25_drop_graph_type_ddl(),
        "ALTER CURRENT GRAPH TYPE SET {}"
    );

    let gql_ddl = registry.generate_gql_graph_type_ddl("NetworkGraph");
    assert!(gql_ddl.starts_with("CREATE GRAPH TYPE NetworkGraph AS {"));

    let pgq_ddl = registry.generate_pgq_ddl("network_pgq").unwrap();
    assert!(pgq_ddl.starts_with("CREATE PROPERTY GRAPH network_pgq"));

    // Requesting only the relationship still pulls in its endpoint node so the
    // edge references the node's real key column.
    let edge_only = registry
        .generate_pgq_ddl_for("network_pgq", &["Follows"])
        .unwrap();
    assert!(edge_only.contains("VERTEX TABLES"));
    assert!(edge_only.contains("SOURCE KEY (\"src_user_id\") REFERENCES \"user\" (\"user_id\")"));
    assert!(!edge_only.contains("SOURCE KEY (\"src_id\")"));
}

#[test]
fn test_unsupported_constraint_ddl_error() {
    let node = create_test_node_schema();
    let rel = create_test_rel_schema();

    assert!(voyager_ogm::emit_node_constraint_ddl(&node, "age", false).is_err());
    assert!(voyager_ogm::emit_node_constraint_ddl(&node, "falkordb", false).is_err());
    assert!(voyager_ogm::emit_rel_constraint_ddl(&rel, "age", false).is_err());
    assert!(voyager_ogm::emit_rel_constraint_ddl(&rel, "falkordb", false).is_err());
    assert!(voyager_ogm::emit_node_drop_constraint_ddl(&node, "age", false).is_err());
    assert!(voyager_ogm::emit_node_drop_constraint_ddl(&node, "falkordb", false).is_err());
    assert!(voyager_ogm::emit_rel_drop_constraint_ddl(&rel, "age", false).is_err());
    assert!(voyager_ogm::emit_rel_drop_constraint_ddl(&rel, "falkordb", false).is_err());
}

#[test]
fn test_table_ddl_sharing_naming_rules_and_reserved_words() {
    let user = NodeSchema::new("UserModel", vec!["User".to_string()])
        .with_field(FieldDescriptor::new("uid", FieldType::String).primary_key())
        .with_field(FieldDescriptor::new("name", FieldType::String).nullable(false));
    let order = NodeSchema::new("OrderModel", vec!["Order".to_string()])
        .with_field(FieldDescriptor::new("oid", FieldType::String).primary_key())
        .with_field(FieldDescriptor::new("amount", FieldType::Float64));
    let places = RelationshipSchema::new("PlacesModel", "PLACES")
        .with_endpoints(vec!["User".to_string()], vec!["Order".to_string()])
        .with_field(FieldDescriptor::new("placed_at", FieldType::DateTime).nullable(false));

    let tables = emit_table_ddl(&[&user, &order], &[&places], "postgres").unwrap();
    assert_eq!(tables.len(), 3);

    let user_table = emit_node_table_ddl(&user, "postgres").unwrap();
    assert_eq!(user_table, tables[0]);
    let places_table = emit_rel_table_ddl(&places, &[&user, &order], "postgres").unwrap();
    assert_eq!(places_table, tables[2]);
    assert!(tables[0].contains("\"uid\" TEXT NOT NULL"));
    assert!(tables[0].contains("\"name\" TEXT NOT NULL"));
    assert!(tables[0].contains("PRIMARY KEY (\"uid\")"));

    assert!(tables[1].starts_with("CREATE TABLE IF NOT EXISTS \"order\" ("));
    assert!(tables[1].contains("\"oid\" TEXT NOT NULL"));
    assert!(tables[1].contains("\"amount\" DOUBLE PRECISION"));
    assert!(tables[1].contains("PRIMARY KEY (\"oid\")"));

    // Edge table uses lowercased type, quoted, with surrogate id key and src_<key> and dst_<key> endpoint columns
    assert!(tables[2].starts_with("CREATE TABLE IF NOT EXISTS \"places\" ("));
    assert!(tables[2].contains("\"id\" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"));
    assert!(tables[2].contains("\"src_uid\" TEXT NOT NULL REFERENCES \"user\" (\"uid\")"));
    assert!(tables[2].contains("\"dst_oid\" TEXT NOT NULL REFERENCES \"order\" (\"oid\")"));
    assert!(tables[2].contains("\"placed_at\" TIMESTAMP WITH TIME ZONE NOT NULL"));
    assert!(!tables[2].contains("PRIMARY KEY (\"src_uid\", \"dst_oid\")"));
}

#[test]
fn test_table_ddl_self_relationship_and_label_differs_from_class() {
    let person = NodeSchema::new("PClass", vec!["ZzPerson".to_string()])
        .with_field(FieldDescriptor::new("pid", FieldType::Int64).primary_key())
        .with_field(FieldDescriptor::new("bio", FieldType::String));
    let knows = RelationshipSchema::new("KClass", "KNOWS")
        .with_endpoints(vec!["ZzPerson".to_string()], vec!["ZzPerson".to_string()])
        .with_field(FieldDescriptor::new("since", FieldType::Int64).nullable(false));

    let tables = emit_table_ddl(&[&person], &[&knows], "postgres").unwrap();
    assert_eq!(tables.len(), 2);

    assert!(tables[0].starts_with("CREATE TABLE IF NOT EXISTS \"zzperson\" ("));
    assert!(tables[0].contains("\"pid\" BIGINT NOT NULL"));
    assert!(tables[0].contains("PRIMARY KEY (\"pid\")"));

    // Self-relationship keeps source and destination endpoint columns distinct with surrogate id key
    assert!(tables[1].starts_with("CREATE TABLE IF NOT EXISTS \"knows\" ("));
    assert!(tables[1].contains("\"id\" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"));
    assert!(tables[1].contains("\"src_pid\" BIGINT NOT NULL REFERENCES \"zzperson\" (\"pid\")"));
    assert!(tables[1].contains("\"dst_pid\" BIGINT NOT NULL REFERENCES \"zzperson\" (\"pid\")"));
    assert!(tables[1].contains("\"since\" BIGINT NOT NULL"));
    assert!(!tables[1].contains("PRIMARY KEY (\"src_pid\", \"dst_pid\")"));
}

#[test]
fn test_table_ddl_generation_duckdb_surrogate_sequence() {
    let user = NodeSchema::new("UserModel", vec!["User".to_string()])
        .with_field(FieldDescriptor::new("uid", FieldType::String).primary_key());
    let order = NodeSchema::new("OrderModel", vec!["Order".to_string()])
        .with_field(FieldDescriptor::new("oid", FieldType::String).primary_key());
    let places = RelationshipSchema::new("PlacesModel", "PLACES")
        .with_endpoints(vec!["User".to_string()], vec!["Order".to_string()])
        .with_field(FieldDescriptor::new("placed_at", FieldType::DateTime).nullable(false));

    let tables = emit_table_ddl(&[&user, &order], &[&places], "duckdb").unwrap();
    // 2 vertex tables + 1 sequence + 1 edge table = 4 statements
    assert_eq!(tables.len(), 4);
    assert!(tables[0].starts_with("CREATE TABLE IF NOT EXISTS \"user\""));
    assert!(tables[1].starts_with("CREATE TABLE IF NOT EXISTS \"order\""));
    assert_eq!(tables[2], "CREATE SEQUENCE IF NOT EXISTS seq_places_id;");
    assert!(tables[3].starts_with("CREATE TABLE IF NOT EXISTS \"places\" ("));
    assert!(tables[3].contains("\"id\" BIGINT DEFAULT nextval('seq_places_id') PRIMARY KEY"));
    assert!(tables[3].contains("\"src_uid\" VARCHAR NOT NULL REFERENCES \"user\" (\"uid\")"));
    assert!(tables[3].contains("\"dst_oid\" VARCHAR NOT NULL REFERENCES \"order\" (\"oid\")"));
}

#[test]
fn test_table_ddl_explicit_relationship_primary_key() {
    let person = NodeSchema::new("Person", vec!["Person".to_string()])
        .with_field(FieldDescriptor::new("pid", FieldType::String).primary_key());
    let calls = RelationshipSchema::new("Calls", "CALLS")
        .with_endpoints(vec!["Person".to_string()], vec!["Person".to_string()])
        .with_field(FieldDescriptor::new("call_id", FieldType::String).primary_key())
        .with_field(FieldDescriptor::new("duration", FieldType::Int64).nullable(false));

    // Postgres: uses explicit PK and no surrogate key
    let pg_tables = emit_table_ddl(&[&person], &[&calls], "postgres").unwrap();
    assert_eq!(pg_tables.len(), 2);
    assert!(pg_tables[1].starts_with("CREATE TABLE IF NOT EXISTS \"calls\" ("));
    assert!(!pg_tables[1].contains("GENERATED ALWAYS AS IDENTITY"));
    assert!(pg_tables[1].contains("\"call_id\" TEXT NOT NULL"));
    assert!(pg_tables[1].contains("PRIMARY KEY (\"call_id\")"));

    // DuckDB: uses explicit PK, no sequence created, no surrogate key
    let duck_tables = emit_table_ddl(&[&person], &[&calls], "duckdb").unwrap();
    assert_eq!(duck_tables.len(), 2); // No sequence!
    assert!(duck_tables[1].starts_with("CREATE TABLE IF NOT EXISTS \"calls\" ("));
    assert!(!duck_tables[1].contains("nextval("));
    assert!(duck_tables[1].contains("\"call_id\" VARCHAR NOT NULL"));
    assert!(duck_tables[1].contains("PRIMARY KEY (\"call_id\")"));
}

#[test]
fn test_table_ddl_surrogate_name_avoid_collision_with_id_field() {
    let node = NodeSchema::new("Item", vec!["Item".to_string()])
        .with_field(FieldDescriptor::new("iid", FieldType::String).primary_key());
    // Relationship has non-PK field named "id"
    let rel = RelationshipSchema::new("Connects", "CONNECTS")
        .with_endpoints(vec!["Item".to_string()], vec!["Item".to_string()])
        .with_field(FieldDescriptor::new("id", FieldType::String).nullable(false))
        .with_field(FieldDescriptor::new("weight", FieldType::Float64));

    let pg_tables = emit_table_ddl(&[&node], &[&rel], "postgres").unwrap();
    assert_eq!(pg_tables.len(), 2);
    // Surrogate column is "connects_id", not "id"
    assert!(
        pg_tables[1].contains("\"connects_id\" BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY")
    );
    assert!(pg_tables[1].contains("\"id\" TEXT NOT NULL"));
    assert!(pg_tables[1].contains("\"weight\" DOUBLE PRECISION"));

    let duck_tables = emit_table_ddl(&[&node], &[&rel], "duckdb").unwrap();
    assert_eq!(duck_tables.len(), 3);
    assert_eq!(
        duck_tables[1],
        "CREATE SEQUENCE IF NOT EXISTS seq_connects_connects_id;"
    );
    assert!(duck_tables[2].contains(
        "\"connects_id\" BIGINT DEFAULT nextval('seq_connects_connects_id') PRIMARY KEY"
    ));
    assert!(duck_tables[2].contains("\"id\" VARCHAR NOT NULL"));
}

#[test]
fn test_table_ddl_type_mapping_postgres_and_duckdb() {
    use voyager_ogm::schema::DdlDialect;

    // Direct mappings
    assert_eq!(
        field_type_to_sql(&FieldType::String, DdlDialect::Sql, "col").unwrap(),
        "TEXT"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::String, DdlDialect::DuckDb, "col").unwrap(),
        "VARCHAR"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Int64, DdlDialect::Sql, "col").unwrap(),
        "BIGINT"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Int64, DdlDialect::DuckDb, "col").unwrap(),
        "BIGINT"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Float64, DdlDialect::Sql, "col").unwrap(),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Float64, DdlDialect::DuckDb, "col").unwrap(),
        "DOUBLE PRECISION"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Boolean, DdlDialect::Sql, "col").unwrap(),
        "BOOLEAN"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Boolean, DdlDialect::DuckDb, "col").unwrap(),
        "BOOLEAN"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Date, DdlDialect::Sql, "col").unwrap(),
        "DATE"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Date, DdlDialect::DuckDb, "col").unwrap(),
        "DATE"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::DateTime, DdlDialect::Sql, "col").unwrap(),
        "TIMESTAMP WITH TIME ZONE"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::DateTime, DdlDialect::DuckDb, "col").unwrap(),
        "TIMESTAMPTZ"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Duration, DdlDialect::Sql, "col").unwrap(),
        "INTERVAL"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Duration, DdlDialect::DuckDb, "col").unwrap(),
        "INTERVAL"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Map, DdlDialect::Sql, "col").unwrap(),
        "JSONB"
    );
    assert_eq!(
        field_type_to_sql(&FieldType::Map, DdlDialect::DuckDb, "col").unwrap(),
        "JSON"
    );
    assert_eq!(
        field_type_to_sql(
            &FieldType::List(Box::new(FieldType::String)),
            DdlDialect::Sql,
            "col"
        )
        .unwrap(),
        "TEXT[]"
    );
    assert_eq!(
        field_type_to_sql(
            &FieldType::List(Box::new(FieldType::String)),
            DdlDialect::DuckDb,
            "col"
        )
        .unwrap(),
        "VARCHAR[]"
    );
    assert_eq!(
        field_type_to_sql(
            &FieldType::Custom("DECIMAL".to_string()),
            DdlDialect::Sql,
            "col"
        )
        .unwrap(),
        "NUMERIC"
    );
    assert_eq!(
        field_type_to_sql(
            &FieldType::Custom("BYTES".to_string()),
            DdlDialect::Sql,
            "col"
        )
        .unwrap(),
        "BYTEA"
    );
    assert_eq!(
        field_type_to_sql(
            &FieldType::Custom("BYTES".to_string()),
            DdlDialect::DuckDb,
            "col"
        )
        .unwrap(),
        "BLOB"
    );
    assert_eq!(
        field_type_to_sql(
            &FieldType::Custom("POINT".to_string()),
            DdlDialect::Sql,
            "col"
        )
        .unwrap(),
        "POINT"
    );
    assert_eq!(
        field_type_to_sql(
            &FieldType::Custom("VECTOR".to_string()),
            DdlDialect::DuckDb,
            "col"
        )
        .unwrap(),
        "FLOAT[]"
    );
}

#[test]
fn test_table_ddl_unsupported_types() {
    use voyager_ogm::schema::DdlDialect;

    // DuckDB does not natively support POINT without spatial extensions
    let err_point = field_type_to_sql(
        &FieldType::Custom("POINT".to_string()),
        DdlDialect::DuckDb,
        "geo_point",
    );
    assert!(err_point.is_err());
    let err_str = err_point.unwrap_err().to_string();
    assert!(err_str.contains("geo_point"));
    assert!(err_str.contains("POINT"));

    // PostgreSQL does not have VECTOR without pgvector
    let err_vec = field_type_to_sql(
        &FieldType::Custom("VECTOR".to_string()),
        DdlDialect::Sql,
        "embedding",
    );
    assert!(err_vec.is_err());
    let err_str = err_vec.unwrap_err().to_string();
    assert!(err_str.contains("embedding"));
    assert!(err_str.contains("VECTOR"));

    // Unrecognized custom type
    let err_unknown = field_type_to_sql(
        &FieldType::Custom("SPATIAL_GEOMETRY".to_string()),
        DdlDialect::Sql,
        "shape",
    );
    assert!(err_unknown.is_err());
    let err_str = err_unknown.unwrap_err().to_string();
    assert!(err_str.contains("shape"));
    assert!(err_str.contains("SPATIAL_GEOMETRY"));
}

#[test]
fn test_pgq_schema_ddl_execution_order() {
    let user = NodeSchema::new("User", vec!["User".to_string()])
        .with_field(FieldDescriptor::new("uid", FieldType::String).primary_key())
        .with_field(FieldDescriptor::new("email", FieldType::String).unique())
        .with_field(FieldDescriptor::new("score", FieldType::Int64).indexed());
    let follows = RelationshipSchema::new("Follows", "FOLLOWS")
        .with_endpoints(vec!["User".to_string()], vec!["User".to_string()])
        .with_field(FieldDescriptor::new("since", FieldType::Int64).indexed());

    let ddl = emit_pgq_schema_ddl("social_g", &[&user], &[&follows], "postgres").unwrap();

    // Must emit:
    // 1. Vertex table
    // 2. Edge table
    // 3. Constraints
    // 4. Indexes
    // 5. CREATE PROPERTY GRAPH
    assert_eq!(ddl.len(), 6);
    assert!(ddl[0].starts_with("CREATE TABLE IF NOT EXISTS \"user\""));
    assert!(ddl[1].starts_with("CREATE TABLE IF NOT EXISTS \"follows\""));
    assert!(ddl[2].starts_with("ALTER TABLE \"user\" ADD CONSTRAINT uq_user_email UNIQUE"));
    assert!(ddl[3].starts_with("CREATE INDEX IF NOT EXISTS idx_user_score ON \"user\""));
    assert!(ddl[4].starts_with("CREATE INDEX IF NOT EXISTS idx_follows_since ON \"follows\""));
    assert!(ddl[5].starts_with("CREATE PROPERTY GRAPH social_g"));
}

#[test]
fn test_duckpgq_schema_ddl_execution_order() {
    let user = NodeSchema::new("User", vec!["User".to_string()])
        .with_field(FieldDescriptor::new("uid", FieldType::String).primary_key())
        .with_field(FieldDescriptor::new("email", FieldType::String).unique())
        .with_field(FieldDescriptor::new("score", FieldType::Int64).indexed());
    let follows = RelationshipSchema::new("Follows", "FOLLOWS")
        .with_endpoints(vec!["User".to_string()], vec!["User".to_string()])
        .with_field(FieldDescriptor::new("since", FieldType::Int64).indexed());

    let ddl = emit_pgq_schema_ddl("duck_social_g", &[&user], &[&follows], "duckdb").unwrap();

    // Must emit in run order:
    // 1. Vertex table
    // 2. Sequence for edge surrogate key
    // 3. Edge table
    // 4. Constraint (CREATE UNIQUE INDEX)
    // 5. Node index
    // 6. Rel index
    // 7. CREATE PROPERTY GRAPH
    assert_eq!(ddl.len(), 7);
    assert!(ddl[0].starts_with("CREATE TABLE IF NOT EXISTS \"user\""));
    assert_eq!(ddl[1], "CREATE SEQUENCE IF NOT EXISTS seq_follows_id;");
    assert!(ddl[2].starts_with("CREATE TABLE IF NOT EXISTS \"follows\""));
    assert!(ddl[2].contains("\"id\" BIGINT DEFAULT nextval('seq_follows_id') PRIMARY KEY"));
    assert!(ddl[3].starts_with("CREATE UNIQUE INDEX IF NOT EXISTS uq_user_email ON \"user\""));
    assert!(ddl[4].starts_with("CREATE INDEX IF NOT EXISTS idx_user_score ON \"user\""));
    assert!(ddl[5].starts_with("CREATE INDEX IF NOT EXISTS idx_follows_since ON \"follows\""));
    assert!(ddl[6].starts_with("CREATE PROPERTY GRAPH duck_social_g"));
}

#[test]
fn test_duckpgq_property_graph_ddl() {
    let user = NodeSchema::new("User", vec!["User".to_string()])
        .with_field(FieldDescriptor::new("uid", FieldType::String).primary_key())
        .with_field(FieldDescriptor::new("name", FieldType::String));
    let follows = RelationshipSchema::new("Follows", "FOLLOWS")
        .with_endpoints(vec!["User".to_string()], vec!["User".to_string()])
        .with_field(FieldDescriptor::new("since", FieldType::Int64));

    let duck_ddl =
        emit_pgq_property_graph_ddl_with_dialect("duck_g", &[&user], &[&follows], "duckdb")
            .unwrap();
    // DuckPGQ requires omitting KEY and PROPERTIES
    assert!(!duck_ddl.contains("KEY (\"uid\")"));
    assert!(!duck_ddl.contains("PROPERTIES"));
    assert!(duck_ddl.contains("\"user\" LABEL \"User\""));
    assert!(duck_ddl.contains("SOURCE KEY (\"src_uid\") REFERENCES \"user\" (\"uid\")"));
    assert!(duck_ddl.contains("DESTINATION KEY (\"dst_uid\") REFERENCES \"user\" (\"uid\")"));
    assert!(duck_ddl.contains("LABEL \"FOLLOWS\""));
}
