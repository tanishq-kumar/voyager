use std::collections::HashMap;
use voyager_drivers::postgres::{AgeValue, PostgresConnection, PostgresTransaction, parse_agtype};
use voyager_drivers::{AsyncConnection, ParsedUri};

const AGE_URI: &str = "age://postgres:voyagerpass123@127.0.0.1:5455/voyager_graph";

async fn get_age_connection() -> Option<PostgresConnection> {
    let parsed = match ParsedUri::parse(AGE_URI) {
        Ok(p) => p,
        Err(_) => return None,
    };

    match PostgresConnection::connect(&parsed).await {
        Ok(conn) => Some(conn),
        Err(e) => {
            eprintln!("Skipping live AGE tests (container not accessible): {}", e);
            None
        }
    }
}

#[tokio::test]
async fn test_live_age_ping_and_parameters() {
    let mut conn = match get_age_connection().await {
        Some(c) => c,
        None => return,
    };

    assert!(conn.ping().await.is_ok());
    assert!(conn.is_valid());
    assert!(!conn.is_in_transaction());

    let params = conn.server_parameters();
    assert!(params.contains_key("server_version"));
    assert!(params.contains_key("client_encoding"));
}

#[tokio::test]
async fn test_live_age_graph_lifecycle_and_cypher() {
    let mut conn = match get_age_connection().await {
        Some(c) => c,
        None => return,
    };

    let graph_name = "net_test_graph";

    // Clean up graph if already existing
    let _ = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.drop_graph('{}', true);",
            graph_name
        ))
        .await;

    // 1. Create Graph
    let create_res = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.create_graph('{}');",
            graph_name
        ))
        .await;
    assert!(
        create_res.is_ok(),
        "Failed to create graph: {:?}",
        create_res
    );

    // 2. Create Vertices via Cypher
    let create_nodes_sql = format!(
        "SELECT * FROM cypher('{}', $$ CREATE (a:Person {{name: 'Alice', age: 30}}), (b:Person {{name: 'Bob', age: 35}}) $$) as (v agtype);",
        graph_name
    );
    let create_nodes_res = conn.execute_simple(&create_nodes_sql).await;
    assert!(
        create_nodes_res.is_ok(),
        "Failed to create vertices: {:?}",
        create_nodes_res
    );

    // 3. Create Edge via Cypher
    let create_edge_sql = format!(
        "SELECT * FROM cypher('{}', $$ MATCH (a:Person {{name: 'Alice'}}), (b:Person {{name: 'Bob'}}) CREATE (a)-[r:KNOWS {{since: 2022}}]->(b) $$) as (v agtype);",
        graph_name
    );
    let create_edge_res = conn.execute_simple(&create_edge_sql).await;
    assert!(
        create_edge_res.is_ok(),
        "Failed to create edge: {:?}",
        create_edge_res
    );

    // 4. Query Vertices and Parse agtype
    let query_vertices_sql = format!(
        "SELECT * FROM cypher('{}', $$ MATCH (p:Person) RETURN p ORDER BY p.name $$) as (p agtype);",
        graph_name
    );
    let query_res = conn.execute_simple(&query_vertices_sql).await.unwrap();
    assert_eq!(query_res.columns, vec!["p".to_string()]);
    assert_eq!(query_res.row_count(), 2);

    let batch = &query_res.batches[0];
    let string_array = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();

    let p1_raw = string_array.value(0);
    let p2_raw = string_array.value(1);

    let p1_val = parse_agtype(p1_raw).unwrap();
    let p2_val = parse_agtype(p2_raw).unwrap();

    if let AgeValue::Vertex(v1) = p1_val {
        assert_eq!(v1.label, "Person");
        assert_eq!(v1.properties.get("name").unwrap(), "Alice");
        assert_eq!(v1.properties.get("age").unwrap(), 30);
    } else {
        panic!("Expected AgeValue::Vertex for p1");
    }

    if let AgeValue::Vertex(v2) = p2_val {
        assert_eq!(v2.label, "Person");
        assert_eq!(v2.properties.get("name").unwrap(), "Bob");
        assert_eq!(v2.properties.get("age").unwrap(), 35);
    } else {
        panic!("Expected AgeValue::Vertex for p2");
    }

    // 5. Query Scalar Projections (Arrow Column Types)
    let query_scalars_sql = format!(
        "SELECT * FROM cypher('{}', $$ MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, r.since, b.name $$) as (a_name agtype, since agtype, b_name agtype);",
        graph_name
    );
    let scalar_res = conn.execute_simple(&query_scalars_sql).await.unwrap();
    assert_eq!(scalar_res.columns.len(), 3);
    assert_eq!(scalar_res.row_count(), 1);

    // 6. Query Path
    let query_path_sql = format!(
        "SELECT * FROM cypher('{}', $$ MATCH p = (a:Person)-[:KNOWS]->(b:Person) RETURN p $$) as (p agtype);",
        graph_name
    );
    let path_res = conn.execute_simple(&query_path_sql).await.unwrap();
    assert_eq!(path_res.row_count(), 1);
    let path_batch = &path_res.batches[0];
    let path_str_col = path_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let path_raw = path_str_col.value(0);
    let path_val = parse_agtype(path_raw).unwrap();

    if let AgeValue::Path(path) = path_val {
        assert_eq!(path.vertices.len(), 2);
        assert_eq!(path.edges.len(), 1);
        assert_eq!(path.edges[0].label, "KNOWS");

        let bolt_path = path.to_bolt_path();
        assert_eq!(bolt_path.nodes.len(), 2);
        assert_eq!(bolt_path.relationships.len(), 1);
    } else {
        panic!("Expected AgeValue::Path");
    }

    // 7. Teardown / Drop Graph
    let drop_res = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.drop_graph('{}', true);",
            graph_name
        ))
        .await;
    assert!(drop_res.is_ok());
}

#[tokio::test]
async fn test_live_age_transaction_rollback() {
    let mut conn = match get_age_connection().await {
        Some(c) => c,
        None => return,
    };

    let graph_name = "net_tx_graph";
    let _ = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.drop_graph('{}', true);",
            graph_name
        ))
        .await;
    let _ = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.create_graph('{}');",
            graph_name
        ))
        .await;

    // Start transaction, create vertex, and roll back
    {
        let mut tx = PostgresTransaction::begin(&mut conn).await.unwrap();
        let create_sql = format!(
            "SELECT * FROM cypher('{}', $$ CREATE (:Person {{name: 'Ghost'}}) $$) as (v agtype);",
            graph_name
        );
        let _ = tx.execute(&create_sql, &HashMap::new()).await.unwrap();
        tx.rollback().await.unwrap();
    }

    // Verify node does not exist
    let verify_sql = format!(
        "SELECT * FROM cypher('{}', $$ MATCH (p:Person {{name: 'Ghost'}}) RETURN p $$) as (p agtype);",
        graph_name
    );
    let res = conn.execute_simple(&verify_sql).await.unwrap();
    assert_eq!(res.row_count(), 0);

    let _ = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.drop_graph('{}', true);",
            graph_name
        ))
        .await;
}

#[tokio::test]
async fn test_live_age_fluent_builder_with_clause() {
    let mut conn = match get_age_connection().await {
        Some(c) => c,
        None => return,
    };

    use voyager_ogm::builder::QueryBuilder;
    use voyager_ogm::emitters::CypherEmitter;
    use voyager_ogm::visitor::AstVisitor;

    let graph_name = "net_fluent_age_graph";
    let _ = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.drop_graph('{}', true);",
            graph_name
        ))
        .await;
    let _ = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.create_graph('{}');",
            graph_name
        ))
        .await;

    // Seed test nodes
    let seed_sql = format!(
        "SELECT * FROM cypher('{}', $$ CREATE (:Person {{name: 'Bob', age: 20}}), (:Person {{name: 'David', age: 25}}), (:Person {{name: 'Alice', age: 30}}), (:Person {{name: 'Charlie', age: 40}}) $$) as (v agtype);",
        graph_name
    );
    conn.execute_simple(&seed_sql).await.unwrap();

    // Build query with WITH clause: MATCH -> WHERE -> WITH p -> WHERE -> RETURN
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["Person"])
        .where_gte("p", "age", 20)
        .r#with()
        .field("p", "", None::<&str>)
        .where_gt("p", "age", 25)
        .r#return()
        .field("p", "name", None::<&str>)
        .order_by_asc("p", "age");

    let (arena, root) = builder.build();
    let mut emitter = CypherEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("Cypher emission failed");

    let mut stmt = compiled.statement.clone();
    for (k, v) in &compiled.parameters {
        if let voyager_ogm::ast::LiteralValue::Int64(i) = v {
            stmt = stmt.replace(&format!("${k}"), &i.to_string());
        }
    }

    let query_sql = format!(
        "SELECT * FROM cypher('{}', $$ {} $$) as (name agtype);",
        graph_name, stmt
    );
    let res = conn.execute_simple(&query_sql).await.unwrap();
    assert_eq!(res.row_count(), 2);

    let batch = &res.batches[0];
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();

    let name1 = parse_agtype(col.value(0)).unwrap();
    let name2 = parse_agtype(col.value(1)).unwrap();
    assert_eq!(name1, AgeValue::String("Alice".to_string()));
    assert_eq!(name2, AgeValue::String("Charlie".to_string()));

    // Clean up
    let _ = conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.drop_graph('{}', true);",
            graph_name
        ))
        .await;
}

#[tokio::test]
async fn test_live_age_simultaneous_explain_and_profile_connections() {
    let mut setup_conn = match get_age_connection().await {
        Some(c) => c,
        None => return,
    };

    use voyager_ogm::builder::QueryBuilder;
    use voyager_ogm::emitters::AgeEmitter;
    use voyager_ogm::visitor::AstVisitor;

    let graph_name = "net_simultaneous_age_graph";

    // Clean up graph if already existing and create fresh
    let _ = setup_conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.drop_graph('{}', true);",
            graph_name
        ))
        .await;

    setup_conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.create_graph('{}');",
            graph_name
        ))
        .await
        .unwrap();

    // Seed test nodes
    let seed_sql = format!(
        "SELECT * FROM cypher('{}', $$ CREATE (:Person {{name: 'Alice', age: 30}}), (:Person {{name: 'Bob', age: 25}}) $$) as (v agtype);",
        graph_name
    );
    setup_conn.execute_simple(&seed_sql).await.unwrap();

    // 1. Build explain query with AgeEmitter
    let mut b_explain = QueryBuilder::new();
    b_explain
        .r#match()
        .node(Some("p"), vec!["Person"])
        .r#return()
        .field("p", "name", None::<&str>)
        .explain();

    let (arena_exp, root_exp) = b_explain.build();
    let mut emitter_exp = AgeEmitter::new(graph_name);
    let compiled_exp = emitter_exp
        .visit_query(&arena_exp, root_exp)
        .expect("AGE explain emission failed");
    assert_eq!(compiled_exp.execution_mode.as_str(), "explain");
    assert!(compiled_exp.statement.starts_with("EXPLAIN SELECT"));

    // 2. Build profile query with AgeEmitter
    let mut b_profile = QueryBuilder::new();
    b_profile
        .r#match()
        .node(Some("p"), vec!["Person"])
        .r#return()
        .field("p", "name", None::<&str>)
        .profile();

    let (arena_prof, root_prof) = b_profile.build();
    let mut emitter_prof = AgeEmitter::new(graph_name);
    let compiled_prof = emitter_prof
        .visit_query(&arena_prof, root_prof)
        .expect("AGE profile emission failed");
    assert_eq!(compiled_prof.execution_mode.as_str(), "profile");
    assert!(
        compiled_prof
            .statement
            .starts_with("EXPLAIN ANALYZE SELECT")
    );

    let stmt_exp = compiled_exp.statement.clone();
    let stmt_prof = compiled_prof.statement.clone();

    // 3. Run 2 connections simultaneously
    let handle1 = tokio::spawn(async move {
        let mut conn1 = get_age_connection().await.expect("AGE connection 1 failed");
        conn1.execute_simple(&stmt_exp).await
    });

    let handle2 = tokio::spawn(async move {
        let mut conn2 = get_age_connection().await.expect("AGE connection 2 failed");
        conn2.execute_simple(&stmt_prof).await
    });

    let (res1, res2) = tokio::join!(handle1, handle2);
    let exp_result = res1
        .expect("Task 1 panicked")
        .expect("AGE explain execution failed");
    let prof_result = res2
        .expect("Task 2 panicked")
        .expect("AGE profile execution failed");

    // Both queries executed concurrently on PostgreSQL/AGE returning query plans
    assert!(exp_result.row_count() > 0);
    assert!(prof_result.row_count() > 0);

    // Clean up
    let _ = setup_conn
        .execute_simple(&format!(
            "SELECT * FROM ag_catalog.drop_graph('{}', true);",
            graph_name
        ))
        .await;
}

#[tokio::test]
async fn test_live_age_execute_rows_and_arrow_equivalence() {
    if get_age_connection().await.is_none() {
        return;
    }

    use voyager_drivers::NativeClient;
    let client = NativeClient::new(AGE_URI, None).unwrap();
    let graph_name = "zz_stage1_462_age";

    let _ = client
        .execute(
            &format!("SELECT * FROM ag_catalog.drop_graph('{graph_name}', true);"),
            &HashMap::new(),
        )
        .await;
    client
        .execute(
            &format!("SELECT * FROM ag_catalog.create_graph('{graph_name}');"),
            &HashMap::new(),
        )
        .await
        .unwrap();

    client
        .execute(
            &format!(
                "SELECT * FROM cypher('{graph_name}', $$ CREATE (a:Person {{name: 'Alice', age: 30}})-[:KNOWS {{since: 2024}}]->(b:Person {{name: 'Bob', age: 25}}) $$) as (v agtype);"
            ),
            &HashMap::new(),
        )
        .await
        .unwrap();

    let q_rich = format!(
        "SELECT * FROM cypher('{graph_name}', $$ MATCH p=(a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, a.age, a, r, p $$) as (name agtype, age agtype, a agtype, r agtype, p agtype);"
    );
    let leg = client.execute(&q_rich, &HashMap::new()).await.unwrap();
    let rows = client.execute_rows(&q_rich, &HashMap::new()).await.unwrap();
    let arr = client
        .execute_arrow(&q_rich, &HashMap::new())
        .await
        .unwrap();

    assert!(rows.batches.is_empty());
    assert!(rows.bolt_rows.is_none());
    assert!(arr.result_rows.is_none());
    assert!(arr.bolt_rows.is_none());
    assert_eq!(rows.row_count(), 1);
    assert_eq!(
        rows.to_record_batch().unwrap().unwrap(),
        leg.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        arr.to_record_batch().unwrap().unwrap(),
        leg.to_record_batch().unwrap().unwrap()
    );

    let _ = client
        .execute(
            &format!("SELECT * FROM ag_catalog.drop_graph('{graph_name}', true);"),
            &HashMap::new(),
        )
        .await;
    client.close().await;
}

#[tokio::test]
async fn test_live_age_stream_lifecycle_early_close_tx_and_hints() {
    if get_age_connection().await.is_none() {
        return;
    }

    use std::time::Duration;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::value::result::ResultValue;
    use voyager_drivers::{NativeClient, NetError, ParamValue, StreamOptions};

    let client = NativeClient::new(
        AGE_URI,
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_acquire_timeout(Duration::from_secs(5)),
        ),
    )
    .unwrap();

    let graph_name = "zz_stage4_458_age";
    let _ = client
        .execute_rows(
            &format!("SELECT * FROM ag_catalog.drop_graph('{graph_name}', true);"),
            &HashMap::new(),
        )
        .await;
    client
        .execute_rows(
            &format!("SELECT * FROM ag_catalog.create_graph('{graph_name}');"),
            &HashMap::new(),
        )
        .await
        .unwrap();
    client
        .execute_rows(
            &format!(
                "SELECT * FROM cypher('{graph_name}', $$ UNWIND range(1, 250) AS i CREATE (:Person {{idx: i, name: 'P' + toString(i)}}) $$) as (v agtype);"
            ),
            &HashMap::new(),
        )
        .await
        .unwrap();

    async fn fetch_pid(c: &NativeClient) -> i64 {
        let res = c
            .execute_rows("SELECT pg_backend_pid() AS pid", &HashMap::new())
            .await
            .unwrap();
        match res.result_rows.as_ref().unwrap()[0][0] {
            ResultValue::Int(pid) => pid,
            ref other => panic!("expected Int backend_pid, got {other:?}"),
        }
    }
    let initial_pid = fetch_pid(&client).await;

    // 1. Multi-chunk AGE stream matches execute_rows().to_record_batch() schema/hints
    let q_vertices = format!(
        "SELECT * FROM cypher('{graph_name}', $$ MATCH (p:Person) RETURN p.idx, p ORDER BY p.idx $$) as (idx agtype, p agtype);"
    );
    let expected_batch = client
        .execute_rows(&q_vertices, &HashMap::new())
        .await
        .unwrap()
        .to_record_batch()
        .unwrap()
        .unwrap();

    let mut full_stream = client
        .stream(&q_vertices, &HashMap::new(), StreamOptions::new(100))
        .await
        .unwrap();
    assert!(full_stream.is_server_streaming());
    assert_eq!(full_stream.columns(), &["idx".to_string(), "p".to_string()]);
    let mut chunk_sizes = Vec::new();
    while let Some(chunk) = full_stream.next_chunk().await.unwrap() {
        chunk_sizes.push(chunk.num_rows());
        let batch = chunk.to_record_batch().unwrap();
        assert_eq!(batch.schema(), expected_batch.schema());
        assert!(matches!(chunk.rows[0][1], ResultValue::Node(_)));
    }
    assert_eq!(chunk_sizes, vec![100, 100, 50]);
    assert_eq!(fetch_pid(&client).await, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 2. Early close on parameterized AGE %s stream reuses same backend_pid
    let q_param = format!(
        "SELECT * FROM cypher('{graph_name}', $$ MATCH (p:Person) WHERE p.idx >= $min_idx RETURN p $$ , %s) as (p agtype);"
    );
    let params = HashMap::from([(
        "min_idx".to_string(),
        ParamValue::Json(serde_json::json!(1)),
    )]);
    let mut early_stream = client
        .stream(&q_param, &params, StreamOptions::new(50))
        .await
        .unwrap();
    let c1 = early_stream.next_chunk().await.unwrap().unwrap();
    assert_eq!(c1.num_rows(), 50);
    early_stream.close().await.unwrap();
    assert_eq!(fetch_pid(&client).await, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 3. Explicit transaction AGE stream + fail-fast + follow-up + COMMIT
    let mut tx = client.begin_transaction().await.unwrap();
    let mut cursor = tx
        .open_stream_cursor(&q_param, &params, &StreamOptions::new(50))
        .await
        .unwrap();
    let err_concurrent = tx
        .execute_rows("SELECT 1 AS ok", &HashMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(err_concurrent, NetError::TransactionError(ref m) if m.contains("active stream"))
    );
    let tx_c1 = tx.next_stream_chunk(&mut cursor).await.unwrap().unwrap();
    assert_eq!(tx_c1.num_rows(), 50);
    tx.close_stream_cursor(&mut cursor).await.unwrap();
    let follow = tx
        .execute_rows(
            &format!(
                "SELECT * FROM cypher('{graph_name}', $$ MATCH (p:Person) WHERE p.idx = 1 RETURN p.name $$) as (name agtype);"
            ),
            &HashMap::new(),
        )
        .await
        .unwrap();
    assert_eq!(follow.row_count(), 1);
    tx.commit().await.unwrap();
    assert_eq!(fetch_pid(&client).await, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 1);

    let _ = client
        .execute_rows(
            &format!("SELECT * FROM ag_catalog.drop_graph('{graph_name}', true);"),
            &HashMap::new(),
        )
        .await;
    client.close().await;
}

#[tokio::test]
async fn test_live_age_stream_ttfb_1k_vs_100k() {
    if get_age_connection().await.is_none() {
        return;
    }

    use std::time::Instant;
    use voyager_drivers::{NativeClient, StreamOptions};

    let client = NativeClient::new(AGE_URI, None).unwrap();
    client.ping().await.unwrap();

    let graph_name = "zz_stage4_458_age_ttfb";
    let _ = client
        .execute_rows(
            &format!("SELECT * FROM ag_catalog.drop_graph('{graph_name}', true);"),
            &HashMap::new(),
        )
        .await;
    client
        .execute_rows(
            &format!("SELECT * FROM ag_catalog.create_graph('{graph_name}');"),
            &HashMap::new(),
        )
        .await
        .unwrap();

    let q_1k = format!(
        "SELECT * FROM cypher('{graph_name}', $$ UNWIND range(1, 1000) AS i RETURN i $$) as (i agtype);"
    );
    let t0_1k = Instant::now();
    let mut s_1k = client
        .stream(&q_1k, &HashMap::new(), StreamOptions::new(1_000))
        .await
        .unwrap();
    let first_1k = s_1k.next_chunk().await.unwrap().unwrap();
    let ttfb_1k = t0_1k.elapsed();
    assert_eq!(first_1k.num_rows(), 1_000);
    while s_1k.next_chunk().await.unwrap().is_some() {}

    let q_100k = format!(
        "SELECT * FROM cypher('{graph_name}', $$ UNWIND range(1, 100000) AS i RETURN i $$) as (i agtype);"
    );
    let t0_100k = Instant::now();
    let mut s_100k = client
        .stream(&q_100k, &HashMap::new(), StreamOptions::new(1_000))
        .await
        .unwrap();
    let first_100k = s_100k.next_chunk().await.unwrap().unwrap();
    let ttfb_100k = t0_100k.elapsed();
    assert_eq!(first_100k.num_rows(), 1_000);
    let mut total_100k = first_100k.num_rows();
    while let Some(c) = s_100k.next_chunk().await.unwrap() {
        total_100k += c.num_rows();
    }
    let ttlb_100k = t0_100k.elapsed();
    assert_eq!(total_100k, 100_000);

    eprintln!(
        "[age stream] ttfb_1k={:?}, ttfb_100k={:?}, ttlb_100k={:?}",
        ttfb_1k, ttfb_100k, ttlb_100k
    );
    assert!(
        ttfb_100k.as_secs_f64() < ttlb_100k.as_secs_f64() * 0.25,
        "100k AGE TTFB {:?} was not well below TTLB {:?}",
        ttfb_100k,
        ttlb_100k
    );

    let _ = client
        .execute_rows(
            &format!("SELECT * FROM ag_catalog.drop_graph('{graph_name}', true);"),
            &HashMap::new(),
        )
        .await;
    client.close().await;
}
