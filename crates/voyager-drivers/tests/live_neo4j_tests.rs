//! Live integration tests for voyager-drivers BoltConnection against running Neo4j container.

use std::collections::HashMap;
use std::sync::Arc;

use voyager_drivers::bolt::BoltConnection;
use voyager_drivers::config::{ConnectionConfig, PoolConfig};
use voyager_drivers::engine::{AsyncConnection, ConnectionFactory};
use voyager_drivers::pool::ConnectionPool;

const NEO4J_URI: &str = "bolt://127.0.0.1:7687";
const NEO4J_USER: &str = "neo4j";
const NEO4J_PASS: &str = "voyagerpass123";

async fn is_neo4j_online() -> bool {
    let config = ConnectionConfig::from_uri(NEO4J_URI).with_auth(NEO4J_USER, NEO4J_PASS);
    match BoltConnection::connect(&config).await {
        Ok(mut conn) => {
            let _ = conn.close().await;
            true
        }
        Err(e) => {
            eprintln!("[DEBUG ERROR CONNECTING TO NEO4J]: {:?}", e);
            false
        }
    }
}

#[tokio::test]
async fn test_live_neo4j_bolt_crud_and_arrow() {
    if !is_neo4j_online().await {
        eprintln!("[SKIP] Live Neo4j instance is not reachable on 127.0.0.1:7687");
        return;
    }

    let config = ConnectionConfig::from_uri(NEO4J_URI).with_auth(NEO4J_USER, NEO4J_PASS);
    let mut conn = BoltConnection::connect(&config)
        .await
        .expect("Failed to connect to live Neo4j");

    assert!(conn.is_valid());
    assert_eq!(conn.version().major, 5);
    assert!(conn.version().minor >= 4);
    if let Some(agent) = conn.server_agent() {
        println!("Connected to live Neo4j agent: {}", agent);
    }

    // 1. Wipe test database
    let _ = conn
        .execute("MATCH (n:VoyagerTest) DETACH DELETE n", &HashMap::new())
        .await
        .expect("Failed to clean database");

    // 2. Insert nodes and relationship
    let mut create_params = HashMap::new();
    create_params.insert(
        "name1".to_string(),
        serde_json::Value::String("Alice".to_string()).into(),
    );
    create_params.insert(
        "age1".to_string(),
        serde_json::Value::Number(30.into()).into(),
    );
    create_params.insert(
        "name2".to_string(),
        serde_json::Value::String("Bob".to_string()).into(),
    );
    create_params.insert(
        "age2".to_string(),
        serde_json::Value::Number(25.into()).into(),
    );
    create_params.insert(
        "since".to_string(),
        serde_json::Value::Number(2022.into()).into(),
    );

    let insert_query = "
        CREATE (a:VoyagerTest:Person {name: $name1, age: $age1})
        CREATE (b:VoyagerTest:Person {name: $name2, age: $age2})
        CREATE (a)-[r:KNOWS {since: $since}]->(b)
        RETURN a.name AS a_name, b.name AS b_name
    ";

    let insert_result = conn
        .execute(insert_query, &create_params)
        .await
        .expect("Failed to insert live records");

    assert_eq!(insert_result.row_count(), 1);
    assert_eq!(insert_result.summary.nodes_created, 2);
    assert_eq!(insert_result.summary.relationships_created, 1);
    assert_eq!(insert_result.summary.properties_set, 5);

    // 3. Query records with Arrow conversion
    let mut query_params = HashMap::new();
    query_params.insert(
        "min_age".to_string(),
        serde_json::Value::Number(20.into()).into(),
    );

    let select_query = "
        MATCH (a:VoyagerTest:Person)-[r:KNOWS]->(b:VoyagerTest:Person)
        WHERE a.age >= $min_age
        RETURN a.name AS sender, a.age AS sender_age, r.since AS since_year, b.name AS recipient
    ";

    let select_result = conn
        .execute(select_query, &query_params)
        .await
        .expect("Failed to select records");

    assert_eq!(select_result.row_count(), 1);
    assert_eq!(
        select_result.columns,
        vec![
            "sender".to_string(),
            "sender_age".to_string(),
            "since_year".to_string(),
            "recipient".to_string()
        ]
    );

    let batch = select_result
        .into_single_batch()
        .unwrap()
        .expect("Missing Arrow RecordBatch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 4);

    // 4. Clean up
    let _ = conn
        .execute("MATCH (n:VoyagerTest) DETACH DELETE n", &HashMap::new())
        .await
        .expect("Failed to clean up test records");

    conn.close().await.expect("Failed to close live connection");
}

/// Factory for pooled live connections.
struct LiveBoltFactory {
    config: ConnectionConfig,
}

#[async_trait::async_trait]
impl ConnectionFactory<BoltConnection> for LiveBoltFactory {
    async fn create(&self) -> voyager_drivers::Result<BoltConnection> {
        BoltConnection::connect(&self.config).await
    }
}

#[tokio::test]
async fn test_live_neo4j_connection_pool_concurrency() {
    if !is_neo4j_online().await {
        eprintln!("[SKIP] Live Neo4j instance is not reachable on 127.0.0.1:7687");
        return;
    }

    let config = ConnectionConfig::from_uri(NEO4J_URI).with_auth(NEO4J_USER, NEO4J_PASS);
    let factory = Arc::new(LiveBoltFactory {
        config: config.clone(),
    });
    let pool_config = PoolConfig::new().with_min_idle(2).with_max_size(5);
    let pool = ConnectionPool::new(pool_config, factory);

    pool.warm_up().await.expect("Failed to warm up live pool");
    assert!(pool.idle_count() >= 2);

    let mut tasks = Vec::new();
    for i in 0..10 {
        let pool_clone = pool.clone();
        tasks.push(tokio::spawn(async move {
            let mut conn = pool_clone.acquire().await.expect("Live acquire failed");
            let mut params = HashMap::new();
            params.insert(
                "val".to_string(),
                serde_json::Value::Number(i.into()).into(),
            );
            let res = conn
                .execute("RETURN $val AS res", &params)
                .await
                .expect("Query failed");
            assert_eq!(res.row_count(), 1);
        }));
    }

    for task in tasks {
        task.await.expect("Task failed");
    }

    assert_eq!(pool.active_count(), 0);
}

#[tokio::test]
async fn test_live_neo4j_fluent_builder_with_clause_and_pagination() {
    if !is_neo4j_online().await {
        eprintln!("[SKIP] Live Neo4j instance is not reachable on 127.0.0.1:7687");
        return;
    }

    use voyager_ogm::builder::QueryBuilder;
    use voyager_ogm::emitters::CypherEmitter;
    use voyager_ogm::visitor::AstVisitor;

    let config = ConnectionConfig::from_uri(NEO4J_URI).with_auth(NEO4J_USER, NEO4J_PASS);
    let mut conn = BoltConnection::connect(&config)
        .await
        .expect("Failed to connect to live Neo4j");

    // Clean up
    let _ = conn
        .execute("MATCH (n:FluentNeo4jTest) DETACH DELETE n", &HashMap::new())
        .await;

    // Seed test records
    let seed_cypher = "CREATE (:FluentNeo4jTest:Person {name: 'Bob', age: 20}), (:FluentNeo4jTest:Person {name: 'David', age: 25}), (:FluentNeo4jTest:Person {name: 'Alice', age: 30}), (:FluentNeo4jTest:Person {name: 'Charlie', age: 40})";
    conn.execute(seed_cypher, &HashMap::new())
        .await
        .expect("Failed to seed records in Neo4j");

    // 1. Test WITH clause pipeline: MATCH -> WHERE -> WITH p -> WHERE -> RETURN
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["FluentNeo4jTest", "Person"])
        .where_gte("p", "age", 20)
        .r#with()
        .field("p", "", None::<&str>)
        .where_gt("p", "age", 25)
        .r#return()
        .field("p", "name", None::<&str>)
        .field("p", "age", None::<&str>)
        .order_by_asc("p", "age");

    let (arena, root) = builder.build();
    let mut emitter = CypherEmitter::new();
    let compiled = emitter
        .visit_query(&arena, root)
        .expect("Cypher emission failed");

    let mut query_params = HashMap::new();
    for (k, v) in &compiled.parameters {
        if let voyager_ogm::ast::LiteralValue::Int64(i) = v {
            query_params.insert(k.clone(), serde_json::json!(i).into());
        }
    }

    let res = conn
        .execute(&compiled.statement, &query_params)
        .await
        .expect("Failed to execute WITH clause query on Neo4j");

    assert_eq!(res.row_count(), 2);
    let batch = res
        .into_single_batch()
        .unwrap()
        .expect("Missing Arrow RecordBatch");
    assert_eq!(batch.num_rows(), 2);

    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let age_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(name_col.value(0), "Alice");
    assert_eq!(age_col.value(0), 30);
    assert_eq!(name_col.value(1), "Charlie");
    assert_eq!(age_col.value(1), 40);

    // 2. Test pagination: offset / skip and limit
    let mut builder2 = QueryBuilder::new();
    builder2
        .r#match()
        .node(Some("p"), vec!["FluentNeo4jTest", "Person"])
        .r#return()
        .field("p", "name", None::<&str>)
        .order_by_asc("p", "age")
        .skip(1)
        .limit(2);

    let (arena2, root2) = builder2.build();
    let mut emitter2 = CypherEmitter::new();
    let compiled2 = emitter2
        .visit_query(&arena2, root2)
        .expect("Cypher emission failed");

    let res2 = conn
        .execute(&compiled2.statement, &HashMap::new())
        .await
        .expect("Failed to execute pagination query on Neo4j");

    assert_eq!(res2.row_count(), 2);
    let batch2 = res2
        .into_single_batch()
        .unwrap()
        .expect("Missing Arrow RecordBatch");
    let name_col2 = batch2
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(name_col2.value(0), "David");
    assert_eq!(name_col2.value(1), "Alice");

    // Clean up
    let _ = conn
        .execute("MATCH (n:FluentNeo4jTest) DETACH DELETE n", &HashMap::new())
        .await;

    conn.close().await.expect("Failed to close connection");
}

#[tokio::test]
async fn test_live_neo4j_simultaneous_explain_and_profile_connections() {
    if !is_neo4j_online().await {
        eprintln!("[SKIP] Live Neo4j instance is not reachable on 127.0.0.1:7687");
        return;
    }

    use voyager_ogm::builder::QueryBuilder;
    use voyager_ogm::emitters::CypherEmitter;
    use voyager_ogm::visitor::AstVisitor;

    let config = ConnectionConfig::from_uri(NEO4J_URI).with_auth(NEO4J_USER, NEO4J_PASS);

    // Seed test records using a setup connection
    let mut setup_conn = BoltConnection::connect(&config)
        .await
        .expect("Failed to connect to live Neo4j for setup");

    let _ = setup_conn
        .execute(
            "MATCH (n:LiveSimultaneousNeo4jTest) DETACH DELETE n",
            &HashMap::new(),
        )
        .await;

    setup_conn
        .execute(
            "CREATE (:LiveSimultaneousNeo4jTest:Person {name: 'Alice', age: 30}), (:LiveSimultaneousNeo4jTest:Person {name: 'Bob', age: 25})",
            &HashMap::new(),
        )
        .await
        .expect("Failed to seed records in Neo4j");

    // 1. Build explain query
    let mut b_explain = QueryBuilder::new();
    b_explain
        .r#match()
        .node(Some("p"), vec!["LiveSimultaneousNeo4jTest", "Person"])
        .where_gte("p", "age", 20)
        .r#return()
        .field("p", "name", None::<&str>)
        .field("p", "age", None::<&str>)
        .order_by_asc("p", "age")
        .explain();

    let (arena_exp, root_exp) = b_explain.build();
    let mut emitter_exp = CypherEmitter::new();
    let compiled_exp = emitter_exp
        .visit_query(&arena_exp, root_exp)
        .expect("Explain Cypher emission failed");
    assert_eq!(compiled_exp.execution_mode.as_str(), "explain");
    assert!(compiled_exp.statement.starts_with("EXPLAIN "));

    // 2. Build profile query
    let mut b_profile = QueryBuilder::new();
    b_profile
        .r#match()
        .node(Some("p"), vec!["LiveSimultaneousNeo4jTest", "Person"])
        .where_gte("p", "age", 20)
        .r#return()
        .field("p", "name", None::<&str>)
        .field("p", "age", None::<&str>)
        .order_by_asc("p", "age")
        .profile();

    let (arena_prof, root_prof) = b_profile.build();
    let mut emitter_prof = CypherEmitter::new();
    let compiled_prof = emitter_prof
        .visit_query(&arena_prof, root_prof)
        .expect("Profile Cypher emission failed");
    assert_eq!(compiled_prof.execution_mode.as_str(), "profile");
    assert!(compiled_prof.statement.starts_with("PROFILE "));

    let mut exp_params = HashMap::new();
    for (k, v) in &compiled_exp.parameters {
        if let voyager_ogm::ast::LiteralValue::Int64(i) = v {
            exp_params.insert(k.clone(), serde_json::json!(i).into());
        }
    }

    let mut prof_params = HashMap::new();
    for (k, v) in &compiled_prof.parameters {
        if let voyager_ogm::ast::LiteralValue::Int64(i) = v {
            prof_params.insert(k.clone(), serde_json::json!(i).into());
        }
    }

    let config1 = config.clone();
    let config2 = config.clone();
    let stmt_exp = compiled_exp.statement.clone();
    let stmt_prof = compiled_prof.statement.clone();

    // 3. Spawn 2 separate connections simultaneously
    let handle1 = tokio::spawn(async move {
        let mut conn1 = BoltConnection::connect(&config1)
            .await
            .expect("Live connection 1 failed");
        let res = conn1.execute(&stmt_exp, &exp_params).await;
        let _ = conn1.close().await;
        res
    });

    let handle2 = tokio::spawn(async move {
        let mut conn2 = BoltConnection::connect(&config2)
            .await
            .expect("Live connection 2 failed");
        let res = conn2.execute(&stmt_prof, &prof_params).await;
        let _ = conn2.close().await;
        res
    });

    let (res1, res2) = tokio::join!(handle1, handle2);
    let exp_result = res1
        .expect("Task 1 panicked")
        .expect("Explain execution failed");
    let prof_result = res2
        .expect("Task 2 panicked")
        .expect("Profile execution failed");

    // Neo4j EXPLAIN plans without returning data rows
    assert_eq!(exp_result.row_count(), 0);

    // Neo4j PROFILE plans and executes returning data rows
    assert_eq!(prof_result.row_count(), 2);
    let batch = prof_result
        .into_single_batch()
        .unwrap()
        .expect("Missing RecordBatch from Profile");
    assert_eq!(batch.num_rows(), 2);

    let name_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "Bob");
    assert_eq!(name_col.value(1), "Alice");

    // Clean up
    let _ = setup_conn
        .execute(
            "MATCH (n:LiveSimultaneousNeo4jTest) DETACH DELETE n",
            &HashMap::new(),
        )
        .await;

    setup_conn
        .close()
        .await
        .expect("Failed to close setup connection");
}

#[tokio::test]
async fn test_live_neo4j_execute_rows_and_arrow_equivalence() {
    if !is_neo4j_online().await {
        eprintln!("[SKIP] Live Neo4j instance is not reachable on 127.0.0.1:7687");
        return;
    }

    use voyager_drivers::NativeClient;
    let uri = format!("bolt://{NEO4J_USER}:{NEO4J_PASS}@127.0.0.1:7687");
    let client = NativeClient::new(&uri, None).unwrap();

    let _ = client
        .execute("MATCH (n:ZzStage1Neo4j) DETACH DELETE n", &HashMap::new())
        .await;
    client
        .execute(
            "CREATE (a:ZzStage1Neo4j {name: 'Alice', age: 30})-[:ZZ_KNOWS {since: 2024}]->(b:ZzStage1Neo4j {name: 'Bob', age: 25})",
            &HashMap::new(),
        )
        .await
        .unwrap();

    let q_rich = "MATCH p=(a:ZzStage1Neo4j)-[r:ZZ_KNOWS]->(b:ZzStage1Neo4j) \
                  RETURN a.name AS name, a.age AS age, a, r, p, \
                  {k: [1, 'x']} AS m, point({latitude: 52.5, longitude: 13.4}) AS pt, \
                  datetime('2024-01-02T03:04:05+02:00') AS dt";
    let leg = client.execute(q_rich, &HashMap::new()).await.unwrap();
    let rows = client.execute_rows(q_rich, &HashMap::new()).await.unwrap();
    let arr = client.execute_arrow(q_rich, &HashMap::new()).await.unwrap();

    assert!(rows.batches.is_empty());
    assert!(rows.result_rows.is_some());
    assert!(rows.bolt_rows.is_none());
    assert_eq!(rows.row_count(), 1);
    assert!(arr.result_rows.is_none());
    assert!(arr.bolt_rows.is_none());
    assert_eq!(
        rows.to_record_batch().unwrap().unwrap(),
        leg.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        arr.to_record_batch().unwrap().unwrap(),
        leg.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(rows.result_rows, leg.result_rows);

    // 0-row check + tx check
    let mut tx = client.begin_transaction().await.unwrap();
    let q_empty = "MATCH (n:ZzStage1Neo4jMissing) RETURN n.name AS name";
    let tx_leg = tx.execute(q_empty, &HashMap::new()).await.unwrap();
    let tx_rows = tx.execute_rows(q_empty, &HashMap::new()).await.unwrap();
    let tx_arr = tx.execute_arrow(q_empty, &HashMap::new()).await.unwrap();
    assert_eq!(tx_rows.row_count(), 0);
    assert_eq!(
        tx_rows.to_record_batch().unwrap().unwrap(),
        tx_leg.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        tx_arr.to_record_batch().unwrap().unwrap(),
        tx_leg.to_record_batch().unwrap().unwrap()
    );
    tx.commit().await.unwrap();

    let _ = client
        .execute("MATCH (n:ZzStage1Neo4j) DETACH DELETE n", &HashMap::new())
        .await;
    client.close().await;
}

#[tokio::test]
async fn test_live_neo4j_stream_lifecycle_early_close_tx_qid_and_timeout_cancel() {
    if !is_neo4j_online().await {
        eprintln!("[SKIP] Live Neo4j instance is not reachable on 127.0.0.1:7687");
        return;
    }

    use std::time::Duration;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, NetError, ResultValue, StreamOptions};

    let uri = format!("bolt://{NEO4J_USER}:{NEO4J_PASS}@127.0.0.1:7687");
    let client = NativeClient::new(
        &uri,
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_acquire_timeout(Duration::from_secs(5)),
        ),
    )
    .unwrap();

    // 1. Full multi-chunk stream (2,500 rows, fetch_size=1000) -> 3 chunks, eager lease release
    let mut full_stream = client
        .stream(
            "UNWIND range(1, 2500) AS i RETURN i",
            &HashMap::new(),
            StreamOptions::new(1_000),
        )
        .await
        .unwrap();
    assert!(full_stream.is_server_streaming());
    assert_eq!(full_stream.columns(), &["i".to_string()]);
    assert_eq!(client.pool().active_count(), 1);

    let mut chunk_sizes = Vec::new();
    let mut last_val = 0i64;
    while let Some(chunk) = full_stream.next_chunk().await.unwrap() {
        chunk_sizes.push(chunk.num_rows());
        if let Some(row) = chunk.rows.last()
            && let ResultValue::Int(v) = row[0]
        {
            last_val = v;
        }
        assert!(chunk.to_record_batch().is_ok());
    }
    assert_eq!(chunk_sizes, vec![1_000, 1_000, 500]);
    assert_eq!(last_val, 2_500);
    assert!(full_stream.is_closed());
    assert_eq!(client.pool().active_count(), 0);
    assert_eq!(client.pool().idle_count(), 1);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 2. Early close after chunk 1 reuses the exact same pooled connection
    let mut early_stream = client
        .stream(
            "UNWIND range(1, 10000) AS i RETURN i",
            &HashMap::new(),
            StreamOptions::new(100),
        )
        .await
        .unwrap();
    let c1 = early_stream.next_chunk().await.unwrap().expect("chunk 1");
    assert_eq!(c1.num_rows(), 100);
    early_stream.close().await.unwrap();
    assert_eq!(client.pool().active_count(), 0);
    assert_eq!(client.pool().idle_count(), 1);
    let after_early = client
        .execute_rows("RETURN 42 AS n", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(after_early.row_count(), 1);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 3. Close before chunk 1 drains & discards cleanly, reusing the same connection
    let mut unread_stream = client
        .stream(
            "UNWIND range(1, 500) AS i RETURN i",
            &HashMap::new(),
            StreamOptions::new(100),
        )
        .await
        .unwrap();
    unread_stream.close().await.unwrap();
    let after_unread = client
        .execute_rows("RETURN 43 AS n", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(after_unread.row_count(), 1);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 4. Explicit transaction stream: qid propagation, fail-fast guard, close, follow-up & commit
    let mut tx = client.begin_transaction().await.unwrap();
    let mut cursor = tx
        .open_stream_cursor(
            "UNWIND range(1, 500) AS i RETURN i",
            &HashMap::new(),
            &StreamOptions::new(100),
        )
        .await
        .unwrap();
    let err_concurrent = tx
        .execute_rows("RETURN 1 AS x", &HashMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(err_concurrent, NetError::TransactionError(ref m) if m.contains("active stream"))
    );
    let tx_c1 = tx.next_stream_chunk(&mut cursor).await.unwrap().unwrap();
    let tx_c2 = tx.next_stream_chunk(&mut cursor).await.unwrap().unwrap();
    assert_eq!(tx_c1.num_rows(), 100);
    assert_eq!(tx_c2.num_rows(), 100);
    tx.close_stream_cursor(&mut cursor).await.unwrap();
    let tx_after = tx
        .execute_rows("RETURN 99 AS x", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(tx_after.row_count(), 1);
    tx.commit().await.unwrap();
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 5. Mid-stream FAILURE recovers via deferred RESET on the same pooled connection
    let mut err_stream = client
        .stream(
            "UNWIND [1, 2, 0] AS x RETURN 10 / x AS q",
            &HashMap::new(),
            StreamOptions::new(1),
        )
        .await
        .unwrap();
    let ok1 = err_stream.next_chunk().await.unwrap().unwrap();
    let ok2 = err_stream.next_chunk().await.unwrap().unwrap();
    assert_eq!(ok1.num_rows(), 1);
    assert_eq!(ok2.num_rows(), 1);
    let mid_err = err_stream.next_chunk().await.unwrap_err();
    assert!(matches!(mid_err, NetError::ProtocolError(_)));
    let after_err = client
        .execute_rows("RETURN 7 AS ok", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(after_err.row_count(), 1);
    assert_eq!(client.pool().metrics().connections_created, 1);
    assert_eq!(client.pool().metrics().connections_failed_health_check, 0);

    // 6. Mid-flight tokio::time::timeout cancellation replaces broken connection transparently
    let mut slow_stream = client
        .stream(
            "UNWIND range(1, 100) AS a UNWIND range(1, 200000) AS b WITH a, b WHERE (a + b) % 200000 = 0 RETURN a, b",
            &HashMap::new(),
            StreamOptions::new(1),
        )
        .await
        .unwrap();
    let _first_slow = slow_stream.next_chunk().await.unwrap().expect("chunk 1");
    let cancel_res =
        tokio::time::timeout(Duration::from_micros(100), slow_stream.next_chunk()).await;
    assert!(
        cancel_res.is_err(),
        "expected mid-flight timeout cancellation on chunk 2"
    );
    drop(slow_stream);
    assert_eq!(client.pool().total_count(), 0);
    let recovered = client
        .execute_rows("RETURN 1 AS ok", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(recovered.row_count(), 1);
    assert_eq!(client.pool().metrics().connections_created, 2);

    client.close().await;
}

#[tokio::test]
async fn test_live_neo4j_stream_ttfb_1k_vs_100k() {
    if !is_neo4j_online().await {
        eprintln!("[SKIP] Live Neo4j instance is not reachable on 127.0.0.1:7687");
        return;
    }

    use std::time::Instant;
    use voyager_drivers::{NativeClient, StreamOptions};

    let uri = format!("bolt://{NEO4J_USER}:{NEO4J_PASS}@127.0.0.1:7687");
    let client = NativeClient::new(&uri, None).unwrap();
    // Warm up pool connection so handshake isn't billed to TTFB
    client.ping().await.unwrap();

    let t0_1k = Instant::now();
    let mut s_1k = client
        .stream(
            "UNWIND range(1, 1000) AS i RETURN i",
            &HashMap::new(),
            StreamOptions::new(1_000),
        )
        .await
        .unwrap();
    let first_1k = s_1k.next_chunk().await.unwrap().unwrap();
    let ttfb_1k = t0_1k.elapsed();
    assert_eq!(first_1k.num_rows(), 1_000);
    while s_1k.next_chunk().await.unwrap().is_some() {}

    let t0_100k = Instant::now();
    let mut s_100k = client
        .stream(
            "UNWIND range(1, 100000) AS i RETURN i",
            &HashMap::new(),
            StreamOptions::new(1_000),
        )
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
        "[neo4j stream] ttfb_1k={:?}, ttfb_100k={:?}, ttlb_100k={:?}",
        ttfb_1k, ttfb_100k, ttlb_100k
    );
    assert!(
        ttfb_100k.as_secs_f64() < ttlb_100k.as_secs_f64() * 0.25,
        "100k TTFB {:?} was not well below TTLB {:?}",
        ttfb_100k,
        ttlb_100k
    );

    client.close().await;
}
