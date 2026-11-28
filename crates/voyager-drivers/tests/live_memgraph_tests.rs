//! Live integration tests for voyager-drivers BoltConnection against running Memgraph container on port 7688.

use std::collections::HashMap;

use voyager_drivers::bolt::BoltConnection;
use voyager_drivers::config::ConnectionConfig;
use voyager_drivers::engine::AsyncConnection;

const MEMGRAPH_URI: &str = "bolt://127.0.0.1:7688";

async fn is_memgraph_online() -> bool {
    let config = ConnectionConfig::from_uri(MEMGRAPH_URI);
    match BoltConnection::connect(&config).await {
        Ok(mut conn) => {
            let _ = conn.close().await;
            true
        }
        Err(e) => {
            eprintln!("[DEBUG ERROR CONNECTING TO MEMGRAPH]: {:?}", e);
            false
        }
    }
}

#[tokio::test]
async fn test_live_memgraph_bolt_crud_and_arrow() {
    if !is_memgraph_online().await {
        eprintln!("[SKIP] Live Memgraph instance is not reachable on 127.0.0.1:7688");
        return;
    }

    let config = ConnectionConfig::from_uri(MEMGRAPH_URI);
    let mut conn = BoltConnection::connect(&config)
        .await
        .expect("Failed to connect to live Memgraph");

    assert!(conn.is_valid());
    assert_eq!(conn.version().major, 5);
    assert_eq!(conn.version().minor, 2);
    if let Some(agent) = conn.server_agent() {
        println!("Connected to live Memgraph agent: {}", agent);
    }

    // 1. Wipe test database
    let _ = conn
        .execute("MATCH (n:MemgraphTest) DETACH DELETE n", &HashMap::new())
        .await
        .expect("Failed to clean database");

    // 2. Insert nodes and relationship
    let mut create_params = HashMap::new();
    create_params.insert(
        "name1".to_string(),
        serde_json::Value::String("Charlie".to_string()).into(),
    );
    create_params.insert(
        "score".to_string(),
        serde_json::Value::Number(95.into()).into(),
    );

    let insert_query = "
        CREATE (a:MemgraphTest:Student {name: $name1, score: $score})
        RETURN a.name AS name, a.score AS score
    ";

    let insert_result = conn
        .execute(insert_query, &create_params)
        .await
        .expect("Failed to insert records in Memgraph");

    assert_eq!(insert_result.row_count(), 1);

    // 3. Query records with Arrow conversion
    let select_result = conn
        .execute(
            "MATCH (a:MemgraphTest:Student) RETURN a.name AS name, a.score AS score",
            &HashMap::new(),
        )
        .await
        .expect("Failed to query Memgraph");

    assert_eq!(select_result.row_count(), 1);
    assert_eq!(
        select_result.columns,
        vec!["name".to_string(), "score".to_string()]
    );

    let batch = select_result
        .into_single_batch()
        .unwrap()
        .expect("Missing Arrow RecordBatch");
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 2);

    // 4. Clean up
    let _ = conn
        .execute("MATCH (n:MemgraphTest) DETACH DELETE n", &HashMap::new())
        .await
        .expect("Failed to clean up Memgraph test records");

    conn.close()
        .await
        .expect("Failed to close Memgraph connection");
}

#[tokio::test]
async fn test_live_memgraph_fluent_builder_with_clause_and_pagination() {
    if !is_memgraph_online().await {
        eprintln!("[SKIP] Live Memgraph instance is not reachable on 127.0.0.1:7688");
        return;
    }

    use voyager_ogm::builder::QueryBuilder;
    use voyager_ogm::emitters::CypherEmitter;
    use voyager_ogm::visitor::AstVisitor;

    let config = ConnectionConfig::from_uri(MEMGRAPH_URI);
    let mut conn = BoltConnection::connect(&config)
        .await
        .expect("Failed to connect to live Memgraph");

    // Clean up
    let _ = conn
        .execute(
            "MATCH (n:FluentMemgraphTest) DETACH DELETE n",
            &HashMap::new(),
        )
        .await;

    // Seed test records
    let seed_cypher = "CREATE (:FluentMemgraphTest:Person {name: 'Bob', age: 20}), (:FluentMemgraphTest:Person {name: 'David', age: 25}), (:FluentMemgraphTest:Person {name: 'Alice', age: 30}), (:FluentMemgraphTest:Person {name: 'Charlie', age: 40})";
    conn.execute(seed_cypher, &HashMap::new())
        .await
        .expect("Failed to seed records in Memgraph");

    // 1. Test WITH clause pipeline: MATCH -> WHERE -> WITH p -> WHERE -> RETURN
    let mut builder = QueryBuilder::new();
    builder
        .r#match()
        .node(Some("p"), vec!["FluentMemgraphTest", "Person"])
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
        .expect("Failed to execute WITH clause query on Memgraph");

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
        .node(Some("p"), vec!["FluentMemgraphTest", "Person"])
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
        .expect("Failed to execute pagination query on Memgraph");

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
        .execute(
            "MATCH (n:FluentMemgraphTest) DETACH DELETE n",
            &HashMap::new(),
        )
        .await;

    conn.close().await.expect("Failed to close connection");
}

#[tokio::test]
async fn test_live_memgraph_simultaneous_explain_and_profile_connections() {
    if !is_memgraph_online().await {
        eprintln!("[SKIP] Live Memgraph instance is not reachable on 127.0.0.1:7688");
        return;
    }

    use voyager_ogm::builder::QueryBuilder;
    use voyager_ogm::emitters::CypherEmitter;
    use voyager_ogm::visitor::AstVisitor;

    let config = ConnectionConfig::from_uri(MEMGRAPH_URI);

    // Seed test records using a setup connection
    let mut setup_conn = BoltConnection::connect(&config)
        .await
        .expect("Failed to connect to live Memgraph for setup");

    let _ = setup_conn
        .execute(
            "MATCH (n:LiveSimultaneousMemgraphTest) DETACH DELETE n",
            &HashMap::new(),
        )
        .await;

    setup_conn
        .execute(
            "CREATE (:LiveSimultaneousMemgraphTest:Person {name: 'Alice', age: 30}), (:LiveSimultaneousMemgraphTest:Person {name: 'Bob', age: 25})",
            &HashMap::new(),
        )
        .await
        .expect("Failed to seed records in Memgraph");

    // 1. Build explain query
    let mut b_explain = QueryBuilder::new();
    b_explain
        .r#match()
        .node(Some("p"), vec!["LiveSimultaneousMemgraphTest", "Person"])
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
        .node(Some("p"), vec!["LiveSimultaneousMemgraphTest", "Person"])
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

    // Both queries executed concurrently on Memgraph
    // Memgraph returns tabular query plan rows for EXPLAIN
    assert!(exp_result.row_count() > 0);
    assert_eq!(exp_result.columns, vec!["QUERY PLAN"]);

    // Memgraph returns operator execution metrics for PROFILE
    assert!(prof_result.row_count() > 0);
    assert!(prof_result.columns.contains(&"OPERATOR".to_string()));

    // Clean up
    let _ = setup_conn
        .execute(
            "MATCH (n:LiveSimultaneousMemgraphTest) DETACH DELETE n",
            &HashMap::new(),
        )
        .await;

    setup_conn
        .close()
        .await
        .expect("Failed to close setup connection");
}

#[tokio::test]
async fn test_live_memgraph_execute_rows_and_arrow_equivalence() {
    if !is_memgraph_online().await {
        eprintln!("[SKIP] Live Memgraph instance is not reachable on 127.0.0.1:7688");
        return;
    }

    use voyager_drivers::NativeClient;
    let client = NativeClient::new(MEMGRAPH_URI, None).unwrap();

    let _ = client
        .execute("MATCH (n:ZzStage1Mg) DETACH DELETE n", &HashMap::new())
        .await;
    client
        .execute(
            "CREATE (a:ZzStage1Mg {name: 'Alice', age: 30})-[:ZZ_KNOWS {since: 2024}]->(b:ZzStage1Mg {name: 'Bob', age: 25})",
            &HashMap::new(),
        )
        .await
        .unwrap();

    let q_rich = "MATCH p=(a:ZzStage1Mg)-[r:ZZ_KNOWS]->(b:ZzStage1Mg) \
                  RETURN a.name AS name, a.age AS age, a, r, p, {k: [1, 'x']} AS m";
    let leg = client.execute(q_rich, &HashMap::new()).await.unwrap();
    let rows = client.execute_rows(q_rich, &HashMap::new()).await.unwrap();
    let arr = client.execute_arrow(q_rich, &HashMap::new()).await.unwrap();

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
    assert_eq!(rows.result_rows, leg.result_rows);

    let _ = client
        .execute("MATCH (n:ZzStage1Mg) DETACH DELETE n", &HashMap::new())
        .await;
    client.close().await;
}

#[tokio::test]
async fn test_live_memgraph_stream_lifecycle_early_close_tx_qid_and_timeout_cancel() {
    if !is_memgraph_online().await {
        eprintln!("[SKIP] Live Memgraph instance is not reachable on 127.0.0.1:7688");
        return;
    }

    use std::time::Duration;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, NetError, ResultValue, StreamOptions};

    let client = NativeClient::new(
        MEMGRAPH_URI,
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

    // 2. Early close after chunk 1 handles Memgraph DISCARD has_more=false and reuses connection
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

    // 5. Mid-stream error recovers via deferred RESET on the same pooled connection
    let mut err_stream = client
        .stream(
            "UNWIND [1, 2, 0] AS x RETURN 10 / x AS q",
            &HashMap::new(),
            StreamOptions::new(1),
        )
        .await
        .unwrap();
    let mut saw_error = false;
    while let Some(res) = match err_stream.next_chunk().await {
        Ok(c) => c,
        Err(_) => {
            saw_error = true;
            None
        }
    } {
        assert_eq!(res.num_rows(), 1);
    }
    assert!(saw_error, "expected division by zero error during stream");
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
async fn test_live_memgraph_stream_ttfb_1k_vs_100k() {
    if !is_memgraph_online().await {
        eprintln!("[SKIP] Live Memgraph instance is not reachable on 127.0.0.1:7688");
        return;
    }

    use std::time::Instant;
    use voyager_drivers::{NativeClient, StreamOptions};

    let client = NativeClient::new(MEMGRAPH_URI, None).unwrap();
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
        "[memgraph stream] ttfb_1k={:?}, ttfb_100k={:?}, ttlb_100k={:?}",
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
