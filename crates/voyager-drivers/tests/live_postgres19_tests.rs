//! Live integration test suite for PostgreSQL 19 Beta 3 over native wire protocol.

use std::collections::HashMap;
use voyager_drivers::postgres::{PostgresConnection, PostgresTransaction};
use voyager_drivers::{AsyncConnection, ParsedUri};

const PG19_USER: &str = "postgres";
const PG19_PASSWORD: &str = "voyagerpass123";
const PG19_DATABASE: &str = "postgres";

fn pg19_host() -> String {
    std::env::var("VOYAGER_TEST_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn pg19_uri() -> String {
    format!(
        "postgresql://{PG19_USER}:{PG19_PASSWORD}@{}:5456/{PG19_DATABASE}",
        pg19_host()
    )
}

async fn get_pg19_connection() -> Option<PostgresConnection> {
    connect_pg19(&pg19_uri()).await
}

async fn connect_pg19(uri: &str) -> Option<PostgresConnection> {
    let parsed = match ParsedUri::parse(uri) {
        Ok(p) => p,
        Err(_) => return None,
    };

    match PostgresConnection::connect(&parsed).await {
        Ok(conn) => Some(conn),
        Err(e) => {
            eprintln!(
                "Skipping live PostgreSQL 19 tests (container not accessible): {}",
                e
            );
            None
        }
    }
}

async fn get_pg19_connection_with_cache(size: usize) -> Option<PostgresConnection> {
    connect_pg19(&format!(
        "{}?prepared_statement_cache_size={size}",
        pg19_uri()
    ))
    .await
}

#[tokio::test]
async fn test_live_postgres19_ping_and_version() {
    let mut conn = match get_pg19_connection().await {
        Some(c) => c,
        None => return,
    };

    assert!(conn.ping().await.is_ok());
    assert!(conn.is_valid());
    assert!(!conn.is_in_transaction());

    let params = conn.server_parameters();
    let version = params.get("server_version").cloned().unwrap_or_default();
    println!("PostgreSQL 19 Live Server Version: {}", version);
    assert!(!version.is_empty());
}

#[tokio::test]
async fn test_live_postgres19_recursive_graph_and_arrow() {
    let mut conn = match get_pg19_connection().await {
        Some(c) => c,
        None => return,
    };

    // 1. Setup Table Schema & Hierarchy
    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS net_categories;")
        .await;
    conn.execute_simple(
        "CREATE TABLE net_categories (id INT PRIMARY KEY, name TEXT, parent_id INT);",
    )
    .await
    .unwrap();

    let insert_sql = "INSERT INTO net_categories VALUES (1, 'Root', NULL), (2, 'Electronics', 1), (3, 'Laptops', 2), (4, 'Gaming Laptops', 3);";
    let insert_res = conn.execute_simple(insert_sql).await.unwrap();
    assert_eq!(insert_res.summary.nodes_created, 4);

    // 2. Query Hierarchy using Recursive CTE (Graph Traversal in SQL)
    let recursive_query = "
        WITH RECURSIVE cat_hierarchy AS (
            SELECT id, name, parent_id, 1 as depth
            FROM net_categories
            WHERE id = 1
            UNION ALL
            SELECT c.id, c.name, c.parent_id, ch.depth + 1
            FROM net_categories c
            JOIN cat_hierarchy ch ON c.parent_id = ch.id
        )
        SELECT id, name, parent_id, depth FROM cat_hierarchy ORDER BY depth;
    ";

    let result = conn.execute_simple(recursive_query).await.unwrap();
    assert_eq!(result.columns, vec!["id", "name", "parent_id", "depth"]);
    assert_eq!(result.row_count(), 4);

    let batch = &result.batches[0];
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(batch.num_rows(), 4);

    let id_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(id_col.value(0), 1);
    assert_eq!(id_col.value(1), 2);
    assert_eq!(id_col.value(2), 3);
    assert_eq!(id_col.value(3), 4);

    let name_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(name_col.value(0), "Root");
    assert_eq!(name_col.value(1), "Electronics");
    assert_eq!(name_col.value(2), "Laptops");
    assert_eq!(name_col.value(3), "Gaming Laptops");

    let depth_col = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .unwrap();
    assert_eq!(depth_col.value(0), 1);
    assert_eq!(depth_col.value(3), 4);

    // 3. Clean up
    let _ = conn.execute_simple("DROP TABLE net_categories;").await;
}

#[tokio::test]
async fn test_live_postgres19_transactions() {
    let mut conn = match get_pg19_connection().await {
        Some(c) => c,
        None => return,
    };

    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS net_tx_test;")
        .await;
    conn.execute_simple("CREATE TABLE net_tx_test (id INT PRIMARY KEY, name TEXT);")
        .await
        .unwrap();

    // 1. Committed Transaction
    {
        let mut tx = PostgresTransaction::begin(&mut conn).await.unwrap();
        let _ = tx
            .execute(
                "INSERT INTO net_tx_test VALUES (1, 'Committed');",
                &HashMap::new(),
            )
            .await
            .unwrap();
        let _ = tx.commit().await.unwrap();
    }

    let check_committed = conn
        .execute_simple("SELECT id, name FROM net_tx_test WHERE id = 1;")
        .await
        .unwrap();
    assert_eq!(check_committed.row_count(), 1);

    // 2. Aborted / Rolled Back Transaction
    {
        let mut tx = PostgresTransaction::begin(&mut conn).await.unwrap();
        let _ = tx
            .execute(
                "INSERT INTO net_tx_test VALUES (2, 'Aborted');",
                &HashMap::new(),
            )
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }

    let check_aborted = conn
        .execute_simple("SELECT id, name FROM net_tx_test WHERE id = 2;")
        .await
        .unwrap();
    assert_eq!(check_aborted.row_count(), 0);

    // 3. Savepoints
    {
        let mut tx = PostgresTransaction::begin(&mut conn).await.unwrap();
        let _ = tx
            .execute(
                "INSERT INTO net_tx_test VALUES (3, 'Before SP');",
                &HashMap::new(),
            )
            .await
            .unwrap();
        tx.savepoint("sp1").await.unwrap();
        let _ = tx
            .execute(
                "INSERT INTO net_tx_test VALUES (4, 'Inside SP');",
                &HashMap::new(),
            )
            .await
            .unwrap();
        tx.rollback_to_savepoint("sp1").await.unwrap();
        let _ = tx.commit().await.unwrap();
    }

    let check_sp_kept = conn
        .execute_simple("SELECT id FROM net_tx_test WHERE id = 3;")
        .await
        .unwrap();
    assert_eq!(check_sp_kept.row_count(), 1);

    let check_sp_rolled = conn
        .execute_simple("SELECT id FROM net_tx_test WHERE id = 4;")
        .await
        .unwrap();
    assert_eq!(check_sp_rolled.row_count(), 0);

    // Clean up
    let _ = conn.execute_simple("DROP TABLE net_tx_test;").await;
}

#[tokio::test]
async fn test_live_postgres19_sql_pgq_graph_table_and_arrow() {
    use voyager_ogm::builder::QueryBuilder;
    use voyager_ogm::emitters::sql_pgq::SqlPgqEmitter;
    use voyager_ogm::visitor::AstVisitor;

    let mut conn = match get_pg19_connection().await {
        Some(c) => c,
        None => return,
    };

    // 1. Setup Tables and Property Graph DDL in PostgreSQL 19
    let _ = conn
        .execute_simple("DROP PROPERTY GRAPH IF EXISTS net_pgq_graph;")
        .await;
    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS net_pgq_knows;")
        .await;
    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS net_pgq_person;")
        .await;

    conn.execute_simple(
        "CREATE TABLE net_pgq_person (id INT PRIMARY KEY, name TEXT, age INT, city TEXT);",
    )
    .await
    .unwrap();
    conn.execute_simple(
        "CREATE TABLE net_pgq_knows (src INT REFERENCES net_pgq_person(id), dst INT REFERENCES net_pgq_person(id), since INT, PRIMARY KEY (src, dst));",
    )
    .await
    .unwrap();

    conn.execute_simple(
        "INSERT INTO net_pgq_person VALUES (1, 'Alice', 34, 'London'), (2, 'Bob', 28, 'Berlin'), (3, 'Charlie', 42, 'London');",
    )
    .await
    .unwrap();
    conn.execute_simple("INSERT INTO net_pgq_knows VALUES (1, 2, 2020), (2, 3, 2021);")
        .await
        .unwrap();

    conn.execute_simple(
        "CREATE PROPERTY GRAPH net_pgq_graph
            VERTEX TABLES (net_pgq_person LABEL \"Person\" PROPERTIES (name, age, city))
            EDGE TABLES (
                net_pgq_knows
                SOURCE KEY (src) REFERENCES net_pgq_person (id)
                DESTINATION KEY (dst) REFERENCES net_pgq_person (id)
                LABEL \"KNOWS\"
                PROPERTIES (since)
            );",
    )
    .await
    .unwrap();

    // 2. Traversal Query built via Voyager QueryBuilder and SqlPgqEmitter
    let mut builder = QueryBuilder::new();
    builder
        .match_node(Some("a"), vec!["Person"])
        .to(vec!["KNOWS".to_string()], Some("r".to_string()))
        .node(Some("b"), vec!["Person"])
        .field("a", "name", Some("source".to_string()))
        .field("b", "name", Some("target".to_string()))
        .order_by_asc("a", "name");

    let (arena, root) = builder.build();
    let mut pgq = SqlPgqEmitter::new("net_pgq_graph");
    let compiled = pgq.visit_query(&arena, root).unwrap();

    assert!(
        compiled
            .statement
            .contains("GRAPH_TABLE (net_pgq_graph MATCH")
    );
    assert!(
        compiled
            .statement
            .contains("ORDER BY source ASC NULLS LAST")
    );

    let res = conn.execute_simple(&compiled.statement).await.unwrap();
    assert_eq!(res.columns, vec!["source", "target"]);
    assert_eq!(res.row_count(), 2);

    let batch = &res.batches[0];
    let src_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    let dst_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .unwrap();
    assert_eq!(src_col.value(0), "Alice");
    assert_eq!(dst_col.value(0), "Bob");
    assert_eq!(src_col.value(1), "Bob");
    assert_eq!(dst_col.value(1), "Charlie");

    // 3. Grouped Aggregation Query via SqlPgqEmitter
    let mut agg_builder = QueryBuilder::new();
    agg_builder
        .match_node(Some("p"), vec!["Person"])
        .field("p", "city", Some("city".to_string()))
        .select_property_aggregate(
            "p",
            "name",
            voyager_ogm::ast::AggregationFunc::Count,
            Some("total_count".to_string()),
        )
        .order_by_asc("p", "city");

    let (agg_arena, agg_root) = agg_builder.build();
    let mut pgq_agg = SqlPgqEmitter::new("net_pgq_graph");
    let compiled_agg = pgq_agg.visit_query(&agg_arena, agg_root).unwrap();

    assert!(compiled_agg.statement.contains("GROUP BY city"));
    assert!(
        compiled_agg
            .statement
            .contains("ORDER BY city ASC NULLS LAST")
    );

    let agg_res = conn.execute_simple(&compiled_agg.statement).await.unwrap();
    assert_eq!(agg_res.columns, vec!["city", "total_count"]);
    assert_eq!(agg_res.row_count(), 2);

    // Clean up
    let _ = conn
        .execute_simple("DROP PROPERTY GRAPH IF EXISTS net_pgq_graph;")
        .await;
    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS net_pgq_knows;")
        .await;
    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS net_pgq_person;")
        .await;
}

fn int_param(value: i64) -> HashMap<String, voyager_drivers::ParamValue> {
    let mut params = HashMap::new();
    params.insert(
        "v".to_string(),
        voyager_drivers::ParamValue::from(serde_json::json!(value)),
    );
    params
}

#[tokio::test]
async fn test_live_postgres19_prepared_statement_cache_evicts_and_reuses() {
    let Some(mut conn) = get_pg19_connection_with_cache(2).await else {
        return;
    };

    // Three distinct SQL texts exceed the capacity of two. Every statement must
    // still return the right rows after the oldest entry is evicted and closed.
    for offset in 0..3 {
        let query = format!("SELECT $v::bigint + {offset} AS value");
        let result = conn.execute(&query, &int_param(41)).await.unwrap();
        let batch = result.batches.first().expect("one batch");
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("bigint column");
        assert_eq!(column.value(0), 41 + offset);
        assert!(conn.prepared_statement_cache_len() <= 2);
    }

    let again = conn
        .execute("SELECT $v::bigint + 0 AS value", &int_param(7))
        .await
        .unwrap();
    let column = again
        .batches
        .first()
        .expect("one batch")
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("bigint column");
    assert_eq!(column.value(0), 7);
    assert!(conn.prepared_statement_cache_len() <= 2);
}

#[tokio::test]
async fn test_live_postgres19_prepared_statement_cache_disabled() {
    let Some(mut conn) = get_pg19_connection_with_cache(0).await else {
        return;
    };

    for _ in 0..2 {
        let result = conn
            .execute("SELECT $v::bigint AS value", &int_param(5))
            .await
            .unwrap();
        let column = result
            .batches
            .first()
            .expect("one batch")
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("bigint column");
        assert_eq!(column.value(0), 5);
        assert_eq!(conn.prepared_statement_cache_len(), 0);
    }
}

#[tokio::test]
async fn test_live_postgres19_prepared_statement_cache_recovers_after_schema_change() {
    let Some(mut conn) = get_pg19_connection().await else {
        return;
    };

    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS zz_pg461_cache_schema;")
        .await;
    conn.execute_simple("CREATE TABLE zz_pg461_cache_schema (id INT);")
        .await
        .unwrap();
    conn.execute_simple("INSERT INTO zz_pg461_cache_schema VALUES (1), (2);")
        .await
        .unwrap();

    let query = "SELECT * FROM zz_pg461_cache_schema WHERE id >= $v";
    let first = conn.execute(query, &int_param(1)).await.unwrap();
    assert_eq!(first.columns, vec!["id"]);
    assert_eq!(first.row_count(), 2);
    assert_eq!(conn.prepared_statement_cache_len(), 1);

    // A named statement's result type changes when a column is added. PostgreSQL
    // answers 0A000 ("cached plan must not change result type"); the connection
    // must drop the cached entry and re-parse once instead of surfacing the error.
    conn.execute_simple("ALTER TABLE zz_pg461_cache_schema ADD COLUMN label TEXT;")
        .await
        .unwrap();
    conn.execute_simple("UPDATE zz_pg461_cache_schema SET label = 'row-' || id;")
        .await
        .unwrap();

    let second = conn.execute(query, &int_param(1)).await.unwrap();
    assert_eq!(second.columns, vec!["id", "label"]);
    assert_eq!(second.row_count(), 2);
    assert_eq!(conn.prepared_statement_cache_len(), 1);

    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS zz_pg461_cache_schema;")
        .await;
}

#[tokio::test]
async fn test_live_postgres19_prepared_statement_cache_does_not_retry_inside_transaction() {
    let Some(mut conn) = get_pg19_connection().await else {
        return;
    };

    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS zz_pg461_cache_tx;")
        .await;
    conn.execute_simple("CREATE TABLE zz_pg461_cache_tx (id INT);")
        .await
        .unwrap();
    conn.execute_simple("INSERT INTO zz_pg461_cache_tx VALUES (1), (2);")
        .await
        .unwrap();

    let query = "SELECT * FROM zz_pg461_cache_tx WHERE id >= $v";
    let first = conn.execute(query, &int_param(1)).await.unwrap();
    assert_eq!(first.columns, vec!["id"]);
    assert_eq!(first.row_count(), 2);
    assert_eq!(conn.prepared_statement_cache_len(), 1);

    conn.begin_transaction().await.unwrap();
    // Change the result type from a second connection so the ALTER survives the
    // rollback below. The transaction has not read the table yet, so it holds no
    // lock that would block the ALTER.
    let Some(mut other) = get_pg19_connection().await else {
        return;
    };
    other
        .execute_simple("ALTER TABLE zz_pg461_cache_tx ADD COLUMN label TEXT;")
        .await
        .unwrap();
    other
        .execute_simple("UPDATE zz_pg461_cache_tx SET label = 'row-' || id;")
        .await
        .unwrap();
    let _ = other.close().await;

    // The first 0A000 already aborts the transaction, so a second Parse would fail
    // with 25P02: the original error must be returned and the stale name forgotten.
    let error = conn
        .execute(query, &int_param(1))
        .await
        .expect_err("cached plan result-type change should fail inside the transaction");
    let message = error.to_string();
    assert!(
        message.contains("0A000") || message.contains("cached plan must not change result type"),
        "expected the original 0A000 error, got: {message}"
    );
    assert_eq!(conn.prepared_statement_cache_len(), 0);
    assert!(conn.can_rollback());
    conn.rollback_transaction().await.unwrap();

    // The entry was dropped, so after ROLLBACK the same statement re-parses and works.
    let after = conn.execute(query, &int_param(1)).await.unwrap();
    assert_eq!(after.columns, vec!["id", "label"]);
    assert_eq!(after.row_count(), 2);
    assert_eq!(conn.prepared_statement_cache_len(), 1);

    let _ = conn
        .execute_simple("DROP TABLE IF EXISTS zz_pg461_cache_tx;")
        .await;
}

#[tokio::test]
async fn test_live_postgres19_prepared_statement_cache_unknown_inside_transaction() {
    let Some(mut conn) = get_pg19_connection().await else {
        return;
    };

    let query = "SELECT $v::bigint + 200 AS value";
    let first = conn.execute(query, &int_param(1)).await.unwrap();
    assert_eq!(first.row_count(), 1);
    assert_eq!(conn.prepared_statement_cache_len(), 1);

    conn.begin_transaction().await.unwrap();
    conn.execute_simple("DEALLOCATE ALL;").await.unwrap();
    let error = conn
        .execute(query, &int_param(2))
        .await
        .expect_err("the server forgot the named statement");
    let message = error.to_string();
    assert!(
        message.contains("26000") || message.to_lowercase().contains("does not exist"),
        "expected the original 26000 error, got: {message}"
    );
    assert_eq!(conn.prepared_statement_cache_len(), 0);
    assert!(conn.can_rollback());
    conn.rollback_transaction().await.unwrap();

    let after = conn.execute(query, &int_param(3)).await.unwrap();
    let column = after
        .batches
        .first()
        .expect("one batch")
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("bigint column");
    assert_eq!(column.value(0), 203);
    assert_eq!(conn.prepared_statement_cache_len(), 1);
}

#[tokio::test]
async fn test_live_postgres19_prepared_statement_cache_recovers_after_deallocate() {
    let Some(mut conn) = get_pg19_connection().await else {
        return;
    };

    let query = "SELECT $v::bigint + 100 AS value";
    let first = conn.execute(query, &int_param(1)).await.unwrap();
    assert_eq!(first.row_count(), 1);
    assert_eq!(conn.prepared_statement_cache_len(), 1);

    // The server forgets the named statement; the client sees SQLSTATE 26000 and
    // must re-parse instead of failing the query.
    conn.execute_simple("DEALLOCATE ALL;").await.unwrap();
    let second = conn.execute(query, &int_param(2)).await.unwrap();
    let column = second
        .batches
        .first()
        .expect("one batch")
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::Int64Array>()
        .expect("bigint column");
    assert_eq!(column.value(0), 102);
    assert_eq!(conn.prepared_statement_cache_len(), 1);
}

#[tokio::test]
async fn test_live_postgres19_execute_rows_and_arrow_equivalence() {
    use voyager_drivers::NativeClient;
    use voyager_drivers::value::result::ResultValue;

    if get_pg19_connection().await.is_none() {
        return;
    }

    let client = NativeClient::new(&pg19_uri(), None).unwrap();
    let _ = client
        .execute("DROP TABLE IF EXISTS zz_stage1_462_pg;", &HashMap::new())
        .await;
    client
        .execute(
            "CREATE TABLE zz_stage1_462_pg (\
             id int, b bool, f double precision, num numeric(10, 2), \
             uid uuid, d date, t time, ts timestamp, iv interval, \
             js jsonb, arr int[], mix_i int[], mix_n numeric[], mix_u uuid[])",
            &HashMap::new(),
        )
        .await
        .unwrap();

    // 1. 0-row typed PG table equivalence
    let q_zero = "SELECT id, b, f, num, uid, d, t, ts, iv, js, arr FROM zz_stage1_462_pg";
    let leg0 = client.execute(q_zero, &HashMap::new()).await.unwrap();
    let row0 = client.execute_rows(q_zero, &HashMap::new()).await.unwrap();
    let arr0 = client.execute_arrow(q_zero, &HashMap::new()).await.unwrap();
    assert_eq!(row0.row_count(), 0);
    assert!(row0.batches.is_empty());
    assert!(arr0.result_rows.is_none());
    assert_eq!(
        row0.to_record_batch().unwrap().unwrap(),
        leg0.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        arr0.to_record_batch().unwrap().unwrap(),
        leg0.to_record_batch().unwrap().unwrap()
    );

    // 2. Populated rows including mixed-depth arrays
    client
        .execute(
            "INSERT INTO zz_stage1_462_pg VALUES \
             (1, true, 3.5, 12.30, '12345678-1234-5678-1234-567812345678', '2024-01-02', '14:30:15', '2024-01-02 03:04:05', '1 day 02:00:00', '{\"a\":1}', '{{1,2},{3,NULL}}', '{1,2}', '{1.5,2}', '{12345678-1234-5678-1234-567812345678}'), \
             (2, false, 1.25, 0.50, NULL, NULL, NULL, NULL, NULL, NULL, '{{4,5},{6,7}}', '{{3,NULL},{5,6}}', '{{3.25},{4}}', '{{87654321-4321-8765-4321-876543218765}}')",
            &HashMap::new(),
        )
        .await
        .unwrap();

    let q_all = "SELECT id, b, f, num, uid, d, t, ts, iv, js, arr, mix_i, mix_n, mix_u FROM zz_stage1_462_pg ORDER BY id";
    let leg = client.execute(q_all, &HashMap::new()).await.unwrap();
    let rows = client.execute_rows(q_all, &HashMap::new()).await.unwrap();
    let arr = client.execute_arrow(q_all, &HashMap::new()).await.unwrap();

    assert!(rows.batches.is_empty());
    assert!(rows.bolt_rows.is_none());
    assert!(arr.result_rows.is_none());
    assert!(arr.bolt_rows.is_none());
    assert_eq!(rows.row_count(), 2);
    assert_eq!(
        rows.to_record_batch().unwrap().unwrap(),
        leg.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        arr.to_record_batch().unwrap().unwrap(),
        leg.to_record_batch().unwrap().unwrap()
    );

    // Parameterized query inside a transaction
    let mut tx = client.begin_transaction().await.unwrap();
    let params = int_param(1);
    let q_param = "SELECT id, mix_i, mix_n FROM zz_stage1_462_pg WHERE id >= $v ORDER BY id";
    let tx_leg = tx.execute(q_param, &params).await.unwrap();
    let tx_rows = tx.execute_rows(q_param, &params).await.unwrap();
    let tx_arr = tx.execute_arrow(q_param, &params).await.unwrap();
    assert_eq!(
        tx_rows.to_record_batch().unwrap().unwrap(),
        tx_leg.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        tx_arr.to_record_batch().unwrap().unwrap(),
        tx_leg.to_record_batch().unwrap().unwrap()
    );
    let leg_bolt = tx_leg.bolt_rows.as_ref().unwrap();
    assert_eq!(
        tx_rows.result_rows.as_ref().unwrap()[0][1],
        ResultValue::from(&leg_bolt[0][1])
    );
    tx.commit().await.unwrap();

    let _ = client
        .execute("DROP TABLE IF EXISTS zz_stage1_462_pg;", &HashMap::new())
        .await;
    client.close().await;
}

#[tokio::test]
async fn test_live_postgres19_stream_lifecycle_early_close_tx_and_timeout_cancel() {
    if get_pg19_connection().await.is_none() {
        return;
    }

    use std::time::Duration;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::value::result::ResultValue;
    use voyager_drivers::{NativeClient, NetError, StreamOptions};

    let client = NativeClient::new(
        &pg19_uri(),
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_acquire_timeout(Duration::from_secs(5)),
        ),
    )
    .unwrap();

    // Helper to inspect current backend_pid on the single pooled connection
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
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 1. Full multi-chunk stream (2,500 rows, fetch_size=1000) -> 3 chunks, eager lease release
    let mut full_stream = client
        .stream(
            "SELECT g FROM generate_series(1, 2500) AS g",
            &HashMap::new(),
            StreamOptions::new(1_000),
        )
        .await
        .unwrap();
    assert!(full_stream.is_server_streaming());
    assert_eq!(full_stream.columns(), &["g".to_string()]);
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
    assert_eq!(fetch_pid(&client).await, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 2. Early close after chunk 1 reuses the exact same backend_pid
    let mut early_stream = client
        .stream(
            "SELECT g FROM generate_series(1, 10000) AS g",
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
    assert_eq!(fetch_pid(&client).await, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 3. Close before chunk 1 drains & closes portal, reusing same backend_pid
    let mut unread_stream = client
        .stream(
            "SELECT g FROM generate_series(1, 500) AS g",
            &HashMap::new(),
            StreamOptions::new(100),
        )
        .await
        .unwrap();
    unread_stream.close().await.unwrap();
    assert_eq!(fetch_pid(&client).await, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 4. Explicit transaction stream: fail-fast guard, early close, follow-up INSERT, COMMIT
    let _ = client
        .execute_rows("DROP TABLE IF EXISTS zz_stage4_458_tx;", &HashMap::new())
        .await;
    client
        .execute_rows(
            "CREATE TABLE zz_stage4_458_tx (id INT PRIMARY KEY);",
            &HashMap::new(),
        )
        .await
        .unwrap();

    let mut tx = client.begin_transaction().await.unwrap();
    let mut cursor = tx
        .open_stream_cursor(
            "SELECT g FROM generate_series(1, 500) AS g",
            &HashMap::new(),
            &StreamOptions::new(100),
        )
        .await
        .unwrap();
    let err_concurrent = tx
        .execute_rows("SELECT 1 AS x", &HashMap::new())
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
    tx.execute_rows("INSERT INTO zz_stage4_458_tx VALUES (42);", &HashMap::new())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let check_tx = client
        .execute_rows("SELECT id FROM zz_stage4_458_tx;", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(check_tx.row_count(), 1);
    assert_eq!(fetch_pid(&client).await, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 5. Mid-stream ErrorResponse recovers via Sync on the same backend_pid
    let mut err_stream = client
        .stream(
            "SELECT 1000 / (150 - g) AS q FROM generate_series(1, 300) AS g",
            &HashMap::new(),
            StreamOptions::new(100),
        )
        .await
        .unwrap();
    let ok1 = err_stream.next_chunk().await.unwrap().unwrap();
    assert_eq!(ok1.num_rows(), 100);
    let mid_err = err_stream.next_chunk().await.unwrap_err();
    assert!(matches!(mid_err, NetError::ProtocolError(_)));
    assert_eq!(fetch_pid(&client).await, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 6. Multi-statement SQL (42601) returns clear hint and reuses same backend_pid
    let multi_err = match client
        .stream(
            "SELECT 1; SELECT 2;",
            &HashMap::new(),
            StreamOptions::new(10),
        )
        .await
    {
        Ok(_) => panic!("multi-statement stream must fail with 42601"),
        Err(e) => e,
    };
    let multi_msg = multi_err.to_string();
    assert!(multi_msg.contains("42601"), "got: {multi_msg}");
    assert!(
        multi_msg.contains("single SQL statement"),
        "got: {multi_msg}"
    );
    assert_eq!(fetch_pid(&client).await, initial_pid);

    // 7. Unconstrained NUMERIC keeps identical Utf8 + voyager.type=decimal schema across scale-drifting chunks
    let mut num_stream = client
        .stream(
            "SELECT CASE WHEN g <= 2 THEN 1.5::numeric ELSE 1.123456::numeric END AS n FROM generate_series(1, 4) AS g",
            &HashMap::new(),
            StreamOptions::new(2),
        )
        .await
        .unwrap();
    let nc1 = num_stream
        .next_chunk()
        .await
        .unwrap()
        .unwrap()
        .to_record_batch()
        .unwrap();
    let nc2 = num_stream
        .next_chunk()
        .await
        .unwrap()
        .unwrap()
        .to_record_batch()
        .unwrap();
    assert!(num_stream.next_chunk().await.unwrap().is_none());
    assert_eq!(nc1.schema(), nc2.schema());
    assert_eq!(
        nc1.schema().field(0).data_type(),
        &arrow_schema::DataType::Utf8
    );
    assert_eq!(
        nc1.schema()
            .field(0)
            .metadata()
            .get("voyager.type")
            .map(String::as_str),
        Some("decimal")
    );

    // 8. Stale cached statement (26000) recovery on open_stream after DEALLOCATE ALL
    let q_cached = "SELECT g FROM generate_series(1, $v::int) AS g";
    let _ = client.execute_rows(q_cached, &int_param(3)).await.unwrap();
    let _ = client
        .execute_rows("DEALLOCATE ALL;", &HashMap::new())
        .await
        .unwrap();
    let mut s_26000 = client
        .stream(q_cached, &int_param(3), StreamOptions::new(2))
        .await
        .unwrap();
    let mut rows_26000 = 0usize;
    while let Some(chunk) = s_26000.next_chunk().await.unwrap() {
        rows_26000 += chunk.num_rows();
    }
    assert_eq!(rows_26000, 3);
    assert_eq!(fetch_pid(&client).await, initial_pid);

    // 9. prepared_statement_cache_size=0 streams cleanly across repeated parameterized calls
    let nocache_uri = format!("{}?prepared_statement_cache_size=0", pg19_uri());
    let nocache_client = NativeClient::new(&nocache_uri, None).unwrap();
    for v in [10i64, 20i64] {
        let mut s = nocache_client
            .stream(
                "SELECT g FROM generate_series(1, $v::int) AS g",
                &int_param(v),
                StreamOptions::new(5),
            )
            .await
            .unwrap();
        let mut count = 0usize;
        while let Some(chunk) = s.next_chunk().await.unwrap() {
            count += chunk.num_rows();
        }
        assert_eq!(count, v as usize);
    }
    nocache_client.close().await;

    // 10. Drop unclosed stream marks broken & replaces backend_pid
    let mut dropped_stream = client
        .stream(
            "SELECT g FROM generate_series(1, 500) AS g",
            &HashMap::new(),
            StreamOptions::new(10),
        )
        .await
        .unwrap();
    assert_eq!(
        dropped_stream
            .next_chunk()
            .await
            .unwrap()
            .unwrap()
            .num_rows(),
        10
    );
    drop(dropped_stream);
    assert_eq!(client.pool().total_count(), 0);
    let pid_after_drop = fetch_pid(&client).await;
    assert_ne!(pid_after_drop, initial_pid);
    assert_eq!(client.pool().metrics().connections_created, 2);

    // 11. Mid-flight tokio::time::timeout cancellation replaces backend_pid transparently
    let mut slow_stream = client
        .stream(
            "SELECT g, pg_sleep(0.05) FROM generate_series(1, 200) AS g",
            &HashMap::new(),
            StreamOptions::new(1),
        )
        .await
        .unwrap();
    let _first_slow = slow_stream.next_chunk().await.unwrap().expect("chunk 1");
    let cancel_res = tokio::time::timeout(Duration::from_millis(5), slow_stream.next_chunk()).await;
    assert!(
        cancel_res.is_err(),
        "expected mid-flight timeout cancellation on chunk 2"
    );
    drop(slow_stream);
    assert_eq!(client.pool().total_count(), 0);
    let pid_after_cancel = fetch_pid(&client).await;
    assert_ne!(pid_after_cancel, pid_after_drop);
    assert_eq!(client.pool().metrics().connections_created, 3);

    let _ = client
        .execute_rows("DROP TABLE IF EXISTS zz_stage4_458_tx;", &HashMap::new())
        .await;
    client.close().await;
}

#[tokio::test]
async fn test_live_postgres19_stream_ttfb_1k_vs_100k() {
    if get_pg19_connection().await.is_none() {
        return;
    }

    use std::time::Instant;
    use voyager_drivers::{NativeClient, StreamOptions};

    let client = NativeClient::new(&pg19_uri(), None).unwrap();
    client.ping().await.unwrap();

    let t0_1k = Instant::now();
    let mut s_1k = client
        .stream(
            "SELECT g FROM generate_series(1, 1000) AS g",
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
            "SELECT g FROM generate_series(1, 100000) AS g",
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
        "[pg19 stream] ttfb_1k={:?}, ttfb_100k={:?}, ttlb_100k={:?}",
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
