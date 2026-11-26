//! Live tests for Bolt database selection (#396).
//!
//! `VOYAGER_TEST_HOST` selects the machine running Neo4j (7687) and Memgraph (7688);
//! it defaults to `127.0.0.1`.

use std::collections::HashMap;
use std::time::Duration;

use arrow_array::{Array, Int64Array, StringArray};
use voyager_drivers::bolt::BoltConnection;
use voyager_drivers::config::ConnectionConfig;
use voyager_drivers::engine::{AsyncConnection, QueryResult};

const NEO4J_USER: &str = "neo4j";
const NEO4J_PASS: &str = "voyagerpass123";
const PROBE_LABEL: &str = "ZzDb396Probe";

fn test_host() -> String {
    std::env::var("VOYAGER_TEST_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

async fn connect_neo4j(host: &str, database: Option<&str>) -> Option<BoltConnection> {
    let mut config =
        ConnectionConfig::from_uri(format!("bolt://{host}:7687")).with_auth(NEO4J_USER, NEO4J_PASS);
    if let Some(database) = database {
        config = config.with_database(database);
    }
    BoltConnection::connect(&config).await.ok()
}

fn string_at(result: &QueryResult, row: usize, column: usize) -> Option<String> {
    let batch = result.batches.first()?;
    let values = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()?;
    values.is_valid(row).then(|| values.value(row).to_string())
}

fn int_at(result: &QueryResult, row: usize, column: usize) -> Option<i64> {
    let batch = result.batches.first()?;
    let values = batch.column(column).as_any().downcast_ref::<Int64Array>()?;
    values.is_valid(row).then(|| values.value(row))
}

async fn current_database(conn: &mut BoltConnection) -> String {
    let result = conn
        .execute(
            "CALL db.info() YIELD name RETURN name AS name",
            &HashMap::new(),
        )
        .await
        .expect("CALL db.info() failed");
    string_at(&result, 0, 0).expect("CALL db.info() returned no database name")
}

async fn probe_count(conn: &mut BoltConnection) -> i64 {
    let result = conn
        .execute(
            &format!("MATCH (n:{PROBE_LABEL}) RETURN count(n) AS c"),
            &HashMap::new(),
        )
        .await
        .expect("probe count failed");
    int_at(&result, 0, 0).expect("probe count returned no value")
}

async fn database_status(conn: &mut BoltConnection, name: &str) -> Option<String> {
    let mut params = HashMap::new();
    params.insert(
        "name".to_string(),
        voyager_drivers::ParamValue::Json(serde_json::Value::String(name.to_string())),
    );
    let result = conn
        .execute(
            "SHOW DATABASES YIELD name, currentStatus WHERE name = $name RETURN currentStatus",
            &params,
        )
        .await
        .ok()?;
    string_at(&result, 0, 0)
}

#[tokio::test]
async fn test_live_neo4j_database_selection() {
    let host = test_host();
    let Some(mut default_conn) = connect_neo4j(&host, None).await else {
        eprintln!("[SKIP] Neo4j is not reachable on {host}:7687");
        return;
    };
    let default_database = current_database(&mut default_conn).await;
    assert_eq!(default_database, "neo4j");

    // A database named in the connection config reaches RUN and BEGIN on Bolt 5.1+.
    let Some(mut system_conn) = connect_neo4j(&host, Some("system")).await else {
        panic!("Neo4j accepted the default database but not 'system'");
    };
    assert_eq!(system_conn.database(), Some("system"));
    assert_eq!(current_database(&mut system_conn).await, "system");

    system_conn
        .begin_transaction()
        .await
        .expect("BEGIN on system failed");
    assert_eq!(current_database(&mut system_conn).await, "system");
    system_conn
        .rollback_transaction()
        .await
        .expect("ROLLBACK failed");

    // Community edition cannot create a database; fall back to proving which database
    // answered with db.info() and SHOW DATABASES, as the issue allows.
    let database = format!("zz_db_396_{}", std::process::id());
    let created = default_conn
        .execute(&format!("CREATE DATABASE {database}"), &HashMap::new())
        .await;
    match created {
        Ok(_) => {
            let mut online = false;
            for _ in 0..40 {
                if database_status(&mut default_conn, &database)
                    .await
                    .as_deref()
                    == Some("online")
                {
                    online = true;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            assert!(online, "database {database} never came online");

            let mut probe_created = false;
            let mut probe_rows = -1;
            let mut second_conn = connect_neo4j(&host, Some(&database)).await;
            if let Some(conn) = second_conn.as_mut() {
                assert_eq!(current_database(conn).await, database);
                probe_created = conn
                    .execute(
                        &format!("CREATE (n:{PROBE_LABEL} {{id: 1}})"),
                        &HashMap::new(),
                    )
                    .await
                    .is_ok();
                if probe_created {
                    probe_rows = probe_count(conn).await;
                }
            }
            if let Some(mut conn) = second_conn.take() {
                let _ = conn.close().await;
            }
            let default_rows = probe_count(&mut default_conn).await;

            let _ = default_conn
                .execute(
                    &format!("DROP DATABASE {database} IF EXISTS"),
                    &HashMap::new(),
                )
                .await;

            assert!(probe_created, "node creation in {database} failed");
            assert_eq!(probe_rows, 1, "probe node missing from {database}");
            assert_eq!(
                default_rows, 0,
                "probe node leaked into the default database"
            );
        }
        Err(error) => {
            eprintln!("[SKIP] Cannot create a second Neo4j database (Community edition): {error}");
            let result = default_conn
                .execute(
                    "SHOW DATABASES YIELD name, default RETURN name, default",
                    &HashMap::new(),
                )
                .await
                .expect("SHOW DATABASES failed");
            assert!(
                result.row_count() >= 2,
                "expected neo4j and system databases"
            );
        }
    }

    let _ = default_conn.close().await;
    let _ = system_conn.close().await;
}

#[tokio::test]
async fn test_live_memgraph_keeps_working_without_a_database() {
    let host = test_host();
    let config = ConnectionConfig::from_uri(format!("bolt://{host}:7688"));
    let Ok(mut conn) = BoltConnection::connect(&config).await else {
        eprintln!("[SKIP] Memgraph is not reachable on {host}:7688");
        return;
    };

    assert_eq!(conn.database(), None);
    let result = conn
        .execute("RETURN 1 AS n", &HashMap::new())
        .await
        .expect("Memgraph query failed");
    assert_eq!(result.row_count(), 1);
    let _ = conn.close().await;
}
