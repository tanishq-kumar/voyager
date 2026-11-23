//! Live TLS tests against the TLS test stack (`containers/compose.tls.yaml`).
//!
//! - `VOYAGER_TEST_HOST`: host that runs the databases (default `127.0.0.1`).
//! - `VOYAGER_TEST_TLS_CA`: the test CA certificate (default `containers/tls/certs/ca.crt`,
//!   written by `containers/tls/gen-certs.sh`).
//!
//! - The client certificate and key for mutual TLS are read from the CA's directory
//!   (`client.crt`, `client.key`).
//!
//! Endpoints: Neo4j 7687 (TLS optional), Memgraph 7690 (TLS only), FalkorDB 6380 (TLS only),
//! FalkorDB 6381 (mutual TLS), Apache AGE 5455 and PostgreSQL 19 5456 (`ssl=on`). A test returns without failing when
//! its port is unreachable or the CA file is missing, like the other `live_*` tests.
//! Wrong-hostname cases connect to the host while verifying a name that is not in the
//! server certificate (`tls_server_name`).
#![cfg(feature = "tls")]

use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;

use voyager_drivers::{
    AsyncConnection, BoltConnection, ConnectionConfig, NetError, ParsedUri, PostgresConnection,
    RedisConnection, connect_any,
};

const WRONG_NAME: &str = "wrong-name.voyager.invalid";

fn host() -> String {
    std::env::var("VOYAGER_TEST_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn ca_file() -> PathBuf {
    std::env::var_os("VOYAGER_TEST_TLS_CA")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../containers/tls/certs/ca.crt")
        })
}

/// Returns the CA path when the endpoint is reachable and the CA exists, else prints a skip.
fn live(port: u16, what: &str) -> Option<String> {
    let ca = ca_file();
    if !ca.is_file() {
        eprintln!(
            "[SKIP] {what}: test CA not found at {} (run containers/tls/gen-certs.sh or set VOYAGER_TEST_TLS_CA)",
            ca.display()
        );
        return None;
    }
    let reachable = (host().as_str(), port)
        .to_socket_addrs()
        .map(|addresses| {
            addresses.into_iter().any(|address| {
                std::net::TcpStream::connect_timeout(&address, Duration::from_millis(500)).is_ok()
            })
        })
        .unwrap_or(false);
    if !reachable {
        eprintln!("[SKIP] {what} is not reachable on {}:{port}", host());
        return None;
    }
    Some(ca.display().to_string())
}

/// A CA that did not sign the server certificate.
fn unrelated_ca() -> (tempdir::Dir, String) {
    let dir = tempdir::Dir::new();
    let key = rcgen::KeyPair::generate().unwrap();
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Unrelated CA");
    let cert = params.self_signed(&key).unwrap();
    let path = dir.0.join("unrelated-ca.pem");
    std::fs::write(&path, cert.pem()).unwrap();
    (dir, path.display().to_string())
}

mod tempdir {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub struct Dir(pub PathBuf);

    impl Dir {
        pub fn new() -> Self {
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let path = std::env::temp_dir().join(format!(
                "voyager-live-tls-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

fn expect_tls_error<T>(result: Result<T, NetError>, context: &str) -> String {
    match result {
        Err(NetError::TlsError(message)) => message,
        Err(other) => panic!("{context}: expected a TLS error, got {other:?}"),
        Ok(_) => panic!("{context}: expected a TLS error, but connected"),
    }
}

async fn bolt(uri: &str) -> Result<BoltConnection, NetError> {
    let config = ConnectionConfig::from_uri(uri);
    BoltConnection::connect(&config).await
}

async fn bolt_return_one(conn: &mut BoltConnection) {
    let result = conn
        .execute("RETURN 1 AS n", &HashMap::new())
        .await
        .expect("query over TLS");
    assert_eq!(result.row_count(), 1);
}

// ---------------------------------------------------------------------------
// Neo4j (TLS optional on 7687)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn live_neo4j_bolt_tls() {
    let Some(ca) = live(7687, "Neo4j") else {
        return;
    };
    let h = host();
    let auth = "neo4j:voyagerpass123";

    for scheme in ["bolt+s", "neo4j+s"] {
        let mut conn = bolt(&format!("{scheme}://{auth}@{h}:7687?tls_ca_file={ca}"))
            .await
            .unwrap_or_else(|e| panic!("{scheme}: {e}"));
        assert!(conn.is_tls(), "{scheme}");
        bolt_return_one(&mut conn).await;
        conn.close().await.unwrap();
    }

    // +ssc accepts the certificate without a CA.
    let mut conn = bolt(&format!("bolt+ssc://{auth}@{h}:7687"))
        .await
        .expect("bolt+ssc");
    assert!(conn.is_tls());
    bolt_return_one(&mut conn).await;

    // A URI parameter cannot turn bolt+s into plaintext, even though this port allows it.
    for query in ["tls=false", "ssl=0", "SSLMODE=disable"] {
        match bolt(&format!("bolt+s://{auth}@{h}:7687?{query}")).await {
            Err(NetError::InvalidUri(_)) => {}
            Err(other) => panic!("{query}: expected InvalidUri, got {other:?}"),
            Ok(_) => panic!("{query}: connected"),
        }
    }

    // The same port still accepts plaintext (TLS level OPTIONAL).
    let mut plain = bolt(&format!("bolt://{auth}@{h}:7687")).await.unwrap();
    assert!(!plain.is_tls());
    bolt_return_one(&mut plain).await;

    let (_dir, unrelated) = unrelated_ca();
    expect_tls_error(
        bolt(&format!("bolt+s://{auth}@{h}:7687?tls_ca_file={unrelated}")).await,
        "Neo4j wrong CA",
    );
    expect_tls_error(
        bolt(&format!("bolt+s://{auth}@{h}:7687")).await,
        "Neo4j system roots do not trust the test CA",
    );
    let message = expect_tls_error(
        bolt(&format!(
            "bolt+s://{auth}@{h}:7687?tls_ca_file={ca}&tls_server_name={WRONG_NAME}"
        ))
        .await,
        "Neo4j wrong hostname",
    );
    assert!(message.contains("not valid for name"), "{message}");

    // A name from the certificate works through the override (SNI + verification).
    bolt(&format!(
        "bolt+s://{auth}@{h}:7687?tls_ca_file={ca}&tls_server_name=localhost"
    ))
    .await
    .expect("server name override");
}

// ---------------------------------------------------------------------------
// Memgraph (TLS only on 7690)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn live_memgraph_bolt_tls() {
    let Some(ca) = live(7690, "Memgraph TLS") else {
        return;
    };
    let h = host();
    for scheme in ["bolt+s", "memgraph+s"] {
        let mut conn = bolt(&format!("{scheme}://{h}:7690?tls_ca_file={ca}"))
            .await
            .unwrap_or_else(|e| panic!("{scheme}: {e}"));
        assert!(conn.is_tls());
        bolt_return_one(&mut conn).await;
        conn.close().await.unwrap();
    }
    let (_dir, unrelated) = unrelated_ca();
    expect_tls_error(
        bolt(&format!("bolt+s://{h}:7690?tls_ca_file={unrelated}")).await,
        "Memgraph wrong CA",
    );
    expect_tls_error(
        bolt(&format!(
            "bolt+s://{h}:7690?tls_ca_file={ca}&tls_server_name={WRONG_NAME}"
        ))
        .await,
        "Memgraph wrong hostname",
    );
}

// ---------------------------------------------------------------------------
// FalkorDB (TLS only on 6380)
// ---------------------------------------------------------------------------

async fn redis(uri: &str) -> Result<RedisConnection, NetError> {
    RedisConnection::connect(&ParsedUri::parse(uri)?).await
}

#[tokio::test]
async fn live_falkordb_tls() {
    let Some(ca) = live(6380, "FalkorDB TLS") else {
        return;
    };
    let h = host();
    let graph = format!("voyager_tls_test_{}", std::process::id());

    let mut conn = redis(&format!("rediss://{h}:6380/{graph}?tls_ca_file={ca}"))
        .await
        .expect("rediss");
    assert!(conn.is_tls());
    conn.ping().await.unwrap();
    conn.execute("CREATE (:TlsProbe {v: 1})", &HashMap::new())
        .await
        .expect("write over TLS");
    let result = conn
        .execute("MATCH (n:TlsProbe) RETURN n.v AS v", &HashMap::new())
        .await
        .expect("read over TLS");
    assert_eq!(result.row_count(), 1);
    conn.graph_delete(&graph)
        .await
        .expect("drop the test graph");

    for scheme in ["falkordbs", "valkeys"] {
        let mut conn = redis(&format!("{scheme}://{h}:6380?tls_ca_file={ca}"))
            .await
            .unwrap_or_else(|e| panic!("{scheme}: {e}"));
        assert!(conn.is_tls());
        conn.ping().await.unwrap();
    }

    // Plaintext RESP against the TLS-only port is not reported as a connection.
    assert!(
        redis(&format!("redis://{h}:6380?connect_timeout=5"))
            .await
            .is_err()
    );
    // A parameter cannot turn `rediss` into plaintext.
    assert!(matches!(
        redis(&format!("rediss://{h}:6380?tls=false")).await,
        Err(NetError::InvalidUri(_))
    ));

    // `?tls=insecure` encrypts without verifying.
    assert!(
        redis(&format!("redis://{h}:6380?tls=insecure"))
            .await
            .unwrap()
            .is_tls()
    );

    let (_dir, unrelated) = unrelated_ca();
    expect_tls_error(
        redis(&format!("rediss://{h}:6380?tls_ca_file={unrelated}")).await,
        "FalkorDB wrong CA",
    );
    expect_tls_error(
        redis(&format!(
            "rediss://{h}:6380?tls_ca_file={ca}&tls_server_name={WRONG_NAME}"
        ))
        .await,
        "FalkorDB wrong hostname",
    );
}

#[tokio::test]
async fn live_falkordb_mutual_tls() {
    let Some(ca) = live(6381, "FalkorDB mutual TLS") else {
        return;
    };
    let dir = ca_file().parent().map(PathBuf::from).unwrap_or_default();
    let (cert, key) = (dir.join("client.crt"), dir.join("client.key"));
    if !cert.is_file() || !key.is_file() {
        eprintln!(
            "[SKIP] FalkorDB mutual TLS: no client certificate in {} (re-run containers/tls/gen-certs.sh)",
            dir.display()
        );
        return;
    }
    let h = host();
    let mut conn = redis(&format!(
        "rediss://{h}:6381?tls_ca_file={ca}&tls_cert_file={}&tls_key_file={}",
        cert.display(),
        key.display()
    ))
    .await
    .expect("client certificate accepted");
    conn.ping().await.unwrap();

    // Without a client certificate the server ends the session after the handshake.
    let refused = match redis(&format!("rediss://{h}:6381?tls_ca_file={ca}")).await {
        Ok(mut conn) => conn.ping().await.is_err(),
        Err(_) => true,
    };
    assert!(
        refused,
        "a session without a client certificate must not work"
    );
}

// ---------------------------------------------------------------------------
// PostgreSQL 19 (5456) and Apache AGE (5455), ssl=on
// ---------------------------------------------------------------------------

async fn pg(uri: &str) -> Result<PostgresConnection, NetError> {
    PostgresConnection::connect(&ParsedUri::parse(uri)?).await
}

async fn pg_sslmodes(port: u16, scheme: &str, what: &str) {
    let Some(ca) = live(port, what) else {
        return;
    };
    let h = host();
    let base = format!("{scheme}://postgres:voyagerpass123@{h}:{port}/voyager_graph");

    // (query, expect TLS)
    let ok = [
        (format!("sslmode=verify-full&sslrootcert={ca}"), true),
        (format!("sslmode=verify-ca&sslrootcert={ca}"), true),
        (
            format!("sslmode=verify-ca&sslrootcert={ca}&tls_server_name={WRONG_NAME}"),
            true,
        ),
        ("sslmode=require".to_string(), true),
        // Direct TLS negotiation (PostgreSQL 17+): no SSLRequest, ALPN `postgresql`.
        (
            format!("sslmode=verify-full&sslrootcert={ca}&sslnegotiation=direct"),
            true,
        ),
        (
            format!("sslmode=verify-ca&tls_ca_file={ca}&sslnegotiation=direct"),
            true,
        ),
        ("sslmode=require&sslnegotiation=direct".to_string(), true),
        ("sslmode=prefer".to_string(), true),
        ("sslmode=allow".to_string(), false),
        ("sslmode=disable".to_string(), false),
    ];
    for (query, tls) in ok {
        let uri = format!("{base}?{query}");
        let mut conn = pg(&uri)
            .await
            .unwrap_or_else(|e| panic!("{what} {query}: {e}"));
        assert_eq!(conn.is_tls(), tls, "{what} {query}");
        let result = conn
            .execute("SELECT 1 AS n", &HashMap::new())
            .await
            .unwrap_or_else(|e| panic!("{what} {query}: {e}"));
        assert_eq!(result.row_count(), 1);
        conn.close().await.unwrap();
    }

    let (_dir, unrelated) = unrelated_ca();
    for query in [
        format!("sslmode=verify-full&sslrootcert={ca}&tls_server_name={WRONG_NAME}"),
        format!("sslmode=verify-ca&sslrootcert={unrelated}"),
        format!("sslmode=require&sslrootcert={unrelated}"),
        format!("sslmode=prefer&sslrootcert={unrelated}"),
        "sslmode=verify-full".to_string(),
        // `tls_ca_file=system` implies verify-full against the system roots (no test CA there).
        "tls_ca_file=system".to_string(),
        format!("sslmode=verify-full&sslrootcert={unrelated}&sslnegotiation=direct"),
    ] {
        expect_tls_error(
            pg(&format!("{base}?{query}")).await,
            &format!("{what} {query}"),
        );
    }

    // Weakening or contradictory TLS parameters are rejected before connecting.
    let tls_base = format!("{scheme}+s://postgres:voyagerpass123@{h}:{port}/voyager_graph");
    for uri in [
        format!("{tls_base}?sslmode=disable"),
        format!("{tls_base}?ssl=false"),
        format!("{tls_base}?sslmode=require"),
        format!("{base}?sslmode=require&tls_ca_file=system"),
        format!("{base}?sslmode=verify-full&sslmode=disable"),
        format!("{base}?sslmode=prefer&sslnegotiation=direct"),
    ] {
        match pg(&uri).await {
            Err(NetError::InvalidUri(_)) => {}
            Err(other) => panic!("{what} {uri}: expected InvalidUri, got {other:?}"),
            Ok(_) => panic!("{what} {uri}: connected"),
        }
    }

    // connect_any goes through the same client.
    let mut conn = connect_any(&format!("{base}?sslmode=verify-full&sslrootcert={ca}"))
        .await
        .expect("connect_any over TLS");
    conn.ping().await.unwrap();
}

#[tokio::test]
async fn live_postgres19_sslmodes() {
    pg_sslmodes(5456, "postgresql", "PostgreSQL 19").await;
}

#[tokio::test]
async fn live_age_sslmodes() {
    // `age://` also loads the AGE extension after connecting.
    pg_sslmodes(5455, "age", "Apache AGE").await;
}
