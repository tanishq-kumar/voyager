//! Live `doctor --uri` tests.
//!
//! Each test probes its TCP port first and returns without failing when the
//! database is not running, matching the `live_*` tests in `voyager-drivers`.
//! Start the containers with
//! `podman compose -f containers/compose.yaml up -d neo4j apache-age falkordb`,
//! or point the tests at another machine with `VOYAGER_TEST_HOST` (default
//! `127.0.0.1`).

use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Command, Output};
use std::time::Duration;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_voyager")
}

/// Host that runs the live databases: `VOYAGER_TEST_HOST`, or `127.0.0.1`.
fn host() -> String {
    std::env::var("VOYAGER_TEST_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn is_reachable(port: u16) -> bool {
    let Ok(addresses) = (host().as_str(), port).to_socket_addrs() else {
        return false;
    };
    addresses
        .into_iter()
        .any(|address| TcpStream::connect_timeout(&address, Duration::from_millis(500)).is_ok())
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn live_doctor_checks_neo4j() {
    if !is_reachable(7687) {
        eprintln!("[SKIP] Live Neo4j is not reachable on {}:7687", host());
        return;
    }

    let output = Command::new(bin())
        .args([
            "doctor",
            "--uri",
            &format!("bolt://{}:7687", host()),
            "--username",
            "neo4j",
            "--password-env",
            "VOYAGER_NEO4J_PASSWORD",
        ])
        .env("VOYAGER_NEO4J_PASSWORD", "voyagerpass123")
        .output()
        .expect("run voyager doctor");
    assert!(output.status.success(), "{}", stderr(&output));

    let text = stdout(&output);
    assert!(text.contains("protocol: Bolt v"), "{text}");
    assert!(text.contains("server version: Neo4j"), "{text}");
    assert!(text.contains("dialect: cypher"), "{text}");
    assert!(
        text.contains("latency:") && text.contains("ping round trip"),
        "{text}"
    );
    assert!(text.contains("status: ok"), "{text}");
}

#[test]
fn live_doctor_checks_memgraph_without_credentials() {
    if !is_reachable(7688) {
        eprintln!("[SKIP] Live Memgraph is not reachable on {}:7688", host());
        return;
    }

    let output = Command::new(bin())
        .args(["doctor", "--uri", &format!("bolt://{}:7688", host())])
        .output()
        .expect("run voyager doctor");
    assert!(output.status.success(), "{}", stderr(&output));

    let text = stdout(&output);
    assert!(text.contains("protocol: Bolt v"), "{text}");
    assert!(text.contains("server version:"), "{text}");
    assert!(text.to_lowercase().contains("memgraph"), "{text}");
    assert!(text.contains("dialect: cypher"), "{text}");
    assert!(
        text.contains("latency:") && text.contains("ping round trip"),
        "{text}"
    );
    assert!(text.contains("status: ok"), "{text}");
}

#[test]
fn live_doctor_checks_falkordb_without_credentials() {
    if !is_reachable(6379) {
        eprintln!("[SKIP] Live FalkorDB is not reachable on {}:6379", host());
        return;
    }

    let output = Command::new(bin())
        .args(["doctor", "--uri", &format!("redis://{}:6379", host())])
        .output()
        .expect("run voyager doctor");
    assert!(output.status.success(), "{}", stderr(&output));

    let text = stdout(&output);
    assert!(text.contains("protocol: Redis/RESP"), "{text}");
    assert!(text.contains("server version:"), "{text}");
    assert!(text.contains("dialect: cypher"), "{text}");
    assert!(
        text.contains("latency:") && text.contains("ping round trip"),
        "{text}"
    );
    assert!(text.contains("status: ok"), "{text}");
}

#[test]
fn live_doctor_checks_apache_age() {
    if !is_reachable(5455) {
        eprintln!("[SKIP] Live Apache AGE is not reachable on {}:5455", host());
        return;
    }

    let output = Command::new(bin())
        .args([
            "doctor",
            "--uri",
            &format!(
                "age://postgres:voyagerpass123@{}:5455/voyager_graph",
                host()
            ),
        ])
        .output()
        .expect("run voyager doctor");
    assert!(output.status.success(), "{}", stderr(&output));

    let text = stdout(&output);
    assert!(text.contains("protocol: PostgreSQL wire"), "{text}");
    assert!(text.contains("server version: 18"), "{text}");
    assert!(text.contains("database: voyager_graph"), "{text}");
    assert!(text.contains("dialect: age"), "{text}");
    assert!(
        text.contains("latency:") && text.contains("ping round trip"),
        "{text}"
    );
    assert!(text.contains("status: ok"), "{text}");
}

#[test]
fn live_doctor_checks_postgresql_19() {
    if !is_reachable(5456) {
        eprintln!(
            "[SKIP] Live PostgreSQL 19 is not reachable on {}:5456",
            host()
        );
        return;
    }

    let output = Command::new(bin())
        .args([
            "doctor",
            "--uri",
            &format!(
                "postgresql://postgres:voyagerpass123@{}:5456/voyager_graph",
                host()
            ),
        ])
        .output()
        .expect("run voyager doctor");
    assert!(output.status.success(), "{}", stderr(&output));

    let text = stdout(&output);
    assert!(text.contains("protocol: PostgreSQL wire"), "{text}");
    assert!(text.contains("server version: 19"), "{text}");
    assert!(text.contains("database: voyager_graph"), "{text}");
    assert!(text.contains("dialect: sql_pgq"), "{text}");
    assert!(
        text.contains("latency:") && text.contains("ping round trip"),
        "{text}"
    );
    assert!(text.contains("status: ok"), "{text}");
}

/// The TLS test CA: `VOYAGER_TEST_TLS_CA`, or `containers/tls/certs/ca.crt`.
fn tls_ca() -> Option<String> {
    let path = std::env::var_os("VOYAGER_TEST_TLS_CA")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../containers/tls/certs/ca.crt")
        });
    path.is_file().then(|| path.display().to_string())
}

#[test]
fn live_doctor_checks_tls_endpoints() {
    if !cfg!(feature = "tls") {
        eprintln!("[SKIP] voyager-cli was built without the `tls` feature");
        return;
    }
    let Some(ca) = tls_ca() else {
        eprintln!("[SKIP] TLS test CA not found (containers/tls/gen-certs.sh)");
        return;
    };
    let h = host();
    let checks = [
        (
            7687,
            format!("bolt+s://neo4j:voyagerpass123@{h}:7687?tls_ca_file={ca}"),
            "protocol: Bolt v",
        ),
        (
            6380,
            format!("rediss://{h}:6380?tls_ca_file={ca}"),
            "protocol: Redis/RESP",
        ),
        (
            5456,
            format!(
                "postgresql://postgres:voyagerpass123@{h}:5456/voyager_graph?sslmode=verify-full&sslrootcert={ca}"
            ),
            "protocol: PostgreSQL wire",
        ),
    ];
    for (port, uri, protocol) in checks {
        if !is_reachable(port) {
            eprintln!("[SKIP] {h}:{port} is not reachable");
            continue;
        }
        let output = Command::new(bin())
            .args(["doctor", "--uri", &uri])
            .output()
            .expect("run voyager doctor");
        assert!(output.status.success(), "{uri}: {}", stderr(&output));
        let text = stdout(&output);
        assert!(text.contains(protocol), "{text}");
        assert!(text.contains(" over TLS"), "{text}");
        assert!(text.contains("status: ok"), "{text}");
    }
}
