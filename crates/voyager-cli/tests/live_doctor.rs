//! Live `doctor --uri` tests.
//!
//! Each test probes its TCP port first and returns without failing when the
//! container is not running, matching the `live_*` tests in `voyager-drivers`.
//! Start the containers with
//! `podman compose -f containers/compose.yaml up -d neo4j apache-age falkordb`.

use std::net::{SocketAddr, TcpStream};
use std::process::{Command, Output};
use std::time::Duration;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_voyager")
}

fn is_reachable(port: u16) -> bool {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    TcpStream::connect_timeout(&address, Duration::from_millis(500)).is_ok()
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
        eprintln!("[SKIP] Live Neo4j is not reachable on 127.0.0.1:7687");
        return;
    }

    let output = Command::new(bin())
        .args([
            "doctor",
            "--uri",
            "bolt://127.0.0.1:7687",
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
fn live_doctor_checks_apache_age() {
    if !is_reachable(5455) {
        eprintln!("[SKIP] Live Apache AGE is not reachable on 127.0.0.1:5455");
        return;
    }

    let output = Command::new(bin())
        .args([
            "doctor",
            "--uri",
            "age://postgres:voyagerpass123@127.0.0.1:5455/voyager_graph",
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
fn live_doctor_checks_falkordb() {
    if !is_reachable(6379) {
        eprintln!("[SKIP] Live FalkorDB is not reachable on 127.0.0.1:6379");
        return;
    }

    let output = Command::new(bin())
        .args(["doctor", "--uri", "redis://127.0.0.1:6379"])
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
