//! Integration tests for the `voyager` binary (also run as its `voy` alias).
//!
//! The parity test compiles every `spec/vectors/valid` document with the CLI and
//! compares the statement and parameters with the vector's recorded `expected`
//! output, which is the same source of truth the Python
//! `compile_query_from_spec` harness uses.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use serde_json::Value;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_voyager")
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn run(args: &[&str]) -> Output {
    Command::new(bin())
        .args(args)
        .output()
        .expect("run voyager binary")
}

fn run_with_stdin(args: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(bin())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn voyager binary");
    child
        .stdin
        .as_mut()
        .expect("stdin pipe")
        .write_all(input)
        .expect("write stdin");
    child.wait_with_output().expect("wait for voyager binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn vector(directory: &str, name: &str) -> Value {
    let path = repo_root().join("spec/vectors").join(directory).join(name);
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
    serde_json::from_str(&text).expect("vector JSON")
}

fn valid_vector(name: &str) -> Value {
    vector("valid", name)
}

#[test]
fn compile_all_valid_vectors_match_recorded_statement_and_parameters() {
    let directory = repo_root().join("spec/vectors/valid");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&directory)
        .expect("read valid vector directory")
        .map(|entry| entry.expect("vector entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    entries.sort();
    assert!(!entries.is_empty(), "no valid vectors found");

    let dialects = [
        ("cypher", "cypher"),
        ("age", "age"),
        ("iso_gql", "iso_gql"),
        ("sql_pgq", "sql_pgq"),
        ("duck_pgq", "duck_pgq"),
    ];

    for path in entries {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("vector file name");
        let document: Value = serde_json::from_str(
            &std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display())),
        )
        .expect("vector JSON");

        for (vector_key, dialect) in dialects {
            let expected = &document["expected"][vector_key];
            if expected.get("error").is_some() {
                continue;
            }

            let spec = serde_json::to_vec(&document["spec"]).expect("serialize spec");
            let output = run_with_stdin(
                &["compile", "-", "--dialect", dialect, "--format", "json"],
                &spec,
            );
            assert!(
                output.status.success(),
                "{name}/{vector_key} failed: {}",
                stderr(&output)
            );
            let compiled: Value =
                serde_json::from_slice(&output.stdout).expect("compile JSON output");
            assert_eq!(
                compiled["statement"], expected["statement"],
                "{name}/{vector_key}: statement"
            );
            assert_eq!(
                compiled["parameters"], expected["parameters"],
                "{name}/{vector_key}: parameters"
            );
            assert!(
                compiled["columns"].is_array(),
                "{name}/{vector_key}: columns missing"
            );
        }
    }
}

#[test]
fn compile_reads_stdin_and_keeps_stdout_to_the_statement() {
    let document = valid_vector("basic-match-return.json");
    let spec = serde_json::to_vec(&document["spec"]).expect("serialize spec");
    let statement = document["expected"]["cypher"]["statement"]
        .as_str()
        .expect("statement");

    let output = run_with_stdin(
        &["compile", "-", "--dialect", "cypher", "--format", "text"],
        &spec,
    );
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), format!("{statement}\n"));
    assert!(stderr(&output).is_empty(), "{}", stderr(&output));
}

#[test]
fn compile_show_params_writes_parameters_to_stderr() {
    let document = valid_vector("literal-nested-json.json");
    let spec = serde_json::to_vec(&document["spec"]).expect("serialize spec");
    let statement = document["expected"]["cypher"]["statement"]
        .as_str()
        .expect("statement");

    let output = run_with_stdin(
        &[
            "compile",
            "-",
            "--dialect",
            "cypher",
            "--format",
            "text",
            "--show-params",
        ],
        &spec,
    );
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(stdout(&output), format!("{statement}\n"));
    let errors = stderr(&output);
    assert!(errors.contains("Parameters:"), "{errors}");
    assert!(errors.contains("\"p0\""), "{errors}");
    assert!(errors.contains("\"Alice\""), "{errors}");
}

#[test]
fn compile_json_includes_warnings_when_the_consumer_returns_them() {
    let document = valid_vector("match-path-mode.json");
    let spec = serde_json::to_vec(&document["spec"]).expect("serialize spec");
    let output = run_with_stdin(
        &["compile", "-", "--dialect", "cypher", "--format", "json"],
        &spec,
    );
    assert!(output.status.success(), "{}", stderr(&output));
    let compiled: Value = serde_json::from_slice(&output.stdout).expect("compile JSON output");
    assert!(
        compiled["warnings"]
            .as_array()
            .is_some_and(|warnings| !warnings.is_empty()),
        "warnings missing from {}",
        stdout(&output)
    );
    assert!(stderr(&output).contains("warning:"), "{}", stderr(&output));
}

#[test]
fn validate_accepts_valid_and_reports_each_invalid_path() {
    let document = valid_vector("basic-match-return.json");
    let spec = serde_json::to_vec(&document["spec"]).expect("serialize spec");
    let output = run_with_stdin(&["validate", "-"], &spec);
    assert!(output.status.success(), "{}", stderr(&output));
    assert!(stdout(&output).is_empty(), "valid prints nothing");

    let document = vector("invalid", "unknown-root-key.json");
    let spec = serde_json::to_vec(&document["spec"]).expect("serialize spec");
    let output = run_with_stdin(&["validate", "-"], &spec);
    assert_eq!(output.status.code(), Some(1));
    let errors = stderr(&output);
    assert!(errors.contains("/bogus:"), "{errors}");
    assert!(errors.contains("(INVALID_SPEC)"), "{errors}");
}

#[test]
fn validate_reads_stdin_and_rejects_malformed_json() {
    let document = valid_vector("basic-match-return.json");
    let spec = serde_json::to_vec(&document["spec"]).expect("serialize spec");
    let output = run_with_stdin(&["validate", "-"], &spec);
    assert!(output.status.success(), "{}", stderr(&output));

    let output = run_with_stdin(&["validate", "-"], b"{ not json");
    assert_eq!(output.status.code(), Some(1));
    assert!(
        stderr(&output).contains("(INVALID_SPEC)"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn missing_input_file_exits_four() {
    let output = run(&[
        "compile",
        "/definitely/not/here.json",
        "--dialect",
        "cypher",
    ]);
    assert_eq!(output.status.code(), Some(4));
    assert!(
        stderr(&output).contains("cannot read"),
        "{}",
        stderr(&output)
    );
}

#[test]
fn usage_errors_exit_two() {
    let output = run(&["compile"]);
    assert_eq!(output.status.code(), Some(2));

    let valid = repo_root().join("spec/vectors/valid/basic-match-return.json");
    let output = run(&[
        "compile",
        valid.to_str().expect("vector path"),
        "--dialect",
        "not_a_dialect",
    ]);
    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn doctor_without_uri_reports_only_build_facts() {
    let output = run(&["doctor"]);
    assert!(output.status.success(), "{}", stderr(&output));
    let text = stdout(&output);
    assert!(text.contains("voyager-ogm"), "{text}");
    assert!(text.contains("driver protocol features:"), "{text}");
    assert!(
        text.contains("supported dialects: cypher, age, iso_gql, sql_pgq, duck_pgq"),
        "{text}"
    );
    assert!(!text.contains("latency:"), "no connection was made: {text}");
}

#[test]
fn doctor_connection_failure_exits_three() {
    let output = run(&["doctor", "--uri", "bolt://127.0.0.1:1"]);
    assert_eq!(output.status.code(), Some(3));
    assert!(stderr(&output).contains("doctor:"), "{}", stderr(&output));
}

#[test]
fn doctor_invalid_uri_exits_three() {
    let output = run(&["doctor", "--uri", "not-a-uri"]);
    assert_eq!(output.status.code(), Some(3));
    assert!(stderr(&output).contains("doctor:"), "{}", stderr(&output));
}

#[test]
fn help_documents_exit_codes_for_the_root_and_subcommands() {
    for args in [
        vec!["--help"],
        vec!["doctor", "--help"],
        vec!["compile", "--help"],
        vec!["validate", "--help"],
        vec!["introspect", "--help"],
    ] {
        let output = run(&args);
        assert!(output.status.success(), "{}", stderr(&output));
        let text = stdout(&output);
        assert!(text.contains("Exit codes:"), "{args:?}: {text}");
        assert!(
            text.contains("3  connection, check or catalog query failure"),
            "{args:?}: {text}"
        );
        assert!(
            text.contains("5  engine not supported by introspect"),
            "{args:?}: {text}"
        );
    }
}

#[test]
fn introspect_requires_a_uri() {
    let output = run(&["introspect"]);
    assert_eq!(output.status.code(), Some(2), "{}", stderr(&output));
    assert!(stderr(&output).contains("--uri"), "{}", stderr(&output));
}

#[test]
fn introspect_unsupported_engine_exits_five() {
    let output = run(&["introspect", "--uri", "duckdb:///tmp/voyager-introspect.db"]);
    assert_eq!(output.status.code(), Some(5), "{}", stderr(&output));
    let message = stderr(&output);
    assert!(
        message.starts_with(
            "introspect: Dialect 'DuckDB' does not support feature: live schema introspection"
        ),
        "{message}"
    );
    assert!(stdout(&output).is_empty());
}

#[test]
fn introspect_connection_failure_exits_three() {
    // Port 1 on localhost refuses connections.
    let output = run(&[
        "introspect",
        "--uri",
        "bolt://127.0.0.1:1",
        "--format",
        "json",
    ]);
    assert_eq!(output.status.code(), Some(3), "{}", stderr(&output));
    assert!(
        stderr(&output).starts_with("introspect: "),
        "{}",
        stderr(&output)
    );
    assert!(stdout(&output).is_empty());
}

#[test]
fn introspect_postgres_without_a_username_exits_three() {
    let output = run(&["introspect", "--uri", "postgresql://127.0.0.1:1/db?graph=g"]);
    assert_eq!(output.status.code(), Some(3), "{}", stderr(&output));
    assert!(
        stderr(&output).contains("introspect: PostgreSQL requires a username"),
        "{}",
        stderr(&output)
    );
}

/// The release archives install `voy` as a symlink to `voyager`; the program
/// must behave the same when started under that name.
#[cfg(unix)]
#[test]
fn voy_symlink_runs_the_same_program() {
    let dir = std::env::temp_dir().join(format!("voyager-cli-voy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let voy = dir.join("voy");
    let _ = std::fs::remove_file(&voy);
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_voyager"), &voy).expect("create voy symlink");
    let output = Command::new(&voy)
        .arg("--version")
        .output()
        .expect("run voy symlink");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(output.status.success(), "{}", stderr(&output));
    assert_eq!(
        stdout(&output).trim(),
        format!("voyager {}", env!("CARGO_PKG_VERSION"))
    );
}

#[cfg(feature = "postgres")]
mod pg_stub_tests {
    use super::*;
    use std::io::Read;
    use std::net::TcpListener;

    fn pg_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(msg_type);
        out.extend_from_slice(&(4 + payload.len() as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn pg_param_status(name: &str, value: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(name.as_bytes());
        payload.push(0);
        payload.extend_from_slice(value.as_bytes());
        payload.push(0);
        pg_frame(b'S', &payload)
    }

    fn pg_single_text_row(col: &str, val: &str) -> Vec<u8> {
        let mut out = Vec::new();
        let mut row_desc = Vec::new();
        row_desc.extend_from_slice(&1i16.to_be_bytes());
        row_desc.extend_from_slice(col.as_bytes());
        row_desc.push(0);
        row_desc.extend_from_slice(&0i32.to_be_bytes());
        row_desc.extend_from_slice(&0i16.to_be_bytes());
        row_desc.extend_from_slice(&25i32.to_be_bytes());
        row_desc.extend_from_slice(&(-1i16).to_be_bytes());
        row_desc.extend_from_slice(&(-1i32).to_be_bytes());
        row_desc.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&pg_frame(b'T', &row_desc));

        let mut data_row = Vec::new();
        data_row.extend_from_slice(&1i16.to_be_bytes());
        data_row.extend_from_slice(&(val.len() as i32).to_be_bytes());
        data_row.extend_from_slice(val.as_bytes());
        out.extend_from_slice(&pg_frame(b'D', &data_row));
        out.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));
        out.extend_from_slice(&pg_frame(b'Z', b"I"));
        out
    }

    fn spawn_pg_without_pgq(
        server_version: &'static str,
    ) -> (std::net::SocketAddr, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let handle = std::thread::spawn(move || {
            let Ok((mut stream, _)) = listener.accept() else {
                return;
            };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let mut startup = Vec::new();
            startup.extend_from_slice(&pg_frame(b'R', &0i32.to_be_bytes()));
            startup.extend_from_slice(&pg_param_status("server_version", server_version));
            startup.extend_from_slice(&pg_frame(b'Z', b"I"));
            if stream.write_all(&startup).is_err() {
                return;
            }
            while let Ok(n) = stream.read(&mut buf) {
                if n == 0 || buf[0] == b'X' {
                    break;
                }
                let query = String::from_utf8_lossy(&buf[5..n]);
                let reply = if query.contains("SHOW server_version") {
                    pg_single_text_row("server_version", server_version)
                } else if query.contains("pg_propgraph_element") {
                    pg_single_text_row("json_build_object", r#"{"pgq": false, "age": false}"#)
                } else {
                    pg_single_text_row("?column?", "1")
                };
                if stream.write_all(&reply).is_err() {
                    break;
                }
            }
        });
        (addr, handle)
    }

    #[test]
    fn doctor_warns_and_introspect_fails_when_postgres_lacks_pgq() {
        let (addr, handle) = spawn_pg_without_pgq("19.0");
        let doctor = run(&[
            "doctor",
            "--uri",
            &format!("postgresql://postgres:zz@{addr}/voyager_graph"),
        ]);
        let _ = handle.join();
        assert_eq!(doctor.status.code(), Some(0), "{}", stderr(&doctor));
        assert!(
            stdout(&doctor).contains("status: ok"),
            "{}",
            stdout(&doctor)
        );
        let err = stderr(&doctor);
        assert!(
            err.contains("warning: PostgreSQL 19.0 does not support SQL/PGQ"),
            "{err}"
        );
        assert!(
            err.contains("SQL/PGQ queries will fail on this server"),
            "{err}"
        );
        assert!(
            err.contains("experimental: PostgreSQL 19 Beta 1-3 and 20 development builds"),
            "{err}"
        );

        let (addr, handle) = spawn_pg_without_pgq("19.0");
        let introspect = run(&[
            "introspect",
            "--uri",
            &format!("postgresql://postgres:zz@{addr}/voyager_graph?graph=g"),
        ]);
        let _ = handle.join();
        assert_eq!(introspect.status.code(), Some(5), "{}", stderr(&introspect));
        let err = stderr(&introspect);
        assert!(
            err.contains("PostgreSQL 19.0 does not support SQL/PGQ"),
            "{err}"
        );
        assert!(
            err.contains("experimental: PostgreSQL 19 Beta 1-3 and 20 development builds"),
            "{err}"
        );
    }
}
