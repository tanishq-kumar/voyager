//! Runs every Query Spec v1 reference vector through the Rust consumer.
//!
//! The same documents are used by `just spec-check` through the Python
//! bindings, so the two APIs are pinned to one recorded behaviour.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde_json::Value;
use voyager_ogm::ast::LiteralValue;
use voyager_ogm::spec::{SpecCompileOptions, compile};

const DIALECTS: [(&str, &str); 5] = [
    ("cypher", "cypher"),
    ("age", "age"),
    ("iso_gql", "iso_gql"),
    ("sql_pgq", "sql_pgq"),
    ("duck_pgq", "duckpgq"),
];

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn load_documents(directory: &str) -> Vec<(String, Value)> {
    let path = repo_root().join("spec/vectors").join(directory);
    let mut entries: Vec<_> = fs::read_dir(&path)
        .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
        .map(|entry| entry.expect("vector directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    entries.sort();
    assert!(!entries.is_empty(), "no vectors in {}", path.display());
    entries
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()));
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("vector file name")
                .to_string();
            (name, serde_json::from_str(&text).expect("vector JSON"))
        })
        .collect()
}

fn literal(value: &Value) -> LiteralValue {
    match value {
        Value::Null => LiteralValue::Null,
        Value::Bool(boolean) => LiteralValue::Bool(*boolean),
        Value::Number(number) => {
            if let Some(integer) = number.as_i64() {
                LiteralValue::Int64(integer)
            } else {
                LiteralValue::Float64(number.as_f64().expect("number"))
            }
        }
        Value::String(string) => LiteralValue::String(string.clone()),
        Value::Array(items) => LiteralValue::List(items.iter().map(literal).collect()),
        Value::Object(members) => LiteralValue::Map(
            members
                .iter()
                .map(|(key, value)| (key.clone(), literal(value)))
                .collect(),
        ),
    }
}

fn parameters(value: &Value) -> HashMap<String, LiteralValue> {
    value
        .as_object()
        .expect("parameter object")
        .iter()
        .map(|(key, value)| (key.clone(), literal(value)))
        .collect()
}

fn options() -> SpecCompileOptions {
    SpecCompileOptions {
        use_cache: false,
        ..SpecCompileOptions::default()
    }
}

#[test]
fn valid_vectors_compile_as_recorded() {
    for (name, document) in load_documents("valid") {
        let spec = &document["spec"];
        let expected = document["expected"].as_object().expect("expected object");
        assert_eq!(expected.len(), DIALECTS.len(), "{name}: dialect keys");
        for (vector_key, dialect) in DIALECTS {
            let expected = expected
                .get(vector_key)
                .unwrap_or_else(|| panic!("{name}: missing expected.{vector_key}"));
            let result = compile(spec, dialect, &options());
            if let Some(expected_code) = expected.get("error").and_then(Value::as_str) {
                let error = result
                    .err()
                    .unwrap_or_else(|| panic!("{name}/{vector_key}: expected {expected_code}"));
                assert_eq!(
                    error.code().as_str(),
                    expected_code,
                    "{name}/{vector_key}: {}",
                    error.message()
                );
                continue;
            }
            let compiled = result.unwrap_or_else(|error| {
                panic!(
                    "{name}/{vector_key}: {} at {:?}",
                    error.message(),
                    error.path()
                )
            });
            assert_eq!(
                compiled.compiled.statement,
                expected["statement"].as_str().expect("statement"),
                "{name}/{vector_key}"
            );
            assert_eq!(
                compiled.compiled.parameters,
                parameters(&expected["parameters"]),
                "{name}/{vector_key}"
            );
        }
    }
}

#[test]
fn invalid_vectors_fail_with_the_recorded_path() {
    for (name, document) in load_documents("invalid") {
        let spec = &document["spec"];
        let expected = &document["expected"];
        let error = compile(spec, "cypher", &options())
            .err()
            .unwrap_or_else(|| panic!("{name}: expected an error"));
        assert_eq!(error.code().as_str(), "INVALID_SPEC", "{name}");
        assert_eq!(
            error.path(),
            expected["path"].as_str(),
            "{name}: {}",
            error.message()
        );
        assert!(
            error
                .message()
                .contains(expected["path"].as_str().expect("path"))
                || (expected["path"] == "" && error.message().contains("document root")),
            "{name}: pointer missing from {}",
            error.message()
        );
    }
}
