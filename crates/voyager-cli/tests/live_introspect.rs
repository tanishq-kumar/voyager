//! Live `introspect` tests.
//!
//! Each test creates a small schema with uniquely named labels on one engine,
//! runs `voyager introspect --format json`, compares it with the library
//! result (`voyager_drivers::introspect`) for the same labels, drops what it
//! created, and then checks the reported facts. A test returns without
//! failing when its database port is not reachable. Point the tests at
//! another machine with `VOYAGER_TEST_HOST` (default `127.0.0.1`).

use std::collections::HashMap;
use std::net::{TcpStream, ToSocketAddrs};
use std::process::{Command, Output};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use voyager_drivers::introspect::IntrospectOptions;
use voyager_drivers::uri::ParsedUri;

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

/// A name prefix unique to this test run.
fn unique_prefix(tag: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos())
        .unwrap_or_default();
    format!("Ix265{tag}{}x{nanos}", std::process::id())
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// Runs `voyager introspect --format json` and parses stdout.
fn cli_json(args: &[&str], env: &[(&str, &str)]) -> Result<Value, String> {
    let mut command = Command::new(bin());
    command
        .arg("introspect")
        .args(args)
        .args(["--format", "json"]);
    for (key, value) in env {
        command.env(key, value);
    }
    let output = command.output().map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(format!(
            "exit {:?}: {}",
            output.status.code(),
            stderr(&output)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

async fn library_json(uri: &str) -> Result<Value, String> {
    let parsed = ParsedUri::parse(uri).map_err(|error| error.to_string())?;
    voyager_drivers::introspect(&parsed, &IntrospectOptions::default())
        .await
        .map(|schema| schema.to_json_value())
        .map_err(|error| error.to_string())
}

/// Keeps only the entries whose label or type starts with `prefix`, so a
/// shared database with other schemas still compares deterministically.
fn own(schema: &Value, prefix: &str) -> Value {
    let keep = |key: &str, pick: &dyn Fn(&Value) -> bool| -> Value {
        Value::Array(
            schema[key]
                .as_array()
                .map(|items| items.iter().filter(|item| pick(item)).cloned().collect())
                .unwrap_or_default(),
        )
    };
    let starts = |value: &Value| value.as_str().is_some_and(|s| s.starts_with(prefix));
    json!({
        "nodes": keep("nodes", &|item| starts(&item["name"])),
        "relationships": keep("relationships", &|item| starts(&item["name"])),
        "constraints": keep("constraints", &|item| starts(&item["label_or_type"])),
        "indexes": keep("indexes", &|item| {
            item["labels_or_types"]
                .as_array()
                .is_some_and(|labels| labels.iter().any(starts))
        }),
        "source": schema["source"],
    })
}

fn node<'a>(schema: &'a Value, name: &str) -> &'a Value {
    schema["nodes"]
        .as_array()
        .and_then(|nodes| nodes.iter().find(|node| node["name"] == name))
        .unwrap_or_else(|| panic!("node {name} missing: {schema:#}"))
}

fn relationship<'a>(schema: &'a Value, name: &str) -> &'a Value {
    schema["relationships"]
        .as_array()
        .and_then(|rels| rels.iter().find(|rel| rel["name"] == name))
        .unwrap_or_else(|| panic!("relationship {name} missing: {schema:#}"))
}

/// Checks the facts every engine reports for the shared fixture: a node
/// label with `email` (unique), `name` (indexed), `age` (mixed or integer)
/// and a relationship type with an indexed `since`.
fn check_common_facts(schema: &Value, person: &str, knows: &str) {
    let person_node = node(schema, person);
    assert_eq!(person_node["labels"], json!([person]));
    assert_eq!(person_node["model"], Value::Null);
    let props = &person_node["properties"];
    assert_eq!(props["email"]["type"], "STRING", "{schema:#}");
    assert_eq!(props["email"]["unique"], true, "{schema:#}");
    assert_eq!(props["email"]["indexed"], false, "{schema:#}");
    assert_eq!(props["name"]["type"], "STRING", "{schema:#}");
    assert_eq!(props["name"]["indexed"], true, "{schema:#}");
    assert_eq!(props["name"]["index_type"], "BTREE", "{schema:#}");
    assert_eq!(props["name"]["unique"], false, "{schema:#}");
    for key in ["attribute", "description", "has_default", "constraints"] {
        assert_eq!(props["email"][key], Value::Null, "{key}");
    }

    let knows_rel = relationship(schema, knows);
    assert_eq!(knows_rel["type"], knows);
    assert_eq!(knows_rel["source_labels"], json!([person]), "{schema:#}");
    assert_eq!(knows_rel["target_labels"], json!([person]), "{schema:#}");
    assert_eq!(knows_rel["directed"], true);
    assert_eq!(knows_rel["properties"]["since"]["type"], "INTEGER");
    assert_eq!(
        knows_rel["properties"]["since"]["indexed"], true,
        "{schema:#}"
    );
}

#[tokio::test]
async fn live_introspect_neo4j() {
    use voyager_drivers::{AsyncConnection, BoltConnection, ConnectionConfig};

    if !is_reachable(7687) {
        eprintln!("[SKIP] Live Neo4j is not reachable on {}:7687", host());
        return;
    }
    let prefix = unique_prefix("Neo");
    let person = format!("{prefix}Person");
    let knows = format!("{prefix}Knows");
    let lower = prefix.to_ascii_lowercase();
    let uri = format!("bolt://neo4j:voyagerpass123@{}:7687", host());

    let config = ConnectionConfig::from_uri(uri.clone());
    let mut connection = BoltConnection::connect(&config).await.expect("connect");
    let setup = [
        format!(
            "CREATE CONSTRAINT {lower}_email IF NOT EXISTS FOR (n:{person}) REQUIRE n.email IS UNIQUE"
        ),
        format!("CREATE INDEX {lower}_name IF NOT EXISTS FOR (n:{person}) ON (n.name)"),
        format!("CREATE INDEX {lower}_since IF NOT EXISTS FOR ()-[r:{knows}]-() ON (r.since)"),
        format!(
            "CREATE (:{person} {{email: 'a', name: 'A', age: 1}})-[:{knows} {{since: 2020}}]->\
             (:{person} {{email: 'b', name: 'B', age: 2.5}})"
        ),
    ];
    for statement in &setup {
        connection
            .execute(statement, &HashMap::new())
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }

    let cli = cli_json(
        &[
            "--uri",
            &format!("bolt://{}:7687", host()),
            "--username",
            "neo4j",
            "--password-env",
            "VOYAGER_NEO4J_PASSWORD",
        ],
        &[("VOYAGER_NEO4J_PASSWORD", "voyagerpass123")],
    );
    let library = library_json(&uri).await;

    for statement in [
        format!("MATCH (n:{person}) DETACH DELETE n"),
        format!("DROP CONSTRAINT {lower}_email IF EXISTS"),
        format!("DROP INDEX {lower}_name IF EXISTS"),
        format!("DROP INDEX {lower}_since IF EXISTS"),
    ] {
        connection
            .execute(&statement, &HashMap::new())
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    let _ = connection.close().await;

    let cli = own(&cli.expect("cli"), &prefix);
    let library = own(&library.expect("library"), &prefix);
    assert_eq!(cli, library, "CLI JSON must equal the library result");
    assert_eq!(cli["source"]["engine"], "neo4j");
    check_common_facts(&cli, &person, &knows);
    assert_eq!(node(&cli, &person)["properties"]["age"]["type"], "ANY");
    assert_eq!(cli["constraints"].as_array().map(Vec::len), Some(1));
    // The constraint-backed index plus the two declared indexes.
    assert_eq!(cli["indexes"].as_array().map(Vec::len), Some(3), "{cli:#}");
}

#[tokio::test]
async fn live_introspect_memgraph() {
    use voyager_drivers::{AsyncConnection, BoltConnection, ConnectionConfig};

    if !is_reachable(7688) {
        eprintln!("[SKIP] Live Memgraph is not reachable on {}:7688", host());
        return;
    }
    let prefix = unique_prefix("Mg");
    let person = format!("{prefix}Person");
    let movie = format!("{prefix}Movie");
    let knows = format!("{prefix}Knows");
    let acted_in = format!("{prefix}ActedIn");
    let uri = format!("bolt://{}:7688", host());

    let config = ConnectionConfig::from_uri(uri.clone());
    let mut connection = BoltConnection::connect(&config).await.expect("connect");
    let setup = [
        format!("CREATE CONSTRAINT ON (n:{person}) ASSERT n.email IS UNIQUE"),
        format!("CREATE CONSTRAINT ON (n:{person}) ASSERT EXISTS (n.email)"),
        format!("CREATE CONSTRAINT ON (n:{person}) ASSERT n.age IS TYPED INTEGER"),
        format!("CREATE INDEX ON :{person}"),
        format!("CREATE INDEX ON :{person}(name)"),
        format!("CREATE EDGE INDEX ON :{knows}"),
        format!("CREATE EDGE INDEX ON :{knows}(since)"),
        format!("CREATE INDEX ON :{acted_in}(billing)"),
        format!(
            "CREATE (p1:{person} {{email: 'a', name: 'A', age: 1}})-[:{knows} {{since: 2020}}]->\
             (p2:{person} {{email: 'b', name: 'B', age: 2}}), \
             (p1)-[:{acted_in} {{billing: 1}}]->(:{movie} {{title: 'M'}})"
        ),
    ];
    for statement in &setup {
        connection
            .execute(statement, &HashMap::new())
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }

    let cli = cli_json(&["--uri", &uri], &[]);
    let library = library_json(&uri).await;

    for statement in [
        format!("MATCH (n:{person}) DETACH DELETE n"),
        format!("MATCH (n:{movie}) DETACH DELETE n"),
        format!("DROP CONSTRAINT ON (n:{person}) ASSERT n.email IS UNIQUE"),
        format!("DROP CONSTRAINT ON (n:{person}) ASSERT EXISTS (n.email)"),
        format!("DROP CONSTRAINT ON (n:{person}) ASSERT n.age IS TYPED INTEGER"),
        format!("DROP INDEX ON :{person}(name)"),
        format!("DROP INDEX ON :{person}"),
        format!("DROP EDGE INDEX ON :{knows}(since)"),
        format!("DROP EDGE INDEX ON :{knows}"),
        format!("DROP INDEX ON :{acted_in}(billing)"),
    ] {
        connection
            .execute(&statement, &HashMap::new())
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    let _ = connection.close().await;

    let cli = own(&cli.expect("cli"), &prefix);
    let library = own(&library.expect("library"), &prefix);
    assert_eq!(cli, library, "CLI JSON must equal the library result");
    assert_eq!(cli["source"]["engine"], "memgraph");
    check_common_facts(&cli, &person, &knows);
    let props = &node(&cli, &person)["properties"];
    assert_eq!(props["email"]["nullable"], false, "{cli:#}");
    assert_eq!(props["age"]["type"], "INTEGER", "{cli:#}");
    assert_eq!(
        cli["constraints"].as_array().map(Vec::len),
        Some(3),
        "{cli:#}"
    );

    // #429: Memgraph relationship indexes (both `edge-type*` and `label+property`
    // matching a relationship type) must appear as relationship indexes with no
    // phantom node label.
    let node_names: Vec<&str> = cli["nodes"]
        .as_array()
        .expect("nodes")
        .iter()
        .filter_map(|n| n["name"].as_str())
        .collect();
    assert!(!node_names.contains(&acted_in.as_str()), "{cli:#}");
    assert!(!node_names.contains(&knows.as_str()), "{cli:#}");
    let acted_rel = relationship(&cli, &acted_in);
    assert_eq!(
        acted_rel["properties"]["billing"]["indexed"], true,
        "{cli:#}"
    );
    assert_eq!(
        acted_rel["properties"]["billing"]["index_type"], "BTREE",
        "{cli:#}"
    );
    let indexes = cli["indexes"].as_array().expect("indexes");
    assert!(
        indexes.iter().any(|idx| {
            idx["entity"] == "relationship"
                && idx["labels_or_types"] == json!([acted_in])
                && idx["properties"] == json!(["billing"])
        }),
        "{cli:#}"
    );
    assert!(
        indexes.iter().any(|idx| {
            idx["entity"] == "relationship"
                && idx["labels_or_types"] == json!([knows])
                && idx["properties"] == json!([])
                && idx["index_type"] == "LABEL"
        }),
        "{cli:#}"
    );
    assert!(
        indexes.iter().any(|idx| {
            idx["entity"] == "node"
                && idx["labels_or_types"] == json!([person])
                && idx["properties"] == json!(["name"])
        }),
        "{cli:#}"
    );
}

#[tokio::test]
async fn live_introspect_falkordb() {
    use voyager_drivers::{AsyncConnection, RedisConnection};

    if !is_reachable(6379) {
        eprintln!("[SKIP] Live FalkorDB is not reachable on {}:6379", host());
        return;
    }
    let prefix = unique_prefix("Fk");
    let person = format!("{prefix}Person");
    let movie = format!("{prefix}Movie");
    let knows = format!("{prefix}Knows");
    let graph = prefix.to_ascii_lowercase();
    let uri = format!("falkordb://{}:6379?graph={graph}", host());

    let parsed = ParsedUri::parse(&uri).expect("parse");
    let mut connection = RedisConnection::connect(&parsed).await.expect("connect");
    for query in [
        format!(
            "CREATE (:{person} {{email: 'a', name: 'A', bio: 'first', age: 1}})-[:{knows} {{since: 2020}}]->\
             (:{person} {{email: 'b', name: 'B', bio: 'second', age: 2.5}}), \
             (:{movie} {{title: 'Voyager', overview: 'Graph story'}})"
        ),
        format!("CREATE INDEX FOR (n:{person}) ON (n.email)"),
        format!("CREATE INDEX FOR (n:{person}) ON (n.name)"),
        format!("CREATE FULLTEXT INDEX FOR (n:{person}) ON (n.bio)"),
        format!("CREATE INDEX FOR (n:{movie}) ON (n.title)"),
        format!("CREATE FULLTEXT INDEX FOR (n:{movie}) ON (n.title, n.overview)"),
        format!("CREATE INDEX FOR ()-[r:{knows}]-() ON (r.since)"),
    ] {
        connection
            .graph_query(&graph, &query, &HashMap::new())
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
    }
    for kind in ["UNIQUE", "MANDATORY"] {
        connection
            .execute_raw(&[
                "GRAPH.CONSTRAINT",
                "CREATE",
                &graph,
                kind,
                "NODE",
                &person,
                "PROPERTIES",
                "1",
                "email",
            ])
            .await
            .unwrap_or_else(|error| panic!("{kind} constraint: {error}"));
    }
    // Constraints are built asynchronously; wait until both are operational.
    for _ in 0..50 {
        let result = connection
            .graph_ro_query(
                &graph,
                "CALL db.constraints() YIELD status WHERE status <> 'OPERATIONAL' RETURN count(*) AS pending",
                &HashMap::new(),
            )
            .await
            .expect("constraint status");
        let pending = result
            .rows
            .first()
            .and_then(|row| row.first())
            .map(|value| value.to_string_lossy());
        if pending.as_deref() == Some("0") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let cli = cli_json(&["--uri", &uri], &[]);
    let text_output = Command::new(bin())
        .args(["introspect", "--uri", &uri])
        .output()
        .expect("run voyager introspect text");
    let library = library_json(&uri).await;

    connection.graph_delete(&graph).await.expect("delete graph");
    let _ = connection.close().await;

    assert!(text_output.status.success(), "{}", stderr(&text_output));
    let text = String::from_utf8_lossy(&text_output.stdout);
    assert!(
        text.contains(&format!("FULLTEXT node {movie}(title, overview)")),
        "{text}"
    );
    assert!(
        text.contains(&format!("FULLTEXT node {person}(bio)")),
        "{text}"
    );

    let cli = own(&cli.expect("cli"), &prefix);
    let library = own(&library.expect("library"), &prefix);
    assert_eq!(cli, library, "CLI JSON must equal the library result");
    assert_eq!(cli["source"]["engine"], "falkordb");
    assert_eq!(cli["source"]["database"], graph.as_str());
    check_common_facts(&cli, &person, &knows);
    let props = &node(&cli, &person)["properties"];
    assert_eq!(props["email"]["nullable"], false, "{cli:#}");
    assert_eq!(props["age"]["type"], "ANY", "{cli:#}");

    // #430: multi-property full-text index grouped into one IndexEntry,
    // single-property full-text and range indexes unchanged.
    let fulltext_movie: Vec<_> = cli["indexes"]
        .as_array()
        .expect("indexes")
        .iter()
        .filter(|idx| idx["index_type"] == "FULLTEXT" && idx["labels_or_types"] == json!([movie]))
        .collect();
    assert_eq!(fulltext_movie.len(), 1, "{cli:#}");
    assert_eq!(
        fulltext_movie[0]["properties"],
        json!(["title", "overview"]),
        "{cli:#}"
    );
    let fulltext_person: Vec<_> = cli["indexes"]
        .as_array()
        .expect("indexes")
        .iter()
        .filter(|idx| idx["index_type"] == "FULLTEXT" && idx["labels_or_types"] == json!([person]))
        .collect();
    assert_eq!(fulltext_person.len(), 1, "{cli:#}");
    assert_eq!(fulltext_person[0]["properties"], json!(["bio"]), "{cli:#}");
    assert_eq!(
        node(&cli, &movie)["properties"]["title"]["indexed"],
        true,
        "{cli:#}"
    );
}

#[tokio::test]
async fn live_introspect_postgresql_19() {
    use voyager_drivers::{AsyncConnection, PostgresConnection};

    if !is_reachable(5456) {
        eprintln!(
            "[SKIP] Live PostgreSQL 19 is not reachable on {}:5456",
            host()
        );
        return;
    }
    let prefix = unique_prefix("Pg");
    let lower = prefix.to_ascii_lowercase();
    let person = format!("{prefix}Person");
    let knows = format!("{prefix}Knows");
    let graph = format!("{lower}_g");
    let base = format!(
        "postgresql://postgres:voyagerpass123@{}:5456/voyager_graph",
        host()
    );
    let uri = format!("{base}?graph={graph}");

    let parsed = ParsedUri::parse(&base).expect("parse");
    let mut connection = PostgresConnection::connect(&parsed).await.expect("connect");
    let setup = [
        format!(
            "CREATE TABLE {lower}_person (pid text PRIMARY KEY, email text UNIQUE, name text, \
             age bigint)"
        ),
        format!("CREATE INDEX {lower}_person_name ON {lower}_person (name)"),
        format!(
            "CREATE TABLE {lower}_knows (src text REFERENCES {lower}_person (pid), \
             dst text REFERENCES {lower}_person (pid), since bigint, PRIMARY KEY (src, dst))"
        ),
        format!("CREATE INDEX {lower}_knows_since ON {lower}_knows (since)"),
        format!(
            "CREATE PROPERTY GRAPH {graph} \
             VERTEX TABLES ({lower}_person KEY (pid) LABEL \"{person}\" \
               PROPERTIES (pid, email, name, age)) \
             EDGE TABLES ({lower}_knows KEY (src, dst) \
               SOURCE KEY (src) REFERENCES {lower}_person (pid) \
               DESTINATION KEY (dst) REFERENCES {lower}_person (pid) \
               LABEL \"{knows}\" PROPERTIES (since))"
        ),
    ];
    for statement in &setup {
        connection
            .execute(statement, &HashMap::new())
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }

    let cli = cli_json(&["--uri", &uri], &[]);
    let library = library_json(&uri).await;

    for statement in [
        format!("DROP PROPERTY GRAPH IF EXISTS {graph}"),
        format!("DROP TABLE IF EXISTS {lower}_knows"),
        format!("DROP TABLE IF EXISTS {lower}_person"),
    ] {
        connection
            .execute(&statement, &HashMap::new())
            .await
            .unwrap_or_else(|error| panic!("{statement}: {error}"));
    }
    let _ = connection.close().await;

    let cli = own(&cli.expect("cli"), &prefix);
    let library = own(&library.expect("library"), &prefix);
    assert_eq!(cli, library, "CLI JSON must equal the library result");
    assert_eq!(cli["source"]["engine"], "postgresql");
    check_common_facts(&cli, &person, &knows);
    let person_node = node(&cli, &person);
    assert_eq!(person_node["primary_key"], "pid", "{cli:#}");
    let pid = &person_node["properties"]["pid"];
    assert_eq!(pid["primary_key"], true);
    assert_eq!(pid["nullable"], false);
    assert_eq!(person_node["properties"]["age"]["type"], "INTEGER");
}

#[test]
fn live_introspect_apache_age_is_unsupported() {
    if !is_reachable(5455) {
        eprintln!("[SKIP] Live Apache AGE is not reachable on {}:5455", host());
        return;
    }
    let output = Command::new(bin())
        .args([
            "introspect",
            "--uri",
            &format!(
                "age://postgres:voyagerpass123@{}:5455/voyager_graph",
                host()
            ),
        ])
        .output()
        .expect("run voyager introspect");
    assert_eq!(output.status.code(), Some(5), "{}", stderr(&output));
    assert!(
        stderr(&output).contains("Dialect 'Apache AGE' does not support feature"),
        "{}",
        stderr(&output)
    );
}
