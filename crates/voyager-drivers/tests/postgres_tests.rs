//! Unit tests for PostgreSQL Frontend/Backend v3.0 message codec, authentication, and AGE agtype parser.

use bytes::BytesMut;
use std::collections::HashMap;
use voyager_drivers::postgres::{
    AgeValue, AuthenticationRequest, BackendMessage, FrontendMessage, PG_PROTOCOL_V3, ScramClient,
    StartupMessage, TransactionStatus, age_value_to_canonical_json, clean_agtype_string,
    compute_md5_password, parse_agtype, prepare_postgres_statement, rows_to_record_batch,
};
use voyager_drivers::{ConnectOptions, QueryResult, QuerySummary};

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[test]
fn test_startup_message_encoding() {
    let startup = StartupMessage::new("testuser", Some("testdb"));
    let mut buf = BytesMut::new();
    startup.encode(&mut buf).unwrap();

    // First 4 bytes are length, next 4 are protocol version
    assert!(buf.len() > 8);
    let mut bytes = buf.freeze();
    use bytes::Buf;
    let len = bytes.get_i32();
    let version = bytes.get_i32();

    assert_eq!(version, PG_PROTOCOL_V3);
    assert_eq!(len as usize, bytes.len() + 8);
}

#[test]
fn test_frontend_query_encoding() {
    let msg = FrontendMessage::Query("SELECT 1;".to_string());
    let mut buf = BytesMut::new();
    msg.encode(&mut buf).unwrap();

    assert_eq!(buf[0], b'Q');
    let mut bytes = buf.freeze();
    use bytes::Buf;
    bytes.advance(1); // skip 'Q'
    let len = bytes.get_i32();
    assert_eq!(len, 4 + 9 + 1); // 4 (len) + 9 ("SELECT 1;") + 1 (\0)
    assert_eq!(&bytes[..], b"SELECT 1;\0");
}

#[test]
fn test_frontend_parse_bind_execute_sync() {
    let parse = FrontendMessage::Parse {
        name: "stmt1".to_string(),
        query: "SELECT $1::int;".to_string(),
        param_types: vec![23],
    };
    let mut buf = BytesMut::new();
    parse.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'P');

    let bind = FrontendMessage::Bind {
        portal: "".to_string(),
        statement: "stmt1".to_string(),
        param_formats: vec![0],
        params: vec![Some(b"42".to_vec())],
        result_formats: vec![0],
    };
    buf.clear();
    bind.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'B');

    let exec = FrontendMessage::Execute {
        portal: "".to_string(),
        max_rows: 0,
    };
    buf.clear();
    exec.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'E');

    let sync = FrontendMessage::Sync;
    buf.clear();
    sync.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'S');
    assert_eq!(buf.len(), 5);
}

#[test]
fn test_backend_message_decoding() {
    // Test AuthenticationOk ('R', 0)
    let mut payload = BytesMut::new();
    use bytes::BufMut;
    payload.put_i32(0);
    let msg = BackendMessage::decode(b'R', payload.freeze()).unwrap();
    assert_eq!(
        msg,
        BackendMessage::Authentication(AuthenticationRequest::Ok)
    );

    // Test AuthenticationMD5Password ('R', 5, salt)
    let mut payload = BytesMut::new();
    payload.put_i32(5);
    payload.put_slice(&[1, 2, 3, 4]);
    let msg = BackendMessage::decode(b'R', payload.freeze()).unwrap();
    assert_eq!(
        msg,
        BackendMessage::Authentication(AuthenticationRequest::MD5Password { salt: [1, 2, 3, 4] })
    );

    // Test ReadyForQuery ('Z', 'I')
    let mut payload = BytesMut::new();
    payload.put_u8(b'I');
    let msg = BackendMessage::decode(b'Z', payload.freeze()).unwrap();
    assert_eq!(msg, BackendMessage::ReadyForQuery(TransactionStatus::Idle));

    // Test CommandComplete ('C', "SELECT 10\0")
    let mut payload = BytesMut::new();
    payload.put_slice(b"SELECT 10\0");
    let msg = BackendMessage::decode(b'C', payload.freeze()).unwrap();
    assert_eq!(
        msg,
        BackendMessage::CommandComplete("SELECT 10".to_string())
    );

    // Test ErrorResponse ('E')
    let mut payload = BytesMut::new();
    payload.put_u8(b'S');
    payload.put_slice(b"ERROR\0");
    payload.put_u8(b'C');
    payload.put_slice(b"42P01\0");
    payload.put_u8(b'M');
    payload.put_slice(b"relation does not exist\0");
    payload.put_u8(0);
    let msg = BackendMessage::decode(b'E', payload.freeze()).unwrap();
    if let BackendMessage::ErrorResponse(diag) = msg {
        assert_eq!(diag.severity, "ERROR");
        assert_eq!(diag.code, "42P01");
        assert_eq!(diag.message, "relation does not exist");
    } else {
        panic!("Expected ErrorResponse");
    }
}

#[test]
fn test_md5_authentication_hash() {
    let user = "postgres";
    let password = "voyagerpass123";
    let salt = [0x12, 0x34, 0x56, 0x78];

    let hash = compute_md5_password(user, password, &salt);
    assert_eq!(hash.len(), 36); // "md5" (3) + 32 hex chars + '\0' (1) = 36
    assert!(hash.starts_with(b"md5"));
    assert_eq!(hash.last(), Some(&0));
}

#[test]
fn test_scram_sha256_client_flow() {
    let mut client = ScramClient::new();
    let first_msg = client.client_first_message();
    let first_msg_str = String::from_utf8(first_msg).unwrap();
    assert!(first_msg_str.starts_with("n,,n=,r="));

    // Extract client nonce
    let client_nonce = first_msg_str.strip_prefix("n,,n=,r=").unwrap();

    // Mock server challenge with iterations=4096 and salt
    let server_nonce = format!("{}mock_server_nonce_12345", client_nonce);
    let salt_b64 = "c2FsdF9leGFtcGxlXzEyMzQ="; // "salt_example_1234"
    let challenge = format!("r={},s={},i=4096", server_nonce, salt_b64);

    let client_final = client
        .process_challenge(challenge.as_bytes(), "secretpassword")
        .unwrap();
    let final_str = String::from_utf8(client_final).unwrap();

    assert!(final_str.starts_with("c=biws,r="));
    assert!(final_str.contains(",p="));
}

#[test]
fn test_agtype_clean_and_parse_scalars() {
    assert_eq!(clean_agtype_string("  42::numeric  "), "42");
    assert_eq!(clean_agtype_string("{\"id\": 1}::vertex"), "{\"id\": 1}");

    // Integer
    let val = parse_agtype("12345").unwrap();
    assert_eq!(val, AgeValue::Integer(12345));

    // Float
    let val = parse_agtype("12.3456").unwrap();
    assert_eq!(val, AgeValue::Float(12.3456));

    // Boolean
    let val = parse_agtype("true").unwrap();
    assert_eq!(val, AgeValue::Boolean(true));

    // String
    let val = parse_agtype("\"hello world\"").unwrap();
    assert_eq!(val, AgeValue::String("hello world".to_string()));

    // Null
    let val = parse_agtype("null").unwrap();
    assert_eq!(val, AgeValue::Null);
}

#[test]
fn test_agtype_vertex_and_edge_parsing() {
    let raw_vertex = r#"{"id": 844424930131969, "label": "Person", "properties": {"name": "Alice", "age": 30}}::vertex"#;
    let parsed = parse_agtype(raw_vertex).unwrap();

    if let AgeValue::Vertex(v) = parsed {
        assert_eq!(v.id, 844424930131969);
        assert_eq!(v.label, "Person");
        assert_eq!(v.properties.get("name").unwrap(), "Alice");
        assert_eq!(v.properties.get("age").unwrap(), 30);

        let bolt_node = v.to_bolt_node();
        assert_eq!(bolt_node.id, 844424930131969);
        assert_eq!(bolt_node.labels, vec!["Person".to_string()]);
    } else {
        panic!("Expected AgeValue::Vertex");
    }

    let raw_edge = r#"{"id": 1125899906842625, "label": "KNOWS", "end_id": 844424930131970, "start_id": 844424930131969, "properties": {"since": 2022}}::edge"#;
    let parsed = parse_agtype(raw_edge).unwrap();

    if let AgeValue::Edge(e) = parsed {
        assert_eq!(e.id, 1125899906842625);
        assert_eq!(e.label, "KNOWS");
        assert_eq!(e.start_id, 844424930131969);
        assert_eq!(e.end_id, 844424930131970);
        assert_eq!(e.properties.get("since").unwrap(), 2022);

        let bolt_rel = e.to_bolt_relationship();
        assert_eq!(bolt_rel.id, 1125899906842625);
        assert_eq!(bolt_rel.rel_type, "KNOWS");
    } else {
        panic!("Expected AgeValue::Edge");
    }
}

#[test]
fn test_agtype_path_parsing() {
    let raw_path = r#"[{"id": 1, "label": "Person", "properties": {"name": "Alice"}}::vertex, {"id": 10, "label": "KNOWS", "start_id": 1, "end_id": 2, "properties": {}}::edge, {"id": 2, "label": "Person", "properties": {"name": "Bob"}}::vertex]::path"#;
    let parsed = parse_agtype(raw_path).unwrap();

    if let AgeValue::Path(p) = parsed {
        assert_eq!(p.vertices.len(), 2);
        assert_eq!(p.edges.len(), 1);
        assert_eq!(p.vertices[0].label, "Person");
        assert_eq!(p.edges[0].label, "KNOWS");

        let bolt_path = p.to_bolt_path();
        assert_eq!(bolt_path.nodes.len(), 2);
        assert_eq!(bolt_path.relationships.len(), 1);
    } else {
        panic!("Expected AgeValue::Path");
    }
}

#[test]
fn test_agtype_list_of_vertices() {
    let raw = r#"[{"id": 1, "label": "Person", "properties": {"name": "Alice"}}::vertex, {"id": 2, "label": "Person", "properties": {"name": "Bob"}}::vertex]"#;
    let parsed = parse_agtype(raw).unwrap();
    if let AgeValue::List(items) = parsed {
        assert_eq!(items.len(), 2);
        if let AgeValue::Vertex(v1) = &items[0] {
            assert_eq!(v1.id, 1);
            assert_eq!(v1.label, "Person");
            assert_eq!(v1.properties.get("name").unwrap(), "Alice");
        } else {
            panic!("Expected AgeValue::Vertex for items[0]");
        }
        if let AgeValue::Vertex(v2) = &items[1] {
            assert_eq!(v2.id, 2);
            assert_eq!(v2.label, "Person");
            assert_eq!(v2.properties.get("name").unwrap(), "Bob");
        } else {
            panic!("Expected AgeValue::Vertex for items[1]");
        }
    } else {
        panic!("Expected AgeValue::List");
    }
}

#[test]
fn test_agtype_list_of_edges() {
    let raw = r#"[{"id": 10, "label": "KNOWS", "start_id": 1, "end_id": 2, "properties": {"since": 2020}}::edge]"#;
    let parsed = parse_agtype(raw).unwrap();
    if let AgeValue::List(items) = parsed {
        assert_eq!(items.len(), 1);
        if let AgeValue::Edge(e) = &items[0] {
            assert_eq!(e.id, 10);
            assert_eq!(e.label, "KNOWS");
            assert_eq!(e.start_id, 1);
            assert_eq!(e.end_id, 2);
            assert_eq!(e.properties.get("since").unwrap(), 2020);
        } else {
            panic!("Expected AgeValue::Edge");
        }
    } else {
        panic!("Expected AgeValue::List");
    }
}

#[test]
fn test_agtype_path_inside_list() {
    let raw = r#"[[{"id": 1, "label": "Person", "properties": {}}::vertex, {"id": 10, "label": "KNOWS", "start_id": 1, "end_id": 2, "properties": {}}::edge, {"id": 2, "label": "Person", "properties": {}}::vertex]::path]"#;
    let parsed = parse_agtype(raw).unwrap();
    if let AgeValue::List(items) = parsed {
        assert_eq!(items.len(), 1);
        if let AgeValue::Path(p) = &items[0] {
            assert_eq!(p.vertices.len(), 2);
            assert_eq!(p.edges.len(), 1);
            assert_eq!(p.vertices[0].id, 1);
            assert_eq!(p.edges[0].id, 10);
            assert_eq!(p.vertices[1].id, 2);
        } else {
            panic!("Expected AgeValue::Path");
        }
    } else {
        panic!("Expected AgeValue::List");
    }
}

#[test]
fn test_agtype_map_containing_vertex() {
    let raw = r#"{"node": {"id": 1, "label": "Person", "properties": {"name": "Alice"}}::vertex, "count": 42}"#;
    let parsed = parse_agtype(raw).unwrap();
    if let AgeValue::Map(map) = parsed {
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("count").unwrap(), &AgeValue::Integer(42));
        if let Some(AgeValue::Vertex(v)) = map.get("node") {
            assert_eq!(v.id, 1);
            assert_eq!(v.label, "Person");
            assert_eq!(v.properties.get("name").unwrap(), "Alice");
        } else {
            panic!("Expected AgeValue::Vertex for key 'node'");
        }
    } else {
        panic!("Expected AgeValue::Map");
    }
}

#[test]
fn test_agtype_nested_list() {
    let raw = r#"[[{"id": 1, "label": "Person", "properties": {}}::vertex]]"#;
    let parsed = parse_agtype(raw).unwrap();
    if let AgeValue::List(outer) = parsed {
        assert_eq!(outer.len(), 1);
        if let AgeValue::List(inner) = &outer[0] {
            assert_eq!(inner.len(), 1);
            if let AgeValue::Vertex(v) = &inner[0] {
                assert_eq!(v.id, 1);
                assert_eq!(v.label, "Person");
            } else {
                panic!("Expected AgeValue::Vertex");
            }
        } else {
            panic!("Expected inner AgeValue::List");
        }
    } else {
        panic!("Expected outer AgeValue::List");
    }
}

#[test]
fn test_agtype_quoted_string_case() {
    let raw_str = r#""foo::vertex""#;
    let parsed = parse_agtype(raw_str).unwrap();
    assert_eq!(parsed, AgeValue::String("foo::vertex".to_string()));

    let raw_vertex_with_suffix_in_prop =
        r#"{"id": 1, "label": "Person", "properties": {"info": "user::vertex::edge"}}::vertex"#;
    let parsed = parse_agtype(raw_vertex_with_suffix_in_prop).unwrap();
    if let AgeValue::Vertex(v) = parsed {
        assert_eq!(v.properties.get("info").unwrap(), "user::vertex::edge");
    } else {
        panic!("Expected AgeValue::Vertex");
    }
}

#[test]
fn test_agtype_unannotated_map_stays_map() {
    let raw_map = r#"{"id": 1, "label": "x", "properties": {"city": "Berlin"}}"#;
    let parsed = parse_agtype(raw_map).unwrap();
    if let AgeValue::Map(map) = parsed {
        assert_eq!(map.len(), 3);
        assert_eq!(map.get("id").unwrap(), &AgeValue::Integer(1));
        assert_eq!(
            map.get("label").unwrap(),
            &AgeValue::String("x".to_string())
        );
        if let Some(AgeValue::Map(props)) = map.get("properties") {
            assert_eq!(
                props.get("city").unwrap(),
                &AgeValue::String("Berlin".to_string())
            );
        } else {
            panic!("Expected nested AgeValue::Map for 'properties'");
        }
    } else {
        panic!("Expected unannotated object to decode as AgeValue::Map, not an entity");
    }
}

#[test]
fn test_agtype_unannotated_list_stays_list() {
    let raw_list = r#"[{"id": 1, "label": "Person", "properties": {}}::vertex, {"id": 10, "label": "KNOWS", "start_id": 1, "end_id": 2, "properties": {}}::edge, {"id": 2, "label": "Person", "properties": {}}::vertex]"#;
    let parsed = parse_agtype(raw_list).unwrap();
    if let AgeValue::List(items) = parsed {
        assert_eq!(items.len(), 3);
        assert!(matches!(&items[0], AgeValue::Vertex(v) if v.id == 1));
        assert!(matches!(&items[1], AgeValue::Edge(e) if e.id == 10));
        assert!(matches!(&items[2], AgeValue::Vertex(v) if v.id == 2));
    } else {
        panic!(
            "Expected unannotated array [a, r, b] to decode as AgeValue::List of three items, not AgeValue::Path"
        );
    }
}

#[test]
fn test_rows_to_arrow_record_batch() {
    let cols = vec![
        "id".to_string(),
        "name".to_string(),
        "score".to_string(),
        "active".to_string(),
    ];
    let rows = vec![
        vec![
            Some("101".to_string()),
            Some("Alice".to_string()),
            Some("98.5".to_string()),
            Some("t".to_string()),
        ],
        vec![
            Some("102".to_string()),
            Some("Bob".to_string()),
            Some("85.0".to_string()),
            Some("f".to_string()),
        ],
        vec![
            Some("103".to_string()),
            None,
            Some("91.2".to_string()),
            Some("true".to_string()),
        ],
    ];

    let batch = rows_to_record_batch(&cols, &rows).unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 4);

    let schema = batch.schema();
    assert_eq!(schema.field(0).data_type(), &arrow_schema::DataType::Int64);
    assert_eq!(schema.field(1).data_type(), &arrow_schema::DataType::Utf8);
    assert_eq!(
        schema.field(2).data_type(),
        &arrow_schema::DataType::Float64
    );
    assert_eq!(
        schema.field(3).data_type(),
        &arrow_schema::DataType::Boolean
    );
}

#[test]
fn test_clean_agtype_string_unicode_regression() {
    assert_eq!(clean_agtype_string("é"), "é");
    assert_eq!(clean_agtype_string("Renée"), "Renée");
    assert_eq!(clean_agtype_string("\"Renée\""), "\"Renée\"");
    assert_eq!(
        clean_agtype_string("{\"name\": \"Renée\"}::vertex"),
        "{\"name\": \"Renée\"}"
    );
    assert_eq!(clean_agtype_string("Renée::vertex"), "Renée");
    assert_eq!(clean_agtype_string("\"Renée\"::vertex"), "\"Renée\"");
    assert_eq!(clean_agtype_string("é::vertex"), "é");
    assert_eq!(clean_agtype_string("123::numeric"), "123");

    // Verify rows_to_record_batch type inference on multi-byte strings like Renée does not panic
    let cols = vec!["name".to_string()];
    let rows = vec![vec![Some("Renée".to_string())]];
    let batch =
        rows_to_record_batch(&cols, &rows).expect("Failed to infer record batch with Renée");
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_prepared_age_statement_parse_and_bind_encoding() {
    let query =
        "SELECT * FROM cypher('g', $$ MATCH (n) WHERE n.name = $p0 RETURN n $$, %s) AS (n agtype)";
    let mut params = HashMap::new();
    params.insert("p0".to_string(), serde_json::json!("Alice").into());
    let prepared = prepare_postgres_statement(query, &params).unwrap();

    let parse = FrontendMessage::Parse {
        name: String::new(),
        query: prepared.statement.clone(),
        param_types: prepared.parameter_oids.clone(),
    };
    let mut buf = BytesMut::new();
    parse.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'P');
    assert!(contains_bytes(&buf, b"$1) AS (n agtype)"));
    assert!(contains_bytes(
        &buf,
        b"$$ MATCH (n) WHERE n.name = $p0 RETURN n $$"
    ));
    assert!(!contains_bytes(&buf, b", %s)"));

    let bind = FrontendMessage::Bind {
        portal: String::new(),
        statement: String::new(),
        param_formats: vec![0; prepared.parameter_values.len()],
        params: prepared.parameter_values.clone(),
        result_formats: vec![0],
    };
    let mut buf = BytesMut::new();
    bind.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'B');
    assert!(contains_bytes(&buf, br#"{"p0":"Alice"}"#));
    assert!(!contains_bytes(&buf, b"Alice';"));
}

#[test]
fn test_prepared_sql_statement_parse_and_bind_encoding() {
    let query = "SELECT * FROM t WHERE a = $p10 AND b = $p1 AND c = $p10";
    let mut params = HashMap::new();
    params.insert("p1".to_string(), serde_json::json!(42).into());
    params.insert("p10".to_string(), serde_json::json!("x' OR 1=1 --").into());
    let prepared = prepare_postgres_statement(query, &params).unwrap();

    let parse = FrontendMessage::Parse {
        name: String::new(),
        query: prepared.statement.clone(),
        param_types: prepared.parameter_oids.clone(),
    };
    let mut buf = BytesMut::new();
    parse.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'P');
    assert!(contains_bytes(&buf, b"WHERE a = $1 AND b = $2 AND c = $1"));
    assert!(!contains_bytes(&buf, b"OR 1=1"));

    let bind = FrontendMessage::Bind {
        portal: String::new(),
        statement: String::new(),
        param_formats: vec![0; prepared.parameter_values.len()],
        params: prepared.parameter_values.clone(),
        result_formats: vec![0],
    };
    let mut buf = BytesMut::new();
    bind.encode(&mut buf).unwrap();
    assert_eq!(buf[0], b'B');
    assert!(contains_bytes(&buf, b"x' OR 1=1 --"));
}

#[test]
fn test_age_value_to_canonical_json_shapes() {
    let vertex =
        parse_agtype(r#"{"id": 1, "label": "P", "properties": {"name": "A"}}::vertex"#).unwrap();
    let canonical = age_value_to_canonical_json(&vertex);
    assert_eq!(canonical["kind"], "node");
    assert_eq!(canonical["id"], 1);
    assert_eq!(canonical["labels"][0], "P");
    assert_eq!(canonical["properties"]["name"], "A");

    let edge = parse_agtype(
        r#"{"id": 2, "label": "K", "start_id": 1, "end_id": 3, "properties": {}}::edge"#,
    )
    .unwrap();
    let canonical = age_value_to_canonical_json(&edge);
    assert_eq!(canonical["kind"], "relationship");
    assert_eq!(canonical["type"], "K");
    assert_eq!(canonical["start_id"], 1);
    assert_eq!(canonical["end_id"], 3);

    let path = parse_agtype(
        r#"[{"id": 1, "label": "P", "properties": {}}::vertex,
            {"id": 2, "label": "K", "start_id": 1, "end_id": 3, "properties": {}}::edge,
            {"id": 3, "label": "P", "properties": {}}::vertex]::path"#,
    )
    .unwrap();
    let canonical = age_value_to_canonical_json(&path);
    assert_eq!(canonical["kind"], "path");
    assert_eq!(canonical["nodes"].as_array().unwrap().len(), 2);
    assert_eq!(canonical["relationships"].as_array().unwrap().len(), 1);
    assert_eq!(canonical["sequence"], serde_json::json!([1]));

    let list = parse_agtype(r#"[{"id": 1, "label": "P", "properties": {}}::vertex]"#).unwrap();
    let canonical = age_value_to_canonical_json(&list);
    assert_eq!(canonical[0]["kind"], "node");

    let plain_map = parse_agtype(r#"{"id": 1, "label": "x", "properties": {}}"#).unwrap();
    let canonical = age_value_to_canonical_json(&plain_map);
    assert_eq!(canonical["id"], 1);
    assert!(canonical.get("kind").is_none());
}

#[test]
fn test_connect_options_default_is_not_age() {
    assert!(!ConnectOptions::default().age);
    assert!(
        ConnectOptions::default()
            .prepared_statement_cache_size
            .is_none()
    );
    assert!(
        ConnectOptions {
            age: true,
            ..Default::default()
        }
        .age
    );
}

#[test]
fn test_query_result_defaults_to_no_age_columns() {
    let result = QueryResult::new(vec!["n".to_string()], Vec::new(), QuerySummary::empty());
    assert!(result.age_columns.is_empty());
}

#[test]
fn test_rows_to_record_batch_with_age_columns_decodes_entities_and_scalars() {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;
    use voyager_drivers::postgres::rows_to_record_batch_with_age_columns;

    let v_a = r#"{"id": 1, "label": "ZzS", "properties": {"name": "A"}}::vertex"#;
    let e_k =
        r#"{"id": 2, "label": "ZK", "start_id": 1, "end_id": 3, "properties": {"w": 1}}::edge"#;
    let v_b = r#"{"id": 3, "label": "ZzS", "properties": {"name": "B"}}::vertex"#;
    let path = format!("[{v_a}, {e_k}, {v_b}]::path");

    let cols = vec![
        "a".to_string(),
        "k".to_string(),
        "p".to_string(),
        "score".to_string(),
        "plain".to_string(),
    ];
    let rows = vec![vec![
        Some(v_a.to_string()),
        Some(e_k.to_string()),
        Some(path),
        Some("42".to_string()),
        Some("raw_text".to_string()),
    ]];
    let age_columns = vec![true, true, true, true, false];

    let batch = rows_to_record_batch_with_age_columns(&cols, &rows, &age_columns).unwrap();
    let schema = batch.schema();

    assert_eq!(
        schema
            .field(0)
            .metadata()
            .get("voyager.encoding")
            .map(String::as_str),
        Some("json")
    );
    assert_eq!(
        schema
            .field(0)
            .metadata()
            .get("voyager.kind")
            .map(String::as_str),
        Some("node")
    );
    assert_eq!(
        schema
            .field(1)
            .metadata()
            .get("voyager.kind")
            .map(String::as_str),
        Some("relationship")
    );
    assert_eq!(
        schema
            .field(2)
            .metadata()
            .get("voyager.kind")
            .map(String::as_str),
        Some("path")
    );
    assert_eq!(schema.field(3).data_type(), &DataType::Int64);
    assert_eq!(schema.field(4).data_type(), &DataType::Utf8);
    assert!(!schema.field(4).metadata().contains_key("voyager.encoding"));

    let path_val: serde_json::Value =
        serde_json::from_str(batch.column(2).as_string::<i32>().value(0)).unwrap();
    assert_eq!(path_val["kind"], "path");
    assert!(path_val.get("sequence").is_none());
    assert_eq!(path_val["nodes"][0]["element_id"], serde_json::Value::Null);
}

fn row_description_payload(fields: &[(&str, i32)]) -> BytesMut {
    use bytes::BufMut;
    let mut payload = BytesMut::new();
    payload.put_i16(fields.len() as i16);
    for (name, type_oid) in fields {
        payload.put_slice(name.as_bytes());
        payload.put_u8(0);
        payload.put_i32(0); // table OID
        payload.put_i16(0); // column attribute number
        payload.put_i32(*type_oid);
        payload.put_i16(-1); // variable length
        payload.put_i32(-1); // type modifier
        payload.put_i16(0); // text format
    }
    payload
}

fn data_row_payload(values: &[Option<&str>]) -> BytesMut {
    use bytes::BufMut;
    let mut payload = BytesMut::new();
    payload.put_i16(values.len() as i16);
    for value in values {
        match value {
            Some(bytes) => {
                payload.put_i32(bytes.len() as i32);
                payload.put_slice(bytes.as_bytes());
            }
            None => payload.put_i32(-1),
        }
    }
    payload
}

#[test]
fn test_wire_multidimensional_array_columns_decode_to_nested_arrow_lists() {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use voyager_drivers::postgres::agtype::rows_to_record_batch_with_column_metadata;

    // RowDescription for bigint[][] and text[][][] columns.
    let description = BackendMessage::decode(
        b'T',
        row_description_payload(&[("matrix", 1016), ("cube", 1009)]).freeze(),
    )
    .expect("RowDescription");
    let (columns, field_types, field_type_modifiers) = match description {
        BackendMessage::RowDescription(fields) => (
            fields
                .iter()
                .map(|field| field.name.clone())
                .collect::<Vec<_>>(),
            fields
                .iter()
                .map(|field| field.type_oid)
                .collect::<Vec<_>>(),
            fields
                .iter()
                .map(|field| field.type_modifier)
                .collect::<Vec<_>>(),
        ),
        other => panic!("expected RowDescription, got {other:?}"),
    };

    // DataRow text format for a 2-D matrix with NULLs and a 3-D text cube with NULLs.
    let data = BackendMessage::decode(
        b'D',
        data_row_payload(&[
            Some("{{1,2},{3,NULL}}"),
            Some(r#"{{{alpha,NULL,"beta,gamma"}},{{"NULL",x,"quote""d"}}}"#),
        ])
        .freeze(),
    )
    .expect("DataRow");
    let rows: Vec<Vec<Option<String>>> = match data {
        BackendMessage::DataRow(values) => vec![
            values
                .into_iter()
                .map(|value| {
                    value.map(|bytes| String::from_utf8(bytes.to_vec()).expect("UTF-8 array text"))
                })
                .collect(),
        ],
        other => panic!("expected DataRow, got {other:?}"),
    };

    let batch = rows_to_record_batch_with_column_metadata(
        &columns,
        &rows,
        &[false, false],
        &field_types,
        &field_type_modifiers,
    )
    .expect("multidimensional arrays decode");

    let matrix = batch.column(0).as_list::<i32>();
    let matrix_row_values = matrix.value(0);
    let matrix_rows = matrix_row_values.as_list::<i32>();
    assert_eq!(matrix_rows.len(), 2);
    let first_row_values = matrix_rows.value(0);
    let first = first_row_values.as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!((first.value(0), first.value(1)), (1, 2));
    let second_row_values = matrix_rows.value(1);
    let second = second_row_values.as_primitive::<arrow_array::types::Int64Type>();
    assert_eq!(second.value(0), 3);
    assert!(second.is_null(1));

    let cube = batch.column(1).as_list::<i32>();
    let cube_plane_values = cube.value(0);
    let cube_planes = cube_plane_values.as_list::<i32>();
    assert_eq!(cube_planes.len(), 2);
    let plane_values = cube_planes.value(0);
    let plane = plane_values.as_list::<i32>();
    let cell_values = plane.value(0);
    let values = cell_values.as_string::<i32>();
    assert_eq!(values.value(0), "alpha");
    assert!(values.is_null(1));
    assert_eq!(values.value(2), "beta,gamma");
}

#[tokio::test]
async fn test_pg_execute_rows_and_arrow_equivalence_mixed_arrays_zero_rows_and_age() {
    use bytes::BufMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use voyager_drivers::engine::{AsyncConnection, ResultMode};
    use voyager_drivers::postgres::PostgresConnection;
    use voyager_drivers::value::result::ResultValue;
    use voyager_drivers::{ParamValue, ParsedUri};

    fn pg_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(msg_type);
        out.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn rd_payload(fields: &[(&str, i32, i32)]) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_i16(fields.len() as i16);
        for (name, oid, typmod) in fields {
            payload.put_slice(name.as_bytes());
            payload.put_u8(0);
            payload.put_i32(0);
            payload.put_i16(0);
            payload.put_i32(*oid);
            payload.put_i16(-1);
            payload.put_i32(*typmod);
            payload.put_i16(0);
        }
        payload.to_vec()
    }

    fn dr_bytes(values: &[Option<&[u8]>]) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_i16(values.len() as i16);
        for val in values {
            match val {
                Some(b) => {
                    payload.put_i32(b.len() as i32);
                    payload.put_slice(b);
                }
                None => payload.put_i32(-1),
            }
        }
        payload.to_vec()
    }

    async fn make_pg_with_age(
        script: Vec<Vec<u8>>,
    ) -> (PostgresConnection, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            let (mut server_stream, _) = listener.accept().await.unwrap();
            let _ = server_stream.set_nodelay(true);
            // Startup
            let mut startup = [0u8; 512];
            let _ = server_stream.read(&mut startup).await.unwrap();
            let mut auth_ok = BytesMut::new();
            auth_ok.put_i32(0);
            let mut hello = pg_frame(b'R', &auth_ok);
            hello.extend_from_slice(&pg_frame(b'K', &[0; 8]));
            hello.extend_from_slice(&pg_frame(b'S', b"TimeZone\0UTC\0"));
            hello.extend_from_slice(&pg_frame(b'Z', b"I"));
            server_stream.write_all(&hello).await.unwrap();
            server_stream.flush().await.unwrap();

            // AGE setup: 1) LOAD 'age'; SET search_path ...; 2) resolve_agtype_oid
            for idx in 0..2 {
                let mut buf = [0u8; 1024];
                let _ = server_stream.read(&mut buf).await.unwrap();
                let mut resp = Vec::new();
                if idx == 1 {
                    resp.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("oid", 26, -1)])));
                    resp.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"16400")])));
                    resp.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));
                } else {
                    resp.extend_from_slice(&pg_frame(b'C', b"OK\0"));
                }
                resp.extend_from_slice(&pg_frame(b'Z', b"I"));
                server_stream.write_all(&resp).await.unwrap();
                server_stream.flush().await.unwrap();
            }

            for reply in script {
                let mut buf = [0u8; 4096];
                let n = server_stream.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                server_stream.write_all(&reply).await.unwrap();
                server_stream.flush().await.unwrap();
            }
        });

        let uri = ParsedUri::parse(&format!(
            "age://postgres@{addr}/voyager_graph?sslmode=disable"
        ))
        .unwrap();
        let conn = PostgresConnection::connect_with_age(&uri, true)
            .await
            .unwrap();
        (conn, handle)
    }

    // Build 3 query responses (repeated 3x for execute, execute_with_mode(Rows), execute_with_mode(Arrow)):
    // 1. Simple query with typed scalars, fixed-depth arrays, mixed-depth arrays (int[], numeric[], uuid[]), AGE agtype, and untyped
    let typmod_10_2 = ((10 << 16) | 2) + 4;
    let schema_1 = rd_payload(&[
        ("b", 16, -1),
        ("i", 20, -1),
        ("f", 701, -1),
        ("num", 1700, typmod_10_2),
        ("uid", 2950, -1),
        ("bin", 17, -1),
        ("d", 1082, -1),
        ("t", 1083, -1),
        ("ttz", 1266, -1),
        ("ts", 1114, -1),
        ("tstz", 1184, -1),
        ("iv", 1186, -1),
        ("js", 3802, -1),
        ("arr2d", 1016, -1),
        ("mix_i", 1007, -1),
        ("mix_n", 1231, -1),
        ("mix_u", 2951, -1),
        ("age_v", 16400, -1),
        ("unk", 999999, -1),
    ]);
    let r1 = dr_bytes(&[
        Some(b"t"),
        Some(b"42"),
        Some(b"3.5"),
        Some(b"12.30"),
        Some(b"12345678-1234-5678-1234-567812345678"),
        Some(br"\x01fe"),
        Some(b"2024-01-02"),
        Some(b"14:30:15"),
        Some(b"14:30:15+02"),
        Some(b"2024-01-02 03:04:05"),
        Some(b"2024-01-02 03:04:05+00"),
        Some(b"1 year 2 mons 3 days 04:05:06"),
        Some(br#"{"a":1}"#),
        Some(b"{{1,2},{3,NULL}}"),
        Some(b"{1,2}"),
        Some(b"{1.5,2}"),
        Some(b"{12345678-1234-5678-1234-567812345678}"),
        Some(br#"{"id": 1, "label": "Person", "properties": {"name": "Alice"}}::vertex"#),
        Some(b"77"),
    ]);
    let r2 = dr_bytes(&[
        Some(b"f"),
        None,
        Some(b"1.25"),
        Some(b"0.50"),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(b"{{4,5},{6,7}}"),
        Some(b"{{3,NULL},{5,6}}"),
        Some(b"{{3.25},{4}}"),
        Some(b"{{87654321-4321-8765-4321-876543218765}}"),
        Some(b"10"),
        None,
    ]);
    let mut simple_reply = Vec::new();
    simple_reply.extend_from_slice(&pg_frame(b'T', &schema_1));
    simple_reply.extend_from_slice(&pg_frame(b'D', &r1));
    simple_reply.extend_from_slice(&pg_frame(b'D', &r2));
    simple_reply.extend_from_slice(&pg_frame(b'C', b"SELECT 2\0"));
    simple_reply.extend_from_slice(&pg_frame(b'Z', b"I"));

    // 2. 0-row typed PG table
    let schema_zero = rd_payload(&[
        ("uid", 2950, -1),
        ("num", 1700, typmod_10_2),
        ("ttz", 1266, -1),
        ("arr", 1007, -1),
    ]);
    let mut zero_reply = Vec::new();
    zero_reply.extend_from_slice(&pg_frame(b'T', &schema_zero));
    zero_reply.extend_from_slice(&pg_frame(b'C', b"SELECT 0\0"));
    zero_reply.extend_from_slice(&pg_frame(b'Z', b"I"));

    // 3. Extended parameterized query
    let mut ext_reply = Vec::new();
    ext_reply.extend_from_slice(&pg_frame(b'1', &[]));
    ext_reply.extend_from_slice(&pg_frame(b'2', &[]));
    ext_reply.extend_from_slice(&pg_frame(
        b'T',
        &rd_payload(&[("x", 20, -1), ("mix", 1007, -1)]),
    ));
    ext_reply.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"99"), Some(b"{1,2}")])));
    ext_reply.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"100"), Some(b"{{3}}")])));
    ext_reply.extend_from_slice(&pg_frame(b'C', b"SELECT 2\0"));
    ext_reply.extend_from_slice(&pg_frame(b'Z', b"I"));

    let script = vec![
        simple_reply.clone(),
        simple_reply.clone(),
        simple_reply,
        zero_reply.clone(),
        zero_reply.clone(),
        zero_reply,
        ext_reply.clone(),
        ext_reply.clone(),
        ext_reply,
    ];

    let (mut conn, handle) = make_pg_with_age(script).await;

    // 1. Simple rich + mixed arrays + AGE + untyped
    let leg = conn.execute("SELECT ...", &HashMap::new()).await.unwrap();
    let rows = conn
        .execute_with_mode("SELECT ...", &HashMap::new(), ResultMode::Rows)
        .await
        .unwrap();
    let arr = conn
        .execute_with_mode("SELECT ...", &HashMap::new(), ResultMode::Arrow)
        .await
        .unwrap();

    assert!(rows.batches.is_empty());
    assert!(rows.bolt_rows.is_none());
    assert!(arr.result_rows.is_none());
    assert!(arr.bolt_rows.is_none());
    assert_eq!(rows.row_count(), 2);

    let leg_b = leg.to_record_batch().unwrap().unwrap();
    let row_b = rows.to_record_batch().unwrap().unwrap();
    let arr_b = arr.to_record_batch().unwrap().unwrap();
    assert_eq!(row_b, leg_b);
    assert_eq!(arr_b, leg_b);

    // Mixed-depth PG arrays in result_rows hold lossless ResultValue::List with Decimal / Uuid leaves
    let rrows = rows.result_rows.as_ref().unwrap();
    assert_eq!(
        rrows[0][15],
        ResultValue::List(
            vec![
                ResultValue::Decimal("1.5".into()),
                ResultValue::Decimal("2".into())
            ]
            .into_boxed_slice()
        )
    );
    assert!(
        matches!(rrows[0][16], ResultValue::List(ref items) if matches!(items[0], ResultValue::Uuid(_)))
    );
    assert_eq!(rrows[0][18], ResultValue::Int(77));

    // 2. 0-row typed PG table
    let leg0 = conn.execute("SELECT 0", &HashMap::new()).await.unwrap();
    let row0 = conn
        .execute_with_mode("SELECT 0", &HashMap::new(), ResultMode::Rows)
        .await
        .unwrap();
    let arr0 = conn
        .execute_with_mode("SELECT 0", &HashMap::new(), ResultMode::Arrow)
        .await
        .unwrap();
    assert_eq!(row0.row_count(), 0);
    assert_eq!(
        row0.to_record_batch().unwrap().unwrap(),
        leg0.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        arr0.to_record_batch().unwrap().unwrap(),
        leg0.to_record_batch().unwrap().unwrap()
    );

    // 3. Extended parameterized query
    let params = HashMap::from([("p".to_string(), ParamValue::Json(serde_json::json!(1)))]);
    let leg_e = conn.execute("SELECT $p", &params).await.unwrap();
    let row_e = conn
        .execute_with_mode("SELECT $p", &params, ResultMode::Rows)
        .await
        .unwrap();
    let arr_e = conn
        .execute_with_mode("SELECT $p", &params, ResultMode::Arrow)
        .await
        .unwrap();
    assert_eq!(
        row_e.to_record_batch().unwrap().unwrap(),
        leg_e.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        arr_e.to_record_batch().unwrap().unwrap(),
        leg_e.to_record_batch().unwrap().unwrap()
    );
    // Extended bolt_rows converted to ResultValue matches execute_rows mixed-depth array
    let leg_bolt_rows = leg_e.bolt_rows.as_ref().unwrap();
    assert_eq!(
        row_e.result_rows.as_ref().unwrap()[0][1],
        ResultValue::from(&leg_bolt_rows[0][1])
    );

    drop(conn);
    handle.await.unwrap();
}

#[tokio::test]
async fn test_pg_row_decoder_security_limits_depth_ragged_invalid_and_utf8() {
    use bytes::BufMut;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use voyager_drivers::ParsedUri;
    use voyager_drivers::engine::{AsyncConnection, ResultMode};
    use voyager_drivers::postgres::PostgresConnection;

    fn pg_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(msg_type);
        out.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn rd_one(oid: i32) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_i16(1);
        payload.put_slice(b"v\0");
        payload.put_i32(0);
        payload.put_i16(0);
        payload.put_i32(oid);
        payload.put_i16(-1);
        payload.put_i32(-1);
        payload.put_i16(0);
        pg_frame(b'T', &payload)
    }

    fn dr_one(bytes: &[u8]) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_i16(1);
        payload.put_i32(bytes.len() as i32);
        payload.put_slice(bytes);
        pg_frame(b'D', &payload)
    }

    let deep_pg_arr = format!("{}1{}", "{".repeat(70), "}".repeat(70));
    let deep_age_arr = format!("{}1{}", "[".repeat(70), "]".repeat(70));

    let mut cases: Vec<(Vec<u8>, &str)> = vec![
        (
            [
                rd_one(1007),
                dr_one(deep_pg_arr.as_bytes()),
                pg_frame(b'C', b"SELECT 1\0"),
                pg_frame(b'Z', b"I"),
            ]
            .concat(),
            "depth 65 PG array",
        ),
        (
            [
                rd_one(16400),
                dr_one(deep_age_arr.as_bytes()),
                pg_frame(b'C', b"SELECT 1\0"),
                pg_frame(b'Z', b"I"),
            ]
            .concat(),
            "depth 65 AGE agtype",
        ),
        (
            [
                rd_one(1007),
                dr_one(b"{{1,2},{3}}"),
                pg_frame(b'C', b"SELECT 1\0"),
                pg_frame(b'Z', b"I"),
            ]
            .concat(),
            "ragged PG array",
        ),
        (
            [
                rd_one(23),
                dr_one(b"not_an_int"),
                pg_frame(b'C', b"SELECT 1\0"),
                pg_frame(b'Z', b"I"),
            ]
            .concat(),
            "invalid integer scalar",
        ),
        (
            [
                rd_one(16),
                dr_one(b"maybe"),
                pg_frame(b'C', b"SELECT 1\0"),
                pg_frame(b'Z', b"I"),
            ]
            .concat(),
            "invalid bool scalar",
        ),
        (
            [
                rd_one(23),
                dr_one(&[0xFF, 0xFE]),
                pg_frame(b'C', b"SELECT 1\0"),
                pg_frame(b'Z', b"I"),
            ]
            .concat(),
            "invalid UTF-8 in typed int",
        ),
    ];
    // Add invalid UTF-8 in text column (must succeed with lossy replacement, never panic)
    let valid_text_lossy = [
        rd_one(25),
        dr_one(&[0x61, 0xFF, 0x62]),
        pg_frame(b'C', b"SELECT 1\0"),
        pg_frame(b'Z', b"I"),
    ]
    .concat();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let replies: Vec<Vec<u8>> = cases
        .drain(..)
        .map(|(r, _)| r)
        .chain(std::iter::once(valid_text_lossy))
        .collect();
    let server = tokio::spawn(async move {
        let (mut server_stream, _) = listener.accept().await.unwrap();
        let _ = server_stream.set_nodelay(true);
        let mut startup = [0u8; 512];
        let _ = server_stream.read(&mut startup).await.unwrap();
        let mut auth_ok = BytesMut::new();
        auth_ok.put_i32(0);
        let mut hello = pg_frame(b'R', &auth_ok);
        hello.extend_from_slice(&pg_frame(b'K', &[0; 8]));
        hello.extend_from_slice(&pg_frame(b'Z', b"I"));
        server_stream.write_all(&hello).await.unwrap();
        server_stream.flush().await.unwrap();

        for idx in 0..2 {
            let mut buf = [0u8; 1024];
            let _ = server_stream.read(&mut buf).await.unwrap();
            let mut resp = Vec::new();
            if idx == 1 {
                resp.extend_from_slice(&rd_one(26));
                resp.extend_from_slice(&dr_one(b"16400"));
                resp.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));
            } else {
                resp.extend_from_slice(&pg_frame(b'C', b"OK\0"));
            }
            resp.extend_from_slice(&pg_frame(b'Z', b"I"));
            server_stream.write_all(&resp).await.unwrap();
            server_stream.flush().await.unwrap();
        }
        for reply in replies {
            let mut buf = [0u8; 1024];
            let _ = server_stream.read(&mut buf).await.unwrap();
            server_stream.write_all(&reply).await.unwrap();
            server_stream.flush().await.unwrap();
        }
    });

    let uri = ParsedUri::parse(&format!(
        "age://postgres@{addr}/voyager_graph?sslmode=disable"
    ))
    .unwrap();
    let mut conn = PostgresConnection::connect_with_age(&uri, true)
        .await
        .unwrap();

    for i in 0..6 {
        let err = conn
            .execute_with_mode("SELECT v", &HashMap::new(), ResultMode::Rows)
            .await
            .expect_err(&format!("case {i} must return ProtocolError"));
        assert!(matches!(err, voyager_drivers::NetError::ProtocolError(_)));
    }
    let ok = conn
        .execute_with_mode("SELECT v", &HashMap::new(), ResultMode::Rows)
        .await
        .expect("lossy UTF-8 in text column succeeds safely");
    assert_eq!(ok.row_count(), 1);

    drop(conn);
    server.await.unwrap();
}

#[test]
fn test_pg_backend_message_decodes_portal_suspended() {
    let msg = BackendMessage::decode(b's', bytes::Bytes::new()).expect("PortalSuspended");
    assert_eq!(msg, BackendMessage::PortalSuspended);
}

#[tokio::test]
async fn test_pg_stream_portal_suspended_early_close_errors_and_hints() {
    use bytes::BufMut;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::postgres::PostgresConnection;
    use voyager_drivers::value::result::ResultValue;
    use voyager_drivers::{
        AsyncConnection, ConnectionFactory, ConnectionPool, NativeClient, NetError, ParsedUri,
        StreamOptions,
    };

    let _ = std::any::type_name::<PoolConfig>();

    fn pg_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(msg_type);
        out.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn rd_payload(fields: &[(&str, i32, i32)]) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_i16(fields.len() as i16);
        for (name, oid, typmod) in fields {
            payload.put_slice(name.as_bytes());
            payload.put_u8(0);
            payload.put_i32(0);
            payload.put_i16(0);
            payload.put_i32(*oid);
            payload.put_i16(-1);
            payload.put_i32(*typmod);
            payload.put_i16(0);
        }
        payload.to_vec()
    }

    fn dr_bytes(values: &[Option<&[u8]>]) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_i16(values.len() as i16);
        for val in values {
            match val {
                Some(b) => {
                    payload.put_i32(b.len() as i32);
                    payload.put_slice(b);
                }
                None => payload.put_i32(-1),
            }
        }
        payload.to_vec()
    }

    fn error_frame(code: &str, message: &str) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.extend_from_slice(b"SERROR\0");
        payload.put_u8(b'C');
        payload.extend_from_slice(code.as_bytes());
        payload.put_u8(0);
        payload.put_u8(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.put_u8(0);
        payload.put_u8(0);
        pg_frame(b'E', &payload)
    }

    fn read_frontend_tags(buf: &[u8]) -> Vec<(u8, Vec<u8>)> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos + 5 <= buf.len() {
            let tag = buf[pos];
            let len = i32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]])
                as usize;
            if pos + 1 + len > buf.len() {
                break;
            }
            out.push((tag, buf[pos + 5..pos + 1 + len].to_vec()));
            pos += 1 + len;
        }
        out
    }

    type RecordedFrontendRequests = Arc<parking_lot::Mutex<Vec<Vec<(u8, Vec<u8>)>>>>;

    // 1. Multi-chunk PortalSuspended + unconstrained NUMERIC + AGE + early close + close-before-chunk-1 + 42601 + mid-stream error + 26000 retry
    let recorded_requests: RecordedFrontendRequests = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let req_log = recorded_requests.clone();

    let typmod_10_2 = ((10 << 16) | 2) + 4;
    let schema = rd_payload(&[
        ("id", 23, -1),
        ("num_uncon", 1700, -1),
        ("num_fixed", 1700, typmod_10_2),
        ("age_v", 16400, -1),
    ]);

    let mut s1_chunk1 = Vec::new();
    s1_chunk1.extend_from_slice(&pg_frame(b'1', &[])); // ParseComplete
    s1_chunk1.extend_from_slice(&pg_frame(b'2', &[])); // BindComplete
    s1_chunk1.extend_from_slice(&pg_frame(b'T', &schema));
    s1_chunk1.extend_from_slice(&pg_frame(
        b'D',
        &dr_bytes(&[
            Some(b"1"),
            Some(b"1.5"),
            Some(b"10.20"),
            Some(br#"{"id":1,"label":"Person","properties":{"name":"Ada"}}::vertex"#),
        ]),
    ));
    s1_chunk1.extend_from_slice(&pg_frame(
        b'D',
        &dr_bytes(&[Some(b"2"), Some(b"2.0"), Some(b"20.00"), Some(b"42")]),
    ));
    s1_chunk1.extend_from_slice(&pg_frame(b's', &[])); // PortalSuspended

    let mut s1_chunk2 = Vec::new();
    s1_chunk2.extend_from_slice(&pg_frame(
        b'D',
        &dr_bytes(&[Some(b"3"), Some(b"3.123456"), Some(b"30.50"), None]),
    ));
    s1_chunk2.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));
    // Close(P) + Sync arrives next; let's split s1_chunk2 and close_reply cleanly.

    let mut close_ok = Vec::new();
    close_ok.extend_from_slice(&pg_frame(b'3', &[])); // CloseComplete
    close_ok.extend_from_slice(&pg_frame(b'Z', b"I")); // ReadyForQuery

    // Stream 2 (early close after chunk 1): Bind + Describe + Execute + Flush -> chunk 1
    let mut s2_chunk1 = Vec::new();
    s2_chunk1.extend_from_slice(&pg_frame(b'1', &[]));
    s2_chunk1.extend_from_slice(&pg_frame(b'2', &[]));
    s2_chunk1.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("id", 23, -1)])));
    s2_chunk1.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"10")])));
    s2_chunk1.extend_from_slice(&pg_frame(b's', &[]));

    // Stream 3 (close before chunk 1): open returns RowDescription while Execute returns PortalSuspended
    let mut s3_open_and_inflight = Vec::new();
    s3_open_and_inflight.extend_from_slice(&pg_frame(b'1', &[]));
    s3_open_and_inflight.extend_from_slice(&pg_frame(b'2', &[]));
    s3_open_and_inflight.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("id", 23, -1)])));
    s3_open_and_inflight.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"99")])));
    s3_open_and_inflight.extend_from_slice(&pg_frame(b's', &[]));

    // Stream 4 (multi-statement 42601 on Parse, then Sync -> ReadyForQuery)
    let s4_parse_err = error_frame(
        "42601",
        "cannot insert multiple commands into a prepared statement",
    );
    let s4_sync_ok = pg_frame(b'Z', b"I");

    // Stream 5 (mid-stream ErrorResponse on chunk 2, then Sync -> ReadyForQuery)
    let mut s5_chunk1 = Vec::new();
    s5_chunk1.extend_from_slice(&pg_frame(b'1', &[]));
    s5_chunk1.extend_from_slice(&pg_frame(b'2', &[]));
    s5_chunk1.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("q", 23, -1)])));
    s5_chunk1.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"10")])));
    s5_chunk1.extend_from_slice(&pg_frame(b's', &[]));

    let mut s5_chunk2_err = Vec::new();
    s5_chunk2_err.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"5")])));
    s5_chunk2_err.extend_from_slice(&error_frame("22012", "division by zero"));
    let s5_sync_ok = pg_frame(b'Z', b"I");

    // Stream 6 (26000 stale statement retry on cached statement hit from Stream 2!)
    // First attempt sends Bind without Parse -> server sends 26000, client sends Sync -> ReadyForQuery,
    // then client retries with Parse -> Bind -> Describe -> Execute -> Flush -> CommandComplete, then Close(P) + Sync!
    let s6_26000 = error_frame(
        "26000",
        "prepared statement \"voyager_stmt_1\" does not exist",
    );
    let s6_sync_ok = pg_frame(b'Z', b"I");
    let mut s6_retry_ok = Vec::new();
    s6_retry_ok.extend_from_slice(&pg_frame(b'1', &[]));
    s6_retry_ok.extend_from_slice(&pg_frame(b'2', &[]));
    s6_retry_ok.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("id", 23, -1)])));
    s6_retry_ok.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"77")])));
    s6_retry_ok.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));

    // Follow-up simple query to confirm connection reuse
    let mut ping_ok = Vec::new();
    ping_ok.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("ok", 23, -1)])));
    ping_ok.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"1")])));
    ping_ok.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));
    ping_ok.extend_from_slice(&pg_frame(b'Z', b"I"));

    let script = vec![
        s1_chunk1,
        s1_chunk2,
        close_ok.clone(),
        s2_chunk1,
        close_ok.clone(),
        s3_open_and_inflight,
        close_ok.clone(),
        s4_parse_err,
        s4_sync_ok,
        s5_chunk1,
        s5_chunk2_err,
        s5_sync_ok,
        s6_26000,
        s6_sync_ok,
        s6_retry_ok,
        close_ok,
        ping_ok,
    ];

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut server_stream, _) = listener.accept().await.unwrap();
        let _ = server_stream.set_nodelay(true);
        let mut startup = [0u8; 512];
        let _ = server_stream.read(&mut startup).await.unwrap();
        let mut auth_ok = BytesMut::new();
        auth_ok.put_i32(0);
        let mut hello = pg_frame(b'R', &auth_ok);
        hello.extend_from_slice(&pg_frame(b'K', &[0; 8]));
        hello.extend_from_slice(&pg_frame(b'Z', b"I"));
        server_stream.write_all(&hello).await.unwrap();
        server_stream.flush().await.unwrap();

        // AGE setup (2 queries)
        for idx in 0..2 {
            let mut buf = [0u8; 1024];
            let _ = server_stream.read(&mut buf).await.unwrap();
            let mut resp = Vec::new();
            if idx == 1 {
                resp.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("oid", 26, -1)])));
                resp.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"16400")])));
                resp.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));
            } else {
                resp.extend_from_slice(&pg_frame(b'C', b"OK\0"));
            }
            resp.extend_from_slice(&pg_frame(b'Z', b"I"));
            server_stream.write_all(&resp).await.unwrap();
            server_stream.flush().await.unwrap();
        }

        for reply in script {
            let mut buf = [0u8; 4096];
            let n = server_stream.read(&mut buf).await.unwrap();
            assert!(n > 0, "unexpected client EOF before script finished");
            req_log.lock().push(read_frontend_tags(&buf[..n]));
            server_stream.write_all(&reply).await.unwrap();
            server_stream.flush().await.unwrap();
        }
    });

    struct StaticFactory(tokio::sync::Mutex<Option<PostgresConnection>>);
    #[async_trait::async_trait]
    impl ConnectionFactory<PostgresConnection> for StaticFactory {
        async fn create(&self) -> voyager_drivers::Result<PostgresConnection> {
            self.0
                .lock()
                .await
                .take()
                .ok_or_else(|| NetError::ConnectionFailed("only one conn".to_string()))
        }
    }

    let uri = ParsedUri::parse(&format!(
        "age://postgres@{addr}/voyager_graph?sslmode=disable"
    ))
    .unwrap();
    let conn = PostgresConnection::connect_with_age(&uri, true)
        .await
        .unwrap();
    let pool = ConnectionPool::new(
        PoolConfig::new().with_min_idle(0).with_max_size(1),
        Arc::new(StaticFactory(tokio::sync::Mutex::new(Some(conn)))),
    );

    // 1. Multi-chunk PortalSuspended + unconstrained NUMERIC + AGE + Close+Sync on exhaustion
    let mut lease1 = pool.acquire().await.unwrap();
    let mut cursor1 = lease1
        .open_stream(
            "SELECT id, num_uncon, num_fixed, age_v FROM t",
            &HashMap::new(),
            &StreamOptions::new(2),
        )
        .await
        .unwrap();
    assert!(cursor1.is_server_streaming());
    assert!(!cursor1.is_wire_clean());
    assert!(lease1.has_active_portal());
    assert!(!lease1.is_valid(), "suspended portal must never look valid");
    assert!(
        !lease1.is_locally_alive(),
        "suspended portal must never look locally alive"
    );

    let c1 = lease1
        .next_stream_chunk(&mut cursor1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c1.num_rows(), 2);
    assert!(!cursor1.is_wire_clean());
    assert!(lease1.has_active_portal());

    let c2 = lease1
        .next_stream_chunk(&mut cursor1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c2.num_rows(), 1);
    assert!(cursor1.is_wire_clean());
    assert!(!lease1.has_active_portal());
    assert!(lease1.is_valid());

    // Unconstrained NUMERIC produces identical Utf8 + voyager.type=decimal schema across chunks
    let b1 = c1.to_record_batch().unwrap();
    let b2 = c2.to_record_batch().unwrap();
    assert_eq!(b1.schema().field(1), b2.schema().field(1));
    assert_eq!(
        b1.schema().field(1).data_type(),
        &arrow_schema::DataType::Utf8
    );
    assert_eq!(
        b1.schema()
            .field(1)
            .metadata()
            .get("voyager.type")
            .map(String::as_str),
        Some("decimal")
    );
    assert!(
        !b1.schema()
            .field(1)
            .metadata()
            .contains_key("voyager.encoding")
    );
    assert_eq!(
        b1.schema().field(2).data_type(),
        &arrow_schema::DataType::Decimal128(38, 2)
    );
    assert_eq!(b1.schema().field(2), b2.schema().field(2));
    assert!(matches!(c1.rows[0][3], ResultValue::Node(_)));
    assert!(
        lease1
            .next_stream_chunk(&mut cursor1)
            .await
            .unwrap()
            .is_none()
    );
    drop(lease1);

    // 2. Early close after chunk 1 sends Close('P') + Sync and reuses same connection
    {
        let mut l = pool.acquire().await.unwrap();
        let mut cur = l
            .open_stream(
                "SELECT id FROM big",
                &HashMap::new(),
                &StreamOptions::new(1),
            )
            .await
            .unwrap();
        let s2c1 = l.next_stream_chunk(&mut cur).await.unwrap().unwrap();
        assert_eq!(s2c1.num_rows(), 1);
        l.close_stream(&mut cur).await.unwrap();
        assert!(cur.is_wire_clean());
        assert!(!l.has_active_portal());
    }
    assert_eq!(pool.active_count(), 0);
    assert_eq!(pool.idle_count(), 1);

    // 3. Close before chunk 1 drains in-flight Execute then sends Close('P') + Sync
    {
        let mut l = pool.acquire().await.unwrap();
        let mut cur = l
            .open_stream(
                "SELECT id FROM unread",
                &HashMap::new(),
                &StreamOptions::new(1),
            )
            .await
            .unwrap();
        l.close_stream(&mut cur).await.unwrap();
        assert!(cur.is_wire_clean());
        assert!(!l.has_active_portal());
    }
    assert_eq!(pool.idle_count(), 1);

    // 4. Multi-statement SQL (42601) returns clear hint and recovers via Sync
    {
        let mut l = pool.acquire().await.unwrap();
        let err = l
            .open_stream(
                "SELECT 1; SELECT 2;",
                &HashMap::new(),
                &StreamOptions::new(10),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("42601"), "got: {err}");
        assert!(
            err.to_string().contains("single SQL statement"),
            "got: {err}"
        );
        assert!(l.is_valid());
    }

    // 5. Mid-stream ErrorResponse drains Sync -> ReadyForQuery and reuses same connection
    {
        let mut l = pool.acquire().await.unwrap();
        let mut cur = l
            .open_stream("SELECT 10 / x", &HashMap::new(), &StreamOptions::new(1))
            .await
            .unwrap();
        assert_eq!(
            l.next_stream_chunk(&mut cur)
                .await
                .unwrap()
                .unwrap()
                .num_rows(),
            1
        );
        let mid_err = l.next_stream_chunk(&mut cur).await.unwrap_err();
        assert!(
            mid_err.to_string().contains("division by zero"),
            "got: {mid_err}"
        );
        assert!(cur.is_wire_clean());
        assert!(l.is_valid());
    }
    assert_eq!(pool.idle_count(), 1);

    // 6. Stale cached statement (26000) retries once on idle open_stream using cached query from #2
    {
        let mut l = pool.acquire().await.unwrap();
        let mut cur = l
            .open_stream(
                "SELECT id FROM big",
                &HashMap::new(),
                &StreamOptions::new(1),
            )
            .await
            .unwrap();
        let s6c1 = l.next_stream_chunk(&mut cur).await.unwrap().unwrap();
        assert_eq!(s6c1.rows[0][0], ResultValue::Int(77));
        assert!(l.next_stream_chunk(&mut cur).await.unwrap().is_none());
    }

    // 7. Follow-up simple query on the exact same pooled connection succeeds
    {
        let mut l = pool.acquire().await.unwrap();
        let ok = l.execute("SELECT 1 AS ok", &HashMap::new()).await.unwrap();
        assert_eq!(ok.row_count(), 1);
    }
    assert_eq!(pool.metrics().connections_created, 1);

    // Verify wire protocol tags for open_stream (Parse+Bind+Describe+Execute+Flush, NO Sync!)
    // and Close('P') + Sync on exhaustion/close
    {
        let reqs = recorded_requests.lock();
        let s1_open_tags: Vec<u8> = reqs[0].iter().map(|(t, _)| *t).collect();
        assert_eq!(s1_open_tags, vec![b'P', b'B', b'D', b'E', b'H']);
        assert!(!s1_open_tags.contains(&b'S'));
        let s1_next_tags: Vec<u8> = reqs[1].iter().map(|(t, _)| *t).collect();
        assert_eq!(s1_next_tags, vec![b'E', b'H']);
        let s1_close_tags: Vec<u8> = reqs[2].iter().map(|(t, _)| *t).collect();
        assert_eq!(s1_close_tags, vec![b'C', b'S']);
        assert_eq!(reqs[2][0].1[0], b'P'); // Close target_type = 'P' (portal)

        // Stream 6 first attempt skipped Parse (hit cache), then retried with Parse after Sync
        let s6_hit_tags: Vec<u8> = reqs[12].iter().map(|(t, _)| *t).collect();
        assert_eq!(s6_hit_tags, vec![b'B', b'D', b'E', b'H']);
        let s6_sync_tags: Vec<u8> = reqs[13].iter().map(|(t, _)| *t).collect();
        assert_eq!(s6_sync_tags, vec![b'S']);
        let s6_retry_tags: Vec<u8> = reqs[14].iter().map(|(t, _)| *t).collect();
        assert_eq!(s6_retry_tags, vec![b'P', b'B', b'D', b'E', b'H']);
    }

    pool.close().await;
    server.await.unwrap();

    // 8. Drop marks broken without blocking I/O AND prepared_statement_cache_size=0 uses unnamed ""
    let accepted = Arc::new(AtomicUsize::new(0));
    let accepted_clone = accepted.clone();
    let parse_names: Arc<parking_lot::Mutex<Vec<String>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let parse_names_clone = parse_names.clone();

    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let server2 = tokio::spawn(async move {
        for conn_idx in 0..2 {
            let (mut s, _) = listener2.accept().await.unwrap();
            accepted_clone.fetch_add(1, Ordering::SeqCst);
            let mut startup = [0u8; 512];
            let _ = s.read(&mut startup).await.unwrap();
            let mut auth_ok = BytesMut::new();
            auth_ok.put_i32(0);
            let mut hello = pg_frame(b'R', &auth_ok);
            hello.extend_from_slice(&pg_frame(b'K', &[0; 8]));
            hello.extend_from_slice(&pg_frame(b'Z', b"I"));
            s.write_all(&hello).await.unwrap();
            s.flush().await.unwrap();

            let mut buf = [0u8; 4096];
            let n = s.read(&mut buf).await.unwrap();
            for (tag, payload) in read_frontend_tags(&buf[..n]) {
                if tag == b'P' {
                    let nul = payload.iter().position(|&b| b == 0).unwrap();
                    parse_names_clone
                        .lock()
                        .push(String::from_utf8_lossy(&payload[..nul]).into_owned());
                }
            }

            if conn_idx == 0 {
                let mut open_resp = Vec::new();
                open_resp.extend_from_slice(&pg_frame(b'1', &[]));
                open_resp.extend_from_slice(&pg_frame(b'2', &[]));
                open_resp.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("id", 23, -1)])));
                open_resp.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"1")])));
                open_resp.extend_from_slice(&pg_frame(b's', &[]));
                s.write_all(&open_resp).await.unwrap();
                s.flush().await.unwrap();
                // Client drops ResultStream without Close/Sync -> server observes EOF cleanly.
                let mut eof_buf = [0u8; 64];
                let eof_n = s.read(&mut eof_buf).await.unwrap();
                assert_eq!(
                    eof_n, 0,
                    "Drop must close socket without sending wire bytes"
                );
            } else {
                let mut resp = Vec::new();
                resp.extend_from_slice(&pg_frame(b'T', &rd_payload(&[("ok", 23, -1)])));
                resp.extend_from_slice(&pg_frame(b'D', &dr_bytes(&[Some(b"2")])));
                resp.extend_from_slice(&pg_frame(b'C', b"SELECT 1\0"));
                resp.extend_from_slice(&pg_frame(b'Z', b"I"));
                s.write_all(&resp).await.unwrap();
                s.flush().await.unwrap();
            }
        }
    });

    let uri2 = format!(
        "postgresql://postgres@{addr2}/postgres?sslmode=disable&prepared_statement_cache_size=0"
    );
    let client2 = NativeClient::new(
        &uri2,
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();
    let mut dropped_stream = client2
        .stream("SELECT id FROM t", &HashMap::new(), StreamOptions::new(1))
        .await
        .unwrap();
    assert_eq!(
        dropped_stream
            .next_chunk()
            .await
            .unwrap()
            .unwrap()
            .num_rows(),
        1
    );
    drop(dropped_stream); // Synchronous Drop without close() -> marks broken & drops socket
    assert_eq!(client2.pool().total_count(), 0);

    // Next query opens replacement connection transparently
    let res2 = client2
        .execute_rows("SELECT 2 AS ok", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(res2.row_count(), 1);

    client2.close().await;
    server2.await.unwrap();
    assert_eq!(accepted.load(Ordering::SeqCst), 2);
    assert_eq!(parse_names.lock().as_slice(), &[""]);
}

#[tokio::test]
async fn test_pg_stream_tx_failfast_0a000_nodata_and_cancel() {
    use bytes::BufMut;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, NetError, ParamValue, StreamOptions};

    fn pg_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(msg_type);
        out.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn rd_one(name: &str, oid: i32) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_i16(1);
        payload.put_slice(name.as_bytes());
        payload.put_u8(0);
        payload.put_i32(0);
        payload.put_i16(0);
        payload.put_i32(oid);
        payload.put_i16(-1);
        payload.put_i32(-1);
        payload.put_i16(0);
        pg_frame(b'T', &payload)
    }

    fn dr_one(bytes: &[u8]) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.put_i16(1);
        payload.put_i32(bytes.len() as i32);
        payload.put_slice(bytes);
        pg_frame(b'D', &payload)
    }

    fn error_frame(code: &str, message: &str) -> Vec<u8> {
        let mut payload = BytesMut::new();
        payload.extend_from_slice(b"SERROR\0");
        payload.put_u8(b'C');
        payload.extend_from_slice(code.as_bytes());
        payload.put_u8(0);
        payload.put_u8(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.put_u8(0);
        payload.put_u8(0);
        pg_frame(b'E', &payload)
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        // Conn 1: 0A000 retry on idle, NoData DML, Tx1 (stream + early close + follow-up + COMMIT),
        // Tx2 (mid-stream ErrorResponse -> ReadyForQuery('E') + ROLLBACK).
        let (mut s1, _) = listener.accept().await.unwrap();
        let mut startup = [0u8; 512];
        let _ = s1.read(&mut startup).await.unwrap();
        let mut auth_ok = BytesMut::new();
        auth_ok.put_i32(0);
        let mut hello = pg_frame(b'R', &auth_ok);
        hello.extend_from_slice(&pg_frame(b'K', &[0; 8]));
        hello.extend_from_slice(&pg_frame(b'Z', b"I"));
        s1.write_all(&hello).await.unwrap();
        s1.flush().await.unwrap();

        let script: Vec<Vec<u8>> = vec![
            // 1. Prime cache for "SELECT id FROM tbl WHERE id >= $1"
            [
                pg_frame(b'1', &[]),
                pg_frame(b'2', &[]),
                rd_one("id", 23),
                dr_one(b"1"),
                pg_frame(b'C', b"SELECT 1\0"),
                pg_frame(b'Z', b"I"),
            ]
            .concat(),
            // 2. open_stream cache hit -> 0A000 -> Sync -> retry with Close(S)+Parse+Bind+Describe+Execute+Flush -> Close(P)+Sync
            error_frame("0A000", "cached plan must not change result type"),
            pg_frame(b'Z', b"I"),
            [
                pg_frame(b'3', &[]), // CloseComplete for stale statement
                pg_frame(b'1', &[]),
                pg_frame(b'2', &[]),
                rd_one("id", 23),
                dr_one(b"2"),
                pg_frame(b'C', b"SELECT 1\0"),
            ]
            .concat(),
            [pg_frame(b'3', &[]), pg_frame(b'Z', b"I")].concat(),
            // 3. NoData DML stream ("INSERT INTO t VALUES (1)")
            [
                pg_frame(b'1', &[]),
                pg_frame(b'2', &[]),
                pg_frame(b'n', &[]), // NoData
                pg_frame(b'C', b"INSERT 0 3\0"),
            ]
            .concat(),
            [pg_frame(b'3', &[]), pg_frame(b'Z', b"I")].concat(),
            // 4. Tx1: BEGIN; -> stream -> early close -> follow-up SELECT -> COMMIT;
            [pg_frame(b'C', b"BEGIN\0"), pg_frame(b'Z', b"T")].concat(),
            [
                pg_frame(b'1', &[]),
                pg_frame(b'2', &[]),
                rd_one("x", 23),
                dr_one(b"10"),
                pg_frame(b's', &[]),
            ]
            .concat(),
            [pg_frame(b'3', &[]), pg_frame(b'Z', b"T")].concat(),
            [
                rd_one("ok", 23),
                dr_one(b"99"),
                pg_frame(b'C', b"SELECT 1\0"),
                pg_frame(b'Z', b"T"),
            ]
            .concat(),
            [pg_frame(b'C', b"COMMIT\0"), pg_frame(b'Z', b"I")].concat(),
            // 5. Tx2: BEGIN; -> stream chunk 1 ok -> chunk 2 ErrorResponse -> Sync -> ReadyForQuery('E') -> ROLLBACK;
            [pg_frame(b'C', b"BEGIN\0"), pg_frame(b'Z', b"T")].concat(),
            [
                pg_frame(b'1', &[]),
                pg_frame(b'2', &[]),
                rd_one("x", 23),
                dr_one(b"1"),
                pg_frame(b's', &[]),
            ]
            .concat(),
            error_frame("22012", "division by zero"),
            pg_frame(b'Z', b"E"),
            [pg_frame(b'C', b"ROLLBACK\0"), pg_frame(b'Z', b"I")].concat(),
        ];

        for reply in script {
            let mut buf = [0u8; 4096];
            let n = s1.read(&mut buf).await.unwrap();
            assert!(n > 0);
            s1.write_all(&reply).await.unwrap();
            s1.flush().await.unwrap();
        }

        // Conn 2: mid-flight timeout cancel on chunk 2 stalls forever, forcing replacement
        let (mut s2, _) = listener.accept().await.unwrap();
        let _ = s2.read(&mut startup).await.unwrap();
        s2.write_all(&hello).await.unwrap();
        s2.flush().await.unwrap();

        let mut buf = [0u8; 4096];
        let _ = s2.read(&mut buf).await.unwrap();
        let chunk1 = [
            pg_frame(b'1', &[]),
            pg_frame(b'2', &[]),
            rd_one("x", 23),
            dr_one(b"1"),
            pg_frame(b's', &[]),
        ]
        .concat();
        s2.write_all(&chunk1).await.unwrap();
        s2.flush().await.unwrap();

        // Read Execute for chunk 2 and stall until client drops socket on timeout
        let _ = s2.read(&mut buf).await.unwrap();
        let mut eof_buf = [0u8; 16];
        let _ = s2.read(&mut eof_buf).await;
    });

    let uri = format!("postgresql://postgres@{addr}/postgres?sslmode=disable");
    let client = NativeClient::new(
        &uri,
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();

    let p = HashMap::from([("v".to_string(), ParamValue::Json(serde_json::json!(1)))]);
    let _ = client
        .execute_rows("SELECT id FROM tbl WHERE id >= $v", &p)
        .await
        .unwrap();

    // 2. 0A000 retry on idle open_stream
    let mut s_0a = client
        .stream(
            "SELECT id FROM tbl WHERE id >= $v",
            &p,
            StreamOptions::new(10),
        )
        .await
        .unwrap();
    assert_eq!(s_0a.next_chunk().await.unwrap().unwrap().num_rows(), 1);
    assert!(s_0a.next_chunk().await.unwrap().is_none());

    // 3. NoData DML stream accumulates summary
    let mut s_dml = client
        .stream(
            "INSERT INTO t VALUES (1)",
            &HashMap::new(),
            StreamOptions::new(10),
        )
        .await
        .unwrap();
    assert!(s_dml.columns().is_empty());
    assert!(s_dml.next_chunk().await.unwrap().is_none());
    assert_eq!(s_dml.summary().nodes_created, 3);

    // 4. Tx1: fail-fast guard, early close, follow-up statement, COMMIT
    let mut tx1 = client.begin_transaction().await.unwrap();
    let mut cur1 = tx1
        .open_stream_cursor("SELECT x FROM t", &HashMap::new(), &StreamOptions::new(1))
        .await
        .unwrap();
    assert!(matches!(
        tx1.execute_rows("SELECT 1", &HashMap::new()).await,
        Err(NetError::TransactionError(ref m)) if m.contains("active stream")
    ));
    assert!(matches!(
        tx1.commit().await,
        Err(NetError::TransactionError(ref m)) if m.contains("active stream")
    ));
    assert_eq!(
        tx1.next_stream_chunk(&mut cur1)
            .await
            .unwrap()
            .unwrap()
            .num_rows(),
        1
    );
    tx1.close_stream_cursor(&mut cur1).await.unwrap();
    let follow = tx1
        .execute_rows("SELECT 99 AS ok", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(follow.row_count(), 1);
    tx1.commit().await.unwrap();
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 5. Tx2: mid-stream ErrorResponse inside tx transitions to Failed, rollback reuses conn
    let mut tx2 = client.begin_transaction().await.unwrap();
    let mut cur2 = tx2
        .open_stream_cursor("SELECT x FROM t", &HashMap::new(), &StreamOptions::new(1))
        .await
        .unwrap();
    assert_eq!(
        tx2.next_stream_chunk(&mut cur2)
            .await
            .unwrap()
            .unwrap()
            .num_rows(),
        1
    );
    let tx_err = tx2.next_stream_chunk(&mut cur2).await.unwrap_err();
    assert!(tx_err.to_string().contains("division by zero"));
    assert!(!tx2.has_open_stream());
    tx2.rollback().await.unwrap();
    assert_eq!(client.pool().metrics().connections_created, 1);

    // 6. Mid-flight tokio::time::timeout cancel marks cursor un-reusable and replaces socket
    client.close().await;
    let client2 = NativeClient::new(
        &uri,
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();
    let mut slow = client2
        .stream("SELECT x FROM slow", &HashMap::new(), StreamOptions::new(1))
        .await
        .unwrap();
    assert_eq!(slow.next_chunk().await.unwrap().unwrap().num_rows(), 1);
    let cancelled = tokio::time::timeout(Duration::from_millis(20), slow.next_chunk()).await;
    assert!(cancelled.is_err());
    let close_after_cancel = slow.close().await.unwrap_err();
    assert!(matches!(close_after_cancel, NetError::Closed(_)));
    assert_eq!(client2.pool().total_count(), 0);

    client2.close().await;
    server.await.unwrap();
}
