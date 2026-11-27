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
    assert!(ConnectOptions { age: true }.age);
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
