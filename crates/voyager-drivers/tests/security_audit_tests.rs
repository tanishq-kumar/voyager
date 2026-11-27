use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::io::Cursor;
use voyager_drivers::bolt::packstream::{BoltValue, PackStream};
use voyager_drivers::bolt::stream::{MAX_BOLT_MESSAGE_SIZE, read_message_frame_buffered};
use voyager_drivers::client::connect_any;
use voyager_drivers::config::{ConnectionConfig, TlsMode};
use voyager_drivers::error::NetError;
use voyager_drivers::postgres::prepare_postgres_statement;
use voyager_drivers::redis::resp::RespValue;
use voyager_drivers::redis::transaction::format_cypher_with_params;
use voyager_drivers::uri::{DatabaseProtocol, ParsedUri};
use voyager_ogm::ErrorCode;

#[test]
fn test_falkordb_backslash_quote_injection_defense() {
    let mut params = HashMap::new();
    // Attacker supplies a string ending with a backslash before a quote breakout payload
    params.insert(
        "p".to_string(),
        serde_json::Value::String(r#"malicious\" OR 1=1 --"#.to_string()).into(),
    );

    let formatted = format_cypher_with_params("MATCH (n) RETURN n", &params).unwrap();
    println!("FalkorDB secured query: {}", formatted);

    // Backslashes MUST be escaped before quotes, ensuring Cypher sees an escaped backslash
    // followed by an escaped quote, completely preventing string literal termination breakout.
    assert!(
        formatted.contains(r#"p="malicious\\\" OR 1=1 --""#),
        "Expected properly escaped backslash and quote in formatted query: {}",
        formatted
    );
}

#[test]
fn test_packstream_list32_and_map32_allocation_bomb_rejected() {
    // PackStream marker 0xD6 (List32) with length 0xFFFFFFFF (4.29 billion elements) followed by 0 bytes.
    let mut list_payload = Bytes::from_static(&[0xD6, 0xFF, 0xFF, 0xFF, 0xFF]);
    let list_res = PackStream::decode(&mut list_payload);
    match list_res {
        Err(err) => {
            assert_eq!(err.code(), ErrorCode::ClaimedSizeExceedsBuffer);
            assert!(
                err.to_string().contains("exceeds available buffer bytes"),
                "Expected buffer bounds rejection message shape, got: {}",
                err
            );
        }
        Ok(v) => panic!("Expected ProtocolError on List32 bomb, got Ok: {:?}", v),
    }

    // PackStream marker 0xDA (Map32) with length 0xFFFFFFFF followed by 0 bytes.
    let mut map_payload = Bytes::from_static(&[0xDA, 0xFF, 0xFF, 0xFF, 0xFF]);
    let map_res = PackStream::decode(&mut map_payload);
    match map_res {
        Err(err) => {
            assert_eq!(err.code(), ErrorCode::ClaimedSizeExceedsBuffer);
            assert!(
                err.to_string().contains("exceeds available buffer bytes"),
                "Expected buffer bounds rejection message shape, got: {}",
                err
            );
        }
        Ok(v) => panic!("Expected ProtocolError on Map32 bomb, got Ok: {:?}", v),
    }
}

#[test]
fn test_packstream_recursion_depth_limit_rejected() {
    // Construct an adversarial payload with 70 levels of nested single-element TinyLists:
    // 0x91 is TinyList of length 1.
    let mut payload = vec![0x91; 70];
    payload.push(0x01); // Innermost TinyInt(1)

    let mut bytes = Bytes::from(payload);
    let res = PackStream::decode(&mut bytes);
    match res {
        Err(NetError::ProtocolError(msg)) => {
            assert!(
                msg.contains("exceeded maximum nesting depth of 64"),
                "Expected recursion depth rejection, got: {}",
                msg
            );
        }
        Ok(v) => panic!(
            "Expected ProtocolError on nested list depth bomb, got Ok: {:?}",
            v
        ),
        Err(e) => panic!(
            "Expected ProtocolError on nested list depth bomb, got: {:?}",
            e
        ),
    }

    // Verify that a deeply nested payload within safe limits (e.g. 30 levels) decodes properly
    let mut safe_payload = vec![0x91; 30];
    safe_payload.push(0x2A); // TinyInt(42)

    let mut safe_bytes = Bytes::from(safe_payload);
    let safe_res = PackStream::decode(&mut safe_bytes);
    assert!(
        safe_res.is_ok(),
        "Payload within depth limit should decode successfully"
    );
}

#[test]
fn test_packstream_map_recursion_depth_limit_rejected() {
    // Construct an adversarial payload with 70 levels of nested single-entry TinyMaps:
    // 0xA1 is TinyMap of size 1; 0x81, b'k' is key "k".
    let mut payload = Vec::with_capacity(70 * 3 + 1);
    for _ in 0..70 {
        payload.push(0xA1); // TinyMap(1)
        payload.push(0x81); // TinyString(1)
        payload.push(b'k');
    }
    payload.push(0x01); // Innermost TinyInt(1)

    let mut bytes = Bytes::from(payload);
    let res = PackStream::decode(&mut bytes);
    match res {
        Err(NetError::ProtocolError(msg)) => {
            assert!(
                msg.contains("exceeded maximum nesting depth of 64"),
                "Expected recursion depth rejection, got: {}",
                msg
            );
        }
        Ok(v) => panic!(
            "Expected ProtocolError on nested map depth bomb, got Ok: {:?}",
            v
        ),
        Err(e) => panic!(
            "Expected ProtocolError on nested map depth bomb, got: {:?}",
            e
        ),
    }
}

#[test]
fn test_packstream_structure_recursion_depth_limit_rejected() {
    // Construct an adversarial payload with 70 levels of nested single-field TinyStructs:
    // 0xB1 is TinyStruct with 1 field; 0x72 is tag.
    let mut payload = Vec::with_capacity(70 * 2 + 1);
    for _ in 0..70 {
        payload.push(0xB1); // TinyStruct(1)
        payload.push(0x72); // Tag
    }
    payload.push(0x01); // Innermost TinyInt(1)

    let mut bytes = Bytes::from(payload);
    let res = PackStream::decode(&mut bytes);
    match res {
        Err(NetError::ProtocolError(msg)) => {
            assert!(
                msg.contains("exceeded maximum nesting depth of 64"),
                "Expected recursion depth rejection, got: {}",
                msg
            );
        }
        Ok(v) => panic!(
            "Expected ProtocolError on nested struct depth bomb, got Ok: {:?}",
            v
        ),
        Err(e) => panic!(
            "Expected ProtocolError on nested struct depth bomb, got: {:?}",
            e
        ),
    }
}

#[test]
fn test_packstream_map_incomplete_minimum_entry_bytes_rejected() {
    // Map32 (0xDA) claiming size 100, which requires at least 200 bytes (minimum 2 bytes per key-value entry).
    // Buffer only provides 150 bytes.
    // Under old check: size (100) > buf.remaining() (150) was FALSE, passing invalid state into parser.
    // Under new check: min_required (200) > buf.remaining() (150) is TRUE, properly rejecting upfront.
    let mut payload = vec![0xDA, 0x00, 0x00, 0x00, 0x64]; // Map32 with size = 100
    payload.extend(vec![0xC0; 150]); // 150 bytes of dummy payload

    let mut bytes = Bytes::from(payload);
    let res = PackStream::decode(&mut bytes);
    match res {
        Err(NetError::ProtocolError(msg)) => {
            assert!(
                msg.contains(
                    "requires at least 200 bytes, which exceeds available buffer bytes 150"
                ),
                "Expected 2-byte minimum entry rejection, got: {}",
                msg
            );
        }
        Ok(v) => panic!(
            "Expected ProtocolError on incomplete map entries, got Ok: {:?}",
            v
        ),
        Err(e) => panic!(
            "Expected ProtocolError on incomplete map entries, got: {:?}",
            e
        ),
    }
}

#[test]
fn test_packstream_deep_hierarchical_document_roundtrip() {
    // Construct a valid hierarchical graph document nested 25 levels deep (within the 64 limit)
    // with alternating maps and lists, simulating deep JSON / AST trees stored in graph properties.
    let mut current = BoltValue::Integer(1337);
    for i in 0..25 {
        if i % 2 == 0 {
            let mut map = HashMap::new();
            map.insert("child".to_string(), current);
            map.insert("level".to_string(), BoltValue::Integer(i));
            current = BoltValue::Map(map);
        } else {
            current = BoltValue::List(vec![current, BoltValue::String(format!("node_{}", i))]);
        }
    }

    let mut buf = BytesMut::new();
    PackStream::encode(&current, &mut buf);

    let mut bytes = buf.freeze();
    let decoded = PackStream::decode(&mut bytes)
        .expect("25-level hierarchical document should decode cleanly");
    assert_eq!(decoded, current);
}

#[test]
fn test_resp_array_and_map_allocation_bomb_rejected() {
    // Malicious RESP array claiming 2,147,483,647 elements
    let mut buf = BytesMut::from(&b"*2147483647\r\n"[..]);
    let res = RespValue::parse(&mut buf);
    match res {
        Err(NetError::ProtocolError(msg)) => {
            assert!(
                msg.contains("exceeds maximum permitted collection size"),
                "Expected collection size rejection, got: {}",
                msg
            );
        }
        Ok(v) => panic!("Expected ProtocolError on RESP array bomb, got Ok: {:?}", v),
        Err(e) => panic!("Expected ProtocolError on RESP array bomb, got: {:?}", e),
    }

    // Malicious RESP3 map claiming 2,147,483,647 pairs
    let mut map_buf = BytesMut::from(&b"%2147483647\r\n"[..]);
    let map_res = RespValue::parse(&mut map_buf);
    match map_res {
        Err(NetError::ProtocolError(msg)) => {
            assert!(
                msg.contains("exceeds maximum permitted collection size"),
                "Expected collection size rejection, got: {}",
                msg
            );
        }
        Ok(v) => panic!("Expected ProtocolError on RESP map bomb, got Ok: {:?}", v),
        Err(e) => panic!("Expected ProtocolError on RESP map bomb, got: {:?}", e),
    }
}

#[test]
fn test_postgres_parameter_collision_and_injection_prevention() {
    let mut params = HashMap::new();
    params.insert("p1".to_string(), serde_json::json!(42).into());
    params.insert("p10".to_string(), serde_json::json!(100).into());

    let query = "SELECT * FROM users WHERE id = $p10 AND age = $p1;";
    let prepared = prepare_postgres_statement(query, &params).expect("prepare should succeed");
    println!("PostgreSQL prepared statement: {}", prepared.statement);

    // Prefix collision elimination: $p10 must NOT become $p1.
    assert_eq!(
        prepared.statement,
        "SELECT * FROM users WHERE id = $1 AND age = $2;"
    );
    assert_eq!(
        prepared.parameter_values,
        vec![Some(b"100".to_vec()), Some(b"42".to_vec())]
    );

    // Injection payloads stay in bound values, never in the SQL text.
    let mut quote_params = HashMap::new();
    quote_params.insert(
        "name".to_string(),
        serde_json::json!("O'Reilly'; DROP TABLE authors; --").into(),
    );
    let quote_query = "SELECT * FROM authors WHERE name = $name;";
    let quote_prepared =
        prepare_postgres_statement(quote_query, &quote_params).expect("prepare should succeed");
    assert_eq!(
        quote_prepared.statement,
        "SELECT * FROM authors WHERE name = $1;"
    );
    assert!(!quote_prepared.statement.contains("DROP TABLE"));
    assert_eq!(
        quote_prepared.parameter_values,
        vec![Some(b"O'Reilly'; DROP TABLE authors; --".to_vec())]
    );
}

#[tokio::test]
async fn test_bolt_stream_maximum_payload_guard() {
    assert_eq!(MAX_BOLT_MESSAGE_SIZE, 128 * 1024 * 1024);

    // Test that valid chunked frame reads correctly
    let mut valid_data = Vec::new();
    valid_data.extend_from_slice(&4u16.to_be_bytes());
    valid_data.extend_from_slice(&[1, 2, 3, 4]);
    valid_data.extend_from_slice(&0u16.to_be_bytes());
    let mut valid_cursor = Cursor::new(valid_data);
    let mut read_buf = BytesMut::new();
    let res = read_message_frame_buffered(&mut valid_cursor, &mut read_buf).await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap().as_ref(), &[1, 2, 3, 4]);
}

#[tokio::test]
async fn test_plaintext_tls_downgrade_prevention() {
    // 1. bolt+s scheme
    let uri = "bolt+s://neo4j:password@127.0.0.1:7687";
    let parsed = ParsedUri::parse(uri).expect("Parse should succeed");
    assert_eq!(parsed.protocol, DatabaseProtocol::Bolt);
    assert_eq!(parsed.tls, TlsMode::Required);

    let config = ConnectionConfig::from_uri(uri);
    assert_eq!(
        config.tls,
        TlsMode::Required,
        "ConnectionConfig::from_uri must preserve TLS mode!"
    );

    // With the `tls` feature the client starts TLS (tls_tests.rs checks that the first
    // bytes on the wire are a ClientHello / SSLRequest); without it, every TLS URI is
    // refused before connecting, naming the feature.
    for uri in [
        uri,
        "rediss://default:password@127.0.0.1:6379",
        "postgresql+s://postgres:secret@127.0.0.1:5432/mydb",
    ] {
        match connect_any(uri).await {
            Ok(_) => panic!("{uri}: nothing listens there, expected an error"),
            Err(NetError::TlsError(msg)) => {
                assert!(msg.contains("Plaintext downgrade rejected"), "{msg}");
                assert!(msg.contains("`tls` feature"), "{msg}");
            }
            // With TLS the connection attempt itself fails: nothing listens there.
            Err(_) if voyager_drivers::TLS_AVAILABLE => {}
            Err(other) => {
                panic!("{uri}: without the tls feature expected a TlsError, got {other:?}")
            }
        }
    }
}

#[tokio::test]
async fn test_pool_discards_dead_connection_on_failed_reset() {
    use std::sync::Arc;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::mock::MockConnectionFactory;
    use voyager_drivers::pool::ConnectionPool;

    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new().with_min_idle(1).with_max_size(2);
    let pool = ConnectionPool::new(config, factory.clone());

    pool.warm_up().await.expect("Failed to warm up pool");
    assert_eq!(pool.idle_count(), 1);

    // Acquire the connection, set in_transaction = true and fail_reset = true
    {
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        conn.in_transaction = true;
        conn.fail_reset = true;
        // On drop, it is returned to the pool's idle queue
    }

    // Now acquire again: the pool tries to reset the idle connection.
    // When reset() fails, the pool must discard it rather than leasing a broken connection,
    // and provision a fresh healthy connection from the factory.
    let conn2 = pool
        .acquire()
        .await
        .expect("Failed to acquire fresh connection");
    assert!(conn2.is_valid);
    assert!(!conn2.fail_reset);
    assert_eq!(factory.created_count(), 2);
}

#[test]
fn test_connection_query_timeout_defaults_and_mutation() {
    let config = ConnectionConfig::from_uri("bolt://localhost:7687");
    assert_eq!(
        config.query_timeout,
        Some(std::time::Duration::from_secs(30))
    );

    let parsed_pg = ParsedUri::parse("postgresql://localhost:5432/db?query_timeout=15").unwrap();
    assert_eq!(parsed_pg.params.get("query_timeout").unwrap(), "15");

    let parsed_redis = ParsedUri::parse("redis://localhost:6379?query_timeout=45").unwrap();
    assert_eq!(parsed_redis.params.get("query_timeout").unwrap(), "45");
}

// ============================================================================
// Wave 1 Security & Protocol Safety Regression Tests
// ============================================================================

fn pg_wire_frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(msg_type);
    out.extend_from_slice(&((4 + payload.len()) as i32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

#[test]
fn test_sec1_postgres_negative_field_count_rejected_without_panic() {
    use voyager_drivers::postgres::BackendMessage;

    // RowDescription ('T') with field_count = -1 (2-byte i16)
    let row_desc_neg1 = Bytes::from_static(&[0xFF, 0xFF]);
    let err = BackendMessage::decode(b'T', row_desc_neg1)
        .expect_err("negative RowDescription field_count must be rejected");
    assert!(
        matches!(err, NetError::ProtocolError(ref msg) if msg.contains("Negative RowDescription field_count")),
        "unexpected error: {err:?}"
    );

    // RowDescription ('T') with field_count = i16::MIN
    let row_desc_min = Bytes::copy_from_slice(&i16::MIN.to_be_bytes());
    assert!(matches!(
        BackendMessage::decode(b'T', row_desc_min),
        Err(NetError::ProtocolError(_))
    ));

    // DataRow ('D') with field_count = -1
    let data_row_neg1 = Bytes::from_static(&[0xFF, 0xFF]);
    let err = BackendMessage::decode(b'D', data_row_neg1)
        .expect_err("negative DataRow field_count must be rejected");
    assert!(
        matches!(err, NetError::ProtocolError(ref msg) if msg.contains("Negative DataRow field_count")),
        "unexpected error: {err:?}"
    );

    // Huge positive field_count on truncated payload must return ProtocolError (not OOM)
    let mut huge_row_desc = BytesMut::new();
    huge_row_desc.extend_from_slice(&i16::MAX.to_be_bytes());
    assert!(matches!(
        BackendMessage::decode(b'T', huge_row_desc.freeze()),
        Err(NetError::ProtocolError(_))
    ));
}

#[test]
fn test_sec1_postgres_frontend_encode_rejects_nul_and_overflow_counts() {
    use voyager_drivers::postgres::{FrontendMessage, StartupMessage};

    let mut buf = BytesMut::new();
    let bad_startup = StartupMessage::new("user\0name", Some("db"));
    assert!(matches!(
        bad_startup.encode(&mut buf),
        Err(NetError::ProtocolError(ref m)) if m.contains("NUL")
    ));

    let bad_db = StartupMessage::new("user", Some("db\0name"));
    assert!(matches!(
        bad_db.encode(&mut buf),
        Err(NetError::ProtocolError(ref m)) if m.contains("NUL")
    ));

    let bad_query = FrontendMessage::Query("SELECT 1;\0DROP TABLE x;".to_string());
    assert!(matches!(
        bad_query.encode(&mut buf),
        Err(NetError::ProtocolError(ref m)) if m.contains("NUL")
    ));

    let bad_sasl = FrontendMessage::SASLInitialResponse {
        mechanism: "SCRAM\0-SHA-256".to_string(),
        data: None,
    };
    assert!(matches!(
        bad_sasl.encode(&mut buf),
        Err(NetError::ProtocolError(ref m)) if m.contains("NUL")
    ));

    // > i16::MAX parameter count in Bind must be rejected instead of wrapping silently
    let too_many_params = FrontendMessage::Bind {
        portal: String::new(),
        statement: String::new(),
        param_formats: Vec::new(),
        params: vec![None; (i16::MAX as usize) + 1],
        result_formats: Vec::new(),
    };
    assert!(matches!(
        too_many_params.encode(&mut buf),
        Err(NetError::ProtocolError(ref m)) if m.contains("i16::MAX")
    ));
}

#[tokio::test]
async fn test_sec1_postgres_cleartext_password_rejects_interior_nul() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::postgres::PostgresConnection;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 512];
            let _ = stream.read(&mut buf).await;
            // AuthenticationCleartextPassword ('R', auth_type = 3)
            let _ = stream
                .write_all(&pg_wire_frame(b'R', &3i32.to_be_bytes()))
                .await;
            let _ = stream.flush().await;
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    });

    let uri =
        ParsedUri::parse(&format!("postgresql://user:pw%00evil@{addr}/db")).expect("parse uri");
    // Ensure URI has a literal NUL in password via raw ParsedUri mutation
    let mut nul_uri = uri;
    nul_uri.auth = voyager_drivers::config::Auth::Basic {
        username: "user".to_string(),
        password: "pw\0evil".to_string(),
        realm: None,
    };
    let err = match PostgresConnection::connect(&nul_uri).await {
        Ok(_) => panic!("password with interior NUL must fail"),
        Err(e) => e,
    };
    assert!(
        matches!(err, NetError::AuthenticationFailed(ref m) if m.contains("NUL")),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn test_sec2_postgres_cancellation_and_io_error_poison_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::AsyncConnection;
    use voyager_drivers::postgres::PostgresConnection;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf).await;
            let mut startup_reply = pg_wire_frame(b'R', &0i32.to_be_bytes());
            startup_reply.extend_from_slice(&pg_wire_frame(b'Z', b"I"));
            let _ = stream.write_all(&startup_reply).await;
            let _ = stream.flush().await;

            // Read query 'Q', send CommandComplete WITHOUT ReadyForQuery and stall
            let _ = stream.read(&mut buf).await;
            let _ = stream.write_all(&pg_wire_frame(b'C', b"SELECT 1\0")).await;
            let _ = stream.flush().await;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });

    let uri = ParsedUri::parse(&format!("postgresql://postgres:pw@{addr}/db")).unwrap();
    let mut conn = PostgresConnection::connect(&uri).await.unwrap();
    conn.set_query_timeout(Some(std::time::Duration::from_millis(40)));
    assert!(conn.is_valid());

    // Timeout drops execute_simple_inner mid-read before ReadyForQuery
    let err = conn
        .execute("SELECT 1;", &HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(err, NetError::Timeout(_)), "got {err:?}");
    assert!(
        !conn.is_valid(),
        "timed-out PG connection must be marked broken"
    );

    let reuse_err = conn
        .execute("SELECT 1;", &HashMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(reuse_err, NetError::Closed(_)),
        "got {reuse_err:?}"
    );
    assert!(matches!(conn.reset().await, Err(NetError::Closed(_))));
}

#[tokio::test]
async fn test_sec2_bolt_cancellation_and_failed_reset_poison_connection() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
    use voyager_drivers::AsyncConnection;
    use voyager_drivers::bolt::packstream::PackStream;
    use voyager_drivers::bolt::stream::encode_chunks;
    use voyager_drivers::bolt::{BoltConnection, BoltVersion};

    // 1. Cancellation mid-read poisons BoltConnection
    let (client, mut server) = duplex(4096);
    let mut conn = BoltConnection::from_stream(client, BoltVersion { major: 5, minor: 4 });
    assert!(conn.is_valid());

    // Spawn server task that reads RUN+PULL and stalls before answering
    tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        let _ = server.read(&mut buf).await;
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    });

    {
        let params = HashMap::new();
        let fut = conn.execute("RETURN 1", &params);
        tokio::select! {
            _ = fut => panic!("query should not finish before timeout"),
            _ = tokio::time::sleep(std::time::Duration::from_millis(25)) => {}
        }
    }
    assert!(
        !conn.is_valid(),
        "cancelled Bolt connection must be marked invalid"
    );
    assert!(matches!(
        conn.execute("RETURN 1", &HashMap::new()).await,
        Err(NetError::Closed(_))
    ));
    assert!(matches!(conn.reset().await, Err(NetError::Closed(_))));

    // 2. Swallowed reset() error keeps is_valid == false when server closes during RESET
    let (client2, mut server2) = duplex(4096);
    let mut conn2 = BoltConnection::from_stream(client2, BoltVersion { major: 5, minor: 4 });

    tokio::spawn(async move {
        let mut buf = [0u8; 4096];
        // Read RUN + PULL
        let _ = server2.read(&mut buf).await;

        // Encode Bolt FAILURE (0x7F) for RUN and IGNORED (0x7E) for PULL, then drop stream before RESET reply
        let mut failure_meta = HashMap::new();
        failure_meta.insert(
            "code".to_string(),
            BoltValue::String("Neo.ClientError.Statement.SyntaxError".to_string()),
        );
        failure_meta.insert(
            "message".to_string(),
            BoltValue::String("bad syntax".to_string()),
        );
        let mut fail_payload = BytesMut::new();
        fail_payload.extend_from_slice(&[0xB1, 0x7F]); // TinyStruct(1), Failure
        PackStream::encode(&BoltValue::Map(failure_meta), &mut fail_payload);
        let mut wire = BytesMut::new();
        encode_chunks(&fail_payload, &mut wire);

        let ignored_payload = [0xB0, 0x7E]; // TinyStruct(0), Ignored
        encode_chunks(&ignored_payload, &mut wire);
        let _ = server2.write_all(&wire).await;
        let _ = server2.flush().await;

        // Wait for RESET request, then close without answering
        let _ = server2.read(&mut buf).await;
        drop(server2);
    });

    let err = conn2
        .execute("BAD QUERY", &HashMap::new())
        .await
        .unwrap_err();
    assert!(
        !conn2.is_valid(),
        "failed RESET must keep is_valid == false (err={err:?})"
    );
}

#[tokio::test]
async fn test_sec2_redis_cancellation_poisons_connection_and_reset_fails() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::AsyncConnection;
    use voyager_drivers::redis::RedisConnection;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            // Read HELLO 3
            let _ = stream.read(&mut buf).await;
            let _ = stream.write_all(b"%1\r\n+server\r\n+redis\r\n").await;
            let _ = stream.flush().await;

            // Read command and stall without replying
            let _ = stream.read(&mut buf).await;
            tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        }
    });

    let uri = ParsedUri::parse(&format!("redis://{addr}/graph")).unwrap();
    let mut conn = RedisConnection::connect(&uri).await.unwrap();
    conn.set_query_timeout(Some(std::time::Duration::from_millis(30)));
    assert!(conn.is_valid());

    let err = conn.execute_raw(&["PING"]).await.unwrap_err();
    assert!(matches!(err, NetError::Timeout(_)), "got {err:?}");
    assert!(
        !conn.is_valid(),
        "timed-out Redis connection must be marked invalid"
    );
    assert!(matches!(conn.reset().await, Err(NetError::Closed(_))));
    assert!(matches!(
        conn.execute_raw(&["PING"]).await,
        Err(NetError::Closed(_))
    ));
}

#[tokio::test]
async fn test_sec3_scram_bounds_constant_time_and_sasl_final_enforcement() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::postgres::{PostgresConnection, ScramClient, generate_client_nonce};

    // 1. CSPRNG nonce uniqueness & length
    let n1 = generate_client_nonce();
    let n2 = generate_client_nonce();
    assert_eq!(n1.len(), 24);
    assert_ne!(n1, n2);

    // 2. Iteration bounds enforcement (4096..=1_000_000)
    let mut client = ScramClient::new();
    let first = String::from_utf8(client.client_first_message()).unwrap();
    let nonce = first.strip_prefix("n,,n=,r=").unwrap();
    let salt_b64 = "c2FsdA==";
    for bad_iters in [0u32, 1, 4095, 1_000_001] {
        let challenge = format!("r={nonce}srv,s={salt_b64},i={bad_iters}");
        let err = client
            .process_challenge(challenge.as_bytes(), "secret")
            .expect_err("out-of-range iterations must fail");
        assert!(
            matches!(err, NetError::AuthenticationFailed(_)),
            "got {err:?}"
        );
    }

    // Valid challenge + verify_server_final mismatch check
    let valid_challenge = format!("r={nonce}srv,s={salt_b64},i=4096");
    assert!(
        client
            .process_challenge(valid_challenge.as_bytes(), "secret")
            .is_ok()
    );
    let bad_final = b"v=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    assert!(matches!(
        client.verify_server_final(bad_final),
        Err(NetError::AuthenticationFailed(_))
    ));

    // 3. Server sending AuthenticationOk before SASLFinal must be rejected
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf).await;
            // Send AuthenticationSASL (auth_type = 10, mechanism = "SCRAM-SHA-256\0\0")
            let mut sasl_payload = 10i32.to_be_bytes().to_vec();
            sasl_payload.extend_from_slice(b"SCRAM-SHA-256\0\0");
            let _ = stream.write_all(&pg_wire_frame(b'R', &sasl_payload)).await;
            let _ = stream.flush().await;

            // Read SASLInitialResponse, then immediately send AuthenticationOk + ReadyForQuery (skipping SASLFinal!)
            let _ = stream.read(&mut buf).await;
            let mut bypass = pg_wire_frame(b'R', &0i32.to_be_bytes());
            bypass.extend_from_slice(&pg_wire_frame(b'Z', b"I"));
            let _ = stream.write_all(&bypass).await;
            let _ = stream.flush().await;
        }
    });

    let uri = ParsedUri::parse(&format!("postgresql://user:secret@{addr}/db")).unwrap();
    let err = match PostgresConnection::connect(&uri).await {
        Ok(_) => panic!("AuthenticationOk bypassing SASLFinal must fail"),
        Err(e) => e,
    };
    assert!(
        matches!(err, NetError::AuthenticationFailed(ref m) if m.contains("SASLFinal")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn test_sec4_sec5_resp_overflow_header_cap_and_codec_default() {
    use tokio_util::codec::Decoder;
    use voyager_drivers::redis::RespCodec;

    // SEC-4: oversized / overflowing lengths fail immediately instead of panicking or buffering
    for payload in [
        &b"$9223372036854775807\r\n"[..],
        &b"$134217729\r\n"[..],
        &b"!9223372036854775807\r\n"[..],
        &b"=9223372036854775807\r\n"[..],
    ] {
        let mut buf = BytesMut::from(payload);
        let err = RespValue::parse(&mut buf).expect_err("oversized RESP length must fail");
        assert!(matches!(err, NetError::ProtocolError(_)), "got {err:?}");
    }

    // SEC-5: header line exceeding 64 KB without CRLF fails fast
    let mut long_header = vec![b'+'];
    long_header.extend(std::iter::repeat_n(b'A', 65 * 1024));
    let mut long_buf = BytesMut::from(long_header.as_slice());
    let err = RespValue::parse(&mut long_buf).expect_err("overlong RESP header line must fail");
    assert!(
        matches!(err, NetError::ProtocolError(ref m) if m.contains("maximum length")),
        "got {err:?}"
    );

    // RespCodec::default() must allow normal frames (not max_frame_size = 0)
    let mut codec = RespCodec::default();
    let mut ok_buf = BytesMut::from(&b"+OK\r\n"[..]);
    let decoded = codec.decode(&mut ok_buf).expect("default codec decode");
    assert_eq!(decoded, Some(RespValue::SimpleString("OK".to_string())));
}

#[test]
fn test_sec6_uniform_recursion_depth_cap() {
    use voyager_drivers::ParamValue;
    use voyager_drivers::postgres::parse_agtype;

    // 1. agtype nested arrays & objects > 64 rejected; 30 succeeds
    let deep_arr = format!("{}1{}", "[".repeat(70), "]".repeat(70));
    assert!(matches!(
        parse_agtype(&deep_arr),
        Err(NetError::ProtocolError(ref m)) if m.contains("maximum nesting depth")
    ));
    let safe_arr = format!("{}1{}", "[".repeat(30), "]".repeat(30));
    assert!(parse_agtype(&safe_arr).is_ok());

    let mut deep_obj = String::from("1");
    for _ in 0..70 {
        deep_obj = format!("{{\"a\":{deep_obj}}}");
    }
    assert!(matches!(
        parse_agtype(&deep_obj),
        Err(NetError::ProtocolError(ref m)) if m.contains("maximum nesting depth")
    ));

    // 2. postgres/params nested ParamValue::List & Json > 64 rejected
    let mut deep_param = ParamValue::from(serde_json::json!(1));
    for _ in 0..70 {
        deep_param = ParamValue::List(vec![deep_param]);
    }
    let mut pg_params = HashMap::new();
    pg_params.insert("p".to_string(), deep_param.clone());
    assert!(matches!(
        prepare_postgres_statement("SELECT $p", &pg_params),
        Err(NetError::ProtocolError(ref m)) if m.contains("maximum nesting depth")
    ));

    // 3. redis/transaction nested ParamValue::List > 64 rejected
    assert!(matches!(
        format_cypher_with_params("RETURN $p", &pg_params),
        Err(NetError::ProtocolError(ref m)) if m.contains("maximum nesting depth")
    ));
}

#[tokio::test]
async fn test_sec7_bolt_version_validation_and_bearer_realm_auth() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::bolt::{BoltConnection, BoltRequest};
    use voyager_drivers::config::Auth;

    // 1. Unproposed/HTTP version response must be rejected before sending credentials
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (saw_post_handshake_tx, saw_post_handshake_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut handshake = [0u8; 20];
            let _ = stream.read_exact(&mut handshake).await;
            let _ = stream.write_all(b"HTTP").await;
            let _ = stream.flush().await;
            let mut extra = [0u8; 64];
            let n = stream.read(&mut extra).await.unwrap_or(0);
            let _ = saw_post_handshake_tx.send(n);
        }
    });

    let cfg = ConnectionConfig {
        uri: format!("bolt://neo4j:secret@{addr}"),
        auth: Auth::Basic {
            username: "neo4j".to_string(),
            password: "secret".to_string(),
            realm: Some("native".to_string()),
        },
        ..ConnectionConfig::default()
    };
    let err = match BoltConnection::connect(&cfg).await {
        Ok(_) => panic!("HTTP version response must be rejected"),
        Err(e) => e,
    };
    assert!(matches!(err, NetError::ProtocolError(_)), "got {err:?}");
    assert_eq!(
        saw_post_handshake_rx.await.unwrap(),
        0,
        "client must NOT send credentials after invalid Bolt version response"
    );

    // 2. BoltRequest::logon_with_auth supports Bearer and Basic+realm
    let bearer_req = BoltRequest::logon_with_auth(&Auth::Bearer {
        token: "jwt-token-secret".to_string(),
    });
    match &bearer_req {
        BoltRequest::Logon { auth } => {
            assert_eq!(auth.get("scheme").and_then(|v| v.as_str()), Some("bearer"));
            assert_eq!(
                auth.get("credentials").and_then(|v| v.as_str()),
                Some("jwt-token-secret")
            );
        }
        other => panic!("expected Logon, got {other:?}"),
    }
    let debug_str = format!("{bearer_req:?}");
    assert!(!debug_str.contains("jwt-token-secret"));

    let realm_req = BoltRequest::logon_with_auth(&Auth::Basic {
        username: "alice".to_string(),
        password: "pw".to_string(),
        realm: Some("corp".to_string()),
    });
    match &realm_req {
        BoltRequest::Logon { auth } => {
            assert_eq!(auth.get("scheme").and_then(|v| v.as_str()), Some("basic"));
            assert_eq!(auth.get("realm").and_then(|v| v.as_str()), Some("corp"));
        }
        other => panic!("expected Logon, got {other:?}"),
    }
}

#[tokio::test]
async fn test_sec11_redis_auth_write_error_propagated() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::redis::RedisConnection;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 512];
            let _ = stream.read(&mut buf).await;
            // Reject HELLO 3 so client falls back to RESP2 AUTH, then close socket immediately
            let _ = stream.write_all(b"-ERR unknown command 'HELLO'\r\n").await;
            let _ = stream.flush().await;
            drop(stream);
        }
    });

    let uri = ParsedUri::parse(&format!("redis://user:secret@{addr}/0")).unwrap();
    let res = RedisConnection::connect(&uri).await;
    assert!(
        res.is_err(),
        "RedisConnection::connect must fail when AUTH fails or socket closes"
    );
}

#[test]
fn test_sec13_falkordb_cypher_nul_point_nan_and_param_name_validation() {
    use voyager_drivers::ParamValue;

    // 1. NUL byte inside string parameter rejected
    let mut nul_params = HashMap::new();
    nul_params.insert(
        "s".to_string(),
        ParamValue::from(serde_json::json!("abc\0def")),
    );
    assert!(format_cypher_with_params("RETURN $s", &nul_params).is_err());

    // 2. Non-finite Point2D / Point3D coordinates rejected
    let mut nan_pt = HashMap::new();
    nan_pt.insert(
        "pt".to_string(),
        ParamValue::Point2D {
            srid: 4326,
            x: f64::NAN,
            y: 1.0,
        },
    );
    assert!(format_cypher_with_params("RETURN $pt", &nan_pt).is_err());

    let mut inf_pt = HashMap::new();
    inf_pt.insert(
        "pt".to_string(),
        ParamValue::Point3D {
            srid: 4979,
            x: 1.0,
            y: 2.0,
            z: f64::INFINITY,
        },
    );
    assert!(format_cypher_with_params("RETURN $pt", &inf_pt).is_err());

    // 3. Non-plain top-level Cypher parameter names rejected
    for bad_name in ["bad key", "123abc", "a=1", "a`b", ""] {
        let mut bad_params = HashMap::new();
        bad_params.insert(bad_name.to_string(), ParamValue::from(serde_json::json!(1)));
        assert!(
            format_cypher_with_params("RETURN 1", &bad_params).is_err(),
            "top-level parameter {bad_name:?} must be rejected"
        );
    }
}

#[tokio::test]
async fn test_bug1_bolt_noop_keepalive_chunks_skipped() {
    use voyager_drivers::bolt::stream::read_message_frame;

    // Leading 0x00 0x00 NOOP keepalives followed by [0x00, 0x04, 1, 2, 3, 4, 0x00, 0x00]
    let mut data = Vec::new();
    data.extend_from_slice(&0u16.to_be_bytes());
    data.extend_from_slice(&0u16.to_be_bytes());
    data.extend_from_slice(&4u16.to_be_bytes());
    data.extend_from_slice(&[1, 2, 3, 4]);
    data.extend_from_slice(&0u16.to_be_bytes());

    let mut read_buf = BytesMut::new();
    let frame_buf = read_message_frame_buffered(&mut Cursor::new(data.clone()), &mut read_buf)
        .await
        .expect("buffered reader must skip NOOP keepalives");
    assert_eq!(frame_buf.as_ref(), &[1, 2, 3, 4]);

    let frame_unbuf = read_message_frame(&mut Cursor::new(data))
        .await
        .expect("unbuffered reader must skip NOOP keepalives");
    assert_eq!(frame_unbuf.as_ref(), &[1, 2, 3, 4]);
}

#[tokio::test]
async fn test_bug2_pool_acquire_cancellation_wakes_waiters_and_avoids_leak() {
    use async_trait::async_trait;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use voyager_drivers::AsyncConnection;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::mock::MockConnectionFactory;
    use voyager_drivers::pool::ConnectionPool;
    use voyager_drivers::{ParamValue, QueryResult, QuerySummary};

    struct SlowPingConn {
        ping_entered: Arc<tokio::sync::Notify>,
        gate: Arc<AtomicBool>,
    }

    #[async_trait]
    impl AsyncConnection for SlowPingConn {
        async fn execute(
            &mut self,
            _query: &str,
            _params: &HashMap<String, ParamValue>,
        ) -> voyager_drivers::Result<QueryResult> {
            Ok(QueryResult::empty(QuerySummary::default()))
        }

        async fn ping(&mut self) -> voyager_drivers::Result<()> {
            self.ping_entered.notify_one();
            while !self.gate.load(Ordering::SeqCst) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            Ok(())
        }

        async fn reset(&mut self) -> voyager_drivers::Result<()> {
            Ok(())
        }

        fn is_valid(&self) -> bool {
            true
        }

        fn is_in_transaction(&self) -> bool {
            false
        }

        async fn close(&mut self) -> voyager_drivers::Result<()> {
            Ok(())
        }
    }

    let ping_entered = Arc::new(tokio::sync::Notify::new());
    let gate = Arc::new(AtomicBool::new(false));
    let ping_entered_clone = ping_entered.clone();
    let gate_clone = gate.clone();

    let mut cfg = PoolConfig::new()
        .with_max_size(1)
        .with_min_idle(1)
        .with_acquire_timeout(std::time::Duration::from_millis(300));
    cfg.test_on_acquire = true;

    let pool = ConnectionPool::from_fn(cfg, move || {
        let pe = ping_entered_clone.clone();
        let g = gate_clone.clone();
        async move {
            Ok(SlowPingConn {
                ping_entered: pe,
                gate: g,
            })
        }
    });
    pool.warm_up().await.unwrap();
    assert_eq!(pool.total_count(), 1);

    // Task 1 pops the idle connection and stalls in ping(), while Task 2 queues as a waiter.
    let pool1 = pool.clone();
    let task1 = tokio::spawn(async move { pool1.acquire().await });
    ping_entered.notified().await;

    let pool2 = pool.clone();
    let task2 = tokio::spawn(async move { pool2.acquire().await });
    tokio::task::yield_now().await;

    // Cancel task 1 while it holds the permit and popped idle connection inside ping().
    task1.abort();
    let _ = task1.await;

    // Release gate for any newly created connection; task 2 must receive the returned permit and succeed,
    // and total_count must remain 1 (no leak).
    gate.store(true, Ordering::SeqCst);
    let conn2 = task2
        .await
        .unwrap()
        .expect("waiter must receive returned permit");
    assert_eq!(pool.total_count(), 1);
    drop(conn2);

    // Also keep MockConnectionFactory import verified for future pool tests
    let _ = MockConnectionFactory::new();
}
