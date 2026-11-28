//! Comprehensive unit and integration tests for Bolt wire protocol, PackStream, and stub-server.

use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;

use voyager_drivers::bolt::packstream::param_to_bolt_value;
use voyager_drivers::bolt::{
    BoltConnection, BoltDate, BoltDateTime, BoltDateTimeZoneId, BoltDuration, BoltLocalDateTime,
    BoltLocalTime, BoltNode, BoltPath, BoltPoint2D, BoltPoint3D, BoltRelationship, BoltRequest,
    BoltResponse, BoltStubServer, BoltTime, BoltUnboundRelationship, BoltValue, PackStream,
    bolt_value_to_canonical_json, build_bolt_record_batch, encode_chunks, read_message_frame, srid,
};
use voyager_drivers::config::{ConnectionConfig, PoolConfig};
use voyager_drivers::engine::{AsyncConnection, ConnectionFactory};
use voyager_drivers::params::{ParamTimezone, ParamValue};
use voyager_drivers::pool::ConnectionPool;

#[test]
fn test_packstream_scalars_roundtrip() {
    let test_values = vec![
        BoltValue::Null,
        BoltValue::Boolean(true),
        BoltValue::Boolean(false),
        BoltValue::Integer(0),
        BoltValue::Integer(42),
        BoltValue::Integer(-15),
        BoltValue::Integer(127),
        BoltValue::Integer(-128),
        BoltValue::Integer(32767),
        BoltValue::Integer(-32768),
        BoltValue::Integer(2147483647),
        BoltValue::Integer(-2147483648),
        BoltValue::Integer(9223372036854775807),
        BoltValue::Float(std::f64::consts::PI),
        BoltValue::String("hello world".to_string()),
        BoltValue::String("".to_string()),
        BoltValue::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    ];

    for val in test_values {
        let mut buf = BytesMut::new();
        PackStream::encode(&val, &mut buf);
        let mut bytes = buf.freeze();
        let decoded = PackStream::decode(&mut bytes).expect("Failed to decode PackStream value");
        assert_eq!(val, decoded);
    }
}

#[test]
fn test_packstream_containers_roundtrip() {
    // List
    let list_val = BoltValue::List(vec![
        BoltValue::Integer(1),
        BoltValue::String("two".to_string()),
        BoltValue::Boolean(true),
    ]);
    let mut buf = BytesMut::new();
    PackStream::encode(&list_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_list = PackStream::decode(&mut bytes).expect("Failed to decode list");
    assert_eq!(list_val, decoded_list);

    // Map
    let mut map = HashMap::new();
    map.insert("name".to_string(), BoltValue::String("Alice".to_string()));
    map.insert("age".to_string(), BoltValue::Integer(30));
    let map_val = BoltValue::Map(map);

    let mut buf = BytesMut::new();
    PackStream::encode(&map_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_map = PackStream::decode(&mut bytes).expect("Failed to decode map");
    assert_eq!(map_val, decoded_map);
}

#[test]
fn test_packstream_graph_structures_roundtrip() {
    // 1. Node
    let mut props = HashMap::new();
    props.insert("name".to_string(), BoltValue::String("Alice".to_string()));
    let node = BoltNode {
        id: 101,
        labels: vec!["Person".to_string(), "Developer".to_string()],
        properties: props.clone(),
        element_id: Some("4:101".to_string()),
    };
    let node_val = BoltValue::Node(node.clone());

    let mut buf = BytesMut::new();
    PackStream::encode(&node_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_node = PackStream::decode(&mut bytes).expect("Failed to decode node");
    assert_eq!(node_val, decoded_node);

    // 2. Relationship
    let rel = BoltRelationship {
        id: 202,
        start_node_id: 101,
        end_node_id: 102,
        rel_type: "KNOWS".to_string(),
        properties: props.clone(),
        element_id: Some("5:202".to_string()),
        start_element_id: Some("4:101".to_string()),
        end_element_id: Some("4:102".to_string()),
    };
    let rel_val = BoltValue::Relationship(rel.clone());

    let mut buf = BytesMut::new();
    PackStream::encode(&rel_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_rel = PackStream::decode(&mut bytes).expect("Failed to decode relationship");
    assert_eq!(rel_val, decoded_rel);

    // 3. Path
    let path = BoltPath {
        nodes: vec![node],
        relationships: vec![BoltUnboundRelationship {
            id: 202,
            rel_type: "KNOWS".to_string(),
            properties: props,
            element_id: Some("5:202".to_string()),
        }],
        sequence: vec![1, 1],
    };
    let path_val = BoltValue::Path(path);

    let mut buf = BytesMut::new();
    PackStream::encode(&path_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_path = PackStream::decode(&mut bytes).expect("Failed to decode path");
    assert_eq!(path_val, decoded_path);
}

#[test]
fn test_packstream_spatial_points_roundtrip() {
    // 1. Point2D WGS-84 (SRID 4326)
    let p2d_wgs = BoltValue::Point2D(BoltPoint2D {
        srid: srid::WGS_84_2D,
        x: 12.56,
        y: 55.67,
    });
    let mut buf = BytesMut::new();
    PackStream::encode(&p2d_wgs, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_p2d = PackStream::decode(&mut bytes).expect("Failed to decode Point2D");
    assert_eq!(p2d_wgs, decoded_p2d);

    // 2. Point2D Cartesian (SRID 7203)
    let p2d_cart = BoltValue::Point2D(BoltPoint2D {
        srid: srid::CARTESIAN_2D,
        x: 100.0,
        y: 200.0,
    });
    let mut buf = BytesMut::new();
    PackStream::encode(&p2d_cart, &mut buf);
    let mut bytes = buf.freeze();
    assert_eq!(p2d_cart, PackStream::decode(&mut bytes).unwrap());

    // 3. Point3D WGS-84 (SRID 4979)
    let p3d_wgs = BoltValue::Point3D(BoltPoint3D {
        srid: srid::WGS_84_3D,
        x: 13.4,
        y: 52.5,
        z: 35.0,
    });
    let mut buf = BytesMut::new();
    PackStream::encode(&p3d_wgs, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_p3d = PackStream::decode(&mut bytes).expect("Failed to decode Point3D");
    assert_eq!(p3d_wgs, decoded_p3d);

    // 4. Point3D Cartesian (SRID 9157)
    let p3d_cart = BoltValue::Point3D(BoltPoint3D {
        srid: srid::CARTESIAN_3D,
        x: 10.0,
        y: 20.0,
        z: 30.0,
    });
    let mut buf = BytesMut::new();
    PackStream::encode(&p3d_cart, &mut buf);
    let mut bytes = buf.freeze();
    assert_eq!(p3d_cart, PackStream::decode(&mut bytes).unwrap());
}

#[test]
fn test_bolt_messages_encode_decode() {
    // RUN request
    let mut params = HashMap::new();
    params.insert("p0".to_string(), BoltValue::String("Alice".to_string()));
    let run_req = BoltRequest::run(
        "MATCH (n:Person {name: $p0}) RETURN n",
        params,
        Some("neo4j"),
    );

    let mut buf = BytesMut::new();
    run_req.encode(&mut buf);
    assert!(!buf.is_empty());

    // SUCCESS response
    let mut meta = HashMap::new();
    meta.insert(
        "fields".to_string(),
        BoltValue::List(vec![BoltValue::String("n.name".to_string())]),
    );
    let resp = BoltResponse::Success { metadata: meta };

    let mut resp_buf = BytesMut::new();
    PackStream::encode(
        &BoltValue::Structure {
            tag: 0x70,
            fields: vec![BoltValue::Map(match resp {
                BoltResponse::Success { ref metadata } => metadata.clone(),
                _ => HashMap::new(),
            })],
        },
        &mut resp_buf,
    );

    let mut resp_bytes = resp_buf.freeze();
    let decoded_resp = BoltResponse::decode(&mut resp_bytes).expect("Failed to decode response");
    assert_eq!(resp, decoded_resp);
    assert_eq!(decoded_resp.fields(), Some(vec!["n.name".to_string()]));
}

#[test]
fn test_chunked_stream_large_payload() {
    // Large payload (> 16 KB)
    let large_payload = vec![0xAB; 20_000];
    let mut chunk_buf = BytesMut::new();
    encode_chunks(&large_payload, &mut chunk_buf);

    // Reassemble from buffer
    let mut cursor = std::io::Cursor::new(chunk_buf.freeze());
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let reassembled = rt
        .block_on(async { read_message_frame(&mut cursor).await })
        .expect("Failed to reassemble chunks");

    assert_eq!(reassembled.as_ref(), large_payload.as_slice());
}

#[tokio::test]
async fn test_bolt_stub_server_handshake_and_query() {
    // 1. Start mock Bolt stub server
    let stub = BoltStubServer::start()
        .await
        .expect("Failed to start stub server");
    let uri = stub.uri();

    // 2. Connect client
    let config = ConnectionConfig::from_uri(&uri);
    let mut conn = BoltConnection::connect(&config)
        .await
        .expect("Failed to connect to stub");

    assert!(conn.is_valid());

    // 3. Execute query
    let mut params = HashMap::new();
    params.insert(
        "name".to_string(),
        serde_json::Value::String("Alice".to_string()).into(),
    );

    let result = conn
        .execute("MATCH (n:Person) RETURN n.name, n.age", &params)
        .await
        .expect("Query execution failed");

    assert_eq!(result.columns, vec!["name".to_string(), "age".to_string()]);
    assert_eq!(result.row_count(), 2);

    let batch = result
        .into_single_batch()
        .unwrap()
        .expect("Missing record batch");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);

    // 4. Test liveness ping
    conn.ping().await.expect("Ping failed");

    // 5. Close connection
    conn.close().await.expect("Close failed");
}

#[tokio::test]
async fn test_bolt_stub_server_error_and_reset_recovery() {
    let stub = BoltStubServer::start()
        .await
        .expect("Failed to start stub server");
    let config = ConnectionConfig::from_uri(stub.uri());
    let mut conn = BoltConnection::connect(&config)
        .await
        .expect("Failed to connect");

    // Query containing FAIL will trigger simulated server failure
    let err = conn
        .execute("FAIL SYNTAX QUERY", &HashMap::new())
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains("Simulated query execution failure")
    );

    // Connection automatically reset, should be capable of next query
    let result = conn
        .execute("MATCH (n:Person) RETURN n.name", &HashMap::new())
        .await
        .expect("Subsequent query failed after reset");

    assert_eq!(result.row_count(), 2);
}

/// Factory for pooled Bolt connections.
struct BoltConnectionFactory {
    config: ConnectionConfig,
}

#[async_trait::async_trait]
impl ConnectionFactory<BoltConnection> for BoltConnectionFactory {
    async fn create(&self) -> voyager_drivers::Result<BoltConnection> {
        BoltConnection::connect(&self.config).await
    }
}

#[tokio::test]
async fn test_bolt_connection_pool_end_to_end() {
    let stub = BoltStubServer::start()
        .await
        .expect("Failed to start stub server");
    let config = ConnectionConfig::from_uri(stub.uri());

    let factory = Arc::new(BoltConnectionFactory {
        config: config.clone(),
    });
    let pool_config = PoolConfig::new().with_min_idle(2).with_max_size(4);
    let pool = ConnectionPool::new(pool_config, factory);

    pool.warm_up().await.expect("Pool warmup failed");
    assert_eq!(pool.idle_count(), 2);

    // Acquire and execute across multiple concurrent tasks
    let mut handles = Vec::new();
    for i in 0..8 {
        let pool_clone = pool.clone();
        handles.push(tokio::spawn(async move {
            let mut conn = pool_clone.acquire().await.expect("Acquire failed");
            let mut params = HashMap::new();
            params.insert(
                "idx".to_string(),
                serde_json::Value::Number(i.into()).into(),
            );
            let result = conn
                .execute("MATCH (n:Person) RETURN n.name, n.age", &params)
                .await
                .expect("Execution failed");
            assert_eq!(result.row_count(), 2);
        }));
    }

    for h in handles {
        h.await.expect("Task failed");
    }

    assert_eq!(pool.active_count(), 0);
    assert!(pool.idle_count() >= 2);
}

#[test]
fn test_uri_ipv6_bracket_formatting() {
    use voyager_drivers::uri::ParsedUri;

    let uri_ipv6 = ParsedUri::parse("bolt://[::1]:7687").unwrap();
    assert_eq!(uri_ipv6.socket_addr(), "[::1]:7687");

    let uri_raw_ipv6 = ParsedUri::parse("bolt://::1:7687")
        .unwrap_or_else(|_| ParsedUri::parse("bolt://[::1]:7687").unwrap());
    assert_eq!(uri_raw_ipv6.socket_addr(), "[::1]:7687");

    let uri_ipv4 = ParsedUri::parse("bolt://127.0.0.1:7687").unwrap();
    assert_eq!(uri_ipv4.socket_addr(), "127.0.0.1:7687");
}

#[tokio::test]
async fn test_heterogeneous_and_mixed_number_arrow_conversions() {
    let stub = BoltStubServer::start()
        .await
        .expect("Failed to start stub server");
    let config = ConnectionConfig::from_uri(stub.uri());
    let mut conn = BoltConnection::connect(&config)
        .await
        .expect("Connect failed");

    // Test query execution with stub
    let res = conn
        .execute("MATCH (n:Person) RETURN n.name, n.age", &HashMap::new())
        .await
        .expect("Query failed");
    let batch = res.into_single_batch().unwrap().expect("Batch missing");
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(
        batch.schema().field(0).data_type(),
        &arrow_schema::DataType::Utf8
    );
    assert_eq!(
        batch.schema().field(1).data_type(),
        &arrow_schema::DataType::Int64
    );

    conn.close().await.expect("Close failed");
}

#[test]
fn test_packstream_temporal_types_roundtrip() {
    // 1. Date
    let date_epoch = BoltValue::Date(BoltDate::new(0));
    let mut buf = BytesMut::new();
    PackStream::encode(&date_epoch, &mut buf);
    let mut bytes = buf.freeze();
    let decoded = PackStream::decode(&mut bytes).expect("Failed to decode epoch date");
    assert_eq!(decoded, date_epoch);
    assert_eq!(date_epoch.as_date().unwrap().to_iso_string(), "1970-01-01");

    // Leap year date (Feb 29, 2024)
    let date_leap = BoltValue::Date(BoltDate::from_ymd(2024, 2, 29).expect("valid leap date"));
    let mut buf = BytesMut::new();
    PackStream::encode(&date_leap, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_leap = PackStream::decode(&mut bytes).expect("Failed to decode leap date");
    assert_eq!(decoded_leap, date_leap);
    assert_eq!(date_leap.as_date().unwrap().to_iso_string(), "2024-02-29");
    assert_eq!(date_leap.as_date().unwrap().to_ymd(), (2024, 2, 29));

    // Pre-epoch date (Moon Landing: July 20, 1969)
    let date_moon = BoltValue::Date(BoltDate::from_ymd(1969, 7, 20).expect("valid date"));
    let mut buf = BytesMut::new();
    PackStream::encode(&date_moon, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_moon = PackStream::decode(&mut bytes).expect("Failed to decode pre-epoch date");
    assert_eq!(decoded_moon, date_moon);
    assert_eq!(date_moon.as_date().unwrap().to_iso_string(), "1969-07-20");

    // 2. Time (with timezone offset)
    let time_val = BoltValue::Time(BoltTime::new(52215_123456000, 7200));
    let mut buf = BytesMut::new();
    PackStream::encode(&time_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_time = PackStream::decode(&mut bytes).expect("Failed to decode Time");
    assert_eq!(decoded_time, time_val);
    assert_eq!(
        time_val.as_time().unwrap().to_iso_string(),
        "14:30:15.123456+02:00"
    );

    // Negative offset time (14:30:15-05:00)
    let time_neg = BoltValue::Time(BoltTime::new(52215_000000000, -18000));
    assert_eq!(
        time_neg.as_time().unwrap().to_iso_string(),
        "14:30:15-05:00"
    );

    // 3. LocalTime (no timezone offset)
    let ltime_val = BoltValue::LocalTime(BoltLocalTime::new(52215_123456789));
    let mut buf = BytesMut::new();
    PackStream::encode(&ltime_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_ltime = PackStream::decode(&mut bytes).expect("Failed to decode LocalTime");
    assert_eq!(decoded_ltime, ltime_val);
    assert_eq!(
        ltime_val.as_local_time().unwrap().to_iso_string(),
        "14:30:15.123456789"
    );

    // 4. DateTime (with timezone offset)
    let dt_val = BoltValue::DateTime(BoltDateTime::new(1710513015, 123456000, 7200));
    let mut buf = BytesMut::new();
    PackStream::encode(&dt_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_dt = PackStream::decode(&mut bytes).expect("Failed to decode DateTime");
    assert_eq!(decoded_dt, dt_val);
    assert_eq!(
        dt_val.as_datetime().unwrap().to_iso_string(),
        "2024-03-15T16:30:15.123456+02:00"
    );

    // 5. LocalDateTime
    let ldt_val = BoltValue::LocalDateTime(BoltLocalDateTime::new(1710513015, 500000000));
    let mut buf = BytesMut::new();
    PackStream::encode(&ldt_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_ldt = PackStream::decode(&mut bytes).expect("Failed to decode LocalDateTime");
    assert_eq!(decoded_ldt, ldt_val);
    assert_eq!(
        ldt_val.as_local_datetime().unwrap().to_iso_string(),
        "2024-03-15T14:30:15.500"
    );

    // 6. DateTimeZoneId (with IANA timezone name)
    let dtz_val = BoltValue::DateTimeZoneId(BoltDateTimeZoneId::new(
        1710513015,
        123456000,
        "Europe/Berlin",
    ));
    let mut buf = BytesMut::new();
    PackStream::encode(&dtz_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_dtz = PackStream::decode(&mut bytes).expect("Failed to decode DateTimeZoneId");
    assert_eq!(decoded_dtz, dtz_val);
    assert_eq!(
        dtz_val.as_datetime_zone_id().unwrap().to_iso_string(),
        "2024-03-15T14:30:15.123456Z[Europe/Berlin]"
    );

    // 7. Duration
    let dur_val = BoltValue::Duration(BoltDuration::new(14, 3, 14706, 789000000));
    let mut buf = BytesMut::new();
    PackStream::encode(&dur_val, &mut buf);
    let mut bytes = buf.freeze();
    let decoded_dur = PackStream::decode(&mut bytes).expect("Failed to decode Duration");
    assert_eq!(decoded_dur, dur_val);
    assert_eq!(
        dur_val.as_duration().unwrap().to_iso_string(),
        "P1Y2M3DT4H5M6.789S"
    );

    // Zero duration
    let dur_zero = BoltValue::Duration(BoltDuration::new(0, 0, 0, 0));
    assert_eq!(dur_zero.as_duration().unwrap().to_iso_string(), "PT0S");
}

#[test]
fn test_bolt_canonical_json_entities_and_paths() {
    let mut props_a = HashMap::new();
    props_a.insert("name".to_string(), BoltValue::String("A".to_string()));
    props_a.insert(
        "tags".to_string(),
        BoltValue::List(vec![BoltValue::Integer(1), BoltValue::Boolean(true)]),
    );
    let node_a = BoltNode {
        id: 10,
        labels: vec!["ZzB".to_string(), "ZzA".to_string()],
        properties: props_a,
        element_id: Some("4:db:10".to_string()),
    };

    let mut props_b = HashMap::new();
    props_b.insert("name".to_string(), BoltValue::String("B".to_string()));
    let node_b = BoltNode {
        id: 20,
        labels: vec!["ZzB".to_string()],
        properties: props_b,
        element_id: Some("4:db:20".to_string()),
    };

    let mut rel_props = HashMap::new();
    rel_props.insert("w".to_string(), BoltValue::Integer(1));
    let rel_forward = BoltRelationship {
        id: 30,
        start_node_id: 10,
        end_node_id: 20,
        rel_type: "ZK".to_string(),
        properties: rel_props.clone(),
        element_id: Some("5:db:30".to_string()),
        start_element_id: Some("4:db:10".to_string()),
        end_element_id: Some("4:db:20".to_string()),
    };

    let node_json = bolt_value_to_canonical_json(&BoltValue::Node(node_a.clone()));
    assert_eq!(node_json["kind"], "node");
    assert_eq!(node_json["id"], 10);
    assert_eq!(node_json["element_id"], "4:db:10");
    assert_eq!(node_json["labels"], serde_json::json!(["ZzA", "ZzB"]));
    assert_eq!(node_json["properties"]["name"], "A");
    assert_eq!(
        node_json["properties"]["tags"],
        serde_json::json!([1, true])
    );

    let rel_json = bolt_value_to_canonical_json(&BoltValue::Relationship(rel_forward));
    assert_eq!(rel_json["kind"], "relationship");
    assert_eq!(rel_json["type"], "ZK");
    assert_eq!(rel_json["start_id"], 10);
    assert_eq!(rel_json["end_id"], 20);
    assert_eq!(rel_json["start_element_id"], "4:db:10");
    assert_eq!(rel_json["end_element_id"], "4:db:20");
    assert_eq!(rel_json["properties"], serde_json::json!({"w": 1}));

    // Cycle path: node_a (0) -[rel0 +]-> node_b (1) <-[rel0 -]- node_a (0)
    let unbound = BoltUnboundRelationship {
        id: 31,
        rel_type: "ZK".to_string(),
        properties: rel_props,
        element_id: Some("5:db:31".to_string()),
    };
    let cycle_path = BoltPath {
        nodes: vec![node_a.clone(), node_b.clone()],
        relationships: vec![unbound],
        sequence: vec![1, 1, -1, 0],
    };
    let path_json = bolt_value_to_canonical_json(&BoltValue::Path(cycle_path));
    assert_eq!(path_json["kind"], "path");
    let path_nodes = path_json["nodes"].as_array().unwrap();
    assert_eq!(path_nodes.len(), 3);
    assert_eq!(path_nodes[0]["id"], 10);
    assert_eq!(path_nodes[1]["id"], 20);
    assert_eq!(path_nodes[2]["id"], 10);
    let path_rels = path_json["relationships"].as_array().unwrap();
    assert_eq!(path_rels.len(), 2);
    assert_eq!(path_rels[0]["start_id"], 10);
    assert_eq!(path_rels[0]["end_id"], 20);
    assert_eq!(path_rels[1]["start_id"], 10);
    assert_eq!(path_rels[1]["end_id"], 20);
}

#[test]
fn test_build_bolt_record_batch_entity_metadata_and_json() {
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;

    let mut props = HashMap::new();
    props.insert("name".to_string(), BoltValue::String("A".to_string()));
    let node = BoltNode {
        id: 1,
        labels: vec!["ZzS".to_string()],
        properties: props,
        element_id: None,
    };

    let cols = vec!["n".to_string(), "count".to_string()];
    let rows = vec![vec![BoltValue::Node(node), BoltValue::Integer(42)]];
    let batch = build_bolt_record_batch(&cols, &rows).unwrap();

    let schema = batch.schema();
    let n_field = schema.field(0);
    assert_eq!(n_field.data_type(), &DataType::Utf8);
    assert_eq!(
        n_field
            .metadata()
            .get("voyager.encoding")
            .map(String::as_str),
        Some("json")
    );
    assert_eq!(
        n_field.metadata().get("voyager.kind").map(String::as_str),
        Some("node")
    );

    let count_field = schema.field(1);
    assert_eq!(count_field.data_type(), &DataType::Int64);
    assert!(!count_field.metadata().contains_key("voyager.encoding"));

    let raw_json = batch.column(0).as_string::<i32>().value(0);
    let parsed: serde_json::Value = serde_json::from_str(raw_json).unwrap();
    assert_eq!(parsed["kind"], "node");
    assert_eq!(parsed["element_id"], serde_json::Value::Null);
    assert_eq!(parsed["labels"], serde_json::json!(["ZzS"]));
    assert_eq!(parsed["properties"], serde_json::json!({"name": "A"}));
}

#[test]
fn test_param_to_bolt_value_v5_and_legacy_v4() {
    // Date -> BoltDate
    let d = ParamValue::Date {
        year: 2024,
        month: 1,
        day: 2,
    };
    let bv_d = param_to_bolt_value(&d, 5).unwrap();
    assert_eq!(bv_d.as_date().unwrap().to_iso_string(), "2024-01-02");

    // LocalTime (0x74) and offset Time (0x54)
    let lt = ParamValue::Time {
        hour: 14,
        minute: 30,
        second: 15,
        nanosecond: 123_456_000,
        tz_offset_seconds: None,
    };
    assert_eq!(
        param_to_bolt_value(&lt, 5)
            .unwrap()
            .as_local_time()
            .unwrap()
            .to_iso_string(),
        "14:30:15.123456"
    );

    let ot = ParamValue::Time {
        hour: 14,
        minute: 30,
        second: 15,
        nanosecond: 123_456_000,
        tz_offset_seconds: Some(7200),
    };
    assert_eq!(
        param_to_bolt_value(&ot, 5)
            .unwrap()
            .as_time()
            .unwrap()
            .to_iso_string(),
        "14:30:15.123456+02:00"
    );

    // LocalDateTime (0x64)
    let ldt = ParamValue::LocalDateTime {
        year: 2024,
        month: 1,
        day: 2,
        hour: 3,
        minute: 4,
        second: 5,
        nanosecond: 123_456_000,
    };
    assert_eq!(
        param_to_bolt_value(&ldt, 5)
            .unwrap()
            .as_local_datetime()
            .unwrap()
            .to_iso_string(),
        "2024-01-02T03:04:05.123456"
    );

    // Aware DateTime with offset: 2024-01-02T03:04:05.123456+02:00 -> utc_seconds = 1704157445
    let dt_off = ParamValue::DateTime {
        utc_seconds: 1_704_157_445,
        nanosecond: 123_456_000,
        tz: ParamTimezone::OffsetSeconds(7200),
    };
    let bv_v5 = param_to_bolt_value(&dt_off, 5).unwrap();
    assert_eq!(
        bv_v5.as_datetime().unwrap().to_iso_string(),
        "2024-01-02T03:04:05.123456+02:00"
    );
    let mut buf5 = BytesMut::new();
    PackStream::encode(&bv_v5, &mut buf5);
    assert_eq!(buf5[1], 0x49); // Bolt 5 UTC DateTime tag 'I'

    let bv_v4 = param_to_bolt_value(&dt_off, 4).unwrap();
    match &bv_v4 {
        BoltValue::Structure { tag, fields } => {
            assert_eq!(*tag, 0x46); // Legacy Bolt 4 DateTime tag 'F'
            assert_eq!(fields[0], BoltValue::Integer(1_704_157_445 + 7200));
            assert_eq!(fields[1], BoltValue::Integer(123_456_000));
            assert_eq!(fields[2], BoltValue::Integer(7200));
        }
        other => panic!("Expected legacy 0x46 structure, got {other:?}"),
    }
    let mut buf4 = BytesMut::new();
    PackStream::encode(&bv_v4, &mut buf4);
    assert_eq!(buf4[1], 0x46);
    let decoded_v4 = PackStream::decode(&mut buf4.freeze()).unwrap();
    assert_eq!(
        decoded_v4.as_datetime().unwrap().to_iso_string(),
        "2024-01-02T03:04:05.123456+02:00"
    );

    // Aware DateTime with ZoneId (Europe/Berlin, +01:00 in Jan): 2024-01-02T03:04:05+01:00 -> utc = 1704161045
    let dt_zone = ParamValue::DateTime {
        utc_seconds: 1_704_161_045,
        nanosecond: 0,
        tz: ParamTimezone::ZoneId {
            id: "Europe/Berlin".to_string(),
            offset_seconds: 3600,
        },
    };
    let bv_z5 = param_to_bolt_value(&dt_zone, 5).unwrap();
    let mut buf_z5 = BytesMut::new();
    PackStream::encode(&bv_z5, &mut buf_z5);
    assert_eq!(buf_z5[1], 0x69); // Bolt 5 UTC DateTimeZoneId tag 'i'
    let decoded_z5 = PackStream::decode(&mut buf_z5.freeze()).unwrap();
    assert_eq!(
        decoded_z5.as_datetime_zone_id().unwrap().zone_id,
        "Europe/Berlin"
    );
    assert_eq!(
        decoded_z5.as_datetime_zone_id().unwrap().seconds,
        1_704_161_045
    );

    let bv_z4 = param_to_bolt_value(&dt_zone, 4).unwrap();
    match &bv_z4 {
        BoltValue::Structure { tag, fields } => {
            assert_eq!(*tag, 0x66); // Legacy Bolt 4 DateTimeZoneId tag 'f'
            assert_eq!(fields[0], BoltValue::Integer(1_704_161_045 + 3600));
            assert_eq!(fields[2], BoltValue::String("Europe/Berlin".to_string()));
        }
        other => panic!("Expected legacy 0x66 structure, got {other:?}"),
    }

    // Duration, Bytes, Uuid, Decimal, nested List & Map, and plain String
    let dur = ParamValue::Duration {
        months: 0,
        days: 2,
        seconds: 3661,
        nanoseconds: 500_000_000,
    };
    assert_eq!(
        param_to_bolt_value(&dur, 5)
            .unwrap()
            .as_duration()
            .unwrap()
            .to_iso_string(),
        "P2DT1H1M1.500S"
    );

    let bytes = ParamValue::Bytes(vec![0x01, 0x02, 0xFE]);
    assert_eq!(
        param_to_bolt_value(&bytes, 5).unwrap(),
        BoltValue::Bytes(vec![0x01, 0x02, 0xFE])
    );

    let uuid = ParamValue::Uuid("12345678-1234-5678-1234-567812345678".to_string());
    assert_eq!(
        param_to_bolt_value(&uuid, 5).unwrap(),
        BoltValue::String("12345678-1234-5678-1234-567812345678".to_string())
    );

    let dec = ParamValue::Decimal("123.45".to_string());
    assert_eq!(
        param_to_bolt_value(&dec, 5).unwrap(),
        BoltValue::Float(123.45)
    );
    let dec_nan = ParamValue::Decimal("NaN".to_string());
    assert!(matches!(
        param_to_bolt_value(&dec_nan, 5).unwrap(),
        BoltValue::Float(f) if f.is_nan()
    ));
    let dec_snan = ParamValue::Decimal("sNaN".to_string());
    assert!(matches!(
        param_to_bolt_value(&dec_snan, 5).unwrap(),
        BoltValue::Float(f) if f.is_nan()
    ));
    let dec_inf = ParamValue::Decimal("Infinity".to_string());
    assert_eq!(
        param_to_bolt_value(&dec_inf, 5).unwrap(),
        BoltValue::Float(f64::INFINITY)
    );
    let dec_ninf = ParamValue::Decimal("-Infinity".to_string());
    assert_eq!(
        param_to_bolt_value(&dec_ninf, 5).unwrap(),
        BoltValue::Float(f64::NEG_INFINITY)
    );

    let plain_str = ParamValue::from("2024-01-02");
    assert_eq!(
        param_to_bolt_value(&plain_str, 5).unwrap(),
        BoltValue::String("2024-01-02".to_string())
    );

    let mut nested_map = HashMap::new();
    nested_map.insert("d".to_string(), d.clone());
    nested_map.insert("raw".to_string(), plain_str.clone());
    let nested_list = ParamValue::List(vec![ParamValue::Map(nested_map), bytes.clone()]);
    match param_to_bolt_value(&nested_list, 5).unwrap() {
        BoltValue::List(items) => {
            assert!(
                items[0]
                    .as_map()
                    .unwrap()
                    .get("d")
                    .unwrap()
                    .as_date()
                    .is_some()
            );
            assert_eq!(
                items[0].as_map().unwrap().get("raw").unwrap().as_str(),
                Some("2024-01-02")
            );
            assert_eq!(items[1], BoltValue::Bytes(vec![0x01, 0x02, 0xFE]));
        }
        other => panic!("Expected BoltValue::List, got {other:?}"),
    }
}

#[test]
fn test_build_bolt_record_batch_native_temporal_and_binary_types() {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::{DataType, TimeUnit};
    use voyager_drivers::bolt::{
        BoltDate, BoltDateTime, BoltDateTimeZoneId, BoltDuration, BoltLocalDateTime, BoltLocalTime,
        BoltTime,
    };

    let cols = vec![
        "d".to_string(),
        "lt".to_string(),
        "ttz".to_string(),
        "ldt".to_string(),
        "dt_utc".to_string(),
        "dt_berlin".to_string(),
        "dt_offset".to_string(),
        "dt_unknown_tz".to_string(),
        "dur_td".to_string(),
        "dur_cal".to_string(),
        "bin".to_string(),
        "d_list".to_string(),
    ];
    let rows = vec![vec![
        BoltValue::Date(BoltDate { days: 19_724 }),
        BoltValue::LocalTime(BoltLocalTime {
            nanoseconds: 52_215_123_456_000,
        }),
        BoltValue::Time(BoltTime {
            nanoseconds: 52_215_000_000_000,
            tz_offset_seconds: 7200,
        }),
        BoltValue::LocalDateTime(BoltLocalDateTime {
            seconds: 1_704_164_645,
            nanoseconds: 123_456_000,
        }),
        BoltValue::DateTime(BoltDateTime {
            seconds: 1_704_164_645,
            nanoseconds: 0,
            tz_offset_seconds: 0,
        }),
        BoltValue::DateTimeZoneId(BoltDateTimeZoneId {
            seconds: 1_704_161_045,
            nanoseconds: 0,
            zone_id: "Europe/Berlin".to_string(),
            is_local: false,
        }),
        BoltValue::DateTime(BoltDateTime {
            seconds: 1_704_157_445,
            nanoseconds: 0,
            tz_offset_seconds: 7200,
        }),
        BoltValue::DateTimeZoneId(BoltDateTimeZoneId {
            seconds: 1_704_164_645,
            nanoseconds: 0,
            zone_id: "Mars/Olympus".to_string(),
            is_local: false,
        }),
        BoltValue::Duration(BoltDuration {
            months: 0,
            days: 0,
            seconds: 90_000,
            nanoseconds: 500_000_000,
        }),
        BoltValue::Duration(BoltDuration {
            months: 14,
            days: 3,
            seconds: 4,
            nanoseconds: 5000,
        }),
        BoltValue::Bytes(vec![1, 2, 254]),
        BoltValue::List(vec![
            BoltValue::Date(BoltDate { days: 19_723 }),
            BoltValue::Null,
            BoltValue::Date(BoltDate { days: 19_724 }),
        ]),
    ]];

    let batch = build_bolt_record_batch(&cols, &rows).unwrap();
    let schema = batch.schema();
    assert_eq!(schema.field(0).data_type(), &DataType::Date32);
    assert_eq!(
        schema.field(1).data_type(),
        &DataType::Time64(TimeUnit::Nanosecond)
    );
    assert_eq!(schema.field(2).data_type(), &DataType::Utf8);
    assert_eq!(
        schema
            .field(2)
            .metadata()
            .get("voyager.type")
            .map(String::as_str),
        Some("time_tz")
    );
    assert_eq!(
        schema.field(3).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, None)
    );
    assert_eq!(
        schema.field(4).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(
        schema.field(5).data_type(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some("Europe/Berlin".into()))
    );
    assert_eq!(schema.field(6).data_type(), &DataType::Utf8);
    assert_eq!(
        schema
            .field(6)
            .metadata()
            .get("voyager.type")
            .map(String::as_str),
        Some("datetime_tz")
    );
    assert_eq!(schema.field(7).data_type(), &DataType::Utf8);
    assert_eq!(
        schema
            .field(7)
            .metadata()
            .get("voyager.type")
            .map(String::as_str),
        Some("datetime_tz")
    );
    assert_eq!(
        schema.field(8).data_type(),
        &DataType::Duration(TimeUnit::Microsecond)
    );
    assert!(matches!(schema.field(9).data_type(), DataType::Struct(_)));
    assert_eq!(
        schema
            .field(9)
            .metadata()
            .get("voyager.type")
            .map(String::as_str),
        Some("duration")
    );
    assert_eq!(schema.field(10).data_type(), &DataType::Binary);
    assert!(matches!(schema.field(11).data_type(), DataType::List(_)));
    let date_list = batch.column(11).as_list::<i32>().value(0);
    assert_eq!(date_list.len(), 3);
    assert!(date_list.is_null(1));
}

#[test]
fn test_param_to_bolt_value_float_specials_and_points() {
    use voyager_drivers::bolt::{BoltPoint2D, BoltPoint3D};

    let nan_val = param_to_bolt_value(&ParamValue::Float(f64::NAN), 5).unwrap();
    assert!(matches!(nan_val, BoltValue::Float(f) if f.is_nan()));
    let inf_val = param_to_bolt_value(&ParamValue::Float(f64::INFINITY), 5).unwrap();
    assert_eq!(inf_val, BoltValue::Float(f64::INFINITY));

    let p2 = ParamValue::Point2D {
        srid: 4326,
        x: 13.405,
        y: 52.52,
    };
    assert_eq!(
        param_to_bolt_value(&p2, 5).unwrap(),
        BoltValue::Point2D(BoltPoint2D {
            srid: 4326,
            x: 13.405,
            y: 52.52
        })
    );

    let p3 = ParamValue::Point3D {
        srid: 9157,
        x: 1.0,
        y: 2.0,
        z: 3.0,
    };
    assert_eq!(
        param_to_bolt_value(&p3, 5).unwrap(),
        BoltValue::Point3D(BoltPoint3D {
            srid: 9157,
            x: 1.0,
            y: 2.0,
            z: 3.0
        })
    );
}

#[test]
fn test_duration_iso_mixed_signs_and_param_from_f64() {
    use voyager_drivers::params::format_iso_duration;

    assert_eq!(format_iso_duration(1, 0, 0, -500_000_000), "P1MT-0.5S");
    assert_eq!(format_iso_duration(1, 0, -1, 500_000_000), "P1MT-0.5S");
    assert_eq!(format_iso_duration(-1, -2, -3, -500_000_000), "-P1M2DT3.5S");
    assert_eq!(format_iso_duration(1, -2, 3, 0), "P1M-2DT3S");
    assert_eq!(format_iso_duration(0, 1, -5, 0), "P1DT-5S");
    assert_eq!(format_iso_duration(0, 0, 90_000, 0), "PT25H");

    assert_eq!(
        BoltDuration::new(1, 0, 0, -500_000_000).to_iso_string(),
        "P1MT-0.500S"
    );
    assert_eq!(
        BoltDuration::new(1, 0, -1, 500_000_000).to_iso_string(),
        "P1MT-0.500S"
    );

    assert!(matches!(ParamValue::from(f64::NAN), ParamValue::Float(f) if f.is_nan()));
    assert_eq!(
        ParamValue::from(f64::INFINITY),
        ParamValue::Float(f64::INFINITY)
    );
    assert_eq!(
        ParamValue::from(f64::NEG_INFINITY),
        ParamValue::Float(f64::NEG_INFINITY)
    );
}

#[test]
fn test_bolt_mixed_numeric_routing_legacy_0x66_and_bolt_rows_flag() {
    use arrow_schema::DataType;
    use voyager_drivers::bolt::canonical::{batch_needs_bolt_rows, build_bolt_record_batch};

    // Mixed int + float in list and scalar columns routes to JSON + bolt_rows (no float64 coercion)
    let cols = vec![
        "xs".to_string(),
        "scalar".to_string(),
        "pure_int".to_string(),
    ];
    let rows = vec![
        vec![
            BoltValue::List(vec![
                BoltValue::Integer(9_007_199_254_740_993),
                BoltValue::Float(2.5),
            ]),
            BoltValue::Integer(1),
            BoltValue::Integer(42),
        ],
        vec![
            BoltValue::List(vec![BoltValue::Integer(1), BoltValue::Float(2.5)]),
            BoltValue::Float(2.5),
            BoltValue::Integer(43),
        ],
    ];
    let batch = build_bolt_record_batch(&cols, &rows).unwrap();
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
    assert_eq!(
        batch
            .schema()
            .field(0)
            .metadata()
            .get("voyager.encoding")
            .map(String::as_str),
        Some("json")
    );
    assert_eq!(batch.schema().field(1).data_type(), &DataType::Utf8);
    assert_eq!(
        batch
            .schema()
            .field(1)
            .metadata()
            .get("voyager.encoding")
            .map(String::as_str),
        Some("json")
    );
    assert_eq!(batch.schema().field(2).data_type(), &DataType::Int64);
    assert!(batch_needs_bolt_rows(&batch));

    // Scalar-only batch does not need bolt_rows
    let scalar_batch = build_bolt_record_batch(
        &["n".to_string(), "d".to_string()],
        &[vec![
            BoltValue::Integer(1),
            BoltValue::Duration(BoltDuration::new(0, 0, 90_000, 0)),
        ]],
    )
    .unwrap();
    assert!(!batch_needs_bolt_rows(&scalar_batch));

    // Legacy Bolt v4 tag 0x66 carries local wall-clock seconds
    let local_secs = 1_704_164_645; // 2024-01-02T03:04:05 local
    let legacy_struct = BoltValue::Structure {
        tag: 0x66,
        fields: vec![
            BoltValue::Integer(local_secs),
            BoltValue::Integer(123_456_000),
            BoltValue::String("Europe/Berlin".to_string()),
        ],
    };
    let mut buf = BytesMut::new();
    PackStream::encode(&legacy_struct, &mut buf);
    let decoded = PackStream::decode(&mut buf.freeze()).unwrap();
    let dtz = decoded.as_datetime_zone_id().expect("BoltDateTimeZoneId");
    assert!(dtz.is_local);
    assert_eq!(dtz.seconds, local_secs);
    assert_eq!(dtz.zone_id, "Europe/Berlin");
    assert_eq!(
        dtz.to_iso_string(),
        "2024-01-02T03:04:05.123456[Europe/Berlin]"
    );

    // Legacy 0x66 with a valid IANA zone converts local wall-clock seconds to UTC Timestamp
    let dtz_batch = build_bolt_record_batch(&["dt".to_string()], &[vec![decoded.clone()]]).unwrap();
    assert_eq!(
        dtz_batch.schema().field(0).data_type(),
        &DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            Some("Europe/Berlin".into())
        )
    );
    let ts_col = dtz_batch
        .column(0)
        .as_any()
        .downcast_ref::<arrow_array::TimestampMicrosecondArray>()
        .unwrap();
    // 2024-01-02T03:04:05.123456 Europe/Berlin (+01:00) == 2024-01-02T02:04:05.123456Z
    assert_eq!(ts_col.value(0), 1_704_161_045_123_456);
    assert!(!batch_needs_bolt_rows(&dtz_batch));
}

#[test]
fn test_build_bolt_record_batch_points_and_point_lists() {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;

    let cols = vec![
        "p2".to_string(),
        "p3".to_string(),
        "mixed_dim".to_string(),
        "plist".to_string(),
    ];
    let pt2 = BoltValue::Point2D(BoltPoint2D {
        srid: srid::WGS_84_2D,
        x: 13.405,
        y: 52.52,
    });
    let pt3 = BoltValue::Point3D(BoltPoint3D {
        srid: srid::CARTESIAN_3D,
        x: 1.0,
        y: 2.0,
        z: 3.5,
    });
    let rows = vec![
        vec![
            pt2.clone(),
            pt3.clone(),
            pt2.clone(),
            BoltValue::List(vec![pt2.clone(), BoltValue::Null, pt3.clone()]),
        ],
        vec![BoltValue::Null, pt3.clone(), pt3.clone(), BoltValue::Null],
    ];
    let batch = build_bolt_record_batch(&cols, &rows).unwrap();
    let schema = batch.schema();

    for idx in 0..3 {
        assert!(matches!(schema.field(idx).data_type(), DataType::Struct(_)));
        assert_eq!(
            schema
                .field(idx)
                .metadata()
                .get("voyager.type")
                .map(String::as_str),
            Some("point")
        );
    }
    let p2_struct = batch.column(0).as_struct();
    assert!(!p2_struct.is_null(0));
    assert!(p2_struct.is_null(1));
    assert_eq!(
        p2_struct
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        4326
    );
    assert!(p2_struct.column(3).is_null(0));

    let p3_struct = batch.column(1).as_struct();
    assert_eq!(
        p3_struct
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Float64Array>()
            .unwrap()
            .value(0),
        3.5
    );

    assert!(matches!(schema.field(3).data_type(), DataType::List(_)));
    let plist_col = batch.column(3).as_list::<i32>();
    assert!(plist_col.is_null(1));
    let row0_list = plist_col.value(0);
    assert_eq!(row0_list.len(), 3);
    assert!(row0_list.is_null(1));
    assert!(!voyager_drivers::bolt::canonical::batch_needs_bolt_rows(
        &batch
    ));
}

#[test]
fn test_legacy_0x66_wire_bytes_and_negative_durations() {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::{DataType, TimeUnit};
    use bytes::BufMut;

    // Raw PackStream struct: 0xB3 (3-field struct), 0x66 (legacy DateTimeZoneId)
    let local_secs: i64 = 1_704_164_645; // 2024-01-02T03:04:05
    let mut raw_bytes = BytesMut::new();
    raw_bytes.put_u8(0xB3);
    raw_bytes.put_u8(0x66);
    PackStream::encode(&BoltValue::Integer(local_secs), &mut raw_bytes);
    PackStream::encode(&BoltValue::Integer(123_456_000), &mut raw_bytes);
    PackStream::encode(
        &BoltValue::String("Europe/Berlin".to_string()),
        &mut raw_bytes,
    );

    let decoded_berlin = PackStream::decode(&mut raw_bytes.freeze()).unwrap();
    let mut ny_bytes = BytesMut::new();
    ny_bytes.put_u8(0xB3);
    ny_bytes.put_u8(0x66);
    PackStream::encode(&BoltValue::Integer(local_secs), &mut ny_bytes);
    PackStream::encode(&BoltValue::Integer(123_456_000), &mut ny_bytes);
    PackStream::encode(
        &BoltValue::String("America/New_York".to_string()),
        &mut ny_bytes,
    );
    let decoded_ny = PackStream::decode(&mut ny_bytes.freeze()).unwrap();

    // Mixed-zone 0x66 falls back to Utf8 with resolved offsets (never offset-less!)
    let fallback_batch = build_bolt_record_batch(
        &["dt".to_string()],
        &[vec![decoded_berlin.clone()], vec![decoded_ny]],
    )
    .unwrap();
    assert_eq!(
        fallback_batch.schema().field(0).data_type(),
        &DataType::Utf8
    );
    let strs = fallback_batch.column(0).as_string::<i32>();
    assert_eq!(
        strs.value(0),
        "2024-01-02T03:04:05.123456+01:00[Europe/Berlin]"
    );
    assert_eq!(
        strs.value(1),
        "2024-01-02T03:04:05.123456-05:00[America/New_York]"
    );

    // Canonical JSON (used inside maps/entities) also includes resolved offset for legacy 0x66
    assert_eq!(
        bolt_value_to_canonical_json(&decoded_berlin),
        serde_json::Value::String("2024-01-02T03:04:05.123456+01:00[Europe/Berlin]".to_string())
    );

    // Negative sub-second duration preserves exact components via Struct(months, days, seconds, nanoseconds)
    let neg_sub = BoltValue::Duration(BoltDuration::new(0, 0, 0, -1_000));
    let pos_dur = BoltValue::Duration(BoltDuration::new(0, 0, 5, 0));
    let dur_batch = build_bolt_record_batch(
        &["neg".to_string(), "pos".to_string()],
        &[vec![neg_sub, pos_dur]],
    )
    .unwrap();
    assert!(matches!(
        dur_batch.schema().field(0).data_type(),
        DataType::Struct(_)
    ));
    assert_eq!(
        dur_batch
            .schema()
            .field(0)
            .metadata()
            .get("voyager.type")
            .map(String::as_str),
        Some("duration")
    );
    let neg_struct = dur_batch.column(0).as_struct();
    assert_eq!(
        neg_struct
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        0 // days=0 (not normalized to -1!)
    );
    assert_eq!(
        neg_struct
            .column(3)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .value(0),
        -1_000
    );
    assert_eq!(
        dur_batch.schema().field(1).data_type(),
        &DataType::Duration(TimeUnit::Microsecond)
    );
}

#[tokio::test]
async fn test_bolt_execute_rows_and_execute_arrow_equivalence_and_buffer_reuse() {
    use bytes::BytesMut;
    use tokio::io::AsyncWriteExt;
    use voyager_drivers::bolt::messages::BoltResponse;
    use voyager_drivers::bolt::packstream::{
        BoltDateTimeZoneId, BoltDuration, BoltNode, BoltPath, BoltPoint2D, BoltRelationship,
        BoltUnboundRelationship, BoltValue, PackStream,
    };
    use voyager_drivers::bolt::stream::{encode_chunks, read_message_frame, write_message_frame};
    use voyager_drivers::bolt::{BoltConnection, BoltStubServer, BoltVersion};
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::engine::{AsyncConnection, ResultMode};
    use voyager_drivers::value::result::ResultValue;
    use voyager_drivers::{NativeClient, ParamTimezone, ParamValue};

    // 1. NativeClient & NativeTransaction against BoltStubServer
    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(&stub.uri(), Some(PoolConfig::default())).unwrap();

    let legacy = client.execute("RETURN 1", &HashMap::new()).await.unwrap();
    let rows_res = client
        .execute_rows("RETURN 1", &HashMap::new())
        .await
        .unwrap();
    let arrow_res = client
        .execute_arrow("RETURN 1", &HashMap::new())
        .await
        .unwrap();

    assert!(rows_res.batches.is_empty());
    assert!(rows_res.result_rows.is_some());
    assert!(rows_res.bolt_rows.is_none());
    assert_eq!(rows_res.row_count(), legacy.row_count());

    assert_eq!(arrow_res.batches.len(), 1);
    assert!(arrow_res.result_rows.is_none());
    assert!(arrow_res.bolt_rows.is_none());

    let legacy_batch = legacy.to_record_batch().unwrap().unwrap();
    let rows_batch = rows_res.to_record_batch().unwrap().unwrap();
    let arrow_batch = arrow_res.to_record_batch().unwrap().unwrap();
    assert_eq!(rows_batch, legacy_batch);
    assert_eq!(arrow_batch, legacy_batch);

    // Transaction execute_rows & execute_arrow
    let mut tx = client.begin_transaction().await.unwrap();
    let tx_rows = tx.execute_rows("RETURN 1", &HashMap::new()).await.unwrap();
    let tx_arrow = tx.execute_arrow("RETURN 1", &HashMap::new()).await.unwrap();
    assert!(tx_rows.batches.is_empty());
    assert_eq!(tx_rows.to_record_batch().unwrap().unwrap(), legacy_batch);
    assert!(tx_arrow.result_rows.is_none());
    assert_eq!(tx_arrow.to_record_batch().unwrap().unwrap(), legacy_batch);
    tx.commit().await.unwrap();
    client.close().await;

    // 2. Duplex rich-entity + temporal + spatial + 0-row + parameterized buffer-reuse equivalence
    fn send_msg(resp: &BoltResponse) -> Vec<u8> {
        let val = match resp {
            BoltResponse::Success { metadata } => BoltValue::Structure {
                tag: 0x70,
                fields: vec![BoltValue::Map(metadata.clone())],
            },
            BoltResponse::Record { fields } => BoltValue::Structure {
                tag: 0x71,
                fields: vec![BoltValue::List(fields.clone())],
            },
            _ => unreachable!(),
        };
        let mut payload = BytesMut::new();
        PackStream::encode(&val, &mut payload);
        let mut chunks = BytesMut::new();
        encode_chunks(&payload, &mut chunks);
        chunks.to_vec()
    }

    let node1 = BoltNode {
        id: 1,
        labels: vec!["Person".to_string()],
        properties: HashMap::from([("name".to_string(), BoltValue::String("Alice".to_string()))]),
        element_id: Some("4:a:1".to_string()),
    };
    let node2 = BoltNode {
        id: 2,
        labels: vec!["Person".to_string()],
        properties: HashMap::from([("name".to_string(), BoltValue::String("Bob".to_string()))]),
        element_id: Some("4:a:2".to_string()),
    };
    let rel = BoltRelationship {
        id: 10,
        start_node_id: 1,
        end_node_id: 2,
        rel_type: "KNOWS".to_string(),
        properties: HashMap::from([("since".to_string(), BoltValue::Integer(2024))]),
        element_id: Some("5:a:10".to_string()),
        start_element_id: Some("4:a:1".to_string()),
        end_element_id: Some("4:a:2".to_string()),
    };
    let path = BoltPath {
        nodes: vec![node1.clone(), node2],
        relationships: vec![BoltUnboundRelationship {
            id: 10,
            rel_type: "KNOWS".to_string(),
            properties: HashMap::from([("since".to_string(), BoltValue::Integer(2024))]),
            element_id: Some("5:a:10".to_string()),
        }],
        sequence: vec![1, 1],
    };
    let rich_row = vec![
        BoltValue::Integer(7),
        BoltValue::Node(node1),
        BoltValue::Relationship(rel),
        BoltValue::Path(path),
        BoltValue::Point2D(BoltPoint2D {
            srid: srid::WGS_84_2D,
            x: 13.4,
            y: 52.5,
        }),
        BoltValue::DateTimeZoneId(BoltDateTimeZoneId {
            seconds: 1_700_000_000,
            nanoseconds: 123_000,
            zone_id: "Europe/Berlin".to_string(),
            is_local: false,
        }),
        BoltValue::Duration(BoltDuration::new(1, 2, 3, 4_000)),
        BoltValue::Map(HashMap::from([(
            "k".to_string(),
            BoltValue::List(vec![
                BoltValue::Integer(1),
                BoltValue::String("x".to_string()),
            ]),
        )])),
    ];

    let (client_stream, mut server_stream) = tokio::io::duplex(65536);
    let expected_rich_row = rich_row.clone();
    let server_task = tokio::spawn(async move {
        // Serve 6 queries: (legacy, Rows, Arrow) for rich query + (legacy, Rows, Arrow) for 0-row query
        for idx in 0..6 {
            let mut run_frame = read_message_frame(&mut server_stream).await.unwrap();
            let run_val = PackStream::decode(&mut run_frame).unwrap();
            // Verify direct RUN parameter encoding round-trips cleanly
            let BoltValue::Structure { tag: 0x10, fields } = run_val else {
                panic!("expected RUN");
            };
            assert!(matches!(fields[1], BoltValue::Map(_)));
            let mut pull_frame = read_message_frame(&mut server_stream).await.unwrap();
            let pull_val = PackStream::decode(&mut pull_frame).unwrap();
            assert!(matches!(pull_val, BoltValue::Structure { tag: 0x3F, .. }));

            let mut wire = Vec::new();
            let cols = if idx < 3 {
                vec!["id", "n", "r", "p", "pt", "dt", "dur", "m"]
            } else {
                vec!["empty_col"]
            };
            let run_meta = HashMap::from([(
                "fields".to_string(),
                BoltValue::List(
                    cols.into_iter()
                        .map(|s| BoltValue::String(s.to_string()))
                        .collect(),
                ),
            )]);
            wire.extend_from_slice(&send_msg(&BoltResponse::Success { metadata: run_meta }));
            if idx < 3 {
                wire.extend_from_slice(&send_msg(&BoltResponse::Record {
                    fields: expected_rich_row.clone(),
                }));
            }
            wire.extend_from_slice(&send_msg(&BoltResponse::Success {
                metadata: HashMap::new(),
            }));
            write_message_frame(&mut server_stream, &wire)
                .await
                .unwrap();
            let _ = server_stream.flush().await;
        }
    });

    let mut conn = BoltConnection::from_stream(client_stream, BoltVersion { major: 5, minor: 4 });
    let mut params = HashMap::new();
    params.insert("p_int".to_string(), ParamValue::Json(serde_json::json!(42)));
    params.insert(
        "p_dt".to_string(),
        ParamValue::DateTime {
            utc_seconds: 1_700_000_000,
            nanosecond: 500,
            tz: ParamTimezone::ZoneId {
                id: "UTC".to_string(),
                offset_seconds: 0,
            },
        },
    );
    params.insert(
        "p_list".to_string(),
        ParamValue::List(vec![
            ParamValue::Float(1.5),
            ParamValue::Uuid("abc".to_string()),
        ]),
    );

    let leg_rich = conn.execute("RETURN ...", &params).await.unwrap();
    let row_rich = conn
        .execute_with_mode("RETURN ...", &params, ResultMode::Rows)
        .await
        .unwrap();
    let arr_rich = conn
        .execute_with_mode("RETURN ...", &params, ResultMode::Arrow)
        .await
        .unwrap();

    assert!(row_rich.batches.is_empty());
    assert!(arr_rich.result_rows.is_none());
    assert!(arr_rich.bolt_rows.is_none());
    assert_eq!(row_rich.row_count(), 1);
    assert_eq!(
        row_rich.to_record_batch().unwrap().unwrap(),
        leg_rich.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        arr_rich.to_record_batch().unwrap().unwrap(),
        leg_rich.to_record_batch().unwrap().unwrap()
    );
    let expected_rv: Vec<Vec<ResultValue>> = vec![rich_row.iter().map(ResultValue::from).collect()];
    assert_eq!(row_rich.result_rows.as_deref(), Some(&expected_rv));

    // 0-row queries reusing the same BoltConnection buffers and decoder
    let leg_empty = conn
        .execute("RETURN ... LIMIT 0", &HashMap::new())
        .await
        .unwrap();
    let row_empty = conn
        .execute_with_mode("RETURN ... LIMIT 0", &HashMap::new(), ResultMode::Rows)
        .await
        .unwrap();
    let arr_empty = conn
        .execute_with_mode("RETURN ... LIMIT 0", &HashMap::new(), ResultMode::Arrow)
        .await
        .unwrap();
    assert_eq!(row_empty.row_count(), 0);
    assert_eq!(
        row_empty.to_record_batch().unwrap().unwrap(),
        leg_empty.to_record_batch().unwrap().unwrap()
    );
    assert_eq!(
        arr_empty.to_record_batch().unwrap().unwrap(),
        leg_empty.to_record_batch().unwrap().unwrap()
    );

    server_task.await.unwrap();
}

#[tokio::test]
async fn test_bolt_stream_full_pull_count_and_chunk_fidelity() {
    use voyager_drivers::bolt::BoltStubServer;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, StreamOptions};

    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(&stub.uri(), Some(PoolConfig::default())).unwrap();

    let mut stream = client
        .stream(
            "MATCH (n) RETURN n.name, n.age /* ROWS 10000 */",
            &HashMap::new(),
            StreamOptions::new(1_000),
        )
        .await
        .unwrap();
    assert!(stream.is_server_streaming());
    assert_eq!(stream.columns(), &["name".to_string(), "age".to_string()]);
    assert_eq!(client.pool().active_count(), 1);

    let mut total_rows = 0usize;
    let mut chunk_count = 0usize;
    while let Some(chunk) = stream.next_chunk().await.unwrap() {
        chunk_count += 1;
        assert_eq!(chunk.num_rows(), 1_000);
        let batch = chunk.to_record_batch().unwrap();
        assert_eq!(batch.num_rows(), 1_000);
        total_rows += chunk.num_rows();
    }

    assert_eq!(chunk_count, 10);
    assert_eq!(total_rows, 10_000);
    assert!(stream.is_closed());
    assert_eq!(stub.pull_count(), 10);
    assert_eq!(stub.discard_count(), 0);
    assert_eq!(stub.connection_count(), 1);
    assert_eq!(client.pool().active_count(), 0);
    assert_eq!(client.pool().idle_count(), 1);

    client.close().await;
}

#[tokio::test]
async fn test_bolt_stream_early_close_sends_discard_and_reuses_connection() {
    use voyager_drivers::bolt::BoltStubServer;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, StreamOptions};

    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(
        &stub.uri(),
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();

    let mut stream = client
        .stream(
            "STREAM ROWS 10000",
            &HashMap::new(),
            StreamOptions::new(100),
        )
        .await
        .unwrap();
    let first = stream.next_chunk().await.unwrap().expect("chunk 1");
    assert_eq!(first.num_rows(), 100);
    assert_eq!(stub.discard_count(), 0);

    stream.close().await.unwrap();
    // Idempotent second close
    stream.close().await.unwrap();
    assert!(stream.is_closed());
    // Chunk 1 prefetched chunk 2 before returning; close() drained chunk 2 then sent DISCARD.
    assert_eq!(stub.pull_count(), 2);
    assert_eq!(stub.discard_count(), 1);
    assert_eq!(client.pool().active_count(), 0);
    assert_eq!(client.pool().idle_count(), 1);

    // Subsequent query reuses the exact same connection
    let follow_up = client
        .execute_rows("RETURN 1", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(follow_up.row_count(), 2);
    assert_eq!(stub.connection_count(), 1);
    assert_eq!(stub.reset_count(), 0);

    client.close().await;
}

#[tokio::test]
async fn test_bolt_stream_close_before_chunk1_drains_and_discards() {
    use voyager_drivers::bolt::BoltStubServer;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, StreamOptions};

    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(
        &stub.uri(),
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();

    // Case 1: more rows remain after pipelined chunk 1 -> drains chunk 1 then sends DISCARD
    let mut stream = client
        .stream("STREAM ROWS 500", &HashMap::new(), StreamOptions::new(100))
        .await
        .unwrap();
    stream.close().await.unwrap();
    assert_eq!(stub.pull_count(), 1);
    assert_eq!(stub.discard_count(), 1);
    assert_eq!(stub.connection_count(), 1);
    assert_eq!(client.pool().idle_count(), 1);

    // Case 2: all rows fit in pipelined chunk 1 -> drains chunk 1, sees has_more=false, no DISCARD
    let mut small_stream = client
        .stream("RETURN 1", &HashMap::new(), StreamOptions::new(100))
        .await
        .unwrap();
    small_stream.close().await.unwrap();
    assert_eq!(stub.pull_count(), 2);
    assert_eq!(stub.discard_count(), 1);
    assert_eq!(stub.connection_count(), 1);

    let follow_up = client
        .execute_rows("RETURN 2", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(follow_up.row_count(), 2);
    assert_eq!(stub.connection_count(), 1);

    client.close().await;
}

#[tokio::test]
async fn test_bolt_stream_abrupt_drop_marks_broken_without_blocking_and_replaces() {
    use voyager_drivers::bolt::BoltStubServer;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, StreamOptions};

    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(
        &stub.uri(),
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();

    {
        let mut stream = client
            .stream("STREAM ROWS 500", &HashMap::new(), StreamOptions::new(100))
            .await
            .unwrap();
        let chunk1 = stream.next_chunk().await.unwrap().expect("chunk 1");
        assert_eq!(chunk1.num_rows(), 100);
        // Drop `stream` abruptly without calling close()
    }

    assert_eq!(stub.discard_count(), 0);
    assert_eq!(client.pool().active_count(), 0);
    assert_eq!(client.pool().idle_count(), 0);
    assert_eq!(client.pool().total_count(), 0);

    // Next query opens a fresh replacement connection and succeeds
    let res = client
        .execute_rows("RETURN 1", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(res.row_count(), 2);
    assert_eq!(stub.connection_count(), 2);
    assert_eq!(client.pool().total_count(), 1);

    client.close().await;
}

#[tokio::test]
async fn test_bolt_stream_mid_stream_failure_sends_deferred_reset_and_recovers() {
    use voyager_drivers::bolt::BoltStubServer;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, NetError, StreamOptions};

    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(
        &stub.uri(),
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();

    let mut stream = client
        .stream(
            "STREAM ROWS 500 FAIL_MID_STREAM 150",
            &HashMap::new(),
            StreamOptions::new(100),
        )
        .await
        .unwrap();

    let chunk1 = stream.next_chunk().await.unwrap().expect("chunk 1");
    assert_eq!(chunk1.num_rows(), 100);

    let err = stream
        .next_chunk()
        .await
        .expect_err("chunk 2 must return mid-stream FAILURE");
    assert!(
        matches!(err, NetError::ProtocolError(ref m) if m.contains("/ by zero")),
        "unexpected error: {err:?}"
    );
    assert!(stream.is_closed());
    // Connection returns to pool with pending_reset=true; prepare_for_reuse consumes it
    assert_eq!(client.pool().active_count(), 0);
    assert_eq!(client.pool().idle_count(), 1);

    let res = client
        .execute_rows("RETURN 42", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(res.row_count(), 2);
    assert_eq!(stub.reset_count(), 1);
    assert_eq!(stub.connection_count(), 1);
    assert_eq!(client.pool().metrics().connections_failed_health_check, 0);

    client.close().await;
}

#[tokio::test]
async fn test_bolt_stream_explicit_tx_qid_fail_fast_and_drop_guards() {
    use voyager_drivers::bolt::BoltStubServer;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, NativeTransactionState, NetError, StreamOptions};

    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(
        &stub.uri(),
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();

    // 1. Low-level cursor API tests fail-fast guard on concurrent tx statements + qid on PULL & DISCARD
    let mut tx = client.begin_transaction().await.unwrap();
    let mut cursor = tx
        .open_stream_cursor("STREAM ROWS 500", &HashMap::new(), &StreamOptions::new(100))
        .await
        .unwrap();
    assert!(tx.has_open_stream());

    let err_exec = tx.execute("RETURN 1", &HashMap::new()).await.unwrap_err();
    assert!(matches!(err_exec, NetError::TransactionError(ref m) if m.contains("active stream")));
    let err_rows = tx
        .execute_rows("RETURN 1", &HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(err_rows, NetError::TransactionError(ref m) if m.contains("active stream")));
    let err_commit = tx.commit().await.unwrap_err();
    assert!(matches!(err_commit, NetError::TransactionError(ref m) if m.contains("active stream")));
    let err_second_stream = tx
        .open_stream_cursor("RETURN 1", &HashMap::new(), &StreamOptions::default())
        .await
        .unwrap_err();
    assert!(
        matches!(err_second_stream, NetError::TransactionError(ref m) if m.contains("active stream"))
    );

    // Pull chunk 1 and chunk 2 (verifies qid=0 is sent on subsequent PULL), then close (verifies qid=0 on DISCARD)
    let c1 = tx.next_stream_chunk(&mut cursor).await.unwrap().unwrap();
    let c2 = tx.next_stream_chunk(&mut cursor).await.unwrap().unwrap();
    assert_eq!(c1.num_rows(), 100);
    assert_eq!(c2.num_rows(), 100);
    tx.close_stream_cursor(&mut cursor).await.unwrap();
    assert!(!tx.has_open_stream());

    // Follow-up query + commit succeed on the same transaction
    let after = tx.execute_rows("RETURN 1", &HashMap::new()).await.unwrap();
    assert_eq!(after.row_count(), 2);
    tx.commit().await.unwrap();
    assert_eq!(stub.begin_count(), 1);
    assert_eq!(stub.commit_count(), 1);
    assert_eq!(stub.discard_count(), 1);
    assert_eq!(stub.connection_count(), 1);

    // 2. Ergonomic TransactionStream abrupt Drop marks tx failed and rollback cleans up without hanging
    let mut tx2 = client.begin_transaction().await.unwrap();
    {
        let mut st = tx2
            .stream("STREAM ROWS 500", &HashMap::new(), StreamOptions::new(100))
            .await
            .unwrap();
        let _ = st.next_chunk().await.unwrap().unwrap();
        // Drop `st` without close()
    }
    assert!(!tx2.has_open_stream());
    assert_eq!(tx2.state(), NativeTransactionState::Failed);
    let err_after_abort = tx2.commit().await.unwrap_err();
    assert!(matches!(err_after_abort, NetError::TransactionError(_)));
    tx2.rollback().await.unwrap();
    assert_eq!(tx2.state(), NativeTransactionState::RolledBack);

    // Next query opens replacement connection and succeeds
    let recovered = client
        .execute_rows("RETURN 1", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(recovered.row_count(), 2);
    assert_eq!(stub.connection_count(), 2);

    // 3. Explicit rollback while an open stream cursor is active discards broken connection cleanly
    let mut tx3 = client.begin_transaction().await.unwrap();
    let _open_cursor = tx3
        .open_stream_cursor("STREAM ROWS 500", &HashMap::new(), &StreamOptions::new(100))
        .await
        .unwrap();
    tx3.rollback().await.unwrap();
    assert_eq!(tx3.state(), NativeTransactionState::RolledBack);
    assert!(!tx3.has_open_stream());

    client.close().await;
}

#[tokio::test]
async fn test_bolt_v3_fallback_and_memgraph_discard_quirk_and_timeout_cancel() {
    use bytes::BytesMut;
    use tokio::io::AsyncWriteExt;
    use voyager_drivers::bolt::messages::BoltResponse;
    use voyager_drivers::bolt::packstream::{BoltValue, PackStream};
    use voyager_drivers::bolt::stream::{encode_chunks, read_message_frame, write_message_frame};
    use voyager_drivers::bolt::{BoltConnection, BoltVersion};
    use voyager_drivers::engine::AsyncConnection;
    use voyager_drivers::{NetError, StreamOptions};

    fn frame_resp(resp: &BoltResponse) -> Vec<u8> {
        let val = match resp {
            BoltResponse::Success { metadata } => BoltValue::Structure {
                tag: 0x70,
                fields: vec![BoltValue::Map(metadata.clone())],
            },
            BoltResponse::Record { fields } => BoltValue::Structure {
                tag: 0x71,
                fields: vec![BoltValue::List(fields.clone())],
            },
            _ => unreachable!(),
        };
        let mut payload = BytesMut::new();
        PackStream::encode(&val, &mut payload);
        let mut chunks = BytesMut::new();
        encode_chunks(&payload, &mut chunks);
        chunks.to_vec()
    }

    // 1. Bolt v3.0 falls back to Buffered stream
    let (v3_client, mut v3_server) = tokio::io::duplex(65536);
    let v3_srv = tokio::spawn(async move {
        let _ = read_message_frame(&mut v3_server).await.unwrap();
        let _ = read_message_frame(&mut v3_server).await.unwrap();
        let mut wire = Vec::new();
        wire.extend_from_slice(&frame_resp(&BoltResponse::Success {
            metadata: HashMap::from([(
                "fields".to_string(),
                BoltValue::List(vec![BoltValue::String("n".to_string())]),
            )]),
        }));
        for i in 0..3 {
            wire.extend_from_slice(&frame_resp(&BoltResponse::Record {
                fields: vec![BoltValue::Integer(i)],
            }));
        }
        wire.extend_from_slice(&frame_resp(&BoltResponse::Success {
            metadata: HashMap::new(),
        }));
        write_message_frame(&mut v3_server, &wire).await.unwrap();
        let _ = v3_server.flush().await;
    });

    let mut conn_v3 = BoltConnection::from_stream(v3_client, BoltVersion { major: 3, minor: 0 });
    let mut v3_cursor = conn_v3
        .open_stream("RETURN 1", &HashMap::new(), &StreamOptions::new(2))
        .await
        .unwrap();
    assert!(!v3_cursor.is_server_streaming());
    assert!(v3_cursor.is_wire_clean());
    let b1 = conn_v3
        .next_stream_chunk(&mut v3_cursor)
        .await
        .unwrap()
        .unwrap();
    let b2 = conn_v3
        .next_stream_chunk(&mut v3_cursor)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b1.num_rows(), 2);
    assert_eq!(b2.num_rows(), 1);
    assert!(
        conn_v3
            .next_stream_chunk(&mut v3_cursor)
            .await
            .unwrap()
            .is_none()
    );
    conn_v3.close_stream(&mut v3_cursor).await.unwrap();
    v3_srv.await.unwrap();

    // 2. Memgraph quirks: RUN lacks t_first, DISCARD SUCCESS carries "has_more": false
    let (mg_client, mut mg_server) = tokio::io::duplex(65536);
    let mg_srv = tokio::spawn(async move {
        let _run = read_message_frame(&mut mg_server).await.unwrap();
        let _pull1 = read_message_frame(&mut mg_server).await.unwrap();
        let mut wire = Vec::new();
        // RUN SUCCESS without t_first
        wire.extend_from_slice(&frame_resp(&BoltResponse::Success {
            metadata: HashMap::from([(
                "fields".to_string(),
                BoltValue::List(vec![BoltValue::String("n".to_string())]),
            )]),
        }));
        wire.extend_from_slice(&frame_resp(&BoltResponse::Record {
            fields: vec![BoltValue::Integer(1)],
        }));
        wire.extend_from_slice(&frame_resp(&BoltResponse::Success {
            metadata: HashMap::from([("has_more".to_string(), BoltValue::Boolean(true))]),
        }));
        write_message_frame(&mut mg_server, &wire).await.unwrap();
        let _ = mg_server.flush().await;

        // Chunk 1 prefetches chunk 2 before returning; serve chunk 2 with has_more=true
        // so close_stream drains it and then sends DISCARD.
        let mut pull2_frame = read_message_frame(&mut mg_server).await.unwrap();
        let pull2_msg = PackStream::decode(&mut pull2_frame).unwrap();
        assert!(matches!(pull2_msg, BoltValue::Structure { tag: 0x3F, .. }));
        let mut pull2_wire = Vec::new();
        pull2_wire.extend_from_slice(&frame_resp(&BoltResponse::Record {
            fields: vec![BoltValue::Integer(2)],
        }));
        pull2_wire.extend_from_slice(&frame_resp(&BoltResponse::Success {
            metadata: HashMap::from([("has_more".to_string(), BoltValue::Boolean(true))]),
        }));
        write_message_frame(&mut mg_server, &pull2_wire)
            .await
            .unwrap();
        let _ = mg_server.flush().await;

        let mut discard_frame = read_message_frame(&mut mg_server).await.unwrap();
        let discard_msg = PackStream::decode(&mut discard_frame).unwrap();
        assert!(matches!(
            discard_msg,
            BoltValue::Structure { tag: 0x2F, .. }
        ));
        let disc_wire = frame_resp(&BoltResponse::Success {
            metadata: HashMap::from([("has_more".to_string(), BoltValue::Boolean(false))]),
        });
        write_message_frame(&mut mg_server, &disc_wire)
            .await
            .unwrap();
        let _ = mg_server.flush().await;
    });

    let mut conn_mg = BoltConnection::from_stream(mg_client, BoltVersion { major: 5, minor: 2 });
    let mut mg_cursor = conn_mg
        .open_stream("RETURN 1", &HashMap::new(), &StreamOptions::new(1))
        .await
        .unwrap();
    assert!(mg_cursor.is_server_streaming());
    assert_eq!(mg_cursor.summary().execution_time_ms, 0);
    let mg_c1 = conn_mg
        .next_stream_chunk(&mut mg_cursor)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(mg_c1.num_rows(), 1);
    conn_mg.close_stream(&mut mg_cursor).await.unwrap();
    assert!(mg_cursor.is_wire_clean());
    assert!(conn_mg.is_valid());
    mg_srv.await.unwrap();

    // 3. Mid-flight tokio::time::timeout cancellation marks connection dirty/unusable
    let (to_client, mut to_server) = tokio::io::duplex(65536);
    let to_srv = tokio::spawn(async move {
        let _run = read_message_frame(&mut to_server).await.unwrap();
        let _pull1 = read_message_frame(&mut to_server).await.unwrap();
        let mut wire = Vec::new();
        wire.extend_from_slice(&frame_resp(&BoltResponse::Success {
            metadata: HashMap::from([(
                "fields".to_string(),
                BoltValue::List(vec![BoltValue::String("n".to_string())]),
            )]),
        }));
        wire.extend_from_slice(&frame_resp(&BoltResponse::Record {
            fields: vec![BoltValue::Integer(1)],
        }));
        wire.extend_from_slice(&frame_resp(&BoltResponse::Success {
            metadata: HashMap::from([("has_more".to_string(), BoltValue::Boolean(true))]),
        }));
        write_message_frame(&mut to_server, &wire).await.unwrap();
        let _ = to_server.flush().await;

        let _pull2 = read_message_frame(&mut to_server).await.unwrap();
        // Stall intentionally on chunk 2 so caller timeout fires mid-await
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    });

    let mut conn_to = BoltConnection::from_stream(to_client, BoltVersion { major: 5, minor: 4 });
    let mut to_cursor = conn_to
        .open_stream("RETURN 1", &HashMap::new(), &StreamOptions::new(1))
        .await
        .unwrap();
    let _c1 = conn_to
        .next_stream_chunk(&mut to_cursor)
        .await
        .unwrap()
        .unwrap();
    let cancelled = tokio::time::timeout(
        std::time::Duration::from_millis(20),
        conn_to.next_stream_chunk(&mut to_cursor),
    )
    .await;
    assert!(cancelled.is_err(), "chunk 2 should time out");
    let close_after_cancel = conn_to.close_stream(&mut to_cursor).await;
    assert!(matches!(close_after_cancel, Err(NetError::Closed(_))));
    assert!(!conn_to.is_valid());
    assert!(!to_cursor.is_wire_clean());
    to_srv.abort();
}

#[tokio::test]
async fn test_bolt_stream_prefetch_early_close_and_drop() {
    use voyager_drivers::bolt::BoltStubServer;
    use voyager_drivers::config::PoolConfig;
    use voyager_drivers::{NativeClient, StreamOptions};

    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(
        &stub.uri(),
        Some(PoolConfig::new().with_min_idle(0).with_max_size(1)),
    )
    .unwrap();

    // 1. Prefetch + early close when prefetched chunk still has_more=true:
    //    close() drains the in-flight chunk 2 and sends exactly 1 DISCARD, reusing the conn.
    let mut s1 = client
        .stream("STREAM ROWS 500", &HashMap::new(), StreamOptions::new(100))
        .await
        .unwrap();
    let c1 = s1.next_chunk().await.unwrap().expect("chunk 1");
    assert_eq!(c1.num_rows(), 100);
    assert_eq!(stub.discard_count(), 0);
    s1.close().await.unwrap();
    assert_eq!(stub.pull_count(), 2);
    assert_eq!(stub.discard_count(), 1);
    assert_eq!(stub.connection_count(), 1);
    assert_eq!(client.pool().idle_count(), 1);

    // 2. Prefetch + early close when prefetched chunk IS the final chunk (has_more=false):
    //    close() drains chunk 2, sees has_more=false, sends 0 DISCARDs, and reuses the conn.
    let mut s2 = client
        .stream("STREAM ROWS 150", &HashMap::new(), StreamOptions::new(100))
        .await
        .unwrap();
    let c2 = s2.next_chunk().await.unwrap().expect("chunk 1");
    assert_eq!(c2.num_rows(), 100);
    s2.close().await.unwrap();
    assert_eq!(stub.pull_count(), 4);
    assert_eq!(stub.discard_count(), 1);
    assert_eq!(stub.connection_count(), 1);

    // Follow-up query reuses the same connection without RESET
    let ok = client
        .execute_rows("RETURN 1", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(ok.row_count(), 2);
    assert_eq!(stub.connection_count(), 1);
    assert_eq!(stub.reset_count(), 0);

    // 3. Prefetch + Drop without close(): marks connection broken so next query opens conn #2.
    {
        let mut s3 = client
            .stream("STREAM ROWS 500", &HashMap::new(), StreamOptions::new(100))
            .await
            .unwrap();
        let c3 = s3.next_chunk().await.unwrap().expect("chunk 1");
        assert_eq!(c3.num_rows(), 100);
    }
    let after_drop = client
        .execute_rows("RETURN 2", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(after_drop.row_count(), 2);
    assert_eq!(stub.connection_count(), 2);

    client.close().await;
}
