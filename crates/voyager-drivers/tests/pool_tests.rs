//! Comprehensive unit and integration tests for voyager-drivers generic connection pool.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use voyager_drivers::config::PoolConfig;
use voyager_drivers::engine::{AsyncConnection, AsyncEngine};
use voyager_drivers::mock::{MockConnectionFactory, MockEngine};
use voyager_drivers::pool::ConnectionPool;

#[tokio::test]
async fn test_pool_warm_up_and_acquire() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new().with_min_idle(3).with_max_size(5);
    let pool = ConnectionPool::new(config, factory.clone());

    pool.warm_up().await.expect("Failed to warm up pool");
    assert_eq!(pool.idle_count(), 3);
    assert_eq!(pool.total_count(), 3);
    assert_eq!(pool.active_count(), 0);
    assert_eq!(factory.created_count(), 3);

    // Acquire one connection
    {
        let conn = pool.acquire().await.expect("Failed to acquire connection");
        assert_eq!(pool.idle_count(), 2);
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.total_count(), 3);
        assert_eq!(factory.created_count(), 3); // Reused from idle queue, no new creation
        assert!(conn.is_valid());
    }

    // Connection returned to idle queue on drop
    assert_eq!(pool.idle_count(), 3);
    assert_eq!(pool.active_count(), 0);
}

#[tokio::test]
async fn test_pool_concurrency_and_exhaustion() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new()
        .with_max_size(2)
        .with_acquire_timeout(Duration::from_millis(50));
    let pool = ConnectionPool::new(config, factory.clone());

    // Lease max capacity (2 connections)
    let conn1 = pool.acquire().await.expect("Failed to acquire conn1");
    let conn2 = pool.acquire().await.expect("Failed to acquire conn2");

    assert_eq!(pool.active_count(), 2);
    assert_eq!(pool.idle_count(), 0);

    // 3rd acquire must timeout
    let err = pool.acquire().await.unwrap_err();
    match err {
        voyager_drivers::NetError::PoolExhausted(msg) => {
            assert!(msg.contains("Timed out waiting for connection"));
        }
        other => panic!("Expected PoolExhausted error, got: {:?}", other),
    }

    // Drop conn1 to free a slot
    drop(conn1);
    assert_eq!(pool.idle_count(), 1);
    assert_eq!(pool.active_count(), 1);

    // Now 3rd acquire succeeds
    let conn3 = pool
        .acquire()
        .await
        .expect("Failed to acquire conn3 after drop");
    assert_eq!(pool.active_count(), 2);

    drop(conn2);
    drop(conn3);
    assert_eq!(pool.idle_count(), 2);
    assert_eq!(pool.active_count(), 0);
}

#[tokio::test]
async fn test_pool_broken_connection_recycling() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new().with_max_size(3);
    let pool = ConnectionPool::new(config, factory.clone());

    // Acquire connection and mark it broken
    {
        let mut conn = pool.acquire().await.expect("Failed to acquire connection");
        assert_eq!(pool.total_count(), 1);
        conn.mark_broken();
    }

    // Since connection was broken, it should not be returned to idle queue, total count decremented
    assert_eq!(pool.idle_count(), 0);
    assert_eq!(pool.total_count(), 0);
    assert_eq!(pool.active_count(), 0);

    // Subsequent acquire should create a fresh connection
    {
        let _conn = pool
            .acquire()
            .await
            .expect("Failed to acquire fresh connection");
        assert_eq!(pool.total_count(), 1);
        assert_eq!(factory.created_count(), 2);
    }
    assert_eq!(pool.idle_count(), 1);
}

#[tokio::test]
async fn test_pool_liveness_probe_failure() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new()
        .with_max_size(2)
        .with_test_on_acquire(true);
    let pool = ConnectionPool::new(config, factory.clone());

    // Acquire and return healthy connection
    {
        let mut conn = pool.acquire().await.expect("Failed to acquire");
        // Simulate connection failing silently after return
        conn.fail_ping = true;
    }
    assert_eq!(pool.idle_count(), 1);

    // Next acquire will trigger liveness probe, fail, discard it, and create a fresh one
    {
        let conn = pool
            .acquire()
            .await
            .expect("Failed to acquire after failed probe");
        assert!(!conn.fail_ping);
        assert_eq!(factory.created_count(), 2);
    }
}

#[tokio::test]
async fn test_pool_idle_and_lifetime_eviction() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new()
        .with_max_size(5)
        .with_idle_timeout(Some(Duration::from_millis(20)))
        .with_max_lifetime(Some(Duration::from_millis(50)));
    let pool = ConnectionPool::new(config, factory.clone());

    // Create 2 idle connections
    pool.warm_up().await.expect("Warmup failed");
    assert_eq!(pool.idle_count(), 1); // min_idle default is 1

    // Sleep longer than idle timeout
    sleep(Duration::from_millis(30)).await;

    // Run eviction sweep
    pool.evict_expired().await;
    assert_eq!(pool.idle_count(), 0);
    assert_eq!(pool.total_count(), 0);
    assert_eq!(pool.metrics().connections_evicted, 1);
}

#[tokio::test]
async fn test_mock_engine_end_to_end() {
    let engine = MockEngine::new();

    let params = HashMap::new();
    let result = engine
        .execute("MATCH (n:Person) RETURN n.name", &params)
        .await
        .expect("Query execution failed");

    assert_eq!(result.row_count(), 0);
    assert!(!result.summary.is_mutating());

    engine.ping().await.expect("Engine ping failed");
}

#[tokio::test]
async fn test_pool_async_tx_rollback_on_drop() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new().with_max_size(2);
    let pool = ConnectionPool::new(config, factory.clone());

    let reset_counter;
    {
        let mut conn = pool.acquire().await.expect("Acquire failed");
        conn.in_transaction = true;
        reset_counter = conn.reset_count.clone();
        assert_eq!(reset_counter.load(std::sync::atomic::Ordering::SeqCst), 0);
        // conn drops here while in_transaction is true
    }

    // Give background tokio::spawn task time to run reset()
    sleep(Duration::from_millis(50)).await;

    // Verify reset() was invoked immediately in the background
    assert_eq!(reset_counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(pool.idle_count(), 1);

    // Re-acquire connection; verify transaction state was cleanly rolled back
    let conn2 = pool.acquire().await.expect("Re-acquire failed");
    assert!(!conn2.is_in_transaction());
}

#[tokio::test]
async fn test_pool_direct_waiter_handover() {
    let factory = Arc::new(MockConnectionFactory::new());
    // Pool capacity 1
    let config = PoolConfig::new()
        .with_max_size(1)
        .with_acquire_timeout(Duration::from_millis(500));
    let pool = ConnectionPool::new(config, factory.clone());

    let conn1 = pool.acquire().await.expect("First acquire failed");
    assert_eq!(pool.active_count(), 1);
    assert_eq!(pool.idle_count(), 0);

    let pool_clone = pool.clone();
    let waiter_handle = tokio::spawn(async move {
        // This will block waiting in the saturated direct waiter queue
        let conn_received = pool_clone.acquire().await.expect("Waiter acquire failed");
        assert!(conn_received.is_valid());
        // Verify no extra connection was created
        conn_received
    });

    // Small delay to ensure waiter task has entered the waiter queue
    sleep(Duration::from_millis(20)).await;

    // Drop conn1: this triggers direct handover to the waiter!
    drop(conn1);

    let conn_from_waiter = waiter_handle.await.expect("Waiter task panicked");
    assert_eq!(pool.active_count(), 1);
    assert_eq!(factory.created_count(), 1); // Exactly 1 connection created; handed over directly

    drop(conn_from_waiter);
    assert_eq!(pool.active_count(), 0);
    assert_eq!(pool.idle_count(), 1);
}

#[tokio::test]
async fn test_pool_lifo_reuse_and_oldest_idle_eviction() {
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use voyager_drivers::engine::{ConnectionFactory, QueryResult, QuerySummary};
    use voyager_drivers::{NetError, ParamValue};

    struct TaggedConn {
        id: usize,
        closed_ids: Arc<parking_lot::Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl AsyncConnection for TaggedConn {
        async fn execute(
            &mut self,
            _query: &str,
            _params: &HashMap<String, ParamValue>,
        ) -> voyager_drivers::Result<QueryResult> {
            Ok(QueryResult::empty(QuerySummary::default()))
        }
        async fn ping(&mut self) -> voyager_drivers::Result<()> {
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
            self.closed_ids.lock().push(self.id);
            Ok(())
        }
    }

    struct TaggedFactory {
        next_id: AtomicUsize,
        closed_ids: Arc<parking_lot::Mutex<Vec<usize>>>,
    }

    #[async_trait]
    impl ConnectionFactory<TaggedConn> for TaggedFactory {
        async fn create(&self) -> std::result::Result<TaggedConn, NetError> {
            let id = self.next_id.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(TaggedConn {
                id,
                closed_ids: self.closed_ids.clone(),
            })
        }
    }

    let closed_ids = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let factory = Arc::new(TaggedFactory {
        next_id: AtomicUsize::new(0),
        closed_ids: closed_ids.clone(),
    });
    let config = PoolConfig::new()
        .with_max_size(4)
        .with_idle_timeout(Some(Duration::from_millis(45)));
    let pool = ConnectionPool::new(config, factory);

    let c1 = pool.acquire().await.unwrap();
    let c2 = pool.acquire().await.unwrap();
    assert_eq!(c1.id, 1);
    assert_eq!(c2.id, 2);

    // Return c1 first, then c2 -> c2 is most recently returned (LIFO head).
    drop(c1);
    drop(c2);

    let reused = pool.acquire().await.unwrap();
    assert_eq!(
        reused.id, 2,
        "LIFO pool must hand out most recently returned connection"
    );
    drop(reused);

    // Keep c2 warm across c1's idle_timeout; oldest c1 at the front must be evicted.
    for _ in 0..4 {
        sleep(Duration::from_millis(16)).await;
        let conn = pool.acquire().await.unwrap();
        assert_eq!(conn.id, 2);
        drop(conn);
    }

    assert_eq!(closed_ids.lock().as_slice(), &[1]);
    assert_eq!(pool.total_count(), 1);
    assert_eq!(pool.metrics().connections_evicted, 1);
}

#[tokio::test]
async fn test_pool_idle_threshold_pinging_and_opt_in() {
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new()
        .with_max_size(2)
        .with_test_on_idle_after(Some(Duration::from_millis(40)));
    let pool = ConnectionPool::new(config, factory);

    let ping_counter = {
        let conn = pool.acquire().await.unwrap();
        conn.ping_count.clone()
    };
    assert_eq!(ping_counter.load(std::sync::atomic::Ordering::SeqCst), 0);

    // Reuse within threshold: zero pings.
    for _ in 0..5 {
        let conn = pool.acquire().await.unwrap();
        assert_eq!(ping_counter.load(std::sync::atomic::Ordering::SeqCst), 0);
        drop(conn);
    }

    // Sleep past threshold: triggers one ping.
    sleep(Duration::from_millis(55)).await;
    {
        let _conn = pool.acquire().await.unwrap();
        assert_eq!(ping_counter.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    // Opt-in test_on_acquire=true pings on every acquire even immediately.
    let factory2 = Arc::new(MockConnectionFactory::new());
    let opt_in_cfg = PoolConfig::new()
        .with_max_size(2)
        .with_test_on_acquire(true)
        .with_test_on_idle_after(Some(Duration::from_secs(30)));
    let pool2 = ConnectionPool::new(opt_in_cfg, factory2);
    let ping_counter2 = {
        let conn = pool2.acquire().await.unwrap();
        conn.ping_count.clone()
    };
    for expected in 1..=3 {
        let _conn = pool2.acquire().await.unwrap();
        assert_eq!(
            ping_counter2.load(std::sync::atomic::Ordering::SeqCst),
            expected
        );
    }
}

#[tokio::test]
async fn test_bolt_stub_zero_ping_within_threshold_and_single_probe_ping() {
    use voyager_drivers::{BoltStubServer, NativeClient};

    let stub = BoltStubServer::start().await.expect("stub server");

    // 1. Default config: no RESET sent when reusing within 30s threshold.
    let client = NativeClient::new(
        &stub.uri(),
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_test_on_idle_after(Some(Duration::from_millis(60))),
        ),
    )
    .expect("client");

    for _ in 0..6 {
        client
            .execute("RETURN 1", &HashMap::new())
            .await
            .expect("query");
    }
    assert_eq!(stub.run_count(), 6);
    assert_eq!(
        stub.reset_count(),
        0,
        "reused connection within idle threshold must send 0 RESET round-trips"
    );

    // Idle past threshold -> next query sends 1 RESET before RUN.
    sleep(Duration::from_millis(75)).await;
    client
        .execute("RETURN 1", &HashMap::new())
        .await
        .expect("query after idle");
    assert_eq!(stub.run_count(), 7);
    assert_eq!(stub.reset_count(), 1);

    // NativeClient::ping() sends exactly 1 RESET (not 2) within threshold.
    client.ping().await.expect("ping 1");
    assert_eq!(stub.reset_count(), 2);
    client.ping().await.expect("ping 2");
    assert_eq!(stub.reset_count(), 3);
    client.close().await;

    // 2. Even with test_on_acquire=true, NativeClient::ping() must send only 1 RESET (not 2).
    let stub2 = BoltStubServer::start().await.expect("stub2");
    let client2 = NativeClient::new(
        &stub2.uri(),
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_test_on_acquire(true),
        ),
    )
    .expect("client2");
    client2
        .execute("RETURN 1", &HashMap::new())
        .await
        .expect("warmup");
    assert_eq!(stub2.reset_count(), 0);
    client2.ping().await.expect("single-probe ping");
    assert_eq!(
        stub2.reset_count(),
        1,
        "NativeClient::ping() must send 1 probe even when test_on_acquire=true"
    );
    client2.close().await;
}

#[tokio::test]
async fn test_bolt_stub_failed_query_reuses_connection_without_reconnect() {
    use voyager_drivers::{BoltStubServer, NativeClient};

    let stub = BoltStubServer::start().await.expect("stub server");
    let client = NativeClient::new(
        &stub.uri(),
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_test_on_idle_after(Some(Duration::from_secs(30))),
        ),
    )
    .expect("client");

    // A failed query leaves a deferred RESET unread on the socket. Returning the
    // connection to the pool and checking it out again must consume that reply and
    // reuse the same socket instead of evicting it on the local liveness check.
    let error = client
        .execute("RETURN FAIL", &HashMap::new())
        .await
        .expect_err("stub should fail this query");
    assert!(!error.to_string().is_empty());

    let result = client
        .execute("RETURN 1 AS n", &HashMap::new())
        .await
        .expect("next query must reuse the connection");
    assert!(!result.columns.is_empty());

    assert_eq!(stub.connection_count(), 1, "must not open a new connection");
    assert_eq!(stub.hello_count(), 1, "must not re-handshake");
    assert_eq!(stub.run_count(), 2);
    assert_eq!(client.pool().total_count(), 1);
    let metrics = client.pool().metrics();
    assert_eq!(metrics.connections_created, 1);
    assert_eq!(
        metrics.connections_failed_health_check, 0,
        "the pending RESET reply is expected and must not count as a health failure"
    );
    client.close().await;
}

#[tokio::test]
async fn test_pool_handover_checks_max_lifetime_and_health() {
    // 1. Handover::Connection exceeding max_lifetime is rejected and replaced.
    let factory = Arc::new(MockConnectionFactory::new());
    let config = PoolConfig::new()
        .with_max_size(1)
        .with_max_lifetime(Some(Duration::from_millis(25)))
        .with_acquire_timeout(Duration::from_millis(500));
    let pool = ConnectionPool::new(config, factory.clone());

    let conn1 = pool.acquire().await.unwrap();
    let pool_clone = pool.clone();
    let waiter = tokio::spawn(async move { pool_clone.acquire().await });
    sleep(Duration::from_millis(35)).await;
    drop(conn1);

    let conn2 = waiter
        .await
        .unwrap()
        .expect("waiter should get replacement");
    assert_eq!(factory.created_count(), 2);
    assert_eq!(pool.metrics().connections_evicted, 1);
    assert_eq!(pool.total_count(), 1);
    drop(conn2);

    // 2. Handover::Connection failing local health check is rejected and replaced.
    let factory2 = Arc::new(MockConnectionFactory::new());
    let pool2 = ConnectionPool::new(
        PoolConfig::new()
            .with_max_size(1)
            .with_acquire_timeout(Duration::from_millis(500)),
        factory2.clone(),
    );
    let mut c1 = pool2.acquire().await.unwrap();
    c1.fail_local_check = true;
    let pool2_clone = pool2.clone();
    let waiter2 = tokio::spawn(async move { pool2_clone.acquire().await });
    sleep(Duration::from_millis(15)).await;
    drop(c1);

    let c2 = waiter2
        .await
        .unwrap()
        .expect("waiter should replace unhealthy handover");
    assert!(!c2.fail_local_check);
    assert_eq!(factory2.created_count(), 2);
    assert_eq!(pool2.metrics().connections_failed_health_check, 1);
    assert_eq!(pool2.total_count(), 1);
}

#[tokio::test]
async fn test_pool_local_liveness_evicts_closed_and_dirty_loopback_sockets() {
    use bytes::BytesMut;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::NativeClient;
    use voyager_drivers::bolt::connection::BOLT_MAGIC_PREAMBLE;
    use voyager_drivers::bolt::messages::BoltResponse;
    use voyager_drivers::bolt::packstream::{BoltValue, PackStream};
    use voyager_drivers::bolt::stream::{encode_chunks, read_message_frame, write_message_frame};

    async fn send_resp(sock: &mut tokio::net::TcpStream, resp: &BoltResponse) {
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
        let mut chunk_buf = BytesMut::new();
        encode_chunks(&payload, &mut chunk_buf);
        let _ = write_message_frame(sock, &chunk_buf).await;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_seq = Arc::new(AtomicUsize::new(0));
    let conn_seq_srv = conn_seq.clone();
    let first_closed = Arc::new(tokio::sync::Notify::new());
    let first_closed_srv = first_closed.clone();
    let second_dirtied = Arc::new(tokio::sync::Notify::new());
    let second_dirtied_srv = second_dirtied.clone();
    let third_dropped_mid_query = Arc::new(tokio::sync::Notify::new());
    let third_dropped_srv = third_dropped_mid_query.clone();

    let server_task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let _ = sock.set_nodelay(true);
            let seq = conn_seq_srv.fetch_add(1, Ordering::SeqCst) + 1;
            let n1 = first_closed_srv.clone();
            let n2 = second_dirtied_srv.clone();
            let n3 = third_dropped_srv.clone();
            tokio::spawn(async move {
                let mut hs = [0u8; 20];
                if sock.read_exact(&mut hs).await.is_err() || hs[..4] != BOLT_MAGIC_PREAMBLE {
                    return;
                }
                let _ = sock.write_all(&[0x00, 0x00, 0x04, 0x05]).await;
                let _ = sock.flush().await;
                let Ok(mut hello) = read_message_frame(&mut sock).await else {
                    return;
                };
                let _ = PackStream::decode(&mut hello);
                send_resp(
                    &mut sock,
                    &BoltResponse::Success {
                        metadata: HashMap::new(),
                    },
                )
                .await;

                // Serve queries
                loop {
                    let Ok(mut frame) = read_message_frame(&mut sock).await else {
                        break;
                    };
                    let Ok(BoltValue::Structure { tag, .. }) = PackStream::decode(&mut frame)
                    else {
                        break;
                    };
                    match tag {
                        0x10 => {
                            if seq == 3 && n3_should_drop(&n3) {
                                // Drop right after client sent RUN (simulating slipped-through dead socket)
                                let _ = sock.shutdown().await;
                                drop(sock);
                                return;
                            }
                            let mut meta = HashMap::new();
                            meta.insert(
                                "fields".to_string(),
                                BoltValue::List(vec![BoltValue::String("v".to_string())]),
                            );
                            send_resp(&mut sock, &BoltResponse::Success { metadata: meta }).await;
                        }
                        0x3F => {
                            send_resp(
                                &mut sock,
                                &BoltResponse::Record {
                                    fields: vec![BoltValue::Integer(seq as i64)],
                                },
                            )
                            .await;
                            send_resp(
                                &mut sock,
                                &BoltResponse::Success {
                                    metadata: HashMap::new(),
                                },
                            )
                            .await;

                            if seq == 1 {
                                // Close socket #1 while idle in pool
                                let _ = sock.shutdown().await;
                                drop(sock);
                                n1.notify_one();
                                return;
                            } else if seq == 2 {
                                // Write unsolicited bytes onto idle socket #2
                                let _ = sock.write_all(b"UNEXPECTED").await;
                                let _ = sock.flush().await;
                                n2.notify_one();
                                // Keep socket alive briefly so FIN doesn't beat the dirty-data check
                                sleep(Duration::from_millis(200)).await;
                                return;
                            }
                        }
                        0x02 => break,
                        _ => {
                            send_resp(
                                &mut sock,
                                &BoltResponse::Success {
                                    metadata: HashMap::new(),
                                },
                            )
                            .await;
                        }
                    }
                }
            });
        }
    });

    fn n3_should_drop(notify: &Arc<tokio::sync::Notify>) -> bool {
        // Triggered on second RUN on conn #3? No, wait: we want conn #3 to succeed first,
        // wait until acquired again, and drop on the second RUN! Use a static/bool? No, handle in loop below.
        let _ = notify;
        false
    }

    // Use no ping at all (test_on_acquire=false, test_on_idle_after=None) to prove local check alone catches it
    let client = NativeClient::new(
        &format!("bolt://{addr}"),
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_test_on_acquire(false)
                .with_test_on_idle_after(None),
        ),
    )
    .expect("client");

    // 1. First query opens conn #1; server closes conn #1 immediately after sending SUCCESS
    let r1 = client.execute("RETURN 1", &HashMap::new()).await.unwrap();
    assert_eq!(r1.row_count(), 1);
    first_closed.notified().await;
    sleep(Duration::from_millis(10)).await;

    // 2. Second query must detect EOF locally, evict conn #1, open conn #2, and succeed
    let r2 = client.execute("RETURN 2", &HashMap::new()).await.unwrap();
    assert_eq!(r2.row_count(), 1);
    assert_eq!(conn_seq.load(Ordering::SeqCst), 2);
    assert_eq!(client.pool().metrics().connections_failed_health_check, 1);

    second_dirtied.notified().await;
    sleep(Duration::from_millis(10)).await;

    // 3. Third query must detect unexpected unread bytes on idle conn #2, evict it, open conn #3, and succeed
    let r3 = client.execute("RETURN 3", &HashMap::new()).await.unwrap();
    assert_eq!(r3.row_count(), 1);
    assert_eq!(conn_seq.load(Ordering::SeqCst), 3);
    assert_eq!(client.pool().metrics().connections_failed_health_check, 2);
    assert_eq!(client.pool().total_count(), 1);

    client.close().await;
    server_task.abort();
}

#[tokio::test]
async fn test_pool_slipped_through_dead_connection_returns_error_and_discards() {
    use bytes::BytesMut;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use voyager_drivers::NativeClient;
    use voyager_drivers::bolt::connection::BOLT_MAGIC_PREAMBLE;
    use voyager_drivers::bolt::messages::BoltResponse;
    use voyager_drivers::bolt::packstream::{BoltValue, PackStream};
    use voyager_drivers::bolt::stream::{encode_chunks, read_message_frame, write_message_frame};

    async fn send_resp(sock: &mut tokio::net::TcpStream, resp: &BoltResponse) {
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
        let mut chunk_buf = BytesMut::new();
        encode_chunks(&payload, &mut chunk_buf);
        let _ = write_message_frame(sock, &chunk_buf).await;
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let conn_seq = Arc::new(AtomicUsize::new(0));
    let conn_seq_srv = conn_seq.clone();

    let server_task = tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let _ = sock.set_nodelay(true);
            let seq = conn_seq_srv.fetch_add(1, Ordering::SeqCst) + 1;
            tokio::spawn(async move {
                let mut hs = [0u8; 20];
                if sock.read_exact(&mut hs).await.is_err() || hs[..4] != BOLT_MAGIC_PREAMBLE {
                    return;
                }
                let _ = sock.write_all(&[0x00, 0x00, 0x04, 0x05]).await;
                let _ = sock.flush().await;
                let Ok(mut hello) = read_message_frame(&mut sock).await else {
                    return;
                };
                let _ = PackStream::decode(&mut hello);
                send_resp(
                    &mut sock,
                    &BoltResponse::Success {
                        metadata: HashMap::new(),
                    },
                )
                .await;

                let mut query_num = 0usize;
                loop {
                    let Ok(mut frame) = read_message_frame(&mut sock).await else {
                        break;
                    };
                    let Ok(BoltValue::Structure { tag, .. }) = PackStream::decode(&mut frame)
                    else {
                        break;
                    };
                    match tag {
                        0x10 => {
                            query_num += 1;
                            if seq == 1 && query_num == 2 {
                                // Drop mid-query on 2nd query of conn #1 (after acquire already passed)
                                let _ = sock.shutdown().await;
                                drop(sock);
                                return;
                            }
                            let mut meta = HashMap::new();
                            meta.insert(
                                "fields".to_string(),
                                BoltValue::List(vec![BoltValue::String("v".to_string())]),
                            );
                            send_resp(&mut sock, &BoltResponse::Success { metadata: meta }).await;
                        }
                        0x3F => {
                            send_resp(
                                &mut sock,
                                &BoltResponse::Record {
                                    fields: vec![BoltValue::Integer(seq as i64)],
                                },
                            )
                            .await;
                            send_resp(
                                &mut sock,
                                &BoltResponse::Success {
                                    metadata: HashMap::new(),
                                },
                            )
                            .await;
                        }
                        0x02 => break,
                        _ => {
                            send_resp(
                                &mut sock,
                                &BoltResponse::Success {
                                    metadata: HashMap::new(),
                                },
                            )
                            .await;
                        }
                    }
                }
            });
        }
    });

    let client = NativeClient::new(
        &format!("bolt://{addr}"),
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_test_on_acquire(false)
                .with_test_on_idle_after(None),
        ),
    )
    .expect("client");

    // 1. Warm up conn #1
    client.execute("RETURN 1", &HashMap::new()).await.unwrap();
    assert_eq!(client.pool().total_count(), 1);

    // 2. Conn #1 passes acquire, then server drops mid-query: must return error (no retry) and discard conn #1
    let err = client
        .execute("WRITE_SHOULD_NOT_RETRY", &HashMap::new())
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("closed") || err.to_string().contains("Connection"),
        "unexpected error: {err}"
    );
    assert_eq!(conn_seq.load(Ordering::SeqCst), 1, "must NOT auto-retry");
    assert_eq!(client.pool().total_count(), 0);
    assert_eq!(client.pool().idle_count(), 0);

    // 3. Subsequent query creates fresh conn #2 and succeeds
    client.execute("RETURN 2", &HashMap::new()).await.unwrap();
    assert_eq!(conn_seq.load(Ordering::SeqCst), 2);
    assert_eq!(client.pool().total_count(), 1);

    client.close().await;
    server_task.abort();
}

#[tokio::test]
async fn test_pool_exhaustion_with_active_bolt_stream_and_buffered_immediate_release() {
    use voyager_drivers::mock::MockConnectionFactory;
    use voyager_drivers::pool::ConnectionPool;
    use voyager_drivers::{
        BoltStubServer, NativeClient, NetError, QueryResult, QuerySummary, ResultStream,
        ResultValue, StreamCursor, StreamOptions,
    };

    // 1. Server-streaming Bolt stream with max_size=1 holds the single permit until closed
    let stub = BoltStubServer::start().await.unwrap();
    let client = NativeClient::new(
        &stub.uri(),
        Some(
            PoolConfig::new()
                .with_min_idle(0)
                .with_max_size(1)
                .with_acquire_timeout(Duration::from_millis(100)),
        ),
    )
    .unwrap();

    let mut stream = client
        .stream("STREAM ROWS 500", &HashMap::new(), StreamOptions::new(100))
        .await
        .unwrap();
    let chunk1 = stream.next_chunk().await.unwrap().expect("chunk 1");
    assert_eq!(chunk1.num_rows(), 100);
    assert_eq!(client.pool().active_count(), 1);
    assert_eq!(client.pool().idle_count(), 0);

    let err_exec = client
        .execute_rows("RETURN 1", &HashMap::new())
        .await
        .unwrap_err();
    assert!(matches!(err_exec, NetError::PoolExhausted(_)));

    let err_stream = client
        .stream("RETURN 1", &HashMap::new(), StreamOptions::default())
        .await
        .err()
        .expect("second stream must fail with PoolExhausted");
    assert!(matches!(err_stream, NetError::PoolExhausted(_)));

    stream.close().await.unwrap();
    assert_eq!(client.pool().active_count(), 0);
    assert_eq!(client.pool().idle_count(), 1);

    let ok_res = client
        .execute_rows("RETURN 1", &HashMap::new())
        .await
        .unwrap();
    assert_eq!(ok_res.row_count(), 2);
    assert_eq!(stub.connection_count(), 1);
    client.close().await;

    // 2. Buffered cursor releases its pooled connection immediately on open_stream
    let factory = Arc::new(MockConnectionFactory::new());
    let pool = ConnectionPool::new(
        PoolConfig::new()
            .with_min_idle(0)
            .with_max_size(1)
            .with_acquire_timeout(Duration::from_millis(100)),
        factory,
    );
    let mut conn = pool.acquire().await.unwrap();
    conn.push_canned_result(Ok(QueryResult::from_rows(
        vec!["x".to_string()],
        vec![
            vec![ResultValue::Int(1)],
            vec![ResultValue::Int(2)],
            vec![ResultValue::Int(3)],
        ],
        Vec::new(),
        QuerySummary::empty(),
    )));
    let cursor = conn
        .open_stream("RETURN 1", &HashMap::new(), &StreamOptions::new(2))
        .await
        .unwrap();
    assert!(!cursor.is_server_streaming());
    drop(conn);
    assert_eq!(pool.active_count(), 0);
    assert_eq!(pool.idle_count(), 1);

    // Concurrent acquire succeeds immediately while buffered cursor hasn't been drained yet
    let other_conn = pool.acquire().await.unwrap();
    assert_eq!(pool.active_count(), 1);
    drop(other_conn);

    // And draining ResultStream<MockConnection> with None connection still yields all chunks
    let StreamCursor::Buffered(buf_cursor) = cursor else {
        panic!("expected Buffered cursor");
    };
    let mut buf_stream: ResultStream<voyager_drivers::mock::MockConnection> =
        ResultStream::from_buffered(buf_cursor);
    let c1 = buf_stream.next_chunk().await.unwrap().unwrap();
    let c2 = buf_stream.next_chunk().await.unwrap().unwrap();
    assert_eq!(c1.num_rows(), 2);
    assert_eq!(c2.num_rows(), 1);
    assert!(buf_stream.next_chunk().await.unwrap().is_none());
}
