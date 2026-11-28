//! In-memory mock TCP stub server for Bolt protocol compliance and TestKit simulation.

use bytes::BytesMut;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::connection::BOLT_MAGIC_PREAMBLE;
use super::messages::BoltResponse;
use super::packstream::{BoltValue, PackStream};
use super::stream::{encode_chunks, read_message_frame, write_message_frame};
use crate::error::{NetError, Result};

#[derive(Clone)]
struct StubCounters {
    reset_count: Arc<AtomicUsize>,
    run_count: Arc<AtomicUsize>,
    pull_count: Arc<AtomicUsize>,
    discard_count: Arc<AtomicUsize>,
    begin_count: Arc<AtomicUsize>,
    commit_count: Arc<AtomicUsize>,
    hello_count: Arc<AtomicUsize>,
}

/// Scripted mock Bolt server simulating database responses and error injection over raw TCP.
pub struct BoltStubServer {
    addr: SocketAddr,
    server_task: JoinHandle<()>,
    counters: StubCounters,
    connection_count: Arc<AtomicUsize>,
}

impl BoltStubServer {
    /// Starts a mock Bolt server on an ephemeral local port.
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| {
            NetError::ConnectionFailed(format!("Failed to bind stub server: {}", e))
        })?;
        let addr = listener
            .local_addr()
            .map_err(|e| NetError::ConnectionFailed(format!("Failed to get local addr: {}", e)))?;

        let counters = StubCounters {
            reset_count: Arc::new(AtomicUsize::new(0)),
            run_count: Arc::new(AtomicUsize::new(0)),
            pull_count: Arc::new(AtomicUsize::new(0)),
            discard_count: Arc::new(AtomicUsize::new(0)),
            begin_count: Arc::new(AtomicUsize::new(0)),
            commit_count: Arc::new(AtomicUsize::new(0)),
            hello_count: Arc::new(AtomicUsize::new(0)),
        };
        let connection_count = Arc::new(AtomicUsize::new(0));
        let counters_clone = counters.clone();
        let connection_clone = connection_count.clone();

        let server_task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                connection_clone.fetch_add(1, Ordering::SeqCst);
                // Without TCP_NODELAY the scripted responses hit Linux delayed
                // ACK, adding ~41 ms to every loopback test query.
                let _ = socket.set_nodelay(true);
                let conn_counters = counters_clone.clone();
                tokio::spawn(async move {
                    let _ = Self::handle_connection(&mut socket, conn_counters).await;
                });
            }
        });

        Ok(Self {
            addr,
            server_task,
            counters,
            connection_count,
        })
    }

    /// Returns the total number of Bolt `RESET` messages received by this stub server.
    pub fn reset_count(&self) -> usize {
        self.counters.reset_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of Bolt `RUN` messages received by this stub server.
    pub fn run_count(&self) -> usize {
        self.counters.run_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of Bolt `PULL` messages received by this stub server.
    pub fn pull_count(&self) -> usize {
        self.counters.pull_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of Bolt `DISCARD` messages received by this stub server.
    pub fn discard_count(&self) -> usize {
        self.counters.discard_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of Bolt `BEGIN` messages received by this stub server.
    pub fn begin_count(&self) -> usize {
        self.counters.begin_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of Bolt `COMMIT` messages received by this stub server.
    pub fn commit_count(&self) -> usize {
        self.counters.commit_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of TCP connections accepted by this stub server.
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::SeqCst)
    }

    /// Returns the total number of Bolt `HELLO` messages received by this stub server.
    pub fn hello_count(&self) -> usize {
        self.counters.hello_count.load(Ordering::SeqCst)
    }

    /// Returns the socket address `127.0.0.1:port` of the running stub server.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Returns the Bolt URI for this stub server (`bolt://127.0.0.1:port`).
    pub fn uri(&self) -> String {
        format!("bolt://{}", self.addr)
    }

    fn parse_query_directive(query: &str, keyword: &str) -> Option<usize> {
        let idx = query.find(keyword)?;
        let rest = &query[idx + keyword.len()..];
        let token = rest.split_whitespace().next()?;
        token.parse::<usize>().ok()
    }

    async fn handle_connection(socket: &mut TcpStream, counters: StubCounters) -> Result<()> {
        // 1. Read 20-byte Handshake
        let mut handshake = [0u8; 20];
        socket
            .read_exact(&mut handshake)
            .await
            .map_err(|e| NetError::ConnectionFailed(e.to_string()))?;

        if handshake[0..4] != BOLT_MAGIC_PREAMBLE {
            return Err(NetError::ProtocolError(
                "Invalid magic preamble".to_string(),
            ));
        }

        // Respond with agreed version Bolt v5.4 [0x00, 0x00, 0x04, 0x05]
        socket
            .write_all(&[0x00, 0x00, 0x04, 0x05])
            .await
            .map_err(|e| NetError::ConnectionFailed(e.to_string()))?;
        socket
            .flush()
            .await
            .map_err(|e| NetError::ConnectionFailed(e.to_string()))?;

        // 2. Read HELLO
        let mut frame = read_message_frame(socket).await?;
        let _hello_val = PackStream::decode(&mut frame)?;
        counters.hello_count.fetch_add(1, Ordering::SeqCst);

        // Reply SUCCESS to HELLO
        let mut hello_meta = HashMap::new();
        hello_meta.insert(
            "server".to_string(),
            BoltValue::String("Neo4j/5.15.0".to_string()),
        );
        hello_meta.insert(
            "connection_id".to_string(),
            BoltValue::String("bolt-1".to_string()),
        );
        Self::send_response(
            socket,
            &BoltResponse::Success {
                metadata: hello_meta,
            },
        )
        .await?;

        let mut in_failed_state = false;
        let mut in_transaction = false;
        let mut next_qid: i64 = 0;
        let mut active_qid: Option<i64> = None;
        let mut first_pull_pending = false;
        let mut total_rows: usize = 2;
        let mut cursor_pos: usize = 0;
        let mut use_default_two_rows = true;
        let mut fail_after_row: Option<usize> = None;
        let mut wire_buf = BytesMut::with_capacity(4096);
        let mut payload_buf = BytesMut::with_capacity(512);

        // 3. Message loop
        loop {
            let mut msg_frame = match read_message_frame(socket).await {
                Ok(f) => f,
                Err(_) => break, // Client disconnected
            };

            let req_val = match PackStream::decode(&mut msg_frame) {
                Ok(v) => v,
                Err(_) => break,
            };

            if let BoltValue::Structure { tag, fields } = req_val {
                if in_failed_state && tag != 0x0F && tag != 0x02 {
                    // When in failed state, all subsequent pipelined requests (e.g. PULL) receive IGNORED
                    Self::send_response(
                        socket,
                        &BoltResponse::Ignored {
                            metadata: HashMap::new(),
                        },
                    )
                    .await?;
                    continue;
                }

                match tag {
                    0x11 => {
                        // BEGIN request
                        counters.begin_count.fetch_add(1, Ordering::SeqCst);
                        in_transaction = true;
                        next_qid = 0;
                        active_qid = None;
                        Self::send_response(
                            socket,
                            &BoltResponse::Success {
                                metadata: HashMap::new(),
                            },
                        )
                        .await?;
                    }
                    0x12 => {
                        // COMMIT request
                        counters.commit_count.fetch_add(1, Ordering::SeqCst);
                        in_transaction = false;
                        active_qid = None;
                        first_pull_pending = false;
                        let mut commit_meta = HashMap::new();
                        commit_meta.insert(
                            "bookmark".to_string(),
                            BoltValue::String("stub:bookmark-1".to_string()),
                        );
                        Self::send_response(
                            socket,
                            &BoltResponse::Success {
                                metadata: commit_meta,
                            },
                        )
                        .await?;
                    }
                    0x13 => {
                        // ROLLBACK request
                        in_transaction = false;
                        active_qid = None;
                        first_pull_pending = false;
                        Self::send_response(
                            socket,
                            &BoltResponse::Success {
                                metadata: HashMap::new(),
                            },
                        )
                        .await?;
                    }
                    0x10 => {
                        // RUN request
                        counters.run_count.fetch_add(1, Ordering::SeqCst);
                        let query = fields.first().and_then(|v| v.as_str()).unwrap_or("");
                        let mut run_meta = HashMap::new();

                        let fail_mid = Self::parse_query_directive(query, "FAIL_MID_STREAM ");
                        if fail_mid.is_none() && query.contains("FAIL") {
                            in_failed_state = true;
                            active_qid = None;
                            first_pull_pending = false;
                            let mut fail_meta = HashMap::new();
                            fail_meta.insert(
                                "code".to_string(),
                                BoltValue::String(
                                    "Neo.ClientError.Statement.SyntaxError".to_string(),
                                ),
                            );
                            fail_meta.insert(
                                "message".to_string(),
                                BoltValue::String("Simulated query execution failure".to_string()),
                            );
                            Self::send_response(
                                socket,
                                &BoltResponse::Failure {
                                    metadata: fail_meta,
                                },
                            )
                            .await?;
                        } else {
                            if let Some(rows_n) = Self::parse_query_directive(query, "ROWS ") {
                                total_rows = rows_n;
                                use_default_two_rows = false;
                            } else if fail_mid.is_some() {
                                total_rows = 500;
                                use_default_two_rows = false;
                            } else {
                                total_rows = 2;
                                use_default_two_rows = true;
                            }
                            cursor_pos = 0;
                            fail_after_row = fail_mid;
                            first_pull_pending = true;

                            let fields_list = vec![
                                BoltValue::String("name".to_string()),
                                BoltValue::String("age".to_string()),
                            ];
                            run_meta.insert("fields".to_string(), BoltValue::List(fields_list));
                            run_meta.insert("t_first".to_string(), BoltValue::Integer(1));
                            if in_transaction {
                                let qid = next_qid;
                                next_qid += 1;
                                active_qid = Some(qid);
                                run_meta.insert("qid".to_string(), BoltValue::Integer(qid));
                            } else {
                                active_qid = None;
                            }
                            Self::send_response(
                                socket,
                                &BoltResponse::Success { metadata: run_meta },
                            )
                            .await?;
                        }
                    }
                    0x3F => {
                        // PULL request
                        counters.pull_count.fetch_add(1, Ordering::SeqCst);
                        let extra = fields.first().and_then(|v| match v {
                            BoltValue::Map(m) => Some(m),
                            _ => None,
                        });
                        let req_n = extra
                            .and_then(|m| m.get("n"))
                            .and_then(|v| match v {
                                BoltValue::Integer(n) => Some(*n),
                                _ => None,
                            })
                            .unwrap_or(-1);
                        let req_qid = extra.and_then(|m| m.get("qid")).and_then(|v| match v {
                            BoltValue::Integer(q) => Some(*q),
                            _ => None,
                        });

                        let is_first_pull = first_pull_pending;
                        first_pull_pending = false;
                        if in_transaction
                            && let Some(expected_qid) = active_qid
                            && !is_first_pull
                            && req_qid != Some(expected_qid)
                        {
                            in_failed_state = true;
                            active_qid = None;
                            let mut fail_meta = HashMap::new();
                            fail_meta.insert(
                                "code".to_string(),
                                BoltValue::String("Neo.ClientError.Request.Invalid".to_string()),
                            );
                            fail_meta.insert(
                                "message".to_string(),
                                BoltValue::String(format!(
                                    "Expected qid {expected_qid}, got {req_qid:?}"
                                )),
                            );
                            Self::send_response(
                                socket,
                                &BoltResponse::Failure {
                                    metadata: fail_meta,
                                },
                            )
                            .await?;
                            continue;
                        }

                        let remaining = total_rows.saturating_sub(cursor_pos);
                        let to_send = if req_n < 0 {
                            remaining
                        } else {
                            (req_n as usize).min(remaining)
                        };

                        let mut failed_mid = false;
                        let actual_send = if let Some(limit) = fail_after_row
                            && cursor_pos + to_send >= limit
                        {
                            failed_mid = true;
                            limit.saturating_sub(cursor_pos)
                        } else {
                            to_send
                        };

                        wire_buf.clear();
                        for offset in 0..actual_send {
                            let idx = cursor_pos + offset;
                            let rec = if use_default_two_rows && idx < 2 {
                                if idx == 0 {
                                    vec![
                                        BoltValue::String("Alice".to_string()),
                                        BoltValue::Integer(30),
                                    ]
                                } else {
                                    vec![
                                        BoltValue::String("Bob".to_string()),
                                        BoltValue::Integer(25),
                                    ]
                                }
                            } else {
                                vec![
                                    BoltValue::String(format!("row_{idx}")),
                                    BoltValue::Integer(idx as i64),
                                ]
                            };
                            Self::encode_response_into(
                                &BoltResponse::Record { fields: rec },
                                &mut payload_buf,
                                &mut wire_buf,
                            );
                        }
                        cursor_pos += actual_send;

                        if failed_mid {
                            in_failed_state = true;
                            active_qid = None;
                            let mut fail_meta = HashMap::new();
                            fail_meta.insert(
                                "code".to_string(),
                                BoltValue::String(
                                    "Neo.ClientError.Statement.ArithmeticError".to_string(),
                                ),
                            );
                            fail_meta.insert(
                                "message".to_string(),
                                BoltValue::String("/ by zero".to_string()),
                            );
                            Self::encode_response_into(
                                &BoltResponse::Failure {
                                    metadata: fail_meta,
                                },
                                &mut payload_buf,
                                &mut wire_buf,
                            );
                        } else {
                            let has_more = cursor_pos < total_rows;
                            let mut pull_meta = HashMap::new();
                            pull_meta.insert("has_more".to_string(), BoltValue::Boolean(has_more));
                            if !has_more {
                                active_qid = None;
                                let mut stats_map = HashMap::new();
                                stats_map
                                    .insert("nodes-created".to_string(), BoltValue::Integer(0));
                                pull_meta.insert("stats".to_string(), BoltValue::Map(stats_map));
                                pull_meta.insert("t_last".to_string(), BoltValue::Integer(2));
                            }
                            Self::encode_response_into(
                                &BoltResponse::Success {
                                    metadata: pull_meta,
                                },
                                &mut payload_buf,
                                &mut wire_buf,
                            );
                        }
                        write_message_frame(socket, &wire_buf).await?;
                    }
                    0x2F => {
                        // DISCARD request
                        counters.discard_count.fetch_add(1, Ordering::SeqCst);
                        let extra = fields.first().and_then(|v| match v {
                            BoltValue::Map(m) => Some(m),
                            _ => None,
                        });
                        let req_qid = extra.and_then(|m| m.get("qid")).and_then(|v| match v {
                            BoltValue::Integer(q) => Some(*q),
                            _ => None,
                        });
                        if in_transaction
                            && let Some(expected_qid) = active_qid
                            && req_qid != Some(expected_qid)
                        {
                            in_failed_state = true;
                            active_qid = None;
                            let mut fail_meta = HashMap::new();
                            fail_meta.insert(
                                "code".to_string(),
                                BoltValue::String("Neo.ClientError.Request.Invalid".to_string()),
                            );
                            fail_meta.insert(
                                "message".to_string(),
                                BoltValue::String(format!(
                                    "Expected qid {expected_qid} on DISCARD, got {req_qid:?}"
                                )),
                            );
                            Self::send_response(
                                socket,
                                &BoltResponse::Failure {
                                    metadata: fail_meta,
                                },
                            )
                            .await?;
                            continue;
                        }
                        cursor_pos = total_rows;
                        active_qid = None;
                        first_pull_pending = false;
                        let mut discard_meta = HashMap::new();
                        let mut stats_map = HashMap::new();
                        stats_map.insert("nodes-created".to_string(), BoltValue::Integer(0));
                        discard_meta.insert("stats".to_string(), BoltValue::Map(stats_map));
                        discard_meta.insert("t_last".to_string(), BoltValue::Integer(2));
                        Self::send_response(
                            socket,
                            &BoltResponse::Success {
                                metadata: discard_meta,
                            },
                        )
                        .await?;
                    }
                    0x0F => {
                        // RESET request -> clear failed state and reply SUCCESS
                        counters.reset_count.fetch_add(1, Ordering::SeqCst);
                        in_failed_state = false;
                        in_transaction = false;
                        active_qid = None;
                        first_pull_pending = false;
                        cursor_pos = total_rows;
                        fail_after_row = None;
                        Self::send_response(
                            socket,
                            &BoltResponse::Success {
                                metadata: HashMap::new(),
                            },
                        )
                        .await?;
                    }
                    0x02 => {
                        // GOODBYE -> terminate connection
                        break;
                    }
                    _ => {
                        // Other messages -> reply SUCCESS
                        Self::send_response(
                            socket,
                            &BoltResponse::Success {
                                metadata: HashMap::new(),
                            },
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }

    fn encode_response_into(resp: &BoltResponse, payload: &mut BytesMut, chunk_buf: &mut BytesMut) {
        let val = match resp {
            BoltResponse::Success { metadata } => BoltValue::Structure {
                tag: 0x70,
                fields: vec![BoltValue::Map(metadata.clone())],
            },
            BoltResponse::Record { fields } => BoltValue::Structure {
                tag: 0x71,
                fields: vec![BoltValue::List(fields.clone())],
            },
            BoltResponse::Failure { metadata } => BoltValue::Structure {
                tag: 0x7F,
                fields: vec![BoltValue::Map(metadata.clone())],
            },
            BoltResponse::Ignored { metadata } => BoltValue::Structure {
                tag: 0x7E,
                fields: vec![BoltValue::Map(metadata.clone())],
            },
        };

        payload.clear();
        PackStream::encode(&val, payload);
        encode_chunks(payload, chunk_buf);
    }

    async fn send_response(socket: &mut TcpStream, resp: &BoltResponse) -> Result<()> {
        let mut payload = BytesMut::new();
        let mut chunk_buf = BytesMut::new();
        Self::encode_response_into(resp, &mut payload, &mut chunk_buf);
        write_message_frame(socket, &chunk_buf).await
    }
}

impl Drop for BoltStubServer {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}
