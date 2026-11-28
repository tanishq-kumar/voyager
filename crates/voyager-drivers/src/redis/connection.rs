//! Connection implementation for Redis and FalkorDB graph engines.

use async_trait::async_trait;
use bytes::BytesMut;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;

use crate::config::Auth;
use crate::engine::{AsyncConnection, QueryResult, QuerySummary};
use crate::error::{NetError, Result};
use crate::params::ParamValue;
use crate::redis::codec::{read_resp_value, write_command};
use crate::redis::graph::{FalkorCompactResult, FalkorGraphSchema, FalkorQueryResult};
use crate::redis::resp::RespValue;
use crate::redis::transaction::{RedisTransaction, format_cypher_with_params};
use crate::tls::{NetStream, TlsOptions, TlsVerification};
use crate::uri::ParsedUri;

/// Connection to a Redis / FalkorDB server implementing [`AsyncConnection`].
pub struct RedisConnection {
    stream: NetStream,
    uri: ParsedUri,
    buffer: BytesMut,
    is_resp3: bool,
    server_info: HashMap<String, RespValue>,
    default_graph: String,
    graph_schemas: HashMap<String, Arc<FalkorGraphSchema>>,
    in_transaction: bool,
    is_valid: bool,
    query_timeout: Option<std::time::Duration>,
}

impl RedisConnection {
    /// Asynchronously establishes a new connection to Redis / FalkorDB based on the parsed URI.
    ///
    /// `rediss://`, `falkordbs://` and `valkeys://` (or `?tls=true`) connect over TLS with the
    /// server certificate verified; `?tls=insecure` accepts any certificate. TLS settings come
    /// from the URI parameters (see [`TlsOptions::from_params`]).
    pub async fn connect(uri: &ParsedUri) -> Result<Self> {
        Self::connect_with_tls(uri, &TlsOptions::new()).await
    }

    /// Like [`connect`](Self::connect), with TLS options that win over the URI parameters.
    pub async fn connect_with_tls(uri: &ParsedUri, tls_options: &TlsOptions) -> Result<Self> {
        if uri.tls != crate::config::TlsMode::Disabled && !crate::tls::TLS_AVAILABLE {
            return Err(crate::tls::tls_unavailable(uri.protocol, &uri.host));
        }
        let tls_options = tls_options.clone().or(TlsOptions::from_params(&uri.params));
        let timeout_secs = uri
            .params
            .get("connect_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5);
        let timeout_dur = std::time::Duration::from_secs(timeout_secs);
        match tokio::time::timeout(timeout_dur, Self::connect_raw(uri, &tls_options)).await {
            Ok(res) => res,
            Err(_) => Err(NetError::Timeout(format!(
                "Connection or handshake to Redis/FalkorDB host at {} timed out after {:?}",
                uri.socket_addr(),
                timeout_dur
            ))),
        }
    }

    async fn connect_raw(uri: &ParsedUri, tls_options: &TlsOptions) -> Result<Self> {
        let socket_addr = uri.socket_addr();
        let stream = TcpStream::connect(&socket_addr).await.map_err(|e| {
            NetError::ConnectionFailed(format!(
                "Failed to connect to Redis/FalkorDB host at {}: {}",
                socket_addr, e
            ))
        })?;

        // Disable Nagle's algorithm for low-latency graph query execution
        let _ = stream.set_nodelay(true);

        let stream = match TlsVerification::for_mode(uri.tls) {
            Some(verification) => {
                crate::tls::upgrade(stream, uri.protocol, &uri.host, verification, tls_options)
                    .await?
            }
            None => NetStream::Plain(stream),
        };

        // Resolve default graph name: from database segment, query params, or default
        let default_graph = if let Some(db) = &uri.database {
            db.clone()
        } else if let Some(graph) = uri.params.get("graph") {
            graph.clone()
        } else {
            "voyager_graph".to_string()
        };

        let query_timeout = uri
            .params
            .get("query_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .map(std::time::Duration::from_secs)
            .or(Some(std::time::Duration::from_secs(30)));

        let mut conn = Self {
            stream,
            uri: uri.clone(),
            buffer: BytesMut::with_capacity(8192),
            is_resp3: false,
            server_info: HashMap::new(),
            default_graph,
            graph_schemas: HashMap::new(),
            in_transaction: false,
            is_valid: true,
            query_timeout,
        };

        // Perform protocol handshake & authentication
        conn.handshake().await?;

        Ok(conn)
    }

    /// Performs the RESP handshake, protocol negotiation (RESP3), and authentication.
    async fn handshake(&mut self) -> Result<()> {
        let auth_credentials = match &self.uri.auth {
            Auth::Basic {
                username, password, ..
            } => Some((Some(username.as_str()), password.as_str())),
            Auth::Bearer { token } => Some((None, token.as_str())),
            Auth::None => None,
        };

        // 1. Try RESP3 handshake via HELLO 3
        let hello_result = match auth_credentials {
            Some((Some(user), pass)) => {
                write_command(&mut self.stream, &["HELLO", "3", "AUTH", user, pass]).await
            }
            Some((None, pass)) => {
                write_command(&mut self.stream, &["HELLO", "3", "AUTH", "default", pass]).await
            }
            None => write_command(&mut self.stream, &["HELLO", "3"]).await,
        };

        // The server must answer HELLO (a map, or an error from a server without RESP3);
        // a connection that closes or never answers is not a working session.
        hello_result.map_err(|e| {
            NetError::ConnectionFailed(format!(
                "Redis/FalkorDB host at {}: cannot send HELLO: {e}",
                self.uri.socket_addr()
            ))
        })?;
        let hello_reply = read_resp_value(&mut self.stream, &mut self.buffer)
            .await
            .map_err(|e| {
                NetError::ConnectionFailed(format!(
                    "Redis/FalkorDB host at {} did not answer HELLO: {e}",
                    self.uri.socket_addr()
                ))
            })?;
        let mut switched_to_resp3 = false;
        match hello_reply {
            RespValue::Map(pairs) => {
                self.is_resp3 = true;
                switched_to_resp3 = true;
                for (k, v) in pairs {
                    self.server_info.insert(k.to_string_lossy(), v);
                }
            }
            RespValue::Array(Some(items)) => {
                // Some servers reply with flat arrays to HELLO
                self.is_resp3 = true;
                switched_to_resp3 = true;
                for chunk in items.chunks(2) {
                    if chunk.len() == 2 {
                        self.server_info
                            .insert(chunk[0].to_string_lossy(), chunk[1].clone());
                    }
                }
            }
            RespValue::Error(err) if err.starts_with("WRONGPASS") => {
                return Err(NetError::AuthenticationFailed(format!(
                    "Redis authentication failed: {}",
                    err
                )));
            }
            _ => {
                // Server rejected HELLO 3 (e.g. unknown command), fall back to RESP2
            }
        }

        // 2. If HELLO 3 wasn't supported, fall back to RESP2 and authenticate via AUTH if needed
        if !switched_to_resp3 && let Some((user_opt, pass)) = auth_credentials {
            if let Some(user) = user_opt {
                write_command(&mut self.stream, &["AUTH", user, pass]).await?;
            } else {
                write_command(&mut self.stream, &["AUTH", pass]).await?;
            }

            let resp = read_resp_value(&mut self.stream, &mut self.buffer).await?;
            match resp {
                RespValue::SimpleString(s) if s == "OK" => {}
                RespValue::Error(err) => {
                    return Err(NetError::AuthenticationFailed(format!(
                        "Redis authentication failed: {}",
                        err
                    )));
                }
                other => {
                    return Err(NetError::ProtocolError(format!(
                        "Unexpected authentication response: {:?}",
                        other
                    )));
                }
            }
        }

        // 3. If database index was specified as a numeric index in query params, switch db
        if let Some(db_str) = self.uri.params.get("db")
            && let Ok(db_idx) = db_str.parse::<u32>()
        {
            write_command(&mut self.stream, &["SELECT", &db_idx.to_string()]).await?;
            let resp = read_resp_value(&mut self.stream, &mut self.buffer).await?;
            if let RespValue::Error(err) = resp {
                return Err(NetError::ExecutionError(format!(
                    "Failed to SELECT Redis db {}: {}",
                    db_idx, err
                )));
            }
        }

        Ok(())
    }

    /// Returns whether the connection is encrypted with TLS.
    pub fn is_tls(&self) -> bool {
        self.stream.is_tls()
    }

    /// Returns `true` if this connection negotiated RESP3.
    pub fn is_resp3(&self) -> bool {
        self.is_resp3
    }

    /// Returns the default graph name associated with this connection.
    pub fn default_graph(&self) -> &str {
        &self.default_graph
    }

    /// Sets the default graph name for subsequent queries.
    pub fn set_default_graph(&mut self, graph: String) {
        self.default_graph = graph;
    }

    /// Returns the metadata key-value map reported by the server during HELLO.
    pub fn server_info(&self) -> &HashMap<String, RespValue> {
        &self.server_info
    }

    /// Internal helper to update connection transaction status.
    pub(crate) fn set_in_transaction(&mut self, in_tx: bool) {
        self.in_transaction = in_tx;
    }

    /// Returns the active query timeout.
    pub fn query_timeout(&self) -> Option<std::time::Duration> {
        self.query_timeout
    }

    /// Sets the query timeout for this connection.
    pub fn set_query_timeout(&mut self, timeout: Option<std::time::Duration>) {
        self.query_timeout = timeout;
    }

    /// Executes an arbitrary raw Redis / Valkey command and returns the raw [`RespValue`].
    pub async fn execute_raw(&mut self, args: &[&str]) -> Result<RespValue> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Redis connection is closed or broken".to_string(),
            ));
        }
        if let Some(to) = self.query_timeout {
            match tokio::time::timeout(to, self.execute_raw_inner(args)).await {
                Ok(res) => res,
                Err(_) => {
                    self.is_valid = false;
                    Err(NetError::Timeout(format!(
                        "Redis command timed out after {:?}",
                        to
                    )))
                }
            }
        } else {
            self.execute_raw_inner(args).await
        }
    }

    async fn execute_raw_inner(&mut self, args: &[&str]) -> Result<RespValue> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Redis connection is closed or broken".to_string(),
            ));
        }
        self.is_valid = false;
        write_command(&mut self.stream, args).await?;
        let resp = read_resp_value(&mut self.stream, &mut self.buffer).await?;
        self.is_valid = true;
        Ok(resp)
    }

    /// Executes a Cypher query on FalkorDB via `GRAPH.QUERY`.
    pub async fn graph_query(
        &mut self,
        graph: &str,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<FalkorQueryResult> {
        let formatted = format_cypher_with_params(query, params)?;
        let resp = self
            .execute_raw(&["GRAPH.QUERY", graph, &formatted])
            .await?;
        FalkorQueryResult::parse(resp)
    }

    /// Executes a Cypher query on FalkorDB via `GRAPH.QUERY` in `--compact` mode.
    async fn graph_query_compact(
        &mut self,
        graph: &str,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<FalkorCompactResult> {
        let formatted = format_cypher_with_params(query, params)?;
        let resp = self
            .execute_raw(&["GRAPH.QUERY", graph, &formatted, "--compact"])
            .await?;
        FalkorCompactResult::parse(resp)
    }

    async fn fetch_schema_string_list(
        &mut self,
        graph: &str,
        proc_call: &str,
    ) -> Result<Vec<String>> {
        let resp = self
            .execute_raw(&["GRAPH.RO_QUERY", graph, proc_call, "--compact"])
            .await?;
        let parsed = FalkorCompactResult::parse(resp)?;
        let empty = FalkorGraphSchema::default();
        let mut out = Vec::with_capacity(parsed.rows.len());
        for row in &parsed.rows {
            if let Some(cell) = row.first()
                && let crate::bolt::BoltValue::String(s) =
                    crate::redis::graph::decode_compact_cell(cell, &empty)?
            {
                out.push(s);
            }
        }
        Ok(out)
    }

    async fn refresh_graph_schema(
        &mut self,
        graph: &str,
        version: Option<i64>,
    ) -> Result<Arc<FalkorGraphSchema>> {
        let labels = self
            .fetch_schema_string_list(graph, "CALL db.labels()")
            .await?;
        let relationships = self
            .fetch_schema_string_list(graph, "CALL db.relationshipTypes()")
            .await?;
        let properties = self
            .fetch_schema_string_list(graph, "CALL db.propertyKeys()")
            .await?;
        let schema = Arc::new(FalkorGraphSchema {
            version,
            labels,
            relationships,
            properties,
        });
        self.graph_schemas
            .insert(graph.to_string(), Arc::clone(&schema));
        Ok(schema)
    }

    /// Executes a read-only Cypher query on FalkorDB via `GRAPH.RO_QUERY`.
    pub async fn graph_ro_query(
        &mut self,
        graph: &str,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<FalkorQueryResult> {
        let formatted = format_cypher_with_params(query, params)?;
        let resp = self
            .execute_raw(&["GRAPH.RO_QUERY", graph, &formatted])
            .await?;
        FalkorQueryResult::parse(resp)
    }

    /// Deletes an entire graph from FalkorDB via `GRAPH.DELETE`.
    pub async fn graph_delete(&mut self, graph: &str) -> Result<()> {
        let resp = self.execute_raw(&["GRAPH.DELETE", graph]).await?;
        match resp {
            RespValue::SimpleString(_) => Ok(()),
            RespValue::Error(e) if e.contains("does not exist") || e.contains("Graph key") => {
                // Deleting a non-existent graph is considered safe/idempotent
                Ok(())
            }
            RespValue::Error(e) => Err(NetError::ExecutionError(format!(
                "Failed to delete graph '{}': {}",
                graph, e
            ))),
            _ => Ok(()),
        }
    }

    /// The legacy Redis `MULTI` API is intentionally unsupported as a transaction.
    ///
    /// FalkorDB/Redis cannot provide rollback or read-your-writes, so this
    /// legacy entry point fails like the [`AsyncConnection`] transaction API.
    pub async fn begin_transaction(&mut self) -> Result<RedisTransaction<'_>> {
        Err(falkordb_transaction_error())
    }
}

/// Error explaining why FalkorDB/Redis cannot act as a transactional database.
pub(crate) fn falkordb_transaction_error() -> NetError {
    NetError::TransactionError(
        "FalkorDB does not support transactions: Redis MULTI/EXEC cannot provide rollback or read-your-writes"
            .to_string(),
    )
}

#[async_trait]
impl AsyncConnection for RedisConnection {
    async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        let graph = self.default_graph.clone();
        match self.graph_query_compact(&graph, query, params).await {
            Ok(compact_res) => {
                // Hold the cached schema by `Arc` so a query never deep-clones
                // the label/relationship/property lists.
                let cached = self.graph_schemas.get(&graph).cloned();
                let schema = if compact_res.needs_schema_refresh(cached.as_deref()) {
                    self.refresh_graph_schema(&graph, compact_res.graph_version)
                        .await?
                } else {
                    cached.unwrap_or_default()
                };
                compact_res.to_query_result(&schema)
            }
            Err(e) => {
                let q_lower = query.trim().to_ascii_lowercase();
                let is_index_ddl = q_lower.starts_with("create index")
                    || q_lower.starts_with("drop index")
                    || q_lower.starts_with("create fulltext index")
                    || q_lower.starts_with("drop fulltext index")
                    || q_lower.starts_with("create vector index")
                    || q_lower.starts_with("drop vector index");
                let err_msg = e.to_string().to_ascii_lowercase();
                if is_index_ddl
                    && (err_msg.contains("already indexed") || err_msg.contains("no such index"))
                {
                    return Ok(QueryResult::empty(QuerySummary::default()));
                }
                Err(e)
            }
        }
    }

    async fn ping(&mut self) -> Result<()> {
        let resp = self.execute_raw(&["PING"]).await?;
        match resp {
            RespValue::SimpleString(s) if s == "PONG" => Ok(()),
            RespValue::BulkString(Some(b)) if b == b"PONG" => Ok(()),
            RespValue::Error(err) => Err(NetError::ExecutionError(format!("PING failed: {}", err))),
            other => Err(NetError::ProtocolError(format!(
                "Unexpected PING response: {:?}",
                other
            ))),
        }
    }

    async fn reset(&mut self) -> Result<()> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Redis connection is closed or broken".to_string(),
            ));
        }
        if self.in_transaction {
            self.execute_raw(&["DISCARD"]).await?;
            self.in_transaction = false;
        }
        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn is_in_transaction(&self) -> bool {
        self.in_transaction
    }

    async fn close(&mut self) -> Result<()> {
        let _ = write_command(&mut self.stream, &["QUIT"]).await;
        self.is_valid = false;
        Ok(())
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        Err(falkordb_transaction_error())
    }

    async fn commit_transaction(&mut self) -> Result<()> {
        Err(falkordb_transaction_error())
    }

    async fn rollback_transaction(&mut self) -> Result<()> {
        Err(falkordb_transaction_error())
    }
}
