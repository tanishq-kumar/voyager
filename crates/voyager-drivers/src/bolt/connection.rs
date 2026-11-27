//! Bolt asynchronous connection implementation over TCP or TLS streams.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use bytes::BytesMut;
use std::collections::HashMap;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::debug;

use super::messages::{BoltRequest, BoltResponse};
use super::packstream::{BoltValue, param_to_bolt_value};
use super::stream::{encode_chunks, read_message_frame_buffered, write_message_frame};
use crate::config::{Auth, ConnectionConfig};
use crate::engine::{AsyncConnection, QueryResult, QuerySummary};
use crate::error::{NetError, Result};
use crate::params::ParamValue;
use crate::tls::{NetStream, TlsVerification};
use crate::uri::ParsedUri;

/// Magic 4-byte header sent at the start of every Bolt TCP connection (`0x60 0x60 0xB0 0x17`).
pub const BOLT_MAGIC_PREAMBLE: [u8; 4] = [0x60, 0x60, 0xB0, 0x17];

/// Default version proposals sent during handshake negotiation (v5.4..=5.0, v4.4..=4.2).
pub const BOLT_PROPOSED_VERSIONS: [u8; 16] = [
    0x00, 0x04, 0x04, 0x05, // v5.4 down to v5.0 (5.4, 5.3, 5.2, 5.1, 5.0)
    0x00, 0x02, 0x04, 0x04, // v4.4 down to v4.2 (4.4, 4.3, 4.2)
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
];

/// Negotiated Bolt protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BoltVersion {
    /// Major protocol version (e.g. 5).
    pub major: u8,
    /// Minor protocol version (e.g. 4).
    pub minor: u8,
}

/// Physical asynchronous Bolt connection managing socket I/O, PackStream framing, and state.
pub struct BoltConnection<S: AsyncRead + AsyncWrite + Unpin + Send + Sync = NetStream> {
    stream: S,
    read_buf: BytesMut,
    version: BoltVersion,
    /// True when Bolt 4.4 negotiated the `"utc"` patch via `patch_bolt`.
    utc_patched: bool,
    /// Database selected in the URI or connection config, sent in `RUN` and `BEGIN` on Bolt 4.0+.
    database: Option<String>,
    is_valid: bool,
    in_transaction: bool,
    server_agent: Option<String>,
    /// Last bookmark string received from the server.
    pub last_bookmark: Option<String>,
    /// Last execution metadata received from RUN and PULL responses.
    pub last_metadata: Option<HashMap<String, BoltValue>>,
    /// Client-side execution timeout for individual queries.
    pub query_timeout: Option<Duration>,
}

impl BoltConnection<NetStream> {
    /// Establishes a new connection to the target Bolt endpoint and performs handshake/auth.
    ///
    /// TLS is used when the URI scheme (`bolt+s`, `neo4j+ssc`, ...) or `config.tls` asks for
    /// it; a TLS request is never downgraded to plaintext. Without the `tls` Cargo feature a
    /// TLS request fails with [`NetError::TlsError`].
    pub async fn connect(config: &ConnectionConfig) -> Result<Self> {
        use crate::config::TlsMode;
        let uri = ParsedUri::parse(&config.uri)?;
        let mode = match (config.tls, uri.tls) {
            (TlsMode::Disabled, uri_mode) => uri_mode,
            (TlsMode::SelfSigned, TlsMode::Required) => {
                return Err(NetError::TlsError(format!(
                    "ConnectionConfig asks for TlsMode::SelfSigned, which would weaken the verified TLS that the URI for '{}' requires",
                    uri.host
                )));
            }
            (config_mode, _) => config_mode,
        };
        if mode != TlsMode::Disabled && !crate::tls::TLS_AVAILABLE {
            return Err(crate::tls::tls_unavailable(uri.protocol, &uri.host));
        }
        let addr = uri.socket_addr();

        debug!("Connecting to Bolt server at {}", addr);
        match timeout(
            config.connect_timeout,
            Self::connect_raw(config, &uri, mode),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => Err(NetError::Timeout(format!(
                "Connection or handshake to Bolt server at {} timed out after {:?}",
                addr, config.connect_timeout
            ))),
        }
    }

    /// Returns whether the connection is encrypted with TLS.
    pub fn is_tls(&self) -> bool {
        self.stream.is_tls()
    }

    async fn connect_raw(
        config: &ConnectionConfig,
        uri: &ParsedUri,
        mode: crate::config::TlsMode,
    ) -> Result<Self> {
        let addr = uri.socket_addr();
        let stream = TcpStream::connect(&addr).await.map_err(|e| {
            NetError::ConnectionFailed(format!("Failed to connect to {}: {}", addr, e))
        })?;

        // Disable Nagle's algorithm to eliminate 40-200ms delayed-ACK packet stalls
        let _ = stream.set_nodelay(true);

        // Bolt starts TLS before the handshake preamble.
        let mut stream = match TlsVerification::for_mode(mode) {
            Some(verification) => {
                let options = config
                    .tls_options
                    .clone()
                    .or(crate::tls::TlsOptions::from_params(&uri.params));
                crate::tls::upgrade(stream, uri.protocol, &uri.host, verification, &options).await?
            }
            None => NetStream::Plain(stream),
        };

        // 1. Send Handshake
        let mut handshake_bytes = Vec::with_capacity(20);
        handshake_bytes.extend_from_slice(&BOLT_MAGIC_PREAMBLE);
        handshake_bytes.extend_from_slice(&BOLT_PROPOSED_VERSIONS);

        stream.write_all(&handshake_bytes).await.map_err(|e| {
            NetError::ConnectionFailed(format!("Failed to write Bolt handshake: {}", e))
        })?;
        stream.flush().await.map_err(|e| {
            NetError::ConnectionFailed(format!("Failed to flush Bolt handshake: {}", e))
        })?;

        // 2. Read Server Version Response (4 bytes)
        let mut version_resp = [0u8; 4];
        stream.read_exact(&mut version_resp).await.map_err(|e| {
            NetError::ConnectionFailed(format!("Failed to read Bolt version response: {}", e))
        })?;

        if version_resp == [0, 0, 0, 0] {
            return Err(NetError::ProtocolError(
                "Bolt server rejected proposed protocol versions".to_string(),
            ));
        }

        let major = version_resp[3];
        let minor = version_resp[2];
        let is_proposed = version_resp[0] == 0
            && version_resp[1] == 0
            && ((major == 5 && (0..=4).contains(&minor))
                || (major == 4 && (2..=4).contains(&minor)));
        if !is_proposed {
            return Err(NetError::ProtocolError(format!(
                "Bolt server returned invalid or unproposed protocol version bytes: {:02x?}",
                version_resp
            )));
        }
        let version = BoltVersion { major, minor };
        debug!("Negotiated Bolt protocol version v{}.{}", major, minor);

        // 3. Send HELLO / Auth Handshake
        let auth: &Auth = match &config.auth {
            Auth::None => &uri.auth,
            other => other,
        };

        let db = config.database.as_deref().or(uri.database.as_deref());

        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(65536),
            version,
            utc_patched: false,
            database: db.map(str::to_string),
            is_valid: true,
            in_transaction: false,
            server_agent: None,
            last_bookmark: None,
            last_metadata: None,
            query_timeout: config.query_timeout,
        };

        let is_v51_or_higher = version.major > 5 || (version.major == 5 && version.minor >= 1);
        let is_v53_or_higher = version.major > 5 || (version.major == 5 && version.minor >= 3);

        if is_v51_or_higher {
            // 3a. Bolt 5.1+: HELLO without auth, followed by LOGON
            let user_agent = concat!("Voyager-OGM/", env!("CARGO_PKG_VERSION"));
            let hello_req = if is_v53_or_higher {
                BoltRequest::hello_v53(user_agent, None)
            } else {
                BoltRequest::hello_v51(user_agent, None)
            };
            conn.send_request(&hello_req).await?;
            let resp = conn.receive_response().await?;

            match resp {
                BoltResponse::Success { metadata } => {
                    if let Some(agent) = metadata.get("server").and_then(|v| v.as_str()) {
                        conn.server_agent = Some(agent.to_string());
                        debug!("Connected to Bolt server agent: {}", agent);
                    }
                }
                BoltResponse::Failure { metadata } => {
                    let code = metadata
                        .get("code")
                        .and_then(|v| v.as_str())
                        .unwrap_or("HelloError");
                    let msg = metadata
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Handshake failed");
                    conn.is_valid = false;
                    return Err(NetError::ProtocolError(format!("{}: {}", code, msg)));
                }
                other => {
                    conn.is_valid = false;
                    return Err(NetError::ProtocolError(format!(
                        "Unexpected response to HELLO: {:?}",
                        other
                    )));
                }
            }

            // 3b. Send LOGON
            let logon_req = BoltRequest::logon_with_auth(auth);
            conn.send_request(&logon_req).await?;
            let resp = conn.receive_response().await?;

            match resp {
                BoltResponse::Success { .. } => {}
                BoltResponse::Failure { metadata } => {
                    let code = metadata
                        .get("code")
                        .and_then(|v| v.as_str())
                        .unwrap_or("AuthError");
                    let msg = metadata
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Authentication failed");
                    conn.is_valid = false;
                    return Err(NetError::AuthenticationFailed(format!("{}: {}", code, msg)));
                }
                other => {
                    conn.is_valid = false;
                    return Err(NetError::ProtocolError(format!(
                        "Unexpected response to LOGON: {:?}",
                        other
                    )));
                }
            }
        } else {
            // 3. Legacy Bolt <= 5.0: HELLO with inline auth (and utc patch on 4.4)
            let patches: &[&str] = if version.major == 4 && version.minor == 4 {
                &["utc"]
            } else {
                &[]
            };
            let hello_req = BoltRequest::hello_legacy_with_auth(
                concat!("Voyager-OGM/", env!("CARGO_PKG_VERSION")),
                auth,
                db,
                patches,
            );
            conn.send_request(&hello_req).await?;
            let resp = conn.receive_response().await?;

            match resp {
                BoltResponse::Success { metadata } => {
                    if let Some(agent) = metadata.get("server").and_then(|v| v.as_str()) {
                        conn.server_agent = Some(agent.to_string());
                        debug!("Connected to Bolt server agent: {}", agent);
                    }
                    if let Some(applied) = metadata.get("patch_bolt").and_then(BoltValue::as_list)
                        && applied.iter().any(|v| v.as_str() == Some("utc"))
                    {
                        conn.utc_patched = true;
                        debug!("Negotiated Bolt 4.4 utc patch");
                    }
                }
                BoltResponse::Failure { metadata } => {
                    let code = metadata
                        .get("code")
                        .and_then(|v| v.as_str())
                        .unwrap_or("AuthError");
                    let msg = metadata
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Authentication failed");
                    conn.is_valid = false;
                    return Err(NetError::AuthenticationFailed(format!("{}: {}", code, msg)));
                }
                other => {
                    conn.is_valid = false;
                    return Err(NetError::ProtocolError(format!(
                        "Unexpected response to HELLO: {:?}",
                        other
                    )));
                }
            }
        }

        Ok(conn)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync> BoltConnection<S> {
    /// Creates a `BoltConnection` from an existing stream and negotiated version (useful for tests/stubs).
    pub fn from_stream(stream: S, version: BoltVersion) -> Self {
        Self {
            stream,
            read_buf: BytesMut::with_capacity(65536),
            version,
            utc_patched: false,
            database: None,
            is_valid: true,
            in_transaction: false,
            server_agent: None,
            last_bookmark: None,
            last_metadata: None,
            query_timeout: None,
        }
    }

    /// Returns the negotiated Bolt protocol version.
    pub fn version(&self) -> BoltVersion {
        self.version
    }

    /// Returns `true` when zoned datetimes use UTC epoch seconds (`Bolt >= 5.0` or Bolt 4.4 `utc` patch).
    pub fn uses_utc_datetimes(&self) -> bool {
        self.version.major >= 5 || self.utc_patched
    }

    /// Returns the database selected in the URI or connection config, if any.
    pub fn database(&self) -> Option<&str> {
        self.database.as_deref()
    }

    /// Returns the database to send in auto-commit `RUN` metadata.
    ///
    /// Bolt names the database per transaction; a `RUN` inside an explicit
    /// transaction must rely on the database named by `BEGIN` instead.
    fn auto_commit_database(&self) -> Option<&str> {
        if self.version.major >= 4 && !self.in_transaction {
            self.database.as_deref()
        } else {
            None
        }
    }

    /// Returns the server agent string if reported by the database server.
    pub fn server_agent(&self) -> Option<&str> {
        self.server_agent.as_deref()
    }

    /// Returns the active query timeout.
    pub fn query_timeout(&self) -> Option<Duration> {
        self.query_timeout
    }

    /// Sets the query timeout for this connection.
    pub fn set_query_timeout(&mut self, timeout: Option<Duration>) {
        self.query_timeout = timeout;
    }

    /// Sends an encoded `BoltRequest` message frame over the stream.
    pub async fn send_request(&mut self, request: &BoltRequest) -> Result<()> {
        let mut payload = BytesMut::new();
        request.encode(&mut payload);

        let mut chunk_buf = BytesMut::new();
        encode_chunks(&payload, &mut chunk_buf);

        write_message_frame(&mut self.stream, &chunk_buf).await
    }

    /// Receives and decodes a single `BoltResponse` message from the stream.
    pub async fn receive_response(&mut self) -> Result<BoltResponse> {
        let mut frame_bytes =
            read_message_frame_buffered(&mut self.stream, &mut self.read_buf).await?;
        BoltResponse::decode(&mut frame_bytes)
    }

    /// Converts raw records and column names into an Apache Arrow `RecordBatch`.
    pub fn build_record_batch(
        columns: &[String],
        records: &[Vec<BoltValue>],
    ) -> Result<RecordBatch> {
        crate::bolt::canonical::build_bolt_record_batch(columns, records)
    }

    /// Parses execution mutation metrics from a `SUCCESS` response metadata map.
    fn parse_summary_metrics(metadata: &HashMap<String, BoltValue>) -> QuerySummary {
        let mut summary = QuerySummary::empty();

        if let Some(BoltValue::Map(stats)) = metadata.get("stats") {
            if let Some(BoltValue::Integer(n)) = stats.get("nodes-created") {
                summary.nodes_created = *n as u64;
            }
            if let Some(BoltValue::Integer(n)) = stats.get("nodes-deleted") {
                summary.nodes_deleted = *n as u64;
            }
            if let Some(BoltValue::Integer(n)) = stats.get("relationships-created") {
                summary.relationships_created = *n as u64;
            }
            if let Some(BoltValue::Integer(n)) = stats.get("relationships-deleted") {
                summary.relationships_deleted = *n as u64;
            }
            if let Some(BoltValue::Integer(n)) = stats.get("properties-set") {
                summary.properties_set = *n as u64;
            }
            if let Some(BoltValue::Integer(n)) = stats.get("labels-added") {
                summary.labels_added = *n as u64;
            }
            if let Some(BoltValue::Integer(n)) = stats.get("labels-removed") {
                summary.labels_removed = *n as u64;
            }
            if let Some(BoltValue::Integer(n)) = stats.get("indexes-added") {
                summary.indexes_added = *n as u64;
            }
            if let Some(BoltValue::Integer(n)) = stats.get("constraints-added") {
                summary.constraints_added = *n as u64;
            }
        }

        if let Some(BoltValue::Integer(time)) =
            metadata.get("t_last").or_else(|| metadata.get("t_first"))
        {
            summary.execution_time_ms = *time as u64;
        }

        summary
    }

    /// Executes a parameterized query returning raw BoltValue records, projected columns, and summary.
    pub async fn execute_raw_records(
        &mut self,
        query: &str,
        params: &HashMap<String, BoltValue>,
    ) -> Result<(Vec<String>, Vec<Vec<BoltValue>>, QuerySummary)> {
        let (columns, records, summary, stream_err) = self
            .execute_raw_records_with_options(query, params, None, None, None, None)
            .await?;
        if let Some((code, msg)) = stream_err {
            return Err(NetError::ProtocolError(format!("{}: {}", code, msg)));
        }
        Ok((columns, records, summary))
    }

    /// Executes a parameterized query with advanced metadata options, returning records and stream errors.
    pub async fn execute_raw_records_with_options(
        &mut self,
        query: &str,
        params: &HashMap<String, BoltValue>,
        database: Option<&str>,
        tx_meta: Option<HashMap<String, BoltValue>>,
        timeout_ms: Option<i64>,
        bookmarks: Option<Vec<String>>,
    ) -> Result<(
        Vec<String>,
        Vec<Vec<BoltValue>>,
        QuerySummary,
        Option<(String, String)>,
    )> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Bolt connection is closed or broken".to_string(),
            ));
        }

        let mut extra = HashMap::new();
        if let Some(db) = database {
            extra.insert("db".to_string(), BoltValue::String(db.to_string()));
        }
        if let Some(meta) = tx_meta {
            extra.insert("tx_metadata".to_string(), BoltValue::Map(meta));
        }
        let effective_timeout = match timeout_ms {
            Some(timeout) if timeout > 0 => {
                extra.insert("tx_timeout".to_string(), BoltValue::Integer(timeout));
                Some(Duration::from_millis(timeout as u64))
            }
            _ => {
                if let Some(to) = self.query_timeout {
                    extra.insert(
                        "tx_timeout".to_string(),
                        BoltValue::Integer(to.as_millis() as i64),
                    );
                    Some(to)
                } else {
                    None
                }
            }
        };
        if let Some(bms) = bookmarks {
            let bm_vals: Vec<BoltValue> = bms.into_iter().map(BoltValue::String).collect();
            extra.insert("bookmarks".to_string(), BoltValue::List(bm_vals));
        }

        if let Some(to) = effective_timeout {
            match timeout(to, self.execute_raw_records_inner(query, params, extra)).await {
                Ok(res) => res,
                Err(_) => {
                    self.is_valid = false;
                    Err(NetError::Timeout(format!(
                        "Query execution timed out after {:?}",
                        to
                    )))
                }
            }
        } else {
            self.execute_raw_records_inner(query, params, extra).await
        }
    }

    async fn execute_raw_records_inner(
        &mut self,
        query: &str,
        params: &HashMap<String, BoltValue>,
        extra: HashMap<String, BoltValue>,
    ) -> Result<(
        Vec<String>,
        Vec<Vec<BoltValue>>,
        QuerySummary,
        Option<(String, String)>,
    )> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Bolt connection is closed or broken".to_string(),
            ));
        }

        // 1. Pipeline RUN + PULL
        let run_req = BoltRequest::Run {
            query: query.to_string(),
            params: params.clone(),
            extra,
        };
        let pull_req = BoltRequest::pull_all();

        let mut write_buf = BytesMut::with_capacity(1024);
        let mut run_payload = BytesMut::with_capacity(512);
        run_req.encode(&mut run_payload);
        encode_chunks(&run_payload, &mut write_buf);

        let mut pull_payload = BytesMut::with_capacity(64);
        pull_req.encode(&mut pull_payload);
        encode_chunks(&pull_payload, &mut write_buf);

        self.is_valid = false;
        write_message_frame(&mut self.stream, &write_buf).await?;

        let mut collected_metadata = HashMap::new();

        // 2. Receive RUN response
        let run_resp = self.receive_response().await?;
        let columns: Vec<String> = match run_resp {
            BoltResponse::Success { ref metadata } => {
                collected_metadata.extend(metadata.clone());
                metadata
                    .get("fields")
                    .and_then(|v| v.as_list())
                    .map(|l| {
                        l.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<String>>()
                    })
                    .unwrap_or_default()
            }
            BoltResponse::Failure { metadata } => {
                let code = metadata
                    .get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Neo.ClientError.Statement.SyntaxError");
                let msg = metadata
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Query failed");
                self.receive_response().await?;
                self.reset_inner().await?;
                return Err(NetError::ProtocolError(format!("{}: {}", code, msg)));
            }
            other => {
                return Err(NetError::ProtocolError(format!(
                    "Unexpected RUN response: {:?}",
                    other
                )));
            }
        };

        // 3. Receive stream of RECORDs
        let mut records = Vec::new();
        let mut stream_err = None;
        let summary = loop {
            let resp = self.receive_response().await?;
            match resp {
                BoltResponse::Record { fields } => {
                    records.push(fields);
                }
                BoltResponse::Success { metadata } => {
                    if let Some(BoltValue::String(bm)) = metadata.get("bookmark") {
                        self.last_bookmark = Some(bm.clone());
                    } else if let Some(BoltValue::List(bms)) = metadata.get("bookmarks")
                        && let Some(BoltValue::String(bm)) = bms.last()
                    {
                        self.last_bookmark = Some(bm.clone());
                    }
                    collected_metadata.extend(metadata.clone());
                    self.last_metadata = Some(collected_metadata);
                    self.is_valid = true;
                    break Self::parse_summary_metrics(&metadata);
                }
                BoltResponse::Failure { metadata } => {
                    let code = metadata
                        .get("code")
                        .and_then(|v| v.as_str())
                        .unwrap_or("StreamError");
                    let msg = metadata
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Stream record failed");
                    self.reset_inner().await?;
                    stream_err = Some((code.to_string(), msg.to_string()));
                    break QuerySummary::default();
                }
                other => {
                    return Err(NetError::ProtocolError(format!(
                        "Unexpected stream response: {:?}",
                        other
                    )));
                }
            }
        };

        Ok((columns, records, summary, stream_err))
    }

    /// Begins an explicit transaction block.
    pub async fn begin_tx(&mut self, database: Option<&str>) -> Result<()> {
        self.begin_tx_with_options(database, None, None, None).await
    }

    /// Begins an explicit transaction block with optional configuration (bookmarks, metadata, timeout).
    pub async fn begin_tx_with_options(
        &mut self,
        database: Option<&str>,
        tx_meta: Option<HashMap<String, BoltValue>>,
        timeout_ms: Option<i64>,
        bookmarks: Option<Vec<String>>,
    ) -> Result<()> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Bolt connection is closed or broken".to_string(),
            ));
        }

        let mut extra = HashMap::new();
        if let Some(db) = database {
            extra.insert("db".to_string(), BoltValue::String(db.to_string()));
        }
        if let Some(meta) = tx_meta {
            extra.insert("tx_metadata".to_string(), BoltValue::Map(meta));
        }
        if let Some(timeout) = timeout_ms {
            extra.insert("tx_timeout".to_string(), BoltValue::Integer(timeout));
        }
        if let Some(bms) = bookmarks {
            let bm_vals: Vec<BoltValue> = bms.into_iter().map(BoltValue::String).collect();
            extra.insert("bookmarks".to_string(), BoltValue::List(bm_vals));
        }
        self.is_valid = false;
        self.send_request(&BoltRequest::Begin { extra }).await?;
        let resp = self.receive_response().await?;
        match resp {
            BoltResponse::Success { .. } => {
                self.in_transaction = true;
                self.is_valid = true;
                Ok(())
            }
            BoltResponse::Failure { metadata } => {
                let code = metadata
                    .get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Neo.ClientError.Transaction.InvalidBookmark");
                let msg = metadata
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Begin transaction failed");
                self.reset_inner().await?;
                Err(NetError::ProtocolError(format!("{}: {}", code, msg)))
            }
            other => Err(NetError::ProtocolError(format!(
                "Unexpected BEGIN response: {:?}",
                other
            ))),
        }
    }

    /// Commits an open explicit transaction.
    pub async fn commit_tx(&mut self) -> Result<()> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Bolt connection is closed or broken".to_string(),
            ));
        }

        self.is_valid = false;
        self.send_request(&BoltRequest::Commit).await?;
        let resp = self.receive_response().await?;
        match resp {
            BoltResponse::Success { metadata } => {
                self.in_transaction = false;
                self.is_valid = true;
                if let Some(BoltValue::String(bm)) = metadata.get("bookmark") {
                    self.last_bookmark = Some(bm.clone());
                } else if let Some(BoltValue::List(bms)) = metadata.get("bookmarks")
                    && let Some(BoltValue::String(bm)) = bms.last()
                {
                    self.last_bookmark = Some(bm.clone());
                }
                Ok(())
            }
            BoltResponse::Failure { metadata } => {
                self.in_transaction = false;
                let code = metadata
                    .get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Neo.ClientError.Transaction.TransactionHookFailed");
                let msg = metadata
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Commit transaction failed");
                self.reset_inner().await?;
                Err(NetError::ProtocolError(format!("{}: {}", code, msg)))
            }
            other => {
                self.in_transaction = false;
                Err(NetError::ProtocolError(format!(
                    "Unexpected COMMIT response: {:?}",
                    other
                )))
            }
        }
    }

    /// Aborts and rolls back an open explicit transaction.
    pub async fn rollback_tx(&mut self) -> Result<()> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Bolt connection is closed or broken".to_string(),
            ));
        }

        self.is_valid = false;
        self.send_request(&BoltRequest::Rollback).await?;
        let resp = self.receive_response().await?;
        match resp {
            BoltResponse::Success { .. } => {
                self.in_transaction = false;
                self.is_valid = true;
                Ok(())
            }
            BoltResponse::Failure { metadata } => {
                self.in_transaction = false;
                let code = metadata
                    .get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Neo.ClientError.Transaction.TransactionHookFailed");
                let msg = metadata
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Rollback transaction failed");
                self.reset_inner().await?;
                Err(NetError::ProtocolError(format!("{}: {}", code, msg)))
            }
            other => {
                self.in_transaction = false;
                Err(NetError::ProtocolError(format!(
                    "Unexpected ROLLBACK response: {:?}",
                    other
                )))
            }
        }
    }

    /// Inner execution routine for `AsyncConnection::execute` with pre-allocated buffers.
    async fn execute_inner(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Bolt connection is closed or broken".to_string(),
            ));
        }

        // Convert typed ParamValue params to BoltValue matching the negotiated Bolt version.
        let temporal_major = if self.uses_utc_datetimes() {
            5
        } else {
            self.version.major
        };
        let mut bolt_params = HashMap::with_capacity(params.len());
        for (k, v) in params {
            bolt_params.insert(k.clone(), param_to_bolt_value(v, temporal_major)?);
        }

        let mut extra = HashMap::new();
        if let Some(db) = self.auto_commit_database() {
            extra.insert("db".to_string(), BoltValue::String(db.to_string()));
        }
        if let Some(to) = self.query_timeout {
            extra.insert(
                "tx_timeout".to_string(),
                BoltValue::Integer(to.as_millis() as i64),
            );
        }

        // 1. Pipeline RUN + PULL in a single pre-allocated write buffer
        let run_req = BoltRequest::Run {
            query: query.to_string(),
            params: bolt_params,
            extra,
        };
        let pull_req = BoltRequest::pull_all();

        let mut write_buf = BytesMut::with_capacity(1024);
        let mut run_payload = BytesMut::with_capacity(512);
        run_req.encode(&mut run_payload);
        encode_chunks(&run_payload, &mut write_buf);

        let mut pull_payload = BytesMut::with_capacity(64);
        pull_req.encode(&mut pull_payload);
        encode_chunks(&pull_payload, &mut write_buf);

        // Send pipelined requests with sticky invalidation until terminal response
        self.is_valid = false;
        write_message_frame(&mut self.stream, &write_buf).await?;

        // 2. Receive RUN response (Expect SUCCESS with column fields)
        let run_resp = self.receive_response().await?;
        let columns: Vec<String> = match run_resp {
            BoltResponse::Success { ref metadata } => metadata
                .get("fields")
                .and_then(|v| v.as_list())
                .map(|l| {
                    l.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect::<Vec<String>>()
                })
                .unwrap_or_default(),
            BoltResponse::Failure { metadata } => {
                let code = metadata
                    .get("code")
                    .and_then(|v| v.as_str())
                    .unwrap_or("QueryError");
                let msg = metadata
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Query failed");
                // Consume in-flight IGNORED response from pipelined PULL
                self.receive_response().await?;
                // Issue RESET to clear failure state
                self.reset_inner().await?;
                return Err(NetError::ProtocolError(format!("{}: {}", code, msg)));
            }
            other => {
                return Err(NetError::ProtocolError(format!(
                    "Unexpected RUN response: {:?}",
                    other
                )));
            }
        };

        // 3. Receive stream of RECORD responses followed by final SUCCESS
        let mut records = Vec::new();

        let summary = loop {
            let resp = self.receive_response().await?;
            match resp {
                BoltResponse::Record { fields } => {
                    records.push(fields);
                }
                BoltResponse::Success { metadata } => {
                    self.is_valid = true;
                    break Self::parse_summary_metrics(&metadata);
                }
                BoltResponse::Failure { metadata } => {
                    let code = metadata
                        .get("code")
                        .and_then(|v| v.as_str())
                        .unwrap_or("StreamError");
                    let msg = metadata
                        .get("message")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Stream record failed");
                    self.reset_inner().await?;
                    return Err(NetError::ProtocolError(format!("{}: {}", code, msg)));
                }
                other => {
                    return Err(NetError::ProtocolError(format!(
                        "Unexpected stream response: {:?}",
                        other
                    )));
                }
            }
        };

        // 4. Build Arrow RecordBatch
        let batch = Self::build_record_batch(&columns, &records)?;
        let batches = if batch.num_rows() > 0 || !columns.is_empty() {
            vec![batch]
        } else {
            Vec::new()
        };

        let result = QueryResult::new(columns, batches, summary);
        if result
            .batches
            .iter()
            .any(super::canonical::batch_needs_bolt_rows)
        {
            Ok(result.with_bolt_rows(records))
        } else {
            Ok(result)
        }
    }
    /// Sends `RESET` without requiring `is_valid == true`, used for failure recovery inside
    /// an already-locked in-flight request while keeping `is_valid` false if recovery fails.
    async fn reset_inner(&mut self) -> Result<()> {
        self.is_valid = false;
        self.send_request(&BoltRequest::Reset).await?;
        let resp = self.receive_response().await?;
        match resp {
            BoltResponse::Success { .. } => {
                self.in_transaction = false;
                self.is_valid = true;
                Ok(())
            }
            BoltResponse::Failure { metadata } => {
                let msg = metadata
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Reset failed");
                Err(NetError::ProtocolError(format!("RESET failed: {}", msg)))
            }
            other => Err(NetError::ProtocolError(format!(
                "Unexpected RESET response: {:?}",
                other
            ))),
        }
    }
}

#[async_trait]
impl<S: AsyncRead + AsyncWrite + Unpin + Send + Sync> AsyncConnection for BoltConnection<S> {
    async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        if let Some(to) = self.query_timeout {
            match timeout(to, self.execute_inner(query, params)).await {
                Ok(res) => res,
                Err(_) => {
                    self.is_valid = false;
                    Err(NetError::Timeout(format!(
                        "Query execution timed out after {:?}",
                        to
                    )))
                }
            }
        } else {
            self.execute_inner(query, params).await
        }
    }

    async fn ping(&mut self) -> Result<()> {
        if !self.is_valid {
            return Err(NetError::Closed("Connection is closed".to_string()));
        }
        self.reset().await
    }

    async fn reset(&mut self) -> Result<()> {
        if !self.is_valid {
            return Err(NetError::Closed(
                "Bolt connection is closed or broken".to_string(),
            ));
        }
        self.reset_inner().await
    }

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn protocol_version(&self) -> Option<String> {
        Some(format!("{}.{}", self.version.major, self.version.minor))
    }

    fn is_in_transaction(&self) -> bool {
        self.in_transaction
    }

    async fn close(&mut self) -> Result<()> {
        if self.is_valid {
            let _ = self.send_request(&BoltRequest::Goodbye).await;
            self.is_valid = false;
        }
        Ok(())
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        // Bolt 4.0+ names the database on BEGIN; older versions only had it in HELLO.
        let database = if self.version.major >= 4 {
            self.database.clone()
        } else {
            None
        };
        self.begin_tx(database.as_deref()).await
    }

    async fn commit_transaction(&mut self) -> Result<()> {
        self.commit_tx().await
    }

    async fn rollback_transaction(&mut self) -> Result<()> {
        self.rollback_tx().await
    }
}
