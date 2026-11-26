//! PostgreSQL physical asynchronous connection engine implementing [`AsyncConnection`].

use arrow_array::Array;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::debug;

use crate::config::Auth;
use crate::engine::{AsyncConnection, QueryResult, QuerySummary};
use crate::error::{NetError, Result};
use crate::params::ParamValue;
use crate::postgres::agtype::rows_to_record_batch_with_column_metadata;
use crate::postgres::auth::{ScramClient, compute_md5_password};
use crate::postgres::codec::{
    read_backend_message, write_frontend_message, write_frontend_messages, write_startup_message,
};
use crate::postgres::message::{
    AuthenticationRequest, BackendMessage, FrontendMessage, StartupMessage, TransactionStatus,
};
use crate::postgres::params::{PreparedPostgresStatement, prepare_postgres_statement};
use crate::tls::{NetStream, SslMode, SslNegotiation, TlsOptions};
use crate::uri::ParsedUri;

/// Asynchronous physical connection to a PostgreSQL database server or Apache AGE instance.
pub struct PostgresConnection {
    stream: NetStream,
    uri: ParsedUri,
    server_parameters: HashMap<String, String>,
    backend_pid: i32,
    secret_key: i32,
    transaction_status: TransactionStatus,
    is_age_initialized: bool,
    agtype_oid: Option<i32>,
    query_timeout: Option<std::time::Duration>,
    last_command_tag: Option<String>,
}

/// The 8-byte SSLRequest packet: length 8 and the request code 80877103.
const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xD2, 0x16, 0x2F];

/// Whether bytes are already waiting on the socket. The check is a non-blocking `peek` on the
/// socket itself, so it does not depend on the runtime's cached readiness.
fn has_pending_data(tcp: TcpStream) -> Result<(TcpStream, bool)> {
    let socket = tcp.into_std()?;
    let pending = matches!(socket.peek(&mut [0u8; 1]), Ok(n) if n > 0);
    Ok((TcpStream::from_std(socket)?, pending))
}

/// Appends `detail` to the message of an error.
fn append_detail(error: NetError, detail: &str) -> NetError {
    match error {
        NetError::ConnectionFailed(m) => NetError::ConnectionFailed(format!("{m} {detail}")),
        NetError::AuthenticationFailed(m) => {
            NetError::AuthenticationFailed(format!("{m} {detail}"))
        }
        NetError::Timeout(m) => NetError::Timeout(format!("{m} {detail}")),
        NetError::ProtocolError(m) => NetError::ProtocolError(format!("{m} {detail}")),
        NetError::TlsError(m) => NetError::TlsError(format!("{m} {detail}")),
        NetError::Closed(m) => NetError::Closed(format!("{m} {detail}")),
        NetError::ExecutionError(m) => NetError::ExecutionError(format!("{m} {detail}")),
        NetError::IoError(e) => NetError::ConnectionFailed(format!("{e} {detail}")),
        other => other,
    }
}

impl PostgresConnection {
    /// Asynchronously establishes a new connection to PostgreSQL / Apache AGE based on the parsed URI.
    ///
    /// TLS follows libpq's `sslmode` (see [`SslMode`]): `disable`, `allow`, `prefer`,
    /// `require`, `verify-ca`, `verify-full`, with `sslrootcert`, `sslcert` and `sslkey`.
    /// Only `prefer` continues in plaintext when the server answers the SSLRequest with `N`.
    /// `sslnegotiation=direct` (PostgreSQL 17+) starts TLS without an SSLRequest.
    pub async fn connect(uri: &ParsedUri) -> Result<Self> {
        Self::connect_with_tls(uri, &TlsOptions::new()).await
    }

    /// Like [`connect`](Self::connect), with explicit Apache AGE intent carried
    /// separately from the URI.
    pub async fn connect_with_age(uri: &ParsedUri, age: bool) -> Result<Self> {
        Self::connect_with_tls_and_age(uri, &TlsOptions::new(), age).await
    }

    /// Like [`connect`](Self::connect), with TLS options that win over the URI parameters.
    pub async fn connect_with_tls(uri: &ParsedUri, tls_options: &TlsOptions) -> Result<Self> {
        Self::connect_with_tls_and_age(uri, tls_options, false).await
    }

    /// Like [`connect_with_tls`](Self::connect_with_tls), with explicit Apache AGE intent
    /// carried separately from the URI.
    pub async fn connect_with_tls_and_age(
        uri: &ParsedUri,
        tls_options: &TlsOptions,
        age: bool,
    ) -> Result<Self> {
        let ssl_mode = SslMode::from_uri(uri)?;
        let negotiation = SslNegotiation::from_uri(uri, ssl_mode)?;
        if ssl_mode.may_use_tls() && !crate::tls::TLS_AVAILABLE {
            return Err(crate::tls::tls_unavailable(uri.protocol, &uri.host));
        }
        let tls_options = tls_options.clone().or(TlsOptions::from_params(&uri.params));
        let timeout_secs = uri
            .params
            .get("connect_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(5);
        let timeout_dur = std::time::Duration::from_secs(timeout_secs);
        match tokio::time::timeout(
            timeout_dur,
            Self::connect_raw(uri, ssl_mode, negotiation, &tls_options, age),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => Err(NetError::Timeout(format!(
                "Connection or handshake to PostgreSQL host at {} timed out after {:?}",
                uri.socket_addr(),
                timeout_dur
            ))),
        }
    }

    /// Returns whether the connection is encrypted with TLS.
    pub fn is_tls(&self) -> bool {
        self.stream.is_tls()
    }

    async fn connect_raw(
        uri: &ParsedUri,
        ssl_mode: SslMode,
        negotiation: SslNegotiation,
        tls: &TlsOptions,
        age: bool,
    ) -> Result<Self> {
        if ssl_mode == SslMode::Allow {
            // libpq `allow`: plaintext first; any failure of that attempt (rejected
            // startup, closed or reset connection, ...) retries with TLS.
            return match Self::connect_once(uri, ssl_mode, negotiation, tls, false, age).await {
                Err(plaintext_error) => {
                    debug!(
                        "plaintext attempt failed ({plaintext_error}); retrying with TLS (sslmode=allow)"
                    );
                    Self::connect_once(uri, ssl_mode, negotiation, tls, true, age)
                        .await
                        .map_err(|tls_error| {
                            append_detail(
                                tls_error,
                                &format!(
                                    "after the plaintext attempt failed (sslmode=allow): {plaintext_error}"
                                ),
                            )
                        })
                }
                ok => ok,
            };
        }
        Self::connect_once(
            uri,
            ssl_mode,
            negotiation,
            tls,
            ssl_mode.requests_tls_first(),
            age,
        )
        .await
    }

    /// Starts TLS right away, without an SSLRequest (`sslnegotiation=direct`, PostgreSQL 17+),
    /// offering the ALPN protocol `postgresql` and requiring the server to select it.
    async fn direct_tls(
        tcp: TcpStream,
        uri: &ParsedUri,
        ssl_mode: SslMode,
        tls: &TlsOptions,
    ) -> Result<NetStream> {
        let verification = ssl_mode.verification(tls.ca_file.is_some());
        crate::tls::upgrade_with_alpn(
            tcp,
            uri.protocol,
            &uri.host,
            verification,
            tls,
            Some(SslNegotiation::ALPN),
        )
        .await
        .map_err(|error| {
            append_detail(
                error,
                "(sslnegotiation=direct needs PostgreSQL 17 or later; older servers need sslnegotiation=postgres)",
            )
        })
    }

    /// Sends an SSLRequest and starts TLS on `S`, continues in plaintext on `N` for `prefer`,
    /// and fails otherwise.
    async fn negotiate_tls(
        mut tcp: TcpStream,
        uri: &ParsedUri,
        ssl_mode: SslMode,
        tls: &TlsOptions,
    ) -> Result<NetStream> {
        tcp.write_all(&SSL_REQUEST).await?;
        tcp.flush().await?;
        // Read whatever has arrived: the reply is one byte, and anything after it that the
        // server sent before the TLS handshake (or the plaintext startup) is rejected, as
        // libpq does, so injected bytes are never taken as part of the session.
        let mut reply = [0u8; 64];
        let received = tcp.read(&mut reply).await.map_err(|e| {
            NetError::ConnectionFailed(format!(
                "PostgreSQL host at {} closed the connection after SSLRequest: {e}",
                uri.socket_addr()
            ))
        })?;
        if received == 0 {
            return Err(NetError::ConnectionFailed(format!(
                "PostgreSQL host at {} closed the connection after SSLRequest: early eof",
                uri.socket_addr()
            )));
        }
        let (tcp, pending) = has_pending_data(tcp)?;
        if matches!(reply[0], b'S' | b'N') && (received > 1 || pending) {
            return Err(NetError::ProtocolError(format!(
                "PostgreSQL host at {} sent unexpected data after its SSLRequest reply '{}' (before the TLS handshake); refusing the connection, it may have been tampered with",
                uri.socket_addr(),
                char::from(reply[0])
            )));
        }
        match reply[0] {
            b'S' => {
                let verification = ssl_mode.verification(tls.ca_file.is_some());
                crate::tls::upgrade(tcp, uri.protocol, &uri.host, verification, tls).await
            }
            b'N' if ssl_mode.accepts_plaintext_fallback() => {
                debug!(
                    "PostgreSQL host at {} does not support TLS; continuing in plaintext (sslmode=prefer)",
                    uri.socket_addr()
                );
                Ok(NetStream::Plain(tcp))
            }
            b'N' => Err(NetError::TlsError(format!(
                "PostgreSQL host at {} does not support TLS (it answered the SSLRequest with 'N'), but sslmode={} requires it",
                uri.socket_addr(),
                ssl_mode.as_str()
            ))),
            other => Err(NetError::ProtocolError(format!(
                "unexpected reply 0x{other:02x} to SSLRequest from PostgreSQL host at {}",
                uri.socket_addr()
            ))),
        }
    }

    async fn connect_once(
        uri: &ParsedUri,
        ssl_mode: SslMode,
        negotiation: SslNegotiation,
        tls: &TlsOptions,
        use_tls: bool,
        age: bool,
    ) -> Result<Self> {
        let socket_addr = uri.socket_addr();
        let stream = TcpStream::connect(&socket_addr).await.map_err(|e| {
            NetError::ConnectionFailed(format!(
                "Failed to connect to PostgreSQL host at {}: {}",
                socket_addr, e
            ))
        })?;

        // Optimize latency: disable Nagle's algorithm
        let _ = stream.set_nodelay(true);

        let stream = if use_tls && negotiation == SslNegotiation::Direct {
            Self::direct_tls(stream, uri, ssl_mode, tls).await?
        } else if use_tls {
            Self::negotiate_tls(stream, uri, ssl_mode, tls).await?
        } else {
            NetStream::Plain(stream)
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
            server_parameters: HashMap::new(),
            backend_pid: 0,
            secret_key: 0,
            transaction_status: TransactionStatus::Idle,
            is_age_initialized: false,
            agtype_oid: None,
            query_timeout,
            last_command_tag: None,
        };

        // Perform protocol handshake & authentication
        conn.handshake().await?;

        // If the caller, target protocol or parameters indicate Apache AGE, initialize AGE extension
        if age
            || uri.raw.starts_with("age:")
            || uri.params.contains_key("graph")
            || uri.params.contains_key("graph_name")
            || uri.params.contains_key("age")
            || uri.params.get("dialect").map(|s| s.as_str()) == Some("age")
        {
            conn.init_age().await?;
        }

        Ok(conn)
    }

    /// Performs PostgreSQL Frontend/Backend Protocol v3.0 startup and authentication handshake.
    async fn handshake(&mut self) -> Result<()> {
        let username = match &self.uri.auth {
            Auth::Basic { username, .. } => username.clone(),
            _ => "postgres".to_string(),
        };

        let password = match &self.uri.auth {
            Auth::Basic { password, .. } => password.clone(),
            _ => String::new(),
        };

        let database = self.uri.database.as_deref().unwrap_or("postgres");
        let startup = StartupMessage::new(&username, Some(database));

        write_startup_message(&mut self.stream, &startup).await?;

        let mut scram_client: Option<ScramClient> = None;

        loop {
            let msg = read_backend_message(&mut self.stream).await?;
            match msg {
                BackendMessage::Authentication(auth_req) => match auth_req {
                    AuthenticationRequest::Ok => {
                        // Authentication succeeded, continue waiting for ReadyForQuery
                    }
                    AuthenticationRequest::CleartextPassword => {
                        let mut pwd_bytes = password.as_bytes().to_vec();
                        pwd_bytes.push(0);
                        write_frontend_message(
                            &mut self.stream,
                            &FrontendMessage::Password(pwd_bytes),
                        )
                        .await?;
                    }
                    AuthenticationRequest::MD5Password { salt } => {
                        let md5_bytes = compute_md5_password(&username, &password, &salt);
                        write_frontend_message(
                            &mut self.stream,
                            &FrontendMessage::Password(md5_bytes),
                        )
                        .await?;
                    }
                    AuthenticationRequest::SASL { mechanisms } => {
                        if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                            return Err(NetError::AuthenticationFailed(format!(
                                "No supported SASL mechanism offered by server: {:?}",
                                mechanisms
                            )));
                        }
                        let client = ScramClient::new();
                        let first_msg = client.client_first_message();
                        scram_client = Some(client);

                        write_frontend_message(
                            &mut self.stream,
                            &FrontendMessage::SASLInitialResponse {
                                mechanism: "SCRAM-SHA-256".to_string(),
                                data: Some(first_msg),
                            },
                        )
                        .await?;
                    }
                    AuthenticationRequest::SASLContinue(challenge) => {
                        let client = scram_client.as_mut().ok_or_else(|| {
                            NetError::AuthenticationFailed("SCRAM client state missing".to_string())
                        })?;
                        let response = client.process_challenge(&challenge, &password)?;
                        write_frontend_message(
                            &mut self.stream,
                            &FrontendMessage::SASLResponse(response),
                        )
                        .await?;
                    }
                    AuthenticationRequest::SASLFinal(final_data) => {
                        let client = scram_client.as_ref().ok_or_else(|| {
                            NetError::AuthenticationFailed("SCRAM client state missing".to_string())
                        })?;
                        client.verify_server_final(&final_data)?;
                    }
                    other => {
                        return Err(NetError::AuthenticationFailed(format!(
                            "Unsupported authentication request: {:?}",
                            other
                        )));
                    }
                },
                BackendMessage::ParameterStatus { name, value } => {
                    self.server_parameters.insert(name, value);
                }
                BackendMessage::BackendKeyData {
                    process_id,
                    secret_key,
                } => {
                    self.backend_pid = process_id;
                    self.secret_key = secret_key;
                }
                BackendMessage::ReadyForQuery(status) => {
                    self.transaction_status = status;
                    break;
                }
                BackendMessage::ErrorResponse(diag) => {
                    return Err(NetError::AuthenticationFailed(diag.format_error()));
                }
                BackendMessage::NoticeResponse(_) => {
                    // Ignore notices during handshake
                }
                other => {
                    return Err(NetError::ProtocolError(format!(
                        "Unexpected backend message during startup handshake: {:?}",
                        other
                    )));
                }
            }
        }

        Ok(())
    }

    /// Initializes Apache AGE extension and search path if not already loaded.
    pub async fn init_age(&mut self) -> Result<()> {
        if self.is_age_initialized {
            return Ok(());
        }

        // Execute LOAD 'age'; SET search_path = ag_catalog, "$user", public;
        self.execute_simple("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
            .await?;
        self.is_age_initialized = true;
        self.agtype_oid = self.resolve_agtype_oid().await?;
        Ok(())
    }

    /// Looks up the Apache AGE `agtype` type OID on this server.
    ///
    /// The OID is installation-specific, so it is read from the catalog instead
    /// of hard-coded. Failure to resolve it leaves decoding disabled rather than
    /// failing the connection.
    async fn resolve_agtype_oid(&mut self) -> Result<Option<i32>> {
        let result = self
            .execute_simple_inner(
                "SELECT oid FROM pg_catalog.pg_type                  WHERE typname = 'agtype' AND typnamespace = 'ag_catalog'::regnamespace;",
            )
            .await?;
        let Some(batch) = result.batches.first() else {
            return Ok(None);
        };
        let Some(array) = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
        else {
            return Ok(None);
        };
        if array.is_empty() || array.is_null(0) {
            return Ok(None);
        }
        Ok(Some(array.value(0) as i32))
    }

    /// Marks result columns whose PostgreSQL type is `agtype`.
    fn age_columns(&self, field_types: &[i32]) -> Vec<bool> {
        match self.agtype_oid {
            Some(oid) => field_types.iter().map(|oid_i| *oid_i == oid).collect(),
            None => Vec::new(),
        }
    }

    /// Executes a simple query string via PostgreSQL Simple Query Protocol (`'Q'`).
    pub async fn execute_simple(&mut self, query: &str) -> Result<QueryResult> {
        if let Some(to) = self.query_timeout {
            match tokio::time::timeout(to, self.execute_simple_inner(query)).await {
                Ok(res) => res,
                Err(_) => {
                    self.transaction_status = TransactionStatus::FailedTransaction;
                    Err(NetError::Timeout(format!(
                        "Query execution timed out after {:?}",
                        to
                    )))
                }
            }
        } else {
            self.execute_simple_inner(query).await
        }
    }

    async fn execute_simple_inner(&mut self, query: &str) -> Result<QueryResult> {
        let start_time = Instant::now();
        write_frontend_message(&mut self.stream, &FrontendMessage::Query(query.to_string()))
            .await?;

        let mut columns = Vec::new();
        let mut field_types: Vec<i32> = Vec::new();
        let mut field_type_modifiers: Vec<i32> = Vec::new();
        let mut rows: Vec<Vec<Option<String>>> = Vec::new();
        let mut summary = QuerySummary::empty();
        let mut query_error = None;

        loop {
            let msg = read_backend_message(&mut self.stream).await?;
            match msg {
                BackendMessage::RowDescription(fields) => {
                    field_types = fields.iter().map(|f| f.type_oid).collect();
                    field_type_modifiers = fields.iter().map(|f| f.type_modifier).collect();
                    columns = fields.into_iter().map(|f| f.name).collect();
                }
                BackendMessage::DataRow(vals) => {
                    let mut row = Vec::with_capacity(vals.len());
                    for val in vals {
                        match val {
                            Some(bytes) => {
                                let s = String::from_utf8_lossy(&bytes).into_owned();
                                row.push(Some(s));
                            }
                            None => row.push(None),
                        }
                    }
                    rows.push(row);
                }
                BackendMessage::CommandComplete(tag) => {
                    self.last_command_tag = Some(tag.clone());
                    parse_command_tag(&tag, &mut summary);
                }
                BackendMessage::ReadyForQuery(status) => {
                    self.transaction_status = status;
                    break;
                }
                BackendMessage::ErrorResponse(diag) => {
                    query_error = Some(NetError::ProtocolError(diag.format_error()));
                }
                BackendMessage::EmptyQueryResponse => {}
                BackendMessage::NoticeResponse(_) => {}
                BackendMessage::ParameterStatus { name, value } => {
                    self.server_parameters.insert(name, value);
                }
                other => {
                    tracing::trace!("Ignored message during simple query: {:?}", other);
                }
            }
        }

        if let Some(err) = query_error {
            return Err(err);
        }

        summary.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let age_cols = self.age_columns(&field_types);
        let batch = rows_to_record_batch_with_column_metadata(
            &columns,
            &rows,
            &age_cols,
            &field_types,
            &field_type_modifiers,
        )?;
        let batches = if batch.num_rows() > 0 || !columns.is_empty() {
            vec![batch]
        } else {
            Vec::new()
        };

        Ok(QueryResult::new(columns, batches, summary))
    }

    /// Executes a parameterized statement via the PostgreSQL extended query protocol.
    ///
    /// Sends `Parse` / `Bind` / `Describe` / `Execute` / `Sync` in one flush and
    /// drains responses through `ReadyForQuery`, including after an
    /// `ErrorResponse`, so the connection remains usable and transaction state
    /// reflects the server's report.
    async fn execute_extended_inner(
        &mut self,
        prepared: &PreparedPostgresStatement,
    ) -> Result<QueryResult> {
        let start_time = Instant::now();
        let parameter_count = prepared.parameter_values.len();

        let messages = [
            FrontendMessage::Parse {
                name: String::new(),
                query: prepared.statement.clone(),
                param_types: prepared.parameter_oids.clone(),
            },
            FrontendMessage::Bind {
                portal: String::new(),
                statement: String::new(),
                param_formats: vec![0; parameter_count],
                params: prepared.parameter_values.clone(),
                result_formats: vec![0],
            },
            FrontendMessage::Describe {
                target_type: b'P',
                name: String::new(),
            },
            FrontendMessage::Execute {
                portal: String::new(),
                max_rows: 0,
            },
            FrontendMessage::Sync,
        ];
        write_frontend_messages(&mut self.stream, &messages).await?;

        let response = read_extended_response(&mut self.stream).await?;
        self.transaction_status = response.transaction_status;
        for (name, value) in response.parameter_status {
            self.server_parameters.insert(name, value);
        }
        if let Some(tag) = response.command_tag {
            self.last_command_tag = Some(tag);
        }

        if let Some(err) = response.error {
            return Err(err);
        }

        let mut summary = response.summary;
        summary.execution_time_ms = start_time.elapsed().as_millis() as u64;

        let age_cols = self.age_columns(&response.field_types);
        let batch = rows_to_record_batch_with_column_metadata(
            &response.columns,
            &response.rows,
            &age_cols,
            &response.field_types,
            &response.field_type_modifiers,
        )?;
        let batches = if batch.num_rows() > 0 || !response.columns.is_empty() {
            vec![batch]
        } else {
            Vec::new()
        };

        Ok(QueryResult::new(response.columns, batches, summary))
    }

    /// Executes a prepared statement with the configured query timeout.
    async fn execute_extended(
        &mut self,
        prepared: &PreparedPostgresStatement,
    ) -> Result<QueryResult> {
        if let Some(to) = self.query_timeout {
            match tokio::time::timeout(to, self.execute_extended_inner(prepared)).await {
                Ok(res) => res,
                Err(_) => {
                    self.transaction_status = TransactionStatus::FailedTransaction;
                    Err(NetError::Timeout(format!(
                        "Query execution timed out after {:?}",
                        to
                    )))
                }
            }
        } else {
            self.execute_extended_inner(prepared).await
        }
    }

    /// Returns the active query timeout.
    pub fn query_timeout(&self) -> Option<std::time::Duration> {
        self.query_timeout
    }

    /// Sets the query timeout for this connection.
    pub fn set_query_timeout(&mut self, timeout: Option<std::time::Duration>) {
        self.query_timeout = timeout;
    }

    /// Returns the active transaction status reported by PostgreSQL.
    pub fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    /// Returns the command tag of the most recently completed query.
    pub fn last_command_tag(&self) -> Option<&str> {
        self.last_command_tag.as_deref()
    }

    /// Returns the server parameters collected during handshake.
    pub fn server_parameters(&self) -> &HashMap<String, String> {
        &self.server_parameters
    }

    /// Returns the backend process ID.
    pub fn backend_pid(&self) -> i32 {
        self.backend_pid
    }
}

#[async_trait]
impl AsyncConnection for PostgresConnection {
    async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        let prepared = prepare_postgres_statement(query, params)?;
        if !prepared.is_parameterized() {
            return self.execute_simple(&prepared.statement).await;
        }
        self.execute_extended(&prepared).await
    }

    async fn ping(&mut self) -> Result<()> {
        self.execute_simple("SELECT 1;").await?;
        Ok(())
    }

    async fn reset(&mut self) -> Result<()> {
        if self.transaction_status != TransactionStatus::Idle {
            let _ = self.execute_simple("ROLLBACK;").await;
        }
        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.transaction_status != TransactionStatus::FailedTransaction
    }

    fn is_in_transaction(&self) -> bool {
        self.transaction_status == TransactionStatus::InTransaction
    }

    fn can_rollback(&self) -> bool {
        self.transaction_status != TransactionStatus::Idle
    }

    async fn close(&mut self) -> Result<()> {
        let _ = write_frontend_message(&mut self.stream, &FrontendMessage::Terminate).await;
        Ok(())
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        // The AGE setup (`LOAD 'age'; SET search_path = ...`) is executed as a
        // separate simple query before this point, so it is committed and cannot
        // be undone by rolling back this explicit transaction.
        if self.is_age_initialized {
            self.init_age().await?;
        }
        self.execute_simple("BEGIN;").await?;
        Ok(())
    }

    async fn commit_transaction(&mut self) -> Result<()> {
        if self.transaction_status == TransactionStatus::FailedTransaction {
            return Err(NetError::TransactionError(
                "transaction is aborted; roll back instead of committing".to_string(),
            ));
        }
        self.last_command_tag = None;
        self.execute_simple("COMMIT;").await?;
        match self.last_command_tag.as_deref() {
            Some(tag) if tag.eq_ignore_ascii_case("COMMIT") => Ok(()),
            Some(tag) if tag.eq_ignore_ascii_case("ROLLBACK") => Err(NetError::TransactionError(
                "transaction is aborted; PostgreSQL converted COMMIT into ROLLBACK".to_string(),
            )),
            other => Err(NetError::ProtocolError(format!(
                "Unexpected command tag after COMMIT: {other:?}"
            ))),
        }
    }

    async fn rollback_transaction(&mut self) -> Result<()> {
        self.execute_simple("ROLLBACK;").await?;
        Ok(())
    }

    async fn create_savepoint(&mut self, name: &str) -> Result<()> {
        validate_savepoint_name(name)?;
        self.execute_simple(&format!("SAVEPOINT {};", name)).await?;
        Ok(())
    }

    async fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        validate_savepoint_name(name)?;
        self.execute_simple(&format!("ROLLBACK TO SAVEPOINT {};", name))
            .await?;
        Ok(())
    }

    async fn release_savepoint(&mut self, name: &str) -> Result<()> {
        validate_savepoint_name(name)?;
        self.execute_simple(&format!("RELEASE SAVEPOINT {};", name))
            .await?;
        Ok(())
    }

    fn supports_savepoints(&self) -> bool {
        true
    }
}

/// Validates that a savepoint name is a safe unquoted SQL identifier.
pub(crate) fn validate_savepoint_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let valid_start = chars
        .next()
        .map(|c| c.is_ascii_alphabetic() || c == '_')
        .unwrap_or(false);
    let valid_rest = chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid_start && valid_rest {
        Ok(())
    } else {
        Err(NetError::TransactionError(format!(
            "Invalid savepoint name '{name}'; expected an unquoted SQL identifier"
        )))
    }
}

/// Fully drained response to an extended-query cycle.
struct ExtendedResponse {
    columns: Vec<String>,
    field_types: Vec<i32>,
    field_type_modifiers: Vec<i32>,
    rows: Vec<Vec<Option<String>>>,
    summary: QuerySummary,
    command_tag: Option<String>,
    error: Option<NetError>,
    transaction_status: TransactionStatus,
    parameter_status: Vec<(String, String)>,
}

/// Reads extended-query responses until `ReadyForQuery`, even after an error.
async fn read_extended_response<R>(reader: &mut R) -> Result<ExtendedResponse>
where
    R: AsyncRead + Unpin,
{
    let mut response = ExtendedResponse {
        columns: Vec::new(),
        field_types: Vec::new(),
        field_type_modifiers: Vec::new(),
        rows: Vec::new(),
        summary: QuerySummary::empty(),
        command_tag: None,
        error: None,
        transaction_status: TransactionStatus::Idle,
        parameter_status: Vec::new(),
    };

    loop {
        let msg = read_backend_message(reader).await?;
        match msg {
            BackendMessage::ParseComplete
            | BackendMessage::BindComplete
            | BackendMessage::CloseComplete
            | BackendMessage::NoData
            | BackendMessage::EmptyQueryResponse => {}
            BackendMessage::RowDescription(fields) => {
                response.field_types = fields.iter().map(|f| f.type_oid).collect();
                response.field_type_modifiers = fields.iter().map(|f| f.type_modifier).collect();
                response.columns = fields.into_iter().map(|f| f.name).collect();
            }
            BackendMessage::DataRow(vals) => {
                let mut row = Vec::with_capacity(vals.len());
                for val in vals {
                    match val {
                        Some(bytes) => {
                            let s = String::from_utf8_lossy(&bytes).into_owned();
                            row.push(Some(s));
                        }
                        None => row.push(None),
                    }
                }
                response.rows.push(row);
            }
            BackendMessage::CommandComplete(tag) => {
                parse_command_tag(&tag, &mut response.summary);
                response.command_tag = Some(tag);
            }
            BackendMessage::ReadyForQuery(status) => {
                response.transaction_status = status;
                break;
            }
            BackendMessage::ErrorResponse(diag) => {
                // Keep draining until ReadyForQuery: the server discards the
                // rest of this extended-protocol cycle and reports the final
                // transaction state there.
                if response.error.is_none() {
                    response.error = Some(NetError::ProtocolError(diag.format_error()));
                }
            }
            BackendMessage::NoticeResponse(_) => {}
            BackendMessage::ParameterStatus { name, value } => {
                response.parameter_status.push((name, value));
            }
            other => {
                tracing::trace!("Ignored message during extended query: {:?}", other);
            }
        }
    }

    Ok(response)
}

/// Parses PostgreSQL command tag (e.g. `INSERT 0 1`, `DELETE 5`, `UPDATE 2`) into summary metrics.
fn parse_command_tag(tag: &str, summary: &mut QuerySummary) {
    let parts: Vec<&str> = tag.split_whitespace().collect();
    if parts.is_empty() {
        return;
    }

    match parts[0] {
        "INSERT" => {
            if let Some(count_str) = parts.get(2)
                && let Ok(c) = count_str.parse::<u64>()
            {
                summary.nodes_created += c;
            }
        }
        "DELETE" => {
            if let Some(count_str) = parts.get(1)
                && let Ok(c) = count_str.parse::<u64>()
            {
                summary.nodes_deleted += c;
            }
        }
        "UPDATE" => {
            if let Some(count_str) = parts.get(1)
                && let Ok(c) = count_str.parse::<u64>()
            {
                summary.properties_set += c;
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncWriteExt, duplex};

    fn frame(msg_type: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(msg_type);
        out.extend_from_slice(&(4 + payload.len() as i32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn error_frame(message: &str) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(b"SERROR\0");
        payload.extend_from_slice(b"C42601\0");
        payload.push(b'M');
        payload.extend_from_slice(message.as_bytes());
        payload.push(0);
        payload.push(0);
        frame(b'E', &payload)
    }

    fn ready_frame(status: u8) -> Vec<u8> {
        frame(b'Z', &[status])
    }

    #[tokio::test]
    async fn extended_error_drains_to_ready_for_query_and_recovers() {
        let (mut client, mut server) = duplex(4096);

        let mut first_cycle = Vec::new();
        first_cycle.extend_from_slice(&frame(b'1', &[])); // ParseComplete
        first_cycle.extend_from_slice(&frame(b'2', &[])); // BindComplete
        first_cycle.extend_from_slice(&error_frame("syntax error at or near \"%\""));
        first_cycle.extend_from_slice(&ready_frame(b'E')); // failed transaction
        server.write_all(&first_cycle).await.unwrap();

        let first = read_extended_response(&mut client).await.unwrap();
        assert!(first.error.is_some());
        assert!(
            first
                .error
                .as_ref()
                .unwrap()
                .to_string()
                .contains("syntax error")
        );
        assert_eq!(
            first.transaction_status,
            TransactionStatus::FailedTransaction
        );
        assert!(first.columns.is_empty());

        let mut second_cycle = Vec::new();
        // RowDescription with one text column "n"
        let mut row_desc = Vec::new();
        row_desc.extend_from_slice(&1i16.to_be_bytes());
        row_desc.extend_from_slice(b"n\0");
        row_desc.extend_from_slice(&0i32.to_be_bytes()); // table oid
        row_desc.extend_from_slice(&0i16.to_be_bytes()); // column attr
        row_desc.extend_from_slice(&25i32.to_be_bytes()); // text
        row_desc.extend_from_slice(&(-1i16).to_be_bytes()); // varlena
        row_desc.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        row_desc.extend_from_slice(&0i16.to_be_bytes()); // text format
        second_cycle.extend_from_slice(&frame(b'T', &row_desc));

        let mut data_row = Vec::new();
        data_row.extend_from_slice(&1i16.to_be_bytes());
        data_row.extend_from_slice(&3i32.to_be_bytes());
        data_row.extend_from_slice(b"one");
        second_cycle.extend_from_slice(&frame(b'D', &data_row));
        second_cycle.extend_from_slice(&frame(b'C', b"SELECT 1\0"));
        second_cycle.extend_from_slice(&ready_frame(b'I'));
        server.write_all(&second_cycle).await.unwrap();

        let second = read_extended_response(&mut client).await.unwrap();
        assert!(second.error.is_none());
        assert_eq!(second.transaction_status, TransactionStatus::Idle);
        assert_eq!(second.columns, vec!["n".to_string()]);
        assert_eq!(second.rows, vec![vec![Some("one".to_string())]]);
        assert_eq!(second.command_tag.as_deref(), Some("SELECT 1"));
    }
}
