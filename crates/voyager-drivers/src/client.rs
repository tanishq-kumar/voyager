//! High-level unified asynchronous client with connection pooling across Bolt, PostgreSQL, and Redis/FalkorDB.

use std::collections::HashMap;

#[cfg(feature = "bolt")]
use crate::bolt::BoltConnection;
#[cfg(feature = "bolt")]
use crate::config::ConnectionConfig;
use crate::config::PoolConfig;
use crate::engine::{AsyncConnection, QueryResult};
use crate::error::{NetError, Result};
use crate::native_transaction::NativeTransaction;
use crate::pool::ConnectionPool;
#[cfg(feature = "postgres")]
use crate::postgres::PostgresConnection;
#[cfg(feature = "redis")]
use crate::redis::RedisConnection;
use crate::uri::{DatabaseProtocol, ParsedUri};

/// Extra connection intent that is not encoded in the URI.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConnectOptions {
    /// Run Apache AGE setup (`LOAD 'age'; SET search_path = ...`) on every
    /// PostgreSQL connection, even when the URI has no AGE scheme or parameters.
    pub age: bool,
}

/// Asynchronously connects to any supported database protocol based on the URI scheme.
///
/// TLS URIs (`bolt+s`, `rediss`, `?sslmode=...`, ...) connect over TLS when the `tls`
/// feature is enabled and fail with [`NetError::TlsError`] naming the feature otherwise.
pub async fn connect_any(uri: &str) -> Result<Box<dyn AsyncConnection>> {
    connect_any_with_options(uri, ConnectOptions::default()).await
}

/// Like [`connect_any`], with extra connection intent carried separately from the URI.
pub async fn connect_any_with_options(
    uri: &str,
    options: ConnectOptions,
) -> Result<Box<dyn AsyncConnection>> {
    // Parsing already rejects protocols whose Cargo feature is disabled, with an
    // error that names the missing feature.
    let parsed = ParsedUri::parse(uri)?;
    match parsed.protocol {
        DatabaseProtocol::Bolt => {
            #[cfg(feature = "bolt")]
            {
                let config = ConnectionConfig::from_uri(uri);
                let conn = BoltConnection::connect(&config).await?;
                Ok(Box::new(conn))
            }
            #[cfg(not(feature = "bolt"))]
            {
                Err(NetError::ProtocolError(
                    "voyager-drivers was built without the `bolt` feature; cannot open Bolt connections"
                        .to_string(),
                ))
            }
        }
        DatabaseProtocol::Postgres => {
            #[cfg(feature = "postgres")]
            {
                let conn = PostgresConnection::connect_with_age(&parsed, options.age).await?;
                Ok(Box::new(conn))
            }
            #[cfg(not(feature = "postgres"))]
            {
                // AGE setup only applies to PostgreSQL connections.
                let _ = options;
                Err(NetError::ProtocolError(
                    "voyager-drivers was built without the `postgres` feature; cannot open PostgreSQL connections"
                        .to_string(),
                ))
            }
        }
        DatabaseProtocol::Redis => {
            #[cfg(feature = "redis")]
            {
                let conn = RedisConnection::connect(&parsed).await?;
                Ok(Box::new(conn))
            }
            #[cfg(not(feature = "redis"))]
            {
                Err(NetError::ProtocolError(
                    "voyager-drivers was built without the `redis` feature; cannot open Redis/FalkorDB connections"
                        .to_string(),
                ))
            }
        }
        DatabaseProtocol::DuckDb => Err(NetError::ProtocolError(
            "Embedded DuckDB does not use TCP network connections; use voyager's DuckDbBridge instead"
                .to_string(),
        )),
    }
}

/// Unified high-performance database client managing a pooled connection.
#[derive(Clone)]
pub struct NativeClient {
    pool: ConnectionPool<Box<dyn AsyncConnection>>,
    uri: String,
    protocol: DatabaseProtocol,
}

impl NativeClient {
    /// Creates a new `NativeClient` connecting to the given URI.
    pub fn new(uri: &str, pool_config: Option<PoolConfig>) -> Result<Self> {
        Self::new_with_options(uri, pool_config, ConnectOptions::default())
    }

    /// Creates a new `NativeClient` with extra connection intent carried separately
    /// from the URI. Every pooled or reconnected connection receives the same intent.
    pub fn new_with_options(
        uri: &str,
        pool_config: Option<PoolConfig>,
        options: ConnectOptions,
    ) -> Result<Self> {
        let parsed = ParsedUri::parse(uri)?;
        let protocol = parsed.protocol;
        let uri_owned = uri.to_string();
        let uri_factory = uri_owned.clone();
        let config = pool_config.unwrap_or_default();

        let pool = ConnectionPool::from_fn(config, move || {
            let u = uri_factory.clone();
            async move { connect_any_with_options(&u, options).await }
        });

        Ok(Self {
            pool,
            uri: uri_owned,
            protocol,
        })
    }

    /// Returns the target connection URI.
    pub fn uri(&self) -> &str {
        &self.uri
    }

    /// Returns the active database protocol.
    pub fn protocol(&self) -> DatabaseProtocol {
        self.protocol
    }

    /// Returns a reference to the underlying connection pool.
    pub fn pool(&self) -> &ConnectionPool<Box<dyn AsyncConnection>> {
        &self.pool
    }

    /// Executes a parameterized query using an automatically leased connection from the pool.
    pub async fn execute(
        &self,
        query: &str,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<QueryResult> {
        let mut conn = self.pool.acquire().await?;
        conn.execute(query, params).await
    }

    /// Leases one pooled connection and begins an explicit transaction on it.
    pub async fn begin_transaction(&self) -> Result<NativeTransaction<Box<dyn AsyncConnection>>> {
        let conn = self.pool.acquire().await?;
        NativeTransaction::begin(conn).await
    }

    /// Sends a lightweight liveness probe ping to the database.
    pub async fn ping(&self) -> Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.ping().await
    }

    /// Closes the connection pool and evicts all active connections.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}
