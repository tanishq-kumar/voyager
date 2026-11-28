#![doc = include_str!("../README.md")]
//!
//! # Architecture
//! - [`ConnectionPool`]: Thread-safe, generic connection pool with RAII leasing guards and liveness probes.
//! - [`ParsedUri`]: Structured URI parser for Bolt, PostgreSQL, Redis/FalkorDB, and DuckDB endpoints.
//! - [`AsyncEngine`], [`AsyncConnection`], [`AsyncTransaction`]: Core asynchronous database abstractions.
//! - [`QueryResult`], [`QuerySummary`]: High-performance columnar result containers using Apache Arrow.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod bolt;
pub mod circuit;
pub mod client;
pub mod config;
pub mod engine;
pub mod error;
pub mod introspect;
#[cfg(feature = "testkit")]
pub mod mock;
pub mod native_transaction;
pub mod params;
pub mod pool;
#[cfg(feature = "postgres")]
pub mod postgres;
mod redact;
#[cfg(feature = "redis")]
pub mod redis;
pub mod stream;
pub mod tls;
pub mod uri;
pub mod value;

// Re-exports
#[cfg(feature = "testkit")]
pub use bolt::BoltStubServer;
#[cfg(feature = "bolt")]
pub use bolt::{BoltConnection, BoltRequest, BoltResponse, BoltStreamCursor, BoltVersion};
pub use bolt::{
    BoltNode, BoltPath, BoltRelationship, BoltUnboundRelationship, BoltValue, PackStream,
    bolt_node_to_canonical_json, bolt_path_to_canonical_json, bolt_relationship_to_canonical_json,
    bolt_value_to_canonical_json, build_bolt_record_batch,
};
pub use circuit::{
    BackendRoute, CircuitConfig, CircuitRouter, CircuitSnapshot, CircuitState, ErrorClassification,
    classify_error, classify_net_error, is_semantic_error,
};
pub use client::{ConnectOptions, NativeClient, connect_any, connect_any_with_options};
pub use config::{Auth, ConnectionConfig, PoolConfig, TlsMode};
pub use engine::{
    AsyncConnection, AsyncEngine, AsyncTransaction, ConnectionFactory, FnConnectionFactory,
    QueryResult, QuerySummary, ResultMode,
};
pub use error::{NetError, Result};
pub use introspect::{GraphSchema, IntrospectOptions, introspect};
#[cfg(feature = "testkit")]
pub use mock::{MockConnection, MockConnectionFactory, MockEngine};
pub use native_transaction::{NativeTransaction, NativeTransactionState};
pub use params::{ParamTimezone, ParamValue};
pub use pool::{ConnectionPool, PoolMetricsSnapshot, PooledConnection};
#[cfg(feature = "postgres")]
pub use postgres::{
    AgeEdge, AgePath, AgeValue, AgeVertex, AuthenticationRequest, BackendMessage, FieldDescription,
    FrontendMessage, PgDiagnostic, PostgresConnection, PostgresTransaction, ScramClient,
    StartupMessage, TransactionStatus, age_value_to_arrow_json, age_value_to_canonical_json,
    clean_agtype_string, compute_md5_password, parse_agtype, rows_to_record_batch,
    rows_to_record_batch_with_age_columns,
};
#[cfg(feature = "redis")]
pub use redis::{
    FalkorNode, FalkorQueryResult, FalkorRelationship, FalkorStatistics, RedisConnection,
    RedisTransaction, RespCodec, RespValue,
};
pub use stream::{
    BufferedStreamCursor, DEFAULT_STREAM_FETCH_SIZE, ResultChunk, ResultStream, StreamCursor,
    StreamOptions, TransactionStream,
};
pub use tls::{NetStream, SslMode, SslNegotiation, TLS_AVAILABLE, TlsOptions, TlsVerification};
pub use uri::{DatabaseProtocol, ParsedUri};
pub use value::{ResultValue, values_to_record_batch};
