//! Asynchronous database engine, connection, and transaction traits for Voyager OGM.

use arrow_array::RecordBatch;
#[cfg(feature = "arrow")]
use arrow_select::concat::concat_batches;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::bolt::BoltValue;
use crate::error::{NetError, Result};
use crate::params::ParamValue;
use crate::stream::{ResultChunk, StreamCursor, StreamOptions};
use crate::value::arrow::{ColumnHint, values_to_record_batch_with_hints};
use crate::value::result::ResultValue;

/// Selects the primary in-memory representation produced by query execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ResultMode {
    /// Build Apache Arrow `RecordBatch`es directly without retaining `result_rows`
    /// or `bolt_rows`. Used by `execute_arrow()` / `execute_to_polars()`.
    #[default]
    Arrow,
    /// Retain decoded [`ResultValue`] rows without constructing an Apache Arrow
    /// `RecordBatch`. Arrow can still be materialized lazily via
    /// [`QueryResult::to_record_batch`]. Used by OLTP `execute_rows()`.
    Rows,
}

/// Execution summary metrics and mutation counters returned after query execution.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySummary {
    /// Number of graph nodes created by the query.
    pub nodes_created: u64,
    /// Number of graph nodes deleted by the query.
    pub nodes_deleted: u64,
    /// Number of graph relationships created by the query.
    pub relationships_created: u64,
    /// Number of graph relationships deleted by the query.
    pub relationships_deleted: u64,
    /// Number of properties set or updated on nodes/relationships.
    pub properties_set: u64,
    /// Number of node labels added.
    pub labels_added: u64,
    /// Number of node labels removed.
    pub labels_removed: u64,
    /// Number of schema indexes created.
    pub indexes_added: u64,
    /// Number of schema constraints created.
    pub constraints_added: u64,
    /// Total server-side query planning and execution duration in milliseconds.
    pub execution_time_ms: u64,
}

impl QuerySummary {
    /// Creates an empty summary with all counters initialized to zero.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns `true` if any mutating counters (creates, updates, deletes) are non-zero.
    pub fn is_mutating(&self) -> bool {
        self.nodes_created > 0
            || self.nodes_deleted > 0
            || self.relationships_created > 0
            || self.relationships_deleted > 0
            || self.properties_set > 0
            || self.labels_added > 0
            || self.labels_removed > 0
            || self.indexes_added > 0
            || self.constraints_added > 0
    }
}

/// Query result container containing projected column names, Apache Arrow record batches, and execution statistics.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QueryResult {
    /// Ordered list of projected return column names.
    pub columns: Vec<String>,
    /// Apache Arrow record batches containing tabular columnar results.
    pub batches: Vec<RecordBatch>,
    /// Server-side execution statistics and mutation counters.
    pub summary: QuerySummary,
    /// Per-column flags marking Apache AGE `agtype` columns whose text values
    /// still need decoding before they reach Python.
    pub age_columns: Vec<bool>,
    /// Optional raw Bolt rows retained alongside Arrow batches so nested
    /// entities, maps, and mixed lists preserve full temporal/binary/float fidelity.
    pub bolt_rows: Option<Arc<Vec<Vec<BoltValue>>>>,
    /// Optional pre-converted [`ResultValue`] rows retained alongside Arrow batches
    /// so Bolt/FalkorDB results avoid a second `BoltValue` -> `ResultValue` copy in PyO3.
    pub result_rows: Option<Arc<Vec<Vec<ResultValue>>>>,
    /// Per-column Arrow conversion hints preserved alongside `result_rows`
    /// so lazy Arrow materialization matches `ResultMode::Arrow` identically.
    pub column_hints: Vec<ColumnHint>,
}

impl QueryResult {
    /// Creates a new `QueryResult` with the given columns, record batches, and summary.
    pub fn new(columns: Vec<String>, batches: Vec<RecordBatch>, summary: QuerySummary) -> Self {
        Self {
            columns,
            batches,
            summary,
            age_columns: Vec::new(),
            bolt_rows: None,
            result_rows: None,
            column_hints: Vec::new(),
        }
    }

    /// Creates a row-backed `QueryResult` without constructing an Apache Arrow [`RecordBatch`].
    ///
    /// Arrow can be materialized lazily later via [`Self::to_record_batch`].
    pub fn from_rows(
        columns: Vec<String>,
        rows: Vec<Vec<ResultValue>>,
        column_hints: Vec<ColumnHint>,
        summary: QuerySummary,
    ) -> Self {
        Self {
            columns,
            batches: Vec::new(),
            summary,
            age_columns: Vec::new(),
            bolt_rows: None,
            result_rows: Some(Arc::new(rows)),
            column_hints,
        }
    }

    /// Attaches raw [`BoltValue`] rows for lossless nested entity/container conversion.
    pub fn with_bolt_rows(mut self, rows: Vec<Vec<BoltValue>>) -> Self {
        self.bolt_rows = Some(Arc::new(rows));
        self
    }

    /// Attaches pre-converted [`ResultValue`] rows for zero-copy nested entity/container conversion.
    pub fn with_result_rows(mut self, rows: Vec<Vec<ResultValue>>) -> Self {
        self.result_rows = Some(Arc::new(rows));
        self
    }

    /// Creates an empty `QueryResult` for non-returning mutating queries.
    pub fn empty(summary: QuerySummary) -> Self {
        Self {
            columns: Vec::new(),
            batches: Vec::new(),
            summary,
            age_columns: Vec::new(),
            bolt_rows: None,
            result_rows: None,
            column_hints: Vec::new(),
        }
    }

    /// Returns total row count across batches or retained row buffers in the result.
    pub fn row_count(&self) -> usize {
        if !self.batches.is_empty() {
            self.batches.iter().map(|b| b.num_rows()).sum()
        } else if let Some(ref rows) = self.result_rows {
            rows.len()
        } else if let Some(ref rows) = self.bolt_rows {
            rows.len()
        } else {
            0
        }
    }

    /// Returns `true` if there are no rows in the result.
    pub fn is_empty(&self) -> bool {
        self.row_count() == 0
    }

    /// Returns or lazily materializes a single canonical Arrow [`RecordBatch`].
    ///
    /// Preserves `column_hints` (PostgreSQL OID/typmod, mixed-depth array, and
    /// Apache AGE rules) so lazy materialization from a [`ResultMode::Rows`] result
    /// matches [`ResultMode::Arrow`] identically.
    pub fn to_record_batch(&self) -> Result<Option<RecordBatch>> {
        if !self.batches.is_empty() {
            if self.batches.len() == 1 {
                return Ok(Some(self.batches[0].clone()));
            }
            #[cfg(feature = "arrow")]
            {
                let schema = self.batches[0].schema();
                let concatenated =
                    concat_batches(&schema, &self.batches).map_err(NetError::ArrowError)?;
                return Ok(Some(concatenated));
            }
            #[cfg(not(feature = "arrow"))]
            {
                return Err(NetError::ProtocolError(
                    "Concatenating multiple RecordBatches requires the `arrow` feature".to_string(),
                ));
            }
        }
        if let Some(ref rows) = self.result_rows {
            if self.columns.is_empty() && rows.is_empty() {
                return Ok(None);
            }
            let batch = values_to_record_batch_with_hints(&self.columns, rows, &self.column_hints)?;
            return Ok(Some(batch));
        }
        if !self.columns.is_empty() {
            let batch = values_to_record_batch_with_hints(&self.columns, &[], &self.column_hints)?;
            return Ok(Some(batch));
        }
        Ok(None)
    }

    /// Consolidates multiple Arrow record batches into a single `RecordBatch`.
    #[cfg(feature = "arrow")]
    pub fn into_single_batch(self) -> Result<Option<RecordBatch>> {
        if !self.batches.is_empty() {
            if self.batches.len() == 1 {
                return Ok(Some(self.batches.into_iter().next().unwrap()));
            }
            let schema = self.batches[0].schema();
            let concatenated =
                concat_batches(&schema, &self.batches).map_err(NetError::ArrowError)?;
            return Ok(Some(concatenated));
        }
        if self.result_rows.is_some() || !self.columns.is_empty() {
            return self.to_record_batch();
        }
        Ok(None)
    }
}

/// Abstract asynchronous physical connection to a database.
#[async_trait]
pub trait AsyncConnection: Send + Sync {
    /// Executes a parameterized query string against the active connection.
    async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult>;

    /// Executes a parameterized query string with an explicit [`ResultMode`].
    ///
    /// Backends that do not override this fall back to [`Self::execute`].
    async fn execute_with_mode(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
        _mode: ResultMode,
    ) -> Result<QueryResult> {
        self.execute(query, params).await
    }

    /// Sends a lightweight liveness probe / ping to verify socket connectivity.
    async fn ping(&mut self) -> Result<()>;

    /// Resets the connection state, aborting any active open transactions or buffer states.
    async fn reset(&mut self) -> Result<()>;

    /// Consumes any deferred protocol acknowledgement (for example a Bolt `RESET`
    /// written after a failed query) so the connection is safe to lease again.
    ///
    /// The default implementation has nothing to do. A backend that leaves an
    /// acknowledgement unread must override this and report a failure if the peer
    /// rejected it or the socket failed.
    async fn prepare_for_reuse(&mut self) -> Result<()> {
        Ok(())
    }

    /// Returns `true` if the underlying network socket or stream is currently healthy and open.
    fn is_valid(&self) -> bool;

    /// Performs a non-blocking local liveness check without sending any network round-trip.
    ///
    /// Returns `false` if the connection is marked invalid, the peer has closed the socket,
    /// the socket has an error, or unexpected unread data is pending on an idle connection.
    fn is_locally_alive(&self) -> bool {
        self.is_valid()
    }

    /// Returns the negotiated wire protocol version (for example `"5.4"` for Bolt), if available.
    fn protocol_version(&self) -> Option<String> {
        None
    }

    /// Returns `true` if the connection is currently inside an explicit transaction block.
    fn is_in_transaction(&self) -> bool;

    /// Returns `true` when a ROLLBACK can still be issued to end the current
    /// transaction. Failed transactions (for example PostgreSQL `INERROR`)
    /// need a rollback even though [`Self::is_in_transaction`] is false.
    fn can_rollback(&self) -> bool {
        self.is_in_transaction()
    }

    /// Gracefully closes the underlying socket and releases server resources.
    async fn close(&mut self) -> Result<()>;

    /// Begins an explicit database transaction on this connection.
    ///
    /// The default implementation reports that the dialect does not support
    /// transactions. Backends that do support them override this method.
    async fn begin_transaction(&mut self) -> Result<()> {
        Err(NetError::TransactionError(
            "This backend does not support transactions".to_string(),
        ))
    }

    /// Commits the explicit database transaction currently open on this connection.
    async fn commit_transaction(&mut self) -> Result<()> {
        Err(NetError::TransactionError(
            "This backend does not support transactions".to_string(),
        ))
    }

    /// Rolls back the explicit database transaction currently open on this connection.
    async fn rollback_transaction(&mut self) -> Result<()> {
        Err(NetError::TransactionError(
            "This backend does not support transactions".to_string(),
        ))
    }

    /// Creates a named savepoint inside the active transaction.
    async fn create_savepoint(&mut self, name: &str) -> Result<()> {
        Err(NetError::TransactionError(format!(
            "Savepoint '{name}' is not supported by this backend"
        )))
    }

    /// Rolls the active transaction back to a named savepoint.
    async fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        Err(NetError::TransactionError(format!(
            "Savepoint '{name}' is not supported by this backend"
        )))
    }

    /// Releases a named savepoint from the active transaction.
    async fn release_savepoint(&mut self, name: &str) -> Result<()> {
        Err(NetError::TransactionError(format!(
            "Savepoint '{name}' is not supported by this backend"
        )))
    }

    /// Returns `true` when the backend supports real transaction savepoints.
    fn supports_savepoints(&self) -> bool {
        false
    }

    /// Opens a pull-based result stream cursor for `query` using `options`.
    ///
    /// The default implementation executes the query in [`ResultMode::Rows`] and
    /// returns a [`StreamCursor::Buffered`] client-side chunk slicer.
    async fn open_stream(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
        options: &StreamOptions,
    ) -> Result<StreamCursor> {
        let fetch_size = options.validate_fetch_size()?;
        let result = self
            .execute_with_mode(query, params, ResultMode::Rows)
            .await?;
        Ok(StreamCursor::from_query_result(result, fetch_size))
    }

    /// Pulls the next bounded chunk of rows from an open [`StreamCursor`], or `Ok(None)` when exhausted.
    async fn next_stream_chunk(
        &mut self,
        cursor: &mut StreamCursor,
    ) -> Result<Option<ResultChunk>> {
        match cursor {
            StreamCursor::Buffered(_) => Ok(cursor.next_buffered_chunk()),
            #[cfg(feature = "bolt")]
            StreamCursor::Bolt(_) => Err(NetError::ProtocolError(
                "Bolt stream cursor passed to non-Bolt connection".to_string(),
            )),
        }
    }

    /// Closes an open [`StreamCursor`], discarding any unread server-side rows.
    async fn close_stream(&mut self, cursor: &mut StreamCursor) -> Result<()> {
        match cursor {
            StreamCursor::Buffered(_) => {
                cursor.close_buffered();
                Ok(())
            }
            #[cfg(feature = "bolt")]
            StreamCursor::Bolt(_) => Err(NetError::ProtocolError(
                "Bolt stream cursor passed to non-Bolt connection".to_string(),
            )),
        }
    }

    /// Timeout applied when closing an active stream cursor (defaults to 5 seconds).
    fn stream_close_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(5)
    }
}

#[async_trait]
impl AsyncConnection for Box<dyn AsyncConnection> {
    async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        (**self).execute(query, params).await
    }

    async fn execute_with_mode(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
        mode: ResultMode,
    ) -> Result<QueryResult> {
        (**self).execute_with_mode(query, params, mode).await
    }

    async fn ping(&mut self) -> Result<()> {
        (**self).ping().await
    }

    async fn reset(&mut self) -> Result<()> {
        (**self).reset().await
    }

    async fn prepare_for_reuse(&mut self) -> Result<()> {
        (**self).prepare_for_reuse().await
    }

    fn is_valid(&self) -> bool {
        (**self).is_valid()
    }

    fn is_locally_alive(&self) -> bool {
        (**self).is_locally_alive()
    }

    fn protocol_version(&self) -> Option<String> {
        (**self).protocol_version()
    }

    fn is_in_transaction(&self) -> bool {
        (**self).is_in_transaction()
    }

    fn can_rollback(&self) -> bool {
        (**self).can_rollback()
    }

    async fn close(&mut self) -> Result<()> {
        (**self).close().await
    }

    async fn begin_transaction(&mut self) -> Result<()> {
        (**self).begin_transaction().await
    }

    async fn commit_transaction(&mut self) -> Result<()> {
        (**self).commit_transaction().await
    }

    async fn rollback_transaction(&mut self) -> Result<()> {
        (**self).rollback_transaction().await
    }

    async fn create_savepoint(&mut self, name: &str) -> Result<()> {
        (**self).create_savepoint(name).await
    }

    async fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        (**self).rollback_to_savepoint(name).await
    }

    async fn release_savepoint(&mut self, name: &str) -> Result<()> {
        (**self).release_savepoint(name).await
    }

    fn supports_savepoints(&self) -> bool {
        (**self).supports_savepoints()
    }

    async fn open_stream(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
        options: &StreamOptions,
    ) -> Result<StreamCursor> {
        (**self).open_stream(query, params, options).await
    }

    async fn next_stream_chunk(
        &mut self,
        cursor: &mut StreamCursor,
    ) -> Result<Option<ResultChunk>> {
        (**self).next_stream_chunk(cursor).await
    }

    async fn close_stream(&mut self, cursor: &mut StreamCursor) -> Result<()> {
        (**self).close_stream(cursor).await
    }

    fn stream_close_timeout(&self) -> std::time::Duration {
        (**self).stream_close_timeout()
    }
}

/// Abstract asynchronous database transaction.
#[async_trait]
pub trait AsyncTransaction: Send + Sync {
    /// Executes a parameterized query within the transaction boundary.
    async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult>;

    /// Commits the active transaction to durable storage.
    async fn commit(self: Box<Self>) -> Result<QuerySummary>;

    /// Aborts and rolls back the active transaction.
    async fn rollback(self: Box<Self>) -> Result<()>;
}

/// Factory trait for asynchronously spawning fresh physical database connections.
#[async_trait]
pub trait ConnectionFactory<C: AsyncConnection>: Send + Sync + 'static {
    /// Asynchronously establishes a new connection to the database.
    async fn create(&self) -> Result<C>;
}

/// Function-pointer or closure based connection factory wrapper.
pub struct FnConnectionFactory<F, Fut, C>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<C>> + Send + 'static,
    C: AsyncConnection + 'static,
{
    factory_fn: F,
    _phantom: std::marker::PhantomData<fn() -> (Fut, C)>,
}

impl<F, Fut, C> FnConnectionFactory<F, Fut, C>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<C>> + Send + 'static,
    C: AsyncConnection + 'static,
{
    /// Creates a new connection factory from an asynchronous closure.
    pub fn new(factory_fn: F) -> Self {
        Self {
            factory_fn,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<F, Fut, C> ConnectionFactory<C> for FnConnectionFactory<F, Fut, C>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<C>> + Send + 'static,
    C: AsyncConnection + 'static,
{
    async fn create(&self) -> Result<C> {
        (self.factory_fn)().await
    }
}

/// High-level asynchronous database engine interface.
#[async_trait]
pub trait AsyncEngine: Send + Sync {
    /// Executes a parameterized query using an automatically leased connection from the engine's pool.
    async fn execute(
        &self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult>;

    /// Returns the engine's health status and connection pool statistics.
    async fn ping(&self) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::arrow::EmptyColumnKind;
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;

    #[test]
    fn result_mode_default_is_arrow() {
        assert_eq!(ResultMode::default(), ResultMode::Arrow);
    }

    #[test]
    fn query_result_from_rows_tracks_row_count_and_materializes_lazily() {
        let res = QueryResult::from_rows(
            vec!["id".to_string(), "name".to_string()],
            vec![
                vec![ResultValue::Int(1), ResultValue::Str("a".into())],
                vec![ResultValue::Int(2), ResultValue::Str("b".into())],
            ],
            vec![
                ColumnHint::PgEmptyTyped(EmptyColumnKind::Int),
                ColumnHint::PgEmptyTyped(EmptyColumnKind::Str),
            ],
            QuerySummary::empty(),
        );
        assert!(res.batches.is_empty());
        assert_eq!(res.row_count(), 2);
        assert!(!res.is_empty());

        let batch = res.to_record_batch().unwrap().expect("lazy batch");
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(1),
            2
        );
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "a");

        #[cfg(feature = "arrow")]
        {
            let via_into = res.into_single_batch().unwrap().expect("into_single_batch");
            assert_eq!(via_into, batch);
        }
    }

    #[test]
    fn query_result_from_rows_preserves_zero_row_typed_pg_schema() {
        let typmod_10_2 = ((10 << 16) | 2) + 4;
        let res = QueryResult::from_rows(
            vec!["id".to_string(), "price".to_string(), "tags".to_string()],
            Vec::new(),
            vec![
                ColumnHint::PgEmptyTyped(EmptyColumnKind::Uuid),
                ColumnHint::PgDecimal {
                    typmod: Some(typmod_10_2),
                },
                ColumnHint::PgArray {
                    depth: 1,
                    elem_hint: Box::new(ColumnHint::PgEmptyTyped(EmptyColumnKind::Int)),
                },
            ],
            QuerySummary::empty(),
        );
        assert_eq!(res.row_count(), 0);
        assert!(res.is_empty());
        let batch = res.to_record_batch().unwrap().expect("typed empty batch");
        assert_eq!(batch.num_rows(), 0);
        let schema = batch.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(schema.field(1).data_type(), &DataType::Decimal128(38, 2));
        assert!(matches!(schema.field(2).data_type(), DataType::List(_)));
    }

    #[test]
    fn query_result_from_rows_empty_columns_returns_none_batch() {
        let res = QueryResult::from_rows(Vec::new(), Vec::new(), Vec::new(), QuerySummary::empty());
        assert_eq!(res.row_count(), 0);
        assert!(res.to_record_batch().unwrap().is_none());
        #[cfg(feature = "arrow")]
        assert!(res.into_single_batch().unwrap().is_none());
    }
}
