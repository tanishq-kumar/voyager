//! Native transaction handle that leases one pooled connection for its whole lifetime.

use std::collections::HashMap;

use crate::engine::{AsyncConnection, QueryResult, QuerySummary, ResultMode};
use crate::error::{NetError, Result};
use crate::params::ParamValue;
use crate::pool::PooledConnection;
use crate::stream::{ResultChunk, StreamCursor, StreamOptions, TransactionStream};

/// Lifecycle state of a native transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeTransactionState {
    /// The transaction accepts statements.
    Active,
    /// A statement failed; only rollback (or drop) is allowed.
    Failed,
    /// The transaction committed successfully.
    Committed,
    /// The transaction rolled back successfully.
    RolledBack,
}

impl NativeTransactionState {
    /// Returns the Python-facing lifecycle string.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Failed => "FAILED",
            Self::Committed => "COMMITTED",
            Self::RolledBack => "ROLLED_BACK",
        }
    }
}

/// Explicit database transaction holding a single pooled connection.
///
/// The connection is leased when the transaction begins and is only returned to
/// the pool when the transaction is committed, rolled back, or dropped. This
/// guarantees that every statement issued inside the transaction runs on the
/// same physical connection, so reads observe the transaction's own writes.
///
/// A failed statement marks the transaction [`NativeTransactionState::Failed`].
/// From then on `execute` and `commit` fail closed with a clear "transaction is
/// aborted" error; only `rollback` (a no-op when the driver already aborted the
/// transaction) is allowed.
pub struct NativeTransaction<C: AsyncConnection + 'static> {
    conn: Option<PooledConnection<C>>,
    state: NativeTransactionState,
    supports_savepoints: bool,
    has_open_stream: bool,
    summary: QuerySummary,
}

impl<C: AsyncConnection + 'static> NativeTransaction<C> {
    /// Begins an explicit transaction on an already leased pooled connection.
    pub async fn begin(mut conn: PooledConnection<C>) -> Result<Self> {
        let supports_savepoints = conn.supports_savepoints();
        conn.begin_transaction().await?;
        Ok(Self {
            conn: Some(conn),
            state: NativeTransactionState::Active,
            supports_savepoints,
            has_open_stream: false,
            summary: QuerySummary::empty(),
        })
    }

    /// Returns the current lifecycle state.
    pub fn state(&self) -> NativeTransactionState {
        self.state
    }

    /// Returns `true` while the transaction accepts statements.
    pub fn is_active(&self) -> bool {
        self.state == NativeTransactionState::Active
    }

    /// Returns `true` when this transaction supports real savepoints.
    pub fn supports_savepoints(&self) -> bool {
        self.supports_savepoints
    }

    /// Returns `true` while a streaming cursor is active on this transaction.
    pub fn has_open_stream(&self) -> bool {
        self.has_open_stream
    }

    /// Returns the accumulated mutation summary for the transaction.
    pub fn summary(&self) -> &QuerySummary {
        &self.summary
    }

    fn active_conn(&mut self) -> Result<&mut PooledConnection<C>> {
        match self.state {
            NativeTransactionState::Active => {}
            NativeTransactionState::Failed => {
                return Err(NetError::TransactionError(
                    "transaction is aborted; roll back before executing more statements"
                        .to_string(),
                ));
            }
            NativeTransactionState::Committed | NativeTransactionState::RolledBack => {
                return Err(NetError::TransactionError(
                    "Transaction is already completed".to_string(),
                ));
            }
        }
        if self.has_open_stream {
            return Err(NetError::TransactionError(
                "An active stream is open on this transaction; close or exhaust it first"
                    .to_string(),
            ));
        }
        self.conn.as_mut().ok_or_else(|| {
            NetError::TransactionError("Transaction has no leased connection".to_string())
        })
    }

    fn accumulate(&mut self, summary: &QuerySummary) {
        self.summary.nodes_created += summary.nodes_created;
        self.summary.nodes_deleted += summary.nodes_deleted;
        self.summary.relationships_created += summary.relationships_created;
        self.summary.relationships_deleted += summary.relationships_deleted;
        self.summary.properties_set += summary.properties_set;
        self.summary.labels_added += summary.labels_added;
        self.summary.labels_removed += summary.labels_removed;
        self.summary.indexes_added += summary.indexes_added;
        self.summary.constraints_added += summary.constraints_added;
        self.summary.execution_time_ms += summary.execution_time_ms;
    }

    /// Executes a parameterized statement inside the transaction.
    pub async fn execute(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        let conn = self.active_conn()?;
        match conn.execute(query, params).await {
            Ok(res) => {
                self.accumulate(&res.summary);
                Ok(res)
            }
            Err(err) => {
                // Any statement error aborts the transaction on every backend:
                // Bolt RESET aborts the explicit transaction and PostgreSQL
                // enters FailedTransaction. Never allow a later autocommit.
                self.state = NativeTransactionState::Failed;
                Err(err)
            }
        }
    }

    async fn execute_with_mode(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
        mode: ResultMode,
    ) -> Result<QueryResult> {
        let conn = self.active_conn()?;
        match conn.execute_with_mode(query, params, mode).await {
            Ok(res) => {
                self.accumulate(&res.summary);
                Ok(res)
            }
            Err(err) => {
                self.state = NativeTransactionState::Failed;
                Err(err)
            }
        }
    }

    /// Executes a parameterized statement inside the transaction, retaining decoded
    /// [`ResultValue`](crate::ResultValue) rows without building an Apache Arrow `RecordBatch`.
    pub async fn execute_rows(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        self.execute_with_mode(query, params, ResultMode::Rows)
            .await
    }

    /// Executes a parameterized statement inside the transaction, building an Apache Arrow
    /// `RecordBatch` directly without retaining `result_rows` or `bolt_rows`.
    pub async fn execute_arrow(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
    ) -> Result<QueryResult> {
        self.execute_with_mode(query, params, ResultMode::Arrow)
            .await
    }

    /// Opens a pull-based result stream borrowing this transaction.
    ///
    /// Rejects concurrent statements, `commit()`, or another stream while open.
    pub async fn stream(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
        options: StreamOptions,
    ) -> Result<TransactionStream<'_, C>> {
        let cursor = self.open_stream_cursor(query, params, &options).await?;
        Ok(TransactionStream::new(self, cursor))
    }

    /// Low-level helper opening a [`StreamCursor`] inside this transaction while enforcing
    /// [`Self::has_open_stream`]. Intended for wrapper types such as PyO3 bindings.
    pub async fn open_stream_cursor(
        &mut self,
        query: &str,
        params: &HashMap<String, ParamValue>,
        options: &StreamOptions,
    ) -> Result<StreamCursor> {
        let _ = options.validate_fetch_size()?;
        let conn = self.active_conn()?;
        match conn.open_stream(query, params, options).await {
            Ok(cursor) => {
                self.has_open_stream = true;
                Ok(cursor)
            }
            Err(err) => {
                self.state = NativeTransactionState::Failed;
                Err(err)
            }
        }
    }

    /// Low-level helper pulling the next chunk from an open transaction [`StreamCursor`].
    pub async fn next_stream_chunk(
        &mut self,
        cursor: &mut StreamCursor,
    ) -> Result<Option<ResultChunk>> {
        match self.state {
            NativeTransactionState::Active => {}
            NativeTransactionState::Failed => {
                return Err(NetError::TransactionError(
                    "transaction is aborted; roll back before executing more statements"
                        .to_string(),
                ));
            }
            NativeTransactionState::Committed | NativeTransactionState::RolledBack => {
                return Err(NetError::TransactionError(
                    "Transaction is already completed".to_string(),
                ));
            }
        }
        let conn = self.conn.as_mut().ok_or_else(|| {
            NetError::TransactionError("Transaction has no leased connection".to_string())
        })?;
        match conn.next_stream_chunk(cursor).await {
            Ok(Some(chunk)) => Ok(Some(chunk)),
            Ok(None) => {
                if self.has_open_stream {
                    self.accumulate(cursor.summary());
                    self.has_open_stream = false;
                }
                Ok(None)
            }
            Err(err) => {
                if !cursor.is_wire_clean() {
                    conn.mark_broken();
                }
                self.state = NativeTransactionState::Failed;
                self.has_open_stream = false;
                Err(err)
            }
        }
    }

    /// Low-level helper closing an open transaction [`StreamCursor`].
    pub async fn close_stream_cursor(&mut self, cursor: &mut StreamCursor) -> Result<()> {
        if !self.has_open_stream {
            return Ok(());
        }
        let Some(conn) = self.conn.as_mut() else {
            self.has_open_stream = false;
            return Ok(());
        };
        if !cursor.is_wire_clean() {
            let close_timeout = conn.stream_close_timeout();
            let res = tokio::time::timeout(close_timeout, conn.close_stream(cursor)).await;
            match res {
                Ok(Ok(())) => {
                    self.accumulate(cursor.summary());
                    self.has_open_stream = false;
                    Ok(())
                }
                Ok(Err(err)) => {
                    if !cursor.is_wire_clean() {
                        conn.mark_broken();
                    }
                    self.state = NativeTransactionState::Failed;
                    self.has_open_stream = false;
                    Err(err)
                }
                Err(_) => {
                    conn.mark_broken();
                    self.state = NativeTransactionState::Failed;
                    self.has_open_stream = false;
                    Err(NetError::Timeout(format!(
                        "Closing stream timed out after {:?}",
                        close_timeout
                    )))
                }
            }
        } else {
            let res = conn.close_stream(cursor).await;
            self.accumulate(cursor.summary());
            self.has_open_stream = false;
            res
        }
    }

    /// Synchronously aborts an unclosed server-streaming cursor when its wrapper drops without `close()`.
    pub fn abort_stream_on_drop(&mut self) {
        if let Some(conn) = self.conn.as_mut() {
            conn.mark_broken();
        }
        self.state = NativeTransactionState::Failed;
        self.has_open_stream = false;
    }

    /// Clears `has_open_stream` and accumulates summary when a clean/buffered cursor drops without `close()`.
    pub fn clear_clean_stream(&mut self, cursor: &StreamCursor) {
        if self.has_open_stream {
            self.accumulate(cursor.summary());
            self.has_open_stream = false;
        }
    }

    /// Commits the transaction and returns its accumulated mutation summary.
    pub async fn commit(&mut self) -> Result<QuerySummary> {
        match self.state {
            NativeTransactionState::Failed => {
                return Err(NetError::TransactionError(
                    "transaction is aborted; roll back instead of committing".to_string(),
                ));
            }
            NativeTransactionState::Active => {}
            NativeTransactionState::Committed | NativeTransactionState::RolledBack => {
                return Err(NetError::TransactionError(
                    "Transaction is already completed".to_string(),
                ));
            }
        }
        if self.has_open_stream {
            return Err(NetError::TransactionError(
                "An active stream is open on this transaction; close or exhaust it first"
                    .to_string(),
            ));
        }
        let mut conn = self.conn.take().ok_or_else(|| {
            NetError::TransactionError("Transaction has no leased connection".to_string())
        })?;
        match conn.commit_transaction().await {
            Ok(()) => {
                self.state = NativeTransactionState::Committed;
                drop(conn);
                Ok(self.summary.clone())
            }
            Err(err) => {
                self.state = NativeTransactionState::Failed;
                self.conn = Some(conn);
                Err(err)
            }
        }
    }

    /// Rolls the transaction back and releases its pooled connection.
    pub async fn rollback(&mut self) -> Result<()> {
        match self.state {
            NativeTransactionState::Active | NativeTransactionState::Failed => {}
            NativeTransactionState::Committed | NativeTransactionState::RolledBack => {
                return Err(NetError::TransactionError(
                    "Transaction is already completed".to_string(),
                ));
            }
        }
        let mut conn = self.conn.take().ok_or_else(|| {
            NetError::TransactionError("Transaction has no leased connection".to_string())
        })?;
        if self.has_open_stream {
            self.has_open_stream = false;
            conn.mark_broken();
            drop(conn);
            self.state = NativeTransactionState::RolledBack;
            return Ok(());
        }
        if conn.is_broken() {
            drop(conn);
            self.state = NativeTransactionState::RolledBack;
            return Ok(());
        }
        // A failed Bolt transaction was already aborted by RESET, so there is
        // no server-side transaction left to roll back. PostgreSQL keeps the
        // failed transaction open and does accept ROLLBACK.
        let result = if conn.can_rollback() {
            conn.rollback_transaction().await
        } else {
            Ok(())
        };
        match result {
            Ok(()) => {
                self.state = NativeTransactionState::RolledBack;
                drop(conn);
                Ok(())
            }
            Err(err) => {
                self.state = NativeTransactionState::Failed;
                self.conn = Some(conn);
                Err(err)
            }
        }
    }

    /// Creates a named savepoint inside the transaction.
    pub async fn savepoint(&mut self, name: &str) -> Result<()> {
        if !self.supports_savepoints {
            return Err(NetError::TransactionError(format!(
                "Savepoints are not supported by this backend; cannot create savepoint '{name}'"
            )));
        }
        let conn = self.active_conn()?;
        match conn.create_savepoint(name).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.state = NativeTransactionState::Failed;
                Err(err)
            }
        }
    }

    /// Rolls the transaction back to a named savepoint.
    pub async fn rollback_to_savepoint(&mut self, name: &str) -> Result<()> {
        if !self.supports_savepoints {
            return Err(NetError::TransactionError(format!(
                "Savepoints are not supported by this backend; cannot roll back to '{name}'"
            )));
        }
        match self.state {
            NativeTransactionState::Active | NativeTransactionState::Failed => {}
            NativeTransactionState::Committed | NativeTransactionState::RolledBack => {
                return Err(NetError::TransactionError(
                    "Transaction is already completed".to_string(),
                ));
            }
        }
        if self.has_open_stream {
            return Err(NetError::TransactionError(
                "An active stream is open on this transaction; close or exhaust it first"
                    .to_string(),
            ));
        }
        let conn = self.conn.as_mut().ok_or_else(|| {
            NetError::TransactionError("Transaction has no leased connection".to_string())
        })?;
        match conn.rollback_to_savepoint(name).await {
            Ok(()) => {
                self.state = NativeTransactionState::Active;
                Ok(())
            }
            Err(err) => {
                self.state = NativeTransactionState::Failed;
                Err(err)
            }
        }
    }

    /// Releases a named savepoint from the transaction.
    pub async fn release_savepoint(&mut self, name: &str) -> Result<()> {
        if !self.supports_savepoints {
            return Err(NetError::TransactionError(format!(
                "Savepoints are not supported by this backend; cannot release '{name}'"
            )));
        }
        let conn = self.active_conn()?;
        match conn.release_savepoint(name).await {
            Ok(()) => Ok(()),
            Err(err) => {
                self.state = NativeTransactionState::Failed;
                Err(err)
            }
        }
    }
}

impl<C: AsyncConnection + 'static> Drop for NativeTransaction<C> {
    fn drop(&mut self) {
        // Dropping an open transaction must deterministically end the server
        // transaction. We cannot await here, so mark the pooled connection
        // broken: the pool discards the physical socket instead of RESET/reuse,
        // which makes the server abort the transaction when the socket closes.
        let should_discard = self.conn.as_ref().is_some_and(|conn| match self.state {
            NativeTransactionState::Active => true,
            NativeTransactionState::Failed => conn.can_rollback(),
            NativeTransactionState::Committed | NativeTransactionState::RolledBack => false,
        });
        if should_discard && let Some(conn) = self.conn.as_mut() {
            conn.mark_broken();
        }
    }
}

#[cfg(all(test, feature = "testkit"))]
mod tests {
    use super::*;
    use crate::config::PoolConfig;
    use crate::mock::MockConnectionFactory;
    use crate::pool::ConnectionPool;
    use std::sync::Arc;
    use std::time::Duration;

    type MockConnectionType = crate::mock::MockConnection;

    fn pool_config() -> PoolConfig {
        PoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_secs(1),
            ..PoolConfig::default()
        }
    }

    async fn pool() -> (
        ConnectionPool<MockConnectionType>,
        Arc<MockConnectionFactory>,
    ) {
        let factory = Arc::new(MockConnectionFactory::new());
        let pool = ConnectionPool::new(pool_config(), factory.clone());
        (pool, factory)
    }

    #[tokio::test]
    async fn native_transaction_holds_one_connection_commit_and_rollback() {
        let (pool, factory) = pool().await;
        let conn = pool.acquire().await.expect("acquire connection");
        let mut tx = NativeTransaction::begin(conn).await.expect("begin");
        assert!(tx.is_active());
        assert_eq!(tx.state().as_str(), "ACTIVE");
        assert_eq!(pool.active_count(), 1);
        assert_eq!(pool.idle_count(), 0);

        tx.execute("CREATE (n)", &HashMap::new())
            .await
            .expect("execute inside tx");
        assert_eq!(pool.active_count(), 1);
        assert!(
            factory.row_count() == 0,
            "uncommitted write must not be visible"
        );
        assert!(tx.commit().await.is_ok());
        assert!(!tx.is_active());
        assert_eq!(tx.state().as_str(), "COMMITTED");
        assert_eq!(pool.active_count(), 0);
        assert_eq!(factory.row_count(), 1);

        let conn = pool.acquire().await.expect("reacquire connection");
        let mut tx = NativeTransaction::begin(conn).await.expect("begin again");
        tx.execute("CREATE (n)", &HashMap::new())
            .await
            .expect("execute inside tx");
        tx.rollback().await.expect("rollback");
        assert!(!tx.is_active());
        assert_eq!(tx.state().as_str(), "ROLLED_BACK");
        assert_eq!(pool.active_count(), 0);
        assert_eq!(factory.row_count(), 1);
    }

    #[tokio::test]
    async fn native_transaction_fails_closed_after_statement_error() {
        let (pool, factory) = pool().await;
        let conn = pool.acquire().await.expect("acquire connection");
        let mut tx = NativeTransaction::begin(conn).await.expect("begin");
        tx.execute("CREATE (n)", &HashMap::new())
            .await
            .expect("execute inside tx");

        tx.conn.as_mut().expect("leased connection").fail_execute = true;
        let err = tx
            .execute("CREATE (n)", &HashMap::new())
            .await
            .expect_err("failing statement must return an error");
        assert!(err.to_string().contains("Simulated execution failure"));
        assert_eq!(tx.state().as_str(), "FAILED");
        assert!(!tx.is_active());

        let err = tx
            .execute("CREATE (n)", &HashMap::new())
            .await
            .expect_err("writes after failure must be rejected");
        assert!(err.to_string().contains("aborted"));

        let err = tx
            .commit()
            .await
            .expect_err("commit after failure must fail");
        assert!(err.to_string().contains("aborted"));
        assert_eq!(tx.state().as_str(), "FAILED");

        tx.rollback().await.expect("rollback after failure");
        assert_eq!(tx.state().as_str(), "ROLLED_BACK");
        assert_eq!(factory.row_count(), 0);
    }

    #[tokio::test]
    async fn native_transaction_drop_discards_open_server_transaction() {
        let (pool, factory) = pool().await;
        let conn = pool.acquire().await.expect("acquire connection");
        let mut tx = NativeTransaction::begin(conn).await.expect("begin");
        tx.execute("CREATE (n)", &HashMap::new())
            .await
            .expect("execute inside tx");
        assert!(!tx.conn.as_ref().expect("leased connection").is_broken());

        drop(tx);
        assert_eq!(pool.active_count(), 0);
        assert_eq!(factory.row_count(), 0, "dropped write must be rolled back");

        let conn = pool.acquire().await.expect("reacquire after drop");
        assert!(!conn.is_in_transaction());
        assert_eq!(conn.row_count(), 0);
    }

    #[tokio::test]
    async fn native_transaction_savepoints_follow_backend_support() {
        let (pool, _) = pool().await;
        let conn = pool.acquire().await.expect("acquire connection");
        let mut tx = NativeTransaction::begin(conn).await.expect("begin");
        assert!(tx.supports_savepoints());

        tx.savepoint("sp_1").await.expect("create savepoint");
        tx.rollback_to_savepoint("sp_1")
            .await
            .expect("rollback to savepoint");
        tx.release_savepoint("sp_1")
            .await
            .expect("release savepoint");
        tx.commit().await.expect("commit");
    }

    #[tokio::test]
    async fn native_transaction_rollback_to_savepoint_recovers_from_failed_state() {
        let (pool, _) = pool().await;
        let conn = pool.acquire().await.expect("acquire connection");
        let mut tx = NativeTransaction::begin(conn).await.expect("begin");
        tx.savepoint("sp_recover").await.expect("create savepoint");

        tx.conn.as_mut().expect("leased connection").fail_execute = true;
        let err = tx
            .execute("CREATE (n)", &HashMap::new())
            .await
            .expect_err("expected failure");
        assert!(err.to_string().contains("Simulated execution failure"));
        assert_eq!(tx.state(), NativeTransactionState::Failed);

        tx.conn.as_mut().expect("leased connection").fail_execute = false;
        tx.rollback_to_savepoint("sp_recover")
            .await
            .expect("rollback to savepoint from Failed state must succeed");
        assert_eq!(tx.state(), NativeTransactionState::Active);

        tx.execute("CREATE (n)", &HashMap::new())
            .await
            .expect("execute after savepoint recovery");
        tx.commit().await.expect("commit after recovery");
    }
}
