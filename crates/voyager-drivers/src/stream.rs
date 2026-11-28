//! Pull-based streaming cursors and RAII stream handles for [`AsyncConnection`].

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::RecordBatch;

#[cfg(feature = "bolt")]
use crate::bolt::BoltStreamCursor;
use crate::engine::{AsyncConnection, QueryResult, QuerySummary};
use crate::error::{NetError, Result};
use crate::native_transaction::NativeTransaction;
use crate::pool::PooledConnection;
use crate::value::arrow::{ColumnHint, values_to_record_batch_with_hints};
use crate::value::result::ResultValue;

/// Default number of rows requested per wire pull/execute chunk.
pub const DEFAULT_STREAM_FETCH_SIZE: usize = 1_000;

/// Configuration for a pull-based query stream.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StreamOptions {
    /// Maximum rows fetched per round-trip (`>= 1`; defaults to `1_000`).
    pub fetch_size: usize,
}

impl Default for StreamOptions {
    fn default() -> Self {
        Self {
            fetch_size: DEFAULT_STREAM_FETCH_SIZE,
        }
    }
}

impl StreamOptions {
    /// Creates stream options with the given `fetch_size`.
    pub fn new(fetch_size: usize) -> Self {
        Self { fetch_size }
    }

    /// Sets the maximum rows fetched per round-trip.
    pub fn with_fetch_size(mut self, fetch_size: usize) -> Self {
        self.fetch_size = fetch_size;
        self
    }

    /// Validates that `fetch_size >= 1` and returns it.
    pub(crate) fn validate_fetch_size(&self) -> Result<usize> {
        if self.fetch_size == 0 {
            return Err(NetError::ProtocolError(
                "Stream fetch_size must be at least 1".to_string(),
            ));
        }
        Ok(self.fetch_size)
    }
}

/// One bounded chunk of rows pulled from an active [`ResultStream`] or [`TransactionStream`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ResultChunk {
    /// Shared projected column names (cloned by `Arc`, zero per-chunk string copies).
    pub columns: Arc<[String]>,
    /// Decoded rows in this chunk (`1..=fetch_size`).
    pub rows: Vec<Vec<ResultValue>>,
    /// Per-column Arrow conversion hints (shared by `Arc`).
    pub column_hints: Arc<[ColumnHint]>,
}

impl ResultChunk {
    /// Creates a new chunk from shared column metadata and decoded rows.
    pub fn new(
        columns: Arc<[String]>,
        rows: Vec<Vec<ResultValue>>,
        column_hints: Arc<[ColumnHint]>,
    ) -> Self {
        Self {
            columns,
            rows,
            column_hints,
        }
    }

    /// Returns the number of rows in this chunk.
    pub fn num_rows(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if this chunk contains zero rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Materializes this chunk into a canonical Apache Arrow [`RecordBatch`].
    pub fn to_record_batch(&self) -> Result<RecordBatch> {
        values_to_record_batch_with_hints(&self.columns, &self.rows, &self.column_hints)
    }
}

/// Client-side chunk slicer for protocols without wire cursors (FalkorDB, Bolt v3, Mock).
#[derive(Debug)]
pub struct BufferedStreamCursor {
    columns: Arc<[String]>,
    column_hints: Arc<[ColumnHint]>,
    rows: VecDeque<Vec<ResultValue>>,
    fetch_size: usize,
    summary: QuerySummary,
    exhausted: bool,
}

impl BufferedStreamCursor {
    /// Wraps an already-materialized [`QueryResult`] for chunked consumption.
    pub fn from_query_result(result: QueryResult, fetch_size: usize) -> Self {
        let columns: Arc<[String]> = Arc::from(result.columns);
        let column_hints: Arc<[ColumnHint]> = Arc::from(result.column_hints);
        let rows = if let Some(arc_rows) = result.result_rows {
            let vec_rows = Arc::try_unwrap(arc_rows).unwrap_or_else(|arc| (*arc).clone());
            VecDeque::from(vec_rows)
        } else if let Some(arc_bolt) = result.bolt_rows {
            let vec_bolt = Arc::try_unwrap(arc_bolt).unwrap_or_else(|arc| (*arc).clone());
            vec_bolt
                .into_iter()
                .map(|row| row.into_iter().map(ResultValue::from).collect())
                .collect()
        } else {
            VecDeque::new()
        };
        let exhausted = rows.is_empty();
        Self {
            columns,
            column_hints,
            rows,
            fetch_size: fetch_size.max(1),
            summary: result.summary,
            exhausted,
        }
    }

    /// Returns the next `fetch_size` rows, or `None` when exhausted.
    pub fn next_chunk(&mut self) -> Option<ResultChunk> {
        if self.exhausted || self.rows.is_empty() {
            self.exhausted = true;
            return None;
        }
        let take_n = self.fetch_size.min(self.rows.len());
        let chunk_rows: Vec<Vec<ResultValue>> = self.rows.drain(..take_n).collect();
        if self.rows.is_empty() {
            self.exhausted = true;
        }
        Some(ResultChunk::new(
            self.columns.clone(),
            chunk_rows,
            self.column_hints.clone(),
        ))
    }

    /// Discards any remaining buffered rows.
    pub fn close(&mut self) {
        self.rows.clear();
        self.exhausted = true;
    }

    /// Returns `true` when all buffered rows have been consumed or closed.
    pub fn is_finished(&self) -> bool {
        self.exhausted
    }
}

/// Opaque protocol-specific cursor state held while a stream is open.
#[derive(Debug)]
pub enum StreamCursor {
    /// Native Bolt v4+/v5+ `PULL` / `DISCARD` streaming cursor.
    #[cfg(feature = "bolt")]
    Bolt(BoltStreamCursor),
    /// Client-side chunk slicer for protocols without wire cursors (FalkorDB, Bolt v3, Mock).
    Buffered(BufferedStreamCursor),
}

impl StreamCursor {
    /// Wraps a [`QueryResult`] into a [`StreamCursor::Buffered`] slicer.
    pub fn from_query_result(result: QueryResult, fetch_size: usize) -> Self {
        Self::Buffered(BufferedStreamCursor::from_query_result(result, fetch_size))
    }

    /// Returns `true` if the server is streaming rows incrementally over the wire.
    pub fn is_server_streaming(&self) -> bool {
        !matches!(self, Self::Buffered(_))
    }

    /// Returns `true` if no wire protocol exchange remains pending on the connection.
    pub fn is_wire_clean(&self) -> bool {
        match self {
            #[cfg(feature = "bolt")]
            Self::Bolt(c) => c.is_wire_clean(),
            Self::Buffered(_) => true,
        }
    }

    /// Returns `true` when the cursor has reached exhaustion, close, or failure.
    pub fn is_finished(&self) -> bool {
        match self {
            #[cfg(feature = "bolt")]
            Self::Bolt(c) => c.is_finished(),
            Self::Buffered(c) => c.is_finished(),
        }
    }

    /// Returns the projected column names for this stream.
    pub fn columns(&self) -> &Arc<[String]> {
        match self {
            #[cfg(feature = "bolt")]
            Self::Bolt(c) => c.columns(),
            Self::Buffered(c) => &c.columns,
        }
    }

    /// Returns the per-column Arrow conversion hints for this stream.
    pub fn column_hints(&self) -> &Arc<[ColumnHint]> {
        match self {
            #[cfg(feature = "bolt")]
            Self::Bolt(c) => c.column_hints(),
            Self::Buffered(c) => &c.column_hints,
        }
    }

    /// Returns the query execution summary accumulated so far.
    pub fn summary(&self) -> &QuerySummary {
        match self {
            #[cfg(feature = "bolt")]
            Self::Bolt(c) => c.summary(),
            Self::Buffered(c) => &c.summary,
        }
    }

    /// Advances a buffered cursor; returns `None` when not buffered or exhausted.
    pub(crate) fn next_buffered_chunk(&mut self) -> Option<ResultChunk> {
        match self {
            Self::Buffered(c) => c.next_chunk(),
            #[cfg(feature = "bolt")]
            Self::Bolt(_) => None,
        }
    }

    /// Closes a buffered cursor if applicable.
    pub(crate) fn close_buffered(&mut self) {
        match self {
            Self::Buffered(c) => c.close(),
            #[cfg(feature = "bolt")]
            Self::Bolt(_) => {}
        }
    }
}

/// Pull-based standalone result stream owning a pooled connection until exhaustion or close.
///
/// Buffered protocols release their pooled connection immediately on open. Server-streaming
/// protocols retain the connection until the final chunk arrives or [`Self::close`] completes.
/// Dropping an unclosed server-streaming `ResultStream` synchronously marks its connection
/// broken without blocking I/O so the pool closes the socket and replaces it.
pub struct ResultStream<C: AsyncConnection + 'static> {
    conn: Option<PooledConnection<C>>,
    cursor: StreamCursor,
    closed: bool,
}

impl<C: AsyncConnection + 'static> ResultStream<C> {
    /// Creates a new `ResultStream` from an optional leased connection and open cursor.
    pub(crate) fn new(conn: Option<PooledConnection<C>>, cursor: StreamCursor) -> Self {
        Self {
            conn,
            cursor,
            closed: false,
        }
    }

    /// Creates a `ResultStream` from a client-side buffered cursor without holding a pooled connection.
    pub fn from_buffered(cursor: BufferedStreamCursor) -> Self {
        Self {
            conn: None,
            cursor: StreamCursor::Buffered(cursor),
            closed: false,
        }
    }

    /// Returns the projected column names for this stream.
    pub fn columns(&self) -> &[String] {
        self.cursor.columns()
    }

    /// Returns the per-column Arrow conversion hints for this stream.
    pub fn column_hints(&self) -> &[ColumnHint] {
        self.cursor.column_hints()
    }

    /// Returns `true` if rows are pulled incrementally from the server over the wire.
    pub fn is_server_streaming(&self) -> bool {
        self.cursor.is_server_streaming()
    }

    /// Returns `true` once the stream has been exhausted, explicitly closed, or failed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Returns the accumulated query execution summary.
    pub fn summary(&self) -> &QuerySummary {
        self.cursor.summary()
    }

    /// Pulls the next bounded chunk of rows, or `Ok(None)` when exhausted.
    ///
    /// When the final chunk arrives (`cursor.is_wire_clean()` becomes `true`),
    /// the leased pooled connection is returned to the pool immediately while
    /// still returning that final chunk.
    pub async fn next_chunk(&mut self) -> Result<Option<ResultChunk>> {
        if self.closed {
            return Ok(None);
        }

        if self.conn.is_none() {
            if !self.cursor.is_server_streaming() {
                let chunk = self.cursor.next_buffered_chunk();
                if chunk.is_none() {
                    self.closed = true;
                }
                return Ok(chunk);
            }
            self.closed = true;
            return Ok(None);
        }

        let res = {
            let conn = self.conn.as_mut().expect("checked conn.is_some()");
            conn.next_stream_chunk(&mut self.cursor).await
        };

        match res {
            Ok(Some(chunk)) => {
                if self.cursor.is_wire_clean() {
                    drop(self.conn.take());
                }
                Ok(Some(chunk))
            }
            Ok(None) => {
                drop(self.conn.take());
                self.closed = true;
                Ok(None)
            }
            Err(err) => {
                if let Some(mut conn) = self.conn.take() {
                    if !self.cursor.is_wire_clean() {
                        conn.mark_broken();
                    }
                    drop(conn);
                }
                self.closed = true;
                Err(err)
            }
        }
    }

    /// Cleanly closes the stream, discarding any unread server-side rows. Idempotent.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        if let Some(mut conn) = self.conn.take() {
            if !self.cursor.is_wire_clean() {
                let close_timeout = conn.stream_close_timeout();
                let res =
                    tokio::time::timeout(close_timeout, conn.close_stream(&mut self.cursor)).await;
                match res {
                    Ok(Ok(())) => {
                        drop(conn);
                        return Ok(());
                    }
                    Ok(Err(err)) => {
                        if !self.cursor.is_wire_clean() {
                            conn.mark_broken();
                        }
                        drop(conn);
                        return Err(err);
                    }
                    Err(_) => {
                        conn.mark_broken();
                        drop(conn);
                        return Err(NetError::Timeout(format!(
                            "Closing stream timed out after {:?}",
                            close_timeout
                        )));
                    }
                }
            }
            drop(conn);
        } else {
            self.cursor.close_buffered();
        }
        Ok(())
    }
}

impl<C: AsyncConnection + 'static> Drop for ResultStream<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.as_mut()
            && !self.cursor.is_wire_clean()
        {
            // Never perform blocking I/O in Drop: mark broken so the pool
            // closes the socket and lets the server abort the open stream.
            conn.mark_broken();
        }
    }
}

/// Pull-based stream borrowing an active [`NativeTransaction`].
///
/// While this stream is open, Rust's `&mut` borrow prevents concurrent operations
/// on `tx`. Call [`Self::close`] before dropping for clean early exit; dropping an
/// unclosed server-streaming `TransactionStream` synchronously marks the transaction
/// connection broken without blocking I/O.
pub struct TransactionStream<'tx, C: AsyncConnection + 'static> {
    tx: &'tx mut NativeTransaction<C>,
    cursor: StreamCursor,
    closed: bool,
}

impl<'tx, C: AsyncConnection + 'static> TransactionStream<'tx, C> {
    /// Creates a new `TransactionStream` borrowing `tx`.
    pub(crate) fn new(tx: &'tx mut NativeTransaction<C>, cursor: StreamCursor) -> Self {
        Self {
            tx,
            cursor,
            closed: false,
        }
    }

    /// Returns the projected column names for this stream.
    pub fn columns(&self) -> &[String] {
        self.cursor.columns()
    }

    /// Returns the per-column Arrow conversion hints for this stream.
    pub fn column_hints(&self) -> &[ColumnHint] {
        self.cursor.column_hints()
    }

    /// Returns `true` if rows are pulled incrementally from the server over the wire.
    pub fn is_server_streaming(&self) -> bool {
        self.cursor.is_server_streaming()
    }

    /// Returns `true` once the stream has been exhausted, explicitly closed, or failed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Returns the accumulated query execution summary.
    pub fn summary(&self) -> &QuerySummary {
        self.cursor.summary()
    }

    /// Pulls the next bounded chunk of rows, or `Ok(None)` when exhausted.
    pub async fn next_chunk(&mut self) -> Result<Option<ResultChunk>> {
        if self.closed {
            return Ok(None);
        }
        match self.tx.next_stream_chunk(&mut self.cursor).await {
            Ok(Some(chunk)) => Ok(Some(chunk)),
            Ok(None) => {
                self.closed = true;
                Ok(None)
            }
            Err(err) => {
                self.closed = true;
                Err(err)
            }
        }
    }

    /// Cleanly closes the stream, discarding any unread rows within the transaction. Idempotent.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.tx.close_stream_cursor(&mut self.cursor).await
    }
}

impl<C: AsyncConnection + 'static> Drop for TransactionStream<'_, C> {
    fn drop(&mut self) {
        if !self.closed {
            if !self.cursor.is_wire_clean() {
                self.tx.abort_stream_on_drop();
            } else {
                self.tx.clear_clean_stream(&self.cursor);
            }
        }
    }
}
