"""In-memory mock bridge implementations."""

from __future__ import annotations

import time
from collections.abc import Sequence
from typing import Any

import polars as pl

from voyager_ogm.bridges.base import (
    BridgeResult,
    BufferedBridgeStream,
    BulkExecutionResult,
    SyncBridgeStreamAsAsync,
    _infer_record_columns,
    _validate_savepoint_name,
    validate_graph_name,
    validate_pagination_parameters,
)
from voyager_ogm.codecs import records_to_polars
from voyager_ogm.ingestion import BulkIngestionPlan
from voyager_ogm.types import unwrap_spatial_param


class MockTransaction:
    """In-memory stand-in for a database transaction on MockBridge."""

    def __init__(self, bridge: MockBridge) -> None:
        """Initializes the mock transaction.

        Args:
            bridge: Parent MockBridge that records executed statements.
        """
        self._bridge = bridge
        self._state = "ACTIVE"
        self._savepoints: dict[str, int] = {}
        self._open_stream = False

    @property
    def state(self) -> str:
        """Returns the lifecycle state ('ACTIVE', 'FAILED', 'COMMITTED', 'ROLLED_BACK')."""
        return self._state

    @property
    def is_active(self) -> bool:
        """Returns whether the mock transaction accepts statements."""
        return self._state == "ACTIVE"

    @property
    def supports_savepoints(self) -> bool:
        """Mock transactions support in-memory savepoints."""
        return True

    def _ensure_no_open_stream(self) -> None:
        """Fails fast when a stream is open on this mock transaction."""
        if self._open_stream:
            raise RuntimeError(
                "an active stream is open on this transaction; close or exhaust it first"
            )

    def _clear_open_stream(self) -> None:
        """Clears the open-stream fail-fast flag."""
        self._open_stream = False

    def stream(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        fetch_size: int = 1_000,
    ) -> BufferedBridgeStream:
        """Slices the next canned mock result into ``fetch_size``-row batches.

        Args:
            statement: Query statement.
            parameters: Query parameters.
            fetch_size: Maximum rows per yielded batch.

        Returns:
            A buffered stream over the mock rows.
        """
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        self._ensure_no_open_stream()
        records = self._bridge.execute(statement, parameters)
        self._open_stream = True
        return BufferedBridgeStream.from_records(
            records,
            fetch_size,
            close_callback=self._clear_open_stream,
            gc_callback=self._clear_open_stream,
        )

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement through the mock bridge."""
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        self._ensure_no_open_stream()
        return self._bridge.execute(statement, parameters)

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement through the mock bridge and returns a DataFrame."""
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        self._ensure_no_open_stream()
        return self._bridge.execute_to_polars(statement, parameters)

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan through the mock bridge."""
        if not self.is_active:
            raise ValueError("Cannot execute bulk plan on inactive transaction")
        self._ensure_no_open_stream()
        return self._bridge.execute_bulk(plan_or_statement, batches)

    def commit(self) -> None:
        """Marks the mock transaction committed."""
        self._ensure_no_open_stream()
        if not self.is_active:
            raise ValueError("Cannot commit inactive transaction")
        self._state = "COMMITTED"
        self._savepoints.clear()

    def rollback(self) -> None:
        """Marks the mock transaction rolled back."""
        if not self.is_active:
            raise ValueError("Cannot rollback inactive transaction")
        self._state = "ROLLED_BACK"
        self._savepoints.clear()

    def savepoint(self, name: str) -> None:
        """Creates an in-memory savepoint and records the query log length."""
        if not self.is_active:
            raise ValueError("Cannot create savepoint on inactive transaction")
        _validate_savepoint_name(name)
        if name in self._savepoints:
            raise ValueError(f"Savepoint {name!r} already exists")
        self._savepoints[name] = len(self._bridge.executed_queries)

    def rollback_to_savepoint(self, name: str) -> None:
        """Rolls the mock query log back to a savepoint."""
        if not self.is_active:
            raise ValueError("Cannot rollback savepoint on inactive transaction")
        if name not in self._savepoints:
            raise ValueError(f"Savepoint {name!r} does not exist")
        del self._bridge.executed_queries[self._savepoints[name] :]

    def release_savepoint(self, name: str) -> None:
        """Releases an in-memory savepoint."""
        if not self.is_active:
            raise ValueError("Cannot release savepoint on inactive transaction")
        if name not in self._savepoints:
            raise ValueError(f"Savepoint {name!r} does not exist")
        self._savepoints.pop(name, None)


class MockBridge:
    """In-memory Mock Bridge for zero-network testing and query inspection."""

    def __init__(self, graph_name: str | None = None) -> None:
        if graph_name is not None:
            validate_graph_name(graph_name)
        self.graph_name = graph_name
        self.executed_queries: list[tuple[str, dict[str, Any]]] = []
        self._canned_results: list[
            tuple[list[dict[str, Any]] | pl.DataFrame, tuple[str, ...] | None]
        ] = []

    def queue_result(
        self,
        result: list[dict[str, Any]] | pl.DataFrame,
        columns: Sequence[str] | None = None,
    ) -> None:
        """Queues a canned result to be returned by future execute() calls.

        Args:
            result: Result records or DataFrame to return.
            columns: Optional explicit column names (useful when queueing an empty list).
        """
        resolved_cols: tuple[str, ...] | None
        if columns is not None:
            resolved_cols = tuple(str(c) for c in columns)
        elif isinstance(result, pl.DataFrame):
            resolved_cols = tuple(str(c) for c in result.columns)
        else:
            existing_cols = getattr(result, "columns", None)
            if existing_cols is not None:
                resolved_cols = tuple(str(c) for c in existing_cols)
            else:
                resolved_cols = _infer_record_columns(result) or None
        self._canned_results.append((result, resolved_cols))

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Records the query and returns the next canned result or empty list.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            List of record dictionaries.
        """
        validate_pagination_parameters(statement, parameters)
        params = unwrap_spatial_param(parameters or {})
        self.executed_queries.append((statement, params))
        if self._canned_results:
            res, cols = self._canned_results.pop(0)
            if isinstance(res, pl.DataFrame):
                return BridgeResult(
                    res.to_dicts(),
                    columns=cols if cols is not None else tuple(str(c) for c in res.columns),
                )
            return BridgeResult(res, columns=cols)
        return BridgeResult([], columns=())

    def stream(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        fetch_size: int = 1_000,
    ) -> BufferedBridgeStream:
        """Records the query and slices the next canned result into batches.

        Args:
            statement: Query statement.
            parameters: Query parameters.
            fetch_size: Maximum rows per yielded batch.

        Returns:
            A buffered stream over the canned result rows.
        """
        records = self.execute(statement, parameters)
        return BufferedBridgeStream.from_records(records, fetch_size)

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Records the query and returns the next canned result as a Polars DataFrame.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame.
        """
        validate_pagination_parameters(statement, parameters)
        params = unwrap_spatial_param(parameters or {})
        self.executed_queries.append((statement, params))
        if self._canned_results:
            res, cols = self._canned_results.pop(0)
            if isinstance(res, pl.DataFrame):
                return res
            return records_to_polars(BridgeResult(res, columns=cols))
        return pl.DataFrame()

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk ingestion plan against the mock bridge.

        Args:
            plan_or_statement: BulkIngestionPlan or statement string.
            batches: Batch parameter sequence.

        Returns:
            BulkExecutionResult summary.
        """
        start_time = time.perf_counter()
        statement = ""
        total_records = 0
        total_batches = 0

        if isinstance(plan_or_statement, str):
            statement = plan_or_statement
            batch_list = batches or []
            total_batches = len(batch_list)
            for b in batch_list:
                batch_data = b.get("batch", [])
                total_records += len(batch_data)
                self.executed_queries.append((statement, unwrap_spatial_param(b)))
        else:
            statement = plan_or_statement.statement
            for batch_item in plan_or_statement:
                total_batches += 1
                batch_data = batch_item.parameters.get("batch", [])
                total_records += len(batch_data)
                self.executed_queries.append(
                    (batch_item.statement, unwrap_spatial_param(batch_item.parameters))
                )

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    def begin_transaction(self) -> MockTransaction:
        """Opens an in-memory mock transaction."""
        return MockTransaction(self)

    def ping(self) -> bool:
        """Pings the mock bridge."""
        return True

    def close(self) -> None:
        """Closes the bridge."""
        pass


class AsyncMockTransaction:
    """Asynchronous in-memory stand-in for a database transaction."""

    def __init__(self, transaction: MockTransaction) -> None:
        """Initializes the async wrapper around a sync mock transaction."""
        self._sync = transaction

    @property
    def state(self) -> str:
        """Returns the lifecycle state."""
        return self._sync.state

    @property
    def is_active(self) -> bool:
        """Returns whether the transaction accepts statements."""
        return self._sync.is_active

    @property
    def supports_savepoints(self) -> bool:
        """Mock transactions support in-memory savepoints."""
        return True

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement through the mock bridge."""
        return self._sync.execute(statement, parameters)

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement and returns a Polars DataFrame."""
        return self._sync.execute_to_polars(statement, parameters)

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan through the mock bridge."""
        return self._sync.execute_bulk(plan_or_statement, batches)

    async def commit(self) -> None:
        """Marks the mock transaction committed."""
        self._sync.commit()

    async def rollback(self) -> None:
        """Marks the mock transaction rolled back."""
        self._sync.rollback()

    async def savepoint(self, name: str) -> None:
        """Creates an in-memory savepoint."""
        self._sync.savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rolls the mock query log back to a savepoint."""
        self._sync.rollback_to_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        """Releases an in-memory savepoint."""
        self._sync.release_savepoint(name)


class AsyncMockBridge:
    """Asynchronous in-memory Mock Bridge for testing."""

    def __init__(self, sync_mock: MockBridge | None = None, graph_name: str | None = None) -> None:
        if graph_name is not None:
            validate_graph_name(graph_name)
        self.sync_mock = sync_mock or MockBridge(graph_name=graph_name)
        if graph_name is not None:
            self.sync_mock.graph_name = graph_name

    @property
    def graph_name(self) -> str | None:
        """Returns the configured Apache AGE graph name."""
        return self.sync_mock.graph_name

    @graph_name.setter
    def graph_name(self, value: str | None) -> None:
        if value is not None:
            validate_graph_name(value)
        self.sync_mock.graph_name = value

    @property
    def executed_queries(self) -> list[tuple[str, dict[str, Any]]]:
        """Returns history of executed queries."""
        return self.sync_mock.executed_queries

    def queue_result(
        self,
        result: list[dict[str, Any]] | pl.DataFrame,
        columns: Sequence[str] | None = None,
    ) -> None:
        """Queues canned result for future executions.

        Args:
            result: Result records or DataFrame.
            columns: Optional explicit column names (useful when queueing an empty list).
        """
        self.sync_mock.queue_result(result, columns=columns)

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Asynchronously executes and records query.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            List of record dictionaries.
        """
        return self.sync_mock.execute(statement, parameters)

    async def stream(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        fetch_size: int = 1_000,
    ) -> SyncBridgeStreamAsAsync:
        """Slices the next canned mock result into asynchronous batches.

        Args:
            statement: Query statement.
            parameters: Query parameters.
            fetch_size: Maximum rows per yielded batch.

        Returns:
            An async wrapper over the buffered mock stream.
        """
        return SyncBridgeStreamAsAsync(
            self.sync_mock.stream(statement, parameters, fetch_size), offload=False
        )

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Asynchronously executes query returning Polars DataFrame.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame.
        """
        return self.sync_mock.execute_to_polars(statement, parameters)

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Asynchronously executes bulk ingestion plan.

        Args:
            plan_or_statement: BulkIngestionPlan or statement.
            batches: Batch parameter sequence.

        Returns:
            BulkExecutionResult.
        """
        return self.sync_mock.execute_bulk(plan_or_statement, batches)

    async def begin_transaction(self) -> AsyncMockTransaction:
        """Opens an in-memory mock transaction."""
        return AsyncMockTransaction(MockTransaction(self.sync_mock))

    async def ping(self) -> bool:
        """Pings the async mock bridge."""
        return True

    async def close(self) -> None:
        """Closes bridge."""
        pass
