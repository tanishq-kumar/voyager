"""Database bridging layer and driver adapters for Voyager OGM.

Provides vendor-neutral sync and async bridge protocols, enabling applications
to connect existing database drivers (Neo4j/Memgraph Bolt, DuckDB DuckPGQ, etc.)
or mock bridges without hard coupling to specific driver packages.
"""

from __future__ import annotations

import asyncio
import datetime
import importlib
import inspect
import json
import math
import operator
import re
import time
import uuid
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal, Protocol, overload, runtime_checkable

import polars as pl

from voyager_ogm.agtype import decode_agtype
from voyager_ogm.entities import (
    GraphNode,
    GraphPath,
    GraphRelationship,
    convert_driver_value,
    json_default,
    serialize_records_for_arrow,
    serialize_value_for_arrow,
)
from voyager_ogm.ingestion import BulkIngestionPlan
from voyager_ogm.types import Duration, Json, parse_pg_interval_text, unwrap_spatial_param

_PAGINATION_PARAMETER_PATTERN = re.compile(
    r"\b(?:SKIP|OFFSET|LIMIT)\s+\$([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE
)

#: Statements that need the DuckPGQ extension loaded on the DuckDB connection.
_DUCKPGQ_STATEMENT_PATTERN = re.compile(r"\bGRAPH_TABLE\b|\bPROPERTY\s+GRAPH\b", re.IGNORECASE)

#: Cypher bulk plans that DuckDB cannot parse; ingest them as relational rows instead.
_CYPHER_BULK_STATEMENT_PATTERN = re.compile(r"^\s*UNWIND\b", re.IGNORECASE)


def _duckpgq_error_message(cause: BaseException) -> str:
    """Builds the actionable error shown when DuckPGQ cannot be loaded."""
    return (
        "SQL/PGQ queries on DuckDB require the DuckPGQ extension, but it could not be "
        "loaded. Install it with `INSTALL duckpgq FROM community;` and then run "
        "`LOAD duckpgq;` on the connection (Voyager does the `LOAD` for you once per "
        "connection), or create the Session with install_duckpgq=True so Voyager "
        "installs it first. "
        f"The DuckDB error was: {cause}"
    )


def validate_pagination_parameters(statement: str, parameters: Mapping[str, Any] | None) -> None:
    """Rejects negative or non-integer values bound to pagination parameters.

    Engines disagree on a bad ``SKIP``/``OFFSET``/``LIMIT`` binding: Neo4j and
    Memgraph raise, while Apache AGE, PostgreSQL and DuckDB silently truncate a
    fractional value. Validating before dispatch gives every dialect the same
    clear failure and keeps the error independent of the target database.

    Args:
        statement: Compiled statement, before dialect-specific parameter rewriting.
        parameters: Bound parameter values.

    Raises:
        ValueError: If a parameter bound to ``SKIP``, ``OFFSET`` or ``LIMIT`` is
            not a non-negative integer.
    """
    if not parameters:
        return
    for name in sorted(set(_PAGINATION_PARAMETER_PATTERN.findall(statement))):
        if name not in parameters:
            continue
        value = parameters[name]
        try:
            # operator.index accepts every true integer type (int, NumPy and
            # Polars integers) and rejects floats, strings and Decimal.
            count = None if isinstance(value, bool) else operator.index(value)
        except TypeError:
            count = None
        if count is None or count < 0:
            raise ValueError(
                f"Pagination parameter '${name}' must be a non-negative integer, "
                f"got {type(value).__name__} {value!r}"
            )


@dataclass
class BulkExecutionResult:
    """Result summary of a bulk ingestion execution.

    Attributes:
        total_batches: Number of chunks dispatched to the database.
        total_records: Total count of records inserted or merged.
        duration_seconds: Total wall-clock execution time in seconds.
        statement: Parameterized query statement executed.
    """

    total_batches: int
    total_records: int
    duration_seconds: float
    statement: str


def _infer_record_columns(records: Sequence[Any]) -> tuple[str, ...]:
    """Extracts ordered column names from the first mapping record, if present."""
    if records and isinstance(records[0], Mapping):
        return tuple(str(key) for key in records[0].keys())
    return ()


class BridgeResult(list[dict[str, Any]]):
    """List of record dictionaries carrying driver-reported projection column names.

    Subclasses ``list[dict[str, Any]]`` so bridges preserve their existing list
    return contract while forwarding column metadata on zero-row results.
    """

    def __init__(
        self,
        records: Sequence[dict[str, Any]] = (),
        columns: Sequence[str] | None = None,
    ) -> None:
        super().__init__(records)
        if columns is not None:
            self._columns: tuple[str, ...] = tuple(str(col) for col in columns)
        else:
            self._columns = _infer_record_columns(records)

    @property
    def columns(self) -> tuple[str, ...]:
        """Ordered projection column names from the driver metadata."""
        return self._columns


def _records_to_polars(raw_records: list[dict[str, Any]]) -> pl.DataFrame:
    """Converts bridge records (preserving empty-result ``.columns``) to a Polars DataFrame."""
    columns = getattr(raw_records, "columns", None)
    records = serialize_records_for_arrow(raw_records)
    if records:
        return pl.DataFrame(records)
    if columns:
        return pl.DataFrame(schema=list(columns))
    return pl.DataFrame()


@runtime_checkable
class DatabaseBridge(Protocol):
    """Synchronous Database Bridge Protocol."""

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a compiled statement and returns records as a list of dicts.

        Args:
            statement: Query statement string.
            parameters: Query parameters dictionary.

        Returns:
            List of result records.
        """
        ...

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a compiled statement and returns records as a Polars DataFrame.

        Args:
            statement: Query statement string.
            parameters: Query parameters dictionary.

        Returns:
            Polars DataFrame containing result records.
        """
        ...

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk ingestion plan across batches.

        Args:
            plan_or_statement: BulkIngestionPlan instance or query statement.
            batches: Batch parameter dictionaries if statement is raw string.

        Returns:
            BulkExecutionResult metrics.
        """
        ...

    def close(self) -> None:
        """Closes the underlying driver connection."""
        ...


@runtime_checkable
class AsyncDatabaseBridge(Protocol):
    """Asynchronous Database Bridge Protocol."""

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a compiled statement and returns records as a list of dicts.

        Args:
            statement: Query statement string.
            parameters: Query parameters dictionary.

        Returns:
            List of result records.
        """
        ...

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a compiled statement and returns records as a Polars DataFrame.

        Args:
            statement: Query statement string.
            parameters: Query parameters dictionary.

        Returns:
            Polars DataFrame containing result records.
        """
        ...

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk ingestion plan across batches asynchronously.

        Args:
            plan_or_statement: BulkIngestionPlan instance or query statement.
            batches: Batch parameter dictionaries if statement is raw string.

        Returns:
            BulkExecutionResult metrics.
        """
        ...

    async def close(self) -> None:
        """Closes the underlying driver connection."""
        ...


def _validate_savepoint_name(name: str) -> str:
    """Validates an unquoted savepoint identifier before interpolating SQL.

    Args:
        name: Caller-supplied savepoint name.

    Returns:
        The validated savepoint name.

    Raises:
        ValueError: If the name is not a safe SQL identifier.
    """
    if not isinstance(name, str) or not name:
        raise ValueError("Savepoint name must be a non-empty string")
    if not (name[0].isalpha() or name[0] == "_"):
        raise ValueError(f"Invalid savepoint name {name!r}")
    if not all(ch.isalnum() or ch == "_" for ch in name):
        raise ValueError(f"Invalid savepoint name {name!r}")
    return name


_SAFE_GRAPH_NAME_REGEX = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def validate_graph_name(graph_name: str) -> str:
    """Validates that a graph name is a safe SQL/AGE identifier.

    Graph names in Apache AGE are interpolated into SQL queries
    (e.g. `cypher('<graph_name>', ...)`). To prevent SQL injection and syntax
    errors, graph names must be valid alphanumeric/underscore identifiers
    starting with an ASCII letter or underscore, up to 63 characters in length.

    Args:
        graph_name: The graph name to validate.

    Returns:
        The validated graph name.

    Raises:
        ValueError: If graph_name is not a valid identifier.
    """
    if not isinstance(graph_name, str) or not _SAFE_GRAPH_NAME_REGEX.match(graph_name):
        raise ValueError(
            f"Invalid graph name {graph_name!r}. Graph names must be valid alphanumeric/underscore "
            "identifiers (1-63 characters, starting with an ASCII letter or underscore)."
        )
    return graph_name


def _pg_transaction_status(conn: Any) -> str | None:
    """Returns the psycopg transaction status name when the driver exposes it."""
    info = getattr(conn, "info", None)
    if info is not None and hasattr(info, "transaction_status"):
        status = info.transaction_status
        return str(getattr(status, "name", status)).upper()
    getter = getattr(conn, "get_transaction_status", None)
    if callable(getter):
        try:
            status = getter()
        except Exception:
            return None
        return str(getattr(status, "name", status)).upper()
    return None


def _pg_connection_closed(conn: Any) -> bool:
    """Returns whether a psycopg connection reports itself closed."""
    value = getattr(conn, "closed", 0)
    try:
        return bool(value)
    except Exception:
        return False


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

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement through the mock bridge."""
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        return self._bridge.execute(statement, parameters)

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement through the mock bridge and returns a DataFrame."""
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        return self._bridge.execute_to_polars(statement, parameters)

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan through the mock bridge."""
        if not self.is_active:
            raise ValueError("Cannot execute bulk plan on inactive transaction")
        return self._bridge.execute_bulk(plan_or_statement, batches)

    def commit(self) -> None:
        """Marks the mock transaction committed."""
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
            return _records_to_polars(BridgeResult(res, columns=cols))
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


def _adapt_bolt_time(val: datetime.time) -> Any:
    """Converts an offset-aware Python time to ``neo4j.time.Time`` with a fixed offset.

    The official driver rejects ``datetime.timezone`` objects passed on a plain
    ``datetime.time`` because it calls ``tzinfo.utcoffset`` with a ``Time``
    instance.  ``pytz.FixedOffset`` accepts any object and keeps the offset.
    """
    try:
        from neo4j.time import Time as Neo4jTime
    except ImportError:
        return None
    offset = val.utcoffset()
    if offset is None:
        return None
    try:
        import importlib

        pytz = importlib.import_module("pytz")
    except ImportError:
        return None
    minutes = int(offset.total_seconds() // 60)
    return Neo4jTime(
        val.hour,
        val.minute,
        val.second,
        val.microsecond * 1_000,
        tzinfo=pytz.FixedOffset(minutes),
    )


def _adapt_bolt_zoneinfo_datetime(val: datetime.datetime) -> Any:
    """Converts a ``zoneinfo.ZoneInfo`` datetime to a driver-safe ``DateTime``.

    ``neo4j`` 6.1.0 can segfault while serialising a raw ``ZoneInfo`` value.
    Converting through a ``pytz`` zone mirrors the official client conversion,
    keeps the zone name, and preserves ``fold`` on DST fall-back instants.
    """
    import zoneinfo

    tzinfo = val.tzinfo
    key = getattr(tzinfo, "key", None)
    if not isinstance(tzinfo, zoneinfo.ZoneInfo) or not isinstance(key, str):
        return None
    try:
        from neo4j.time import DateTime as Neo4jDateTime
    except ImportError:
        return None
    try:
        import importlib

        pytz = importlib.import_module("pytz")
    except ImportError:
        # Keeping the instant is safer than handing raw ZoneInfo back to the
        # driver; the IANA zone name is lost on this fallback path.
        offset = val.utcoffset()
        if offset is None:
            return None
        return val.replace(tzinfo=datetime.timezone(offset))
    localized = pytz.timezone(key).localize(val.replace(tzinfo=None), is_dst=not bool(val.fold))
    return Neo4jDateTime(
        localized.year,
        localized.month,
        localized.day,
        localized.hour,
        localized.minute,
        localized.second,
        localized.microsecond * 1_000,
        tzinfo=localized.tzinfo,
    )


def _adapt_bolt_value(val: Any) -> Any:
    """Adapts Voyager Duration, Json, UUID, temporal and tagged spatial values to Bolt driver objects."""
    if isinstance(val, Json):
        return _adapt_bolt_value(val.value)
    if isinstance(val, uuid.UUID):
        return str(val)
    if isinstance(val, datetime.datetime):
        adapted_datetime = _adapt_bolt_zoneinfo_datetime(val)
        if adapted_datetime is not None:
            return adapted_datetime
    elif isinstance(val, datetime.time):
        adapted_time = _adapt_bolt_time(val)
        if adapted_time is not None:
            return adapted_time
    if isinstance(val, Duration):
        try:
            from neo4j.time import Duration as Neo4jDuration

            return Neo4jDuration(
                months=val.months,
                days=val.days,
                seconds=val.seconds,
                nanoseconds=val.nanoseconds,
            )
        except ImportError:
            return val.to_iso()

    try:
        from neo4j.spatial import CartesianPoint, WGS84Point
    except ImportError:
        if isinstance(val, dict):
            if "__voyager_spatial__" in val:
                return val["__voyager_spatial__"]
            return {k: _adapt_bolt_value(v) for k, v in val.items()}
        if isinstance(val, (list, tuple)):
            return [_adapt_bolt_value(x) for x in val]
        return val

    from voyager_ogm.types import WGS_84_2D, WGS_84_3D, Point

    if isinstance(val, Point):
        if val.is_geographic:
            lat = float(val.latitude) if val.latitude is not None else float(val.y)
            lon = float(val.longitude) if val.longitude is not None else float(val.x)
            if val.is_3d and val.height is not None:
                return WGS84Point((lon, lat, float(val.height)))
            return WGS84Point((lon, lat))
        else:
            if val.is_3d and val.z is not None:
                return CartesianPoint((float(val.x), float(val.y), float(val.z)))
            return CartesianPoint((float(val.x), float(val.y)))

    if isinstance(val, dict):
        if "__voyager_spatial__" in val:
            sp = val["__voyager_spatial__"]
            if not isinstance(sp, dict):
                return sp
            srid = sp.get("srid")
            is_geo = (
                (srid in (WGS_84_2D, WGS_84_3D))
                if srid
                else ("latitude" in sp or "longitude" in sp)
            )
            if is_geo:
                lat_raw = sp.get("latitude") if "latitude" in sp else sp.get("y")
                lon_raw = sp.get("longitude") if "longitude" in sp else sp.get("x")
                if lat_raw is None or lon_raw is None:
                    raise ValueError(
                        f"Geographic spatial data missing required latitude/longitude: {sp}"
                    )
                lat = float(lat_raw)
                lon = float(lon_raw)
                height = sp.get("height", sp.get("z"))
                if height is not None:
                    return WGS84Point((lon, lat, float(height)))
                return WGS84Point((lon, lat))
            else:
                if "x" not in sp or "y" not in sp or sp["x"] is None or sp["y"] is None:
                    raise ValueError(
                        f"Cartesian spatial data missing required 'x' or 'y' coordinate: {sp}"
                    )
                x = float(sp["x"])
                y = float(sp["y"])
                z = sp.get("z")
                if z is not None:
                    return CartesianPoint((x, y, float(z)))
                return CartesianPoint((x, y))
        return {k: _adapt_bolt_value(v) for k, v in val.items()}

    if isinstance(val, (list, tuple)):
        return [_adapt_bolt_value(x) for x in val]

    return val


def _adapt_bolt_parameters(params: dict[str, Any]) -> dict[str, Any]:
    """Adapts statement and batch parameters for Bolt protocol execution, converting tagged spatial values to native spatial points."""
    return {k: _adapt_bolt_value(v) for k, v in params.items()}


def _normalize_bolt_record(record: Any) -> dict[str, Any]:
    """Extracts record dictionary from Bolt Record, converting entities to GraphNode/GraphRelationship/GraphPath."""
    if isinstance(record, dict):
        return {k: convert_driver_value(v) for k, v in record.items()}
    items_fn = getattr(record, "items", None)
    if callable(items_fn):
        return {k: convert_driver_value(v) for k, v in items_fn()}
    raw = dict(record.data()) if hasattr(record, "data") else dict(record)
    return {k: convert_driver_value(v) for k, v in raw.items()}


def _bolt_result_keys(result: Any) -> tuple[str, ...]:
    """Extracts ordered column keys from a Bolt Result if exposed by the driver."""
    keys_fn = getattr(result, "keys", None)
    if callable(keys_fn):
        return tuple(str(k) for k in keys_fn())
    return ()


def _bolt_bridge_result(keys: tuple[str, ...], records: list[dict[str, Any]]) -> BridgeResult:
    """Packages Bolt records with driver keys, falling back to first-row keys for test doubles."""
    cols = keys if keys else _infer_record_columns(records)
    return BridgeResult(records, columns=cols)


class Neo4jBoltBridge:
    """Synchronous Neo4j / Memgraph Bolt protocol driver bridge."""

    def __init__(self, driver: Any, database: str | None = None) -> None:
        """Initializes Neo4jBoltBridge.

        Args:
            driver: Neo4j driver instance.
            database: Optional target database name.
        """
        self.driver = driver
        self.database = database

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a Cypher statement over Neo4j Bolt session.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.

        Returns:
            List of record dictionaries.
        """
        validate_pagination_parameters(statement, parameters)
        params = _adapt_bolt_parameters(parameters or {})
        session_kwargs = {"database": self.database} if self.database else {}
        with self.driver.session(**session_kwargs) as session:
            result = session.run(statement, params)
            keys = _bolt_result_keys(result)
            records = [_normalize_bolt_record(record) for record in result]
            return _bolt_bridge_result(keys, records)

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes Cypher statement and returns Polars DataFrame.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame.
        """
        return _records_to_polars(self.execute(statement, parameters))

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes bulk batch ingestion over Bolt protocol.

        Args:
            plan_or_statement: BulkIngestionPlan or query statement.
            batches: Batch parameter sequence.

        Returns:
            BulkExecutionResult summary.
        """
        start_time = time.perf_counter()
        statement = ""
        total_records = 0
        total_batches = 0
        session_kwargs = {"database": self.database} if self.database else {}

        with self.driver.session(**session_kwargs) as session:
            if isinstance(plan_or_statement, str):
                statement = plan_or_statement
                batch_list = batches or []
                total_batches = len(batch_list)
                for b in batch_list:
                    batch_data = b.get("batch", [])
                    total_records += len(batch_data)
                    session.run(statement, _adapt_bolt_parameters(b))
            else:
                statement = plan_or_statement.statement
                for batch_item in plan_or_statement:
                    total_batches += 1
                    batch_data = batch_item.parameters.get("batch", [])
                    total_records += len(batch_data)
                    session.run(batch_item.statement, _adapt_bolt_parameters(batch_item.parameters))

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    def ping(self) -> bool:
        """Pings Neo4j connection to verify liveness."""
        if hasattr(self.driver, "verify_connectivity"):
            try:
                self.driver.verify_connectivity()
                return True
            except Exception:
                return False
        try:
            self.execute("RETURN 1")
            return True
        except Exception:
            return False

    def begin_transaction(self) -> Neo4jBoltTransaction:
        """Begins an explicit Bolt transaction and leases one driver session."""
        return Neo4jBoltTransaction(self.driver, self.database)

    def close(self) -> None:
        """Closes the underlying driver."""
        if hasattr(self.driver, "close"):
            self.driver.close()


class Neo4jBoltTransaction:
    """Explicit Bolt transaction holding one Neo4j/Memgraph driver session."""

    _savepoint_error_message = (
        "Neo4j/Memgraph Bolt transactions do not support savepoints; "
        "use Apache AGE/PostgreSQL for real savepoints"
    )

    def __init__(self, driver: Any, database: str | None = None) -> None:
        """Begins a driver transaction.

        Args:
            driver: Neo4j-compatible driver instance.
            database: Optional target database name.
        """
        self._session = driver.session(**({"database": database} if database else {}))
        self._state = "ACTIVE"
        try:
            self._tx = self._session.begin_transaction()
        except Exception:
            self._session.close()
            self._state = "ROLLED_BACK"
            raise

    @property
    def state(self) -> str:
        """Returns the lifecycle state ('ACTIVE', 'FAILED', 'COMMITTED', 'ROLLED_BACK')."""
        return self._state

    @property
    def is_active(self) -> bool:
        """Returns whether the transaction accepts statements."""
        if self._state != "ACTIVE":
            return False
        closed = getattr(self._tx, "closed", None)
        if callable(closed):
            try:
                return not bool(closed())
            except Exception:
                return True
        return True

    @property
    def supports_savepoints(self) -> bool:
        """Bolt transactions (Neo4j/Memgraph) have no savepoints."""
        return False

    def _mark_failed(self) -> None:
        """Marks the transaction aborted after a statement/driver error."""
        self._state = "FAILED"

    def _close_session(self) -> None:
        """Closes the leased driver session without masking the active error."""
        try:
            self._session.close()
        except Exception:
            pass

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Runs a Cypher statement inside the transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        validate_pagination_parameters(statement, parameters)
        try:
            result = self._tx.run(statement, _adapt_bolt_parameters(parameters or {}))
            keys = _bolt_result_keys(result)
            records = [_normalize_bolt_record(record) for record in result]
            return _bolt_bridge_result(keys, records)
        except BaseException:
            self._mark_failed()
            raise

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Runs a Cypher statement and returns a Polars DataFrame."""
        return _records_to_polars(self.execute(statement, parameters))

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Runs a bulk plan on the transaction's driver transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute bulk plan on inactive transaction")
        start_time = time.perf_counter()
        statement = ""
        total_records = 0
        total_batches = 0

        try:
            if isinstance(plan_or_statement, str):
                statement = plan_or_statement
                batch_list = batches or []
                total_batches = len(batch_list)
                for b in batch_list:
                    total_records += len(b.get("batch", []))
                    self._tx.run(statement, _adapt_bolt_parameters(b))
            else:
                statement = plan_or_statement.statement
                for batch_item in plan_or_statement:
                    total_batches += 1
                    total_records += len(batch_item.parameters.get("batch", []))
                    self._tx.run(
                        batch_item.statement, _adapt_bolt_parameters(batch_item.parameters)
                    )
        except BaseException:
            self._mark_failed()
            raise

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    def commit(self) -> None:
        """Commits the Bolt transaction and closes the driver session."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back instead of committing")
        if not self.is_active:
            raise ValueError("Cannot commit inactive transaction")
        try:
            self._tx.commit()
            self._state = "COMMITTED"
        except BaseException:
            self._mark_failed()
            raise
        finally:
            self._close_session()

    def rollback(self) -> None:
        """Rolls the Bolt transaction back and closes the driver session."""
        if self._state not in ("ACTIVE", "FAILED"):
            raise ValueError("Cannot rollback inactive transaction")
        try:
            self._tx.rollback()
            self._state = "ROLLED_BACK"
        except BaseException:
            self._mark_failed()
            raise
        finally:
            self._close_session()

    def savepoint(self, name: str) -> None:
        """Raises because Bolt engines do not support savepoints."""
        raise RuntimeError(
            "Neo4j/Memgraph Bolt transactions do not support savepoints; "
            "use Apache AGE/PostgreSQL for real savepoints"
        )

    def rollback_to_savepoint(self, name: str) -> None:
        """Raises because Bolt engines do not support savepoints."""
        raise RuntimeError(
            "Neo4j/Memgraph Bolt transactions do not support savepoints; "
            "use Apache AGE/PostgreSQL for real savepoints"
        )

    def release_savepoint(self, name: str) -> None:
        """Raises because Bolt engines do not support savepoints."""
        raise RuntimeError(
            "Neo4j/Memgraph Bolt transactions do not support savepoints; "
            "use Apache AGE/PostgreSQL for real savepoints"
        )


class AsyncNeo4jBoltBridge:
    """Asynchronous Neo4j / Memgraph Bolt protocol driver bridge."""

    def __init__(self, driver: Any, database: str | None = None) -> None:
        """Initializes AsyncNeo4jBoltBridge.

        Args:
            driver: Async Neo4j driver instance.
            database: Optional target database name.
        """
        self.driver = driver
        self.database = database

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Asynchronously executes Cypher statement over async Bolt session.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.

        Returns:
            List of record dictionaries.
        """
        validate_pagination_parameters(statement, parameters)
        params = _adapt_bolt_parameters(parameters or {})
        session_kwargs = {"database": self.database} if self.database else {}
        async with self.driver.session(**session_kwargs) as session:
            result = await session.run(statement, params)
            keys = _bolt_result_keys(result)
            if hasattr(result, "__aiter__"):
                records = []
                async for record in result:
                    records.append(_normalize_bolt_record(record))
                return _bolt_bridge_result(keys, records)
            raw = await result.data()
            records = [_normalize_bolt_record(r) for r in raw]
            return _bolt_bridge_result(keys, records)

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Asynchronously executes Cypher statement returning Polars DataFrame.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame.
        """
        return _records_to_polars(await self.execute(statement, parameters))

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Asynchronously executes bulk ingestion plan over Bolt.

        Args:
            plan_or_statement: BulkIngestionPlan or statement.
            batches: Batch parameter sequence.

        Returns:
            BulkExecutionResult metrics.
        """
        start_time = time.perf_counter()
        statement = ""
        total_records = 0
        total_batches = 0
        session_kwargs = {"database": self.database} if self.database else {}

        async with self.driver.session(**session_kwargs) as session:
            if isinstance(plan_or_statement, str):
                statement = plan_or_statement
                batch_list = batches or []
                total_batches = len(batch_list)
                for b in batch_list:
                    batch_data = b.get("batch", [])
                    total_records += len(batch_data)
                    await session.run(statement, _adapt_bolt_parameters(b))
            else:
                statement = plan_or_statement.statement
                for batch_item in plan_or_statement:
                    total_batches += 1
                    batch_data = batch_item.parameters.get("batch", [])
                    total_records += len(batch_data)
                    await session.run(
                        batch_item.statement, _adapt_bolt_parameters(batch_item.parameters)
                    )

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    async def ping(self) -> bool:
        """Pings Neo4j connection asynchronously to verify liveness."""
        if hasattr(self.driver, "verify_connectivity"):
            try:
                res = self.driver.verify_connectivity()
                if hasattr(res, "__await__"):
                    await res
                return True
            except Exception:
                return False
        try:
            await self.execute("RETURN 1")
            return True
        except Exception:
            return False

    async def begin_transaction(self) -> AsyncNeo4jBoltTransaction:
        """Begins an explicit async Bolt transaction and leases one driver session."""
        session = self.driver.session(**({"database": self.database} if self.database else {}))
        try:
            tx = await session.begin_transaction()
        except Exception:
            await session.close()
            raise
        return AsyncNeo4jBoltTransaction(session, tx)

    async def close(self) -> None:
        """Closes the underlying async driver."""
        if hasattr(self.driver, "close"):
            if inspect.iscoroutinefunction(self.driver.close):
                await self.driver.close()
            else:
                self.driver.close()


class AsyncNeo4jBoltTransaction:
    """Explicit async Bolt transaction holding one driver session."""

    _savepoint_error_message = (
        "Neo4j/Memgraph Bolt transactions do not support savepoints; "
        "use Apache AGE/PostgreSQL for real savepoints"
    )

    def __init__(self, session: Any, tx: Any) -> None:
        """Initializes the transaction wrapper.

        Args:
            session: Async neo4j driver session.
            tx: Async neo4j transaction.
        """
        self._session = session
        self._tx = tx
        self._state = "ACTIVE"

    @property
    def state(self) -> str:
        """Returns the lifecycle state."""
        return self._state

    @property
    def is_active(self) -> bool:
        """Returns whether the transaction accepts statements."""
        if self._state != "ACTIVE":
            return False
        closed = getattr(self._tx, "closed", None)
        if callable(closed):
            try:
                return not bool(closed())
            except Exception:
                return True
        return True

    @property
    def supports_savepoints(self) -> bool:
        """Bolt transactions have no savepoints."""
        return False

    def _mark_failed(self) -> None:
        """Marks the transaction aborted after a statement/driver error."""
        self._state = "FAILED"

    async def _close_session(self) -> None:
        """Closes the leased driver session without masking the active error."""
        try:
            await self._session.close()
        except Exception:
            pass

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Runs a Cypher statement inside the transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        try:
            result = await self._tx.run(statement, _adapt_bolt_parameters(parameters or {}))
            keys = _bolt_result_keys(result)
            if hasattr(result, "__aiter__"):
                records = []
                async for record in result:
                    records.append(_normalize_bolt_record(record))
                return _bolt_bridge_result(keys, records)
            raw = await result.data()
            records = [_normalize_bolt_record(r) for r in raw]
            return _bolt_bridge_result(keys, records)
        except BaseException:
            self._mark_failed()
            raise

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Runs a Cypher statement and returns a Polars DataFrame."""
        return _records_to_polars(await self.execute(statement, parameters))

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Runs a bulk plan on the transaction's driver transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute bulk plan on inactive transaction")
        start_time = time.perf_counter()
        statement = ""
        total_records = 0
        total_batches = 0

        try:
            if isinstance(plan_or_statement, str):
                statement = plan_or_statement
                batch_list = batches or []
                total_batches = len(batch_list)
                for b in batch_list:
                    total_records += len(b.get("batch", []))
                    await self._tx.run(statement, _adapt_bolt_parameters(b))
            else:
                statement = plan_or_statement.statement
                for batch_item in plan_or_statement:
                    total_batches += 1
                    total_records += len(batch_item.parameters.get("batch", []))
                    await self._tx.run(
                        batch_item.statement, _adapt_bolt_parameters(batch_item.parameters)
                    )
        except BaseException:
            self._mark_failed()
            raise

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    async def commit(self) -> None:
        """Commits the transaction and closes the driver session."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back instead of committing")
        if not self.is_active:
            raise ValueError("Cannot commit inactive transaction")
        try:
            await self._tx.commit()
            self._state = "COMMITTED"
        except BaseException:
            self._mark_failed()
            raise
        finally:
            await self._close_session()

    async def rollback(self) -> None:
        """Rolls the transaction back and closes the driver session."""
        if self._state not in ("ACTIVE", "FAILED"):
            raise ValueError("Cannot rollback inactive transaction")
        try:
            await self._tx.rollback()
            self._state = "ROLLED_BACK"
        except BaseException:
            self._mark_failed()
            raise
        finally:
            await self._close_session()

    async def savepoint(self, name: str) -> None:
        """Raises because Bolt engines do not support savepoints."""
        raise RuntimeError(
            "Neo4j/Memgraph Bolt transactions do not support savepoints; "
            "use Apache AGE/PostgreSQL for real savepoints"
        )

    async def rollback_to_savepoint(self, name: str) -> None:
        """Raises because Bolt engines do not support savepoints."""
        raise RuntimeError(
            "Neo4j/Memgraph Bolt transactions do not support savepoints; "
            "use Apache AGE/PostgreSQL for real savepoints"
        )

    async def release_savepoint(self, name: str) -> None:
        """Raises because Bolt engines do not support savepoints."""
        raise RuntimeError(
            "Neo4j/Memgraph Bolt transactions do not support savepoints; "
            "use Apache AGE/PostgreSQL for real savepoints"
        )


def _encode_duckdb_literal(v: Any) -> str:
    """Encodes a Python value as a valid SQL literal for DuckDB / DuckPGQ.

    Handles str with proper quoting, int, float (including nan/inf), bool, None,
    date/datetime/time, list/tuple, and dict/map. Raises TypeError for unsupported types.
    """
    if isinstance(v, Json):
        return _encode_duckdb_literal(v.value)
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        if math.isnan(v):
            return "'nan'::DOUBLE"
        if math.isinf(v):
            return "'inf'::DOUBLE" if v > 0 else "'-inf'::DOUBLE"
        return repr(v)
    if isinstance(v, Decimal):
        if v.is_nan():
            return "'nan'::DOUBLE"
        if v.is_infinite():
            return "'inf'::DOUBLE" if v > 0 else "'-inf'::DOUBLE"
        return str(v)
    if isinstance(v, str):
        escaped = v.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(v, datetime.datetime):
        if v.tzinfo is not None:
            return f"TIMESTAMPTZ '{v.isoformat(sep=' ')}'"
        return f"TIMESTAMP '{v.isoformat(sep=' ')}'"
    if isinstance(v, datetime.date):
        return f"DATE '{v.isoformat()}'"
    if isinstance(v, datetime.time):
        return f"TIME '{v.isoformat()}'"
    if isinstance(v, (list, tuple)):
        items = [_encode_duckdb_literal(elem) for elem in v]
        return f"[{', '.join(items)}]"
    if isinstance(v, (dict, Mapping)):
        items = [
            f"{_encode_duckdb_literal(k)}: {_encode_duckdb_literal(val)}" for k, val in v.items()
        ]
        return f"MAP {{{', '.join(items)}}}"
    raise TypeError(
        f"Unsupported parameter type {type(v).__name__} for SQL literal encoding: {v!r}"
    )


def _literal_substitution(sql: str) -> Callable[[re.Match[str]], str]:
    """Returns a ``re.sub`` replacement that emits ``sql`` literally."""

    def replace(_match: re.Match[str]) -> str:
        return sql

    return replace


def _encode_duckdb_in_tuple(values: Sequence[Any]) -> str:
    """Encodes a non-empty Python sequence as a SQL scalar tuple for ``IN``.

    ``x IN [a, b]`` (a DuckDB list) does not propagate a NULL list element
    through ``NOT IN`` like SQL ``IN`` does, so DuckPGQ membership rewrites use
    a scalar tuple (``x IN (a, b)``) instead.
    """
    return "(" + ", ".join(_encode_duckdb_literal(v) for v in values) + ")"


#: Parameters cast to DuckDB ``JSON`` in the statement, e.g. ``$v::JSON``.
_DUCKDB_JSON_PARAMETER_PATTERN = re.compile(
    r"\$([A-Za-z_][A-Za-z0-9_]*)\s*::\s*JSON\b"
    r"|\bCAST\s*\(\s*\$([A-Za-z_][A-Za-z0-9_]*)\s+AS\s+JSON\s*\)",
    re.IGNORECASE,
)


def _duckdb_json_parameter_keys(statement: str) -> set[str]:
    """Returns parameter names that the statement explicitly casts to ``JSON``."""
    keys: set[str] = set()
    for match in _DUCKDB_JSON_PARAMETER_PATTERN.finditer(statement):
        keys.add(match.group(1) or match.group(2))
    return keys


def _duckdb_interval_value(val: Duration) -> Any:
    """Converts a Voyager ``Duration`` to a DuckDB INTERVAL parameter.

    Microsecond-exact durations without calendar months use ``datetime.timedelta``
    so the bridge matches the official client's component normalization.  Other
    values use interval text because DuckDB's INTERVAL keeps calendar months,
    days and microseconds separately.
    """
    if val.months == 0 and val.nanoseconds % 1_000 == 0:
        interval: Any = val.to_timedelta()
    else:
        total_us = val.seconds * 1_000_000 + val.nanoseconds // 1_000
        sign = "-" if total_us < 0 else ""
        abs_us = abs(total_us)
        secs, us = divmod(abs_us, 1_000_000)
        interval = f"{val.months} months {val.days} days {sign}{secs}.{us:06d} seconds"
    try:
        import duckdb

        return duckdb.IntervalValue(interval)
    except ImportError:
        return interval


def _adapt_duckdb_value(val: Any, *, json_target: bool = False) -> Any:
    """Adapts a parameter value for DuckDB binding."""
    if isinstance(val, Duration):
        return _duckdb_interval_value(val)
    if isinstance(val, datetime.time) and not isinstance(val, datetime.datetime):
        offset = val.utcoffset() if val.tzinfo is not None else None
        if offset is not None and offset.days < 0:
            # DuckDB rejects negative-offset datetime.time objects because its
            # binder expects utcoffset().days == 0.  TIMETZ text round-trips.
            return val.isoformat()
        return val
    if json_target and isinstance(val, (dict, list, tuple)):
        return json.dumps(val, default=json_default)
    if isinstance(val, dict):
        return {k: _adapt_duckdb_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_adapt_duckdb_value(v) for v in val]
    return val


def _adapt_duckdb_parameters(statement: str, params: dict[str, Any]) -> dict[str, Any]:
    """Adapts statement parameters, serialising values whose target column is JSON."""
    if not params:
        return params
    json_keys = _duckdb_json_parameter_keys(statement)
    return {
        key: _adapt_duckdb_value(value, json_target=key in json_keys)
        for key, value in params.items()
    }


_DUCKDB_TIMETZ_TYPE_IDS = frozenset({"time with time zone"})
_DUCKDB_INTERVAL_TYPE_IDS = frozenset({"interval", "timedelta"})
_DUCKDB_UUID_TYPE_IDS = frozenset({"uuid"})
_DUCKDB_JSON_TYPE_IDS = frozenset({"json"})
_DUCKDB_ARROW_CANONICAL_TYPE_IDS = (
    _DUCKDB_TIMETZ_TYPE_IDS | _DUCKDB_UUID_TYPE_IDS | _DUCKDB_JSON_TYPE_IDS
)


def _duckdb_type_id(dtype: Any) -> str:
    """Returns the normalized DuckDB type identifier for a type object or string."""
    raw_id = getattr(dtype, "id", None)
    if raw_id is not None:
        # DuckDB's JSON logical type reports id='varchar' while str(dtype) == 'JSON'.
        if str(dtype).strip().upper() == "JSON":
            return "json"
        return str(raw_id).lower()
    text = str(dtype).strip().upper()
    for prefix in (
        "TIME WITH TIME ZONE",
        "UHUGEINT",
        "HUGEINT",
        "INTERVAL",
        "TIMEDELTA",
        "UUID",
        "JSON",
    ):
        if text == prefix or text.startswith((f"{prefix}[", f"{prefix}(")):
            return prefix.lower()
    return text.lower()


def _duckdb_type_children_map(dtype: Any, expected_id: str) -> dict[str, Any]:
    """Returns a ``name -> child_type`` mapping for nested DuckDB types matching ``expected_id``."""
    if _duckdb_type_id(dtype) == expected_id and hasattr(dtype, "children"):
        children: Sequence[tuple[Any, Any]] = dtype.children
        return {str(name): child for name, child in children}
    return {}


def _duckdb_type_has(dtype: Any, target_ids: frozenset[str]) -> bool:
    """Returns whether ``dtype`` is or recursively contains any type in ``target_ids``."""
    tid = _duckdb_type_id(dtype)
    if tid in target_ids:
        return True
    if hasattr(dtype, "id"):
        if tid in ("list", "array", "struct", "map", "union"):
            for name, child in dtype.children:
                if tid == "array" and name == "size":
                    continue
                if tid == "union" and not name:
                    continue
                if _duckdb_type_has(child, target_ids):
                    return True
        return False
    upper = str(dtype).upper()
    return any(target.upper() in upper for target in target_ids)


def _duckdb_description_has_timetz(description: Sequence[Any]) -> bool:
    """Returns whether a DuckDB cursor description contains ``TIMETZ`` anywhere."""
    return any(
        len(item) > 1 and _duckdb_type_has(item[1], _DUCKDB_TIMETZ_TYPE_IDS) for item in description
    )


def _duckdb_description_has_interval(description: Sequence[Any]) -> bool:
    """Returns whether a DuckDB cursor description contains ``INTERVAL`` anywhere."""
    return any(
        len(item) > 1 and _duckdb_type_has(item[1], _DUCKDB_INTERVAL_TYPE_IDS)
        for item in description
    )


def _duckdb_type_has_union_json(dtype: Any) -> bool:
    """Returns whether ``dtype`` contains a ``UNION`` with a ``JSON`` member anywhere."""
    tid = _duckdb_type_id(dtype)
    if not hasattr(dtype, "id"):
        upper = str(dtype).upper()
        return "UNION" in upper and "JSON" in upper
    if tid == "union":
        return any(
            name and _duckdb_type_has(child, _DUCKDB_JSON_TYPE_IDS)
            for name, child in dtype.children
        )
    if tid in ("list", "array", "struct", "map"):
        return any(
            not (tid == "array" and name == "size") and _duckdb_type_has_union_json(child)
            for name, child in dtype.children
        )
    return False


def _duckdb_description_has_union_json(description: Sequence[Any]) -> bool:
    """Returns whether a DuckDB cursor description contains a ``UNION`` with ``JSON``."""
    return any(len(item) > 1 and _duckdb_type_has_union_json(item[1]) for item in description)


def _duckdb_quote_identifier(name: str) -> str:
    """Quotes a DuckDB identifier, doubling embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def _duckdb_timetz_cast_sql(dtype: Any) -> str:
    """Builds a DuckDB SQL type replacing nested ``TIME WITH TIME ZONE`` with ``VARCHAR``."""
    tid = _duckdb_type_id(dtype)
    if tid == "time with time zone":
        return "VARCHAR"
    if hasattr(dtype, "id"):
        if tid == "list":
            return f"{_duckdb_timetz_cast_sql(dtype.children[0][1])}[]"
        if tid == "array":
            children = dict(dtype.children)
            return f"{_duckdb_timetz_cast_sql(children['child'])}[{children['size']}]"
        if tid == "struct":
            fields = ", ".join(
                f"{_duckdb_quote_identifier(str(name))} {_duckdb_timetz_cast_sql(child)}"
                for name, child in dtype.children
            )
            return f"STRUCT({fields})"
        if tid == "map":
            children = dict(dtype.children)
            return (
                f"MAP({_duckdb_timetz_cast_sql(children['key'])}, "
                f"{_duckdb_timetz_cast_sql(children['value'])})"
            )
        if tid == "union":
            fields = ", ".join(
                f"{_duckdb_quote_identifier(str(name))} {_duckdb_timetz_cast_sql(child)}"
                for name, child in dtype.children
                if name
            )
            return f"UNION({fields})"
        return str(dtype)
    upper = str(dtype).upper()
    depth = upper.count("[")
    return "VARCHAR" + "[]" * depth


def _duckdb_timetz_projection(description: Sequence[Any]) -> str | None:
    """Builds a projection that casts top-level or nested ``TIMETZ`` values to ISO text.

    DuckDB's Arrow export maps ``TIME WITH TIME ZONE`` to a plain ``time64`` and
    loses the offset, while its DB-API rows fold calendar ``INTERVAL`` months
    into 30-day ``timedelta`` values. Projecting only columns containing ``TIMETZ``
    to ``VARCHAR`` (preserving outer ``LIST``/``ARRAY``/``STRUCT``/``MAP``/``UNION``
    structure) keeps Arrow's ``month_day_nano_interval`` precision everywhere.
    """
    projections: list[str] = []
    has_timetz = False
    for idx, item in enumerate(description):
        name = str(item[0])
        dtype = item[1] if len(item) > 1 else ""
        alias = _duckdb_quote_identifier(name)
        if _duckdb_type_has(dtype, _DUCKDB_TIMETZ_TYPE_IDS):
            has_timetz = True
            target_type = _duckdb_timetz_cast_sql(dtype)
            projections.append(f"CAST(#{idx + 1} AS {target_type}) AS {alias}")
        else:
            projections.append(f"#{idx + 1} AS {alias}")
    if not has_timetz:
        return None
    return ", ".join(projections)


def _duckdb_relation_description(relation: Any) -> tuple[tuple[str, Any], ...]:
    """Builds a DB-API-like description preserving DuckDB type objects from a relation."""
    return tuple(
        (str(name), type_obj)
        for name, type_obj in zip(relation.columns, relation.types, strict=True)
    )


def _duckdb_complete_pending(connection: Any) -> None:
    """Clears an unfinished DB-API result so a relation can start.

    ``DuckDBPyConnection.sql`` refuses to run while a prior ``execute()`` result
    is still open (for example after a bare ``fetchone()``), whereas ``execute()``
    implicitly replaces it. ``complete()`` gives the relation path the same
    behaviour without executing the statement twice.
    """
    complete = getattr(connection, "complete", None)
    if not callable(complete):
        return
    try:
        complete()
    except Exception:
        pass


_DUCKDB_TIMETZ_TEXT_PATTERN = re.compile(
    r"^(?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})(?:\.(?P<fraction>\d{1,6}))?"
    r"(?P<tz>Z|z|(?P<sign>[+-])(?P<hours>\d{2})(?::?(?P<minutes>\d{2})(?::?(?P<seconds>\d{2}))?)?)$"
)


def _normalize_timetz_offset(match: re.Match[str], original: str) -> str:
    """Validates and normalizes a matched ``TIMETZ`` UTC offset to ``±HH:MM[:SS]``."""
    tz_raw = match.group("tz")
    if tz_raw in ("Z", "z"):
        return "+00:00"
    sign = match.group("sign")
    off_h = int(match.group("hours"))
    off_m = int(match.group("minutes") or "0")
    off_s = int(match.group("seconds") or "0")
    if not (0 <= off_h <= 23 and 0 <= off_m <= 59 and 0 <= off_s <= 59):
        raise ValueError(f"DuckDB TIMETZ UTC offset out of range: {original!r}")
    if off_h == 0 and off_m == 0 and off_s == 0:
        return "+00:00"
    if off_s == 0:
        return f"{sign}{off_h:02d}:{off_m:02d}"
    return f"{sign}{off_h:02d}:{off_m:02d}:{off_s:02d}"


def _duckdb_timetz_text_iso(value: Any) -> Any:
    """Normalizes DuckDB's ``TIMETZ`` text to ISO-8601 ``HH:MM:SS[.ffffff]±HH:MM[:SS]`` form.

    Accepts every valid DuckDB ``TIMETZ`` value, including ``24:00:00`` (which
    Python's ``datetime.time`` cannot hold), fractional seconds up to 6 digits,
    ``Z``, and ``±HH[:MM[:SS]]`` / ``±HHMM[SS]`` offsets. Recurses into
    lists/tuples, passes through ``None``, and raises on non-string values or
    text that is not a valid DuckDB ``TIMETZ``.
    """
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [_duckdb_timetz_text_iso(item) for item in value]
    if not isinstance(value, str):
        raise TypeError(
            f"Expected DuckDB TIMETZ text string, got {type(value).__name__}: {value!r}"
        )
    match = _DUCKDB_TIMETZ_TEXT_PATTERN.match(value)
    if match is None:
        raise ValueError(f"Unrecognized DuckDB TIMETZ text format: {value!r}")
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    second = int(match.group("second"))
    fraction = match.group("fraction")
    offset = _normalize_timetz_offset(match, value)
    if hour == 24:
        # DuckDB allows 24:00:00 (with optional zero fraction) across the full
        # TIMETZ offset range, which Python's datetime.time cannot represent.
        if minute != 0 or second != 0 or (fraction is not None and int(fraction) != 0):
            raise ValueError(f"DuckDB TIMETZ value out of range: {value!r}")
        return f"24:00:00{offset}"
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        raise ValueError(f"DuckDB TIMETZ value out of range: {value!r}")
    frac_us = int(fraction.ljust(6, "0")) if fraction is not None else 0
    time_part = f"{hour:02d}:{minute:02d}:{second:02d}"
    if frac_us:
        time_part += f".{frac_us:06d}"
    return f"{time_part}{offset}"


def _parse_duckdb_timetz_python(value: Any) -> Any:
    """Decodes DuckDB ``TIMETZ`` to ``datetime.time``, or ISO ``str`` when unrepresentable."""
    if isinstance(value, datetime.time):
        return value
    iso = _duckdb_timetz_text_iso(value)
    if iso.startswith("24:"):
        # End of day has no datetime.time equivalent. Python 3.14's fromisoformat
        # silently maps "24:00:00" to midnight, so keep the ISO text on every version.
        return iso
    try:
        return datetime.time.fromisoformat(iso)
    except ValueError:
        return iso


def _duckdb_time_iso(value: Any) -> Any:
    """Converts a ``datetime.time`` (or nested container of them) to ISO text."""
    if isinstance(value, str):
        return _duckdb_timetz_text_iso(value)
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [_duckdb_time_iso(item) for item in value]
    if isinstance(value, dict):
        return {k: _duckdb_time_iso(v) for k, v in value.items()}
    return value.isoformat()


def _duckdb_uuid_bytes(value: Any) -> Any:
    """Converts a UUID or UUID string to its 16-byte representation."""
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        return [_duckdb_uuid_bytes(item) for item in value]
    if isinstance(value, bytes):
        return value
    if isinstance(value, uuid.UUID):
        return value.bytes
    return uuid.UUID(str(value)).bytes


def _duckdb_arrow_field_metadata(
    dtype: Any,
    arrow_type: Any,
    existing: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes] | None:
    """Returns Voyager/Arrow field metadata for a DuckDB column or nested child field."""
    import pyarrow as pa

    metadata = dict(existing) if existing else {}
    tid = _duckdb_type_id(dtype) if dtype else ""
    if tid == "uuid":
        metadata[b"voyager.type"] = b"uuid"
        if pa.types.is_fixed_size_binary(arrow_type):
            # Same extension tag as the native PostgreSQL backend's UUID columns.
            metadata[b"ARROW:extension:name"] = b"arrow.uuid"
    elif tid == "json":
        metadata[b"voyager.encoding"] = b"json"
        metadata[b"voyager.type"] = b"json"
    elif tid == "time with time zone":
        metadata[b"voyager.type"] = b"time_tz"
    elif tid == "hugeint":
        metadata[b"voyager.type"] = b"hugeint"
    elif (
        _is_month_day_nano_interval(arrow_type) or tid in _DUCKDB_INTERVAL_TYPE_IDS
    ) and pa.types.is_struct(arrow_type):
        metadata[b"voyager.type"] = b"duration"
    return metadata or None


def _duckdb_canonicalized_child_field(orig_field: Any, arrow_type: Any, dtype: Any) -> Any:
    """Rebuilds a nested Arrow child ``pa.Field`` with Voyager metadata for ``dtype``."""
    import pyarrow as pa

    metadata = _duckdb_arrow_field_metadata(dtype, arrow_type, orig_field.metadata)
    return pa.field(orig_field.name, arrow_type, nullable=orig_field.nullable, metadata=metadata)


def _duckdb_canonicalize_arrow_array(arr: Any, dtype: Any) -> Any:
    """Recursively converts ``TIMETZ``, ``UUID``, ``JSON``, and ``month_day_nano_interval`` in Arrow."""
    import pyarrow as pa

    if _is_month_day_nano_interval(arr.type):
        return _duckdb_mdn_column_to_arrow(arr)
    tid = _duckdb_type_id(dtype) if dtype else ""
    if tid == "time with time zone":
        return pa.array([_duckdb_timetz_text_iso(v) for v in arr.to_pylist()], type=pa.string())
    if tid == "uuid":
        if pa.types.is_fixed_size_binary(arr.type) and arr.type.byte_width == 16:
            return arr
        return pa.array([_duckdb_uuid_bytes(v) for v in arr.to_pylist()], type=pa.binary(16))
    if tid == "json":
        if arr.type == pa.string():
            return arr
        return pa.array(
            [
                v if v is None or isinstance(v, str) else json.dumps(v, default=json_default)
                for v in arr.to_pylist()
            ],
            type=pa.string(),
        )
    if pa.types.is_list(arr.type):
        child_dtype = _duckdb_list_child_dtype(dtype, tid)
        new_values = _duckdb_canonicalize_arrow_array(arr.values, child_dtype)
        value_field = _duckdb_canonicalized_child_field(
            arr.type.value_field, new_values.type, child_dtype
        )
        return pa.ListArray.from_arrays(
            arr.offsets, new_values, type=pa.list_(value_field), mask=arr.is_null()
        )
    if pa.types.is_large_list(arr.type):
        child_dtype = _duckdb_list_child_dtype(dtype, tid)
        new_values = _duckdb_canonicalize_arrow_array(arr.values, child_dtype)
        value_field = _duckdb_canonicalized_child_field(
            arr.type.value_field, new_values.type, child_dtype
        )
        return pa.LargeListArray.from_arrays(
            arr.offsets, new_values, type=pa.large_list(value_field), mask=arr.is_null()
        )
    if pa.types.is_fixed_size_list(arr.type):
        child_dtype = _duckdb_array_child_dtype(dtype, tid)
        new_values = _duckdb_canonicalize_arrow_array(arr.values, child_dtype)
        value_field = _duckdb_canonicalized_child_field(
            arr.type.value_field, new_values.type, child_dtype
        )
        return pa.FixedSizeListArray.from_arrays(
            new_values,
            type=pa.list_(value_field, arr.type.list_size),
            mask=arr.is_null(),
        )
    if pa.types.is_struct(arr.type):
        dtype_fields = _duckdb_type_children_map(dtype, "struct")
        struct_fields = []
        new_fields = []
        for i, orig_field in enumerate(arr.type):
            child_dtype = dtype_fields.get(orig_field.name)
            child_arr = _duckdb_canonicalize_arrow_array(arr.field(i), child_dtype)
            new_fields.append(child_arr)
            struct_fields.append(
                _duckdb_canonicalized_child_field(orig_field, child_arr.type, child_dtype)
            )
        return pa.StructArray.from_arrays(new_fields, fields=struct_fields, mask=arr.is_null())
    if pa.types.is_map(arr.type):
        dtype_children = _duckdb_type_children_map(dtype, "map")
        key_dtype = dtype_children.get("key")
        val_dtype = dtype_children.get("value")
        new_keys = _duckdb_canonicalize_arrow_array(arr.keys, key_dtype)
        new_items = _duckdb_canonicalize_arrow_array(arr.items, val_dtype)
        key_field = _duckdb_canonicalized_child_field(arr.type.key_field, new_keys.type, key_dtype)
        item_field = _duckdb_canonicalized_child_field(
            arr.type.item_field, new_items.type, val_dtype
        )
        return pa.MapArray.from_arrays(
            arr.offsets,
            new_keys,
            new_items,
            type=pa.map_(key_field, item_field),
            mask=arr.is_null(),
        )
    if pa.types.is_union(arr.type):
        dtype_members = _duckdb_type_children_map(dtype, "union")
        union_fields = []
        new_children = []
        for i, orig_field in enumerate(arr.type):
            child_dtype = dtype_members.get(orig_field.name)
            child_arr = _duckdb_canonicalize_arrow_array(arr.field(i), child_dtype)
            new_children.append(child_arr)
            union_fields.append(
                _duckdb_canonicalized_child_field(orig_field, child_arr.type, child_dtype)
            )
        names = [f.name for f in union_fields]
        codes = list(arr.type.type_codes)
        if arr.type.mode == "dense":
            union_arr = pa.UnionArray.from_dense(
                arr.type_codes, arr.offsets, new_children, field_names=names, type_codes=codes
            )
            target_type = pa.dense_union(union_fields, type_codes=codes)
        else:
            union_arr = pa.UnionArray.from_sparse(
                arr.type_codes, new_children, field_names=names, type_codes=codes
            )
            target_type = pa.sparse_union(union_fields, type_codes=codes)
        return union_arr.cast(target_type)
    return arr


def _duckdb_empty_arrow_array(arrow_type: Any) -> Any:
    """Builds an empty PyArrow array for ``arrow_type``, including nested ``UnionType``."""
    import pyarrow as pa

    if pa.types.is_union(arrow_type):
        children = [_duckdb_empty_arrow_array(f.type) for f in arrow_type]
        names = [f.name for f in arrow_type]
        codes = list(arrow_type.type_codes)
        if arrow_type.mode == "dense":
            return pa.UnionArray.from_dense(
                pa.array([], type=pa.int8()),
                pa.array([], type=pa.int32()),
                children,
                field_names=names,
                type_codes=codes,
            )
        return pa.UnionArray.from_sparse(
            pa.array([], type=pa.int8()),
            children,
            field_names=names,
            type_codes=codes,
        )
    if pa.types.is_list(arrow_type):
        return pa.ListArray.from_arrays(
            pa.array([0], type=pa.int32()),
            _duckdb_empty_arrow_array(arrow_type.value_type),
            type=arrow_type,
        )
    if pa.types.is_large_list(arrow_type):
        return pa.LargeListArray.from_arrays(
            pa.array([0], type=pa.int64()),
            _duckdb_empty_arrow_array(arrow_type.value_type),
            type=arrow_type,
        )
    if pa.types.is_fixed_size_list(arrow_type):
        return pa.FixedSizeListArray.from_arrays(
            _duckdb_empty_arrow_array(arrow_type.value_type),
            type=arrow_type,
        )
    if pa.types.is_struct(arrow_type):
        return pa.StructArray.from_arrays(
            [_duckdb_empty_arrow_array(f.type) for f in arrow_type],
            fields=list(arrow_type),
        )
    if pa.types.is_map(arrow_type):
        return pa.MapArray.from_arrays(
            pa.array([0], type=pa.int32()),
            _duckdb_empty_arrow_array(arrow_type.key_type),
            _duckdb_empty_arrow_array(arrow_type.item_type),
            type=arrow_type,
        )
    return pa.array([], type=arrow_type)


def _duckdb_canonicalize_arrow_column(column: Any, dtype: Any) -> Any:
    """Canonicalizes an Arrow column (or ChunkedArray) preserving its schema even on 0 rows."""
    import pyarrow as pa

    if isinstance(column, pa.ChunkedArray):
        if column.num_chunks == 0:
            converted_empty = _duckdb_canonicalize_arrow_array(
                _duckdb_empty_arrow_array(column.type), dtype
            )
            return pa.chunked_array([], type=converted_empty.type)
        return _duckdb_canonicalize_arrow_array(column.combine_chunks(), dtype)
    return _duckdb_canonicalize_arrow_array(column, dtype)


def _duckdb_arrow_table_to_canonical(arrow_table: Any, description: Sequence[Any]) -> Any:
    """Rewrites DuckDB Arrow columns to Voyager's documented export representations.

    ``UUID`` becomes ``fixed_size_binary(16)`` (Polars ``Binary``), ``JSON`` stays
    UTF-8 text tagged ``voyager.encoding=json``, ``TIMETZ`` stays ISO text tagged
    ``voyager.type=time_tz``, ``HUGEINT`` stays ``decimal128(38, 0)`` tagged
    ``voyager.type=hugeint`` (Arrow has no int128), and calendar intervals (top-level
    or nested) become ``month_day_nano_interval``-free duration or struct columns.
    """
    import pyarrow as pa

    arrays = []
    fields = []
    for idx, field in enumerate(arrow_table.schema):
        dtype: Any = (
            description[idx][1] if idx < len(description) and len(description[idx]) > 1 else ""
        )
        description_type = str(dtype).upper()
        tid = _duckdb_type_id(dtype) if dtype else ""
        column = arrow_table.column(idx)
        metadata = dict(field.metadata) if field.metadata else {}
        if tid == "hugeint":
            array = column
            metadata[b"voyager.type"] = b"hugeint"
        elif _is_month_day_nano_interval(field.type):
            array = _duckdb_mdn_column_to_arrow(column)
            if pa.types.is_struct(array.type):
                metadata[b"voyager.type"] = b"duration"
        elif _duckdb_type_has(
            dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS
        ) or _arrow_has_month_day_nano_interval(field.type):
            array = _duckdb_canonicalize_arrow_column(column, dtype)
            if tid in ("uuid", "json", "time with time zone"):
                metadata = _duckdb_arrow_field_metadata(dtype, array.type, metadata) or {}
            elif description_type.startswith("UUID["):
                metadata[b"voyager.type"] = b"uuid"
            elif description_type.startswith("TIME WITH TIME ZONE"):
                metadata[b"voyager.type"] = b"time_tz"
        else:
            arrays.append(column)
            fields.append(field)
            continue
        fields.append(
            pa.field(field.name, array.type, nullable=field.nullable, metadata=metadata or None)
        )
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _duckdb_python_values_to_arrow(
    name: str, values: list[Any], dtype: Any, *, nullable: bool = True
) -> tuple[Any, Any]:
    """Builds an Arrow ``(field, array)`` from DuckDB DB-API Python values recursively."""
    import pyarrow as pa

    tid = _duckdb_type_id(dtype) if dtype else ""
    if tid == "time with time zone":
        arr = pa.array([_duckdb_time_iso(value) for value in values], type=pa.string())
    elif tid == "json":
        arr = pa.array(
            [
                value
                if value is None or isinstance(value, str)
                else json.dumps(value, default=json_default)
                for value in values
            ],
            type=pa.string(),
        )
    elif tid == "uuid":
        arr = pa.array([_duckdb_uuid_bytes(value) for value in values], type=pa.binary(16))
    elif tid == "hugeint":
        arr = pa.array(
            [None if value is None else Decimal(int(value)) for value in values],
            type=pa.decimal128(38, 0),
        )
    elif tid in ("interval", "timedelta"):
        arr = pa.array(values, type=pa.duration("us"))
    elif tid == "list" and _duckdb_type_has(dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS):
        child_dtype = _duckdb_list_child_dtype(dtype, tid)
        offsets = [0]
        flat: list[Any] = []
        null_flags: list[bool] = []
        for item in values:
            if item is None:
                null_flags.append(True)
            else:
                null_flags.append(False)
                flat.extend(item)
            offsets.append(len(flat))
        child_name = (
            str(dtype.children[0][0]) if hasattr(dtype, "children") and dtype.children else "item"
        )
        child_field, child_arr = _duckdb_python_values_to_arrow(child_name, flat, child_dtype)
        arr = pa.ListArray.from_arrays(
            pa.array(offsets, type=pa.int32()),
            child_arr,
            type=pa.list_(child_field),
            mask=pa.array(null_flags, type=pa.bool_()),
        )
    elif tid == "array" and _duckdb_type_has(dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS):
        children = _duckdb_type_children_map(dtype, "array")
        child_dtype = _duckdb_array_child_dtype(dtype, tid)
        size = int(children["size"]) if "size" in children else 0
        flat = []
        null_flags = []
        for item in values:
            if item is None:
                null_flags.append(True)
                flat.extend([None] * size)
            else:
                null_flags.append(False)
                flat.extend(item)
        child_field, child_arr = _duckdb_python_values_to_arrow("item", flat, child_dtype)
        arr = pa.FixedSizeListArray.from_arrays(
            child_arr,
            type=pa.list_(child_field, size),
            mask=pa.array(null_flags, type=pa.bool_()),
        )
    elif tid == "struct" and _duckdb_type_has(dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS):
        dtype_fields = _duckdb_type_children_map(dtype, "struct")
        struct_fields = []
        field_arrays = []
        null_flags = [item is None for item in values]
        for fname, child_dtype in dtype_fields.items():
            sub_vals = [None if item is None else item.get(fname) for item in values]
            cfield, carr = _duckdb_python_values_to_arrow(fname, sub_vals, child_dtype)
            struct_fields.append(cfield)
            field_arrays.append(carr)
        arr = pa.StructArray.from_arrays(
            field_arrays, fields=struct_fields, mask=pa.array(null_flags, type=pa.bool_())
        )
    elif tid == "map" and _duckdb_type_has(dtype, _DUCKDB_ARROW_CANONICAL_TYPE_IDS):
        children = _duckdb_type_children_map(dtype, "map")
        key_dtype = children.get("key", "")
        val_dtype = children.get("value", "")
        offsets = [0]
        flat_keys: list[Any] = []
        flat_vals: list[Any] = []
        null_flags = []
        for item in values:
            if item is None:
                null_flags.append(True)
            else:
                null_flags.append(False)
                for k, v in item.items():
                    flat_keys.append(k)
                    flat_vals.append(v)
            offsets.append(len(flat_keys))
        key_field, key_arr = _duckdb_python_values_to_arrow(
            "key", flat_keys, key_dtype, nullable=False
        )
        val_field, val_arr = _duckdb_python_values_to_arrow("value", flat_vals, val_dtype)
        arr = pa.MapArray.from_arrays(
            pa.array(offsets, type=pa.int32()),
            key_arr,
            val_arr,
            type=pa.map_(key_field, val_field),
            mask=pa.array(null_flags, type=pa.bool_()),
        )
    else:
        arr = pa.array(values)

    meta = _duckdb_arrow_field_metadata(dtype, arr.type)
    description_type = str(dtype).upper() if dtype else ""
    if meta is None:
        meta_dict: dict[bytes, bytes] = {}
    else:
        meta_dict = meta
    if description_type.startswith("UUID["):
        meta_dict[b"voyager.type"] = b"uuid"
    elif description_type.startswith("TIME WITH TIME ZONE"):
        meta_dict[b"voyager.type"] = b"time_tz"
    return pa.field(name, arr.type, nullable=nullable, metadata=meta_dict or None), arr


def _duckdb_rows_to_arrow(description: Sequence[Any], rows: Sequence[Sequence[Any]]) -> Any:
    """Builds an Arrow table from DB-API rows when DuckDB's Arrow copy loses TIMETZ offsets."""
    import pyarrow as pa

    arrays = []
    fields = []
    for idx, item in enumerate(description):
        name = str(item[0])
        dtype: Any = item[1] if len(item) > 1 else ""
        values = [row[idx] for row in rows]
        field, array = _duckdb_python_values_to_arrow(name, values, dtype)
        fields.append(field)
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


class DuckDbBridge:
    """Synchronous DuckDB driver bridge with zero-copy Polars / Arrow output."""

    def __init__(self, connection: Any, *, install_duckpgq: bool = False) -> None:
        """Initializes DuckDbBridge.

        Args:
            connection: DuckDB connection object.
            install_duckpgq: Install the DuckPGQ extension when it is not already
                available before loading it. Off by default because installing
                downloads the extension from the DuckDB extension repository.
        """
        self.con = connection
        self._install_duckpgq = install_duckpgq
        self._duckpgq_loaded = False

    @staticmethod
    def _arrow_table(rel: Any) -> Any | None:
        """Returns a DuckDB relation's Arrow table."""
        method = getattr(rel, "to_arrow_table", None)
        if not callable(method):
            return None
        with warnings.catch_warnings():
            # DuckDB warns that a DB-API result copies rows into Arrow; the
            # result is already materialized here, so this is expected.
            warnings.filterwarnings("ignore", message="Arrow output on a DB-API result.*")
            return method()

    def _duckpgq_is_loaded(self) -> bool:
        """Returns whether DuckPGQ is already loaded on this connection."""
        try:
            row = self.con.execute(
                "SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'duckpgq'"
            ).fetchone()
        except Exception:
            return False
        return bool(row and row[0])

    def set_install_duckpgq(self, install: bool) -> None:
        """Sets whether a missing DuckPGQ extension may be installed when it is first needed.

        Args:
            install: When ``True``, ``INSTALL duckpgq FROM community;`` (which downloads
                the extension) is attempted if ``LOAD duckpgq;`` fails.
        """
        self._install_duckpgq = install

    def _enable_duckpgq_grammar(self) -> None:
        """Opts this connection into DuckPGQ's SQL/PGQ grammar on DuckDB 2.0+."""
        extensions: list[str] = []
        try:
            row = self.con.execute("SELECT current_setting('active_grammar_extensions')").fetchone()
        except Exception:
            row = None
        current = row[0] if row else None
        if isinstance(current, (list, tuple)):
            extensions = [str(item) for item in current]
            if "duckpgq" in extensions:
                return
        extensions.append("duckpgq")
        encoded = ", ".join(_encode_duckdb_literal(ext) for ext in extensions)
        self.con.execute(f"SET active_grammar_extensions = [{encoded}];")

    def ensure_duckpgq(self, install: bool | None = None) -> None:
        """Loads DuckPGQ and its grammar on this connection once.

        Loading is idempotent: a connection the caller already prepared with
        ``LOAD duckpgq;`` is detected and left loaded, and later calls are no-ops.
        On DuckDB 2.0+, SQL/PGQ parsing also requires the per-connection
        ``SET active_grammar_extensions = ['duckpgq'];`` opt-in, which Voyager
        runs after loading the extension (or after detecting it already loaded).

        Args:
            install: Install the extension first when it is missing. Defaults to
                the ``install_duckpgq`` value passed to the constructor. With
                ``False``, a missing extension raises an actionable error
                instead of downloading anything.

        Raises:
            RuntimeError: If the extension cannot be loaded, naming DuckPGQ and
                the ``INSTALL duckpgq FROM community;`` command.
        """
        if self._duckpgq_loaded:
            return
        if self._duckpgq_is_loaded():
            self._enable_duckpgq_grammar()
            self._duckpgq_loaded = True
            return
        should_install = self._install_duckpgq if install is None else install
        try:
            self.con.execute("LOAD duckpgq;")
        except Exception as load_error:
            if not should_install:
                raise RuntimeError(_duckpgq_error_message(load_error)) from load_error
            try:
                self.con.execute("INSTALL duckpgq FROM community;")
                self.con.execute("LOAD duckpgq;")
            except Exception as install_error:
                raise RuntimeError(_duckpgq_error_message(install_error)) from install_error
        self._enable_duckpgq_grammar()
        self._duckpgq_loaded = True

    def _ensure_duckpgq_for_statement(self, statement: str) -> None:
        """Loads DuckPGQ before a statement that needs it."""
        if not self._duckpgq_loaded and _DUCKPGQ_STATEMENT_PATTERN.search(statement):
            self.ensure_duckpgq()

    def _format_pgq_statement(
        self, statement: str, parameters: dict[str, Any]
    ) -> tuple[str, dict[str, Any]]:
        """Interpolates parameters for DuckPGQ GRAPH_TABLE queries where native parameter binding is unsupported."""
        validate_pagination_parameters(statement, parameters)
        if "GRAPH_TABLE" in statement and parameters:
            # DuckPGQ's GRAPH_TABLE grammar has no ANY/ALL keyword, so the
            # SQL/PGQ `x = ANY($n)` / `x <> ALL($n)` forms for a list parameter
            # are rewritten to a scalar `IN (...)` tuple. The list is known
            # here, so an empty list becomes FALSE/TRUE like the emitter emits
            # for an empty literal list.
            stmt = statement
            for key, value in parameters.items():
                if not isinstance(value, (list, tuple)):
                    continue
                if value:
                    positive = f" IN {_encode_duckdb_in_tuple(value)}"
                    negative = f" NOT IN {_encode_duckdb_in_tuple(value)}"
                else:
                    # `IN ()` is invalid; FALSE/TRUE is the emitter's empty-list
                    # rendering, kept as a valid comparison of the left operand.
                    positive = " IN (NULL) AND FALSE"
                    negative = " IN (NULL) OR TRUE"
                stmt = re.sub(
                    rf"(?<![<>=!])\s*=\s+ANY\(\s*\${re.escape(key)}\s*\)",
                    _literal_substitution(positive),
                    stmt,
                )
                stmt = re.sub(
                    rf"(?<![<>=!])\s*<>\s+ALL\(\s*\${re.escape(key)}\s*\)",
                    _literal_substitution(negative),
                    stmt,
                )

            regex = re.compile(r"\$([a-zA-Z0-9_]+)\b")

            def repl(m: re.Match[str]) -> str:
                key = m.group(1)
                if key in parameters:
                    return _encode_duckdb_literal(parameters[key])
                return m.group(0)

            stmt = regex.sub(repl, stmt)
            return stmt, {}
        return statement, parameters

    def _execute_sql_relation(
        self, stmt: str, params: dict[str, Any]
    ) -> tuple[Any, Sequence[Any]] | None:
        """Runs ``stmt`` once via ``con.sql`` when supported, returning ``(relation, description)``."""
        sql_fn = getattr(self.con, "sql", None)
        if not callable(sql_fn):
            return None
        _duckdb_complete_pending(self.con)
        relation = sql_fn(stmt, params=params or None)
        if relation is None:
            return None, ()
        return relation, _duckdb_relation_description(relation)

    def _relation_arrow_table(self, relation: Any, description: Sequence[Any]) -> Any | None:
        """Materializes ``relation`` as Arrow once, projecting any ``TIMETZ`` values to text."""
        projection = _duckdb_timetz_projection(description)
        if projection is not None:
            return self._arrow_table(relation.project(projection))
        return self._arrow_table(relation)

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes query on DuckDB.

        Preserves exact calendar ``months``, ``days``, ``seconds``, and
        ``nanoseconds`` on ``INTERVAL`` values (top-level or nested in
        ``LIST``/``ARRAY``/``STRUCT``/``MAP``/``UNION``) and retains ``TIMETZ``
        UTC offsets by projecting ``TIMETZ`` to ISO text before reading Arrow
        ``month_day_nano_interval``. Queries without any ``INTERVAL`` value
        decode via ``fetchall()``. ``TIMETZ`` values representable by Python's
        ``datetime.time`` decode as timezone-aware ``datetime.time`` objects,
        while valid database values Python cannot hold (such as ``24:00:00``)
        remain normalized ISO text strings (e.g. ``'24:00:00+00:00'``).

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            List of record dictionaries.
        """
        self._ensure_duckpgq_for_statement(statement)
        stmt, params = self._format_pgq_statement(statement, parameters or {})
        params = _adapt_duckdb_parameters(stmt, params)
        sql_res = self._execute_sql_relation(stmt, params)
        if sql_res is not None:
            relation, description = sql_res
            if relation is None or not description:
                return BridgeResult([], columns=())
            if _duckdb_description_has_interval(description) or _duckdb_description_has_union_json(
                description
            ):
                arrow_table = self._relation_arrow_table(relation, description)
                if arrow_table is not None:
                    return _records_from_duckdb_arrow(description, arrow_table)
            return _records_from_duckdb_rows(description, relation.fetchall())

        rel = self.con.execute(stmt, params)
        if not rel.description:
            return BridgeResult([], columns=())
        if (
            _duckdb_description_has_interval(rel.description)
            or _duckdb_description_has_union_json(rel.description)
        ) and not _duckdb_description_has_timetz(rel.description):
            arrow_table = self._arrow_table(rel)
            if arrow_table is not None:
                return _records_from_duckdb_arrow(rel.description, arrow_table)
        return _records_from_duckdb_rows(rel.description, rel.fetchall())

    def execute_to_arrow(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
        """Executes a query and returns an Arrow table with Voyager's export types.

        ``UUID`` columns use ``fixed_size_binary(16)`` (``arrow.uuid`` storage),
        ``JSON`` columns are UTF-8 JSON text tagged ``voyager.encoding=json``,
        ``TIMETZ`` columns (and nested ``TIMETZ`` values) are ISO text tagged
        ``voyager.type=time_tz`` because Arrow has no timezone-aware time type,
        and ``HUGEINT`` columns stay ``decimal128(38, 0)`` tagged
        ``voyager.type=hugeint`` because Arrow has no portable int128 type.

        Statements run once through ``con.sql`` (including single queries, DML
        ``RETURNING``, and multi-statement SQL), projecting any top-level or
        nested ``TIMETZ`` to ISO text before one Arrow materialization so
        calendar ``INTERVAL`` months are preserved everywhere. Connections without
        ``sql()`` fall back to the single-execution DB-API path.
        """
        import pyarrow as pa

        self._ensure_duckpgq_for_statement(statement)
        stmt, params = self._format_pgq_statement(statement, parameters or {})
        params = _adapt_duckdb_parameters(stmt, params)
        sql_res = self._execute_sql_relation(stmt, params)
        if sql_res is not None:
            relation, description = sql_res
            if relation is None or not description:
                return pa.Table.from_pylist([])
            arrow_table = self._relation_arrow_table(relation, description)
            if arrow_table is None:
                return _duckdb_rows_to_arrow(description, relation.fetchall())
            return _duckdb_arrow_table_to_canonical(arrow_table, description)

        rel = self.con.execute(stmt, params)
        if not rel.description:
            return pa.Table.from_pylist([])
        description = tuple(rel.description)
        if _duckdb_description_has_timetz(description):
            return _duckdb_rows_to_arrow(description, rel.fetchall())
        arrow_table = self._arrow_table(rel)
        if arrow_table is None:
            return _duckdb_rows_to_arrow(description, rel.fetchall())
        return _duckdb_arrow_table_to_canonical(arrow_table, description)

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes query and streams to Polars DataFrame from the Arrow result.

        Statements run once through ``con.sql``; top-level or nested ``TIMETZ``
        values are projected to ISO text on that relation so calendar ``INTERVAL``
        months still stream from Arrow instead of DB-API rows.

        ``UUID`` columns are ``Binary`` (16 bytes), ``TIMETZ`` and ``JSON``
        columns are UTF-8 text (Polars strips Arrow field metadata), and
        ``HUGEINT`` columns are ``pl.Decimal`` because Polars' ``Int128`` cannot
        be exported back to Arrow.

        DuckDB ``UNION`` columns, top-level or nested, are exported as a
        ``Struct`` with one field per union member plus a ``tag`` string field
        naming the active member (the tag field is suffixed only if a member is
        already called ``tag``). Polars has no union data type and cannot import
        an Arrow union, so this conversion happens before ``pl.from_arrow()``.
        A struct preserves each member's Arrow type and any nested structure;
        tagged JSON text would lose both. A ``UserWarning`` names each converted
        column.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame.
        """
        self._ensure_duckpgq_for_statement(statement)
        stmt, params = self._format_pgq_statement(statement, parameters or {})
        params = _adapt_duckdb_parameters(stmt, params)
        sql_res = self._execute_sql_relation(stmt, params)
        if sql_res is not None:
            relation, description = sql_res
            if relation is None or not description:
                return pl.DataFrame()
            arrow_table = self._relation_arrow_table(relation, description)
            if arrow_table is None:
                return _duckdb_arrow_table_to_polars(
                    _duckdb_rows_to_arrow(description, relation.fetchall())
                )
            return _duckdb_arrow_table_to_polars(
                _duckdb_arrow_table_to_canonical(arrow_table, description)
            )

        rel = self.con.execute(stmt, params)
        if not rel.description:
            return pl.DataFrame()
        description = tuple(rel.description)
        if _duckdb_description_has_timetz(description):
            return _duckdb_arrow_table_to_polars(_duckdb_rows_to_arrow(description, rel.fetchall()))
        arrow_table = self._arrow_table(rel)
        if arrow_table is not None:
            arrow_table = _duckdb_arrow_table_to_canonical(arrow_table, description)
            return _duckdb_arrow_table_to_polars(arrow_table)
        return _records_to_polars(_records_from_duckdb_rows(description, rel.fetchall()))

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes bulk ingestion plan in DuckDB.

        Args:
            plan_or_statement: BulkIngestionPlan or statement.
            batches: Batch parameter sequence.

        Returns:
            BulkExecutionResult metrics.
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
                if _CYPHER_BULK_STATEMENT_PATTERN.match(statement):
                    # A failed statement aborts the whole explicit transaction on
                    # DuckDB 2.0, so never send the Cypher plan to the parser.
                    self._fallback_bulk_ingest(statement, batch_data)
                    continue
                try:
                    self.con.execute(statement, b)
                except Exception:
                    self._fallback_bulk_ingest(statement, batch_data)
        else:
            statement = plan_or_statement.statement
            for batch_item in plan_or_statement:
                total_batches += 1
                batch_data = batch_item.parameters.get("batch", [])
                total_records += len(batch_data)
                if _CYPHER_BULK_STATEMENT_PATTERN.match(batch_item.statement):
                    self._fallback_bulk_ingest(batch_item.statement, batch_data)
                    continue
                try:
                    self.con.execute(batch_item.statement, batch_item.parameters)
                except Exception:
                    self._fallback_bulk_ingest(batch_item.statement, batch_data)

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    def _fallback_bulk_ingest(self, statement: str, batch_data: list[dict[str, Any]]) -> None:
        """Fallback for relational graph tables in DuckDB when raw Cypher UNWIND is executed."""
        if not batch_data:
            return
        import re

        import pyarrow as pa

        match = re.search(r":([A-Za-z0-9_]+)", statement)
        table_name = match.group(1) if match else "entities"
        tbl = pa.Table.from_pylist(batch_data)
        self.con.register("_voyager_temp_batch", tbl)
        try:
            tables = [r[0] for r in self.con.execute("SHOW TABLES").fetchall()]
            if table_name not in tables:
                cols = tbl.column_names
                col_defs = []
                for col in cols:
                    if col == "id":
                        col_defs.append(f"{col} VARCHAR PRIMARY KEY")
                    else:
                        col_defs.append(f"{col} VARCHAR")
                self.con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})")

            self.con.execute(
                f"INSERT OR REPLACE INTO {table_name} BY NAME SELECT * FROM _voyager_temp_batch"
            )
        finally:
            self.con.unregister("_voyager_temp_batch")

    def ping(self) -> bool:
        """Pings DuckDB to verify connection liveness."""
        try:
            self.con.execute("SELECT 1")
            return True
        except Exception:
            return False

    def begin_transaction(self) -> DuckDbBridgeTransaction:
        """Begins an explicit DuckDB transaction on the bridge connection."""
        return DuckDbBridgeTransaction(self)

    def close(self) -> None:
        """Closes connection."""
        if hasattr(self.con, "close"):
            self.con.close()


class DuckDbBridgeTransaction:
    """Explicit DuckDB / DuckPGQ transaction holding one connection.

    DuckDB has no savepoints and no nested transactions: ``BEGIN`` inside an
    existing transaction fails. A failing statement aborts the whole DuckDB
    transaction, so this wrapper marks the transaction ``FAILED`` and rejects
    further statements until it is rolled back.
    """

    _savepoint_error_message = (
        "DuckDB transactions do not support savepoints; "
        "use Apache AGE/PostgreSQL for real savepoints"
    )

    def __init__(self, bridge: DuckDbBridge) -> None:
        """Starts an explicit transaction on the bridge's connection.

        Args:
            bridge: Parent DuckDB bridge.

        Raises:
            RuntimeError: If the connection is already inside a transaction.
        """
        self._bridge = bridge
        try:
            bridge.con.execute("BEGIN")
        except Exception as exc:
            raise RuntimeError(
                "cannot begin a DuckDB transaction: the connection may already be inside "
                f"a transaction (DuckDB has no nested transactions): {exc}"
            ) from exc
        self._state = "ACTIVE"

    @property
    def state(self) -> str:
        """Returns the lifecycle state ('ACTIVE', 'FAILED', 'COMMITTED', 'ROLLED_BACK')."""
        return self._state

    @property
    def is_active(self) -> bool:
        """Returns whether the transaction accepts statements."""
        return self._state == "ACTIVE"

    @property
    def supports_savepoints(self) -> bool:
        """DuckDB has no savepoints."""
        return False

    def _mark_failed(self) -> None:
        """Marks the transaction aborted after a statement/driver error."""
        self._state = "FAILED"

    def _ensure_executable(self) -> None:
        """Rejects further statements once the transaction has failed."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement inside the transaction through the DuckPGQ bridge."""
        self._ensure_executable()
        try:
            return self._bridge.execute(statement, parameters)
        except BaseException:
            self._mark_failed()
            raise

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement through the DuckPGQ bridge and returns Polars."""
        self._ensure_executable()
        try:
            return self._bridge.execute_to_polars(statement, parameters)
        except BaseException:
            self._mark_failed()
            raise

    def execute_to_arrow(self, statement: str, parameters: dict[str, Any] | None = None) -> Any:
        """Executes a statement through the DuckPGQ bridge and returns Arrow."""
        self._ensure_executable()
        try:
            return self._bridge.execute_to_arrow(statement, parameters)
        except BaseException:
            self._mark_failed()
            raise

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan inside the transaction."""
        self._ensure_executable()
        try:
            return self._bridge.execute_bulk(plan_or_statement, batches)
        except BaseException:
            self._mark_failed()
            raise

    def commit(self) -> None:
        """Commits the transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back instead of committing")
        if not self.is_active:
            raise ValueError("Cannot commit inactive transaction")
        try:
            self._bridge.con.execute("COMMIT")
            self._state = "COMMITTED"
        except BaseException:
            self._mark_failed()
            raise

    def rollback(self) -> None:
        """Rolls the transaction back."""
        if self._state not in ("ACTIVE", "FAILED"):
            raise ValueError("Cannot rollback inactive transaction")
        try:
            self._bridge.con.execute("ROLLBACK")
            self._state = "ROLLED_BACK"
        except BaseException:
            self._mark_failed()
            raise

    def savepoint(self, name: str) -> None:
        """Raises because DuckDB does not support savepoints."""
        raise RuntimeError(self._savepoint_error_message)

    def rollback_to_savepoint(self, name: str) -> None:
        """Raises because DuckDB does not support savepoints."""
        raise RuntimeError(self._savepoint_error_message)

    def release_savepoint(self, name: str) -> None:
        """Raises because DuckDB does not support savepoints."""
        raise RuntimeError(self._savepoint_error_message)


class AsyncDuckDbBridge:
    """Asynchronous DuckDB driver bridge using asyncio worker threads."""

    def __init__(self, connection: Any, *, install_duckpgq: bool = False) -> None:
        """Initializes AsyncDuckDbBridge.

        Args:
            connection: DuckDB connection object.
            install_duckpgq: Install the DuckPGQ extension when it is not already
                available before loading it. Off by default because installing
                downloads the extension from the DuckDB extension repository.
        """
        self.sync_bridge = DuckDbBridge(connection, install_duckpgq=install_duckpgq)

    def set_install_duckpgq(self, install: bool) -> None:
        """Sets the DuckPGQ install opt-in; see `DuckDbBridge.set_install_duckpgq`."""
        self.sync_bridge.set_install_duckpgq(install)

    def ensure_duckpgq(self, install: bool | None = None) -> None:
        """Loads DuckPGQ on the wrapped connection once; see `DuckDbBridge.ensure_duckpgq`."""
        self.sync_bridge.ensure_duckpgq(install)

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Asynchronously executes query.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            List of record dictionaries.
        """
        return await asyncio.to_thread(self.sync_bridge.execute, statement, parameters)

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Asynchronously streams query results to Polars DataFrame.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame.
        """
        return await asyncio.to_thread(self.sync_bridge.execute_to_polars, statement, parameters)

    async def execute_to_arrow(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> Any:
        """Asynchronously streams query results to an Arrow table."""
        return await asyncio.to_thread(self.sync_bridge.execute_to_arrow, statement, parameters)

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
        return await asyncio.to_thread(self.sync_bridge.execute_bulk, plan_or_statement, batches)

    async def ping(self) -> bool:
        """Pings DuckDB asynchronously to verify connection liveness."""
        return await asyncio.to_thread(self.sync_bridge.ping)

    async def begin_transaction(self) -> AsyncDuckDbBridgeTransaction:
        """Begins an explicit DuckDB transaction asynchronously."""
        sync_tx = await asyncio.to_thread(self.sync_bridge.begin_transaction)
        return AsyncDuckDbBridgeTransaction(sync_tx)

    async def close(self) -> None:
        """Closes connection asynchronously."""
        await asyncio.to_thread(self.sync_bridge.close)


class AsyncDuckDbBridgeTransaction:
    """Asynchronous wrapper around a synchronous DuckDB transaction."""

    def __init__(self, transaction: DuckDbBridgeTransaction) -> None:
        """Initializes the async transaction wrapper."""
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
        """DuckDB has no savepoints."""
        return False

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement inside the transaction."""
        try:
            return await asyncio.to_thread(self._sync.execute, statement, parameters)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement and returns a Polars DataFrame."""
        try:
            return await asyncio.to_thread(self._sync.execute_to_polars, statement, parameters)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def execute_to_arrow(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> Any:
        """Executes a statement and returns an Arrow table."""
        try:
            return await asyncio.to_thread(self._sync.execute_to_arrow, statement, parameters)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan inside the transaction."""
        try:
            return await asyncio.to_thread(self._sync.execute_bulk, plan_or_statement, batches)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def commit(self) -> None:
        """Commits the transaction."""
        try:
            await asyncio.to_thread(self._sync.commit)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def rollback(self) -> None:
        """Rolls the transaction back."""
        try:
            await asyncio.to_thread(self._sync.rollback)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def savepoint(self, name: str) -> None:
        """Raises because DuckDB does not support savepoints."""
        raise RuntimeError(self._sync._savepoint_error_message)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Raises because DuckDB does not support savepoints."""
        raise RuntimeError(self._sync._savepoint_error_message)

    async def release_savepoint(self, name: str) -> None:
        """Raises because DuckDB does not support savepoints."""
        raise RuntimeError(self._sync._savepoint_error_message)


def mask_uri_password(uri: str) -> str:
    """Masks password credentials in a database URI for safe error messages and logging.

    Replaces password with '***' while retaining username, host, port, path, and query.
    For non-string or non-URI inputs, returns the input converted to string.
    """
    if not isinstance(uri, str) or "://" not in uri:
        return str(uri)
    try:
        scheme, rest = uri.split("://", 1)
        slash_idx = rest.find("/")
        query_idx = rest.find("?")
        limit = len(rest)
        if slash_idx != -1:
            limit = min(limit, slash_idx)
        if query_idx != -1:
            limit = min(limit, query_idx)

        authority = rest[:limit]
        if "@" in authority:
            userinfo, hostport = authority.rsplit("@", 1)
            if ":" in userinfo:
                user, _ = userinfo.split(":", 1)
                masked_authority = f"{user}:***@{hostport}"
            else:
                masked_authority = f"{userinfo}@{hostport}"
            return f"{scheme}://{masked_authority}{rest[limit:]}"
    except Exception:
        pass
    return uri


def _description_type_oid(description: Any) -> int | None:
    """Returns the PostgreSQL type OID from a cursor description entry."""
    type_code = getattr(description, "type_code", None)
    if isinstance(type_code, int):
        return type_code
    oid = getattr(type_code, "oid", None)
    if isinstance(oid, int):
        return oid
    if isinstance(description, (list, tuple)) and len(description) > 1:
        value = description[1]
        if isinstance(value, int):
            return value
        oid = getattr(value, "oid", None)
        if isinstance(oid, int):
            return oid
    return None


def _postgres_agtype_oid(conn: Any) -> int | None:
    """Returns the server's ``agtype`` type OID, or None when AGE is not installed."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT oid FROM pg_catalog.pg_type "
                "WHERE typname = 'agtype' AND typnamespace = 'ag_catalog'::regnamespace"
            )
            row = cur.fetchone()
    except Exception:
        return None
    if not row:
        return None
    oid = row[0]
    return oid if isinstance(oid, int) else None


def setup_postgres_age(conn: Any, force: bool = False) -> None:
    """Configures a PostgreSQL connection for Apache AGE if required or installed.

    Executes `LOAD 'age'; SET search_path = ag_catalog, "$user", public;`.
    If `force` is True (e.g. for `age://` schemes), the setup runs unconditionally and raises on failure.
    If `force` is False (e.g. for `postgresql://` schemes or user-supplied connections), it first verifies
    whether the 'age' extension is installed via `SELECT 1 FROM pg_extension WHERE extname = 'age'`;
    if the extension is absent, plain PostgreSQL remains untouched.
    """
    if conn is None or not hasattr(conn, "cursor"):
        return
    if getattr(conn, "_voyager_age_initialized", False):
        return

    should_setup = force
    if not should_setup:
        try:
            with conn.cursor() as cur:
                if not hasattr(cur, "fetchone"):
                    return
                cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'age'")
                row = cur.fetchone()
                if row:
                    should_setup = True
        except Exception:
            return

    if should_setup:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
        try:
            conn._voyager_age_initialized = True
        except Exception:
            pass


def _duckdb_mdn_to_duration(val: Any) -> Duration:
    """Converts a PyArrow ``MonthDayNano`` value to an exact :class:`Duration`."""
    total_ns = int(val.nanoseconds)
    sign = -1 if total_ns < 0 else 1
    abs_ns = abs(total_ns)
    secs = sign * (abs_ns // 1_000_000_000)
    ns = sign * (abs_ns % 1_000_000_000)
    return Duration(
        months=int(val.months),
        days=int(val.days),
        seconds=secs,
        nanoseconds=ns,
    )


def _is_month_day_nano_interval(arrow_type: Any) -> bool:
    """Returns whether ``arrow_type`` is PyArrow's ``month_day_nano_interval``."""
    import pyarrow as pa

    return bool(arrow_type == pa.month_day_nano_interval())


def _duckdb_hashable_map_key(decoded_key: Any, raw_key: Any) -> Any:
    """Returns ``decoded_key`` when hashable as a Python dict key, else ``raw_key``."""
    try:
        hash(decoded_key)
    except TypeError:
        return raw_key
    return decoded_key


def _decode_duckdb_arrow_array(arr: Any, dtype: Any) -> list[Any]:
    """Recursively decodes a DuckDB PyArrow array into Python values using ``dtype``."""
    import uuid as _uuid

    import pyarrow as pa

    if isinstance(arr, pa.ChunkedArray):
        if arr.num_chunks == 0:
            return []
        arr = arr.combine_chunks()
    if len(arr) == 0:
        return []
    tid = _duckdb_type_id(dtype) if dtype else ""
    if tid == "time with time zone":
        return [None if v is None else _parse_duckdb_timetz_python(v) for v in arr.to_pylist()]
    if tid in ("hugeint", "uhugeint"):
        return [None if v is None else int(v) for v in arr.to_pylist()]
    if tid == "uuid":
        return [
            None
            if v is None
            else (
                v
                if isinstance(v, _uuid.UUID)
                else (_uuid.UUID(bytes=v) if isinstance(v, bytes) else _uuid.UUID(str(v)))
            )
            for v in arr.to_pylist()
        ]
    if tid == "json":
        return [
            None if v is None else (json.loads(v) if isinstance(v, str) else v)
            for v in arr.to_pylist()
        ]
    if _is_month_day_nano_interval(arr.type):
        return [None if v is None else _duckdb_mdn_to_duration(v) for v in arr.to_pylist()]
    if pa.types.is_list(arr.type) or pa.types.is_large_list(arr.type):
        child_dtype = _duckdb_list_child_dtype(dtype, tid)
        decoded_vals = _decode_duckdb_arrow_array(arr.values, child_dtype)
        offsets = arr.offsets.to_pylist()
        nulls = arr.is_null().to_pylist()
        base = arr.offset
        return [
            None if is_null else decoded_vals[offsets[base + i] : offsets[base + i + 1]]
            for i, is_null in enumerate(nulls)
        ]
    if pa.types.is_fixed_size_list(arr.type):
        child_dtype = _duckdb_array_child_dtype(dtype, tid)
        decoded_vals = _decode_duckdb_arrow_array(arr.values, child_dtype)
        size = arr.type.list_size
        nulls = arr.is_null().to_pylist()
        base = arr.offset * size
        return [
            None if is_null else tuple(decoded_vals[base + i * size : base + (i + 1) * size])
            for i, is_null in enumerate(nulls)
        ]
    if pa.types.is_map(arr.type):
        children = _duckdb_type_children_map(dtype, "map")
        decoded_keys = _decode_duckdb_arrow_array(arr.keys, children.get("key", ""))
        decoded_items = _decode_duckdb_arrow_array(arr.items, children.get("value", ""))
        raw_keys = arr.keys.to_pylist()
        offsets = arr.offsets.to_pylist()
        nulls = arr.is_null().to_pylist()
        base = arr.offset
        return [
            None
            if is_null
            else {
                _duckdb_hashable_map_key(decoded_keys[j], raw_keys[j]): decoded_items[j]
                for j in range(offsets[base + i], offsets[base + i + 1])
            }
            for i, is_null in enumerate(nulls)
        ]
    if pa.types.is_struct(arr.type):
        dtype_fields = _duckdb_type_children_map(dtype, "struct")
        names = [f.name for f in arr.type]
        decoded_fields = [
            _decode_duckdb_arrow_array(arr.field(idx), dtype_fields.get(name, ""))
            for idx, name in enumerate(names)
        ]
        nulls = arr.is_null().to_pylist()
        return [
            None if is_null else {name: decoded_fields[idx][i] for idx, name in enumerate(names)}
            for i, is_null in enumerate(nulls)
        ]
    if pa.types.is_union(arr.type):
        dtype_members = _duckdb_type_children_map(dtype, "union")
        decoded_members = {
            code: _decode_duckdb_arrow_array(
                arr.field(idx), dtype_members.get(arr.type.field(idx).name, "")
            )
            for idx, code in enumerate(arr.type.type_codes)
        }
        codes = arr.type_codes.to_pylist()
        if arr.type.mode == "dense":
            offsets = arr.offsets.to_pylist()
            return [decoded_members[codes[i]][offsets[i]] for i in range(len(arr))]
        return [decoded_members[codes[i]][i] for i in range(len(arr))]
    return [_normalize_duckdb_value(v) for v in arr.to_pylist()]


def _records_from_duckdb_rows(
    description: Sequence[Any], rows: Sequence[Sequence[Any]]
) -> BridgeResult:
    """Builds a :class:`BridgeResult` from an already-fetched DuckDB cursor."""
    cols = [str(d[0]) for d in description]
    dtypes = [d[1] if len(d) > 1 else "" for d in description]
    records: list[dict[str, Any]] = []
    for row in rows:
        rec: dict[str, Any] = {}
        for idx, (col, val) in enumerate(zip(cols, row, strict=False)):
            dtype = dtypes[idx] if idx < len(dtypes) else ""
            rec[col] = _normalize_duckdb_value(val, dtype)
        records.append(rec)
    return BridgeResult(records, columns=cols)


def _records_from_duckdb_arrow(description: Sequence[Any], arrow_table: Any) -> BridgeResult:
    """Builds a :class:`BridgeResult` from a DuckDB PyArrow table, preserving interval months."""
    cols = [str(d[0]) for d in description]
    if arrow_table.num_rows == 0:
        return BridgeResult([], columns=cols)
    dtypes = [d[1] if len(d) > 1 else "" for d in description]
    decoded_cols = [
        _decode_duckdb_arrow_array(arrow_table.column(idx), dtypes[idx]) for idx in range(len(cols))
    ]
    num_rows = arrow_table.num_rows
    records: list[dict[str, Any]] = [
        {col: decoded_cols[col_idx][row_idx] for col_idx, col in enumerate(cols)}
        for row_idx in range(num_rows)
    ]
    return BridgeResult(records, columns=cols)


def _arrow_has_month_day_nano_interval(arrow_type: Any) -> bool:
    """Returns whether ``arrow_type`` is or contains ``month_day_nano_interval``."""
    import pyarrow as pa

    if _is_month_day_nano_interval(arrow_type):
        return True
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _arrow_has_month_day_nano_interval(arrow_type.value_type)
    if pa.types.is_struct(arrow_type) or pa.types.is_union(arrow_type):
        return any(_arrow_has_month_day_nano_interval(f.type) for f in arrow_type)
    if pa.types.is_map(arrow_type):
        return _arrow_has_month_day_nano_interval(
            arrow_type.key_type
        ) or _arrow_has_month_day_nano_interval(arrow_type.item_type)
    return False


def _arrow_has_union(arrow_type: Any) -> bool:
    """Returns whether ``arrow_type`` is or contains an Arrow union."""
    import pyarrow as pa

    if pa.types.is_union(arrow_type):
        return True
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _arrow_has_union(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        return any(_arrow_has_union(f.type) for f in arrow_type)
    if pa.types.is_map(arrow_type):
        return _arrow_has_union(arrow_type.key_type) or _arrow_has_union(arrow_type.item_type)
    return False


def _duckdb_union_field_names(fields: Sequence[Any]) -> list[str]:
    """Returns unique, non-empty struct field names for a union's members."""
    used: set[str] = set()
    names: list[str] = []
    for idx, field in enumerate(fields):
        base = str(field.name) or f"member_{idx}"
        candidate = base
        suffix = 2
        while candidate in used:
            candidate = f"{base}_{suffix}"
            suffix += 1
        used.add(candidate)
        names.append(candidate)
    return names


def _duckdb_union_tag_name(names: Sequence[str]) -> str:
    """Returns a ``tag`` field name that does not collide with a union member."""
    candidate = "tag"
    suffix = 2
    while candidate in names:
        candidate = f"tag_{suffix}"
        suffix += 1
    return candidate


def _duckdb_union_tag_name_for_type(arrow_type: Any) -> str:
    """Returns the tag field name used for the first union found in ``arrow_type``."""
    import pyarrow as pa

    if pa.types.is_union(arrow_type):
        return _duckdb_union_tag_name(_duckdb_union_field_names(list(arrow_type)))
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return _duckdb_union_tag_name_for_type(arrow_type.value_type)
    if pa.types.is_struct(arrow_type):
        for field in arrow_type:
            if _arrow_has_union(field.type):
                return _duckdb_union_tag_name_for_type(field.type)
    if pa.types.is_map(arrow_type):
        if _arrow_has_union(arrow_type.key_type):
            return _duckdb_union_tag_name_for_type(arrow_type.key_type)
        return _duckdb_union_tag_name_for_type(arrow_type.item_type)
    return "tag"


def _duckdb_arrow_union_to_struct(arr: Any) -> Any:
    """Converts an Arrow union array to a struct with one field per member plus ``tag``."""
    import pyarrow as pa

    union_type = arr.type
    fields = list(union_type)
    names = _duckdb_union_field_names(fields)
    tag_name = _duckdb_union_tag_name(names)
    codes = arr.type_codes.to_pylist()
    code_to_name = {
        int(code): name for code, name in zip(union_type.type_codes, names, strict=True)
    }
    tag_array = pa.array([code_to_name[int(code)] for code in codes], type=pa.string())
    num_rows = len(arr)
    children = []
    for idx, code in enumerate(union_type.type_codes):
        selected_code = int(code)
        mask = [value == selected_code for value in codes]
        raw_child = arr.field(idx)
        if union_type.mode == "dense":
            selected = raw_child
            positions = arr.offsets.to_pylist()
        else:
            # Sparse union children are full length and their unselected slots are
            # undefined, so filter before recursing (a nested union's unused slots
            # may not be valid) and scatter the converted values back by position.
            selected = raw_child.filter(pa.array(mask))
            positions = []
            offset = 0
            for selected_row in mask:
                if selected_row:
                    positions.append(offset)
                    offset += 1
                else:
                    positions.append(-1)
        converted = _duckdb_arrow_convert_unions(selected)
        if num_rows == 0:
            full_child = pa.nulls(0, type=converted.type)
        else:
            indices = pa.array(
                [
                    int(position) if selected_row else None
                    for position, selected_row in zip(positions, mask, strict=True)
                ],
                type=pa.int64(),
            )
            full_child = converted.take(indices)
        children.append(full_child)
    return pa.StructArray.from_arrays(
        [*children, tag_array],
        names=[*names, tag_name],
    )


def _duckdb_arrow_convert_unions(arr: Any) -> Any:
    """Recursively replaces Arrow union arrays with tagged struct arrays."""
    import pyarrow as pa

    arrow_type = arr.type
    if pa.types.is_union(arrow_type):
        return _duckdb_arrow_union_to_struct(arr)
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        values = _duckdb_arrow_convert_unions(arr.values)
        if values.type == arr.values.type:
            return arr
        if pa.types.is_list(arrow_type):
            return pa.ListArray.from_arrays(arr.offsets, values, mask=arr.is_null())
        return pa.LargeListArray.from_arrays(arr.offsets, values, mask=arr.is_null())
    if pa.types.is_fixed_size_list(arrow_type):
        values = _duckdb_arrow_convert_unions(arr.values)
        if values.type == arr.values.type:
            return arr
        return pa.FixedSizeListArray.from_arrays(values, arrow_type.list_size, mask=arr.is_null())
    if pa.types.is_struct(arrow_type):
        if not any(_arrow_has_union(field.type) for field in arrow_type):
            return arr
        children = [
            _duckdb_arrow_convert_unions(arr.field(idx)) for idx in range(arrow_type.num_fields)
        ]
        return pa.StructArray.from_arrays(
            children,
            names=[field.name for field in arrow_type],
            mask=arr.is_null(),
        )
    if pa.types.is_map(arrow_type):
        if not (_arrow_has_union(arrow_type.key_type) or _arrow_has_union(arrow_type.item_type)):
            return arr
        keys = _duckdb_arrow_convert_unions(arr.keys)
        items = _duckdb_arrow_convert_unions(arr.items)
        return pa.MapArray.from_arrays(arr.offsets, keys, items, mask=arr.is_null())
    return arr


def _duckdb_arrow_type_without_unions(arrow_type: Any) -> Any:
    """Returns the Arrow type produced by replacing every union with a tagged struct."""
    import pyarrow as pa

    if pa.types.is_union(arrow_type):
        fields = list(arrow_type)
        names = _duckdb_union_field_names(fields)
        converted = [
            pa.field(name, _duckdb_arrow_type_without_unions(field.type), nullable=field.nullable)
            for name, field in zip(names, fields, strict=True)
        ]
        converted.append(pa.field(_duckdb_union_tag_name(names), pa.string()))
        return pa.struct(converted)
    if pa.types.is_list(arrow_type):
        return pa.list_(_duckdb_arrow_type_without_unions(arrow_type.value_type))
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(_duckdb_arrow_type_without_unions(arrow_type.value_type))
    if pa.types.is_fixed_size_list(arrow_type):
        return pa.list_(
            _duckdb_arrow_type_without_unions(arrow_type.value_type), arrow_type.list_size
        )
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [
                pa.field(
                    field.name,
                    _duckdb_arrow_type_without_unions(field.type),
                    nullable=field.nullable,
                    metadata=field.metadata,
                )
                for field in arrow_type
            ]
        )
    if pa.types.is_map(arrow_type):
        return pa.map_(
            _duckdb_arrow_type_without_unions(arrow_type.key_type),
            _duckdb_arrow_type_without_unions(arrow_type.item_type),
        )
    return arrow_type


def _duckdb_arrow_column_without_unions(column: Any) -> Any:
    """Converts every union in an Arrow column (or ChunkedArray), preserving empty schemas."""
    import pyarrow as pa

    if isinstance(column, pa.ChunkedArray):
        chunks = [_duckdb_arrow_convert_unions(chunk) for chunk in column.chunks]
        if not chunks:
            return pa.chunked_array([], type=_duckdb_arrow_type_without_unions(column.type))
        return pa.chunked_array(chunks, type=chunks[0].type)
    return _duckdb_arrow_convert_unions(column)


def _duckdb_mdn_column_to_arrow(col: Any) -> Any:
    """Converts a scalar ``month_day_nano_interval`` Arrow column to Polars-safe Arrow."""
    import pyarrow as pa

    values = col.to_pylist()
    can_duration_us = True
    can_duration_ns = True
    for val in values:
        if val is None:
            continue
        if val.months != 0 or val.days != 0:
            can_duration_us = False
            can_duration_ns = False
            break
        ns = int(val.nanoseconds)
        if ns % 1_000 != 0 or not (-(2**63) <= ns // 1_000 < 2**63):
            can_duration_us = False
        if not (-(2**63) <= ns < 2**63):
            can_duration_ns = False

    if can_duration_us:
        converted = [None if v is None else int(v.nanoseconds) // 1_000 for v in values]
        return pa.array(converted, type=pa.duration("us"))
    if can_duration_ns:
        converted = [None if v is None else int(v.nanoseconds) for v in values]
        return pa.array(converted, type=pa.duration("ns"))

    struct_type = pa.struct(
        [
            pa.field("months", pa.int64(), nullable=False),
            pa.field("days", pa.int64(), nullable=False),
            pa.field("seconds", pa.int64(), nullable=False),
            pa.field("nanoseconds", pa.int64(), nullable=False),
        ]
    )
    struct_vals: list[dict[str, int] | None] = []
    for v in values:
        if v is None:
            struct_vals.append(None)
        else:
            dur = _duckdb_mdn_to_duration(v)
            struct_vals.append(
                {
                    "months": dur.months,
                    "days": dur.days,
                    "seconds": dur.seconds,
                    "nanoseconds": dur.nanoseconds,
                }
            )
    return pa.array(struct_vals, type=struct_type)


def _ensure_polars_df(val: Any) -> pl.DataFrame:
    """Narrows ``pl.from_arrow(...)`` to a ``pl.DataFrame``."""
    if isinstance(val, pl.DataFrame):
        return val
    return val.to_frame()


def _duckdb_arrow_table_to_polars(arrow_table: Any) -> pl.DataFrame:
    """Converts a DuckDB Arrow table to Polars without re-executing.

    Avoids Polars interval panics and replaces Arrow ``UNION`` columns, which
    Polars cannot import, with a struct per union member plus a ``tag`` field.
    """
    import pyarrow as pa

    schema = arrow_table.schema
    if not any(
        _arrow_has_month_day_nano_interval(f.type) or _arrow_has_union(f.type) for f in schema
    ):
        return _ensure_polars_df(pl.from_arrow(arrow_table))

    new_cols = []
    new_fields = []
    for idx, field in enumerate(schema):
        col = arrow_table.column(idx)
        if _arrow_has_union(field.type):
            warnings.warn(
                f"DuckDB UNION column {field.name!r} was converted to a struct with a "
                f"{_duckdb_union_tag_name_for_type(field.type)!r} "
                "field because Polars cannot import Arrow union types.",
                UserWarning,
                stacklevel=2,
            )
            col = _duckdb_arrow_column_without_unions(col)
            field = pa.field(field.name, col.type, nullable=field.nullable)
        if _is_month_day_nano_interval(field.type):
            arr = _duckdb_mdn_column_to_arrow(col)
            new_cols.append(arr)
            meta = dict(field.metadata) if field.metadata else {}
            if pa.types.is_struct(arr.type):
                meta[b"voyager.type"] = b"duration"
            new_fields.append(
                pa.field(field.name, arr.type, nullable=field.nullable, metadata=meta or None)
            )
        elif _arrow_has_month_day_nano_interval(field.type):
            arr = _duckdb_canonicalize_arrow_column(col, "")
            new_cols.append(arr)
            new_fields.append(pa.field(field.name, arr.type, nullable=field.nullable))
        else:
            new_cols.append(col)
            new_fields.append(field)

    return _ensure_polars_df(
        pl.from_arrow(pa.Table.from_arrays(new_cols, schema=pa.schema(new_fields)))
    )


def _duckdb_list_child_dtype(dtype: Any, tid: str) -> Any:
    """Returns the element type of a DuckDB ``LIST`` type or type string."""
    if tid == "list" and hasattr(dtype, "children"):
        return dtype.children[0][1]
    if isinstance(dtype, str):
        text = dtype.strip()
        if text.endswith("[]"):
            return text[:-2]
    return ""


def _duckdb_array_child_dtype(dtype: Any, tid: str) -> Any:
    """Returns the element type of a DuckDB fixed-size ``ARRAY`` type or type string."""
    if tid == "array":
        return _duckdb_type_children_map(dtype, "array").get("child", "")
    if isinstance(dtype, str):
        text = dtype.strip()
        if text.endswith("]") and not text.endswith("[]") and "[" in text:
            return text[: text.rfind("[")]
    return ""


def _normalize_duckdb_value(val: Any, dtype: Any = "") -> Any:
    """Normalizes DuckDB Python values (converting ``timedelta``, ``TIMETZ``, ``UUID``, and ``JSON``)."""
    if val is None:
        return None
    tid = _duckdb_type_id(dtype) if dtype else ""
    if tid == "time with time zone":
        return _parse_duckdb_timetz_python(val)
    if tid == "json":
        if isinstance(val, str):
            return json.loads(val)
        return val
    if tid == "uuid":
        if isinstance(val, uuid.UUID):
            return val
        if isinstance(val, bytes):
            return uuid.UUID(bytes=val)
        if isinstance(val, str):
            return uuid.UUID(val)
        return val
    if isinstance(val, datetime.timedelta):
        return Duration.from_timedelta(val)
    if isinstance(val, list):
        child_dtype = _duckdb_list_child_dtype(dtype, tid)
        return [_normalize_duckdb_value(item, child_dtype) for item in val]
    if isinstance(val, tuple):
        child_dtype = _duckdb_array_child_dtype(dtype, tid)
        return tuple(_normalize_duckdb_value(item, child_dtype) for item in val)
    if isinstance(val, dict):
        if tid == "map":
            children = _duckdb_type_children_map(dtype, "map")
            key_dtype = children.get("key", "")
            val_dtype = children.get("value", "")
            return {
                _duckdb_hashable_map_key(_normalize_duckdb_value(k, key_dtype), k): (
                    _normalize_duckdb_value(v, val_dtype)
                )
                for k, v in val.items()
            }
        dtype_fields = _duckdb_type_children_map(dtype, "struct")
        return {k: _normalize_duckdb_value(v, dtype_fields.get(k, "")) for k, v in val.items()}
    if tid == "union" and isinstance(val, str):
        if _duckdb_type_has(dtype, _DUCKDB_TIMETZ_TYPE_IDS):
            try:
                return _parse_duckdb_timetz_python(val)
            except (TypeError, ValueError):
                pass
        if _duckdb_type_has(dtype, _DUCKDB_JSON_TYPE_IDS) and not _duckdb_type_has(
            dtype, frozenset({"varchar"})
        ):
            return json.loads(val)
    return val


def _format_pg_duration_text(dur: Duration) -> str:
    """Formats a :class:`Duration` into PostgreSQL ``interval`` text syntax."""
    total_us = dur.seconds * 1_000_000 + dur.nanoseconds // 1_000
    if dur.months == 0 and dur.days == 0 and total_us == 0:
        return "0 seconds"
    sign = "-" if total_us < 0 else ""
    abs_us = abs(total_us)
    secs, us = divmod(abs_us, 1_000_000)
    return f"{dur.months} mons {dur.days} days {sign}{secs}.{us:06d} seconds"


_JSON_CAST_RE = re.compile(
    r"\s*::\s*(?:[A-Za-z_][A-Za-z0-9_$]*\s*\.\s*)?(?:jsonb|json)\b", re.IGNORECASE
)


def _placeholder_forces_json(statement: str, position: int) -> bool:
    """Returns whether the placeholder ending at ``position`` is cast to json/jsonb."""
    return _JSON_CAST_RE.match(statement, position) is not None


def _unwrap_json_marker(value: Any) -> tuple[bool, Any]:
    """Returns ``(was_explicit, unwrapped)`` after recursively removing ``Json`` wrappers."""
    if isinstance(value, Json):
        return True, _unwrap_json_marker(value.value)[1]
    if isinstance(value, Mapping):
        return False, {k: _unwrap_json_marker(v)[1] for k, v in value.items()}
    if isinstance(value, tuple):
        return False, tuple(_unwrap_json_marker(v)[1] for v in value)
    if isinstance(value, list):
        return False, [_unwrap_json_marker(v)[1] for v in value]
    return False, value


def _strip_json_markers(value: Any) -> Any:
    """Recursively removes ``Json`` wrappers, returning the contained values."""
    return _unwrap_json_marker(value)[1]


def _pg_json_adapter(value: Any) -> Any:
    """Wraps a value for psycopg so PostgreSQL binds it as ``jsonb``."""
    try:
        from psycopg.types.json import Jsonb

        return Jsonb(value)
    except ImportError:
        try:
            extras = importlib.import_module("psycopg2.extras")
            return extras.Json(value)
        except ImportError:
            return value


def _encode_pg_array_element(value: Any) -> Any:
    """Adapts one element of a PostgreSQL array parameter.

    psycopg cannot adapt a raw ``dict``; inside an array each mapping becomes a
    ``jsonb`` element (giving ``jsonb[]``), and nested lists recurse so they
    become multidimensional arrays. Explicit ``Json(...)`` elements keep their
    JSON element type.
    """
    if isinstance(value, Json):
        return _pg_json_adapter(_unwrap_json_marker(value)[1])
    if isinstance(value, Mapping):
        return _pg_json_adapter(value)
    if isinstance(value, tuple):
        return tuple(_encode_pg_array_element(item) for item in value)
    if isinstance(value, list):
        return [_encode_pg_array_element(item) for item in value]
    return value


def _encode_pg_parameter(value: Any, *, force_json: bool) -> Any:
    """Chooses the PostgreSQL parameter encoding for one value.

    The rule matches the native driver and psycopg: ``Json(...)`` wrappers and
    maps bind as ``jsonb``; an explicit ``::json``/``::jsonb`` cast forces the
    whole value into one JSON document (including empty and nested lists, so
    ``[]`` becomes ``[]``, never ``{}``). Without a cast a list stays a SQL
    array: a list of dicts becomes ``jsonb[]``, a list of scalars a typed array,
    and nested lists a multidimensional array. An empty list therefore remains
    an empty array literal for array-typed slots.
    """
    explicit, unwrapped = _unwrap_json_marker(value)
    if explicit or force_json or isinstance(unwrapped, Mapping):
        return _pg_json_adapter(unwrapped)
    if isinstance(value, (list, tuple)):
        # Use the original list so nested ``Json(...)`` markers keep their JSON
        # element type instead of being flattened by ``_unwrap_json_marker``.
        return _encode_pg_array_element(value)
    return unwrapped


def _configure_pg_adapters(cursor: Any) -> bool:
    """Registers per-cursor ``interval`` loader and ``Duration`` dumper on psycopg 3 cursors."""
    adapters = getattr(cursor, "adapters", None)
    if adapters is None:
        return False
    try:
        from psycopg.adapt import Dumper, Loader
    except ImportError:
        return False

    class _VoyagerIntervalLoader(Loader):
        def load(self, data: Any) -> Any:
            text = bytes(data).decode("utf-8")
            try:
                return parse_pg_interval_text(text)
            except ValueError:
                try:
                    from psycopg.types.datetime import IntervalLoader

                    return Duration.from_timedelta(
                        IntervalLoader(self.oid, self.connection).load(data)
                    )
                except Exception:
                    return text

    class _VoyagerDurationDumper(Dumper):
        oid = 1186  # PostgreSQL INTERVAL OID

        def dump(self, obj: Duration) -> bytes:
            return _format_pg_duration_text(obj).encode("utf-8")

    adapters.register_loader("interval", _VoyagerIntervalLoader)
    adapters.register_dumper(Duration, _VoyagerDurationDumper)
    return True


def _normalize_pg_fallback_value(val: Any) -> Any:
    """Converts ``datetime.timedelta`` values to :class:`Duration` when cursor adapters are absent."""
    if isinstance(val, datetime.timedelta):
        return Duration.from_timedelta(val)
    if isinstance(val, list):
        return [_normalize_pg_fallback_value(item) for item in val]
    if isinstance(val, dict):
        return {k: _normalize_pg_fallback_value(v) for k, v in val.items()}
    return val


class PostgresBridge:
    """Synchronous PostgreSQL driver bridge (psycopg 3 / psycopg2) with zero-copy Polars output."""

    def __init__(self, connection: Any, graph_name: str | None = None) -> None:
        """Initializes PostgresBridge.

        Args:
            connection: PostgreSQL connection instance (e.g. psycopg.Connection).
            graph_name: Optional Apache AGE graph name to target.
        """
        if graph_name is not None:
            validate_graph_name(graph_name)
        self.conn = connection
        self._conn = connection
        self.graph_name = graph_name
        setup_postgres_age(self.conn, force=False)
        self._agtype_oid = (
            _postgres_agtype_oid(self.conn)
            if getattr(self.conn, "_voyager_age_initialized", False) is True
            else None
        )

    def _decode_row(
        self, cursor: Any, row: Sequence[Any], *, convert_timedelta: bool = False
    ) -> tuple[Any, ...]:
        """Decodes PostgreSQL ``agtype`` columns and normalizes fallback intervals."""
        agtype_indices: list[int] = []
        if self._agtype_oid is not None and cursor.description:
            agtype_indices = [
                index
                for index, description in enumerate(cursor.description)
                if _description_type_oid(description) == self._agtype_oid
            ]
        if not agtype_indices and not convert_timedelta:
            return tuple(row)
        decoded = list(row)
        for index in agtype_indices:
            if index < len(decoded) and decoded[index] is not None:
                decoded[index] = decode_agtype(decoded[index])
        if convert_timedelta:
            for index, val in enumerate(decoded):
                if index not in agtype_indices:
                    decoded[index] = _normalize_pg_fallback_value(val)
        return tuple(decoded)

    @property
    def connection(self) -> Any:
        """Returns underlying PostgreSQL connection instance."""
        return self.conn

    def _format_pgq_statement(
        self, statement: str, parameters: dict[str, Any]
    ) -> tuple[str, list[Any] | tuple[Any, ...]]:
        """Formats statement and parameters for PostgreSQL execution."""
        validate_pagination_parameters(statement, parameters)
        if not parameters:
            return statement, ()

        if "cypher(" in statement:
            if not getattr(self.conn, "_voyager_age_initialized", False):
                setup_postgres_age(self.conn, force=True)
            import json

            return statement, (json.dumps(_strip_json_markers(parameters)),)

        ordered_params: list[Any] = []
        regex = re.compile(r"\$([a-zA-Z0-9_]+)\b")

        def repl(m: re.Match[str]) -> str:
            key = m.group(1)
            if key in parameters:
                ordered_params.append(
                    _encode_pg_parameter(
                        parameters[key],
                        force_json=_placeholder_forces_json(statement, m.end()),
                    )
                )
                return "%s"
            return m.group(0)

        escaped_stmt = statement.replace("%", "%%")
        stmt = regex.sub(repl, escaped_stmt)
        if not ordered_params:
            return statement, ()
        return stmt, ordered_params

    def _execute_cursor(self, cursor: Any, stmt: str, params: Any) -> BridgeResult:
        """Executes a formatted statement on a psycopg cursor and decodes AGE/interval columns."""
        has_adapters = _configure_pg_adapters(cursor)
        cursor.execute(stmt, params or None)
        if cursor.description:
            cols = [str(d[0]) for d in cursor.description]
            rows = cursor.fetchall()
            decoded = [
                self._decode_row(cursor, row, convert_timedelta=not has_adapters) for row in rows
            ]
            return BridgeResult(
                [dict(zip(cols, row, strict=False)) for row in decoded],
                columns=cols,
            )
        return BridgeResult([], columns=())

    def _execute_cursor_polars(self, cursor: Any, stmt: str, params: Any) -> pl.DataFrame:
        """Executes a formatted statement on a psycopg cursor and returns a Polars DataFrame."""
        has_adapters = _configure_pg_adapters(cursor)
        cursor.execute(stmt, params or None)
        if cursor.description:
            cols = [str(d[0]) for d in cursor.description]
            rows = cursor.fetchall()
            decoded = [
                tuple(
                    serialize_value_for_arrow(v)
                    for v in self._decode_row(cursor, row, convert_timedelta=not has_adapters)
                )
                for row in rows
            ]
            return pl.DataFrame(decoded, schema=cols, orient="row")
        return pl.DataFrame()

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes query on PostgreSQL.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            List of record dictionaries.
        """
        stmt, params = self._format_pgq_statement(statement, parameters or {})
        with self.conn.cursor() as cur:
            return self._execute_cursor(cur, stmt, params)

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes query and streams to Polars DataFrame using zero-copy extraction.

        Args:
            statement: Query statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame.
        """
        stmt, params = self._format_pgq_statement(statement, parameters or {})
        with self.conn.cursor() as cur:
            return self._execute_cursor_polars(cur, stmt, params)

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes bulk ingestion plan in PostgreSQL.

        Args:
            plan_or_statement: BulkIngestionPlan or statement string.
            batches: Batch parameter sequence when a statement string is provided.

        Returns:
            BulkExecutionResult metrics.
        """
        start_time = time.perf_counter()
        statement = ""
        total_records = 0
        total_batches = 0
        with self.conn.cursor() as cur:
            if isinstance(plan_or_statement, str):
                statement = plan_or_statement
                batch_list = batches or []
                total_batches = len(batch_list)
                for b in batch_list:
                    batch_data = b.get("batch", [])
                    total_records += len(batch_data)
                    stmt, params = self._format_pgq_statement(statement, b)
                    cur.execute(stmt, params or None)
            else:
                statement = plan_or_statement.statement
                for batch_item in plan_or_statement:
                    total_batches += 1
                    batch_data = batch_item.parameters.get("batch", [])
                    total_records += len(batch_data)
                    stmt, params = self._format_pgq_statement(
                        batch_item.statement, batch_item.parameters
                    )
                    cur.execute(stmt, params or None)
        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    def ping(self) -> bool:
        """Pings PostgreSQL to verify connection liveness."""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    def begin_transaction(self) -> PostgresBridgeTransaction:
        """Begins an explicit PostgreSQL/AGE transaction on the bridge connection."""
        return PostgresBridgeTransaction(self)

    def close(self) -> None:
        """Closes connection."""
        if hasattr(self.conn, "close"):
            self.conn.close()


class PostgresBridgeTransaction:
    """Explicit PostgreSQL / Apache AGE transaction holding one connection."""

    def __init__(self, bridge: PostgresBridge) -> None:
        """Starts a transaction on the bridge's connection.

        The AGE setup (`LOAD 'age'`, `SET search_path`) runs before `BEGIN`, so a
        rollback cannot undo it.

        Args:
            bridge: Parent PostgreSQL bridge.
        """
        self._bridge = bridge
        conn = bridge.conn
        setup_postgres_age(conn, force=False)

        self._original_autocommit = getattr(conn, "autocommit", None)
        if self._original_autocommit is False:
            # Commit any pending work (including AGE setup) and take explicit
            # control of BEGIN/COMMIT so psycopg does not start its own block.
            conn.commit()
            conn.autocommit = True

        try:
            self._execute_sql("BEGIN")
        except Exception:
            self._restore_autocommit()
            raise
        self._state = "ACTIVE"

    @property
    def state(self) -> str:
        """Returns the lifecycle state ('ACTIVE', 'FAILED', 'COMMITTED', 'ROLLED_BACK')."""
        return self._state

    @property
    def is_active(self) -> bool:
        """Returns whether the transaction accepts statements."""
        if self._state != "ACTIVE":
            return False
        conn = self._bridge.conn
        if _pg_connection_closed(conn):
            return False
        status = _pg_transaction_status(conn)
        if status is None:
            return True
        return status in ("INTRANS", "ACTIVE")

    @property
    def supports_savepoints(self) -> bool:
        """PostgreSQL/AGE supports real savepoints."""
        return True

    def _mark_failed(self) -> None:
        """Marks the transaction aborted after a statement/driver error."""
        self._state = "FAILED"

    def _ensure_executable(self) -> None:
        """Rejects further statements once the transaction has failed."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")

    def _restore_autocommit(self) -> None:
        """Restores the connection's original autocommit mode."""
        if self._original_autocommit is not None:
            try:
                self._bridge.conn.autocommit = self._original_autocommit
            except Exception:
                pass

    def _execute_sql(self, statement: str, parameters: Sequence[Any] = ()) -> str | None:
        """Executes a control statement and returns its command tag."""
        with self._bridge.conn.cursor() as cur:
            cur.execute(statement, parameters or None)
            return getattr(cur, "statusmessage", None)

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement inside the transaction."""
        self._ensure_executable()
        stmt, params = self._bridge._format_pgq_statement(statement, parameters or {})
        try:
            with self._bridge.conn.cursor() as cur:
                return self._bridge._execute_cursor(cur, stmt, params)
        except BaseException:
            self._mark_failed()
            raise

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement and returns a Polars DataFrame."""
        self._ensure_executable()
        stmt, params = self._bridge._format_pgq_statement(statement, parameters or {})
        try:
            with self._bridge.conn.cursor() as cur:
                return self._bridge._execute_cursor_polars(cur, stmt, params)
        except BaseException:
            self._mark_failed()
            raise

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan inside the transaction."""
        self._ensure_executable()
        start_time = time.perf_counter()
        statement = ""
        total_records = 0
        total_batches = 0

        try:
            with self._bridge.conn.cursor() as cur:
                if isinstance(plan_or_statement, str):
                    statement = plan_or_statement
                    batch_list = batches or []
                    total_batches = len(batch_list)
                    for b in batch_list:
                        total_records += len(b.get("batch", []))
                        stmt, params = self._bridge._format_pgq_statement(statement, b)
                        cur.execute(stmt, params or None)
                else:
                    statement = plan_or_statement.statement
                    for batch_item in plan_or_statement:
                        total_batches += 1
                        total_records += len(batch_item.parameters.get("batch", []))
                        stmt, params = self._bridge._format_pgq_statement(
                            batch_item.statement, batch_item.parameters
                        )
                        cur.execute(stmt, params or None)
        except BaseException:
            self._mark_failed()
            raise

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    def commit(self) -> None:
        """Commits the transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back instead of committing")
        if not self.is_active:
            raise ValueError("Cannot commit inactive transaction")
        try:
            status = _pg_transaction_status(self._bridge.conn)
            if status == "INERROR":
                raise RuntimeError("transaction is aborted; PostgreSQL is in a failed state")
            tag = self._execute_sql("COMMIT")
            if tag is not None and tag.upper().startswith("ROLLBACK"):
                raise RuntimeError(
                    "transaction is aborted; PostgreSQL converted COMMIT into ROLLBACK"
                )
            self._state = "COMMITTED"
        except BaseException:
            self._mark_failed()
            raise
        finally:
            self._restore_autocommit()

    def rollback(self) -> None:
        """Rolls the transaction back."""
        if self._state not in ("ACTIVE", "FAILED"):
            raise ValueError("Cannot rollback inactive transaction")
        try:
            self._execute_sql("ROLLBACK")
            self._state = "ROLLED_BACK"
        except BaseException:
            self._mark_failed()
            raise
        finally:
            self._restore_autocommit()

    def savepoint(self, name: str) -> None:
        """Creates a real SAVEPOINT."""
        self._ensure_executable()
        name = _validate_savepoint_name(name)
        self._execute_sql(f"SAVEPOINT {name}")

    def rollback_to_savepoint(self, name: str) -> None:
        """Rolls back to a real SAVEPOINT."""
        self._ensure_executable()
        name = _validate_savepoint_name(name)
        self._execute_sql(f"ROLLBACK TO SAVEPOINT {name}")

    def release_savepoint(self, name: str) -> None:
        """Releases a real SAVEPOINT."""
        self._ensure_executable()
        name = _validate_savepoint_name(name)
        self._execute_sql(f"RELEASE SAVEPOINT {name}")


class AsyncPostgresBridge:
    """Asynchronous PostgreSQL driver bridge using asyncio worker threads."""

    def __init__(self, connection: Any, graph_name: str | None = None) -> None:
        """Initializes AsyncPostgresBridge.

        Args:
            connection: PostgreSQL connection instance.
            graph_name: Optional Apache AGE graph name to target.
        """
        if graph_name is not None:
            validate_graph_name(graph_name)
        self.sync_bridge = PostgresBridge(connection, graph_name=graph_name)
        self.conn = connection
        self._conn = connection

    @property
    def graph_name(self) -> str | None:
        """The configured Apache AGE graph name, or None."""
        return self.sync_bridge.graph_name

    @graph_name.setter
    def graph_name(self, value: str | None) -> None:
        if value is not None:
            validate_graph_name(value)
        self.sync_bridge.graph_name = value

    @property
    def connection(self) -> Any:
        """Returns underlying PostgreSQL connection instance."""
        return self.conn

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Asynchronously executes query."""
        return await asyncio.to_thread(self.sync_bridge.execute, statement, parameters)

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Asynchronously streams query results to Polars DataFrame."""
        return await asyncio.to_thread(self.sync_bridge.execute_to_polars, statement, parameters)

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Asynchronously executes bulk ingestion plan."""
        return await asyncio.to_thread(self.sync_bridge.execute_bulk, plan_or_statement, batches)

    async def ping(self) -> bool:
        """Pings PostgreSQL asynchronously to verify connection liveness."""
        return await asyncio.to_thread(self.sync_bridge.ping)

    async def begin_transaction(self) -> AsyncPostgresBridgeTransaction:
        """Begins an explicit PostgreSQL/AGE transaction asynchronously."""
        sync_tx = await asyncio.to_thread(self.sync_bridge.begin_transaction)
        return AsyncPostgresBridgeTransaction(sync_tx)

    async def close(self) -> None:
        """Closes connection asynchronously."""
        await asyncio.to_thread(self.sync_bridge.close)


class AsyncPostgresBridgeTransaction:
    """Asynchronous wrapper around a synchronous PostgreSQL/AGE transaction."""

    def __init__(self, transaction: PostgresBridgeTransaction) -> None:
        """Initializes the async transaction wrapper."""
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
        """PostgreSQL/AGE supports real savepoints."""
        return True

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement inside the transaction."""
        try:
            return await asyncio.to_thread(self._sync.execute, statement, parameters)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement and returns a Polars DataFrame."""
        try:
            return await asyncio.to_thread(self._sync.execute_to_polars, statement, parameters)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan inside the transaction."""
        try:
            return await asyncio.to_thread(self._sync.execute_bulk, plan_or_statement, batches)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def commit(self) -> None:
        """Commits the transaction."""
        try:
            await asyncio.to_thread(self._sync.commit)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def rollback(self) -> None:
        """Rolls the transaction back."""
        try:
            await asyncio.to_thread(self._sync.rollback)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def savepoint(self, name: str) -> None:
        """Creates a real SAVEPOINT."""
        await asyncio.to_thread(self._sync.savepoint, name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rolls back to a real SAVEPOINT."""
        await asyncio.to_thread(self._sync.rollback_to_savepoint, name)

    async def release_savepoint(self, name: str) -> None:
        """Releases a real SAVEPOINT."""
        await asyncio.to_thread(self._sync.release_savepoint, name)


def _adapt_falkordb_value(val: Any) -> Any:
    """Adapts rich parameter values (temporal types, Duration, Point, etc.) for FalkorDB query execution."""
    if isinstance(val, Json):
        return _adapt_falkordb_value(val.value)
    if isinstance(val, uuid.UUID):
        return str(val)
    if isinstance(val, (datetime.date, datetime.time, datetime.datetime)):
        return val.isoformat()
    if isinstance(val, datetime.timedelta):
        from voyager_ogm.types import to_iso_duration

        return to_iso_duration(val)
    if isinstance(val, Duration):
        return val.to_iso()
    if isinstance(val, dict):
        if "__voyager_spatial__" in val:
            sp = val["__voyager_spatial__"]
            unwrapped = unwrap_spatial_param(sp) if isinstance(sp, dict) else sp
            return json.dumps(unwrapped)
        return {k: _adapt_falkordb_value(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_adapt_falkordb_value(x) for x in val]
    return val


def _strip_falkordb_utc(value: Any) -> Any:
    """Removes the spurious UTC tzinfo falkordb-py attaches to localdatetime values."""
    if isinstance(value, datetime.datetime) and value.tzinfo is not None:
        return value.replace(tzinfo=None)
    if isinstance(value, GraphNode):
        return GraphNode._from_trusted(
            node_id=value.id,
            element_id=value.element_id,
            labels=value.labels,
            properties={k: _strip_falkordb_utc(v) for k, v in value.properties.items()},
        )
    if isinstance(value, GraphRelationship):
        return GraphRelationship._from_trusted(
            rel_id=value.id,
            element_id=value.element_id,
            rel_type=value.type,
            start_id=value.start_id,
            end_id=value.end_id,
            properties={k: _strip_falkordb_utc(v) for k, v in value.properties.items()},
            start_element_id=value.start_element_id,
            end_element_id=value.end_element_id,
        )
    if isinstance(value, GraphPath):
        return GraphPath._from_trusted(
            tuple(_strip_falkordb_utc(n) for n in value.nodes),
            tuple(_strip_falkordb_utc(r) for r in value.relationships),
        )
    if isinstance(value, list):
        return [_strip_falkordb_utc(item) for item in value]
    if isinstance(value, dict):
        return {k: _strip_falkordb_utc(v) for k, v in value.items()}
    return value


def _adapt_falkordb_parameters(params: dict[str, Any]) -> dict[str, Any]:
    """Adapts query parameters for FalkorDB execution."""
    return {k: _adapt_falkordb_value(v) for k, v in params.items()}


class FalkorDBBridge:
    """FalkorDB driver bridge for graph execution."""

    def __init__(self, connection: Any, graph_name: str = "voyager_graph") -> None:
        """Initializes FalkorDBBridge.

        A FalkorDB server holds several independent named graphs, and every query runs
        against one of them. Without a name this bridge uses ``voyager_graph``, the same
        default as Voyager's native FalkorDB client, so a session reads and writes the
        same graph on either backend.

        Args:
            connection: FalkorDB client or Graph instance.
            graph_name: Name of the graph to use when a FalkorDB client is provided.
                Ignored when a Graph instance is passed; its own name is used.
        """
        self.graph_name = graph_name
        if hasattr(connection, "select_graph"):
            self.client = connection
            self.graph = connection.select_graph(graph_name)
        elif hasattr(connection, "query"):
            self.graph = connection
            self.client = getattr(connection, "falkordb", None)
            if hasattr(self.graph, "name"):
                self.graph_name = self.graph.name
        else:
            self.graph = connection
            self.client = None

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a Cypher statement on FalkorDB.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.

        Returns:
            List of record dictionaries.
        """
        validate_pagination_parameters(statement, parameters)
        params = _adapt_falkordb_parameters(parameters or {})
        try:
            res = self.graph.query(statement, params)
        except Exception as e:
            stmt_lower = statement.strip().lower()
            leading = " ".join(stmt_lower.split()[:3])
            is_index_ddl = leading.startswith(
                (
                    "create index",
                    "drop index",
                    "create fulltext index",
                    "drop fulltext index",
                    "create vector index",
                    "drop vector index",
                )
            )
            err_msg = str(e).lower()
            if is_index_ddl and ("already indexed" in err_msg or "no such index" in err_msg):
                return BridgeResult([], columns=())
            raise

        if not hasattr(res, "header") or not res.header:
            return BridgeResult([], columns=())

        col_names = [
            str(h[1]) if isinstance(h, (list, tuple)) and len(h) > 1 else str(h) for h in res.header
        ]
        result_set = getattr(res, "result_set", None)
        if not result_set:
            return BridgeResult([], columns=col_names)

        records: list[dict[str, Any]] = []
        for row in result_set:
            rec: dict[str, Any] = {}
            for col, val in zip(col_names, row, strict=False):
                rec[col] = _strip_falkordb_utc(convert_driver_value(val))
            records.append(rec)
        return BridgeResult(records, columns=col_names)

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes Cypher statement and returns Polars DataFrame.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame.
        """
        return _records_to_polars(self.execute(statement, parameters))

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes bulk batch ingestion on FalkorDB.

        Args:
            plan_or_statement: BulkIngestionPlan or query statement.
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
                self.graph.query(statement, _adapt_falkordb_parameters(b))
        else:
            statement = plan_or_statement.statement
            for batch_item in plan_or_statement:
                total_batches += 1
                batch_data = batch_item.parameters.get("batch", [])
                total_records += len(batch_data)
                self.graph.query(
                    batch_item.statement, _adapt_falkordb_parameters(batch_item.parameters)
                )

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    def begin_transaction(self) -> Any:
        """Raises because FalkorDB/Redis cannot provide transactional rollback."""
        raise RuntimeError(
            "FalkorDB does not support transactions: Redis MULTI/EXEC cannot provide "
            "rollback or read-your-writes"
        )

    def ping(self) -> bool:
        """Pings FalkorDB to verify connection liveness."""
        try:
            self.graph.query("RETURN 1")
            return True
        except Exception:
            return False

    def close(self) -> None:
        """Closes FalkorDB connection."""
        if self.client and hasattr(self.client, "close"):
            try:
                self.client.close()
            except Exception:
                pass
        elif hasattr(self.graph, "close"):
            try:
                self.graph.close()
            except Exception:
                pass


class AsyncFalkorDBBridge:
    """Asynchronous FalkorDB driver bridge using asyncio worker threads."""

    def __init__(self, connection: Any, graph_name: str = "voyager_graph") -> None:
        """Initializes AsyncFalkorDBBridge.

        Args:
            connection: FalkorDB client or Graph instance.
            graph_name: Target graph key name if a FalkorDB instance is provided.
        """
        self.sync_bridge = FalkorDBBridge(connection, graph_name=graph_name)

    @property
    def graph_name(self) -> str:
        """Target graph key name."""
        return self.sync_bridge.graph_name

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Asynchronously executes query."""
        return await asyncio.to_thread(self.sync_bridge.execute, statement, parameters)

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Asynchronously streams query results to Polars DataFrame."""
        return await asyncio.to_thread(self.sync_bridge.execute_to_polars, statement, parameters)

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Asynchronously executes bulk ingestion plan."""
        return await asyncio.to_thread(self.sync_bridge.execute_bulk, plan_or_statement, batches)

    async def begin_transaction(self) -> Any:
        """Raises because FalkorDB/Redis cannot provide transactional rollback."""
        raise RuntimeError(
            "FalkorDB does not support transactions: Redis MULTI/EXEC cannot provide "
            "rollback or read-your-writes"
        )

    async def ping(self) -> bool:
        """Pings FalkorDB asynchronously to verify connection liveness."""
        return await asyncio.to_thread(self.sync_bridge.ping)

    async def close(self) -> None:
        """Closes FalkorDB connection asynchronously."""
        await asyncio.to_thread(self.sync_bridge.close)


def _normalize_relate_value(val: Any) -> Any:
    """Normalizes Relate native objects (Node, Relationship, Path, Point, Duration) via the shared converter."""
    return convert_driver_value(val)


def _normalize_relate_record(record: Any) -> dict[str, Any]:
    """Normalizes a single record returned by Relate Cursor or QueryResult."""
    if isinstance(record, dict):
        return {k: _normalize_relate_value(v) for k, v in record.items()}
    if hasattr(record, "items"):
        return {k: _normalize_relate_value(v) for k, v in record.items()}
    return record


def _adapt_relate_value(val: Any) -> Any:
    """Adapts Voyager spatial types and nested structures to Relate-compatible values."""
    from voyager_ogm.types import Point

    if isinstance(val, Point):
        try:
            import relate

            if val.is_3d:
                z_raw = val.z if val.z is not None else val.height
                if z_raw is not None:
                    z_val = float(z_raw)
                    return relate.Point(val.srid, float(val.x), float(val.y), z_val)
            return relate.Point(val.srid, float(val.x), float(val.y))
        except (ImportError, Exception):
            return {"srid": val.srid, "x": float(val.x), "y": float(val.y)}

    if isinstance(val, dict):
        if "__voyager_spatial__" in val:
            sp = val["__voyager_spatial__"]
            if isinstance(sp, dict):
                srid = int(sp.get("srid", 4326))
                x_raw = sp.get("longitude")
                if x_raw is None:
                    x_raw = sp.get("x", 0.0)
                y_raw = sp.get("latitude")
                if y_raw is None:
                    y_raw = sp.get("y", 0.0)
                x = float(x_raw if x_raw is not None else 0.0)
                y = float(y_raw if y_raw is not None else 0.0)
                z = sp.get("height")
                if z is None:
                    z = sp.get("z")
                try:
                    import relate

                    if z is not None:
                        return relate.Point(srid, x, y, float(z))
                    return relate.Point(srid, x, y)
                except (ImportError, Exception):
                    return {"srid": srid, "x": x, "y": y}
        return {k: _adapt_relate_value(v) for k, v in val.items()}

    if isinstance(val, (list, tuple)):
        return [_adapt_relate_value(x) for x in val]

    return val


def _adapt_relate_parameters(params: dict[str, Any]) -> dict[str, Any]:
    return {k: _adapt_relate_value(v) for k, v in params.items()}


def _relate_result_columns(res: Any) -> tuple[str, ...]:
    """Extracts projection column names from a Relate cursor/result object."""
    res_cols = getattr(res, "columns", None)
    if res_cols:
        return tuple(str(c) for c in res_cols)
    desc = getattr(res, "description", None)
    if desc:
        return tuple(str(d[0]) for d in desc)
    return ()


class RelateBridge:
    """Synchronous Relate embedded graph database driver bridge (https://relatedb.ai).

    Connects in-process to Relate databases (via relate-db), enabling zero-network,
    in-memory or single-file (.rl) property graph execution with openCypher / Cypher 25
    and zero-copy Apache Arrow / Polars streaming.
    """

    def __init__(self, connection: Any) -> None:
        """Initializes RelateBridge.

        Args:
            connection: A relate.Connection or relate.Database instance.
        """
        self.con = connection

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a Cypher statement against the embedded Relate database.

        Args:
            statement: Cypher statement string.
            parameters: Query parameters dictionary.

        Returns:
            List of normalized record dictionaries.
        """
        validate_pagination_parameters(statement, parameters)
        params = _adapt_relate_parameters(parameters or {})
        res = self.con.execute(statement, params)
        cols = _relate_result_columns(res)
        if hasattr(res, "fetchall"):
            rows = res.fetchall()
        elif hasattr(res, "data"):
            rows = res.data
        else:
            rows = list(res)
        normalized = [_normalize_relate_record(row) for row in rows]
        if not cols:
            cols = _infer_record_columns(normalized)
        return BridgeResult(normalized, columns=cols)

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a Cypher statement and streams results directly into a Polars DataFrame via zero-copy Arrow IPC.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.

        Returns:
            Polars DataFrame containing result records.
        """
        params = _adapt_relate_parameters(parameters or {})
        if hasattr(self.con, "execute"):
            res = self.con.execute(statement, params)
            if hasattr(res, "to_arrow"):
                try:
                    tbl = res.to_arrow()
                    df_out = pl.from_arrow(tbl)
                    if isinstance(df_out, pl.DataFrame):
                        return df_out
                    return pl.DataFrame(df_out)
                except Exception:
                    pass
            cols = _relate_result_columns(res)
            records = getattr(res, "fetchall", lambda: getattr(res, "data", []))()
            normalized = [_normalize_relate_record(r) for r in records]
            return _records_to_polars(BridgeResult(normalized, columns=cols))
        return pl.DataFrame()

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes bulk batch ingestion against the embedded Relate engine.

        Args:
            plan_or_statement: BulkIngestionPlan or Cypher statement.
            batches: Batch parameter sequence.

        Returns:
            BulkExecutionResult summary metrics.
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
                self.con.execute(statement, _adapt_relate_parameters(b))
        else:
            statement = plan_or_statement.statement
            for batch_item in plan_or_statement:
                total_batches += 1
                batch_data = batch_item.parameters.get("batch", [])
                total_records += len(batch_data)
                self.con.execute(
                    batch_item.statement, _adapt_relate_parameters(batch_item.parameters)
                )

        return BulkExecutionResult(
            total_batches=total_batches,
            total_records=total_records,
            duration_seconds=time.perf_counter() - start_time,
            statement=statement,
        )

    def ping(self) -> bool:
        """Pings Relate to verify liveness."""
        try:
            self.con.execute("RETURN 1")
            return True
        except Exception:
            return False

    def begin_transaction(self) -> RelateBridgeTransaction:
        """Begins an explicit Relate transaction on the bridge connection."""
        return RelateBridgeTransaction(self)

    def close(self) -> None:
        """Closes the underlying Relate database connection."""
        if hasattr(self.con, "close"):
            self.con.close()


class RelateBridgeTransaction:
    """Explicit Relate transaction driven by the connection's autocommit flag.

    Relate is DB-API based: setting ``autocommit = False`` groups statements
    into a transaction that ``commit()`` / ``rollback()`` ends. The original
    autocommit value is restored on every completion path. A failing statement
    leaves the engine transaction in a failed state, so further statements are
    rejected until rollback.
    """

    _savepoint_error_message = (
        "Relate transactions do not support savepoints; "
        "use Apache AGE/PostgreSQL for real savepoints"
    )

    def __init__(self, bridge: RelateBridge) -> None:
        """Starts an explicit transaction on the bridge's connection.

        Args:
            bridge: Parent Relate bridge.

        Raises:
            RuntimeError: If the connection already has autocommit disabled and
                an open transaction with pending work. That work belongs to the
                caller, so it is never silently committed or discarded.
        """
        self._bridge = bridge
        conn = bridge.con
        self._original_autocommit = getattr(conn, "autocommit", True)
        if self._original_autocommit is False:
            if self._transaction_open(conn):
                raise RuntimeError(
                    "cannot begin a Relate transaction: the connection already has an "
                    "explicit transaction with pending work; commit or roll it back first"
                )
        else:
            try:
                conn.autocommit = False
            except Exception as exc:
                self._restore_autocommit()
                raise RuntimeError(f"cannot begin a Relate transaction: {exc}") from exc
        self._state = "ACTIVE"

    @property
    def state(self) -> str:
        """Returns the lifecycle state ('ACTIVE', 'FAILED', 'COMMITTED', 'ROLLED_BACK')."""
        return self._state

    @property
    def is_active(self) -> bool:
        """Returns whether the transaction accepts statements."""
        return self._state == "ACTIVE"

    @property
    def supports_savepoints(self) -> bool:
        """Relate has no savepoints."""
        return False

    @staticmethod
    def _transaction_open(conn: Any) -> bool:
        """Returns whether the Relate connection has an open transaction."""
        probe = getattr(conn, "_transaction_open", None)
        if callable(probe):
            try:
                return bool(probe())
            except Exception:
                return True
        # Without a public probe, assume pending work rather than risk it.
        return True

    def _restore_autocommit(self) -> None:
        """Restores the connection's original autocommit mode (best effort)."""
        original = self._original_autocommit
        if original is None:
            return
        conn = self._bridge.con
        try:
            if bool(getattr(conn, "autocommit", original)) == bool(original):
                return
            if original and self._transaction_open(conn):
                # Relate's autocommit setter commits; end any failed/open
                # transaction first so the setter cannot raise or commit it.
                try:
                    conn.rollback()
                except Exception:
                    pass
            conn.autocommit = original
        except Exception:
            try:
                if original and self._transaction_open(conn):
                    conn.rollback()
            except Exception:
                pass
            try:
                conn.autocommit = original
            except Exception:
                pass

    def _mark_failed(self) -> None:
        """Marks the transaction aborted after a statement/driver error."""
        self._state = "FAILED"

    def _ensure_executable(self) -> None:
        """Rejects further statements once the transaction has failed."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement inside the transaction."""
        self._ensure_executable()
        try:
            return self._bridge.execute(statement, parameters)
        except BaseException:
            self._mark_failed()
            raise

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement and returns a Polars DataFrame."""
        self._ensure_executable()
        try:
            return self._bridge.execute_to_polars(statement, parameters)
        except BaseException:
            self._mark_failed()
            raise

    def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan inside the transaction."""
        self._ensure_executable()
        try:
            return self._bridge.execute_bulk(plan_or_statement, batches)
        except BaseException:
            self._mark_failed()
            raise

    def commit(self) -> None:
        """Commits the transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back instead of committing")
        if not self.is_active:
            raise ValueError("Cannot commit inactive transaction")
        try:
            self._bridge.con.commit()
            self._state = "COMMITTED"
        except BaseException:
            self._mark_failed()
            raise
        finally:
            self._restore_autocommit()

    def rollback(self) -> None:
        """Rolls the transaction back."""
        if self._state not in ("ACTIVE", "FAILED"):
            raise ValueError("Cannot rollback inactive transaction")
        try:
            self._bridge.con.rollback()
            self._state = "ROLLED_BACK"
        except BaseException:
            self._mark_failed()
            raise
        finally:
            self._restore_autocommit()

    def savepoint(self, name: str) -> None:
        """Raises because Relate does not support savepoints."""
        raise RuntimeError(self._savepoint_error_message)

    def rollback_to_savepoint(self, name: str) -> None:
        """Raises because Relate does not support savepoints."""
        raise RuntimeError(self._savepoint_error_message)

    def release_savepoint(self, name: str) -> None:
        """Raises because Relate does not support savepoints."""
        raise RuntimeError(self._savepoint_error_message)


class AsyncRelateBridge:
    """Asynchronous Relate driver bridge.

    Because embedded Relate connections are thread-affine (DB-API threadsafety = 1),
    queries are executed cooperatively on the active event loop thread without
    violating thread-affinity boundaries.
    """

    def __init__(self, connection: Any) -> None:
        """Initializes AsyncRelateBridge.

        Args:
            connection: Relate connection or database instance.
        """
        self.sync_bridge = RelateBridge(connection)

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Asynchronously executes query on Relate."""
        await asyncio.sleep(0)
        return self.sync_bridge.execute(statement, parameters)

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Asynchronously streams query results to Polars DataFrame."""
        await asyncio.sleep(0)
        return self.sync_bridge.execute_to_polars(statement, parameters)

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Asynchronously executes bulk ingestion plan."""
        await asyncio.sleep(0)
        return self.sync_bridge.execute_bulk(plan_or_statement, batches)

    async def ping(self) -> bool:
        """Pings Relate asynchronously to verify connection liveness."""
        await asyncio.sleep(0)
        return self.sync_bridge.ping()

    async def begin_transaction(self) -> AsyncRelateBridgeTransaction:
        """Begins an explicit Relate transaction on the event-loop thread."""
        await asyncio.sleep(0)
        return AsyncRelateBridgeTransaction(self.sync_bridge.begin_transaction())

    async def close(self) -> None:
        """Closes Relate connection asynchronously."""
        await asyncio.sleep(0)
        self.sync_bridge.close()


class AsyncRelateBridgeTransaction:
    """Asynchronous wrapper around a synchronous Relate transaction.

    Relate connections are thread-affine, so every call stays on the event-loop
    thread, matching :class:`AsyncRelateBridge`.
    """

    def __init__(self, transaction: RelateBridgeTransaction) -> None:
        """Initializes the async transaction wrapper."""
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
        """Relate has no savepoints."""
        return False

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Executes a statement inside the transaction."""
        await asyncio.sleep(0)
        try:
            return self._sync.execute(statement, parameters)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Executes a statement and returns a Polars DataFrame."""
        await asyncio.sleep(0)
        try:
            return self._sync.execute_to_polars(statement, parameters)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def execute_bulk(
        self,
        plan_or_statement: BulkIngestionPlan | str,
        batches: Sequence[dict[str, Any]] | None = None,
    ) -> BulkExecutionResult:
        """Executes a bulk plan inside the transaction."""
        await asyncio.sleep(0)
        try:
            return self._sync.execute_bulk(plan_or_statement, batches)
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def commit(self) -> None:
        """Commits the transaction."""
        await asyncio.sleep(0)
        try:
            self._sync.commit()
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def rollback(self) -> None:
        """Rolls the transaction back."""
        await asyncio.sleep(0)
        try:
            self._sync.rollback()
        except asyncio.CancelledError:
            self._sync._mark_failed()
            raise

    async def savepoint(self, name: str) -> None:
        """Raises because Relate does not support savepoints."""
        raise RuntimeError(self._sync._savepoint_error_message)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Raises because Relate does not support savepoints."""
        raise RuntimeError(self._sync._savepoint_error_message)

    async def release_savepoint(self, name: str) -> None:
        """Raises because Relate does not support savepoints."""
        raise RuntimeError(self._sync._savepoint_error_message)


# Optional namespace prefixes accepted in front of a connection URI, matching the
# native URI parser (voyager-drivers `ParsedUri::parse`). They select the backend
# for a Session; the Python driver bridge ignores them and uses the URI after them.
_URI_NAMESPACE_PREFIXES = (
    "voyager+native:",
    "voyager+bridge:",
    "voyager-drivers:",
    "voyager:",
    "voy:",
    "vn:",
)


def _strip_uri_namespace_prefix(uri: str) -> str:
    """Removes an optional Voyager namespace prefix such as ``vn:`` or ``voy:``."""
    stripped = uri.strip()
    for prefix in _URI_NAMESPACE_PREFIXES:
        if stripped.startswith(prefix):
            return stripped[len(prefix) :].strip()
    return stripped


_BRIDGE_REGISTRY: list[tuple[Callable[[Any], bool], type, bool]] = []


def register_bridge(
    predicate_or_type: type | Callable[[Any], bool],
    bridge_class: type,
    is_async: bool = False,
) -> None:
    """Registers a new database driver adapter into the global bridge registry.

    Allows third-party and custom drivers to be auto-detected by Session.

    Args:
        predicate_or_type: Driver class type or matcher predicate function.
        bridge_class: Adapter class to instantiate.
        is_async: Flag indicating whether this adapter implements AsyncDatabaseBridge.
    """
    matcher: Callable[[Any], bool]
    if isinstance(predicate_or_type, type):
        target_cls = predicate_or_type

        def _matcher(obj: Any) -> bool:
            return isinstance(obj, target_cls)

        matcher = _matcher
    else:
        matcher = predicate_or_type

    _BRIDGE_REGISTRY.insert(0, (matcher, bridge_class, is_async))


def _is_neo4j_sync_driver(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "neo4j" in type_name and "Async" not in type_name and hasattr(obj, "session")


def _is_neo4j_async_driver(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "neo4j" in type_name and "Async" in type_name and hasattr(obj, "session")


def _is_duckdb_conn(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "duckdb" in type_name and hasattr(obj, "execute")


def _is_postgres_conn(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "psycopg" in type_name and hasattr(obj, "cursor")


def _is_falkordb_conn(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    return "falkordb" in type_name and (hasattr(obj, "query") or hasattr(obj, "select_graph"))


def _is_relate_conn(obj: Any) -> bool:
    type_name = f"{type(obj).__module__}.{type(obj).__qualname__}"
    if "relate" in type_name and hasattr(obj, "execute"):
        return True
    if hasattr(obj, "execute_arrow") and hasattr(obj, "begin_transaction"):
        return True
    return False


register_bridge(_is_neo4j_sync_driver, Neo4jBoltBridge, is_async=False)
register_bridge(_is_neo4j_async_driver, AsyncNeo4jBoltBridge, is_async=True)
register_bridge(_is_duckdb_conn, DuckDbBridge, is_async=False)
register_bridge(_is_duckdb_conn, AsyncDuckDbBridge, is_async=True)
register_bridge(_is_postgres_conn, PostgresBridge, is_async=False)
register_bridge(_is_postgres_conn, AsyncPostgresBridge, is_async=True)
register_bridge(_is_falkordb_conn, FalkorDBBridge, is_async=False)
register_bridge(_is_falkordb_conn, AsyncFalkorDBBridge, is_async=True)
register_bridge(_is_relate_conn, RelateBridge, is_async=False)
register_bridge(_is_relate_conn, AsyncRelateBridge, is_async=True)


@overload
def create_bridge(
    driver_or_connection: Any = None,
    is_async: Literal[False] = False,
    fallback: str | None = None,
    *,
    auth: tuple[str, str] | Any | None = None,
    graph_name: str | None = None,
) -> DatabaseBridge: ...


@overload
def create_bridge(
    driver_or_connection: Any = None,
    is_async: Literal[True] = True,
    fallback: str | None = None,
    *,
    auth: tuple[str, str] | Any | None = None,
    graph_name: str | None = None,
) -> AsyncDatabaseBridge: ...


@overload
def create_bridge(
    driver_or_connection: Any = None,
    is_async: bool = False,
    fallback: str | None = None,
    *,
    auth: tuple[str, str] | Any | None = None,
    graph_name: str | None = None,
) -> DatabaseBridge | AsyncDatabaseBridge: ...


def create_bridge(
    driver_or_connection: Any = None,
    is_async: bool = False,
    fallback: str | None = None,
    *,
    auth: tuple[str, str] | Any | None = None,
    graph_name: str | None = None,
) -> DatabaseBridge | AsyncDatabaseBridge:
    """Creates or adapts a database bridge from a user-supplied driver, connection, or URI.

    Args:
        driver_or_connection: Database driver, connection instance, MockBridge, or custom bridge.
        is_async: Whether an async bridge is required.
        fallback: Fallback strategy when driver import fails or URI scheme is unrecognized.
            Pass ``fallback="mock"`` to explicitly opt into returning an in-memory MockBridge
            for testing, mocking, or demonstrations.
        auth: Explicit authentication credentials. For Bolt schemes, accepts a (username, password)
            tuple or a neo4j auth token (e.g. bearer_auth). For PostgreSQL/AGE schemes, accepts
            a (username, password) tuple passed as connect kwargs. For FalkorDB/Redis schemes,
            accepts a (username, password) tuple. Explicit auth overrides credentials present
            in the URI.
        graph_name: Optional graph name, validated as a safe identifier. For Apache AGE and
            PostgreSQL it selects the AGE graph. For FalkorDB it selects one of the
            server's named graphs; without it, the graph comes from the URI path
            (``redis://host:6379/<graph>``) or ``?graph=``, and otherwise defaults to
            ``voyager_graph``, the native FalkorDB client's default.

    Returns:
        Configured database bridge adapter.

    Raises:
        ValueError: If no driver/URI was provided, or if the URI scheme is unrecognized and
            fallback="mock" was not explicitly requested, or if graph_name is invalid.
        ImportError: If the official driver package required for the URI scheme is not installed
            and fallback="mock" was not explicitly requested.
        TypeError: If the driver_or_connection object type is unsupported, or if an invalid
            auth type was provided for the scheme.
    """
    if graph_name is not None:
        validate_graph_name(graph_name)

    if driver_or_connection is None:
        if fallback == "mock":
            return (
                AsyncMockBridge(graph_name=graph_name)
                if is_async
                else MockBridge(graph_name=graph_name)
            )
        raise ValueError(
            "No database driver, connection, or URI provided. "
            "Provide a valid driver instance, connection URI, or pass fallback='mock' (or use a 'mock://' URI) for offline testing."
        )

    if isinstance(driver_or_connection, MockBridge):
        if graph_name is not None:
            driver_or_connection.graph_name = graph_name
        return (
            AsyncMockBridge(driver_or_connection, graph_name=driver_or_connection.graph_name)
            if is_async
            else driver_or_connection
        )
    if isinstance(driver_or_connection, AsyncMockBridge):
        if graph_name is not None:
            driver_or_connection.graph_name = graph_name
        return driver_or_connection if is_async else driver_or_connection.sync_mock

    if (
        is_async
        and isinstance(driver_or_connection, AsyncDatabaseBridge)
        and inspect.iscoroutinefunction(getattr(driver_or_connection, "execute", None))
    ):
        if graph_name is not None and hasattr(driver_or_connection, "graph_name"):
            driver_or_connection.graph_name = graph_name
        return driver_or_connection
    if (
        not is_async
        and isinstance(driver_or_connection, DatabaseBridge)
        and not inspect.iscoroutinefunction(getattr(driver_or_connection, "execute", None))
    ):
        if graph_name is not None and hasattr(driver_or_connection, "graph_name"):
            driver_or_connection.graph_name = graph_name
        return driver_or_connection

    for matcher, bridge_cls, reg_is_async in _BRIDGE_REGISTRY:
        if reg_is_async == is_async and matcher(driver_or_connection):
            bridge_inst = bridge_cls(driver_or_connection)
            if graph_name is not None and hasattr(bridge_inst, "graph_name"):
                bridge_inst.graph_name = graph_name
            return bridge_inst

    if is_async:
        for matcher, bridge_cls, reg_is_async in _BRIDGE_REGISTRY:
            if not reg_is_async and matcher(driver_or_connection):
                sync_inst = bridge_cls(driver_or_connection)
                if isinstance(sync_inst, DuckDbBridge):
                    return AsyncDuckDbBridge(driver_or_connection)
                elif isinstance(sync_inst, FalkorDBBridge):
                    return AsyncFalkorDBBridge(driver_or_connection)
                elif isinstance(sync_inst, RelateBridge):
                    return AsyncRelateBridge(driver_or_connection)
                elif isinstance(sync_inst, PostgresBridge):
                    async_inst = AsyncPostgresBridge(driver_or_connection)
                    if graph_name is not None:
                        async_inst.graph_name = graph_name
                    return async_inst

    # If a database connection URI string was provided, attempt to auto-instantiate the official driver
    if isinstance(driver_or_connection, str):
        driver_or_connection = _strip_uri_namespace_prefix(driver_or_connection)
        if driver_or_connection.startswith("mock://"):
            from urllib.parse import parse_qs, urlsplit

            parsed_mock = urlsplit(driver_or_connection)
            qs_mock = parse_qs(parsed_mock.query)
            uri_mock_graph = qs_mock.get("graph", qs_mock.get("graph_name", [None]))[0]
            resolved_mock_graph = graph_name if graph_name is not None else uri_mock_graph
            if resolved_mock_graph is not None:
                validate_graph_name(resolved_mock_graph)
            return (
                AsyncMockBridge(graph_name=resolved_mock_graph)
                if is_async
                else MockBridge(graph_name=resolved_mock_graph)
            )

        if "://" not in driver_or_connection:
            if fallback == "mock":
                return AsyncMockBridge() if is_async else MockBridge()
            raise ValueError(
                f"Invalid connection URI: '{mask_uri_password(driver_or_connection)}'. Expected a valid URI scheme (e.g. 'bolt://', 'duckdb://', 'postgresql://', 'falkordb://', or 'mock://')."
            )

        scheme = driver_or_connection.split("://", 1)[0].lower()
        if scheme in (
            "bolt",
            "neo4j",
            "memgraph",
            "bolt+s",
            "neo4j+s",
            "memgraph+s",
            "bolt+ssc",
            "neo4j+ssc",
            "memgraph+ssc",
        ):
            try:
                import neo4j
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'neo4j' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    "Install the optional extra with: "
                    "pip install \"voyager-ogm[neo4j]\", or pass fallback='mock' for offline testing."
                ) from e
            from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit

            parsed_bolt = urlsplit(driver_or_connection)
            netloc = parsed_bolt.netloc
            hostport = netloc.rsplit("@", 1)[-1]
            bolt_clean_uri = urlunsplit(
                (
                    parsed_bolt.scheme,
                    hostport,
                    parsed_bolt.path,
                    "",
                    "",
                )
            )
            bolt_kwargs: dict[str, Any] = {}
            if parsed_bolt.query:
                qs = parse_qs(parsed_bolt.query)
                if "connect_timeout" in qs:
                    try:
                        bolt_kwargs["connection_timeout"] = float(qs["connect_timeout"][0])
                    except ValueError:
                        pass
                if "connection_timeout" in qs:
                    try:
                        bolt_kwargs["connection_timeout"] = float(qs["connection_timeout"][0])
                    except ValueError:
                        pass

            effective_auth = None
            if auth is not None:
                effective_auth = auth
            elif "@" in netloc:
                userinfo = netloc.rsplit("@", 1)[0]
                if ":" in userinfo:
                    u_raw, p_raw = userinfo.split(":", 1)
                    effective_auth = (unquote(u_raw), unquote(p_raw))
                else:
                    effective_auth = (unquote(userinfo), "")

            if effective_auth is not None:
                bolt_kwargs["auth"] = effective_auth

            if is_async and hasattr(neo4j, "AsyncGraphDatabase"):
                async_drv = neo4j.AsyncGraphDatabase.driver(bolt_clean_uri, **bolt_kwargs)
                return AsyncNeo4jBoltBridge(async_drv)
            elif hasattr(neo4j, "GraphDatabase"):
                sync_drv = neo4j.GraphDatabase.driver(bolt_clean_uri, **bolt_kwargs)
                return Neo4jBoltBridge(sync_drv)
            else:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise RuntimeError(
                    f"Unable to instantiate neo4j driver from '{mask_uri_password(driver_or_connection)}'."
                )
        elif scheme == "duckdb":
            if auth is not None:
                raise TypeError("DuckDB does not accept auth credentials")
            try:
                import duckdb
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'duckdb' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    "Install the optional extra with: "
                    "pip install \"voyager-ogm[duckdb]\", or pass fallback='mock' for offline testing."
                ) from e
            path = driver_or_connection.replace("duckdb://", "") or ":memory:"
            duck_conn = duckdb.connect(path)
            duck_bridge = DuckDbBridge(duck_conn)
            return AsyncDuckDbBridge(duck_conn) if is_async else duck_bridge
        elif scheme in (
            "postgresql",
            "postgres",
            "age",
            "postgresql+s",
            "postgres+s",
            "age+s",
            "postgresql+ssc",
            "postgres+ssc",
            "age+ssc",
        ):
            try:
                import psycopg
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'psycopg' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    "Install the optional extra with: "
                    "pip install \"voyager-ogm[age]\", or pass fallback='mock' for offline testing."
                ) from e
            from urllib.parse import parse_qs, parse_qsl, urlencode, urlsplit, urlunsplit

            is_age_scheme = scheme in ("age", "age+s", "age+ssc")
            is_ssl_scheme = scheme in (
                "postgresql+s",
                "postgres+s",
                "age+s",
                "postgresql+ssc",
                "postgres+ssc",
                "age+ssc",
            )

            parsed_pg = urlsplit(driver_or_connection)
            target_scheme = "postgresql" if (is_age_scheme or is_ssl_scheme) else parsed_pg.scheme
            qs = parse_qs(parsed_pg.query)
            uri_graph = qs.get("graph", qs.get("graph_name", [None]))[0]
            resolved_graph = graph_name if graph_name is not None else uri_graph
            if resolved_graph is not None:
                validate_graph_name(resolved_graph)

            # psycopg rejects unknown libpq keywords, so the graph selectors are
            # consumed here and must not reach the connection string.
            pg_query_pairs = [
                (key, value)
                for key, value in parse_qsl(parsed_pg.query, keep_blank_values=True)
                if key not in ("graph", "graph_name")
            ]
            if is_ssl_scheme and "sslmode" not in qs and "ssl" not in qs:
                pg_query_pairs.append(("sslmode", "require"))
            new_query = urlencode(pg_query_pairs)

            clean_pg_uri = urlunsplit(
                (target_scheme, parsed_pg.netloc, parsed_pg.path, new_query, parsed_pg.fragment)
            )

            pg_kwargs: dict[str, Any] = {"autocommit": True}
            if is_ssl_scheme:
                pg_kwargs["sslmode"] = "require"

            if auth is not None:
                if not isinstance(auth, tuple) or len(auth) != 2:
                    raise TypeError(
                        f"Postgres/AGE auth must be a (username, password) tuple, got {type(auth).__name__}"
                    )
                pg_kwargs["user"] = auth[0]
                pg_kwargs["password"] = auth[1]

            pg_conn = psycopg.connect(clean_pg_uri, **pg_kwargs)
            setup_postgres_age(pg_conn, force=is_age_scheme)
            pg_bridge = PostgresBridge(pg_conn, graph_name=resolved_graph)
            return (
                AsyncPostgresBridge(pg_conn, graph_name=resolved_graph) if is_async else pg_bridge
            )
        elif scheme in (
            "falkordb",
            "falkordbs",
            "falkor",
            "redis",
            "rediss",
            "valkey",
            "valkeys",
        ):
            try:
                from falkordb import FalkorDB
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'falkordb' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    f"Install it with 'pip install falkordb', or pass fallback='mock' for offline testing."
                ) from e
            from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

            parsed_url = urlsplit(driver_or_connection)
            redis_scheme = "rediss" if scheme in ("rediss", "falkordbs", "valkeys") else "redis"
            qs = parse_qs(parsed_url.query)
            redis_qs = {}
            for k, vals in qs.items():
                if k == "connect_timeout":
                    redis_qs["socket_connect_timeout"] = vals[0]
                elif k in (
                    "socket_timeout",
                    "socket_connect_timeout",
                    "password",
                    "db",
                    "username",
                    "decode_responses",
                    "ssl",
                    "ssl_cert_reqs",
                ):
                    redis_qs[k] = vals[0]

            falkor_kwargs: dict[str, Any] = {}
            if auth is not None:
                if not isinstance(auth, tuple) or len(auth) != 2:
                    raise TypeError(
                        f"FalkorDB/Redis auth must be a (username, password) tuple, got {type(auth).__name__}"
                    )
                falkor_kwargs["username"] = auth[0]
                falkor_kwargs["password"] = auth[1]

            clean_url = urlunsplit(
                (
                    redis_scheme,
                    parsed_url.netloc,
                    parsed_url.path,
                    urlencode(redis_qs),
                    parsed_url.fragment,
                )
            )
            uri_graph = qs.get("graph", qs.get("graph_name", [None]))[0]
            if not uri_graph and parsed_url.path and parsed_url.path != "/":
                uri_graph = parsed_url.path.lstrip("/")
            resolved_graph = (
                graph_name
                if graph_name is not None
                else (uri_graph if uri_graph else "voyager_graph")
            )
            if resolved_graph is not None:
                validate_graph_name(resolved_graph)
            parsed = FalkorDB.from_url(clean_url, **falkor_kwargs)
            f_bridge = FalkorDBBridge(parsed, graph_name=resolved_graph)
            return AsyncFalkorDBBridge(parsed, graph_name=resolved_graph) if is_async else f_bridge
        elif scheme in ("relate", "relatedb"):
            if auth is not None:
                raise TypeError("Relate does not accept auth credentials")
            try:
                import relate
            except ImportError as e:
                if fallback == "mock":
                    return AsyncMockBridge() if is_async else MockBridge()
                raise ImportError(
                    f"The 'relate-db' Python package is required to connect to URI '{mask_uri_password(driver_or_connection)}'. "
                    "Install the optional extra with: "
                    "pip install \"voyager-ogm[relate]\", or pass fallback='mock' for offline testing."
                ) from e
            path = driver_or_connection.split("://", 1)[1]
            target_path = ":memory:" if (not path or path == ":memory:") else path
            relate_conn = relate.connect(target_path)
            r_bridge = RelateBridge(relate_conn)
            return AsyncRelateBridge(relate_conn) if is_async else r_bridge
        else:
            if fallback == "mock":
                return AsyncMockBridge() if is_async else MockBridge()
            raise ValueError(
                f"No bridge available for URI '{mask_uri_password(driver_or_connection)}' (unrecognized scheme: '{scheme}'). "
                f"Tried adapters: bolt, duckdb, postgres, falkordb, relate. "
                f"Install required driver or check the URI, or pass fallback='mock' (or use a 'mock://' URI) for offline testing."
            )

    if fallback == "mock":
        return AsyncMockBridge() if is_async else MockBridge()
    raise TypeError(
        f"Unsupported database driver or connection object of type {type(driver_or_connection).__name__}. "
        f"Expected a recognized driver connection instance, DatabaseBridge, or URI string."
    )
