"""Shared bridge protocols, result containers, and validation helpers."""

from __future__ import annotations

import asyncio
import inspect
import operator
import re
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

import polars as pl

from voyager_ogm.ingestion import BulkIngestionPlan

_PAGINATION_PARAMETER_PATTERN = re.compile(
    r"\b(?:SKIP|OFFSET|LIMIT)\s+\$([A-Za-z_][A-Za-z0-9_]*)\b", re.IGNORECASE
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
    # Most OLTP statements bind no pagination at all. A case-insensitive
    # substring check avoids the regular-expression scan on that hot path.
    upper = statement.upper()
    if "LIMIT" not in upper and "SKIP" not in upper and "OFFSET" not in upper:
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


class BridgeStreamChunk:
    """One bounded batch of bridge rows, with lazy dictionary or Arrow output.

    DuckDB streams keep the raw Arrow record batch so ``to_dicts()`` reuses the
    driver's Arrow-to-Python decoding, while ``to_record_batch()`` returns the
    canonical Voyager Arrow batch. Buffered and driver-cursor bridges store row
    dictionaries directly.
    """

    def __init__(
        self,
        records: Sequence[dict[str, Any]] | None = None,
        *,
        columns: Sequence[str] = (),
        arrow_batch: Any = None,
        decode_records: Callable[[], Sequence[dict[str, Any]]] | None = None,
    ) -> None:
        """Initializes a stream chunk.

        Args:
            records: Row dictionaries, when already decoded.
            columns: Projected column names.
            arrow_batch: Canonical Arrow record batch, when the chunk is Arrow-backed.
            decode_records: Lazy dictionary decoder for Arrow-backed chunks.
        """
        self._records = list(records) if records is not None else None
        self._columns = tuple(str(column) for column in columns)
        self._arrow_batch = arrow_batch
        self._decode_records = decode_records

    @property
    def columns(self) -> list[str]:
        """Projected column names."""
        return list(self._columns)

    @property
    def num_rows(self) -> int:
        """Number of rows in this chunk."""
        if self._records is not None:
            return len(self._records)
        if self._arrow_batch is not None:
            return int(self._arrow_batch.num_rows)
        return 0

    def to_dicts(self) -> list[dict[str, Any]]:
        """Returns this chunk as row dictionaries."""
        if self._records is None:
            if self._decode_records is not None:
                self._records = list(self._decode_records())
            elif self._arrow_batch is not None:
                import pyarrow as pa

                self._records = pa.Table.from_batches([self._arrow_batch]).to_pylist()
            else:
                self._records = []
        return self._records

    def to_record_batch(self) -> Any:
        """Returns this chunk as a single canonical ``pyarrow.RecordBatch``."""
        import pyarrow as pa

        if self._arrow_batch is not None:
            return self._arrow_batch
        records = self.to_dicts()
        if not records:
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=pa.null()) for _ in self._columns], names=list(self._columns)
            )
        return pa.RecordBatch.from_pylist(records)


class BridgeStream:
    """Synchronous pull-based bridge stream base class.

    Subclasses pull rows from a driver cursor; ``close()`` runs the owner's
    cleanup callback (closing a driver session or named cursor). ``__del__``
    only clears local state and calls an optional garbage-collection callback:
    it never performs network or blocking I/O.
    """

    def __init__(
        self,
        *,
        columns: Sequence[str],
        is_server_streaming: bool,
        close_callback: Callable[[], Any] | None = None,
        gc_callback: Callable[[], Any] | None = None,
    ) -> None:
        """Initializes the stream base.

        Args:
            columns: Projected column names.
            is_server_streaming: Whether the driver pulls rows from a server cursor.
            close_callback: Cleanup run on exhaustion or explicit close.
            gc_callback: Non-blocking local cleanup run when the stream is dropped.
        """
        self._columns = tuple(str(column) for column in columns)
        self._is_server_streaming = bool(is_server_streaming)
        self._close_callback = close_callback
        self._gc_callback = gc_callback
        self._closed = False

    @property
    def columns(self) -> list[str]:
        """Projected column names."""
        return list(self._columns)

    @property
    def is_server_streaming(self) -> bool:
        """Whether the driver pulls rows incrementally from a server cursor."""
        return self._is_server_streaming

    @property
    def is_closed(self) -> bool:
        """Whether the stream is exhausted or explicitly closed."""
        return self._closed

    def next_batch(self) -> BridgeStreamChunk | None:
        """Pulls the next chunk, or returns ``None`` once exhausted."""
        raise NotImplementedError

    def close(self) -> None:
        """Closes the stream and runs the owner's cleanup callback. Idempotent."""
        if self._closed:
            return
        self._closed = True
        callback = self._close_callback
        self._close_callback = None
        if callback is not None:
            callback()

    def __del__(self) -> None:
        """Clears local state when a stream is dropped without being closed."""
        if getattr(self, "_closed", True):
            return
        self._closed = True
        self._close_callback = None
        callback = getattr(self, "_gc_callback", None)
        self._gc_callback = None
        if callback is not None:
            try:
                callback()
            except Exception:
                pass


class BufferedBridgeStream(BridgeStream):
    """Client-side chunk slicer over an already-materialized bridge result."""

    def __init__(
        self,
        records: Sequence[dict[str, Any]],
        columns: Sequence[str],
        fetch_size: int = 1_000,
        *,
        is_server_streaming: bool = False,
        close_callback: Callable[[], Any] | None = None,
        gc_callback: Callable[[], Any] | None = None,
    ) -> None:
        """Initializes a buffered stream over ``records``.

        Args:
            records: Materialized row dictionaries.
            columns: Projected column names.
            fetch_size: Maximum rows per yielded chunk.
            is_server_streaming: Always ``False`` for buffered slicing.
            close_callback: Cleanup run on exhaustion or explicit close.
            gc_callback: Non-blocking local cleanup run when the stream is dropped.
        """
        super().__init__(
            columns=columns,
            is_server_streaming=is_server_streaming,
            close_callback=close_callback,
            gc_callback=gc_callback,
        )
        self._records = list(records)
        self._fetch_size = max(1, int(fetch_size))
        self._offset = 0

    @classmethod
    def from_records(
        cls,
        records: Sequence[dict[str, Any]],
        fetch_size: int = 1_000,
        *,
        is_server_streaming: bool = False,
        close_callback: Callable[[], Any] | None = None,
        gc_callback: Callable[[], Any] | None = None,
    ) -> BufferedBridgeStream:
        """Builds a buffered stream, reading column metadata from the result when present.

        Args:
            records: Materialized row dictionaries.
            fetch_size: Maximum rows per yielded chunk.
            is_server_streaming: Whether the source used a server cursor.
            close_callback: Cleanup run on exhaustion or explicit close.
            gc_callback: Non-blocking local cleanup run when the stream is dropped.

        Returns:
            A buffered stream over ``records``.
        """
        columns = getattr(records, "columns", None)
        if columns is None:
            columns = _infer_record_columns(records)
        return cls(
            records,
            columns,
            fetch_size,
            is_server_streaming=is_server_streaming,
            close_callback=close_callback,
            gc_callback=gc_callback,
        )

    def next_batch(self) -> BridgeStreamChunk | None:
        """Returns the next slice of at most ``fetch_size`` rows."""
        if self._closed:
            return None
        if self._offset >= len(self._records):
            self.close()
            return None
        end = min(len(self._records), self._offset + self._fetch_size)
        chunk = BridgeStreamChunk(self._records[self._offset : end], columns=self._columns)
        self._offset = end
        return chunk


class GeneratorBridgeStream(BridgeStream):
    """Bridge stream backed by a generator of chunks (driver cursors, server streams)."""

    def __init__(
        self,
        chunks: Iterator[BridgeStreamChunk],
        *,
        columns: Sequence[str],
        is_server_streaming: bool,
        close_callback: Callable[[], Any] | None = None,
        gc_callback: Callable[[], Any] | None = None,
    ) -> None:
        """Initializes a generator-backed stream.

        Args:
            chunks: Iterator yielding decoded chunks.
            columns: Projected column names.
            is_server_streaming: Whether the driver pulls rows from a server cursor.
            close_callback: Cleanup run on exhaustion or explicit close.
            gc_callback: Non-blocking local cleanup run when the stream is dropped.
        """
        super().__init__(
            columns=columns,
            is_server_streaming=is_server_streaming,
            close_callback=close_callback,
            gc_callback=gc_callback,
        )
        self._chunks = chunks

    def next_batch(self) -> BridgeStreamChunk | None:
        """Pulls the next chunk from the underlying generator."""
        if self._closed:
            return None
        try:
            return next(self._chunks)
        except StopIteration:
            self.close()
            return None


class AsyncBridgeStream:
    """Asynchronous pull-based bridge stream backed by an async generator."""

    def __init__(
        self,
        chunks: AsyncIterator[BridgeStreamChunk],
        *,
        columns: Sequence[str],
        is_server_streaming: bool,
        close_callback: Callable[[], Any] | None = None,
        gc_callback: Callable[[], Any] | None = None,
    ) -> None:
        """Initializes an async generator-backed stream.

        Args:
            chunks: Async iterator yielding decoded chunks.
            columns: Projected column names.
            is_server_streaming: Whether the driver pulls rows from a server cursor.
            close_callback: Cleanup run on exhaustion or explicit close.
            gc_callback: Non-blocking local cleanup run when the stream is dropped.
        """
        self._columns = tuple(str(column) for column in columns)
        self._is_server_streaming = bool(is_server_streaming)
        self._chunks = chunks
        self._close_callback = close_callback
        self._gc_callback = gc_callback
        self._closed = False

    @property
    def columns(self) -> list[str]:
        """Projected column names."""
        return list(self._columns)

    @property
    def is_server_streaming(self) -> bool:
        """Whether the driver pulls rows incrementally from a server cursor."""
        return self._is_server_streaming

    @property
    def is_closed(self) -> bool:
        """Whether the stream is exhausted or explicitly closed."""
        return self._closed

    async def next_batch(self) -> BridgeStreamChunk | None:
        """Pulls the next chunk from the async generator."""
        if self._closed:
            return None
        try:
            return await self._chunks.__anext__()
        except StopAsyncIteration:
            await self.close()
            return None

    async def close(self) -> None:
        """Closes the stream and runs the owner's cleanup callback. Idempotent."""
        if self._closed:
            return
        self._closed = True
        callback = self._close_callback
        self._close_callback = None
        if callback is not None:
            result = callback()
            if inspect.isawaitable(result):
                await result

    def __del__(self) -> None:
        """Clears local state when a stream is dropped without being closed."""
        if getattr(self, "_closed", True):
            return
        self._closed = True
        self._close_callback = None
        callback = getattr(self, "_gc_callback", None)
        self._gc_callback = None
        if callback is not None:
            try:
                callback()
            except Exception:
                pass


class SyncBridgeStreamAsAsync:
    """Adapts a synchronous bridge stream to the async stream protocol.

    ``offload=True`` runs every pull and close on a worker thread, matching how
    the existing async PostgreSQL and DuckDB bridges execute statements.
    """

    def __init__(self, stream: BridgeStream, *, offload: bool = True) -> None:
        """Wraps ``stream``.

        Args:
            stream: Synchronous bridge stream.
            offload: Run pulls and close in ``asyncio.to_thread``.
        """
        self._stream = stream
        self._offload = offload

    @property
    def columns(self) -> list[str]:
        """Projected column names."""
        return self._stream.columns

    @property
    def is_server_streaming(self) -> bool:
        """Whether the wrapped driver pulls rows incrementally from the server."""
        return self._stream.is_server_streaming

    @property
    def is_closed(self) -> bool:
        """Whether the wrapped stream is exhausted or explicitly closed."""
        return self._stream.is_closed

    async def next_batch(self) -> BridgeStreamChunk | None:
        """Pulls the next chunk, optionally on a worker thread."""
        if self._offload:
            return await asyncio.to_thread(self._stream.next_batch)
        return self._stream.next_batch()

    async def close(self) -> None:
        """Closes the wrapped stream, optionally on a worker thread."""
        if self._offload:
            await asyncio.to_thread(self._stream.close)
            return
        self._stream.close()


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
