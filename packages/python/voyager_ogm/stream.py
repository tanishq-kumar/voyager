"""Pull-based result streams for constant-memory query execution.

``Session.stream()`` / ``AsyncSession.stream()`` / ``Transaction.stream()`` /
``AsyncTransaction.stream()`` return the :class:`ResultStream` and
:class:`AsyncResultStream` wrappers defined here. A stream owns the leased
database cursor (a pooled native connection, a named PostgreSQL portal, a
DuckDB Arrow record batch reader, or a buffered bridge slice) until it is
exhausted, closed, or garbage collected.

The wrappers are format-agnostic: every engine yields the same batch shapes.

* ``as_format="dicts"`` yields ``list[dict[str, Any]]`` batches.
* ``as_format="arrow"`` yields ``pyarrow.RecordBatch`` batches on every engine.
* ``as_format="polars"`` yields ``polars.DataFrame`` batches.

``iter_rows()`` flattens batches back into one dictionary per row, and
``to_arrow_reader()`` exposes one long-lived ``pyarrow.RecordBatchReader`` with
a schema locked from the first batch. Plain iteration yields batches, matching
the existing ``ExecutionResult.iter_batches()`` contract.
"""

from __future__ import annotations

import inspect
import warnings
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from typing import Any, Literal

from voyager_ogm.bridges.base import BufferedBridgeStream

__all__ = [
    "DEFAULT_STREAM_FETCH_SIZE",
    "AsyncResultStream",
    "ResultStream",
    "StreamFormat",
]

DEFAULT_STREAM_FETCH_SIZE = 4_000
"""Default number of rows requested per wire pull, portal execute, or Arrow batch."""

StreamFormat = Literal["dicts", "arrow", "polars"]
"""Supported per-batch output formats."""

_STREAM_FORMATS: dict[str, StreamFormat] = {
    "dicts": "dicts",
    "arrow": "arrow",
    "polars": "polars",
}


def _validate_stream_format(as_format: str) -> StreamFormat:
    """Validates a stream batch format and returns it typed."""
    try:
        return _STREAM_FORMATS[as_format]
    except KeyError:
        raise ValueError(
            f"Unsupported stream format: {as_format!r}. Supported formats: 'dicts', 'arrow', 'polars'"
        ) from None


def _is_native_sync_source(source: Any) -> bool:
    """Returns whether ``source`` is a PyO3 ``NativeResultStream``-like object."""
    return callable(getattr(source, "next_chunk_sync", None))


def _is_native_async_source(source: Any) -> bool:
    """Returns whether ``source`` is a PyO3 ``NativeResultStream``-like object."""
    return callable(getattr(source, "next_chunk", None))


def _chunk_to_dicts(chunk: Any) -> list[dict[str, Any]]:
    """Converts one raw chunk into row dictionaries."""
    to_dicts = getattr(chunk, "to_dicts", None)
    if callable(to_dicts):
        result = to_dicts()
        return result if isinstance(result, list) else list(result)
    return []


def _empty_batch(columns: Sequence[str], schema: Any = None) -> Any:
    """Builds a zero-row ``pyarrow.RecordBatch`` for an empty result."""
    import pyarrow as pa

    if schema is not None:
        return pa.RecordBatch.from_arrays(
            [pa.array([], type=field.type) for field in schema], schema=schema
        )
    return pa.RecordBatch.from_arrays(
        [pa.array([], type=pa.null()) for _ in columns], names=list(columns)
    )


def _chunk_to_record_batch(chunk: Any) -> Any:
    """Converts one raw chunk into a single ``pyarrow.RecordBatch``."""
    import pyarrow as pa

    to_batch = getattr(chunk, "to_record_batch", None)
    if callable(to_batch):
        return to_batch()
    reader = pa.RecordBatchReader.from_stream(chunk)
    table = reader.read_all()
    if table.num_rows == 0:
        return _empty_batch(table.column_names, schema=table.schema)
    batches = table.combine_chunks().to_batches()
    if len(batches) == 1:
        return batches[0]
    return pa.concat_batches(batches)


def _chunk_to_polars(chunk: Any) -> Any:
    """Converts one raw chunk into a ``polars.DataFrame``."""
    import polars as pl

    to_batch = getattr(chunk, "to_record_batch", None)
    if callable(to_batch):
        return pl.DataFrame(to_batch())
    return pl.DataFrame(chunk)


def _format_chunk(chunk: Any, as_format: StreamFormat) -> Any:
    """Converts one raw chunk to the configured batch format."""
    if as_format == "dicts":
        return _chunk_to_dicts(chunk)
    if as_format == "arrow":
        return _chunk_to_record_batch(chunk)
    return _chunk_to_polars(chunk)


class _StreamRelease:
    """One-shot reference-count release shared by sync and async stream wrappers."""

    def __init__(self, on_release: Callable[[], None] | None) -> None:
        self._on_release = on_release
        self._released = False

    @property
    def released(self) -> bool:
        """Whether the owning session has already been notified."""
        return self._released

    def release(self) -> None:
        """Notifies the owning session exactly once."""
        if self._released:
            return
        self._released = True
        callback = self._on_release
        self._on_release = None
        if callback is not None:
            callback()


class ResultStream:
    """Synchronous pull-based result stream with a bounded row buffer.

    Streams are opened by :meth:`Session.stream` (and
    :meth:`Transaction.stream`) before this object is returned, so syntax
    errors surface at the call site and ``columns`` is available immediately.
    Use it as a context manager so an early ``break`` discards unread
    server-side rows deterministically::

        with session.stream(query, fetch_size=1_000) as stream:
            for batch in stream:
                process(batch)

    Plain iteration yields batches in the configured ``as_format``;
    :meth:`iter_rows` yields one dictionary per row.
    """

    def __init__(
        self,
        source: Any,
        *,
        statement: str = "",
        dialect: str = "cypher",
        as_format: str = "dicts",
        on_release: Callable[[], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        """Wraps an already-opened native or bridge stream source.

        Args:
            source: A ``NativeResultStream`` or bridge stream object.
            statement: Executed statement, for diagnostics and repr.
            dialect: Query dialect, for diagnostics.
            as_format: Batch format (``"dicts"``, ``"arrow"``, or ``"polars"``).
            on_release: Called once when the stream is exhausted, closed, or dropped.
            on_error: Called when a mid-stream pull raises, never for close/exhaustion.
        """
        self._source = source
        self._statement = statement
        self._dialect = dialect
        self._as_format = _validate_stream_format(as_format)
        self._release = _StreamRelease(on_release)
        self._on_error = on_error
        self._closed = False
        self._reader_created = False
        self._is_native_sync = _is_native_sync_source(source)
        self._next_dicts_sync = (
            getattr(source, "next_dicts_sync", None) if self._is_native_sync else None
        )
        if not callable(self._next_dicts_sync):
            self._next_dicts_sync = None

    @property
    def columns(self) -> tuple[str, ...]:
        """Projected column names, available before the first batch is pulled."""
        source = self._source
        columns = getattr(source, "columns", ()) if source is not None else ()
        return tuple(str(column) for column in columns)

    @property
    def statement(self) -> str:
        """Executed statement."""
        return self._statement

    @property
    def dialect(self) -> str:
        """Query dialect used to compile the statement."""
        return self._dialect

    @property
    def as_format(self) -> StreamFormat:
        """Per-batch output format."""
        return self._as_format

    @property
    def is_server_streaming(self) -> bool:
        """Whether rows are pulled incrementally from the server over the wire."""
        source = self._source
        return bool(source is not None and getattr(source, "is_server_streaming", False))

    @property
    def is_closed(self) -> bool:
        """Whether the stream is exhausted, explicitly closed, or failed."""
        if self._closed or self._release.released:
            return True
        source = self._source
        return bool(source is None or getattr(source, "is_closed", False))

    @property
    def summary(self) -> Any:
        """Backend execution summary counters, when the source exposes them."""
        source = self._source
        return getattr(source, "summary", None) if source is not None else None

    def __enter__(self) -> ResultStream:
        """Enters the stream context."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> Literal[False]:
        """Closes the stream, never masking an exception raised by the body."""
        try:
            self.close()
        except BaseException:
            if exc_type is None:
                raise
        return False

    def __iter__(self) -> ResultStream:
        """Iterates over batches in the configured format."""
        return self

    def __next__(self) -> Any:
        """Pulls and formats the next batch, or raises ``StopIteration``."""
        if self._as_format == "dicts" and self._next_dicts_sync is not None:
            batch = self._next_dicts_batch()
            if batch is None:
                raise StopIteration
            return batch
        chunk = self._next_chunk()
        if chunk is None:
            raise StopIteration
        return _format_chunk(chunk, self._as_format)

    def _next_dicts_batch(self) -> list[dict[str, Any]] | None:
        """Fast path pulling one native dict batch directly without an ArrowStream wrapper."""
        if self._closed:
            return None
        next_dicts = self._next_dicts_sync
        if next_dicts is None:
            return None
        try:
            batch = next_dicts()
        except BaseException as exc:
            self._release.release()
            if self._on_error is not None and isinstance(exc, Exception):
                try:
                    self._on_error(exc)
                except Exception:
                    pass
            raise
        if batch is None:
            self._closed = True
            self._release.release()
            return None
        return batch

    def _next_chunk(self) -> Any:
        """Pulls one raw chunk and releases the lease when the wire is drained."""
        if self._closed:
            return None
        source = self._source
        if source is None:
            return None
        try:
            if self._is_native_sync:
                chunk = source.next_chunk_sync()
            else:
                chunk = source.next_batch()
        except BaseException as exc:
            self._release.release()
            if self._on_error is not None and isinstance(exc, Exception):
                try:
                    self._on_error(exc)
                except Exception:
                    pass
            raise
        if chunk is None:
            self._closed = True
            self._release.release()
            return None
        return chunk

    def iter_rows(self) -> Iterator[dict[str, Any]]:
        """Yields one dictionary per row regardless of the configured batch format."""
        if self._next_dicts_sync is not None:
            while True:
                batch = self._next_dicts_batch()
                if batch is None:
                    return
                yield from batch
        while True:
            chunk = self._next_chunk()
            if chunk is None:
                return
            yield from _chunk_to_dicts(chunk)

    def iter_batches(self) -> Iterator[Any]:
        """Yields batches in the configured format (alias for plain iteration)."""
        if self._as_format == "dicts" and self._next_dicts_sync is not None:
            while True:
                batch = self._next_dicts_batch()
                if batch is None:
                    return
                yield batch
        while True:
            chunk = self._next_chunk()
            if chunk is None:
                return
            yield _format_chunk(chunk, self._as_format)

    def to_arrow_reader(self) -> Any:
        """Returns a schema-locked ``pyarrow.RecordBatchReader`` over the remaining rows.

        The schema is locked from the first batch (or the typed empty schema of
        a zero-row native stream). Incompatible mid-stream drift raises at the
        point the offending batch is pulled instead of emitting corrupt Arrow.

        Raises:
            RuntimeError: If the Arrow C stream was already requested once.
        """
        if self._reader_created:
            raise RuntimeError(
                "to_arrow_reader() may only be called once per stream: "
                "the Apache Arrow C Data Interface enforces single-pass consumption"
            )
        self._reader_created = True
        source = self._source
        if source is None:
            import pyarrow as pa

            return pa.RecordBatchReader.from_batches(
                pa.schema([(column, pa.null()) for column in self.columns]), []
            )

        if _is_native_sync_source(source):
            import pyarrow as pa

            inner = pa.RecordBatchReader.from_stream(source)
            schema = inner.schema

            def native_batches() -> Iterator[Any]:
                try:
                    yield from inner
                finally:
                    try:
                        self.close()
                    except BaseException:
                        pass

            return pa.RecordBatchReader.from_batches(schema, native_batches())

        import pyarrow as pa

        first = self._next_chunk()
        if first is None:
            return pa.RecordBatchReader.from_batches(
                pa.schema([(column, pa.null()) for column in self.columns]), []
            )
        first_batch = _chunk_to_record_batch(first)
        schema = first_batch.schema

        def bridge_batches() -> Iterator[Any]:
            try:
                yield first_batch
                while True:
                    chunk = self._next_chunk()
                    if chunk is None:
                        return
                    batch = _chunk_to_record_batch(chunk)
                    if batch.schema != schema:
                        import pyarrow as pa

                        try:
                            table = pa.Table.from_batches([batch]).cast(schema)
                        except Exception as exc:
                            raise ValueError(
                                "Stream schema drift for columns "
                                f"{list(schema.names)}: locked {schema} but chunk "
                                f"{batch.schema} cannot be cast; use as_format='dicts' "
                                "or CAST(...) in the query"
                            ) from exc
                        batch = table.combine_chunks().to_batches()[0]
                    yield batch
            finally:
                try:
                    self.close()
                except BaseException:
                    pass

        return pa.RecordBatchReader.from_batches(schema, bridge_batches())

    def close(self) -> None:
        """Closes the stream, discarding any unread rows. Idempotent."""
        if self._closed:
            return
        self._closed = True
        source = self._source
        try:
            if source is not None:
                close_sync = getattr(source, "close_sync", None)
                if callable(close_sync):
                    close_sync()
                else:
                    close = getattr(source, "close", None)
                    if callable(close):
                        close()
        finally:
            self._release.release()

    def __del__(self) -> None:
        """Warns when a stream is garbage collected without being closed.

        Never performs network or blocking I/O: dropping the underlying native
        stream marks its pooled connection broken in Rust and returns the permit
        immediately, and a bridge source is left for its owner's garbage
        collection.
        """
        if getattr(self, "_release", None) is None or self._release.released:
            return
        try:
            warnings.warn(
                f"Unclosed {type(self).__name__} for statement {self._statement!r}; "
                "use it as a context manager or call close() to release the connection",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )
        except Exception:
            pass
        self._closed = True
        self._source = None
        self._release.release()


class AsyncResultStream:
    """Asynchronous pull-based result stream with a bounded row buffer.

    :meth:`AsyncSession.stream` returns this context manager directly, so both
    idioms work::

        async with session.stream(query, fetch_size=1_000) as stream:
            async for batch in stream:
                await process(batch)

        stream = await session.stream(query)

    ``await stream.open()`` and ``await stream.aclose()`` are available for
    manual lifecycle control.
    """

    def __init__(
        self,
        opener: Callable[[], Any],
        *,
        statement: str = "",
        dialect: str = "cypher",
        as_format: str = "dicts",
        on_open: Callable[[], None] | None = None,
        on_release: Callable[[], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
    ) -> None:
        """Creates a lazy async stream that opens on first use.

        Args:
            opener: Callable returning an awaitable source object.
            statement: Executed statement, for diagnostics and repr.
            dialect: Query dialect, for diagnostics.
            as_format: Batch format (``"dicts"``, ``"arrow"``, or ``"polars"``).
            on_open: Called once after the source opens.
            on_release: Called once when the stream is exhausted, closed, or dropped.
            on_error: Called when a mid-stream pull raises, never for close/exhaustion.
        """
        self._opener = opener
        self._statement = statement
        self._dialect = dialect
        self._as_format = _validate_stream_format(as_format)
        self._on_open = on_open
        self._release = _StreamRelease(on_release)
        self._on_error = on_error
        self._source: Any = None
        self._opened = False
        self._closed = False
        self._is_native_async = False
        self._next_dicts: Callable[[], Any] | None = None

    @property
    def columns(self) -> tuple[str, ...]:
        """Projected column names, populated after :meth:`open`."""
        source = self._source
        columns = getattr(source, "columns", ()) if source is not None else ()
        return tuple(str(column) for column in columns)

    @property
    def statement(self) -> str:
        """Executed statement."""
        return self._statement

    @property
    def dialect(self) -> str:
        """Query dialect used to compile the statement."""
        return self._dialect

    @property
    def as_format(self) -> StreamFormat:
        """Per-batch output format."""
        return self._as_format

    @property
    def is_server_streaming(self) -> bool:
        """Whether rows are pulled incrementally from the server over the wire."""
        source = self._source
        return bool(source is not None and getattr(source, "is_server_streaming", False))

    @property
    def is_closed(self) -> bool:
        """Whether the stream is exhausted, explicitly closed, or failed."""
        if self._closed or self._release.released:
            return True
        source = self._source
        return bool(source is None or getattr(source, "is_closed", False))

    @property
    def summary(self) -> Any:
        """Backend execution summary counters, when the source exposes them."""
        source = self._source
        return getattr(source, "summary", None) if source is not None else None

    async def open(self) -> AsyncResultStream:
        """Opens the stream if it is not already open and returns it."""
        if self._source is not None:
            return self
        self._source = await self._opener()
        self._opened = True
        self._is_native_async = _is_native_async_source(self._source)
        next_dicts = getattr(self._source, "next_dicts", None) if self._is_native_async else None
        self._next_dicts = next_dicts if callable(next_dicts) else None
        if self._on_open is not None:
            callback = self._on_open
            self._on_open = None
            callback()
        return self

    def __await__(self) -> Any:
        """Allows ``stream = await session.stream(...)``."""
        return self._await_open().__await__()

    async def _await_open(self) -> AsyncResultStream:
        """Opens the stream and returns ``self`` for ``__await__``."""
        return await self.open()

    async def __aenter__(self) -> AsyncResultStream:
        """Opens the stream and enters the async context."""
        return await self.open()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> Literal[False]:
        """Closes the stream, never masking an exception raised by the body."""
        try:
            await self.aclose()
        except BaseException:
            if exc_type is None:
                raise
        return False

    def __aiter__(self) -> AsyncResultStream:
        """Iterates over batches in the configured format."""
        return self

    async def __anext__(self) -> Any:
        """Pulls and formats the next batch, or raises ``StopAsyncIteration``."""
        if self._as_format == "dicts":
            if self._source is None and not self._closed:
                await self.open()
            if self._next_dicts is not None:
                batch = await self._next_dicts_batch()
                if batch is None:
                    raise StopAsyncIteration
                return batch
        chunk = await self._next_chunk()
        if chunk is None:
            raise StopAsyncIteration
        return _format_chunk(chunk, self._as_format)

    async def _next_dicts_batch(self) -> list[dict[str, Any]] | None:
        """Fast path pulling one native dict batch directly without an ArrowStream wrapper."""
        if self._closed:
            return None
        next_dicts = self._next_dicts
        if next_dicts is None:
            return None
        try:
            batch = await next_dicts()
        except BaseException as exc:
            self._release.release()
            if self._on_error is not None and isinstance(exc, Exception):
                try:
                    self._on_error(exc)
                except Exception:
                    pass
            raise
        if batch is None:
            self._closed = True
            self._release.release()
            return None
        return batch

    async def _next_chunk(self) -> Any:
        """Pulls one raw chunk and releases the lease when the wire is drained."""
        if self._closed:
            return None
        if self._source is None:
            await self.open()
        source = self._source
        if source is None:
            return None
        try:
            if self._is_native_async:
                chunk = await source.next_chunk()
            else:
                result = source.next_batch()
                chunk = await result if inspect.isawaitable(result) else result
        except BaseException as exc:
            self._release.release()
            if self._on_error is not None and isinstance(exc, Exception):
                try:
                    self._on_error(exc)
                except Exception:
                    pass
            raise
        if chunk is None:
            self._closed = True
            self._release.release()
            return None
        return chunk

    async def iter_rows(self) -> AsyncIterator[dict[str, Any]]:
        """Yields one dictionary per row regardless of the configured batch format."""
        if self._source is None and not self._closed:
            await self.open()
        if self._next_dicts is not None:
            while True:
                batch = await self._next_dicts_batch()
                if batch is None:
                    return
                for row in batch:
                    yield row
            return
        while True:
            chunk = await self._next_chunk()
            if chunk is None:
                return
            for row in _chunk_to_dicts(chunk):
                yield row

    async def iter_batches(self) -> AsyncIterator[Any]:
        """Yields batches in the configured format (alias for async iteration)."""
        if self._as_format == "dicts":
            if self._source is None and not self._closed:
                await self.open()
            if self._next_dicts is not None:
                while True:
                    batch = await self._next_dicts_batch()
                    if batch is None:
                        return
                    yield batch
                return
        while True:
            chunk = await self._next_chunk()
            if chunk is None:
                return
            yield _format_chunk(chunk, self._as_format)

    async def aclose(self) -> None:
        """Closes the stream, discarding any unread rows. Idempotent."""
        if self._closed:
            return
        self._closed = True
        source = self._source
        try:
            if source is not None:
                close = getattr(source, "close", None)
                if callable(close):
                    result = close()
                    if inspect.isawaitable(result):
                        await result
        finally:
            self._release.release()

    async def close(self) -> None:
        """Alias for :meth:`aclose`."""
        await self.aclose()

    def __del__(self) -> None:
        """Warns when a stream is garbage collected without being closed.

        Never performs network or blocking I/O: dropping the underlying native
        stream marks its pooled connection broken in Rust and returns the permit
        immediately.
        """
        if (
            getattr(self, "_release", None) is None
            or not getattr(self, "_opened", False)
            or self._release.released
        ):
            return
        try:
            warnings.warn(
                f"Unclosed {type(self).__name__} for statement {self._statement!r}; "
                "use it as an async context manager or call aclose() to release the connection",
                ResourceWarning,
                stacklevel=2,
                source=self,
            )
        except Exception:
            pass
        self._closed = True
        self._source = None
        self._release.release()


def open_bridge_stream(
    bridge: Any,
    statement: str,
    parameters: dict[str, Any] | None,
    fetch_size: int,
) -> Any:
    """Opens a sync bridge stream, falling back to a buffered slice for custom bridges."""
    stream = getattr(bridge, "stream", None)
    if callable(stream):
        return stream(statement, parameters, fetch_size)
    records = bridge.execute(statement, parameters)
    return BufferedBridgeStream.from_records(records, fetch_size)


async def open_async_bridge_stream(
    bridge: Any,
    statement: str,
    parameters: dict[str, Any] | None,
    fetch_size: int,
) -> Any:
    """Opens an async bridge stream, falling back to a buffered slice for custom bridges."""
    stream = getattr(bridge, "stream", None)
    if callable(stream):
        result = stream(statement, parameters, fetch_size)
        if inspect.isawaitable(result):
            return await result
        return result
    records = await bridge.execute(statement, parameters)
    return BufferedBridgeStream.from_records(records, fetch_size)
