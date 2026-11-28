"""Neo4j and Memgraph Bolt bridge implementations."""

from __future__ import annotations

import datetime
import inspect
import time
import uuid
from collections.abc import Sequence
from typing import Any

import polars as pl

from voyager_ogm.bridges.base import (
    AsyncBridgeStream,
    BridgeResult,
    BridgeStreamChunk,
    BulkExecutionResult,
    GeneratorBridgeStream,
    _infer_record_columns,
    validate_pagination_parameters,
)
from voyager_ogm.codecs import normalize_uuid, records_to_polars, zoneinfo_key
from voyager_ogm.entities import convert_driver_value
from voyager_ogm.ingestion import BulkIngestionPlan
from voyager_ogm.types import Duration, Json


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
    key = zoneinfo_key(val)
    if key is None:
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
        return normalize_uuid(val)
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


async def _await_if_needed(value: Any) -> Any:
    """Awaits ``value`` when it is awaitable, so sync and async drivers share one path."""
    if inspect.isawaitable(value):
        return await value
    return value


def _bolt_execute(runner: Any, statement: str, parameters: dict[str, Any] | None) -> BridgeResult:
    """Runs one Bolt statement through ``runner`` and normalizes its records once."""
    validate_pagination_parameters(statement, parameters)
    result = runner(statement, _adapt_bolt_parameters(parameters or {}))
    keys = _bolt_result_keys(result)
    records = [_normalize_bolt_record(record) for record in result]
    return _bolt_bridge_result(keys, records)


async def _bolt_execute_async(
    runner: Any, statement: str, parameters: dict[str, Any] | None
) -> BridgeResult:
    """Runs one async Bolt statement and normalizes its records once."""
    validate_pagination_parameters(statement, parameters)
    result = await runner(statement, _adapt_bolt_parameters(parameters or {}))
    keys = _bolt_result_keys(result)
    if hasattr(result, "__aiter__"):
        records = [_normalize_bolt_record(record) async for record in result]
        return _bolt_bridge_result(keys, records)
    raw = await result.data()
    return _bolt_bridge_result(keys, [_normalize_bolt_record(r) for r in raw])


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
        session_kwargs = {"database": self.database} if self.database else {}
        with self.driver.session(**session_kwargs) as session:
            return _bolt_execute(session.run, statement, parameters)

    def stream(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        fetch_size: int = 1_000,
    ) -> GeneratorBridgeStream:
        """Opens a pull-based server-side Bolt stream over a leased driver session.

        The official driver pulls ``fetch_size`` records at a time from the open
        result and sends ``DISCARD`` when the stream is closed early, so only one
        chunk is resident in Python memory.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.
            fetch_size: Maximum records pulled per batch.

        Returns:
            A server-streaming bridge stream yielding record dictionaries.
        """
        validate_pagination_parameters(statement, parameters)
        fetch_size = max(1, int(fetch_size))
        session_kwargs = {"database": self.database} if self.database else {}
        session = self.driver.session(fetch_size=fetch_size, **session_kwargs)
        try:
            result = session.run(statement, _adapt_bolt_parameters(parameters or {}))
        except BaseException:
            session.close()
            raise

        columns = _bolt_result_keys(result)

        def _chunks() -> Any:
            buffer: list[dict[str, Any]] = []
            for record in result:
                buffer.append(_normalize_bolt_record(record))
                if len(buffer) >= fetch_size:
                    yield BridgeStreamChunk(buffer, columns=columns)
                    buffer = []
            if buffer:
                yield BridgeStreamChunk(buffer, columns=columns)

        return GeneratorBridgeStream(
            _chunks(),
            columns=columns,
            is_server_streaming=True,
            close_callback=session.close,
        )

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
        return records_to_polars(self.execute(statement, parameters))

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
        self._open_stream = False
        self._active_stream: Any = None
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

    def _ensure_no_open_stream(self) -> None:
        """Fails fast when a stream is open on this transaction."""
        if self._open_stream:
            raise RuntimeError(
                "an active stream is open on this transaction; close or exhaust it first"
            )

    def _clear_open_stream(self) -> None:
        """Clears the open-stream fail-fast flag."""
        self._open_stream = False
        self._active_stream = None

    def stream(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        fetch_size: int = 1_000,
    ) -> GeneratorBridgeStream:
        """Opens a pull-based stream on the transaction's driver transaction.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.
            fetch_size: Maximum records pulled per batch.

        Returns:
            A server-streaming bridge stream yielding record dictionaries.
        """
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        self._ensure_no_open_stream()
        validate_pagination_parameters(statement, parameters)
        fetch_size = max(1, int(fetch_size))
        try:
            result = self._tx.run(statement, _adapt_bolt_parameters(parameters or {}))
        except BaseException:
            self._mark_failed()
            raise
        columns = _bolt_result_keys(result)

        def _chunks() -> Any:
            buffer: list[dict[str, Any]] = []
            for record in result:
                buffer.append(_normalize_bolt_record(record))
                if len(buffer) >= fetch_size:
                    yield BridgeStreamChunk(buffer, columns=columns)
                    buffer = []
            if buffer:
                yield BridgeStreamChunk(buffer, columns=columns)

        def _close() -> None:
            try:
                result.consume()
            finally:
                self._clear_open_stream()

        self._open_stream = True
        stream = GeneratorBridgeStream(
            _chunks(),
            columns=columns,
            is_server_streaming=True,
            close_callback=_close,
            gc_callback=self._clear_open_stream,
        )
        self._active_stream = stream
        return stream

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Runs a Cypher statement inside the transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        self._ensure_no_open_stream()
        validate_pagination_parameters(statement, parameters)
        try:
            return _bolt_execute(self._tx.run, statement, parameters)
        except BaseException:
            self._mark_failed()
            raise

    def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Runs a Cypher statement and returns a Polars DataFrame."""
        return records_to_polars(self.execute(statement, parameters))

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
        self._ensure_no_open_stream()
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
        self._ensure_no_open_stream()
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
        stream = self._active_stream
        self._active_stream = None
        if stream is not None:
            try:
                stream.close()
            except Exception:
                pass
        self._open_stream = False
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
        session_kwargs = {"database": self.database} if self.database else {}
        async with self.driver.session(**session_kwargs) as session:
            return await _bolt_execute_async(session.run, statement, parameters)

    async def stream(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        fetch_size: int = 1_000,
    ) -> AsyncBridgeStream:
        """Opens a pull-based server-side Bolt stream on an async driver session.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.
            fetch_size: Maximum records pulled per batch.

        Returns:
            An async server-streaming bridge stream yielding record dictionaries.
        """
        validate_pagination_parameters(statement, parameters)
        fetch_size = max(1, int(fetch_size))
        session_kwargs = {"database": self.database} if self.database else {}
        session = self.driver.session(fetch_size=fetch_size, **session_kwargs)
        try:
            result = await _await_if_needed(
                session.run(statement, _adapt_bolt_parameters(parameters or {}))
            )
            keys = await _await_if_needed(result.keys()) if hasattr(result, "keys") else ()
        except BaseException:
            await _await_if_needed(session.close())
            raise

        columns = tuple(str(key) for key in keys)

        async def _chunks() -> Any:
            buffer: list[dict[str, Any]] = []
            if hasattr(result, "__aiter__"):
                async for record in result:
                    buffer.append(_normalize_bolt_record(record))
                    if len(buffer) >= fetch_size:
                        yield BridgeStreamChunk(buffer, columns=columns)
                        buffer = []
            else:
                records = await _await_if_needed(result.data())
                for record in records:
                    buffer.append(_normalize_bolt_record(record))
                    if len(buffer) >= fetch_size:
                        yield BridgeStreamChunk(buffer, columns=columns)
                        buffer = []
            if buffer:
                yield BridgeStreamChunk(buffer, columns=columns)

        return AsyncBridgeStream(
            _chunks(),
            columns=columns,
            is_server_streaming=True,
            close_callback=session.close,
        )

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
        return records_to_polars(await self.execute(statement, parameters))

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
        self._open_stream = False
        self._active_stream: Any = None

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

    def _ensure_no_open_stream(self) -> None:
        """Fails fast when a stream is open on this transaction."""
        if self._open_stream:
            raise RuntimeError(
                "an active stream is open on this transaction; close or exhaust it first"
            )

    def _clear_open_stream(self) -> None:
        """Clears the open-stream fail-fast flag."""
        self._open_stream = False
        self._active_stream = None

    async def stream(
        self,
        statement: str,
        parameters: dict[str, Any] | None = None,
        fetch_size: int = 1_000,
    ) -> AsyncBridgeStream:
        """Opens a pull-based stream on the async driver transaction.

        Args:
            statement: Cypher statement.
            parameters: Query parameters.
            fetch_size: Maximum records pulled per batch.

        Returns:
            An async server-streaming bridge stream yielding record dictionaries.
        """
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        self._ensure_no_open_stream()
        validate_pagination_parameters(statement, parameters)
        fetch_size = max(1, int(fetch_size))
        try:
            result = await _await_if_needed(
                self._tx.run(statement, _adapt_bolt_parameters(parameters or {}))
            )
            keys = await _await_if_needed(result.keys()) if hasattr(result, "keys") else ()
        except BaseException:
            self._mark_failed()
            raise
        columns = tuple(str(key) for key in keys)

        async def _chunks() -> Any:
            buffer: list[dict[str, Any]] = []
            if hasattr(result, "__aiter__"):
                async for record in result:
                    buffer.append(_normalize_bolt_record(record))
                    if len(buffer) >= fetch_size:
                        yield BridgeStreamChunk(buffer, columns=columns)
                        buffer = []
            else:
                records = await _await_if_needed(result.data())
                for record in records:
                    buffer.append(_normalize_bolt_record(record))
                    if len(buffer) >= fetch_size:
                        yield BridgeStreamChunk(buffer, columns=columns)
                        buffer = []
            if buffer:
                yield BridgeStreamChunk(buffer, columns=columns)

        async def _close() -> None:
            try:
                if hasattr(result, "consume"):
                    await _await_if_needed(result.consume())
            finally:
                self._clear_open_stream()

        self._open_stream = True
        stream = AsyncBridgeStream(
            _chunks(),
            columns=columns,
            is_server_streaming=True,
            close_callback=_close,
            gc_callback=self._clear_open_stream,
        )
        self._active_stream = stream
        return stream

    async def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        """Runs a Cypher statement inside the transaction."""
        if self._state == "FAILED":
            raise RuntimeError("transaction is aborted; roll back before executing more statements")
        if not self.is_active:
            raise ValueError("Cannot execute query on inactive transaction")
        self._ensure_no_open_stream()
        validate_pagination_parameters(statement, parameters)
        try:
            return await _bolt_execute_async(self._tx.run, statement, parameters)
        except BaseException:
            self._mark_failed()
            raise

    async def execute_to_polars(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> pl.DataFrame:
        """Runs a Cypher statement and returns a Polars DataFrame."""
        return records_to_polars(await self.execute(statement, parameters))

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
        self._ensure_no_open_stream()
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
        self._ensure_no_open_stream()
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
        stream = self._active_stream
        self._active_stream = None
        if stream is not None:
            try:
                await stream.close()
            except Exception:
                pass
        self._open_stream = False
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
