"""PostgreSQL and Apache AGE bridge implementations."""

from __future__ import annotations

import asyncio
import datetime
import importlib
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any

import polars as pl

from voyager_ogm.agtype import decode_agtype
from voyager_ogm.bridges.base import (
    BridgeResult,
    BulkExecutionResult,
    _validate_savepoint_name,
    validate_graph_name,
    validate_pagination_parameters,
)
from voyager_ogm.codecs import records_to_polars, strip_json_markers, unwrap_json_marker
from voyager_ogm.ingestion import BulkIngestionPlan
from voyager_ogm.types import Duration, Json, parse_pg_interval_text


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
        return _pg_json_adapter(unwrap_json_marker(value)[1])
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
    explicit, unwrapped = unwrap_json_marker(value)
    if explicit or force_json or isinstance(unwrapped, Mapping):
        return _pg_json_adapter(unwrapped)
    if isinstance(value, (list, tuple)):
        # Use the original list so nested ``Json(...)`` markers keep their JSON
        # element type instead of being flattened by ``unwrap_json_marker``.
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

            return statement, (json.dumps(strip_json_markers(parameters)),)

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
        return records_to_polars(self._execute_cursor(cursor, stmt, params))

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
