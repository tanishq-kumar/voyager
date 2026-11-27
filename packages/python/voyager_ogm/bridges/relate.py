"""Relate embedded database bridge implementations."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Sequence
from typing import Any

import polars as pl

from voyager_ogm.bridges.base import (
    BridgeResult,
    BulkExecutionResult,
    _infer_record_columns,
    validate_pagination_parameters,
)
from voyager_ogm.codecs import arrow_table_to_polars, records_to_polars
from voyager_ogm.entities import convert_driver_value
from voyager_ogm.ingestion import BulkIngestionPlan


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
                    return arrow_table_to_polars(res.to_arrow())
                except Exception:
                    pass
            cols = _relate_result_columns(res)
            records = getattr(res, "fetchall", lambda: getattr(res, "data", []))()
            normalized = [_normalize_relate_record(r) for r in records]
            return records_to_polars(BridgeResult(normalized, columns=cols))
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
