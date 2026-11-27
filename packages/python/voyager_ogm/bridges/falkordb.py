"""FalkorDB bridge implementations."""

from __future__ import annotations

import asyncio
import datetime
import time
import uuid
from collections.abc import Sequence
from typing import Any

import polars as pl

from voyager_ogm.bridges.base import (
    BridgeResult,
    BulkExecutionResult,
    validate_pagination_parameters,
)
from voyager_ogm.codecs import normalize_uuid, point_param_map, records_to_polars
from voyager_ogm.entities import (
    GraphNode,
    GraphPath,
    GraphRelationship,
    convert_driver_value,
)
from voyager_ogm.ingestion import BulkIngestionPlan
from voyager_ogm.types import Duration, Json, Point, to_iso_duration


def _adapt_falkordb_value(val: Any) -> Any:
    """Adapts rich parameter values (temporal types, Duration, Point, etc.) for FalkorDB query execution."""
    if isinstance(val, Json):
        return _adapt_falkordb_value(val.value)
    if isinstance(val, uuid.UUID):
        return normalize_uuid(val)
    if isinstance(val, (datetime.date, datetime.time, datetime.datetime)):
        return val.isoformat()
    if isinstance(val, datetime.timedelta):
        return to_iso_duration(val)
    if isinstance(val, Duration):
        return val.to_iso()
    if isinstance(val, Point):
        return point_param_map(val) or val.to_cypher_dict()
    if isinstance(val, dict):
        if "__voyager_spatial__" in val:
            # FalkorDB has no native Point parameter; bind an openCypher point
            # map instead of the JSON string older versions sent (#395).
            point_map = point_param_map(val)
            if point_map is not None:
                return point_map
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
        return records_to_polars(self.execute(statement, parameters))

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
