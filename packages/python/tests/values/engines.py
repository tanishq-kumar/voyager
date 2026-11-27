"""Engine adapters for differential value conformance testing across 6 databases (#299)."""

from __future__ import annotations

import datetime
import json
import os
import socket
import uuid
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import polars as pl
import pyarrow as pa
from conftest import get_test_host
from voyager_ogm import Duration, GraphNode, GraphRelationship, Point, Session
from voyager_ogm.agtype import decode_agtype
from voyager_ogm.bridge import DuckDbBridge, FalkorDBBridge, Neo4jBoltBridge, PostgresBridge

PG_USER = os.environ.get("VOYAGER_TEST_PG_USER", "postgres")
PG_PASSWORD = os.environ.get("VOYAGER_TEST_PG_PASSWORD", "voyagerpass123")
PG_DATABASE = os.environ.get("VOYAGER_TEST_PG_DB", "voyager_graph")

DEFAULT_PORTS: dict[str, int] = {
    "neo4j": 7687,
    "memgraph": 7688,
    "falkordb": 6379,
    "age": 5455,
    "pg19": 5456,
}


class EngineUnavailableError(RuntimeError):
    """Raised when a live engine port or optional client dependency is unavailable."""


def engine_endpoint(engine: str) -> tuple[str, int]:
    """Returns ``(host, port)`` for a network database engine."""
    host = get_test_host()
    env_var = f"VOYAGER_TEST_{engine.upper()}_PORT"
    raw = os.environ.get(env_var, "").strip()
    port = int(raw) if raw else DEFAULT_PORTS[engine]
    return host, port


def is_port_open(host: str, port: int, timeout: float = 1.5) -> bool:
    """Fast TCP probe to verify a database container is listening."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _ensure_reachable(engine: str) -> tuple[str, int]:
    host, port = engine_endpoint(engine)
    if not is_port_open(host, port):
        raise EngineUnavailableError(f"{engine} is not reachable on {host}:{port}")
    return host, port


def _canonicalize_official_value(val: Any, engine: str) -> Any:
    """Normalizes official client wrapper objects to Voyager's canonical Python types.

    Keeps driver spatial objects (``WGS84Point`` / ``CartesianPoint``) and unexpected
    types intact so differential checks against ``bridge`` and ``native`` see real
    type differences.
    """
    if val is None or isinstance(
        val, (bool, int, float, str, bytes, Decimal, uuid.UUID, Duration, Point)
    ):
        return val
    if isinstance(val, datetime.timedelta):
        return Duration.from_timedelta(val)
    if isinstance(val, datetime.datetime):
        if engine == "falkordb" and val.tzinfo is not None:
            return val.replace(tzinfo=None)
        return val
    if isinstance(val, (datetime.date, datetime.time)):
        return val
    if isinstance(val, list):
        return [_canonicalize_official_value(item, engine) for item in val]
    if isinstance(val, dict):
        return {k: _canonicalize_official_value(v, engine) for k, v in val.items()}

    mod = type(val).__module__
    name = type(val).__name__
    if mod.startswith("neo4j.time"):
        if name == "Date":
            try:
                return datetime.date(val.year, val.month, val.day)
            except Exception:
                return str(val.iso_format())
        if name == "Time":
            ns = getattr(val, "nanosecond", 0)
            if ns % 1000 != 0:
                return str(val.iso_format())
            return datetime.time(
                val.hour, val.minute, val.second, ns // 1000, tzinfo=getattr(val, "tzinfo", None)
            )
        if name == "DateTime":
            ns = getattr(val, "nanosecond", 0)
            if ns % 1000 != 0 or not (1 <= val.year <= 9999):
                return str(val.iso_format())
            tz = getattr(val, "tzinfo", None)
            try:
                return datetime.datetime(
                    val.year,
                    val.month,
                    val.day,
                    val.hour,
                    val.minute,
                    val.second,
                    ns // 1000,
                    tzinfo=tz,
                )
            except Exception:
                return str(val.iso_format())
        if name == "Duration":
            return Duration(
                months=val.months,
                days=val.days,
                seconds=val.seconds,
                nanoseconds=val.nanoseconds,
            )
    mro = type(val).__mro__
    if any(c.__module__.startswith("neo4j.graph") for c in mro):
        mro_names = {c.__name__ for c in mro}
        if "Node" in mro_names:
            return GraphNode._from_trusted(
                node_id=None,
                element_id=getattr(val, "element_id", None),
                labels=frozenset(getattr(val, "labels", ())),
                properties={
                    k: _canonicalize_official_value(v, engine) for k, v in dict(val).items()
                },
            )
        if "Relationship" in mro_names:
            return GraphRelationship._from_trusted(
                rel_id=None,
                element_id=getattr(val, "element_id", None),
                rel_type=getattr(val, "type", ""),
                start_id=None,
                end_id=None,
                properties={
                    k: _canonicalize_official_value(v, engine) for k, v in dict(val).items()
                },
            )
    if mod.startswith("dateutil.relativedelta") and name == "relativedelta":
        total_days = int(getattr(val, "days", 0))
        total_months = int(getattr(val, "years", 0)) * 12 + int(getattr(val, "months", 0))
        secs = (
            int(getattr(val, "hours", 0)) * 3600
            + int(getattr(val, "minutes", 0)) * 60
            + int(getattr(val, "seconds", 0))
        )
        ns = int(getattr(val, "microseconds", 0)) * 1000
        return Duration(months=total_months, days=total_days, seconds=secs, nanoseconds=ns)
    if mod.startswith("falkordb"):
        if name == "Node":
            return GraphNode._from_trusted(
                node_id=getattr(val, "id", None),
                element_id=None,
                labels=frozenset(getattr(val, "labels", ())),
                properties={
                    k: _canonicalize_official_value(v, engine)
                    for k, v in dict(getattr(val, "properties", {})).items()
                },
            )
        if name == "Edge":
            return GraphRelationship._from_trusted(
                rel_id=getattr(val, "id", None),
                element_id=None,
                rel_type=getattr(val, "relation", ""),
                start_id=getattr(val, "src_node", None),
                end_id=getattr(val, "dest_node", None),
                properties={
                    k: _canonicalize_official_value(v, engine)
                    for k, v in dict(getattr(val, "properties", {})).items()
                },
            )
    return val


def _to_official_bolt_param(val: Any) -> Any:
    """Converts Voyager types to ``neo4j`` Python driver input types for the official reference lane."""
    import pytz
    from neo4j.spatial import CartesianPoint, WGS84Point
    from neo4j.time import DateTime as Neo4jDateTime
    from neo4j.time import Duration as Neo4jDuration
    from neo4j.time import Time as Neo4jTime

    if isinstance(val, Duration):
        return Neo4jDuration(
            months=val.months, days=val.days, seconds=val.seconds, nanoseconds=val.nanoseconds
        )
    if isinstance(val, Point):
        if val.is_geographic:
            if val.is_3d and val.height is not None:
                return WGS84Point((val.x, val.y, val.height))
            return WGS84Point((val.x, val.y))
        if val.is_3d and val.z is not None:
            return CartesianPoint((val.x, val.y, val.z))
        return CartesianPoint((val.x, val.y))
    if isinstance(val, datetime.datetime) and val.tzinfo is not None:
        key = getattr(val.tzinfo, "key", None)
        if isinstance(key, str):
            tz = pytz.timezone(key)
            naive = val.replace(tzinfo=None)
            localized = tz.localize(naive, is_dst=not bool(val.fold))
            return Neo4jDateTime(
                localized.year,
                localized.month,
                localized.day,
                localized.hour,
                localized.minute,
                localized.second,
                localized.microsecond * 1000,
                tzinfo=localized.tzinfo,
            )
        return val
    if isinstance(val, datetime.time) and val.tzinfo is not None:
        off = val.utcoffset()
        if off is not None:
            return Neo4jTime(
                val.hour,
                val.minute,
                val.second,
                val.microsecond * 1000,
                tzinfo=pytz.FixedOffset(int(off.total_seconds() // 60)),
            )
    if isinstance(val, uuid.UUID):
        return str(val)
    if isinstance(val, list):
        return [_to_official_bolt_param(x) for x in val]
    if isinstance(val, dict):
        return {k: _to_official_bolt_param(v) for k, v in val.items()}
    return val


def _pg_array_leaf_type(value: Any) -> str | None:
    """Maps one scalar list element to its PostgreSQL array element SQL type."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "bigint"
    if isinstance(value, float):
        return "double precision"
    if isinstance(value, Decimal):
        return "numeric"
    if isinstance(value, bytes):
        return "bytea"
    if isinstance(value, str):
        return "text"
    if isinstance(value, datetime.datetime):
        return "timestamptz" if value.tzinfo is not None else "timestamp"
    if isinstance(value, datetime.date):
        return "date"
    if isinstance(value, datetime.time):
        return "timetz" if value.tzinfo is not None else "time"
    if isinstance(value, Duration):
        return "interval"
    if isinstance(value, uuid.UUID):
        return "uuid"
    return None


def _pg_nested_array_sql_type(value: Any) -> str | None:
    """Returns a rectangular multidimensional SQL array type such as ``bigint[][]``.

    Returns ``None`` for mixed, ragged or otherwise non-array values so the
    caller can fall back to ``jsonb`` exactly like the existing 1-D logic.
    """

    def walk(node: Any) -> tuple[int, str | None] | None:
        if not isinstance(node, list):
            return 0, _pg_array_leaf_type(node)
        children = [child for child in node if child is not None]
        if not children:
            return None
        infos = [walk(child) for child in children]
        if any(info is None for info in infos):
            return None
        ranks = {info[0] for info in infos if info is not None}
        types = {info[1] for info in infos if info is not None}
        if len(ranks) != 1 or len(types) != 1:
            return None
        rank = ranks.pop()
        leaf = types.pop()
        if leaf is None:
            return None
        if rank > 0:
            lengths = {len(child) for child in node if isinstance(child, list)}
            if len(lengths) > 1:
                return None
        return rank + 1, leaf

    info = walk(value)
    if info is None:
        return None
    rank, leaf = info
    if rank < 2 or leaf is None:
        return None
    return leaf + "[]" * rank


def _pg_sql_type_for_value(value: Any, value_kind: str) -> str:
    """Selects the PostgreSQL column SQL type for storing ``value``."""
    mapping = {
        "null": "text",
        "bool": "boolean",
        "int": "bigint",
        "float": "double precision",
        "decimal": "numeric",
        "str": "text",
        "bytes": "bytea",
        "date": "date",
        "time_naive": "time",
        "time_offset": "timetz",
        "datetime_naive": "timestamp",
        "datetime_offset": "timestamptz",
        "datetime_zone": "timestamptz",
        "duration": "interval",
        "uuid": "uuid",
        "json_doc": "jsonb",
        "map": "jsonb",
    }
    if value_kind in mapping:
        return mapping[value_kind]
    if value_kind == "list":
        if not value:
            return "bigint[]"
        item = next((x for x in value if x is not None), None)
        if item is None:
            return "bigint[]"
        if isinstance(item, list):
            nested = _pg_nested_array_sql_type(value)
            return nested if nested is not None else "jsonb"
        if isinstance(item, bool):
            return "boolean[]"
        if isinstance(item, int):
            return "bigint[]"
        if isinstance(item, float):
            return "double precision[]"
        if isinstance(item, Decimal):
            return "numeric[]"
        if isinstance(item, bytes):
            return "bytea[]"
        if isinstance(item, str):
            return "text[]"
        if isinstance(item, datetime.datetime):
            return "timestamptz[]" if item.tzinfo is not None else "timestamp[]"
        if isinstance(item, datetime.date):
            return "date[]"
        if isinstance(item, datetime.time):
            return "timetz[]" if item.tzinfo is not None else "time[]"
        if isinstance(item, Duration):
            return "interval[]"
        if isinstance(item, uuid.UUID):
            return "uuid[]"
        return "jsonb"
    return "text"


def _duckdb_sql_type_for_value(value: Any, value_kind: str) -> str:
    """Selects the DuckDB column SQL type for storing ``value``."""
    mapping = {
        "null": "VARCHAR",
        "bool": "BOOLEAN",
        "float": "DOUBLE",
        "str": "VARCHAR",
        "bytes": "BLOB",
        "date": "DATE",
        "time_naive": "TIME",
        "time_offset": "TIMETZ",
        "datetime_naive": "TIMESTAMP",
        "datetime_offset": "TIMESTAMPTZ",
        "duration": "INTERVAL",
        "uuid": "UUID",
        "json_doc": "JSON",
    }
    if value_kind in mapping:
        return mapping[value_kind]
    if value_kind == "int":
        if isinstance(value, int) and (value < -(2**63) or value > 2**63 - 1):
            return "HUGEINT"
        return "BIGINT"
    if value_kind == "decimal":
        assert isinstance(value, Decimal)
        exp = value.as_tuple().exponent
        scale = -exp if isinstance(exp, int) and exp < 0 else 0
        scale = min(scale, 18)
        return f"DECIMAL(38, {scale})"
    if value_kind == "map":
        return "JSON"
    if value_kind == "list":
        if not value:
            return "BIGINT[]"
        first = next((x for x in value if x is not None), None)
        if isinstance(first, list):
            non_null_subs = [sub for sub in value if sub is not None]
            if all(
                isinstance(sub, list)
                and all(isinstance(x, int) and not isinstance(x, bool) for x in sub)
                for sub in non_null_subs
            ) and any(non_null_subs):
                return "BIGINT[][]"
            if all(
                isinstance(sub, list)
                and all(
                    isinstance(x, datetime.date) and not isinstance(x, datetime.datetime)
                    for x in sub
                )
                for sub in non_null_subs
            ) and any(non_null_subs):
                return "DATE[][]"
            return "JSON"
        if isinstance(first, bool):
            return "BOOLEAN[]"
        if isinstance(first, int):
            if any(
                isinstance(x, int) and (x < -(2**63) or x > 2**63 - 1)
                for x in value
                if x is not None
            ):
                return "HUGEINT[]"
            return "BIGINT[]"
        if isinstance(first, float):
            return "DOUBLE[]"
        if isinstance(first, Decimal):
            exp = first.as_tuple().exponent
            scale = -exp if isinstance(exp, int) and exp < 0 else 0
            return f"DECIMAL(38, {min(scale, 18)})[]"
        if isinstance(first, bytes):
            return "BLOB[]"
        if isinstance(first, str):
            return "VARCHAR[]"
        if isinstance(first, datetime.datetime):
            return "TIMESTAMPTZ[]" if first.tzinfo is not None else "TIMESTAMP[]"
        if isinstance(first, datetime.date):
            return "DATE[]"
        if isinstance(first, datetime.time):
            return "TIMETZ[]" if first.tzinfo is not None else "TIME[]"
        if isinstance(first, Duration):
            return "INTERVAL[]"
        if isinstance(first, uuid.UUID):
            return "UUID[]"
        if first is None:
            return "BIGINT[]"
        return "JSON"
    return "VARCHAR"


def _falkordb_expr_and_param(val: Any, value_kind: str, var: str = "$v") -> tuple[str, Any]:
    """Returns Cypher expression and parameter payload for FalkorDB."""
    if value_kind == "date":
        return f"date({var})", val.isoformat() if isinstance(val, datetime.date) else val
    if value_kind == "time_naive":
        return f"localtime({var})", val.isoformat() if isinstance(val, datetime.time) else val
    if value_kind == "datetime_naive":
        return f"localdatetime({var})", val.isoformat() if isinstance(
            val, datetime.datetime
        ) else val
    if value_kind == "duration":
        return f"duration({var})", val.to_iso() if isinstance(val, Duration) else val
    if value_kind == "uuid":
        return var, str(val) if isinstance(val, uuid.UUID) else val
    if value_kind == "list" and isinstance(val, list):
        return var, [str(x) if isinstance(x, uuid.UUID) else x for x in val]
    if value_kind == "map" and isinstance(val, dict):
        return var, {k: (str(v) if isinstance(v, uuid.UUID) else v) for k, v in val.items()}
    return var, val


def _extract_arrow_cell(table: pa.Table, col_name: str) -> tuple[Any, str]:
    """Extracts ``(python_value, arrow_type_summary)`` from row 0 of a PyArrow Table."""
    field = table.schema.field(col_name)
    meta = ""
    if field.metadata:
        meta_str = ", ".join(
            f"{k.decode('utf-8', 'replace')}={v.decode('utf-8', 'replace')}"
            for k, v in sorted(field.metadata.items())
        )
        meta = f" [{meta_str}]"
    arrow_type_desc = f"{field.type}{meta}"
    col = table.column(col_name)
    raw = col[0].as_py() if table.num_rows > 0 else None
    return raw, arrow_type_desc


def _extract_polars_cell(df: pl.DataFrame, col_name: str) -> tuple[Any, str]:
    """Extracts ``(python_value, polars_dtype_summary)`` from row 0 of a Polars DataFrame."""
    dtype_desc = f"pl.{df.schema[col_name]}"
    val = df[col_name].to_list()[0] if df.height > 0 else None
    return val, dtype_desc


@dataclass
class ExecutionBundle:
    """Results across all available backends for one test case."""

    official: Any
    bridge: Any = None
    native: Any = None
    arrow_val: Any = None
    arrow_type: str = ""
    polars_val: Any = None
    polars_type: str = ""
    server_type: str = "n/a"


class BaseValueAdapter:
    """Common interface for executing value conformance round-trips on one live engine."""

    engine: str
    backends: tuple[str, ...]
    supports_transactions: bool = True
    supports_entities: bool = True

    def close(self) -> None:
        """Releases connections and drops temporary test artifacts."""

    def run_param(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        """Executes parameter round-trip and returns ``(result_val, type_desc, server_type)``."""
        raise NotImplementedError

    def run_stored(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        """Writes value as a property/column, reads it back, and cleans up."""
        raise NotImplementedError

    def run_nested_list(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        """Round-trips ``[value]`` inside a list."""
        raise NotImplementedError

    def run_nested_map(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        """Round-trips ``{'k': value}`` inside a map."""
        raise NotImplementedError

    def run_entity_prop(
        self, backend: str, value: Any, value_kind: str, *, entity_kind: str
    ) -> tuple[Any, str, str]:
        """Stores ``value`` on a node or relationship property and reads the entity back."""
        raise NotImplementedError


class BoltValueAdapter(BaseValueAdapter):
    """Neo4j and Memgraph adapter covering official, bridge, native, Arrow, and Polars."""

    backends = ("official", "bridge", "native", "native_arrow", "native_polars")

    def __init__(self, engine: str) -> None:
        self.engine = engine
        host, port = _ensure_reachable(engine)
        try:
            import neo4j
        except ImportError as exc:
            raise EngineUnavailableError("neo4j driver is not installed") from exc

        auth = (
            ("neo4j", os.environ.get("VOYAGER_TEST_NEO4J_PASSWORD", "voyagerpass123"))
            if engine == "neo4j"
            else None
        )
        uri = f"bolt://{host}:{port}"
        native_uri = (
            f"bolt://{auth[0]}:{auth[1]}@{host}:{port}?connect_timeout=5"
            if auth is not None
            else f"bolt://{host}:{port}?connect_timeout=5"
        )
        try:
            self.driver = neo4j.GraphDatabase.driver(uri, auth=auth, connection_timeout=5.0)
            self.driver.verify_connectivity()
        except Exception as exc:
            raise EngineUnavailableError(f"{engine} unreachable: {exc}") from exc

        self.bridge = Session(bridge=Neo4jBoltBridge(self.driver), dialect=engine)
        self.native = Session(native_uri, dialect=engine, backend="native", pool_size=2)
        self.label = f"ZzVal_{engine}_{uuid.uuid4().hex[:8]}"

    def close(self) -> None:
        try:
            with self.driver.session() as s:
                s.run(f"MATCH (n:{self.label}) DETACH DELETE n").consume()
        except Exception:
            pass
        for handle in (self.bridge, self.native, self.driver):
            try:
                handle.close()
            except Exception:
                pass

    def _exec_cypher(
        self,
        backend: str,
        statement: str,
        params: dict[str, Any],
        col: str = "v",
        *,
        in_tx: bool = False,
    ) -> tuple[Any, str]:
        if backend == "official":
            off_params = {k: _to_official_bolt_param(v) for k, v in params.items()}
            with self.driver.session() as s:
                if in_tx:
                    rec = s.execute_write(lambda tx: tx.run(statement, off_params).single())
                else:
                    rec = s.run(statement, off_params).single()
                raw = rec[col] if rec is not None else None
                return _canonicalize_official_value(raw, self.engine), ""
        sess = self.bridge if backend == "bridge" else self.native
        if backend == "bridge" and _contains_zoneinfo(params):
            return _run_bolt_bridge_zoneinfo_isolated(
                self.engine, self.driver, statement, params, col
            )
        if in_tx:
            with sess.transaction():
                if backend == "native_arrow":
                    return _extract_arrow_cell(sess.execute_to_arrow(statement, params), col)
                if backend == "native_polars":
                    return _extract_polars_cell(sess.execute_to_polars(statement, params), col)
                row = sess.execute(statement, params).first()
                return (row[col] if row is not None else None), ""
        if backend == "native_arrow":
            return _extract_arrow_cell(sess.execute_to_arrow(statement, params), col)
        if backend == "native_polars":
            return _extract_polars_cell(sess.execute_to_polars(statement, params), col)
        row = sess.execute(statement, params).first()
        return (row[col] if row is not None else None), ""

    def _server_type(self, value: Any, *, stored: bool = False) -> str:
        try:
            if not stored:
                row = self.native.execute("RETURN valueType($v) AS t", {"v": value}).first()
                return str(row["t"]) if row else "unknown"
            tag = uuid.uuid4().hex[:8]
            try:
                self.native.execute(
                    f"CREATE (n:{self.label} {{tag: $tag, v: $v}})",
                    {"tag": tag, "v": value},
                )
                row = self.native.execute(
                    f"MATCH (n:{self.label} {{tag: $tag}}) RETURN valueType(n.v) AS t",
                    {"tag": tag},
                ).first()
                return str(row["t"]) if row else "unknown"
            finally:
                self.native.execute(
                    f"MATCH (n:{self.label} {{tag: $tag}}) DETACH DELETE n",
                    {"tag": tag},
                )
        except Exception as exc:
            return f"<server_type error: {exc}>"

    def run_param(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        val, type_desc = self._exec_cypher(backend, "RETURN $v AS v", {"v": value}, in_tx=in_tx)
        stype = self._server_type(value) if backend == "native" else ""
        return val, type_desc, stype

    def run_stored(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        tag = f"{backend}_{uuid.uuid4().hex[:8]}"
        try:
            create_q = f"CREATE (n:{self.label} {{tag: $tag}}) SET n.v = $v RETURN n.v AS v"
            val, type_desc = self._exec_cypher(
                backend, create_q, {"tag": tag, "v": value}, in_tx=in_tx
            )
            return val, type_desc, ""
        finally:
            try:
                self.native.execute(
                    f"MATCH (n:{self.label} {{tag: $tag}}) DETACH DELETE n", {"tag": tag}
                )
            except Exception:
                pass

    def run_nested_list(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        val, type_desc = self._exec_cypher(backend, "RETURN $v AS v", {"v": [value]})
        return val, type_desc, ""

    def run_nested_map(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        val, type_desc = self._exec_cypher(backend, "RETURN $v AS v", {"v": {"k": value}})
        return val, type_desc, ""

    def run_entity_prop(
        self, backend: str, value: Any, value_kind: str, *, entity_kind: str
    ) -> tuple[Any, str, str]:
        tag = f"{backend}_{entity_kind}_{uuid.uuid4().hex[:8]}"
        try:
            if entity_kind == "node":
                q = f"CREATE (n:{self.label} {{tag: $tag}}) SET n.v = $v RETURN n AS e"
            else:
                q = (
                    f"CREATE (a:{self.label} {{tag: $tag}})-[r:ZZ_VAL_REL]->(b:{self.label} {{tag: $tag}}) "
                    "SET r.v = $v RETURN r AS e"
                )
            entity, type_desc = self._exec_cypher(backend, q, {"tag": tag, "v": value}, col="e")
            return _extract_entity_prop_value(entity), type_desc, ""
        finally:
            try:
                self.native.execute(
                    f"MATCH (n:{self.label} {{tag: $tag}}) DETACH DELETE n", {"tag": tag}
                )
            except Exception:
                pass


class FalkorDBValueAdapter(BaseValueAdapter):
    """FalkorDB adapter covering official, bridge, native, Arrow, and Polars."""

    engine = "falkordb"
    backends = ("official", "bridge", "native", "native_arrow", "native_polars")
    supports_transactions = False

    def __init__(self) -> None:
        host, port = _ensure_reachable("falkordb")
        try:
            import falkordb
        except ImportError as exc:
            raise EngineUnavailableError("falkordb driver is not installed") from exc

        self.graph_name = f"zz_val_fk_{uuid.uuid4().hex[:8]}"
        try:
            self.client = falkordb.FalkorDB(host=host, port=port)
            self.client.connection.ping()
            self.graph = self.client.select_graph(self.graph_name)
        except Exception as exc:
            raise EngineUnavailableError(f"falkordb unreachable: {exc}") from exc

        self.bridge = Session(
            bridge=FalkorDBBridge(self.graph, graph_name=self.graph_name), dialect="falkordb"
        )
        self.native = Session(
            f"redis://{host}:{port}/{self.graph_name}", dialect="falkordb", backend="native"
        )

    def close(self) -> None:
        for handle in (self.bridge, self.native):
            try:
                handle.close()
            except Exception:
                pass
        try:
            self.graph.delete()
        except Exception:
            pass

    def _exec(
        self, backend: str, statement: str, params: dict[str, Any], col: str = "v"
    ) -> tuple[Any, str]:
        if backend == "official":
            off_params = dict(params)
            res = self.graph.query(statement, off_params)
            rows = getattr(res, "result_set", None) or []
            raw = rows[0][0] if rows else None
            return _canonicalize_official_value(raw, "falkordb"), ""
        sess = self.bridge if backend == "bridge" else self.native
        if backend == "native_arrow":
            return _extract_arrow_cell(sess.execute_to_arrow(statement, params), col)
        if backend == "native_polars":
            return _extract_polars_cell(sess.execute_to_polars(statement, params), col)
        row = sess.execute(statement, params).first()
        return (row[col] if row is not None else None), ""

    def run_param(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        if backend == "official":
            expr, pval = _falkordb_expr_and_param(value, value_kind)
            val, desc = self._exec(backend, f"RETURN {expr} AS v", {"v": pval})
        else:
            expr, _ = _falkordb_expr_and_param(value, value_kind)
            val, desc = self._exec(backend, f"RETURN {expr} AS v", {"v": value})
        return val, desc, ""

    def run_stored(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        tag = f"{backend}_{uuid.uuid4().hex[:8]}"
        expr, pval = _falkordb_expr_and_param(value, value_kind)
        param_val = pval if backend == "official" else value
        try:
            q = f"CREATE (n:ZzValStored {{tag: $tag, v: {expr}}}) RETURN n.v AS v"
            return (*self._exec(backend, q, {"tag": tag, "v": param_val}), "")
        finally:
            try:
                self.graph.query("MATCH (n:ZzValStored {tag: $tag}) DETACH DELETE n", {"tag": tag})
            except Exception:
                pass

    def run_nested_list(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        expr, pval = _falkordb_expr_and_param(value, value_kind)
        param_val = pval if backend == "official" else value
        val, desc = self._exec(backend, f"RETURN [{expr}] AS v", {"v": param_val})
        return val, desc, ""

    def run_nested_map(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        expr, pval = _falkordb_expr_and_param(value, value_kind)
        param_val = pval if backend == "official" else value
        val, desc = self._exec(backend, f"RETURN {{k: {expr}}} AS v", {"v": param_val})
        return val, desc, ""

    def run_entity_prop(
        self, backend: str, value: Any, value_kind: str, *, entity_kind: str
    ) -> tuple[Any, str, str]:
        tag = f"{backend}_{entity_kind}_{uuid.uuid4().hex[:8]}"
        expr, pval = _falkordb_expr_and_param(value, value_kind)
        param_val = pval if backend == "official" else value
        try:
            if entity_kind == "node":
                q = f"CREATE (n:ZzValEntity {{tag: $tag, v: {expr}}}) RETURN n AS e"
            else:
                q = (
                    f"CREATE (a:ZzValEntity {{tag: $tag}})-[r:ZZ_VAL_REL {{v: {expr}}}]->"
                    "(b:ZzValEntity {tag: $tag}) RETURN r AS e"
                )
            entity, desc = self._exec(backend, q, {"tag": tag, "v": param_val}, col="e")
            return _extract_entity_prop_value(entity), desc, ""
        finally:
            try:
                self.graph.query("MATCH (n:ZzValEntity {tag: $tag}) DETACH DELETE n", {"tag": tag})
            except Exception:
                pass


class AgeValueAdapter(BaseValueAdapter):
    """Apache AGE adapter covering official (psycopg + agtype), bridge, native, Arrow, and Polars."""

    engine = "age"
    backends = ("official", "bridge", "native", "native_arrow", "native_polars")

    def __init__(self) -> None:
        host, port = _ensure_reachable("age")
        try:
            import psycopg
        except ImportError as exc:
            raise EngineUnavailableError("psycopg is not installed") from exc

        self.graph_name = f"zz_val_age_{uuid.uuid4().hex[:8]}"
        try:
            self.conn = psycopg.connect(
                host=host,
                port=port,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DATABASE,
                connect_timeout=5,
                autocommit=True,
            )
            with self.conn.cursor() as cur:
                cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
                cur.execute(f"SELECT ag_catalog.create_graph('{self.graph_name}')")
        except Exception as exc:
            raise EngineUnavailableError(f"AGE unreachable: {exc}") from exc

        self.bridge = Session(
            bridge=PostgresBridge(self.conn, graph_name=self.graph_name), dialect="age"
        )
        self.native = Session(
            f"postgresql://{PG_USER}:{PG_PASSWORD}@{host}:{port}/{PG_DATABASE}",
            dialect="age",
            graph_name=self.graph_name,
            backend="native",
            pool_size=2,
        )

    def close(self) -> None:
        try:
            self.native.close()
        except Exception:
            pass
        try:
            with self.conn.cursor() as cur:
                cur.execute("LOAD 'age'; SET search_path = ag_catalog, \"$user\", public;")
                cur.execute(f"SELECT ag_catalog.drop_graph('{self.graph_name}', true)")
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass

    def _exec(
        self,
        backend: str,
        cypher_body: str,
        params: dict[str, Any],
        col: str = "v",
        *,
        in_tx: bool = False,
    ) -> tuple[Any, str]:
        sql = (
            f"SELECT * FROM cypher('{self.graph_name}', $$ {cypher_body} $$, %s) AS ({col} agtype)"
        )
        if backend == "official":
            payload = (json.dumps(params),)
            with self.conn.cursor() as cur:
                cur.execute(sql, payload)
                row = cur.fetchone()
            raw = row[0] if row is not None else None
            return decode_agtype(raw) if isinstance(raw, str) else raw, ""
        sess = self.bridge if backend == "bridge" else self.native
        if in_tx:
            with sess.transaction():
                if backend == "native_arrow":
                    return _extract_arrow_cell(sess.execute_to_arrow(sql, params), col)
                if backend == "native_polars":
                    return _extract_polars_cell(sess.execute_to_polars(sql, params), col)
                row = sess.execute(sql, params).first()
                return (row[col] if row is not None else None), ""
        if backend == "native_arrow":
            return _extract_arrow_cell(sess.execute_to_arrow(sql, params), col)
        if backend == "native_polars":
            return _extract_polars_cell(sess.execute_to_polars(sql, params), col)
        row = sess.execute(sql, params).first()
        return (row[col] if row is not None else None), ""

    def run_param(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        val, desc = self._exec(backend, "RETURN $v", {"v": value}, in_tx=in_tx)
        return val, desc, "agtype"

    def run_stored(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        tag = f"{backend}_{uuid.uuid4().hex[:8]}"
        try:
            val, desc = self._exec(
                backend,
                "CREATE (n:ZzValStored {tag: $tag, v: $v}) RETURN n.v",
                {"tag": tag, "v": value},
                in_tx=in_tx,
            )
            return val, desc, "agtype"
        finally:
            try:
                self._exec(
                    "official",
                    "MATCH (n:ZzValStored {tag: $tag}) DETACH DELETE n RETURN count(n)",
                    {"tag": tag},
                )
            except Exception:
                pass

    def run_nested_list(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        val, desc = self._exec(backend, "RETURN $v", {"v": [value]})
        return val, desc, "agtype"

    def run_nested_map(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        val, desc = self._exec(backend, "RETURN $v", {"v": {"k": value}})
        return val, desc, "agtype"

    def run_entity_prop(
        self, backend: str, value: Any, value_kind: str, *, entity_kind: str
    ) -> tuple[Any, str, str]:
        tag = f"{backend}_{entity_kind}_{uuid.uuid4().hex[:8]}"
        try:
            if entity_kind == "node":
                body = "CREATE (n:ZzValEntity {tag: $tag, v: $v}) RETURN n"
            else:
                body = (
                    "CREATE (a:ZzValEntity {tag: $tag})-[r:ZZ_VAL_REL {v: $v}]->"
                    "(b:ZzValEntity {tag: $tag}) RETURN r"
                )
            entity, desc = self._exec(backend, body, {"tag": tag, "v": value}, col="e")
            return _extract_entity_prop_value(entity), desc, "agtype"
        finally:
            try:
                self._exec(
                    "official",
                    "MATCH (n:ZzValEntity {tag: $tag}) DETACH DELETE n RETURN count(n)",
                    {"tag": tag},
                )
            except Exception:
                pass


class Postgres19ValueAdapter(BaseValueAdapter):
    """PostgreSQL 19 Beta 3 SQL/PGQ adapter covering official, bridge, native, Arrow, and Polars."""

    engine = "pg19"
    backends = ("official", "bridge", "native", "native_arrow", "native_polars")

    def __init__(self) -> None:
        host, port = _ensure_reachable("pg19")
        try:
            import psycopg
        except ImportError as exc:
            raise EngineUnavailableError("psycopg is not installed") from exc

        self.schema = f"zz_val_pg19_{uuid.uuid4().hex[:8]}"
        try:
            self.conn = psycopg.connect(
                host=host,
                port=port,
                user=PG_USER,
                password=PG_PASSWORD,
                dbname=PG_DATABASE,
                connect_timeout=5,
                autocommit=True,
            )
            with self.conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA "{self.schema}"')
                cur.execute(f'SET search_path TO "{self.schema}", public')
        except Exception as exc:
            raise EngineUnavailableError(f"PG19 unreachable: {exc}") from exc

        self.bridge = Session(bridge=PostgresBridge(self.conn), dialect="sql_pgq")
        self.native = Session(
            f"postgresql://{PG_USER}:{PG_PASSWORD}@{host}:{port}/{PG_DATABASE}",
            dialect="sql_pgq",
            backend="native",
            pool_size=2,
        )
        self.native.execute(f'SET search_path TO "{self.schema}", public')

    def close(self) -> None:
        try:
            self.native.close()
        except Exception:
            pass
        try:
            with self.conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{self.schema}" CASCADE')
        except Exception:
            pass
        try:
            self.conn.close()
        except Exception:
            pass

    def _exec(
        self,
        backend: str,
        sql_named: str,
        sql_pos: str,
        params_named: dict[str, Any],
        params_pos: tuple[Any, ...],
        col: str = "v",
        *,
        in_tx: bool = False,
        force_jsonb: bool = False,
    ) -> tuple[Any, str]:
        if backend == "official":
            from psycopg.types.json import Jsonb

            adapted = tuple(
                Jsonb(p) if force_jsonb else _adapt_pg_official_value(p) for p in params_pos
            )
            with self.conn.cursor() as cur:
                if in_tx:
                    with self.conn.transaction():
                        cur.execute(sql_pos, adapted)
                        row = cur.fetchone() if cur.description else None
                else:
                    cur.execute(sql_pos, adapted)
                    row = cur.fetchone() if cur.description else None
            raw = row[0] if row is not None else None
            return _canonicalize_official_value(raw, "pg19"), ""

        sess = self.bridge if backend == "bridge" else self.native
        if in_tx:
            with sess.transaction():
                if backend == "native_arrow":
                    return _extract_arrow_cell(sess.execute_to_arrow(sql_named, params_named), col)
                if backend == "native_polars":
                    return _extract_polars_cell(
                        sess.execute_to_polars(sql_named, params_named), col
                    )
                row = sess.execute(sql_named, params_named).first()
                return (row[col] if row is not None else None), ""
        if backend == "native_arrow":
            return _extract_arrow_cell(sess.execute_to_arrow(sql_named, params_named), col)
        if backend == "native_polars":
            return _extract_polars_cell(sess.execute_to_polars(sql_named, params_named), col)
        row = sess.execute(sql_named, params_named).first()
        return (row[col] if row is not None else None), ""

    def _param_cast_suffix(self, value: Any, value_kind: str) -> str:
        if value_kind in ("json_doc", "map"):
            return "::jsonb"
        if value_kind == "str":
            return "::text"
        if value_kind == "list":
            return f"::{_pg_sql_type_for_value(value, 'list')}"
        return ""

    def run_param(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        cast = self._param_cast_suffix(value, value_kind)
        if value_kind == "null":
            val, desc = self._exec(
                backend,
                "SELECT $v::text AS v",
                "SELECT %s::text AS v",
                {"v": None},
                (None,),
                in_tx=in_tx,
            )
            return val, desc, "text"
        sql_n = f"SELECT $v{cast} AS v"
        sql_p = f"SELECT %s{cast} AS v"
        if value_kind == "duration" and backend == "official":
            sql_p = "SELECT %s::interval AS v"
        val, desc = self._exec(
            backend,
            sql_n,
            sql_p,
            {"v": value},
            (value,),
            in_tx=in_tx,
            force_jsonb=(cast == "::jsonb"),
        )
        stype = ""
        if backend == "native":
            try:
                trow = self.native.execute(
                    f"SELECT pg_typeof($v{cast})::text AS t", {"v": value}
                ).first()
                stype = str(trow["t"]) if trow else "unknown"
            except Exception as exc:
                stype = f"<err: {exc}>"
        return val, desc, stype

    def run_stored(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        table = f"zz_t_{uuid.uuid4().hex[:8]}"
        sql_type = _pg_sql_type_for_value(value, value_kind)
        fq = f'"{self.schema}"."{table}"'
        with self.conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {fq} (id integer PRIMARY KEY, v {sql_type})")
        try:
            cast = "::jsonb" if sql_type == "jsonb" else ""
            if backend == "official":
                from psycopg.types.json import Jsonb

                pval = Jsonb(value) if sql_type == "jsonb" else _adapt_pg_official_value(value)
                with self.conn.cursor() as cur:
                    if in_tx:
                        with self.conn.transaction():
                            cur.execute(f"INSERT INTO {fq} (id, v) VALUES (1, %s{cast})", (pval,))
                            cur.execute(f"SELECT v, pg_typeof(v)::text FROM {fq} WHERE id = 1")
                            row = cur.fetchone()
                    else:
                        cur.execute(f"INSERT INTO {fq} (id, v) VALUES (1, %s{cast})", (pval,))
                        cur.execute(f"SELECT v, pg_typeof(v)::text FROM {fq} WHERE id = 1")
                        row = cur.fetchone()
                return (
                    (_canonicalize_official_value(row[0], "pg19") if row else None),
                    "",
                    (row[1] if row else ""),
                )

            sess = self.bridge if backend == "bridge" else self.native
            ins = f"INSERT INTO {fq} (id, v) VALUES ($id, $v{cast})"
            sel = f"SELECT v FROM {fq} WHERE id = 1"
            if in_tx:
                with sess.transaction():
                    sess.execute(ins, {"id": 1, "v": value})
                    if backend == "native_arrow":
                        val, desc = _extract_arrow_cell(sess.execute_to_arrow(sel), "v")
                    elif backend == "native_polars":
                        val, desc = _extract_polars_cell(sess.execute_to_polars(sel), "v")
                    else:
                        r = sess.execute(sel).first()
                        val, desc = (r["v"] if r else None), ""
            else:
                sess.execute(ins, {"id": 1, "v": value})
                if backend == "native_arrow":
                    val, desc = _extract_arrow_cell(sess.execute_to_arrow(sel), "v")
                elif backend == "native_polars":
                    val, desc = _extract_polars_cell(sess.execute_to_polars(sel), "v")
                else:
                    r = sess.execute(sel).first()
                    val, desc = (r["v"] if r else None), ""
            return val, desc, sql_type
        finally:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(f"DROP TABLE IF EXISTS {fq}")
            except Exception:
                pass

    def run_nested_list(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        if value_kind in ("json_doc", "map"):
            safe = _pg_json_safe(value)
            return self.run_param(backend, [safe], "json_doc")
        if value_kind == "list":
            safe = _pg_json_safe(value)
            # The nested-list path wraps the value in one more list. Use a typed
            # multidimensional array when the shape is rectangular and fall back
            # to jsonb for mixed or ragged lists, matching the 1-D behaviour.
            if _pg_nested_array_sql_type([safe]) is not None:
                return self.run_param(backend, [safe], "list")
            return self.run_param(backend, [safe], "json_doc")
        if value_kind == "null":
            return self.run_param(backend, [None], "list")
        return self.run_param(backend, [value], "list")

    def run_nested_map(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        return self.run_param(backend, {"k": _pg_json_safe(value)}, "json_doc")

    def run_entity_prop(
        self, backend: str, value: Any, value_kind: str, *, entity_kind: str
    ) -> tuple[Any, str, str]:
        # In SQL/PGQ, node and relationship properties live in vertex/edge tables.
        return self.run_stored(backend, value, value_kind)


def _adapt_pg_official_value(val: Any) -> Any:
    if isinstance(val, Duration):
        if val.months == 0 and val.nanoseconds % 1000 == 0:
            return val.to_timedelta()
        total_us = val.seconds * 1_000_000 + val.nanoseconds // 1_000
        sign = "-" if total_us < 0 else ""
        abs_us = abs(total_us)
        secs, us = divmod(abs_us, 1_000_000)
        return f"{val.months} mons {val.days} days {sign}{secs}.{us:06d} seconds"
    if isinstance(val, list):
        return [_adapt_pg_official_value(x) for x in val]
    return val


def _pg_json_safe(val: Any) -> Any:
    """Converts non-JSON Python scalars cleanly when embedding values inside ``jsonb`` tests."""
    if val is None or isinstance(val, (bool, int, float, str)):
        return val
    if isinstance(val, list):
        return [_pg_json_safe(x) for x in val]
    if isinstance(val, dict):
        return {str(k): _pg_json_safe(v) for k, v in val.items()}
    return str(val)


class DuckDBValueAdapter(BaseValueAdapter):
    """DuckDB embedded adapter covering official, bridge, bridge_arrow, and bridge_polars."""

    engine = "duckdb"
    backends = ("official", "bridge", "bridge_arrow", "bridge_polars")

    def __init__(self) -> None:
        try:
            import duckdb
        except ImportError as exc:
            raise EngineUnavailableError("duckdb is not installed") from exc
        self.con = duckdb.connect(":memory:")
        self.bridge = Session(bridge=DuckDbBridge(self.con), dialect="duckpgq")

    def close(self) -> None:
        try:
            self.bridge.close()
        except Exception:
            pass
        try:
            self.con.close()
        except Exception:
            pass

    def _exec(
        self, backend: str, sql: str, params: dict[str, Any], col: str = "v", *, in_tx: bool = False
    ) -> tuple[Any, str]:
        if backend == "official":
            off_params = _adapt_duckdb_official_params(params)
            if in_tx:
                self.con.execute("BEGIN")
                try:
                    rel = self.con.execute(sql, off_params)
                    desc = rel.description
                    row = rel.fetchone()
                    self.con.execute("COMMIT")
                except Exception:
                    self.con.execute("ROLLBACK")
                    raise
            else:
                rel = self.con.execute(sql, off_params)
                desc = rel.description
                row = rel.fetchone()
            raw = row[0] if row is not None else None
            if (
                raw is not None
                and isinstance(raw, str)
                and desc
                and len(desc[0]) > 1
                and str(desc[0][1]).upper() == "JSON"
            ):
                return json.loads(raw), ""
            return _canonicalize_official_value(raw, "duckdb"), ""
        if in_tx:
            with self.bridge.transaction():
                if backend == "bridge_arrow":
                    return _extract_arrow_cell(self.bridge.execute_to_arrow(sql, params), col)
                if backend == "bridge_polars":
                    return _extract_polars_cell(self.bridge.execute_to_polars(sql, params), col)
                row = self.bridge.execute(sql, params).first()
                return (row[col] if row is not None else None), ""
        if backend == "bridge_arrow":
            return _extract_arrow_cell(self.bridge.execute_to_arrow(sql, params), col)
        if backend == "bridge_polars":
            return _extract_polars_cell(self.bridge.execute_to_polars(sql, params), col)
        row = self.bridge.execute(sql, params).first()
        return (row[col] if row is not None else None), ""

    def run_param(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        if value_kind in ("json_doc", "map"):
            sql = "SELECT $v::JSON AS v"
            pval = json.dumps(value) if backend == "official" else value
        elif (
            value_kind == "int"
            and isinstance(value, int)
            and (value < -(2**63) or value > 2**63 - 1)
        ):
            sql = "SELECT $v::HUGEINT AS v"
            pval = value
        elif value_kind == "time_offset":
            sql = "SELECT $v::TIMETZ AS v"
            pval = value
        elif (
            value_kind == "duration"
            and backend == "official"
            and isinstance(value, Duration)
            and value.months != 0
        ):
            sql = "SELECT $v::INTERVAL AS v"
            pval = value
        else:
            sql = "SELECT $v AS v"
            pval = value
        val, desc = self._exec(backend, sql, {"v": pval}, in_tx=in_tx)
        stype = ""
        if backend == "bridge":
            try:
                trow = self.con.execute(
                    "SELECT typeof($v) AS t",
                    {"v": _adapt_duckdb_official_params({"v": value})["v"]},
                ).fetchone()
                stype = str(trow[0]) if trow else "unknown"
            except Exception as exc:
                stype = f"<err: {exc}>"
        return val, desc, stype

    def run_stored(
        self, backend: str, value: Any, value_kind: str, *, in_tx: bool = False
    ) -> tuple[Any, str, str]:
        table = f"zz_duck_{uuid.uuid4().hex[:8]}"
        sql_type = _duckdb_sql_type_for_value(value, value_kind)
        self.con.execute(f'CREATE TABLE "{table}" (id INT, v {sql_type})')
        try:
            pval = json.dumps(_pg_json_safe(value)) if sql_type == "JSON" else value
            ins = f'INSERT INTO "{table}" VALUES ($id, $v)'
            sel = f'SELECT v FROM "{table}" WHERE id = 1'
            if backend == "official":
                self.con.execute(
                    ins, {"id": 1, "v": _adapt_duckdb_official_params({"v": pval})["v"]}
                )
                sel_rel = self.con.execute(sel)
                desc = sel_rel.description
                row = sel_rel.fetchone()
                raw = row[0] if row is not None else None
                if (
                    raw is not None
                    and isinstance(raw, str)
                    and desc
                    and len(desc[0]) > 1
                    and str(desc[0][1]).upper() == "JSON"
                ):
                    return json.loads(raw), "", sql_type
                return _canonicalize_official_value(raw, "duckdb"), "", sql_type
            if in_tx:
                with self.bridge.transaction():
                    self.bridge.execute(ins, {"id": 1, "v": pval})
                    if backend == "bridge_arrow":
                        val, desc = _extract_arrow_cell(self.bridge.execute_to_arrow(sel), "v")
                    elif backend == "bridge_polars":
                        val, desc = _extract_polars_cell(self.bridge.execute_to_polars(sel), "v")
                    else:
                        r = self.bridge.execute(sel).first()
                        val, desc = (r["v"] if r else None), ""
            else:
                self.bridge.execute(ins, {"id": 1, "v": pval})
                if backend == "bridge_arrow":
                    val, desc = _extract_arrow_cell(self.bridge.execute_to_arrow(sel), "v")
                elif backend == "bridge_polars":
                    val, desc = _extract_polars_cell(self.bridge.execute_to_polars(sel), "v")
                else:
                    r = self.bridge.execute(sel).first()
                    val, desc = (r["v"] if r else None), ""
            return val, desc, sql_type
        finally:
            try:
                self.con.execute(f'DROP TABLE IF EXISTS "{table}"')
            except Exception:
                pass

    def run_nested_list(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        return self.run_stored(backend, [value], "list")

    def run_nested_map(self, backend: str, value: Any, value_kind: str) -> tuple[Any, str, str]:
        return self.run_param(backend, {"k": _pg_json_safe(value)}, "json_doc")

    def run_entity_prop(
        self, backend: str, value: Any, value_kind: str, *, entity_kind: str
    ) -> tuple[Any, str, str]:
        return self.run_stored(backend, value, value_kind)


def _contains_zoneinfo(val: Any) -> bool:
    """Returns whether ``val`` contains a ``zoneinfo.ZoneInfo`` tzinfo anywhere."""
    import zoneinfo

    if isinstance(val, datetime.datetime):
        return isinstance(val.tzinfo, zoneinfo.ZoneInfo)
    if isinstance(val, dict):
        return any(_contains_zoneinfo(v) for v in val.values())
    if isinstance(val, (list, tuple)):
        return any(_contains_zoneinfo(x) for x in val)
    return False


def _run_bolt_bridge_zoneinfo_isolated(
    engine: str,
    _driver: Any,
    statement: str,
    params: dict[str, Any],
    col: str,
) -> tuple[Any, str]:
    """Runs Neo4jBoltBridge with ZoneInfo in a subprocess so neo4j==6.1.0 SIGSEGV cannot kill pytest."""
    import pickle
    import subprocess
    import sys

    host, port = engine_endpoint(engine)
    auth = (
        ("neo4j", os.environ.get("VOYAGER_TEST_NEO4J_PASSWORD", "voyagerpass123"))
        if engine == "neo4j"
        else None
    )
    payload = pickle.dumps((host, port, auth, engine, statement, params, col))
    script = (
        "import pickle, sys, neo4j; "
        "from voyager_ogm import Session; "
        "from voyager_ogm.bridge import Neo4jBoltBridge; "
        "host, port, auth, engine, stmt, params, col = pickle.loads(sys.stdin.buffer.read()); "
        "drv = neo4j.GraphDatabase.driver(f'bolt://{host}:{port}', auth=auth, connection_timeout=5.0); "
        "s = Session(bridge=Neo4jBoltBridge(drv), dialect=engine); "
        "r = s.execute(stmt, params).first(); "
        "sys.stdout.buffer.write(pickle.dumps(r[col] if r else None)); "
        "s.close(); drv.close()"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        input=payload,
        capture_output=True,
        timeout=10,
        check=False,
    )
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", "replace").strip()
        raise RuntimeError(
            f"Neo4jBoltBridge crashed on zoneinfo.ZoneInfo input (returncode={proc.returncode}): {err}"
        )
    return pickle.loads(proc.stdout), ""


def _extract_entity_prop_value(entity: Any) -> Any:
    """Extracts property ``v`` from GraphNode/GraphRelationship or Arrow/Polars entity structs."""
    if isinstance(entity, (GraphNode, GraphRelationship)):
        return entity.properties.get("v")
    if isinstance(entity, str) and entity.startswith("{"):
        try:
            parsed = json.loads(entity)
            if isinstance(parsed, dict) and "properties" in parsed:
                entity = parsed
        except Exception:
            pass
    if isinstance(entity, dict) and isinstance(entity.get("properties"), dict):
        return entity["properties"].get("v")
    return entity


def _adapt_duckdb_official_value(v: Any) -> Any:
    if isinstance(v, datetime.time) and v.tzinfo is not None:
        off = v.utcoffset()
        if off is not None and off.days < 0:
            return v.isoformat()
        return v
    if isinstance(v, Duration):
        if v.months == 0 and v.nanoseconds % 1000 == 0:
            return v.to_timedelta()
        total_us = v.seconds * 1_000_000 + v.nanoseconds // 1_000
        sign = "-" if total_us < 0 else ""
        abs_us = abs(total_us)
        secs, us = divmod(abs_us, 1_000_000)
        return f"{v.months} months {v.days} days {sign}{secs}.{us:06d} seconds"
    if isinstance(v, list):
        return [_adapt_duckdb_official_value(x) for x in v]
    return v


def _adapt_duckdb_official_params(params: dict[str, Any]) -> dict[str, Any]:
    return {k: _adapt_duckdb_official_value(v) for k, v in params.items()}


def open_adapter(engine: str) -> BaseValueAdapter:
    """Instantiates the live adapter for ``engine``."""
    if engine in ("neo4j", "memgraph"):
        return BoltValueAdapter(engine)
    if engine == "falkordb":
        return FalkorDBValueAdapter()
    if engine == "age":
        return AgeValueAdapter()
    if engine == "pg19":
        return Postgres19ValueAdapter()
    if engine == "duckdb":
        return DuckDBValueAdapter()
    raise ValueError(f"Unknown engine: {engine!r}")
