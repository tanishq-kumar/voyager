"""DuckDB bridge implementations with zero-copy Arrow and Polars output."""

from __future__ import annotations

import asyncio
import datetime
import json
import math
import re
import time
import uuid
import warnings
from collections.abc import Callable, Mapping, Sequence
from decimal import Decimal
from typing import Any

import polars as pl

from voyager_ogm.bridges._duckdb_arrow import (
    _DUCKDB_JSON_TYPE_IDS,
    _DUCKDB_TIMETZ_TYPE_IDS,
    _duckdb_array_child_dtype,
    _duckdb_arrow_table_to_canonical,
    _duckdb_complete_pending,
    _duckdb_description_has_interval,
    _duckdb_description_has_timetz,
    _duckdb_description_has_union_json,
    _duckdb_list_child_dtype,
    _duckdb_relation_description,
    _duckdb_rows_to_arrow,
    _duckdb_timetz_projection,
    _duckdb_type_children_map,
    _duckdb_type_has,
    _duckdb_type_id,
    _parse_duckdb_timetz_python,
)
from voyager_ogm.bridges.base import (
    BridgeResult,
    BulkExecutionResult,
    validate_pagination_parameters,
)
from voyager_ogm.codecs import (
    arrow_table_to_polars,
    is_month_day_nano_interval,
    mdn_to_duration,
    records_to_polars,
)
from voyager_ogm.entities import json_default
from voyager_ogm.ingestion import BulkIngestionPlan
from voyager_ogm.types import Duration, Json

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
                return arrow_table_to_polars(
                    _duckdb_rows_to_arrow(description, relation.fetchall())
                )
            return arrow_table_to_polars(_duckdb_arrow_table_to_canonical(arrow_table, description))

        rel = self.con.execute(stmt, params)
        if not rel.description:
            return pl.DataFrame()
        description = tuple(rel.description)
        if _duckdb_description_has_timetz(description):
            return arrow_table_to_polars(_duckdb_rows_to_arrow(description, rel.fetchall()))
        arrow_table = self._arrow_table(rel)
        if arrow_table is not None:
            arrow_table = _duckdb_arrow_table_to_canonical(arrow_table, description)
            return arrow_table_to_polars(arrow_table)
        return records_to_polars(_records_from_duckdb_rows(description, rel.fetchall()))

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
    if is_month_day_nano_interval(arr.type):
        return [None if v is None else mdn_to_duration(v) for v in arr.to_pylist()]
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
