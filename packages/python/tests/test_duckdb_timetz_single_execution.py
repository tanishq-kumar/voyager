"""Regression tests that DuckDB TIMETZ exports execute a statement only once (#439)."""

from __future__ import annotations

from typing import Any

import pytest
from voyager_ogm import Session
from voyager_ogm.bridge import DuckDbBridge


class _CountingConnection:
    """Delegates to a DuckDB connection and records execution calls."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.execute_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.sql_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def reset(self) -> None:
        """Clears recorded calls."""
        self.execute_calls.clear()
        self.sql_calls.clear()

    def execute(self, *args: Any, **kwargs: Any) -> Any:
        """Records and forwards an eager DB-API execution."""
        self.execute_calls.append((args, kwargs))
        return self.inner.execute(*args, **kwargs)

    def sql(self, *args: Any, **kwargs: Any) -> Any:
        """Records and forwards a lazy relation creation."""
        self.sql_calls.append((args, kwargs))
        return self.inner.sql(*args, **kwargs)


@pytest.fixture
def counting_duckdb():
    """Yields a counted in-memory DuckDB connection plus a bridge Session."""
    duckdb = pytest.importorskip("duckdb")
    inner = duckdb.connect(":memory:")
    counting = _CountingConnection(inner)
    session = Session(bridge=DuckDbBridge(counting), dialect="duckpgq")
    try:
        yield inner, counting, session
    finally:
        session.close()
        inner.close()


def test_timetz_arrow_and_polars_run_the_statement_once(counting_duckdb) -> None:
    """A volatile TIMETZ query advances its sequence exactly once per export."""
    inner, counting, session = counting_duckdb
    inner.execute("CREATE SEQUENCE zz_timetz_seq START 1")
    statement = "SELECT nextval('zz_timetz_seq') AS n, '10:00:00+02:00'::TIMETZ AS t"

    counting.reset()
    table = session.execute_to_arrow(statement)
    assert table.column("n").to_pylist() == [1]
    assert table.column("t").to_pylist() == ["10:00:00+02:00"]
    assert len(counting.sql_calls) == 1
    assert counting.execute_calls == []
    assert _currval(inner, "zz_timetz_seq") == 1

    counting.reset()
    frame = session.execute_to_polars(statement)
    assert frame["n"].to_list() == [2]
    assert frame["t"].to_list() == ["10:00:00+02:00"]
    assert len(counting.sql_calls) == 1
    assert counting.execute_calls == []
    assert _currval(inner, "zz_timetz_seq") == 2


def _currval(connection: Any, sequence: str) -> int:
    """Reads a sequence's current value, draining the DuckDB result fully."""
    rows = connection.execute(f"SELECT currval('{sequence}')").fetchall()
    return int(rows[0][0])


def test_multi_statement_timetz_uses_one_eager_execution(counting_duckdb) -> None:
    """Multi-statement input keeps the eager path and still executes once."""
    _inner, counting, session = counting_duckdb
    statement = "SELECT 1 AS n; SELECT '11:00:00+03'::TIMETZ AS t"

    counting.reset()
    table = session.execute_to_arrow(statement)
    # The eager DB-API row path renders the offset through ``datetime.time.isoformat``.
    assert table.column("t").to_pylist() == ["11:00:00+03:00"]
    assert counting.sql_calls == []
    assert len(counting.execute_calls) == 1


def test_lazy_relation_clears_a_pending_dbapi_result(counting_duckdb) -> None:
    """A bare fetchone() leaves a result that DuckDB's lazy sql() must clear first."""
    inner, _counting, session = counting_duckdb
    inner.execute("SELECT 1").fetchone()

    table = session.execute_to_arrow("SELECT '10:00:00+02:00'::TIMETZ AS t")
    assert table.column("t").to_pylist() == ["10:00:00+02:00"]
