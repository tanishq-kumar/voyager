"""DuckDB ``execute()`` fast path runs statements exactly once (#461)."""

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


def _row_count(connection: Any, table: str) -> int:
    """Counts rows directly on the underlying DuckDB connection."""
    rows = connection.execute(f"SELECT count(*) FROM {table}").fetchall()
    return int(rows[0][0])


def test_simple_read_uses_eager_execute_once(counting_duckdb) -> None:
    """A plain point lookup runs through ``con.execute()`` once, without a relation."""
    inner, counting, session = counting_duckdb
    inner.execute("CREATE TABLE zz_fast_users (id BIGINT, name VARCHAR)")
    inner.execute("INSERT INTO zz_fast_users VALUES (1, 'Ada'), (2, 'Grace')")

    counting.reset()
    row = session.execute("SELECT id, name FROM zz_fast_users WHERE id = $id", {"id": 1}).first()
    assert row == {"id": 1, "name": "Ada"}
    assert len(counting.execute_calls) == 1
    assert counting.sql_calls == []


def test_dml_without_returning_runs_exactly_once(counting_duckdb) -> None:
    """INSERT/UPDATE/DELETE without RETURNING run once and change rows once."""
    inner, counting, session = counting_duckdb
    inner.execute("CREATE TABLE zz_fast_dml (id BIGINT, name VARCHAR)")

    counting.reset()
    session.execute("INSERT INTO zz_fast_dml VALUES (1, 'Ada')")
    assert len(counting.execute_calls) == 1
    assert counting.sql_calls == []
    assert _row_count(inner, "zz_fast_dml") == 1

    counting.reset()
    session.execute("UPDATE zz_fast_dml SET name = 'Grace' WHERE id = 1")
    assert len(counting.execute_calls) == 1
    assert counting.sql_calls == []
    assert inner.execute("SELECT name FROM zz_fast_dml WHERE id = 1").fetchone() == ("Grace",)

    counting.reset()
    session.execute("DELETE FROM zz_fast_dml WHERE id = 1")
    assert len(counting.execute_calls) == 1
    assert counting.sql_calls == []
    assert _row_count(inner, "zz_fast_dml") == 0


def test_dml_with_returning_runs_exactly_once(counting_duckdb) -> None:
    """INSERT ... RETURNING stays on the single-execution relation path."""
    inner, counting, session = counting_duckdb
    inner.execute("CREATE SEQUENCE zz_fast_seq START 1")
    inner.execute("CREATE TABLE zz_fast_returning (id BIGINT, name VARCHAR)")

    counting.reset()
    rows = session.execute(
        "INSERT INTO zz_fast_returning VALUES (nextval('zz_fast_seq'), 'Ada') RETURNING id, name"
    )
    assert rows.first() == {"id": 1, "name": "Ada"}
    assert len(counting.sql_calls) == 1
    assert counting.execute_calls == []
    assert _row_count(inner, "zz_fast_returning") == 1
    assert inner.execute("SELECT currval('zz_fast_seq')").fetchone() == (1,)

    counting.reset()
    updated = session.execute(
        "UPDATE zz_fast_returning SET name = 'Grace' WHERE id = 1 RETURNING id, name"
    )
    assert updated.first() == {"id": 1, "name": "Grace"}
    assert len(counting.sql_calls) == 1
    assert counting.execute_calls == []

    counting.reset()
    deleted = session.execute("DELETE FROM zz_fast_returning WHERE id = 1 RETURNING id, name")
    assert deleted.first() == {"id": 1, "name": "Grace"}
    assert len(counting.sql_calls) == 1
    assert counting.execute_calls == []
    assert _row_count(inner, "zz_fast_returning") == 0


def test_table_read_with_timetz_and_interval_keeps_both(counting_duckdb) -> None:
    """A table read with TIMETZ and calendar INTERVAL keeps the offset and months."""
    import datetime

    from voyager_ogm import Duration

    inner, _counting, session = counting_duckdb
    inner.execute("CREATE TABLE zz_fast_temporal (t TIMETZ, iv INTERVAL)")
    inner.execute(
        "INSERT INTO zz_fast_temporal VALUES ('10:00:00+02'::TIMETZ, INTERVAL '1 year 2 months')"
    )

    row = session.execute("SELECT t, iv FROM zz_fast_temporal").first()
    assert row is not None
    assert row["t"] == datetime.time(10, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    assert row["iv"] == Duration(months=14)
