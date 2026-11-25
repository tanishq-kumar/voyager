"""Regression tests for SQL/PGQ string predicates (#318).

``STARTS WITH``, ``ENDS WITH`` and ``CONTAINS`` used to compile to ``LIKE`` with
the search value as the pattern, so ``%`` and ``_`` in the value acted as
wildcards. They now compile to wildcard-free functions and match literally, as
in Cypher. A NULL property or NULL search value still excludes the row.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
from conftest import is_port_open as _is_port_open
from voyager_ogm import Field, Node, Query, node, reset_alias_counters
from voyager_ogm.bridge import DuckDbBridge, PostgresBridge
from voyager_ogm.session import Session

psycopg: Any = None
try:
    import psycopg
except ImportError:
    pass

duckdb: Any = None
try:
    import duckdb
except ImportError:
    pass

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
POSTGRES19_URI = f"postgresql://postgres:voyagerpass123@{HOST}:5456/voyager_graph"
GRAPH = "fix318_graph"
PERSON = "fix318_person"

NAMES = ["10%_discount", "1000_items", "test_user", "test%user", "a\\b", "ab", None]
ROWS = [(index + 1, name) for index, name in enumerate(NAMES)]
VALUES = ["10%", "test_", "\\", "%", "_", "user", ""]
METHODS = ["startswith", "endswith", "contains"]


@node(label="Person")
class Person(Node):
    id = Field()
    name = Field()


@pytest.fixture(autouse=True)
def _reset():
    reset_alias_counters()


def _compile(dialect: str, method: str, value: Any) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .where(getattr(p.name, method)(value))
        .return_(p.name)
        .compile(dialect, graph_name=GRAPH)
    )
    return compiled.statement, compiled.parameters


def _expected(method: str, value: str) -> list[str]:
    check = {
        "startswith": str.startswith,
        "endswith": str.endswith,
        "contains": str.__contains__,
    }[method]
    return sorted(name for name in NAMES if name is not None and check(name, value))


def _names(rows: list[dict[str, Any]]) -> list[str]:
    return sorted(row["name"] for row in rows)


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_string_predicates_compile_without_like(dialect: str) -> None:
    statement, parameters = _compile(dialect, "startswith", "10%")
    assert "starts_with(p.name, $p0)" in statement
    assert parameters == {"p0": "10%"}

    statement, parameters = _compile(dialect, "contains", "a_b")
    assert "strpos(p.name, $p0) > 0" in statement
    assert parameters == {"p0": "a_b"}

    statement, parameters = _compile(dialect, "endswith", "\\")
    assert "right(p.name, length($p0)) = $p0" in statement
    assert parameters == {"p0": "\\"}

    for method in METHODS:
        assert "LIKE" not in _compile(dialect, method, "x")[0]


def _has_duckpgq() -> bool:
    if duckdb is None:
        return False
    try:
        connection = duckdb.connect(":memory:")
        connection.execute("LOAD duckpgq;")
        connection.close()
        return True
    except Exception:
        return False


HAS_DUCKPGQ = _has_duckpgq()


@pytest.fixture
def duckpgq_bridge():
    connection = duckdb.connect()
    connection.execute("LOAD duckpgq;")
    connection.execute(f"CREATE TABLE {PERSON} (id INT PRIMARY KEY, name TEXT);")
    connection.executemany(f"INSERT INTO {PERSON} VALUES (?, ?);", ROWS)
    connection.execute(f'CREATE PROPERTY GRAPH {GRAPH} VERTEX TABLES ({PERSON} LABEL "Person");')
    yield DuckDbBridge(connection)
    connection.close()


@pytest.mark.skipif(not HAS_DUCKPGQ, reason="DuckPGQ extension is not available")
@pytest.mark.parametrize("method", METHODS)
@pytest.mark.parametrize("value", VALUES)
def test_duckpgq_matches_search_value_literally(duckpgq_bridge, method: str, value: str) -> None:
    statement, parameters = _compile("duckpgq", method, value)
    rows = duckpgq_bridge.execute(statement, parameters)
    assert _names(rows) == _expected(method, value), statement


@pytest.mark.skipif(not HAS_DUCKPGQ, reason="DuckPGQ extension is not available")
@pytest.mark.parametrize("method", METHODS)
def test_duckpgq_null_search_value_excludes_every_row(duckpgq_bridge, method: str) -> None:
    statement, parameters = _compile("duckpgq", method, "x")
    rows = duckpgq_bridge.execute(statement, {"p0": None})
    assert rows == [], statement


@pytest.mark.live
class TestPostgres19StringPredicates:
    """PostgreSQL 19 Beta 3 GRAPH_TABLE execution of #318 through both backends."""

    @pytest.fixture
    def pg19(self):
        if psycopg is None:
            pytest.skip("psycopg is not installed in this environment")
        if not _is_port_open(HOST, 5456):
            pytest.skip(f"PostgreSQL 19 container not available on {HOST}:5456")
        conn_str = (
            f"host={HOST} port=5456 user=postgres password=voyagerpass123 "
            "dbname=voyager_graph connect_timeout=3"
        )
        try:
            conn = psycopg.connect(conn_str, autocommit=True)
        except Exception as exc:
            pytest.skip(f"PostgreSQL 19 container not available on {HOST}:5456: {exc}")
        with conn.cursor() as cur:
            cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {GRAPH} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PERSON} CASCADE;")
            cur.execute(f"CREATE TABLE {PERSON} (id INT PRIMARY KEY, name TEXT);")
            cur.executemany(f"INSERT INTO {PERSON} VALUES (%s, %s);", ROWS)
            cur.execute(f'CREATE PROPERTY GRAPH {GRAPH} VERTEX TABLES ({PERSON} LABEL "Person");')
        yield conn
        with conn.cursor() as cur:
            cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {GRAPH} CASCADE;")
            cur.execute(f"DROP TABLE IF EXISTS {PERSON} CASCADE;")
        conn.close()

    @pytest.mark.parametrize("method", METHODS)
    @pytest.mark.parametrize("value", VALUES)
    def test_bridge_and_native_match_search_value_literally(
        self, pg19, method: str, value: str
    ) -> None:
        bridge = PostgresBridge(pg19)
        native = Session(POSTGRES19_URI, backend="native", dialect="sql_pgq")
        statement, parameters = _compile("sql_pgq", method, value)
        expected = _expected(method, value)
        assert _names(bridge.execute(statement, parameters)) == expected, statement
        assert _names(native.execute(statement, parameters)) == expected, statement

    @pytest.mark.parametrize("method", METHODS)
    def test_null_search_value_excludes_every_row(self, pg19, method: str) -> None:
        bridge = PostgresBridge(pg19)
        statement, _ = _compile("sql_pgq", method, "x")
        assert bridge.execute(statement, {"p0": None}) == [], statement
