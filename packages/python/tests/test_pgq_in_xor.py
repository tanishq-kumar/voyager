"""Regression tests for SQL/PGQ ``IN`` lists (#303) and ``XOR`` (#304).

SQL:2023 PGQ emitted `x IN [$p0, $p1]` for a literal list, `x IN $p0` for a
list parameter and `a XOR b` for XOR. PostgreSQL SQL/PGQ (experimental: PostgreSQL 19 Beta 1-3 and 20 development builds) rejects all three. These
tests cover the emitted SQL on SQL/PGQ and DuckPGQ, DuckPGQ execution, and live
PostgreSQL 19 Beta 3 execution for the literal, parameter, negated, empty and NULL
cases, plus the XOR three-valued result.
"""

from __future__ import annotations

from typing import Any

import pytest
from conftest import get_test_host
from conftest import is_port_open as _is_port_open
from voyager_ogm import Field, Node, Query, compile_query_from_spec, node, reset_alias_counters
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

HOST = get_test_host()
POSTGRES19_URI = f"postgresql://postgres:voyagerpass123@{HOST}:5456/voyager_graph"
GRAPH = "fix303_304_graph"
PERSON = "fix303_304_person"

ROWS = [
    (1, "Alice", 30, "Paris"),
    (2, "Bob", 25, "London"),
    (3, "Carol", None, None),
    (4, None, 40, "Berlin"),
    (5, "Dave", 35, "Paris"),
]


@node(label="Person")
class Person(Node):
    id = Field()
    name = Field()
    age = Field()
    city = Field()


@pytest.fixture(autouse=True)
def _reset():
    reset_alias_counters()


def _membership_spec(op: str, right: Any) -> dict[str, Any]:
    return {
        "spec_version": 1,
        "matches": [
            {
                "paths": [[["node", "p", ["Person"]]]],
                "where": ["bin", op, ["prop", "p", "name"], right],
            }
        ],
        "projections": [["field", "p", "name", None]],
    }


def _literal_list(*names: str) -> list[Any]:
    return ["list", [["lit", name] for name in names]]


def _in_literal(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    compiled = compile_query_from_spec(
        _membership_spec("in", _literal_list("Alice", "Bob")), dialect, graph_name=graph
    )
    return compiled["statement"], compiled["parameters"]


def _in_parameter(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .where(p.name.in_(["Alice", "Bob"]))
        .return_(p.name)
        .compile(dialect, graph_name=graph)
    )
    return compiled.statement, compiled.parameters


def _not_in_literal(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    compiled = compile_query_from_spec(
        _membership_spec("not_in", _literal_list("Alice", "Bob")), dialect, graph_name=graph
    )
    return compiled["statement"], compiled["parameters"]


def _not_in_parameter(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .where(p.name.not_in(["Alice", "Bob"]))
        .return_(p.name)
        .compile(dialect, graph_name=graph)
    )
    return compiled.statement, compiled.parameters


def _not_over_in(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .where(~p.name.in_(["Alice", "Bob"]))
        .return_(p.name)
        .compile(dialect, graph_name=graph)
    )
    return compiled.statement, compiled.parameters


def _empty_in_literal(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    compiled = compile_query_from_spec(
        _membership_spec("in", ["list", []]), dialect, graph_name=graph
    )
    return compiled["statement"], compiled["parameters"]


def _empty_not_in_literal(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    compiled = compile_query_from_spec(
        _membership_spec("not_in", ["list", []]), dialect, graph_name=graph
    )
    return compiled["statement"], compiled["parameters"]


def _empty_in_parameter(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p).where(p.name.in_([])).return_(p.name).compile(dialect, graph_name=graph)
    )
    return compiled.statement, compiled.parameters


def _empty_not_in_parameter(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p).where(p.name.not_in([])).return_(p.name).compile(dialect, graph_name=graph)
    )
    return compiled.statement, compiled.parameters


def _in_parameter_with_null(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .where(p.name.in_(["Alice", None]))
        .return_(p.name)
        .compile(dialect, graph_name=graph)
    )
    return compiled.statement, compiled.parameters


def _not_in_parameter_with_null(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .where(p.name.not_in(["Alice", None]))
        .return_(p.name)
        .compile(dialect, graph_name=graph)
    )
    return compiled.statement, compiled.parameters


def _xor(dialect: str, graph: str) -> tuple[str, dict[str, Any]]:
    p = Person(alias="p")
    compiled = (
        Query.match(p)
        .where((p.age > 30) ^ (p.city == "Paris"))
        .return_(p.name)
        .compile(dialect, graph_name=graph)
    )
    return compiled.statement, compiled.parameters


EXECUTION_CASES = [
    ("in_literal_list", _in_literal, ["Alice", "Bob"]),
    ("in_list_parameter", _in_parameter, ["Alice", "Bob"]),
    ("not_in_literal_list", _not_in_literal, ["Carol", "Dave"]),
    ("not_in_parameter", _not_in_parameter, ["Carol", "Dave"]),
    ("not_over_in", _not_over_in, ["Carol", "Dave"]),
    ("in_empty_literal_list", _empty_in_literal, []),
    ("not_in_empty_literal_list", _empty_not_in_literal, ["Alice", "Bob", "Carol", "Dave", None]),
    ("in_empty_parameter", _empty_in_parameter, []),
    (
        "not_in_empty_parameter",
        _empty_not_in_parameter,
        ["Alice", "Bob", "Carol", "Dave", None],
    ),
    ("in_parameter_with_null", _in_parameter_with_null, ["Alice"]),
    ("not_in_parameter_with_null", _not_in_parameter_with_null, []),
    ("xor", _xor, ["Alice", None]),
]


def _names(rows: list[dict[str, Any]]) -> list[str | None]:
    return sorted((row["name"] for row in rows), key=lambda value: (value is None, value))


# ---------------------------------------------------------------------------
# Compile output
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_in_literal_list_compiles_to_parenthesised_values(dialect: str) -> None:
    statement, parameters = _in_literal(dialect, "g")
    assert "p.name IN ($p0, $p1)" in statement
    assert parameters == {"p0": "Alice", "p1": "Bob"}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_in_list_parameter_compiles_to_any(dialect: str) -> None:
    statement, parameters = _in_parameter(dialect, "g")
    assert "p.name = ANY($p0)" in statement
    assert parameters == {"p0": ["Alice", "Bob"]}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_not_in_compiles_to_all_or_a_parenthesised_list(dialect: str) -> None:
    literal_statement, literal_parameters = _not_in_literal(dialect, "g")
    assert "p.name NOT IN ($p0, $p1)" in literal_statement
    assert literal_parameters == {"p0": "Alice", "p1": "Bob"}

    parameter_statement, parameter_parameters = _not_in_parameter(dialect, "g")
    assert "p.name <> ALL($p0)" in parameter_statement
    assert parameter_parameters == {"p0": ["Alice", "Bob"]}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_not_over_in_stays_a_negated_membership(dialect: str) -> None:
    statement, parameters = _not_over_in(dialect, "g")
    assert "NOT (p.name = ANY($p0))" in statement
    assert parameters == {"p0": ["Alice", "Bob"]}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_empty_literal_list_compiles_to_constants(dialect: str) -> None:
    statement, parameters = _empty_in_literal(dialect, "g")
    assert "WHERE FALSE" in statement
    assert parameters == {}

    statement, parameters = _empty_not_in_literal(dialect, "g")
    assert "WHERE TRUE" in statement
    assert parameters == {}


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_xor_lowers_to_or_and_not_and_reuses_parameters(dialect: str) -> None:
    statement, parameters = _xor(dialect, "g")
    assert (
        "((p.age > $p0) OR (p.city = $p1)) AND NOT ((p.age > $p0) AND (p.city = $p1))" in statement
    )
    assert " XOR " not in statement
    assert parameters == {"p0": 30, "p1": "Paris"}


# ---------------------------------------------------------------------------
# DuckDB / DuckPGQ embedded execution
# ---------------------------------------------------------------------------


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
    connection.execute(
        f"CREATE TABLE {PERSON} (id INT PRIMARY KEY, name TEXT, age INT, city TEXT);"
    )
    connection.executemany(f"INSERT INTO {PERSON} VALUES (?, ?, ?, ?);", ROWS)
    connection.execute(f'CREATE PROPERTY GRAPH {GRAPH} VERTEX TABLES ({PERSON} LABEL "Person");')
    yield DuckDbBridge(connection)
    connection.close()


@pytest.mark.skipif(not HAS_DUCKPGQ, reason="DuckPGQ extension is not available")
@pytest.mark.parametrize(
    ("case_name", "build", "expected"),
    EXECUTION_CASES,
    ids=[case[0] for case in EXECUTION_CASES],
)
def test_duckpgq_executes_membership_and_xor(
    duckpgq_bridge, case_name: str, build: Any, expected: list[str | None]
) -> None:
    statement, parameters = build("duckpgq", GRAPH)
    rows = duckpgq_bridge.execute(statement, parameters)
    assert _names(rows) == expected, (case_name, statement, _names(rows))


# ---------------------------------------------------------------------------
# Live PostgreSQL 19
# ---------------------------------------------------------------------------


def _setup_postgres19(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {GRAPH} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {PERSON} CASCADE;")
        cur.execute(f"CREATE TABLE {PERSON} (id INT PRIMARY KEY, name TEXT, age INT, city TEXT);")
        cur.executemany(f"INSERT INTO {PERSON} VALUES (%s, %s, %s, %s);", ROWS)
        cur.execute(f'CREATE PROPERTY GRAPH {GRAPH} VERTEX TABLES ({PERSON} LABEL "Person");')


def _teardown_postgres19(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {GRAPH} CASCADE;")
        cur.execute(f"DROP TABLE IF EXISTS {PERSON} CASCADE;")


@pytest.mark.live
class TestPostgres19InXor:
    """PostgreSQL 19 GRAPH_TABLE execution of #303 and #304 through both backends."""

    @pytest.fixture
    def pg19(self):
        if psycopg is None:
            pytest.skip("psycopg is not installed in this environment")
        if not _is_port_open(HOST, 5456):
            pytest.skip("PostgreSQL 19 container not available on port 5456")
        conn_str = (
            f"host={HOST} port=5456 user=postgres password=voyagerpass123 "
            "dbname=voyager_graph connect_timeout=3"
        )
        try:
            conn = psycopg.connect(conn_str, autocommit=True)
        except Exception as exc:
            pytest.skip(f"PostgreSQL 19 container not available on port 5456: {exc}")
        _setup_postgres19(conn)
        yield conn
        _teardown_postgres19(conn)
        conn.close()

    @pytest.mark.parametrize(
        ("case_name", "build", "expected"),
        EXECUTION_CASES,
        ids=[case[0] for case in EXECUTION_CASES],
    )
    def test_bridge_and_native_execute_membership_and_xor(
        self, pg19, case_name: str, build: Any, expected: list[str | None]
    ) -> None:
        bridge = PostgresBridge(pg19)
        native = Session(POSTGRES19_URI, backend="native", dialect="sql_pgq")
        statement, parameters = build("sql_pgq", GRAPH)

        bridge_names = _names(bridge.execute(statement, parameters))
        native_names = _names(native.execute(statement, parameters))
        assert bridge_names == expected, (case_name, statement, bridge_names)
        assert native_names == expected, (case_name, statement, native_names)

    def test_old_statements_are_rejected(self, pg19) -> None:
        bridge = PostgresBridge(pg19)
        old_statements = [
            (
                f'SELECT * FROM GRAPH_TABLE ({GRAPH} MATCH (p IS "Person") '
                "WHERE p.name IN ['Alice', 'Bob'] COLUMNS (p.name))",
                {},
            ),
            (
                f'SELECT * FROM GRAPH_TABLE ({GRAPH} MATCH (p IS "Person") '
                "WHERE p.name IN $names COLUMNS (p.name))",
                {"names": ["Alice", "Bob"]},
            ),
            (
                f'SELECT * FROM GRAPH_TABLE ({GRAPH} MATCH (p IS "Person") '
                "WHERE (p.age > 30) XOR (p.city = 'Paris') COLUMNS (p.name))",
                {},
            ),
        ]
        for statement, parameters in old_statements:
            with pytest.raises(Exception, match="syntax error"):
                bridge.execute(statement, parameters)
