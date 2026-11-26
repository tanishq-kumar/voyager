"""Parameterised ``skip`` / ``limit`` in Query Spec v1 (issue #321)."""

from __future__ import annotations

from typing import Any

import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import (
    CompiledQuery,
    MockBridge,
    Node,
    Query,
    Session,
    VoyagerError,
    compile_query_from_spec,
    param,
)
from voyager_ogm.bridge import validate_pagination_parameters
from voyager_ogm.models import Field

DIALECTS = ("cypher", "age", "iso_gql", "sql_pgq", "duckpgq")

BASE_SPEC: dict[str, Any] = {
    "spec_version": 1,
    "matches": [{"paths": [[["node", "p", ["Person"]]]]}],
    "projections": [["field", "p", "name", None]],
}


def _with_pagination(**pagination: Any) -> dict[str, Any]:
    return {**BASE_SPEC, **pagination}


@pytest.mark.parametrize(
    ("dialect", "statement"),
    [
        ("cypher", "MATCH (p:Person) RETURN p.name SKIP $s LIMIT $l"),
        (
            "age",
            "SELECT * FROM cypher('age_graph', $$ MATCH (p:Person) RETURN p.name "
            'SKIP $s LIMIT $l $$, %s) AS ("name" agtype)',
        ),
        ("iso_gql", "MATCH (p:Person) RETURN p.name OFFSET $s LIMIT $l"),
        (
            "sql_pgq",
            'SELECT * FROM GRAPH_TABLE (graph_table MATCH (p IS "Person") '
            "COLUMNS (p.name)) LIMIT $l OFFSET $s",
        ),
        (
            "duckpgq",
            'SELECT * FROM GRAPH_TABLE (graph_table MATCH (p IS "Person") '
            "COLUMNS (p.name)) LIMIT $l OFFSET $s",
        ),
    ],
)
def test_consumer_accepts_pagination_parameters(dialect: str, statement: str) -> None:
    """A parameter compiles to the dialect's native pagination placeholder."""
    compiled = compile_query_from_spec(
        _with_pagination(skip=["param", "s"], limit=["param", "$l"]), dialect
    )
    assert compiled["statement"] == statement
    assert compiled["parameters"] == {}


def test_consumer_accepts_parameter_inside_with() -> None:
    """Both WITH and root pagination positions accept a parameter."""
    spec = {
        "spec_version": 1,
        "matches": BASE_SPEC["matches"],
        "with_clauses": [
            {
                "projections": [["field", "p", "name", "n"]],
                "skip": ["param", "ws"],
                "limit": ["param", "wl"],
            }
        ],
        "projections": [["expr", ["ident", "n"], None]],
        "skip": ["param", "s"],
        "limit": ["param", "l"],
    }
    compiled = compile_query_from_spec(spec, "cypher")
    assert compiled["statement"] == (
        "MATCH (p:Person) WITH p.name AS n SKIP $ws LIMIT $wl RETURN n SKIP $s LIMIT $l"
    )


def test_consumer_rejects_offset_in_favour_of_skip() -> None:
    """offset is no longer an alias; the error names the canonical key."""
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(
            _with_pagination(offset=["param", "$o"], limit=["param", "l"]), "cypher"
        )
    error = exc_info.value
    assert error.code == "INVALID_SPEC"
    assert error.path == "/offset"
    assert 'use "skip" instead of "offset"' in str(error)


@pytest.mark.parametrize(
    ("value", "pointer"),
    [
        (["param"], "/limit"),
        (["param", 7], "/limit/1"),
        (["param", ""], "/limit/1"),
        (["param", "l", None], "/limit/2"),
        (["lit", "l"], "/limit/0"),
        ({"param": "l"}, "/limit"),
    ],
)
def test_consumer_rejects_malformed_pagination_parameter(value: Any, pointer: str) -> None:
    """A malformed parameter array is INVALID_SPEC at its own JSON Pointer."""
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(_with_pagination(limit=value), "cypher")
    assert exc_info.value.code == "INVALID_SPEC"
    assert exc_info.value.path == pointer


def test_consumer_rejects_malformed_with_pagination_parameter() -> None:
    """The WITH position reports its own pointer."""
    spec = {
        "spec_version": 1,
        "matches": BASE_SPEC["matches"],
        "with_clauses": [{"projections": [["field", "p", "name", "n"]], "skip": ["param"]}],
        "projections": [["expr", ["ident", "n"], None]],
    }
    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(spec, "cypher")
    assert exc_info.value.path == "/with_clauses/0/skip"


def test_consumer_keeps_literal_validation() -> None:
    """Existing literal rules are unchanged by the parameter form."""
    for value in (-1, 1.5, True):
        with pytest.raises(VoyagerError) as exc_info:
            compile_query_from_spec(_with_pagination(skip=value), "cypher")
        assert exc_info.value.code == "INVALID_SPEC"
        assert exc_info.value.path == "/skip"


class _PaginationPerson(Node):
    __labels__ = ["PaginationPerson"]

    name: str = Field()
    age: int = Field()


def test_fluent_pagination_parameter_round_trips_through_spec() -> None:
    """A fluent parameter stays a parameter in the exported spec."""
    p = _PaginationPerson(alias="p")
    query = Query.match(p).return_(p.name).skip(param("s")).limit(param("$l"))

    compiled = query.compile("cypher")
    assert compiled.statement == "MATCH (p:PaginationPerson) RETURN p.name SKIP $s LIMIT $l"

    spec = query.to_spec_dict()
    assert spec["skip"] == ["param", "s"]
    assert spec["limit"] == ["param", "l"]

    from_spec = compile_query_from_spec(spec, "cypher")
    assert from_spec["statement"] == compiled.statement
    assert from_spec["parameters"] == compiled.parameters


def test_fluent_pagination_parameters_compile_in_every_dialect() -> None:
    """Every emitter keeps both pagination positions as parameters."""
    p = _PaginationPerson(alias="p")
    query = Query.match(p).return_(p.name).skip(param("s")).limit(param("l"))

    assert query.compile("cypher").statement.endswith("SKIP $s LIMIT $l")
    assert query.compile("iso_gql").statement.endswith("OFFSET $s LIMIT $l")
    assert query.compile("age").statement.endswith('SKIP $s LIMIT $l $$, %s) AS ("name" agtype)')
    assert query.compile("sql_pgq").statement.endswith("LIMIT $l OFFSET $s")
    assert query.compile("duckpgq").statement.endswith("LIMIT $l OFFSET $s")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        (-1, "non-negative integer"),
        (1.5, "non-negative integer"),
        (True, "non-negative integer"),
        ("2", "non-negative integer"),
    ],
)
def test_bound_pagination_value_is_validated_before_dispatch(value: Any, message: str) -> None:
    """Negative and non-integer bindings fail in the client, not in an engine."""
    with pytest.raises(ValueError, match=message):
        validate_pagination_parameters("RETURN 1 SKIP $s LIMIT $l", {"s": 0, "l": value})


def test_bound_pagination_value_validation_through_session() -> None:
    """The session validates Query and CompiledQuery execution, including native."""
    bridge = MockBridge()
    session = Session(bridge=bridge, dialect="cypher")
    compiled = CompiledQuery(
        statement="MATCH (p:Person) RETURN p.name SKIP $s LIMIT $l", parameters={}
    )
    with pytest.raises(ValueError, match="Pagination parameter '\\$l'"):
        session.execute(compiled, {"s": 0, "l": -1})
    assert bridge.executed_queries == []


# ---------------------------------------------------------------------------
# Live execution
# ---------------------------------------------------------------------------

HOST = get_test_host()


def _port_open(port: int) -> bool:
    return is_port_open(HOST, port, timeout=2.0)


def _unwind_spec() -> dict[str, Any]:
    return {
        "spec_version": 1,
        "unwinds": [["$rows", "x"]],
        "projections": [["expr", ["ident", "x"], None]],
        "order_by": [[["ident", "x"], True]],
        "skip": ["param", "s"],
        "limit": ["param", "l"],
    }


def _graph_table_spec() -> dict[str, Any]:
    return {
        "spec_version": 1,
        "matches": [{"paths": [[["node", "a", ["PaginationUser"]]]]}],
        "projections": [["field", "a", "username", "u"]],
        "order_by": [["a", "username", True]],
        "skip": ["param", "s"],
        "limit": ["param", "l"],
    }


UNWIND_PARAMS = {"rows": [1, 2, 3, 4, 5], "s": 1, "l": 2}
EXPECTED_UNWIND_ROWS = [{"x": 2}, {"x": 3}]


def _assert_unwind_rows(
    compiled: dict[str, Any],
    bridge: Any,
    expected: list[dict[str, Any]] = EXPECTED_UNWIND_ROWS,
) -> None:
    rows = bridge.execute(compiled["statement"], UNWIND_PARAMS)
    assert rows == expected


@pytest.mark.live
def test_live_neo4j_pagination_parameters() -> None:
    """Neo4j binds SKIP and LIMIT parameters."""
    if not _port_open(7687):
        pytest.skip(f"Neo4j not reachable on {HOST}:7687")
    neo4j = pytest.importorskip("neo4j")
    from voyager_ogm.bridge import Neo4jBoltBridge

    driver = neo4j.GraphDatabase.driver(f"bolt://{HOST}:7687", auth=("neo4j", "voyagerpass123"))
    try:
        bridge = Neo4jBoltBridge(driver)
        compiled = compile_query_from_spec(_unwind_spec(), "cypher")
        _assert_unwind_rows(compiled, bridge)
        with pytest.raises(ValueError, match="Pagination parameter"):
            bridge.execute(compiled["statement"], {**UNWIND_PARAMS, "l": -1})
    finally:
        driver.close()


@pytest.mark.live
def test_live_memgraph_pagination_parameters() -> None:
    """Memgraph binds SKIP and LIMIT parameters."""
    if not _port_open(7688):
        pytest.skip(f"Memgraph not reachable on {HOST}:7688")
    neo4j = pytest.importorskip("neo4j")
    from voyager_ogm.bridge import Neo4jBoltBridge

    driver = neo4j.GraphDatabase.driver(f"bolt://{HOST}:7688")
    try:
        bridge = Neo4jBoltBridge(driver)
        compiled = compile_query_from_spec(_unwind_spec(), "cypher")
        _assert_unwind_rows(compiled, bridge)
    finally:
        driver.close()


@pytest.mark.live
def test_live_falkordb_pagination_parameters() -> None:
    """FalkorDB binds SKIP and LIMIT parameters."""
    if not _port_open(6379):
        pytest.skip(f"FalkorDB not reachable on {HOST}:6379")
    falkordb = pytest.importorskip("falkordb")
    from voyager_ogm.bridge import FalkorDBBridge

    client = falkordb.FalkorDB(host=HOST, port=6379)
    graph = client.select_graph("feat321_pagination")
    try:
        bridge = FalkorDBBridge(graph)
        compiled = compile_query_from_spec(_unwind_spec(), "cypher")
        _assert_unwind_rows(compiled, bridge)
    finally:
        try:
            graph.delete()
        finally:
            client.close()


@pytest.mark.live
def test_live_apache_age_pagination_parameters() -> None:
    """Apache AGE binds SKIP and LIMIT parameters through the agtype map."""
    if not _port_open(5455):
        pytest.skip(f"Apache AGE not reachable on {HOST}:5455")
    psycopg = pytest.importorskip("psycopg")
    from voyager_ogm.bridge import PostgresBridge

    graph_name = "feat321_age_pagination"
    conn = psycopg.connect(
        f"host={HOST} port=5455 user=postgres password=voyagerpass123 "
        "dbname=voyager_graph connect_timeout=5",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SET search_path = ag_catalog, public")
            cur.execute("SELECT 1 FROM ag_catalog.ag_graph WHERE name = %s", (graph_name,))
            if cur.fetchone() is None:
                cur.execute("SELECT ag_catalog.create_graph(%s)", (graph_name,))
        bridge = PostgresBridge(conn, graph_name=graph_name)
        compiled = compile_query_from_spec(_unwind_spec(), "age", graph_name=graph_name)
        _assert_unwind_rows(compiled, bridge)
        with pytest.raises(ValueError, match="Pagination parameter"):
            bridge.execute(compiled["statement"], {**UNWIND_PARAMS, "s": 1.5})
    finally:
        with conn.cursor() as cur:
            cur.execute("SET search_path = ag_catalog, public")
            cur.execute("SELECT ag_catalog.drop_graph(%s, true)", (graph_name,))
        conn.close()


@pytest.mark.live
def test_live_postgres19_pgq_pagination_parameters() -> None:
    """PostgreSQL 19 Beta 3 SQL/PGQ binds LIMIT and OFFSET parameters."""
    port = 5456
    graph_name = "feat321_pg_pagination"
    if not _port_open(port):
        pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:{port}")
    if not _port_open(7687):
        pytest.skip("Neo4j baseline not reachable")
    psycopg = pytest.importorskip("psycopg")
    from voyager_ogm.bridge import PostgresBridge

    conn = psycopg.connect(
        f"host={HOST} port={port} user=postgres password=voyagerpass123 "
        "dbname=voyager_graph connect_timeout=5",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {graph_name}")
            cur.execute("DROP TABLE IF EXISTS feat321_pagination_users CASCADE")
            cur.execute("CREATE TABLE feat321_pagination_users (id INT PRIMARY KEY, username TEXT)")
            cur.execute(
                "INSERT INTO feat321_pagination_users VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d')"
            )
            cur.execute(
                f"CREATE PROPERTY GRAPH {graph_name} VERTEX TABLES "
                '(feat321_pagination_users LABEL "PaginationUser" '
                "PROPERTIES (username))"
            )
        bridge = PostgresBridge(conn)
        compiled = compile_query_from_spec(_graph_table_spec(), "sql_pgq", graph_name=graph_name)
        rows = bridge.execute(compiled["statement"], {"s": 1, "l": 2})
        assert rows == [{"u": "b"}, {"u": "c"}]
        with pytest.raises(ValueError, match="Pagination parameter"):
            bridge.execute(compiled["statement"], {"s": -1, "l": 2})
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {graph_name}")
            cur.execute("DROP TABLE IF EXISTS feat321_pagination_users CASCADE")
        conn.close()


@pytest.mark.live
def test_live_duckpgq_pagination_parameters() -> None:
    """DuckPGQ executes parameterised pagination through DuckDbBridge inlining."""
    duckdb = pytest.importorskip("duckdb")
    from voyager_ogm.bridge import DuckDbBridge

    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        try:
            conn.execute("LOAD 'duckpgq'")
        except Exception:
            pytest.skip("DuckPGQ extension could not be loaded")
        graph_name = "feat321_duck_pagination"
        conn.execute(
            "CREATE TABLE feat321_pagination_users "
            "(id VARCHAR PRIMARY KEY, username VARCHAR);"
            "INSERT INTO feat321_pagination_users VALUES "
            "('1','a'),('2','b'),('3','c'),('4','d');"
            f"CREATE PROPERTY GRAPH {graph_name} VERTEX TABLES "
            '(feat321_pagination_users LABEL "PaginationUser");'
        )
        bridge = DuckDbBridge(conn)
        compiled = compile_query_from_spec(_graph_table_spec(), "duckpgq", graph_name=graph_name)
        rows = bridge.execute(compiled["statement"], {"s": 1, "l": 2})
        assert rows == [{"u": "b"}, {"u": "c"}]
        with pytest.raises(ValueError, match="Pagination parameter"):
            bridge.execute(compiled["statement"], {"s": 1, "l": 1.5})
    finally:
        conn.close()
