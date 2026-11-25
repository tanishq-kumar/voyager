"""Live label-predicate execution across engines (#306).

Each engine that supports a label test in a ``WHERE`` clause runs the compiled
label predicate from Query Spec v1 and the rows are compared with Neo4j. The
host comes from ``VOYAGER_TEST_HOST`` (default ``127.0.0.1``).
"""

from __future__ import annotations

import json
import os
import socket
from typing import Any

import pytest
from voyager_ogm import VoyagerError, compile_query_from_spec

pytestmark = pytest.mark.live

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "voyagerpass123")
AGE_PASSWORD = os.environ.get("AGE_PASSWORD", "voyagerpass123")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "voyagerpass123")
GRAPH = "label_predicate_306"

SPEC: dict[str, Any] = {
    "spec_version": 1,
    "matches": [
        {
            "paths": [
                [
                    ["node", "root", ["LpRoot"]],
                    ["edge", "outgoing", [], None],
                    ["node", "i", []],
                ]
            ],
            "where": [
                "bin",
                "and",
                ["bin", "gt", ["prop", "i", "var"], ["lit", "te"]],
                ["label", "i", ["LpTextNode"]],
            ],
        }
    ],
    "projections": [["field", "i", "var", "v"]],
    "order_by": [["i", "var", True]],
}

SEED = (
    "CREATE (root:LpRoot {name: 'x', __lp306: true}), "
    "(root)-[:LP_T]->(:LpTextNode {var: 'text', __lp306: true}), "
    "(root)-[:LP_T]->(:LpTextNode {var: 'zebra', __lp306: true}), "
    "(root)-[:LP_T]->(:LpTextNode {var: 'alpha', __lp306: true}), "
    "(root)-[:LP_T]->(:LpIntNode {var: 0, __lp306: true})"
)

CLEANUP = "MATCH (n) WHERE n.__lp306 = true DETACH DELETE n"

EXPECTED = ["text", "zebra"]


def _is_open(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=1):
            return True
    except OSError:
        return False


def _neo4j_rows() -> list[str]:
    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(
        f"bolt://{HOST}:7687", auth=("neo4j", NEO4J_PASSWORD), connection_timeout=2.0
    )
    try:
        with driver.session() as session:
            session.run(CLEANUP).consume()
            session.run(SEED).consume()
            try:
                compiled = compile_query_from_spec(SPEC, "cypher")
                return [
                    record["v"]
                    for record in session.run(compiled["statement"], compiled["parameters"])
                ]
            finally:
                session.run(CLEANUP).consume()
    finally:
        driver.close()


def test_live_neo4j_label_predicate() -> None:
    """Neo4j is the reference row set for the other engines."""
    if not _is_open(7687):
        pytest.skip("Neo4j container not reachable")
    assert _neo4j_rows() == EXPECTED


def test_live_memgraph_label_predicate() -> None:
    """Memgraph accepts the openCypher ``i:LpTextNode`` label test."""
    if not _is_open(7688):
        pytest.skip("Memgraph container not reachable")
    if not _is_open(7687):
        pytest.skip("Neo4j is needed as the row baseline")
    neo4j_rows = _neo4j_rows()

    from neo4j import GraphDatabase

    driver = GraphDatabase.driver(f"bolt://{HOST}:7688", auth=None, connection_timeout=2.0)
    try:
        with driver.session() as session:
            session.run(CLEANUP).consume()
            session.run(SEED).consume()
            try:
                compiled = compile_query_from_spec(SPEC, "cypher")
                rows = [
                    record["v"]
                    for record in session.run(compiled["statement"], compiled["parameters"])
                ]
            finally:
                session.run(CLEANUP).consume()
    finally:
        driver.close()
    assert rows == neo4j_rows


def test_live_falkordb_label_predicate() -> None:
    """FalkorDB accepts the openCypher ``i:LpTextNode`` label test."""
    if not _is_open(6379):
        pytest.skip("FalkorDB container not reachable")
    if not _is_open(7687):
        pytest.skip("Neo4j is needed as the row baseline")
    neo4j_rows = _neo4j_rows()

    from falkordb import FalkorDB

    client = FalkorDB(host=HOST, port=6379)
    graph = client.select_graph(GRAPH)
    try:
        try:
            graph.delete()
        except Exception:
            pass
        graph = client.select_graph(GRAPH)
        graph.query(SEED)
        compiled = compile_query_from_spec(SPEC, "cypher")
        result = graph.query(compiled["statement"], params=compiled["parameters"])
        rows = [row[0] for row in result.result_set]
        assert rows == neo4j_rows
    finally:
        try:
            graph.delete()
        except Exception:
            pass


def _agtype_scalar(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def test_live_apache_age_label_predicate() -> None:
    """Apache AGE accepts ``label(i) = 'LpTextNode'`` in WHERE."""
    if not _is_open(5455):
        pytest.skip("Apache AGE container not reachable")
    if not _is_open(7687):
        pytest.skip("Neo4j is needed as the row baseline")
    neo4j_rows = _neo4j_rows()

    import psycopg

    with psycopg.connect(
        host=HOST,
        port=5455,
        user="postgres",
        password=AGE_PASSWORD,
        dbname="voyager_graph",
        autocommit=True,
        connect_timeout=2,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute('SET search_path = ag_catalog, "$user", public;')
            cur.execute("SELECT count(*) FROM ag_graph WHERE name = %s", (GRAPH,))
            if cur.fetchone()[0]:
                cur.execute("SELECT drop_graph(%s, true)", (GRAPH,))
            cur.execute("SELECT create_graph(%s)", (GRAPH,))
            try:
                cur.execute(f"SELECT * FROM cypher('{GRAPH}', $$ {SEED} $$) AS (a agtype)")
                compiled = compile_query_from_spec(SPEC, "age", graph_name=GRAPH)
                params = (json.dumps(compiled["parameters"]),) if compiled["parameters"] else ()
                cur.execute(compiled["statement"], params)
                rows = [_agtype_scalar(row[0]) for row in cur.fetchall()]
            finally:
                cur.execute("SELECT drop_graph(%s, true)", (GRAPH,))
    assert rows == neo4j_rows


def test_live_postgres19_rejects_label_predicate_in_where() -> None:
    """PostgreSQL SQL/PGQ (19 Beta 3) has no label test in GRAPH_TABLE WHERE, so we raise."""
    if not _is_open(5456):
        pytest.skip("PostgreSQL 19 container not reachable")

    import psycopg

    with psycopg.connect(
        host=HOST,
        port=5456,
        user="postgres",
        password=PG_PASSWORD,
        dbname="voyager_graph",
        autocommit=True,
        connect_timeout=2,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("DROP PROPERTY GRAPH IF EXISTS lp306_graph")
            cur.execute("DROP TABLE IF EXISTS lp306_text CASCADE")
            try:
                cur.execute("CREATE TABLE lp306_text (id int PRIMARY KEY, var text)")
                cur.execute("INSERT INTO lp306_text VALUES (1, 'text'), (2, 'zebra')")
                cur.execute(
                    "CREATE PROPERTY GRAPH lp306_graph "
                    "VERTEX TABLES (lp306_text LABEL LpTextNode PROPERTIES (var))"
                )
                with pytest.raises(psycopg.errors.SyntaxError):
                    cur.execute(
                        "SELECT * FROM GRAPH_TABLE (lp306_graph "
                        "MATCH (i) WHERE i IS LpTextNode COLUMNS (i.var AS v))"
                    )
            finally:
                cur.execute("DROP PROPERTY GRAPH IF EXISTS lp306_graph")
                cur.execute("DROP TABLE IF EXISTS lp306_text CASCADE")

    with pytest.raises(VoyagerError) as exc_info:
        compile_query_from_spec(SPEC, "sql_pgq")
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
