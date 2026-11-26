"""Live checks for inferred Field nullability (#332).

Set ``VOYAGER_TEST_HOST`` to run against another machine (via ``conftest.get_test_host()``).
"""

from __future__ import annotations

import os
import socket

import pytest
from conftest import get_test_host
from voyager_ogm import Field, NativeClient, Node, SchemaManager, Session, node

pytestmark = pytest.mark.live

HOST = get_test_host()
PID = os.getpid()


@node("Zz332InferredOnly")
class Zz332InferredOnly(Node):
    name: str = Field(unique=True)
    age: int = Field(index=True)
    nickname: str | None = Field(default=None)


@node("Zz332MemgraphNode")
class Zz332MemgraphNode(Node):
    uid: str = Field(primary_key=True)
    code: str = Field(nullable=False)
    note: str | None


@node("Zz332PgNode")
class Zz332PgNode(Node):
    uid: str = Field(primary_key=True)
    name: str
    note: str | None


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=0.5):
            return True
    except OSError:
        return False


def _neo4j_driver(port: int, password: str | None):
    neo4j = pytest.importorskip("neo4j")
    auth = ("neo4j", password) if password is not None else None
    return neo4j.GraphDatabase.driver(f"bolt://{HOST}:{port}", auth=auth, connection_timeout=2.0)


def test_live_neo4j_community_inferred_non_null_fields_skip_nothing() -> None:
    """A model with only inferred non-nullable fields creates no skipped statements."""
    if not _reachable(7687):
        pytest.skip(f"Neo4j is not reachable on {HOST}:7687")
    driver = _neo4j_driver(7687, os.getenv("NEO4J_PASSWORD", "voyagerpass123"))
    try:
        driver.verify_connectivity()
    except Exception:
        driver.close()
        pytest.skip(f"Neo4j is not reachable on {HOST}:7687")

    try:
        with driver.session() as s:
            edition = s.run("CALL dbms.components() YIELD edition RETURN edition").single()[
                "edition"
            ]
        if str(edition).lower() != "community":
            pytest.skip(f"Requires Neo4j Community Edition on {HOST}:7687")

        session = Session(bridge=driver, dialect="cypher")
        session.drop_constraints(Zz332InferredOnly)
        try:
            created = session.create_constraints(Zz332InferredOnly)
            assert created, "expected at least the inferred field's UNIQUE constraint"
            assert not any("IS NOT NULL" in stmt for stmt in created)
        finally:
            session.drop_constraints(Zz332InferredOnly)
            session.execute("MATCH (n:Zz332InferredOnly) DETACH DELETE n")
    finally:
        driver.close()


def test_live_memgraph_explicit_nullable_false_creates_and_drops_exists() -> None:
    """An explicit nullable=False still creates and drops ASSERT EXISTS on Memgraph."""
    if not _reachable(7688):
        pytest.skip(f"Memgraph is not reachable on {HOST}:7688")
    driver = _neo4j_driver(7688, None)
    try:
        driver.verify_connectivity()
    except Exception:
        driver.close()
        pytest.skip(f"Memgraph is not reachable on {HOST}:7688")

    session = Session(bridge=driver, dialect="memgraph")
    try:
        session.drop_constraints(Zz332MemgraphNode)
        session.execute("MATCH (n:Zz332MemgraphNode) DETACH DELETE n")
        created = session.create_constraints(Zz332MemgraphNode)
        assert any("ASSERT EXISTS (n.code)" in stmt for stmt in created)
        assert not any("ASSERT EXISTS (n.note)" in stmt for stmt in created)
        dropped = session.drop_constraints(Zz332MemgraphNode)
        assert any("ASSERT EXISTS (n.code)" in stmt for stmt in dropped)
    finally:
        try:
            session.drop_constraints(Zz332MemgraphNode)
            session.execute("MATCH (n:Zz332MemgraphNode) DETACH DELETE n")
        finally:
            driver.close()


def test_live_postgres19_inferred_not_null_tables_run() -> None:
    """Tables from generate_pgq_schema_ddl mark inferred fields NOT NULL and execute."""
    if not _reachable(5456):
        pytest.skip(f"PostgreSQL 19 is not reachable on {HOST}:5456")

    graph = f"zz332_pg_g_{PID}"
    table = "zz332pgnode"
    base = f"postgresql://postgres:voyagerpass123@{HOST}:5456/voyager_graph"
    client = NativeClient(base, min_idle=1, max_size=2)
    try:
        client.execute_sync(f"DROP PROPERTY GRAPH IF EXISTS {graph} CASCADE")
        client.execute_sync(f'DROP TABLE IF EXISTS "{table}" CASCADE')

        ddl = SchemaManager.generate_pgq_schema_ddl(graph, Zz332PgNode, dialect="postgres")
        assert any('"name" TEXT NOT NULL' in statement for statement in ddl)
        assert any(
            '"note" TEXT' in statement and '"note" TEXT NOT NULL' not in statement
            for statement in ddl
        )
        for statement in ddl:
            client.execute_sync(statement)

        rows = client.execute_sync(
            "SELECT column_name, is_nullable FROM information_schema.columns "
            f"WHERE table_name = '{table}'"
        ).to_dicts()
        nullability = {row["column_name"]: row["is_nullable"] for row in rows}
        assert nullability["name"] == "NO"
        assert nullability["note"] == "YES"
    finally:
        client.execute_sync(f"DROP PROPERTY GRAPH IF EXISTS {graph} CASCADE")
        client.execute_sync(f'DROP TABLE IF EXISTS "{table}" CASCADE')
