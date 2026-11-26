"""Live tests for Bolt database selection and Memgraph compatibility (#396).

The native backend is `voyager_ogm`'s Rust client; the Python bridge is the official
`neo4j` driver wrapped in :class:`Neo4jBoltBridge` with ``database=``.
"""

from __future__ import annotations

import os
import socket

import pytest
from voyager_ogm import Session

pytestmark = pytest.mark.live

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
NEO4J_PORT = int(os.environ.get("VOYAGER_TEST_NEO4J_PORT", "7687"))
MEMGRAPH_PORT = int(os.environ.get("VOYAGER_TEST_MEMGRAPH_PORT", "7688"))
NEO4J_USER = "neo4j"
NEO4J_PASS = "voyagerpass123"


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=1.0):
            return True
    except OSError:
        return False


def _database_name(session: Session) -> str:
    rows = session.execute("CALL db.info() YIELD name RETURN name AS name").all()
    assert len(rows) == 1
    return str(rows[0]["name"])


def test_live_native_session_targets_the_uri_database() -> None:
    """A native Session whose URI names system runs its query in system, not neo4j."""
    if not _port_open(NEO4J_PORT):
        pytest.skip(f"Neo4j is not reachable on {HOST}:{NEO4J_PORT}")

    default_session = Session(
        f"bolt://{NEO4J_USER}:{NEO4J_PASS}@{HOST}:{NEO4J_PORT}",
        backend="native",
    )
    try:
        assert _database_name(default_session) == "neo4j"
    finally:
        default_session.close()

    system_session = Session(
        f"bolt://{NEO4J_USER}:{NEO4J_PASS}@{HOST}:{NEO4J_PORT}/system",
        backend="native",
    )
    try:
        assert _database_name(system_session) == "system"
    finally:
        system_session.close()


def test_live_python_bridge_database_argument_agrees() -> None:
    """The official-driver bridge with database='system' answers from system too."""
    neo4j = pytest.importorskip("neo4j")
    if not _port_open(NEO4J_PORT):
        pytest.skip(f"Neo4j is not reachable on {HOST}:{NEO4J_PORT}")

    from voyager_ogm.bridge import Neo4jBoltBridge

    driver = neo4j.GraphDatabase.driver(
        f"bolt://{HOST}:{NEO4J_PORT}", auth=(NEO4J_USER, NEO4J_PASS)
    )
    try:
        bridge = Neo4jBoltBridge(driver, database="system")
        session = Session(bridge=bridge, dialect="cypher")
        try:
            assert _database_name(session) == "system"
        finally:
            session.close()
    finally:
        driver.close()


def test_live_memgraph_still_answers() -> None:
    """Memgraph is single-database; no db metadata is sent, so queries keep working."""
    if not _port_open(MEMGRAPH_PORT):
        pytest.skip(f"Memgraph is not reachable on {HOST}:{MEMGRAPH_PORT}")

    session = Session(f"bolt://{HOST}:{MEMGRAPH_PORT}", backend="native")
    try:
        rows = session.execute("RETURN 1 AS n").all()
        assert rows[0]["n"] == 1
    finally:
        session.close()
