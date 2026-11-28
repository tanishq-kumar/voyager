"""Live checks for the OLTP quick fixes in issue #459."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from conftest import get_test_host, is_port_open
from voyager_ogm import AsyncSession, Session, create_bridge
from voyager_ogm.bridges.falkordb import AsyncFalkorDBBridge

pytestmark = pytest.mark.live

HOST = get_test_host()
PG19_PORT = 5456
FALKORDB_PORT = 6379
PG_USER = "postgres"
PG_PASSWORD = "voyagerpass123"
PG_DATABASE = "voyager_graph"


def _pg_connection_count() -> int:
    psycopg = pytest.importorskip("psycopg")
    with (
        psycopg.connect(
            host=HOST,
            port=PG19_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DATABASE,
            connect_timeout=5,
            autocommit=True,
        ) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE usename = %s AND datname = %s AND pid <> pg_backend_pid()",
            (PG_USER, PG_DATABASE),
        )
        return int(cur.fetchone()[0])


def test_native_session_opens_only_its_native_connection() -> None:
    """A native-only session builds no bridge and opens no second backend connection."""
    if not is_port_open(HOST, PG19_PORT):
        pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:{PG19_PORT}")

    uri = f"postgresql://{PG_USER}:{PG_PASSWORD}@{HOST}:{PG19_PORT}/{PG_DATABASE}"
    base = _pg_connection_count()
    session = Session(uri, backend="native", pool_size=2)
    try:
        assert session._bridge is None
        with pytest.raises(RuntimeError, match="requires a driver bridge"):
            _ = session.bridge
        assert session.execute("SELECT 1 AS v").all() == [{"v": 1}]
    finally:
        session.close()

    after = _pg_connection_count()
    # The native pool keeps at most its one warmed connection; the old eager
    # create_bridge() left a second, unused PostgreSQL backend behind.
    assert after - base <= 1, f"native session opened {after - base} extra connections"


def test_falkordb_graph_name_paths_select_the_requested_graph() -> None:
    """FalkorDB clients, Graph instances and async bridges honour graph_name."""
    if not is_port_open(HOST, FALKORDB_PORT):
        pytest.skip(f"FalkorDB not reachable on {HOST}:{FALKORDB_PORT}")
    falkordb: Any = pytest.importorskip("falkordb")

    run = uuid.uuid4().hex[:8]
    graph_a = f"zz_oltp_quick_a_{run}"
    graph_b = f"zz_oltp_quick_b_{run}"
    client = falkordb.FalkorDB(host=HOST, port=FALKORDB_PORT)
    try:
        sync_bridge = create_bridge(client, graph_name=graph_a)
        sync_bridge.execute("CREATE (n:ZzQuick {id: 1})")
        sync_bridge.graph_name = graph_b
        assert sync_bridge.graph_name == graph_b
        assert sync_bridge.execute("MATCH (n:ZzQuick) RETURN n.id AS id") == []

        async_bridge = AsyncFalkorDBBridge(client, graph_name=graph_a)
        assert async_bridge.graph_name == graph_a
        assert async_bridge.sync_bridge.execute("MATCH (n:ZzQuick) RETURN n.id AS id") == [
            {"id": 1}
        ]

        graph_bridge = create_bridge(client.select_graph(graph_a))
        assert graph_bridge.graph_name == graph_a
    finally:
        for name in (graph_a, graph_b):
            try:
                client.select_graph(name).delete()
            except Exception:
                pass
        client.close()


@pytest.mark.asyncio
async def test_async_native_session_has_no_bridge() -> None:
    """AsyncSession(backend='native') also skips create_bridge()."""
    if not is_port_open(HOST, PG19_PORT):
        pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:{PG19_PORT}")

    uri = f"postgresql://{PG_USER}:{PG_PASSWORD}@{HOST}:{PG19_PORT}/{PG_DATABASE}"
    session = AsyncSession(uri, backend="native", pool_size=2)
    try:
        assert session._bridge is None
        with pytest.raises(RuntimeError, match="requires a driver bridge"):
            _ = session.bridge
    finally:
        await session.close()
