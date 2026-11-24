"""Live FalkorDB parameter-binding and batched write tests (#339).

Verifies parameter rendering (list of maps, nested maps/lists, hostile strings,
non-plain identifier keys) and batched writes (flush, run_bulk, relationships)
on FalkorDB native backend and compares results with the bridge backend.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest
from voyager_ogm import Field, Node, Relationship, RelationshipTo, Session
from voyager_ogm.ingestion import BulkIngestionPlan

pytestmark = pytest.mark.live

# The falkordb/redis driver packages are not installed on Python 3.14 (see the
# pyproject.toml markers), and a FalkorDB Session needs them on both backends.
BATCH_ERRORS: tuple[type[BaseException], ...] = (ValueError, RuntimeError)
try:
    from redis.exceptions import RedisError
except ImportError:
    pass
else:
    BATCH_ERRORS = (*BATCH_ERRORS, RedisError)

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
BACKENDS = ["native", "bridge"]


class ZzLiveKnows(Relationship, type_name="ZZ_LIVE_KNOWS"):
    since: int = 2026


class ZzLivePerson(Node):
    __label__ = "ZzLivePerson339"

    pid: int = Field(primary_key=True)
    name: str = Field()
    friends: RelationshipTo[ZzLivePerson] = RelationshipTo(
        target="ZzLivePerson", rel_type="ZZ_LIVE_KNOWS", model=ZzLiveKnows
    )


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


@pytest.fixture(autouse=True)
def _check_falkordb_reachable() -> None:
    if not _port_open(HOST, 6379):
        pytest.skip(f"FalkorDB not reachable on {HOST}:6379")


def _open_session(backend: str) -> tuple[Session, str]:
    pytest.importorskip("falkordb")
    graph_name = f"zz_falkor_339_{uuid.uuid4().hex[:8]}"
    uri = f"redis://{HOST}:6379/{graph_name}"
    session = Session(
        uri,
        dialect="cypher",
        backend=backend,
        graph_name=graph_name,
        enable_identity_map=True,
    )
    return session, graph_name


def _cleanup_session(session: Session, graph_name: str) -> None:
    try:
        session.execute("MATCH (n) DETACH DELETE n")
    except Exception:
        pass
    finally:
        session.close()


@pytest.mark.parametrize("backend", BACKENDS)
def test_live_falkordb_unwind_list_of_maps(backend: str) -> None:
    session, graph = _open_session(backend)
    try:
        stmt = (
            "UNWIND $batch AS row "
            "MERGE (n:ZzLiveMapNode339 {pid: row.pid}) "
            "SET n.name = row.name "
            "RETURN n.pid AS pid, n.name AS name ORDER BY pid"
        )
        params = {
            "batch": [
                {"pid": 1, "name": "alpha"},
                {"pid": 2, "name": "beta"},
                {"pid": 3, "name": "gamma"},
            ]
        }
        res = session.execute(stmt, params)
        records = res.all()
        assert len(records) == 3
        assert records[0]["pid"] == 1 and records[0]["name"] == "alpha"
        assert records[1]["pid"] == 2 and records[1]["name"] == "beta"
        assert records[2]["pid"] == 3 and records[2]["name"] == "gamma"

        cnt = session.execute("MATCH (n:ZzLiveMapNode339) RETURN count(n) AS cnt").first()
        assert cnt is not None
        assert cnt["cnt"] == 3
    finally:
        _cleanup_session(session, graph)


@pytest.mark.parametrize("backend", BACKENDS)
def test_live_falkordb_nested_maps_and_lists(backend: str) -> None:
    session, graph = _open_session(backend)
    try:
        stmt = (
            "UNWIND $batch AS row "
            "CREATE (n:ZzLiveNested339 {pid: row.pid}) "
            "SET n.subval = row.sub.val, n.score = row.sub.score, n.active = row.sub.active "
            "RETURN n.pid AS pid, n.subval AS subval, n.score AS score, n.active AS active ORDER BY pid"
        )
        params = {
            "batch": [
                {
                    "pid": 1,
                    "sub": {"val": "sub1", "score": 10.5, "active": True},
                    "tags": ["tag1", "tag2"],
                },
                {
                    "pid": 2,
                    "sub": {"val": "sub2", "score": 20.0, "active": False},
                    "tags": ["tag3"],
                },
            ]
        }
        res = session.execute(stmt, params)
        records = res.all()
        assert len(records) == 2
        assert records[0]["pid"] == 1 and records[0]["subval"] == "sub1"
        assert records[0]["score"] == 10.5 and records[0]["active"] is True
        assert records[1]["pid"] == 2 and records[1]["subval"] == "sub2"
        assert records[1]["score"] == 20.0 and records[1]["active"] is False

        # Read back in a fresh query and count
        cnt = session.execute("MATCH (n:ZzLiveNested339) RETURN count(n) AS cnt").first()
        assert cnt is not None
        assert cnt["cnt"] == 2
    finally:
        _cleanup_session(session, graph)


@pytest.mark.parametrize("backend", BACKENDS)
def test_live_falkordb_hostile_strings(backend: str) -> None:
    session, graph = _open_session(backend)
    try:
        hostile_val = (
            'quote: "hello", backslash: \\, newline: \n, cr: \r, tab: \t, '
            "emoji: 🚀, unicode: 日本語 ☃"
        )
        res = session.execute(
            "MERGE (n:ZzLiveHostile339 {key: 1}) SET n.val = $val RETURN n.val AS val",
            {"val": hostile_val},
        )
        row = res.first()
        assert row is not None
        assert row["val"] == hostile_val

        # Read back in a fresh query
        read_res = session.execute(
            "MATCH (n:ZzLiveHostile339 {key: 1}) RETURN n.val AS val"
        ).first()
        assert read_res is not None
        assert read_res["val"] == hostile_val
    finally:
        _cleanup_session(session, graph)


@pytest.mark.parametrize("backend", BACKENDS)
def test_live_falkordb_non_plain_identifier_keys(backend: str) -> None:
    session, graph = _open_session(backend)
    try:
        stmt = (
            "UNWIND $batch AS row "
            "CREATE (n:ZzLiveKey339 {pid: row.pid}) "
            "SET n.plain = row.plain_key, n.dash = row.`dash-key`, n.dot = row.`dot.key`, "
            "    n.space = row.`key with space`, n.lead = row.`123lead_num` "
            "RETURN n.pid AS pid, n.plain AS plain, n.dash AS dash, n.dot AS dot, "
            "       n.space AS space, n.lead AS lead"
        )
        params = {
            "batch": [
                {
                    "pid": 1,
                    "plain_key": "val1",
                    "dash-key": "val2",
                    "dot.key": "val3",
                    "key with space": "val4",
                    "123lead_num": "val5",
                }
            ]
        }
        res = session.execute(stmt, params)
        row = res.first()
        assert row is not None
        assert row["pid"] == 1
        assert row["plain"] == "val1"
        assert row["dash"] == "val2"
        assert row["dot"] == "val3"
        assert row["space"] == "val4"
        assert row["lead"] == "val5"

        cnt = session.execute("MATCH (n:ZzLiveKey339) RETURN count(n) AS cnt").first()
        assert cnt is not None
        assert cnt["cnt"] == 1
    finally:
        _cleanup_session(session, graph)


@pytest.mark.parametrize("backend", BACKENDS)
def test_live_falkordb_flush_nodes_and_relationships(backend: str) -> None:
    session, graph = _open_session(backend)
    try:
        p1 = ZzLivePerson(pid=33901, name="Alice")
        p2 = ZzLivePerson(pid=33902, name="Bob")
        p3 = ZzLivePerson(pid=33903, name="Carol")

        for p in (p1, p2, p3):
            session.register(p)

        flush_node_results = session.flush()
        assert len(flush_node_results) >= 1
        assert sum(r.total_records for r in flush_node_results) == 3

        # Connect relationships
        p1.friends.connect(p2, properties={"since": 2026})
        p1.friends.connect(p3)

        flush_rel_results = session.flush()
        assert len(flush_rel_results) == 0  # node flush empty
        assert len(session._pending_relationships) == 0

        # Verify node counts in DB
        node_cnt = session.execute("MATCH (n:ZzLivePerson339) RETURN count(n) AS cnt").first()
        assert node_cnt is not None
        assert node_cnt["cnt"] == 3

        # Verify relationship counts in DB
        rel_cnt = session.execute(
            "MATCH (:ZzLivePerson339)-[r:ZZ_LIVE_KNOWS]->(:ZzLivePerson339) RETURN count(r) AS cnt"
        ).first()
        assert rel_cnt is not None
        assert rel_cnt["cnt"] == 2
    finally:
        _cleanup_session(session, graph)


@pytest.mark.parametrize("backend", BACKENDS)
def test_live_falkordb_bulk_ingest_plan(backend: str) -> None:
    session, graph = _open_session(backend)
    try:
        plan = BulkIngestionPlan(
            statement=(
                "UNWIND $batch AS row CREATE (n:ZzLiveBulkPlan339 {pid: row.pid, label: row.label})"
            ),
            total_records=4,
            batch_size=2,
            dialect="cypher",
            batches_data=[
                [{"pid": 1, "label": "first"}, {"pid": 2, "label": "second"}],
                [{"pid": 3, "label": "third"}, {"pid": 4, "label": "fourth"}],
            ],
        )

        res = session.run_bulk(plan)
        assert res.total_batches == 2
        assert res.total_records == 4

        # Read back and count
        cnt = session.execute("MATCH (n:ZzLiveBulkPlan339) RETURN count(n) AS cnt").first()
        assert cnt is not None
        assert cnt["cnt"] == 4
    finally:
        _cleanup_session(session, graph)


@pytest.mark.parametrize("backend", BACKENDS)
def test_live_falkordb_failing_batched_write_raises(backend: str) -> None:
    session, graph = _open_session(backend)
    try:
        plan = BulkIngestionPlan(
            statement="SYNTAX_ERROR_CYPHER_DOES_NOT_EXIST $$$",
            total_records=2,
            batch_size=1,
            dialect="cypher",
            batches_data=[[{"x": 1}], [{"x": 2}]],
        )
        with pytest.raises(BATCH_ERRORS):
            session.run_bulk(plan)
    finally:
        _cleanup_session(session, graph)
