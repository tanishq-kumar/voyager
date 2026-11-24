"""Live checks for a ``None`` field default on Neo4j and Memgraph (#333).

``= None`` is a real default: a new node reports it but never stores it as an
explicit null, merging with the default does not change a stored value, and a
mixed bulk merge only writes the field for the rows that supply it.

Set ``VOYAGER_TEST_HOST`` to run against another machine (default 127.0.0.1).
"""

from __future__ import annotations

import os
import socket

import pytest
from voyager_ogm import Field, NativeClient, Node, Query, Session, node

pytestmark = pytest.mark.live

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
PID = os.getpid()
LABEL = f"Zz333NoneDefault{PID}"


@node(LABEL)
class Zz333NoneDefaultLive(Node):
    uid: str = Field(primary_key=True)
    nickname: str | None = None
    extra: str | None = Field(default=None)
    status: str = "new"


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=0.5):
            return True
    except OSError:
        return False


def _properties(session: Session, uid: str) -> dict[str, object]:
    rows = session.execute(
        f"MATCH (n:{LABEL} {{uid: $uid}}) "
        "RETURN n.uid AS uid, n.status AS status, n.nickname AS nickname, "
        "n.extra AS extra, n.nickname IS NOT NULL AS has_nickname, "
        "n.extra IS NOT NULL AS has_extra",
        {"uid": uid},
    )
    assert rows, f"expected a stored {LABEL} node for uid={uid!r}"
    return rows[0]


def _exercise_none_default(port: int, userinfo: str) -> None:
    if not _reachable(port):
        pytest.skip(f"database is not reachable on {HOST}:{port}")

    uri = f"bolt://{userinfo}{HOST}:{port}?connect_timeout=2"
    try:
        client = NativeClient(uri, min_idle=1, max_size=2)
        client.ping_sync()
    except Exception as exc:
        pytest.skip(f"database is not reachable on {HOST}:{port}: {exc}")

    session = Session(bridge=client, dialect="cypher")
    try:
        session.execute(f"MATCH (n:{LABEL}) DETACH DELETE n")

        # CREATE: the None defaults are absent, the non-None default is written.
        session.execute(Query.create(Zz333NoneDefaultLive(uid="created")))
        created = _properties(session, "created")
        assert created["uid"] == "created"
        assert created["status"] == "new"
        assert created["nickname"] is None
        assert created["extra"] is None
        assert created["has_nickname"] is False
        assert created["has_extra"] is False

        # MERGE creates with the same semantics.
        session.execute(Query.merge(Zz333NoneDefaultLive(uid="merged")))
        merged = _properties(session, "merged")
        assert merged["status"] == "new"
        assert merged["has_nickname"] is False
        assert merged["has_extra"] is False

        # A supplied value is written on MATCH, and a later merge that leaves the
        # None default must not overwrite it.
        session.execute(Query.merge(Zz333NoneDefaultLive(uid="merged", nickname="ada")))
        assert _properties(session, "merged")["nickname"] == "ada"
        session.execute(Query.merge(Zz333NoneDefaultLive(uid="merged")))
        assert _properties(session, "merged")["nickname"] == "ada"

        # Bulk MERGE: a mixed batch writes nickname only for the row supplying it.
        session.run_bulk(
            session.bulk_upsert(
                Zz333NoneDefaultLive,
                [{"uid": "bulk-set", "nickname": "bulk"}, {"uid": "bulk-default"}],
                key_field="uid",
            )
        )
        assert _properties(session, "bulk-set")["nickname"] == "bulk"
        defaulted = _properties(session, "bulk-default")
        assert defaulted["has_nickname"] is False
        assert defaulted["has_extra"] is False

        # Merging the stored row without the field preserves the stored value.
        session.run_bulk(
            session.bulk_upsert(
                Zz333NoneDefaultLive,
                [{"uid": "bulk-set"}],
                key_field="uid",
            )
        )
        assert _properties(session, "bulk-set")["nickname"] == "bulk"
    finally:
        session.execute(f"MATCH (n:{LABEL}) DETACH DELETE n")
        session.close()


def test_live_neo4j_none_default_create_merge_readback() -> None:
    """Neo4j: None defaults are absent on CREATE/MERGE and preserved on match."""
    _exercise_none_default(7687, "neo4j:voyagerpass123@")


def test_live_memgraph_none_default_create_merge_readback() -> None:
    """Memgraph: None defaults are absent on CREATE/MERGE and preserved on match."""
    _exercise_none_default(7688, "")
