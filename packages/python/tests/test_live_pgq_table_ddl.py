"""Live tests: relational table DDL, constraints, indexes and SQL/PGQ property graph (#327).

Verifies that the full ordered schema DDL emitted by `generate_pgq_schema_ddl`:
1. Executes on PostgreSQL 19 Beta 3 in one pass (tables + constraints + indexes + CREATE PROPERTY GRAPH).
2. Supports self-relationships, labels differing from class names, and reserved-word labels ("User", "Order").
3. Enables SQL/PGQ queries via `GRAPH_TABLE` matching the model schema.
4. Matches `introspect_schema` with `export_schema` for the same models.
5. Executes end-to-end on embedded DuckPGQ.

Databases:
- PostgreSQL 19 Beta 3 on VOYAGER_TEST_HOST:5456 (default 127.0.0.1)
- DuckDB / DuckPGQ embedded
"""

from __future__ import annotations

import os
import socket
from collections.abc import Iterator
from typing import Any

import pytest
from voyager_ogm import (
    DuckDbBridge,
    Field,
    NativeClient,
    Node,
    Query,
    Relationship,
    export_schema,
    introspect_schema,
    node,
    relationship,
)
from voyager_ogm.schema import SchemaManager

pytestmark = pytest.mark.live

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")
PORT = 5456
PID = os.getpid()

GRAPH = f"zz327_g_{PID}"
USER_LABEL = "User"
ORDER_LABEL = "Order"
PLACES_TYPE = "ZZ327_PLACES"
HUMAN_LABEL = "Zz327Human"
KNOWS_TYPE = "ZZ327_KNOWS"
CALLS_TYPE = "ZZ327_CALLS"


@node(USER_LABEL)
class Zz327User(Node):
    uid: str = Field(primary_key=True)
    email: str = Field(unique=True)
    name: str = Field(index=True)
    age: int = Field(default=0, nullable=False)


@node(ORDER_LABEL)
class Zz327Order(Node):
    oid: str = Field(primary_key=True)
    total: float = Field(default=0.0, nullable=False)


@node(HUMAN_LABEL)
class Zz327Person(Node):
    pid: str = Field(primary_key=True)
    display_name: str = Field(nullable=False)


@relationship(PLACES_TYPE, source_node=Zz327User, target_node=Zz327Order)
class Zz327Places(Relationship):
    placed_at: str = Field(nullable=False)


@relationship(KNOWS_TYPE, source_node=Zz327Person, target_node=Zz327Person)
class Zz327Knows(Relationship):
    since: int = Field(default=2020, nullable=False)


@relationship(CALLS_TYPE, source_node=Zz327Person, target_node=Zz327Person)
class Zz327Calls(Relationship):
    call_id: str = Field(primary_key=True)
    duration: int = Field(default=60, nullable=False)


MODELS: list[Any] = [Zz327User, Zz327Order, Zz327Person, Zz327Places, Zz327Knows, Zz327Calls]

TABLES_TO_DROP = [
    CALLS_TYPE.lower(),
    PLACES_TYPE.lower(),
    KNOWS_TYPE.lower(),
    USER_LABEL.lower(),
    ORDER_LABEL.lower(),
    HUMAN_LABEL.lower(),
]


def _reachable() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=0.5):
            return True
    except OSError:
        return False


def _drop_all_pg(client: NativeClient) -> None:
    client.execute_sync(f"DROP PROPERTY GRAPH IF EXISTS {GRAPH} CASCADE")
    for table in TABLES_TO_DROP:
        client.execute_sync(f'DROP TABLE IF EXISTS "{table}" CASCADE')


@pytest.fixture
def postgres19() -> Iterator[tuple[NativeClient, str]]:
    if not _reachable():
        pytest.skip(f"PostgreSQL 19 is not reachable on {HOST}:{PORT}")
    base = f"postgresql://postgres:voyagerpass123@{HOST}:{PORT}/voyager_graph"
    client = NativeClient(base, min_idle=1, max_size=2)
    _drop_all_pg(client)
    try:
        yield client, f"{base}?graph={GRAPH}"
    finally:
        _drop_all_pg(client)
        client.close()


def test_live_postgresql_19_full_pgq_schema_ddl(
    postgres19: tuple[NativeClient, str],
) -> None:
    """Verifies single-pass schema DDL execution, querying, and live introspection on PostgreSQL 19."""
    client, uri = postgres19

    # 1. Generate full ordered schema DDL and execute in one pass
    ddl_statements = SchemaManager.generate_pgq_schema_ddl(GRAPH, *MODELS, dialect="postgres")
    for statement in ddl_statements:
        client.execute_sync(statement)

    # 2. Insert data
    user_tbl = USER_LABEL.lower()
    order_tbl = ORDER_LABEL.lower()
    human_tbl = HUMAN_LABEL.lower()
    places_tbl = PLACES_TYPE.lower()
    knows_tbl = KNOWS_TYPE.lower()
    calls_tbl = CALLS_TYPE.lower()

    client.execute_sync(
        f'INSERT INTO "{user_tbl}" (uid, email, name, age) '
        "VALUES ('u1', 'alice@test.org', 'Alice', 30), ('u2', 'bob@test.org', 'Bob', 25)"
    )
    client.execute_sync(
        f"INSERT INTO \"{order_tbl}\" (oid, total) VALUES ('o1', 99.5), ('o2', 150.0)"
    )
    client.execute_sync(
        f"INSERT INTO \"{human_tbl}\" (pid, display_name) VALUES ('p1', 'Charlie'), ('p2', 'Diana')"
    )
    client.execute_sync(
        f'INSERT INTO "{places_tbl}" (src_uid, dst_oid, placed_at) '
        "VALUES ('u1', 'o1', '2026-01-01')"
    )
    # Insert TWO edges of the same type between the same pair of nodes (surrogate key generated)
    client.execute_sync(
        f"INSERT INTO \"{knows_tbl}\" (src_pid, dst_pid, since) VALUES ('p1', 'p2', 2020), ('p1', 'p2', 2024)"
    )
    # Insert TWO edges of the same type between the same pair of nodes with explicit primary key
    client.execute_sync(
        f'INSERT INTO "{calls_tbl}" (src_pid, dst_pid, call_id, duration) '
        "VALUES ('p1', 'p2', 'c1', 120), ('p1', 'p2', 'c2', 300)"
    )

    # 3. Query inter-model relationship with SQL/PGQ
    u, o = Zz327User("u1"), Zz327Order("o1")
    order_q = (
        Query.match(u)
        .to(Zz327Places, var="pl")
        .node(o)
        .where(u.uid == "u1")
        .return_(buyer=u.name, order_id=o.oid, amount=o.total)
        .compile("sql_pgq", graph_name=GRAPH)
    )
    rows = client.execute_sync(order_q.statement, order_q.parameters).to_dicts()
    assert rows == [{"buyer": "Alice", "order_id": "o1", "amount": 99.5}]

    # 4. Query self-relationship with SQL/PGQ (parallel edges with surrogate key)
    p1, p2 = Zz327Person("p1"), Zz327Person("p2")
    knows_q = (
        Query.match(p1)
        .to(Zz327Knows, var="k")
        .node(p2)
        .where(p1.pid == "p1")
        .return_(person=p1.display_name, friend=p2.display_name, since=Zz327Knows.since)
        .compile("sql_pgq", graph_name=GRAPH)
    )
    knows_rows = client.execute_sync(knows_q.statement, knows_q.parameters).to_dicts()
    assert len(knows_rows) == 2
    assert sorted([r["since"] for r in knows_rows]) == [2020, 2024]

    # Query relationship model that declares its own primary key (parallel edges with explicit PK)
    calls_q = (
        Query.match(p1)
        .to(Zz327Calls, var="c")
        .node(p2)
        .where(p1.pid == "p1")
        .return_(
            person=p1.display_name,
            friend=p2.display_name,
            call_id=Zz327Calls.call_id,
            duration=Zz327Calls.duration,
        )
        .compile("sql_pgq", graph_name=GRAPH)
    )
    calls_rows = client.execute_sync(calls_q.statement, calls_q.parameters).to_dicts()
    assert len(calls_rows) == 2
    assert sorted([r["call_id"] for r in calls_rows]) == ["c1", "c2"]
    assert sorted([r["duration"] for r in calls_rows]) == [120, 300]

    # 5. Live schema introspection comparison with export_schema
    live = introspect_schema(uri)
    assert live["source"]["engine"] == "postgresql"
    assert live["source"]["database"] == GRAPH

    declared = export_schema(MODELS)
    live_nodes_by_label = {n["name"]: n for n in live["nodes"]}
    for exp_node in declared["nodes"]:
        label = exp_node["labels"][0]
        assert label in live_nodes_by_label, f"Node label {label} missing from introspection"
        live_node = live_nodes_by_label[label]
        assert live_node["primary_key"] == exp_node["primary_key"]
        for prop_name, exp_prop in exp_node["properties"].items():
            assert prop_name in live_node["properties"], (
                f"Property {prop_name} missing from {label}"
            )
            live_prop = live_node["properties"][prop_name]
            assert live_prop["type"] == exp_prop["type"]
            assert live_prop["nullable"] == exp_prop["nullable"]
            if exp_prop["primary_key"] or exp_prop["unique"]:
                assert live_prop["unique"] is True

    live_rels_by_type = {r["name"]: r for r in live["relationships"]}
    for exp_rel in declared["relationships"]:
        rel_type = exp_rel["type"]
        assert rel_type in live_rels_by_type, (
            f"Relationship type {rel_type} missing from introspection"
        )
        live_rel = live_rels_by_type[rel_type]
        assert live_rel["source_labels"] == exp_rel["source_labels"]
        assert live_rel["target_labels"] == exp_rel["target_labels"]
        for prop_name, exp_prop in exp_rel["properties"].items():
            assert prop_name in live_rel["properties"], (
                f"Property {prop_name} missing from {rel_type}"
            )
            live_prop = live_rel["properties"][prop_name]
            assert live_prop["type"] == exp_prop["type"]
            assert live_prop["nullable"] == exp_prop["nullable"]


def test_live_duckpgq_full_schema_ddl() -> None:
    """Verifies single-pass schema DDL execution and SQL/PGQ querying on embedded DuckPGQ."""
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        conn.execute(
            "SET custom_extension_repository = 'http://duckpgq.s3.eu-north-1.amazonaws.com';"
        )
        conn.execute("INSTALL 'duckpgq'; LOAD 'duckpgq';")
    except Exception as e:
        pytest.skip(f"DuckPGQ extension could not be loaded: {e}")

    duck_graph = f"zz327_duck_g_{PID}"
    # Generate full schema DDL for DuckDB
    ddl_statements = SchemaManager.generate_pgq_schema_ddl(duck_graph, *MODELS, dialect="duckdb")
    for statement in ddl_statements:
        conn.execute(statement)

    user_tbl = USER_LABEL.lower()
    order_tbl = ORDER_LABEL.lower()
    human_tbl = HUMAN_LABEL.lower()
    places_tbl = PLACES_TYPE.lower()
    knows_tbl = KNOWS_TYPE.lower()
    calls_tbl = CALLS_TYPE.lower()

    # Insert rows
    conn.execute(
        f"INSERT INTO \"{user_tbl}\" (uid, email, name, age) VALUES ('u1', 'alice@duck.org', 'Alice', 30), "
        f"('u2', 'bob@duck.org', 'Bob', 25);"
    )
    conn.execute(f"INSERT INTO \"{order_tbl}\" (oid, total) VALUES ('o1', 42.0), ('o2', 88.0);")
    conn.execute(
        f"INSERT INTO \"{human_tbl}\" (pid, display_name) VALUES ('p1', 'Charlie'), ('p2', 'Diana');"
    )
    conn.execute(
        f"INSERT INTO \"{places_tbl}\" (src_uid, dst_oid, placed_at) VALUES ('u1', 'o1', '2026-01-01');"
    )
    # Insert TWO edges between same pair of nodes (surrogate key auto-generated from sequence)
    conn.execute(
        f"INSERT INTO \"{knows_tbl}\" (src_pid, dst_pid, since) VALUES ('p1', 'p2', 2020), ('p1', 'p2', 2024);"
    )
    # Insert TWO edges between same pair of nodes with explicit primary key
    conn.execute(
        f"INSERT INTO \"{calls_tbl}\" (src_pid, dst_pid, call_id, duration) VALUES ('p1', 'p2', 'c1', 120), ('p1', 'p2', 'c2', 300);"
    )

    bridge = DuckDbBridge(conn)

    # Query with DuckPGQ
    u, o = Zz327User("u1"), Zz327Order("o1")
    order_q = (
        Query.match(u)
        .to(Zz327Places, var="pl")
        .node(o)
        .where(u.uid == "u1")
        .return_(buyer=u.name, order_id=o.oid)
        .compile("duckpgq", graph_name=duck_graph)
    )
    res = bridge.execute(order_q.statement, order_q.parameters)
    assert res == [{"buyer": "Alice", "order_id": "o1"}]

    # Query self-relationship with DuckPGQ (parallel edges with surrogate key)
    p1, p2 = Zz327Person("p1"), Zz327Person("p2")
    knows_q = (
        Query.match(p1)
        .to(Zz327Knows, var="k")
        .node(p2)
        .where(p1.pid == "p1")
        .return_(person=p1.display_name, friend=p2.display_name, since=Zz327Knows.since)
        .compile("duckpgq", graph_name=duck_graph)
    )
    knows_res = bridge.execute(knows_q.statement, knows_q.parameters)
    assert len(knows_res) == 2
    assert sorted([r["since"] for r in knows_res]) == [2020, 2024]

    # Query relationship model with explicit primary key
    calls_q = (
        Query.match(p1)
        .to(Zz327Calls, var="c")
        .node(p2)
        .where(p1.pid == "p1")
        .return_(
            person=p1.display_name,
            friend=p2.display_name,
            call_id=Zz327Calls.call_id,
            duration=Zz327Calls.duration,
        )
        .compile("duckpgq", graph_name=duck_graph)
    )
    calls_res = bridge.execute(calls_q.statement, calls_q.parameters)
    assert len(calls_res) == 2
    assert sorted([r["call_id"] for r in calls_res]) == ["c1", "c2"]
    assert sorted([r["duration"] for r in calls_res]) == [120, 300]

    # Introspecting duckdb raises UnsupportedFeature
    with pytest.raises(Exception) as exc_info:
        introspect_schema("duckdb:///:memory:")
    assert "DuckDB" in str(exc_info.value)
