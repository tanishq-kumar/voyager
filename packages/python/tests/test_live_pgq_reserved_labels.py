"""Live PostgreSQL 19 Beta 3 test: reserved words as SQL/PGQ labels.

The node labels are ``User`` and ``Order``, which are reserved SQL words, so the
whole path has to be quoted: the vertex and edge tables, the ``CREATE PROPERTY
GRAPH`` statement, and the ``IS "User"``/``IS "Order"`` patterns the SQL/PGQ
emitter produces. The test derives the relational table DDL from
``export_schema`` (the library has no table DDL emitter, and ``CREATE PROPERTY
GRAPH`` only describes existing tables), runs it together with the generated
constraint, index and property-graph DDL, inserts rows, queries through the
SQL/PGQ emitter, and drops everything afterwards.

Set ``VOYAGER_TEST_HOST`` to run against another machine (via
``conftest.get_test_host()``); the test is skipped when the port is not reachable.
"""

from __future__ import annotations

import os
import socket
from collections.abc import Iterator
from typing import Any

import pytest
from conftest import get_test_host
from voyager_ogm import (
    Field,
    NativeClient,
    Node,
    Query,
    Relationship,
    export_schema,
    node,
    relationship,
)
from voyager_ogm.schema import SchemaManager

pytestmark = pytest.mark.live

HOST = get_test_host()
PORT = 5456
GRAPH = f"fix325b_g_{os.getpid()}"

# Deliberately reserved SQL words (and Cypher keywords) as labels.
USER = "User"
ORDER = "Order"
PLACES = "PLACES"


@node(USER)
class Fix325bUser(Node):
    uid: str = Field(primary_key=True)
    email: str = Field(unique=True)
    name: str = Field(index=True)
    age: int = 0


@node(ORDER)
class Fix325bOrder(Node):
    oid: str = Field(primary_key=True)
    total: float = 0.0


@relationship(PLACES, source_node=Fix325bUser, target_node=Fix325bOrder)
class Fix325bPlaces(Relationship):
    at: str = Field(index=True)


MODELS: list[Any] = [Fix325bUser, Fix325bOrder, Fix325bPlaces]

_SQL_TYPES = {
    "STRING": "TEXT",
    "INTEGER": "BIGINT",
    "FLOAT": "DOUBLE PRECISION",
    "BOOLEAN": "BOOLEAN",
    "DATETIME": "TIMESTAMP WITH TIME ZONE",
    "DATE": "DATE",
    "DURATION": "INTERVAL",
}


def _reachable() -> bool:
    try:
        with socket.create_connection((HOST, PORT), timeout=0.5):
            return True
    except OSError:
        return False


def _quote(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def _sql_type(property_schema: dict[str, Any]) -> str:
    return _SQL_TYPES.get(str(property_schema["type"]), "TEXT")


def _column(name: str, property_schema: dict[str, Any]) -> str:
    definition = f"{_quote(name)} {_sql_type(property_schema)}"
    if not property_schema["nullable"]:
        definition += " NOT NULL"
    return definition


def _node_table_ddl(node_schema: dict[str, Any]) -> str:
    columns = [
        _column(name, property_schema)
        for name, property_schema in node_schema["properties"].items()
    ]
    primary_key = node_schema["primary_key"]
    if primary_key is not None:
        columns.append(f"PRIMARY KEY ({_quote(primary_key)})")
    table = node_schema["labels"][0].lower()
    return f"CREATE TABLE {_quote(table)} ({', '.join(columns)})"


def _edge_table_ddl(
    relationship_schema: dict[str, Any],
    nodes_by_label: dict[str, dict[str, Any]],
) -> str:
    source = nodes_by_label[relationship_schema["source_labels"][0]]
    target = nodes_by_label[relationship_schema["target_labels"][0]]
    source_table = source["labels"][0].lower()
    target_table = target["labels"][0].lower()
    source_key = f"src_{source['primary_key']}"
    target_key = f"dst_{target['primary_key']}"
    source_property = source["properties"][source["primary_key"]]
    target_property = target["properties"][target["primary_key"]]
    columns = [
        f"{_column(source_key, source_property)} REFERENCES "
        f"{_quote(source_table)} ({_quote(source['primary_key'])})",
        f"{_column(target_key, target_property)} REFERENCES "
        f"{_quote(target_table)} ({_quote(target['primary_key'])})",
    ]
    columns.extend(
        _column(name, property_schema)
        for name, property_schema in relationship_schema["properties"].items()
    )
    columns.append(f"PRIMARY KEY ({_quote(source_key)}, {_quote(target_key)})")
    table = relationship_schema["type"].lower()
    return f"CREATE TABLE {_quote(table)} ({', '.join(columns)})"


def _table_ddl() -> list[str]:
    schema = export_schema(MODELS)
    nodes_by_label = {entry["labels"][0]: entry for entry in schema["nodes"]}
    return [_node_table_ddl(entry) for entry in schema["nodes"]] + [
        _edge_table_ddl(entry, nodes_by_label) for entry in schema["relationships"]
    ]


def _drop_everything(client: NativeClient) -> None:
    client.execute_sync(f"DROP PROPERTY GRAPH IF EXISTS {GRAPH} CASCADE")
    for table in (PLACES.lower(), ORDER.lower(), USER.lower()):
        client.execute_sync(f"DROP TABLE IF EXISTS {_quote(table)} CASCADE")


@pytest.fixture
def postgres19() -> Iterator[NativeClient]:
    if not _reachable():
        pytest.skip(f"PostgreSQL 19 is not reachable on {HOST}:{PORT}")
    base = f"postgresql://postgres:voyagerpass123@{HOST}:{PORT}/voyager_graph"
    client = NativeClient(base, min_idle=1, max_size=2)
    _drop_everything(client)
    try:
        yield client
    finally:
        _drop_everything(client)
        client.close()


def test_live_postgresql_19_reserved_word_labels(postgres19: NativeClient) -> None:
    """Runs generated table, constraint, index and property-graph DDL for ``User``/``Order``."""
    client = postgres19

    for statement in _table_ddl():
        client.execute_sync(statement)
    for statement in SchemaManager.generate_constraint_ddl(*MODELS, dialect="postgres"):
        client.execute_sync(statement)
    for statement in SchemaManager.generate_index_ddl(*MODELS, dialect="postgres"):
        client.execute_sync(statement)

    property_graph = SchemaManager.generate_pgq_ddl(GRAPH, *MODELS)
    assert f'LABEL "{USER}"' in property_graph
    assert f'LABEL "{ORDER}"' in property_graph
    assert f'REFERENCES "{USER.lower()}"' in property_graph
    assert f'REFERENCES "{ORDER.lower()}"' in property_graph
    assert 'SOURCE KEY ("src_uid")' in property_graph
    assert 'DESTINATION KEY ("dst_oid")' in property_graph
    client.execute_sync(property_graph)

    client.execute_sync(
        f'INSERT INTO "{USER.lower()}" (uid, email, name, age) '
        "VALUES ('u1', 'a@x', 'Alice', 30), ('u2', 'b@x', 'Bob', 40)"
    )
    client.execute_sync(
        f"INSERT INTO \"{ORDER.lower()}\" (oid, total) VALUES ('o1', 10.5), ('o2', 20.0)"
    )
    client.execute_sync(
        f'INSERT INTO "{PLACES.lower()}" (src_uid, dst_oid, at) '
        "VALUES ('u1', 'o1', '2024-01-01'), ('u2', 'o2', '2024-02-02')"
    )

    user, order = Fix325bUser("u1"), Fix325bOrder("o1")
    joined = (
        Query.match(user)
        .to(Fix325bPlaces, var="p")
        .node(order)
        .where(user.uid == "u1")
        .return_(user_name=user.name, order_id=order.oid)
        .compile("sql_pgq", graph_name=GRAPH)
    )
    assert f'IS "{USER}"' in joined.statement
    assert f'IS "{ORDER}"' in joined.statement
    assert f'IS "{PLACES}"' in joined.statement
    rows = client.execute_sync(joined.statement, joined.parameters).to_dicts()
    assert rows == [{"user_name": "Alice", "order_id": "o1"}]

    big_orders = (
        Query.match(order)
        .where(order.total > 15.0)
        .return_(oid=order.oid, total=order.total)
        .order_by(order.oid)
        .compile("sql_pgq", graph_name=GRAPH)
    )
    assert f'IS "{ORDER}"' in big_orders.statement
    rows = client.execute_sync(big_orders.statement, big_orders.parameters).to_dicts()
    assert rows == [{"oid": "o2", "total": 20.0}]

    # The documented migration re-creates only the graph descriptor; the tables
    # and their rows stay in place because CREATE PROPERTY GRAPH describes
    # existing tables.
    client.execute_sync(SchemaManager.generate_pgq_drop_ddl(GRAPH))
    client.execute_sync(property_graph)
    rows = client.execute_sync(joined.statement, joined.parameters).to_dicts()
    assert rows == [{"user_name": "Alice", "order_id": "o1"}]
