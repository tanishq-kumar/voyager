"""Live tests: ``introspect_schema`` against ``export_schema`` on each engine.

Each test creates the schema of the models below, inserts a few nodes and
relationships, introspects the database, compares every fact the engine reports
with ``export_schema`` for the same models, and drops everything afterwards.
The PostgreSQL 19 case builds its tables with the generated naming rule and
creates the property graph from the generated DDL, then queries it with the
SQL/PGQ emitter.

Set ``VOYAGER_TEST_HOST`` to run against another machine (default
``127.0.0.1``); a test is skipped when its port is not reachable.
"""

from __future__ import annotations

import os
import socket
import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from voyager_ogm import (
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
# The label deliberately differs from the class name.
PERSON = "IxPyHuman"
COMPANY = "IxPyCompany"
KNOWS = "IX_PY_KNOWS"
WORKS_AT = "IX_PY_WORKS_AT"


@node(PERSON)
class IxPyPerson(Node):
    pid: str = Field(primary_key=True)
    email: str = Field(unique=True)
    name: str | None = Field(default=None, index=True)
    age: int = 0


@node(COMPANY)
class IxPyCompany(Node):
    cid: str = Field(primary_key=True)
    name: str | None = Field(default=None, index=True)


@relationship(KNOWS, source_labels=[PERSON], target_labels=[PERSON])
class IxPyKnows(Relationship):
    since: int = Field(index=True)


@relationship(WORKS_AT, source_labels=[PERSON], target_labels=[COMPANY])
class IxPyWorksAt(Relationship):
    since: int = Field()


MODELS: list[Any] = [IxPyPerson, IxPyCompany, IxPyKnows, IxPyWorksAt]

CREATE_DATA = (
    f"CREATE (p1:{PERSON} {{pid: 'p1', email: 'a@x', name: 'A', age: 1}})"
    f"-[:{KNOWS} {{since: 2020}}]->"
    f"(p2:{PERSON} {{pid: 'p2', email: 'b@x', name: 'B', age: 2}}),"
    f"(p2)-[:{WORKS_AT} {{since: 2021}}]->"
    f"(:{COMPANY} {{cid: 'c1', name: 'Acme'}})"
)


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=0.5):
            return True
    except OSError:
        return False


def _entry(entries: list[dict[str, Any]], name: str) -> dict[str, Any]:
    matches = [entry for entry in entries if entry["name"] == name]
    assert matches, f"{name} missing from {[entry['name'] for entry in entries]}"
    return matches[0]


def _compare_properties(
    model_props: dict[str, Any],
    live_props: dict[str, Any],
    *,
    check_nullable: bool,
    check_primary_key: bool,
) -> None:
    for name, declared in model_props.items():
        live = live_props[name]
        expected_keys = set(declared) - {"default", "vector"}
        assert set(live) == expected_keys, name
        for key in ("attribute", "description", "has_default", "constraints"):
            assert live[key] is None, (name, key)
        assert live["type"] == declared["type"], name
        assert live["unique"] == (declared["unique"] or declared["primary_key"]), name
        if not (declared["unique"] or declared["primary_key"]):
            # A unique or primary-key property is backed by its constraint's
            # index, which the generated DDL does not declare separately.
            assert live["indexed"] == declared["indexed"], name
            assert live["index_type"] == declared["index_type"], name
        if check_nullable:
            assert live["nullable"] == declared["nullable"], name
        if check_primary_key:
            assert bool(live["primary_key"]) == declared["primary_key"], name


def _compare(
    live: dict[str, Any], *, check_nullable: bool, check_primary_key: bool = False
) -> None:
    exported = export_schema(MODELS)
    for declared in exported["nodes"]:
        label = declared["labels"][0]
        found = _entry(live["nodes"], label)
        assert set(found) == set(declared)
        assert found["labels"] == declared["labels"]
        assert found["model"] is None and found["description"] is None
        if check_primary_key:
            assert found["primary_key"] == declared["primary_key"]
        _compare_properties(
            declared["properties"],
            found["properties"],
            check_nullable=check_nullable,
            check_primary_key=check_primary_key,
        )
    for declared in exported["relationships"]:
        found = _entry(live["relationships"], declared["type"])
        assert set(found) == set(declared)
        assert found["source_labels"] == declared["source_labels"]
        assert found["target_labels"] == declared["target_labels"]
        assert found["directed"] == declared["directed"]
        _compare_properties(
            declared["properties"],
            found["properties"],
            check_nullable=check_nullable,
            check_primary_key=False,
        )


def _run_all(execute: Callable[[str], Any], statements: list[str]) -> None:
    for statement in statements:
        execute(statement)


@pytest.fixture
def neo4j() -> Iterator[tuple[NativeClient, str]]:
    if not _reachable(7687):
        pytest.skip(f"Neo4j is not reachable on {HOST}:7687")
    uri = f"bolt://neo4j:voyagerpass123@{HOST}:7687"
    client = NativeClient(uri, min_idle=1, max_size=2)
    yield client, uri
    client.execute_sync(f"MATCH (n:{PERSON}) DETACH DELETE n")
    client.execute_sync(f"MATCH (n:{COMPANY}) DETACH DELETE n")
    _run_all(client.execute_sync, SchemaManager.generate_drop_constraint_ddl(*MODELS))
    _run_all(client.execute_sync, SchemaManager.generate_drop_index_ddl(*MODELS))
    client.close()


def test_live_neo4j_matches_export_schema(neo4j: tuple[NativeClient, str]) -> None:
    client, uri = neo4j
    for statement in SchemaManager.generate_constraint_ddl(*MODELS, dialect="cypher"):
        try:
            client.execute_sync(statement)
        except ValueError as exc:
            # Existence constraints need Neo4j Enterprise Edition.
            if "NOT NULL" not in statement:
                raise
            assert "Enterprise" in str(exc) or "edition" in str(exc).lower(), exc
    _run_all(client.execute_sync, SchemaManager.generate_index_ddl(*MODELS, dialect="cypher"))
    client.execute_sync(CREATE_DATA)

    live = introspect_schema(uri)
    assert live["source"]["engine"] == "neo4j"
    community = live["source"]["edition"] == "community"
    _compare(live, check_nullable=not community)
    # The same result through a NativeClient source.
    assert introspect_schema(client)["nodes"] == live["nodes"]


@pytest.fixture
def memgraph() -> Iterator[tuple[NativeClient, str]]:
    if not _reachable(7688):
        pytest.skip(f"Memgraph is not reachable on {HOST}:7688")
    uri = f"bolt://{HOST}:7688"
    client = NativeClient(uri, min_idle=1, max_size=2)
    yield client, uri
    client.execute_sync(f"MATCH (n:{PERSON}) DETACH DELETE n")
    client.execute_sync(f"MATCH (n:{COMPANY}) DETACH DELETE n")
    for statement in SchemaManager.generate_drop_constraint_ddl(
        *MODELS, dialect="memgraph"
    ) + SchemaManager.generate_drop_index_ddl(*MODELS, dialect="memgraph"):
        client.execute_sync(statement)
    client.close()


def test_live_memgraph_matches_export_schema(memgraph: tuple[NativeClient, str]) -> None:
    client, uri = memgraph
    _run_all(
        client.execute_sync,
        SchemaManager.generate_constraint_ddl(*MODELS, dialect="memgraph")
        + SchemaManager.generate_index_ddl(*MODELS, dialect="memgraph"),
    )
    client.execute_sync(CREATE_DATA)

    live = introspect_schema(uri)
    assert live["source"]["engine"] == "memgraph"
    _compare(live, check_nullable=True)


@pytest.fixture
def falkordb() -> Iterator[tuple[Any, str, str]]:
    if not _reachable(6379):
        pytest.skip(f"FalkorDB is not reachable on {HOST}:6379")
    falkordb_pkg = pytest.importorskip("falkordb")
    graph_name = f"ix_py_{os.getpid()}"
    db = falkordb_pkg.FalkorDB(host=HOST, port=6379)
    graph = db.select_graph(graph_name)
    yield db, graph_name, f"falkordb://{HOST}:6379?graph={graph_name}"
    graph.delete()


def test_live_falkordb_matches_export_schema(falkordb: tuple[Any, str, str]) -> None:
    db, graph_name, uri = falkordb
    graph = db.select_graph(graph_name)
    graph.query(CREATE_DATA)
    _run_all(graph.query, SchemaManager.generate_index_ddl(*MODELS, dialect="falkordb"))
    # FalkorDB manages constraints with GRAPH.CONSTRAINT; a unique constraint
    # needs a range index on the property first.
    exported = export_schema(MODELS)
    for declared in exported["nodes"]:
        label = declared["labels"][0]
        for prop in declared["properties"].values():
            if prop["unique"] or prop["primary_key"]:
                graph.query(f"CREATE INDEX FOR (n:{label}) ON (n.{prop['name']})")
                db.connection.execute_command(
                    "GRAPH.CONSTRAINT", "CREATE", graph_name, "UNIQUE", "NODE",
                    label, "PROPERTIES", "1", prop["name"],
                )  # fmt: skip
            if not prop["nullable"]:
                db.connection.execute_command(
                    "GRAPH.CONSTRAINT", "CREATE", graph_name, "MANDATORY", "NODE",
                    label, "PROPERTIES", "1", prop["name"],
                )  # fmt: skip
    for _ in range(50):
        statuses = [row[4] for row in graph.query("CALL db.constraints()").result_set]
        if all(status == "OPERATIONAL" for status in statuses):
            break
        time.sleep(0.1)

    live = introspect_schema(uri)
    assert live["source"]["engine"] == "falkordb"
    assert live["source"]["database"] == graph_name
    _compare(live, check_nullable=True)


@pytest.fixture
def postgres19() -> Iterator[tuple[NativeClient, str]]:
    if not _reachable(5456):
        pytest.skip(f"PostgreSQL 19 is not reachable on {HOST}:5456")
    base = f"postgresql://postgres:voyagerpass123@{HOST}:5456/voyager_graph"
    client = NativeClient(base, min_idle=1, max_size=2)
    graph = f"ix_py_g_{os.getpid()}"
    yield client, f"{base}?graph={graph}"
    client.execute_sync(f"DROP PROPERTY GRAPH IF EXISTS {graph} CASCADE")
    for table in (WORKS_AT.lower(), KNOWS.lower(), COMPANY.lower(), PERSON.lower()):
        client.execute_sync(f'DROP TABLE IF EXISTS "{table}" CASCADE')
    client.close()


def test_live_postgresql_19_matches_export_schema(
    postgres19: tuple[NativeClient, str],
) -> None:
    client, uri = postgres19
    graph = uri.rsplit("graph=", 1)[1]
    person, company = PERSON.lower(), COMPANY.lower()
    knows, works_at = KNOWS.lower(), WORKS_AT.lower()
    # Tables are named after the vertex label and edge type in lower case, the
    # same rule the generated constraint, index and property-graph DDL uses.
    # Edge columns are src_<key> and dst_<key>, so a self-relationship has two
    # distinct endpoint columns.
    client.execute_sync(
        f'CREATE TABLE "{person}" (pid text PRIMARY KEY, email text, name text, age bigint)'
    )
    client.execute_sync(f'CREATE TABLE "{company}" (cid text PRIMARY KEY, name text)')
    client.execute_sync(
        f'CREATE TABLE "{knows}" (src_pid text REFERENCES "{person}" (pid), '
        f'dst_pid text REFERENCES "{person}" (pid), since bigint, '
        f"PRIMARY KEY (src_pid, dst_pid))"
    )
    client.execute_sync(
        f'CREATE TABLE "{works_at}" (src_pid text REFERENCES "{person}" (pid), '
        f'dst_cid text REFERENCES "{company}" (cid), since bigint, '
        f"PRIMARY KEY (src_pid, dst_cid))"
    )
    _run_all(
        client.execute_sync,
        SchemaManager.generate_constraint_ddl(*MODELS, dialect="postgres")
        + SchemaManager.generate_index_ddl(*MODELS, dialect="postgres"),
    )
    # Build the property graph from the generated DDL instead of by hand.
    client.execute_sync(SchemaManager.generate_pgq_ddl(graph, *MODELS))
    _run_all(
        client.execute_sync,
        [
            f'INSERT INTO "{person}" (pid, email, name, age) '
            "VALUES ('p1', 'a@x', 'A', 1), ('p2', 'b@x', 'B', 2)",
            f"INSERT INTO \"{company}\" (cid, name) VALUES ('c1', 'Acme')",
            f"INSERT INTO \"{knows}\" (src_pid, dst_pid, since) VALUES ('p1', 'p2', 2020)",
            f"INSERT INTO \"{works_at}\" (src_pid, dst_cid, since) VALUES ('p2', 'c1', 2021)",
        ],
    )

    # Query the generated graph with the SQL/PGQ emitter's output, including
    # the self-relationship and the relationship between the two models.
    p1, p2, c1 = IxPyPerson("p1"), IxPyPerson("p2"), IxPyCompany("c1")
    friend_query = (
        Query.match(p1)
        .to(IxPyKnows, var="k")
        .node(p2)
        .where(p1.pid == "p1")
        .return_(person_name=p1.name, friend_name=p2.name)
        .compile("sql_pgq", graph_name=graph)
    )
    friend_rows = client.execute_sync(friend_query.statement, friend_query.parameters).to_dicts()
    assert friend_rows == [{"person_name": "A", "friend_name": "B"}]

    employer_query = (
        Query.match(p1)
        .to(IxPyWorksAt, var="w")
        .node(c1)
        .where(p1.pid == "p2")
        .return_(person_name=p1.name, company_name=c1.name)
        .compile("sql_pgq", graph_name=graph)
    )
    employer_rows = client.execute_sync(
        employer_query.statement, employer_query.parameters
    ).to_dicts()
    assert employer_rows == [{"person_name": "B", "company_name": "Acme"}]

    live = introspect_schema(uri)
    assert live["source"]["engine"] == "postgresql"
    assert live["source"]["database"] == graph
    _compare(live, check_nullable=True, check_primary_key=True)
