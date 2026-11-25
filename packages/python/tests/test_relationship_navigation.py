"""Tests for declarative relationship navigation (#121).

Covers descriptors, forward references, managers, staging, rollback, the
identity map, sync and async sessions, and live Neo4j/Memgraph execution.
"""

from __future__ import annotations

import os
import socket
from typing import Any, cast

import pytest
from voyager_ogm import (
    AsyncSession,
    Field,
    Node,
    Relationship,
    RelationshipFrom,
    RelationshipTo,
    SchemaManager,
    Session,
    VoyagerError,
    export_schema,
    node,
    relationship,
    reset_alias_counters,
)
from voyager_ogm.bridge import AsyncMockBridge, MockBridge, create_bridge
from voyager_ogm.models import (
    RelationshipFrom as ModelsRelationshipFrom,
)
from voyager_ogm.models import (
    RelationshipTo as ModelsRelationshipTo,
)
from voyager_ogm.relationships import register_model_class

HOST = os.environ.get("VOYAGER_TEST_HOST", "127.0.0.1")


class RecordingMockBridge(MockBridge):
    """Mock bridge that answers navigation RETURN queries with configured rows."""

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        super().__init__()
        self.rows = rows or []

    def execute(
        self, statement: str, parameters: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        super().execute(statement, parameters)
        marker = " RETURN "
        if marker not in statement or not self.rows:
            return []
        return list(self.rows)


class Feat121Knows(Relationship, type_name="FEAT121_KNOWS"):
    since: int


class Feat121Follows(Relationship, type_name="FEAT121_FOLLOWS"):
    since: int = 2026


@node(label="Feat121Person")
class Feat121Person:
    id: str = Field(primary_key=True)
    name: str
    age: int = 0
    friends: RelationshipTo[Feat121Person] = RelationshipTo(
        target="Feat121Person", rel_type="FEAT121_KNOWS", model=Feat121Knows
    )
    followers: RelationshipFrom[Feat121Person] = RelationshipFrom(
        target="Feat121Person", rel_type="FEAT121_FOLLOWS"
    )
    plain_friends: RelationshipTo[Feat121Person] = RelationshipTo(
        target="Feat121Person", rel_type="FEAT121_KNOWS"
    )


@node(label="Feat121CircleA")
class Feat121CircleA:
    id: str = Field(primary_key=True)
    peers: RelationshipTo[Feat121CircleB] = RelationshipTo(
        target="Feat121CircleB", rel_type="FEAT121_CIRCLE"
    )


@node(label="Feat121CircleB")
class Feat121CircleB:
    id: str = Field(primary_key=True)
    peers: RelationshipTo[Feat121CircleA] = RelationshipTo(
        target="Feat121CircleA", rel_type="FEAT121_CIRCLE"
    )


@node(label="Feat121Broken")
class Feat121Broken:
    id: str = Field(primary_key=True)
    missing: RelationshipTo[Feat121NoSuchModel] = RelationshipTo(  # noqa: F821
        target="Feat121NoSuchModel", rel_type="FEAT121_MISSING"
    )


@pytest.fixture(autouse=True)
def _reset_aliases() -> None:
    reset_alias_counters()


def _session(rows: list[dict[str, Any]] | None = None) -> tuple[Session, MockBridge]:
    bridge = RecordingMockBridge(rows)
    return Session(bridge=bridge, enable_identity_map=True), bridge


def _people() -> tuple[Feat121Person, Feat121Person, Feat121Person]:
    return (
        Feat121Person(id="alice", name="Alice", age=30),
        Feat121Person(id="bob", name="Bob", age=18),
        Feat121Person(id="carol", name="Carol", age=16),
    )


def test_descriptors_are_exported_from_package_and_models() -> None:
    assert RelationshipTo is ModelsRelationshipTo
    assert RelationshipFrom is ModelsRelationshipFrom
    assert isinstance(Feat121Person.friends, RelationshipTo)
    assert isinstance(Feat121Person.followers, RelationshipFrom)


def test_instance_access_returns_manager_without_io() -> None:
    session, bridge = _session()
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    manager = alice.friends

    assert manager.__class__.__name__ == "RelationshipManager"
    assert bridge.executed_queries == []


def test_string_and_circular_forward_references_resolve() -> None:
    assert Feat121CircleA.peers.resolve() is Feat121CircleB
    assert Feat121CircleB.peers.resolve() is Feat121CircleA
    assert Feat121Person.friends.resolve() is Feat121Person


def test_unknown_target_name_raises_clear_error() -> None:
    broken = Feat121Broken(id="x")
    with pytest.raises(LookupError, match="Feat121NoSuchModel"):
        _ = broken.missing


def test_module_qualified_forward_reference_resolves() -> None:
    qualified = f"{Feat121Person.__module__}.Feat121Person"
    descriptor = RelationshipTo(target=qualified, rel_type="FEAT121_QUALIFIED")

    assert descriptor.resolve() is Feat121Person

    unknown = RelationshipTo(target="no_such_module.Feat121Person", rel_type="FEAT121_QUALIFIED")
    with pytest.raises(LookupError, match="no_such_module.Feat121Person"):
        unknown.resolve()


def test_ambiguous_bare_forward_reference_lists_candidates() -> None:
    left = type("Feat121DupModel", (), {"__module__": "feat121_left"})
    right = type("Feat121DupModel", (), {"__module__": "feat121_right"})
    register_model_class(left)
    register_model_class(right)
    descriptor = RelationshipTo(target="Feat121DupModel", rel_type="FEAT121_DUP")

    with pytest.raises(LookupError, match="ambiguous") as exc_info:
        descriptor.resolve()

    message = str(exc_info.value)
    assert "feat121_left.Feat121DupModel" in message
    assert "feat121_right.Feat121DupModel" in message


def test_hydration_decodes_apache_age_agtype_scalars() -> None:
    bridge = RecordingMockBridge([{"id": '"bob"', "name": '"Bob"', "age": "18"}])
    session = Session(bridge=bridge, dialect="age", enable_identity_map=True)
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    result = alice.friends.all()

    assert [person.get("id") for person in result] == ["bob"]
    assert result[0].get("name") == "Bob"
    assert result[0].get("age") == 18


def test_hydration_rejects_missing_projection_column() -> None:
    bridge = RecordingMockBridge([{"someone_else": "bob"}])
    session = Session(bridge=bridge, enable_identity_map=True)
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    with pytest.raises(VoyagerError, match="has no 'id' column") as exc_info:
        alice.friends.all()

    assert exc_info.value.code == "SCHEMA_ERROR"
    assert "someone_else" in str(exc_info.value)


def test_hydration_rejects_null_target_primary_key() -> None:
    bridge = RecordingMockBridge([{"id": None, "name": "Bob", "age": 18}])
    session = Session(bridge=bridge, enable_identity_map=True)
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    with pytest.raises(VoyagerError, match="is null") as exc_info:
        alice.friends.all()

    assert exc_info.value.code == "SCHEMA_ERROR"


def test_hydration_rejects_non_mapping_record() -> None:
    bridge = RecordingMockBridge()
    bridge.rows = cast("list[dict[str, Any]]", ["not", "a", "mapping"])
    session = Session(bridge=bridge, enable_identity_map=True)
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    with pytest.raises(VoyagerError, match="expected each row to be a mapping"):
        alice.friends.all()


def test_manager_on_instance_without_session_raises() -> None:
    alice = Feat121Person(id="alice", name="Alice")
    with pytest.raises(RuntimeError, match="no session"):
        alice.friends.all()


def test_unsaved_source_and_target_raise_clear_errors() -> None:
    session, _ = _session()
    alice = Feat121Person(name="Alice")
    session.register(alice)
    with pytest.raises(RuntimeError, match="not saved yet"):
        alice.friends.all()

    saved = Feat121Person(id="saved", name="Saved", session=session)
    unsaved_target = Feat121Person(name="Unsaved")
    with pytest.raises(RuntimeError, match="not saved yet"):
        saved.friends.connect(unsaved_target)


def test_all_hydrates_through_identity_map() -> None:
    session, _ = _session([{"id": "bob", "name": "Bob", "age": 18}])
    alice, bob, _ = _people()
    session.register(alice, is_clean=True)
    session.register(bob, is_clean=True)

    result = alice.friends.all()

    assert result == [bob]
    assert result[0] is bob
    assert result[0] is session.get(Feat121Person, "bob")
    statement = session.bridge.executed_queries[-1][0]
    assert "FEAT121_KNOWS" in statement
    assert "alice" in session.bridge.executed_queries[-1][1].values()


def test_filter_by_expression_and_keyword() -> None:
    session, bridge = _session([{"id": "bob", "name": "Bob", "age": 18}])
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    adults = alice.friends.filter(Feat121Person.age >= 18)
    named = alice.friends.filter(name="Bob")

    assert [p.get("id") for p in adults] == ["bob"]
    assert [p.get("id") for p in named] == ["bob"]
    assert "WHERE" in bridge.executed_queries[-2][0]
    assert "WHERE" in bridge.executed_queries[-1][0]


def test_incoming_direction_navigates_from_the_declared_side() -> None:
    session, bridge = _session([{"id": "carol", "name": "Carol", "age": 16}])
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    followers = alice.followers.all()

    assert [p.get("id") for p in followers] == ["carol"]
    statement = bridge.executed_queries[-1][0]
    assert "<-[" in statement
    assert "FEAT121_FOLLOWS" in statement


def test_relationship_model_and_plain_descriptor_both_navigate() -> None:
    session, _ = _session([{"id": "bob", "name": "Bob", "age": 18}])
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    assert [p.get("id") for p in alice.friends.all()] == ["bob"]
    assert [p.get("id") for p in alice.plain_friends.all()] == ["bob"]


def test_connect_and_disconnect_are_staged_and_written_on_flush() -> None:
    session, bridge = _session()
    alice, bob, _ = _people()
    for person in (alice, bob):
        session.register(person, is_clean=True)

    alice.friends.connect(bob, properties={"since": 2026})
    assert bridge.executed_queries == []
    assert len(session._pending_relationships) == 1

    session.flush()
    statement, params = bridge.executed_queries[-1]
    assert "CREATE" in statement
    assert "FEAT121_KNOWS" in statement
    assert params["p2"] == 2026
    assert session._pending_relationships == []

    alice.friends.disconnect(bob)
    assert len(session._pending_relationships) == 1
    assert bridge.executed_queries[-1] == (statement, params)
    session.flush()
    assert "DELETE" in bridge.executed_queries[-1][0]


def test_rollback_discards_staged_operations() -> None:
    session, _ = _session()
    alice, bob, _ = _people()
    for person in (alice, bob):
        session.register(person, is_clean=True)

    with pytest.raises(RuntimeError):
        with session.unit_of_work():
            alice.friends.connect(bob)
            assert len(session._pending_relationships) == 1
            raise RuntimeError("rollback")

    assert session._pending_relationships == []


def test_batched_connect_and_disconnect_use_unwind() -> None:
    session, bridge = _session()
    alice, bob, carol = _people()
    for person in (alice, bob, carol):
        session.register(person, is_clean=True)

    alice.friends.connect(bob)
    alice.friends.connect(carol)
    session.flush()
    statement, params = bridge.executed_queries[-1]
    assert statement.startswith("UNWIND $targets AS target_key")
    assert params["targets"] == ["bob", "carol"]

    alice.friends.disconnect(bob)
    alice.friends.disconnect(carol)
    session.flush()
    statement, params = bridge.executed_queries[-1]
    assert statement.startswith("UNWIND $targets AS target_key")
    assert "DELETE" in statement
    assert params["targets"] == ["bob", "carol"]


def test_incoming_connect_creates_edge_from_target_to_source() -> None:
    session, bridge = _session()
    alice, _, carol = _people()
    for person in (alice, carol):
        session.register(person, is_clean=True)

    alice.followers.connect(carol)
    session.flush()
    statement = bridge.executed_queries[-1][0]
    assert "CREATE" in statement
    assert "<-[:FEAT121_FOLLOWS]-" in statement


def test_flush_return_value_is_unchanged_when_relationship_operations_are_staged() -> None:
    session, _ = _session()
    alice, bob, _ = _people()
    for person in (alice, bob):
        session.register(person, is_clean=True)

    assert session.flush() == []
    assert session.last_relationship_results == []

    alice.friends.connect(bob)
    flushed = session.flush()

    assert flushed == []
    assert len(session.last_relationship_results) == 1
    assert session.last_relationship_results[0].statement.startswith("MATCH")


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_relationship_writes_are_rejected_before_io_on_read_only_dialects(
    dialect: str,
) -> None:
    bridge = MockBridge()
    session = Session(
        bridge=bridge, dialect=dialect, graph_name="feat121_graph", enable_identity_map=True
    )
    alice, bob, _ = _people()
    for person in (alice, bob):
        session.register(person, is_clean=True)

    with pytest.raises(VoyagerError, match="read-only") as exc_info:
        alice.friends.connect(bob)
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert session._pending_relationships == []
    assert bridge.executed_queries == []

    with pytest.raises(VoyagerError, match="read-only") as exc_info:
        alice.friends.disconnect(bob)
    assert exc_info.value.code == "UNSUPPORTED_FEATURE"
    assert session._pending_relationships == []
    assert bridge.executed_queries == []


@pytest.mark.parametrize("dialect", ["sql_pgq", "duckpgq"])
def test_navigation_projects_scalar_columns_for_sql_pgq_dialects(dialect: str) -> None:
    bridge = MockBridge()
    session = Session(
        bridge=bridge,
        dialect=dialect,
        graph_name="feat121_graph",
        enable_identity_map=True,
    )
    alice, _, _ = _people()
    session.register(alice, is_clean=True)

    assert alice.friends.all() == []

    statement = bridge.executed_queries[0][0]
    assert "GRAPH_TABLE" in statement
    assert "COLUMNS (" in statement
    assert "._feat121person_" not in statement
    assert 'AS "id"' in statement
    assert 'AS "name"' in statement
    assert 'AS "age"' in statement


def test_export_schema_lists_relationship_attributes_without_changing_keys() -> None:
    schema = export_schema([Feat121Person, Feat121Knows])
    person = next(node for node in schema["nodes"] if node["name"] == "Feat121Person")

    assert {"name", "model", "labels", "description", "primary_key", "properties"} <= set(person)
    names = [item["name"] for item in person["relationship_attributes"]]
    assert names == ["friends", "followers", "plain_friends"]
    assert person["relationship_attributes"][0]["rel_type"] == "FEAT121_KNOWS"
    assert person["relationship_attributes"][1]["direction"] == "incoming"
    assert "properties" not in person["relationship_attributes"][0]


@pytest.mark.asyncio
async def test_async_manager_reads_and_staging() -> None:
    bridge = AsyncMockBridge(RecordingMockBridge([{"id": "bob", "name": "Bob", "age": 18}]))
    session = AsyncSession(bridge=bridge, enable_identity_map=True)
    alice, bob, _ = _people()
    for person in (alice, bob):
        session.register(person, is_clean=True)

    result = await alice.friends.all()
    filtered = await alice.friends.filter(Feat121Person.age >= 18)
    incoming = await alice.followers.all()

    assert result == [bob]
    assert filtered == [bob]
    assert incoming == [bob]
    assert any("<-[" in statement for statement, _ in bridge.executed_queries)

    alice.friends.connect(bob)
    assert len(session._pending_relationships) == 1
    await session.flush()
    assert session._pending_relationships == []
    assert any("CREATE" in statement for statement, _ in bridge.executed_queries)


def _port_open(port: int) -> bool:
    try:
        with socket.create_connection((HOST, port), timeout=1.0):
            return True
    except OSError:
        return False


def _live_driver(port: int) -> Any:
    from neo4j import GraphDatabase

    if port == 7687:
        return GraphDatabase.driver(
            f"bolt://{HOST}:7687",
            auth=("neo4j", "voyagerpass123"),
            connection_timeout=2.0,
        )
    return GraphDatabase.driver(
        f"bolt://{HOST}:7688",
        auth=("", ""),
        connection_timeout=2.0,
    )


def _live_cleanup(session: Session) -> None:
    """Removes the live test nodes with a dialect-appropriate statement."""
    if session.dialect == "age":
        graph = session.graph_name
        session.execute(
            "SELECT * FROM cypher("
            f"'{graph}', $$ MATCH (n:Feat121LivePerson) DETACH DELETE n $$"
            ") AS (deleted agtype)"
        )
    else:
        session.execute("MATCH (n:Feat121LivePerson) DETACH DELETE n")


def _seed_live_people(session: Session) -> None:
    """Creates the live test nodes with raw statements."""
    for person_id, name, age in (
        ("alice", "Alice", 30),
        ("bob", "Bob", 18),
        ("carol", "Carol", 16),
    ):
        session.execute(
            f"CREATE (n:Feat121LivePerson {{id: '{person_id}', name: '{name}', age: {age}}})"
        )


def _exercise_live_relationship_navigation(
    session: Session, *, write_with_flush: bool = True
) -> None:
    _live_cleanup(session)
    try:
        alice = Feat121LivePerson(id="alice", name="Alice", age=30)
        bob = Feat121LivePerson(id="bob", name="Bob", age=18)
        carol = Feat121LivePerson(id="carol", name="Carol", age=16)
        if write_with_flush:
            for person in (alice, bob, carol):
                session.register(person)
            session.flush()

            alice.friends.connect(bob, properties={"since": 2026})
            alice.friends.connect(carol)
            alice.followers.connect(carol)
            session.flush()
        else:
            _seed_live_people(session)
            for person in (alice, bob, carol):
                session.register(person, is_clean=True)

            alice.friends.connect(bob, properties={"since": 2026})
            session.flush()
            alice.friends.connect(carol)
            session.flush()
            alice.followers.connect(carol)
            session.flush()

        friends = {person.get("id"): person for person in alice.friends.all()}
        assert set(friends) == {"bob", "carol"}
        assert {person_id: person.get("name") for person_id, person in friends.items()} == {
            "bob": "Bob",
            "carol": "Carol",
        }
        assert {person_id: person.get("age") for person_id, person in friends.items()} == {
            "bob": 18,
            "carol": 16,
        }
        adults = alice.friends.filter(Feat121LivePerson.age >= 18)
        assert [person.get("id") for person in adults] == ["bob"]
        assert adults[0].get("name") == "Bob"
        assert {person.get("id") for person in alice.followers.all()} == {"carol"}

        if write_with_flush:
            alice.friends.disconnect(bob)
            alice.followers.disconnect(carol)
            session.flush()
        else:
            alice.friends.disconnect(bob)
            session.flush()
            alice.followers.disconnect(carol)
            session.flush()
        assert {person.get("id") for person in alice.friends.all()} == {"carol"}
        assert alice.followers.all() == []
    finally:
        _live_cleanup(session)


@node(label="Feat121LivePerson")
class Feat121LivePerson:
    id: str = Field(primary_key=True)
    name: str
    age: int = 0
    friends: RelationshipTo[Feat121LivePerson] = RelationshipTo(
        target="Feat121LivePerson", rel_type="FEAT121_LIVE_KNOWS", model=Feat121Knows
    )
    followers: RelationshipFrom[Feat121LivePerson] = RelationshipFrom(
        target="Feat121LivePerson", rel_type="FEAT121_LIVE_FOLLOWS"
    )


LIVE_FALKOR_GRAPH = "feat121_live_navigation"
LIVE_AGE_GRAPH = "feat121_live_navigation"


def _open_live_session(engine: str, backend: str) -> tuple[Session, Any]:
    """Opens a live session for one engine and backend.

    Returns the session and a zero-argument cleanup callable. ``backend`` is
    ``"native"`` for the default URI session (``backend="auto"`` routing to the
    Rust client) or ``"bridge"`` for the Python driver bridge.
    """
    if engine == "neo4j":
        if not _port_open(7687):
            pytest.skip(f"Neo4j not reachable on {HOST}:7687")
        if backend == "native":
            session = Session(
                f"bolt://neo4j:voyagerpass123@{HOST}:7687",
                dialect="cypher",
                enable_identity_map=True,
            )
            return session, session.close
        driver = _live_driver(7687)
        try:
            driver.verify_connectivity()
        except Exception as exc:  # pragma: no cover - environment dependent
            driver.close()
            pytest.skip(f"Neo4j not reachable on {HOST}:7687: {exc}")
        session = Session(bridge=driver, dialect="cypher", enable_identity_map=True)

        def _close_neo4j() -> None:
            session.close()
            driver.close()

        return session, _close_neo4j

    if engine == "memgraph":
        if not _port_open(7688):
            pytest.skip(f"Memgraph not reachable on {HOST}:7688")
        if backend == "native":
            session = Session(
                f"bolt://{HOST}:7688",
                dialect="cypher",
                enable_identity_map=True,
            )
            return session, session.close
        driver = _live_driver(7688)
        try:
            driver.verify_connectivity()
        except Exception as exc:  # pragma: no cover - environment dependent
            driver.close()
            pytest.skip(f"Memgraph not reachable on {HOST}:7688: {exc}")
        session = Session(bridge=driver, dialect="cypher", enable_identity_map=True)

        def _close_memgraph() -> None:
            session.close()
            driver.close()

        return session, _close_memgraph

    if engine == "falkordb":
        if not _port_open(6379):
            pytest.skip(f"FalkorDB not reachable on {HOST}:6379")
        try:
            from falkordb import FalkorDB
        except ImportError:
            pytest.skip("falkordb package is required for live FalkorDB tests")

        client = FalkorDB(host=HOST, port=6379)
        if backend == "native":
            session = Session(
                f"redis://{HOST}:6379/{LIVE_FALKOR_GRAPH}",
                dialect="cypher",
                graph_name=LIVE_FALKOR_GRAPH,
                enable_identity_map=True,
            )
        else:
            graph = client.select_graph(LIVE_FALKOR_GRAPH)
            session = Session(
                bridge=create_bridge(graph),
                dialect="cypher",
                enable_identity_map=True,
            )

        def _close_falkordb() -> None:
            session.close()
            try:
                client.select_graph(LIVE_FALKOR_GRAPH).delete()
            except Exception:  # pragma: no cover - best-effort cleanup
                pass

        return session, _close_falkordb

    if engine == "age":
        if not _port_open(5455):
            pytest.skip(f"Apache AGE not reachable on {HOST}:5455")
        psycopg = pytest.importorskip("psycopg")
        uri = f"postgresql://postgres:voyagerpass123@{HOST}:5455/voyager_graph"
        try:
            conn = psycopg.connect(uri, autocommit=True, connect_timeout=2)
        except Exception as exc:  # pragma: no cover - environment dependent
            pytest.skip(f"Apache AGE not reachable on {HOST}:5455: {exc}")

        created_graph = False
        with conn.cursor() as cur:
            cur.execute("LOAD 'age';")
            cur.execute('SET search_path = ag_catalog, "$user", public;')
            cur.execute(
                "SELECT count(*) FROM ag_catalog.ag_graph WHERE name = %s;",
                (LIVE_AGE_GRAPH,),
            )
            if cur.fetchone()[0] == 0:
                cur.execute("SELECT ag_catalog.create_graph(%s);", (LIVE_AGE_GRAPH,))
                created_graph = True

        if backend == "native":
            session = Session(
                "age://postgres:voyagerpass123@"
                f"{HOST}:5455/voyager_graph?graph_name={LIVE_AGE_GRAPH}",
                dialect="age",
                graph_name=LIVE_AGE_GRAPH,
                enable_identity_map=True,
            )
        else:
            session = Session(
                bridge=create_bridge(uri, graph_name=LIVE_AGE_GRAPH),
                dialect="age",
                graph_name=LIVE_AGE_GRAPH,
                enable_identity_map=True,
            )

        def _close_age() -> None:
            session.close()
            try:
                if created_graph:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT ag_catalog.drop_graph(%s, true);",
                            (LIVE_AGE_GRAPH,),
                        )
                conn.close()
            except Exception:  # pragma: no cover - best-effort cleanup
                pass

        return session, _close_age

    raise AssertionError(f"unknown live engine {engine!r}")


@pytest.mark.live
@pytest.mark.parametrize("backend", ["native", "bridge"])
@pytest.mark.parametrize("engine", ["neo4j", "memgraph", "falkordb", "age"])
def test_live_relationship_navigation(engine: str, backend: str) -> None:
    session, close = _open_live_session(engine, backend)
    try:
        if backend == "native":
            assert session.backend == "native"
        else:
            assert session.backend == "bridge"
        _exercise_live_relationship_navigation(
            session,
            write_with_flush=True,
        )
    finally:
        close()


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_async_relationship_navigation_on_native_session() -> None:
    if not _port_open(7687):
        pytest.skip(f"Neo4j not reachable on {HOST}:7687")
    session = AsyncSession(
        f"bolt://neo4j:voyagerpass123@{HOST}:7687",
        dialect="cypher",
        enable_identity_map=True,
    )
    assert session.backend == "native"

    async def cleanup() -> None:
        await session.execute("MATCH (n:Feat121LivePerson) DETACH DELETE n")

    await cleanup()
    try:
        alice = Feat121LivePerson(id="alice", name="Alice", age=30)
        bob = Feat121LivePerson(id="bob", name="Bob", age=18)
        carol = Feat121LivePerson(id="carol", name="Carol", age=16)
        for person in (alice, bob, carol):
            session.register(person)
        await session.flush()

        alice.friends.connect(bob, properties={"since": 2026})
        alice.friends.connect(carol)
        await session.flush()

        friends = {person.get("id") for person in await alice.friends.all()}
        assert friends == {"bob", "carol"}
        assert {
            person.get("id") for person in await alice.friends.filter(Feat121LivePerson.age >= 18)
        } == {"bob"}
    finally:
        await cleanup()
        await session.close()


@node(label="Feat121PgqSource")
class Feat121PgqSource(Node):
    id: str = Field(primary_key=True)
    name: str = Field()
    targets: RelationshipTo[Feat121PgqTarget] = RelationshipTo(
        target="Feat121PgqTarget", rel_type="FEAT121_PGQ_KNOWS"
    )


@node(label="Feat121PgqTarget")
class Feat121PgqTarget(Node):
    id: str = Field(primary_key=True)
    name: str = Field()
    age: int = 0


@relationship(
    type_name="FEAT121_PGQ_KNOWS",
    source_node=Feat121PgqSource,
    target_node=Feat121PgqTarget,
)
class Feat121PgqKnows(Relationship):
    since: int = 2026


PGQ_GRAPH = "feat121_pgq_navigation"


def _assert_pgq_navigation(session: Session) -> None:
    source = Feat121PgqSource(id="s1", name="Source")
    session.register(source, is_clean=True)

    targets = source.targets.all()
    assert {target.get("id") for target in targets} == {"t1", "t2"}
    assert {target.get("name") for target in targets} == {"Bob", "Carol"}
    assert {target.get("age") for target in targets} == {18, 16}

    adults = source.targets.filter(Feat121PgqTarget.age >= 18)
    assert [target.get("id") for target in adults] == ["t1"]
    assert adults[0].get("name") == "Bob"


def test_pgq_ddl_supports_relationship_descriptors() -> None:
    ddl = SchemaManager.generate_pgq_ddl(
        PGQ_GRAPH, Feat121PgqSource, Feat121PgqTarget, Feat121PgqKnows
    )

    assert 'LABEL "Feat121PgqSource"' in ddl
    assert 'LABEL "Feat121PgqTarget"' in ddl
    assert 'LABEL "FEAT121_PGQ_KNOWS"' in ddl
    assert 'SOURCE KEY ("src_id") REFERENCES "feat121pgqsource" ("id")' in ddl
    assert 'DESTINATION KEY ("dst_id") REFERENCES "feat121pgqtarget" ("id")' in ddl


@pytest.mark.live
def test_live_postgres19_pgq_relationship_navigation() -> None:
    if not _port_open(5456):
        pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:5456")
    psycopg = pytest.importorskip("psycopg")
    uri = f"postgresql://postgres:voyagerpass123@{HOST}:5456/voyager_graph"
    try:
        conn = psycopg.connect(uri, autocommit=True, connect_timeout=2)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"PostgreSQL 19 not reachable on {HOST}:5456: {exc}")

    cur = conn.cursor()
    session: Session | None = None
    try:
        cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {PGQ_GRAPH} CASCADE;")
        cur.execute(
            "DROP TABLE IF EXISTS feat121_pgq_knows CASCADE; "
            "DROP TABLE IF EXISTS feat121pgqsource CASCADE; "
            "DROP TABLE IF EXISTS feat121pgqtarget CASCADE;"
        )
        cur.execute("CREATE TABLE feat121pgqsource (id TEXT PRIMARY KEY, name TEXT);")
        cur.execute("CREATE TABLE feat121pgqtarget (id TEXT PRIMARY KEY, name TEXT, age INT);")
        cur.execute(
            "CREATE TABLE feat121_pgq_knows ("
            "src_id TEXT REFERENCES feat121pgqsource(id), "
            "dst_id TEXT REFERENCES feat121pgqtarget(id), "
            "since INT, PRIMARY KEY (src_id, dst_id));"
        )
        cur.execute("INSERT INTO feat121pgqsource VALUES ('s1', 'Source');")
        cur.execute("INSERT INTO feat121pgqtarget VALUES ('t1', 'Bob', 18), ('t2', 'Carol', 16);")
        cur.execute("INSERT INTO feat121_pgq_knows VALUES ('s1', 't1', 2026), ('s1', 't2', 2026);")
        cur.execute(
            SchemaManager.generate_pgq_ddl(
                PGQ_GRAPH, Feat121PgqSource, Feat121PgqTarget, Feat121PgqKnows
            )
        )

        session = Session(
            bridge=create_bridge(uri),
            dialect="sql_pgq",
            graph_name=PGQ_GRAPH,
            enable_identity_map=True,
        )
        _assert_pgq_navigation(session)
    finally:
        if session is not None:
            session.close()
        try:
            cur.execute(f"DROP PROPERTY GRAPH IF EXISTS {PGQ_GRAPH} CASCADE;")
            cur.execute(
                "DROP TABLE IF EXISTS feat121_pgq_knows CASCADE; "
                "DROP TABLE IF EXISTS feat121pgqsource CASCADE; "
                "DROP TABLE IF EXISTS feat121pgqtarget CASCADE;"
            )
        except Exception:  # pragma: no cover - best-effort cleanup
            pass
        conn.close()


@pytest.mark.live
def test_embedded_duckpgq_relationship_navigation() -> None:
    duckdb = pytest.importorskip("duckdb")
    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    try:
        conn.execute("LOAD 'duckpgq';")
    except Exception as exc:  # pragma: no cover - environment dependent
        conn.close()
        pytest.skip(f"DuckPGQ extension could not be loaded: {exc}")

    session: Session | None = None
    try:
        conn.execute("CREATE TABLE feat121pgqsource (id VARCHAR PRIMARY KEY, name VARCHAR);")
        conn.execute(
            "CREATE TABLE feat121pgqtarget (id VARCHAR PRIMARY KEY, name VARCHAR, age INT);"
        )
        conn.execute(
            "CREATE TABLE feat121pgqknows "
            "(feat121pgqsource_id VARCHAR, feat121pgqtarget_id VARCHAR, since INT);"
        )
        conn.execute("INSERT INTO feat121pgqsource VALUES ('s1', 'Source');")
        conn.execute("INSERT INTO feat121pgqtarget VALUES ('t1', 'Bob', 18), ('t2', 'Carol', 16);")
        conn.execute("INSERT INTO feat121pgqknows VALUES ('s1', 't1', 2026), ('s1', 't2', 2026);")
        conn.execute(
            f"""
            CREATE PROPERTY GRAPH {PGQ_GRAPH}
              VERTEX TABLES (
                feat121pgqsource LABEL "Feat121PgqSource",
                feat121pgqtarget LABEL "Feat121PgqTarget"
              )
              EDGE TABLES (
                feat121pgqknows
                  SOURCE KEY (feat121pgqsource_id) REFERENCES feat121pgqsource (id)
                  DESTINATION KEY (feat121pgqtarget_id) REFERENCES feat121pgqtarget (id)
                  LABEL "FEAT121_PGQ_KNOWS"
              );
            """
        )
        session = Session(
            bridge=conn,
            dialect="duckpgq",
            graph_name=PGQ_GRAPH,
            enable_identity_map=True,
        )
        _assert_pgq_navigation(session)
    finally:
        if session is not None:
            session.close()
        else:
            conn.close()

    from voyager_ogm._voyager_rs import NativeSchemaRegistry

    entry = NativeSchemaRegistry.global_registry().get_node("Feat121Person")
    declarations = entry["relationship_declarations"]
    assert declarations[1]["direction"] == "incoming"
    assert declarations[0]["target_model"] == "Feat121Person"
    assert declarations[0]["relationship_model"] == "Feat121Knows"
